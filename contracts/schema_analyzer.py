#!/usr/bin/env python3
"""SchemaEvolutionAnalyzer: Diffs schema snapshots and classifies changes.

Features:
    - Quick diff via schema hash comparison before full field diff
    - Registry-based blast radius for breaking changes
    - Per-consumer failure mode analysis
    - Human-readable diffs (plain-English descriptions)
    - Migration checklist with timing tags
    - Rollback plan with file paths
    - Deprecation timeline template

Usage:
    python contracts/schema_analyzer.py \
        --contract-id week3-document-refinery-extractions \
        --registry contract_registry/subscriptions.yaml \
        --output validation_reports/schema_evolution.json
"""

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from contracts.registry import load_registry, get_subscribers, get_breaking_field_subscribers


def load_snapshots(contract_id, since=None):
    """Load all timestamped snapshots for a contract, optionally filtered by --since."""
    snapshot_dir = Path("schema_snapshots") / contract_id
    if not snapshot_dir.exists():
        return []

    snapshots = []
    for path in sorted(snapshot_dir.glob("*.yaml")):
        if since and path.stem < since:
            continue
        with open(path) as f:
            data = yaml.safe_load(f)
            snapshots.append({
                "path": str(path),
                "timestamp": path.stem,
                "contract": data
            })

    return snapshots


def quick_hash_diff(contract_id):
    """Check schema_snapshots/index.jsonl for hash changes before doing a full diff."""
    index_path = Path("schema_snapshots") / "index.jsonl"
    if not index_path.exists():
        return None

    entries = []
    with open(index_path) as f:
        for line in f:
            if line.strip():
                entry = json.loads(line)
                if entry.get("contract_id") == contract_id:
                    entries.append(entry)

    if len(entries) < 2:
        return None

    latest = entries[-1]
    previous = entries[-2]

    return {
        "hashes_match": latest["schema_hash"] == previous["schema_hash"],
        "old_hash": previous["schema_hash"],
        "new_hash": latest["schema_hash"],
        "old_snapshot": previous["snapshot_path"],
        "new_snapshot": latest["snapshot_path"],
        "old_timestamp": previous["snapshot_timestamp"],
        "new_timestamp": latest["snapshot_timestamp"]
    }


def classify_change(field_name, old_clause, new_clause):
    """Classify a schema change using the taxonomy."""
    if old_clause is None:
        if new_clause.get("required", False):
            return "BREAKING", "Add non-nullable column — coordinate with all producers"
        return "COMPATIBLE", "Add nullable column — consumers can ignore"

    if new_clause is None:
        return "BREAKING", "Remove column — deprecation period mandatory"

    # Type change
    if old_clause.get("type") != new_clause.get("type"):
        return "BREAKING", f'Type change {old_clause.get("type")} -> {new_clause.get("type")}'

    # Range change
    if old_clause.get("maximum") != new_clause.get("maximum"):
        return "BREAKING", (f'Range change maximum '
                            f'{old_clause.get("maximum")} -> {new_clause.get("maximum")}')

    if old_clause.get("minimum") != new_clause.get("minimum"):
        return "BREAKING", (f'Range change minimum '
                            f'{old_clause.get("minimum")} -> {new_clause.get("minimum")}')

    # Enum change
    if old_clause.get("enum") != new_clause.get("enum"):
        old_enum = set(old_clause.get("enum", []))
        new_enum = set(new_clause.get("enum", []))
        added = new_enum - old_enum
        removed = old_enum - new_enum
        if removed:
            return "BREAKING", f"Enum values removed: {removed}"
        return "COMPATIBLE", f"Enum values added: {added}"

    # Pattern change
    if old_clause.get("pattern") != new_clause.get("pattern"):
        return "BREAKING", (f'Pattern change '
                            f'{old_clause.get("pattern")} -> {new_clause.get("pattern")}')

    # Required change
    if not old_clause.get("required") and new_clause.get("required"):
        return "BREAKING", "Field changed from optional to required"
    if old_clause.get("required") and not new_clause.get("required"):
        return "COMPATIBLE", "Field changed from required to optional"

    # Description change only
    if old_clause.get("description") != new_clause.get("description"):
        return "COMPATIBLE", "Description updated"

    return "COMPATIBLE", "No material change"


def human_readable_diff(change):
    """Convert a schema change to a plain-English description."""
    field = change["field"]
    ctype = change["change_type"]
    classification = change["classification"]

    if ctype == "field_added":
        required = change.get("new_value", {}).get("required", False)
        return (f"A new {'required' if required else 'optional'} field '{field}' was added. "
                f"{'All producers must now supply this field.' if required else 'Consumers can safely ignore it.'}")

    if ctype == "field_removed":
        return (f"The field '{field}' was removed. "
                f"Any consumer reading this field will break. A deprecation period is mandatory.")

    if ctype == "field_modified":
        desc = change["description"]
        if "Type change" in desc:
            return f"The data type of '{field}' changed ({desc}). All consumers parsing this field must update."
        if "Range change" in desc:
            return f"The allowed range for '{field}' changed ({desc}). Downstream validation rules need updating."
        if "Enum values removed" in desc:
            return f"Allowed values for '{field}' were narrowed ({desc}). Consumers depending on removed values will fail."
        if "Pattern change" in desc:
            return f"The expected format pattern for '{field}' changed ({desc}). Regex-based consumers must update."
        if "optional to required" in desc:
            return f"The field '{field}' became required. Producers that omit it will now cause validation failures."
        return f"The field '{field}' was modified: {desc}"

    return change["description"]


def diff_schemas(old_contract, new_contract):
    """Diff two contract schemas and classify each change."""
    old_schema = old_contract.get("schema", {})
    new_schema = new_contract.get("schema", {})

    all_fields = set(list(old_schema.keys()) + list(new_schema.keys()))
    changes = []

    for field in sorted(all_fields):
        old_clause = old_schema.get(field)
        new_clause = new_schema.get(field)

        if old_clause == new_clause:
            continue

        classification, description = classify_change(field, old_clause, new_clause)
        changes.append({
            "field": field,
            "change_type": (
                "field_added" if old_clause is None else
                "field_removed" if new_clause is None else
                "field_modified"
            ),
            "classification": classification,
            "description": description,
            "human_readable": "",
            "old_value": old_clause,
            "new_value": new_clause
        })

    # Fill in human-readable descriptions
    for change in changes:
        change["human_readable"] = human_readable_diff(change)

    return changes


def per_consumer_failure_modes(contract_id, changes, registry_path):
    """Analyze how each breaking change affects each registered consumer."""
    if not Path(registry_path).exists():
        return []

    registry = load_registry(registry_path)
    subscribers = get_subscribers(registry, contract_id)
    breaking = [c for c in changes if c["classification"] == "BREAKING"]

    consumer_impacts = []
    for sub in subscribers:
        sub_breaking_fields = {bf["field"]: bf["reason"] for bf in sub.get("breaking_fields", [])}
        fields_consumed = set(sub.get("fields_consumed", []))

        affected_changes = []
        for bc in breaking:
            field = bc["field"]
            # Check if this breaking change affects a field the consumer cares about
            if field in fields_consumed or field in sub_breaking_fields:
                affected_changes.append({
                    "field": field,
                    "change": bc["description"],
                    "reason_critical": sub_breaking_fields.get(field, "Field is consumed by this subscriber"),
                    "human_readable": bc["human_readable"]
                })
            # Also check prefix matching for nested fields
            for bf_field in sub_breaking_fields:
                if field.startswith(bf_field) or bf_field.startswith(field):
                    if not any(ac["field"] == field for ac in affected_changes):
                        affected_changes.append({
                            "field": field,
                            "change": bc["description"],
                            "reason_critical": sub_breaking_fields[bf_field],
                            "human_readable": bc["human_readable"]
                        })

        if affected_changes:
            consumer_impacts.append({
                "subscriber_id": sub["subscriber_id"],
                "subscriber_team": sub.get("subscriber_team", "unknown"),
                "contact": sub.get("contact", "unknown"),
                "validation_mode": sub.get("validation_mode", "AUDIT"),
                "affected_changes": affected_changes,
                "failure_severity": "CRITICAL" if sub.get("validation_mode") == "ENFORCE" else "HIGH"
            })

    return consumer_impacts


def generate_migration_checklist(changes, consumer_impacts):
    """Generate a migration checklist with timing tags."""
    checklist = []
    breaking = [c for c in changes if c["classification"] == "BREAKING"]

    if not breaking:
        checklist.append("[INFO] No breaking changes detected. Safe to deploy.")
        return checklist

    # IMMEDIATE items
    for bc in breaking:
        if bc["change_type"] == "field_removed":
            checklist.append(
                f"[IMMEDIATE] Notify all consumers of field '{bc['field']}' removal. "
                f"Begin deprecation period."
            )
        elif "Type change" in bc["description"]:
            checklist.append(
                f"[IMMEDIATE] Field '{bc['field']}' type changed. "
                f"Coordinate dual-write period with upstream producers."
            )

    # BEFORE DEPLOY items
    for ci in consumer_impacts:
        for ac in ci["affected_changes"]:
            checklist.append(
                f"[BEFORE DEPLOY] Update {ci['subscriber_id']} ({ci['subscriber_team']}) "
                f"for field '{ac['field']}': {ac['change']}. "
                f"Contact: {ci['contact']}"
            )

    # AFTER DEPLOY items
    for bc in breaking:
        if bc["change_type"] == "field_removed":
            checklist.append(
                f"[AFTER DEPLOY] Verify no consumer references field '{bc['field']}'. "
                f"Remove from contract after confirmation."
            )
        checklist.append(
            f"[AFTER DEPLOY] Re-run validation for field '{bc['field']}' to confirm compliance."
        )

    return checklist


def generate_rollback_plan(changes, contract_id):
    """Generate a rollback plan with file paths."""
    breaking = [c for c in changes if c["classification"] == "BREAKING"]
    plan = []

    if not breaking:
        plan.append("No rollback needed — all changes are compatible.")
        return plan

    safe_name = contract_id.replace("-", "_")
    plan.append(f"1. Revert contract: `git checkout HEAD~1 -- generated_contracts/{safe_name}.yaml`")
    plan.append(f"2. Revert dbt schema: `git checkout HEAD~1 -- generated_contracts/{safe_name}_dbt.yml`")

    for i, bc in enumerate(breaking, 3):
        plan.append(
            f"{i}. Restore field '{bc['field']}' to previous state: "
            f"{json.dumps(bc['old_value'], default=str)}"
        )

    plan.append(f"{len(breaking) + 3}. Re-run validation: "
                f"`python contracts/runner.py --contract generated_contracts/{safe_name}.yaml "
                f"--data <DATA_PATH> --mode ENFORCE --output validation_reports/rollback_check.json`")
    plan.append(f"{len(breaking) + 4}. Regenerate schema snapshot: "
                f"`python contracts/generator.py --source <DATA_PATH> "
                f"--contract-id {contract_id} --output generated_contracts/`")

    return plan


def generate_deprecation_timeline(field_name, contract_id):
    """Generate a deprecation timeline template for a removed/changed field."""
    return {
        "field": field_name,
        "contract_id": contract_id,
        "phases": [
            {
                "phase": "ANNOUNCE",
                "description": f"Notify all consumers that '{field_name}' will change/be removed.",
                "timing": "T+0 (immediately)",
                "action": "Send notification to subscriber contacts from registry."
            },
            {
                "phase": "ALIAS",
                "description": f"Add backward-compatible alias for '{field_name}' if applicable.",
                "timing": "T+1 week",
                "action": "Update contract to support both old and new format."
            },
            {
                "phase": "MIGRATION",
                "description": "All consumers update to read from new field/format.",
                "timing": "T+2 weeks",
                "action": "Track consumer migration status via registry."
            },
            {
                "phase": "CUTOVER",
                "description": f"Stop producing old format of '{field_name}'.",
                "timing": "T+4 weeks",
                "action": "Update producer pipeline. Remove alias from contract."
            },
            {
                "phase": "CLEANUP",
                "description": "Remove all references to old field/format.",
                "timing": "T+6 weeks",
                "action": "Delete deprecated clauses. Archive old snapshots."
            }
        ]
    }


def generate_migration_impact(contract_id, changes, old_contract, new_contract,
                              registry_path=None):
    """Generate a migration impact report for breaking changes."""
    breaking = [c for c in changes if c["classification"] == "BREAKING"]

    # Get downstream from contract lineage
    downstream = new_contract.get("lineage", {}).get("downstream", [])
    registry_subs = new_contract.get("lineage", {}).get("registry_subscribers", [])

    # Per-consumer failure modes from registry
    consumer_impacts = []
    if registry_path and Path(registry_path).exists():
        consumer_impacts = per_consumer_failure_modes(
            contract_id, changes, registry_path
        )

    # Migration checklist with timing tags
    checklist = generate_migration_checklist(changes, consumer_impacts)

    # Rollback plan with file paths
    rollback = generate_rollback_plan(changes, contract_id)

    # Deprecation timelines for removed/type-changed fields
    deprecation_timelines = []
    for bc in breaking:
        if bc["change_type"] == "field_removed" or "Type change" in bc["description"]:
            deprecation_timelines.append(
                generate_deprecation_timeline(bc["field"], contract_id)
            )

    impact = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_changes": len(changes),
        "breaking_changes": len(breaking),
        "compatible_changes": len(changes) - len(breaking),
        "overall_verdict": "BREAKING" if breaking else "COMPATIBLE",
        "changes": changes,
        "blast_radius": {
            "source": "registry" if consumer_impacts else "lineage",
            "affected_consumers": (
                [ci["subscriber_id"] for ci in consumer_impacts]
                if consumer_impacts else
                [d.get("id", "unknown") for d in downstream]
            ),
            "consumer_count": len(consumer_impacts) or len(downstream),
            "registry_subscribers": [rs.get("id", "unknown") for rs in registry_subs]
        },
        "consumer_failure_modes": consumer_impacts,
        "migration_checklist": checklist,
        "rollback_plan": rollback,
        "deprecation_timelines": deprecation_timelines
    }

    return impact


def main():
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer")
    parser.add_argument("--contract-id", required=True, help="Contract ID to analyze")
    parser.add_argument("--since", default=None,
                        help="Only consider snapshots since this timestamp (YYYYMMDD_HHMMSS)")
    parser.add_argument("--registry", default="contract_registry/subscriptions.yaml",
                        help="Path to contract registry subscriptions YAML")
    parser.add_argument("--output", required=True, help="Output path for evolution report JSON")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"SchemaEvolutionAnalyzer — {args.contract_id}")
    print(f"{'='*60}")

    # Quick hash diff first
    hash_result = quick_hash_diff(args.contract_id)
    if hash_result:
        if hash_result["hashes_match"]:
            print(f"  Quick diff: schema hashes match — no changes detected")
        else:
            print(f"  Quick diff: schema hashes differ")
            print(f"    Old: {hash_result['old_hash'][:12]}... ({hash_result['old_timestamp']})")
            print(f"    New: {hash_result['new_hash'][:12]}... ({hash_result['new_timestamp']})")

    snapshots = load_snapshots(args.contract_id, since=args.since)
    print(f"  Found {len(snapshots)} snapshots")

    if len(snapshots) < 2:
        print("  WARNING: Need at least 2 snapshots to diff. Run the generator twice.")
        report = {
            "report_id": str(uuid.uuid4()),
            "contract_id": args.contract_id,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "total_changes": 0,
            "breaking_changes": 0,
            "compatible_changes": 0,
            "overall_verdict": "NO_DIFF_AVAILABLE",
            "changes": [],
            "snapshots_compared": len(snapshots),
            "message": "Insufficient snapshots. Run generator on original and modified data."
        }
    else:
        # Compare latest two snapshots
        old = snapshots[-2]
        new = snapshots[-1]
        print(f"  Comparing: {old['timestamp']} -> {new['timestamp']}")

        changes = diff_schemas(old["contract"], new["contract"])
        report = generate_migration_impact(
            args.contract_id, changes, old["contract"], new["contract"],
            registry_path=args.registry
        )
        report["snapshots_compared"] = {
            "old": old["path"],
            "new": new["path"]
        }
        if hash_result:
            report["hash_diff"] = hash_result

        breaking = [c for c in changes if c["classification"] == "BREAKING"]
        compatible = [c for c in changes if c["classification"] == "COMPATIBLE"]
        print(f"  Changes: {len(changes)} total — {len(breaking)} BREAKING, {len(compatible)} COMPATIBLE")
        print(f"  Verdict: {report['overall_verdict']}")

        for c in changes:
            marker = "BREAKING" if c["classification"] == "BREAKING" else "COMPATIBLE"
            print(f"    [{marker}] {c['field']}: {c['description']}")
            print(f"             {c['human_readable']}")

        # Print consumer impacts
        if report.get("consumer_failure_modes"):
            print(f"\n  Consumer failure modes:")
            for ci in report["consumer_failure_modes"]:
                print(f"    {ci['subscriber_id']} ({ci['subscriber_team']}): "
                      f"{len(ci['affected_changes'])} affected fields, "
                      f"severity={ci['failure_severity']}")

        # Write separate migration impact file when breaking changes detected
        if breaking:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            impact_path = Path(args.output).parent / f"migration_impact_{args.contract_id}_{ts}.json"
            with open(impact_path, "w") as f:
                json.dump(report, f, indent=2)
            print(f"\n  Migration impact written to {impact_path}")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n  Report written to {output_path}")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
