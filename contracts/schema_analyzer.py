#!/usr/bin/env python3
"""SchemaEvolutionAnalyzer: Diffs schema snapshots and classifies changes.

Usage:
    python contracts/schema_analyzer.py \
        --contract-id week3-document-refinery-extractions \
        --output validation_reports/schema_evolution.json
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml


def load_snapshots(contract_id):
    """Load all timestamped snapshots for a contract."""
    snapshot_dir = Path("schema_snapshots") / contract_id
    if not snapshot_dir.exists():
        return []

    snapshots = []
    for path in sorted(snapshot_dir.glob("*.yaml")):
        with open(path) as f:
            data = yaml.safe_load(f)
            snapshots.append({
                "path": str(path),
                "timestamp": path.stem,
                "contract": data
            })

    return snapshots


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
            "old_value": old_clause,
            "new_value": new_clause
        })

    return changes


def generate_migration_impact(contract_id, changes, old_contract, new_contract):
    """Generate a migration impact report for breaking changes."""
    breaking = [c for c in changes if c["classification"] == "BREAKING"]

    # Get downstream from contract lineage
    downstream = new_contract.get("lineage", {}).get("downstream", [])

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
            "affected_consumers": [d.get("id", "unknown") for d in downstream],
            "consumer_count": len(downstream)
        },
        "migration_checklist": [],
        "rollback_plan": []
    }

    for i, bc in enumerate(breaking, 1):
        impact["migration_checklist"].append(
            f"{i}. Update downstream consumers for field '{bc['field']}': {bc['description']}"
        )
        impact["rollback_plan"].append(
            f"{i}. Revert field '{bc['field']}' to previous state: {json.dumps(bc['old_value'])}"
        )

    if not breaking:
        impact["migration_checklist"].append("No breaking changes detected. Safe to deploy.")
        impact["rollback_plan"].append("No rollback needed.")

    return impact


def main():
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer")
    parser.add_argument("--contract-id", required=True, help="Contract ID to analyze")
    parser.add_argument("--output", required=True, help="Output path for evolution report JSON")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"SchemaEvolutionAnalyzer — {args.contract_id}")
    print(f"{'='*60}")

    snapshots = load_snapshots(args.contract_id)
    print(f"  Found {len(snapshots)} snapshots")

    if len(snapshots) < 2:
        print("  WARNING: Need at least 2 snapshots to diff. Run the generator twice.")
        # Create a minimal report
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
        print(f"  Comparing: {old['timestamp']} → {new['timestamp']}")

        changes = diff_schemas(old["contract"], new["contract"])
        report = generate_migration_impact(
            args.contract_id, changes, old["contract"], new["contract"]
        )
        report["snapshots_compared"] = {
            "old": old["path"],
            "new": new["path"]
        }

        breaking = [c for c in changes if c["classification"] == "BREAKING"]
        compatible = [c for c in changes if c["classification"] == "COMPATIBLE"]
        print(f"  Changes: {len(changes)} total — {len(breaking)} BREAKING, {len(compatible)} COMPATIBLE")
        print(f"  Verdict: {report['overall_verdict']}")

        for c in changes:
            marker = "🔴" if c["classification"] == "BREAKING" else "🟢"
            print(f"    {marker} {c['field']}: {c['description']}")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n  Report written to {output_path}")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
