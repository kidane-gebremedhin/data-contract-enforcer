# Spec 07: Migration Impact Report Format

## Problem

The project document requires: "When a breaking change is detected, auto-generate `migration_impact_{contract_id}_{timestamp}.json` containing: the exact diff (human-readable), compatibility verdict, full blast radius from the lineage graph, per-consumer failure mode analysis, an ordered migration checklist, and a rollback plan."

Currently, `contracts/schema_analyzer.py` generates a basic migration impact section within the schema evolution report, but:
1. It does not write a separate `migration_impact_*.json` file
2. Per-consumer failure mode analysis is missing
3. The migration checklist is generic (no per-consumer steps)
4. The rollback plan lacks detail (just "revert field to previous state")
5. No registry integration for per-subscriber impact

## Changes Required

### 1. Separate Migration Impact Report File

**File:** `contracts/schema_analyzer.py`

When breaking changes are detected, write a dedicated migration impact file:

```python
def write_migration_impact_report(impact, contract_id):
    """Write a separate migration impact report file."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"migration_impact_{contract_id}_{ts}.json"
    output_path = Path("validation_reports") / filename
    
    with open(output_path, "w") as f:
        json.dump(impact, f, indent=2)
    
    print(f"  Migration impact report: {output_path}")
    return str(output_path)
```

### 2. Full Migration Impact Report Schema

```json
{
  "report_id": "uuid-v4",
  "contract_id": "week3-document-refinery-extractions",
  "generated_at": "2026-04-03T12:00:00Z",
  "snapshots_compared": {
    "old": "schema_snapshots/week3-.../20260401_215526.yaml",
    "new": "schema_snapshots/week3-.../20260403_120000.yaml"
  },
  "summary": {
    "total_changes": 3,
    "breaking_changes": 1,
    "compatible_changes": 2,
    "overall_verdict": "BREAKING"
  },
  "changes": [
    {
      "field": "fact_confidence",
      "change_type": "field_modified",
      "classification": "BREAKING",
      "description": "Range change maximum 1.0 -> 100.0",
      "old_value": {"type": "number", "minimum": 0.0, "maximum": 1.0, "required": true},
      "new_value": {"type": "number", "minimum": 0.0, "maximum": 100.0, "required": true},
      "human_readable_diff": "The maximum allowed value for fact_confidence changed from 1.0 to 100.0. This is a BREAKING change because downstream consumers expect values in the 0.0-1.0 range."
    }
  ],
  "blast_radius": {
    "source": "registry",
    "total_affected_subscribers": 2,
    "registry_subscribers": [
      {
        "subscriber_id": "week4-cartographer",
        "contact": "week4-team@org.com",
        "validation_mode": "ENFORCE",
        "breaking_fields_affected": ["extracted_facts.confidence"],
        "failure_mode": "Node ranking in lineage graph will produce incorrect weights. Edge weights that were 0.0-1.0 will now be 0-100, distorting graph traversal and shortest-path computations.",
        "estimated_impact": "HIGH — graph analysis results will be silently wrong",
        "action_required": "Update node ranking logic to normalize confidence values, OR block deployment until producer reverts."
      },
      {
        "subscriber_id": "week7-enforcer",
        "contact": "week7-team@org.com",
        "validation_mode": "AUDIT",
        "breaking_fields_affected": ["extracted_facts.confidence"],
        "failure_mode": "Embedding drift baseline was computed with 0.0-1.0 confidence values. New values will trigger false drift alerts.",
        "estimated_impact": "MEDIUM — false positive alerts, not data corruption",
        "action_required": "Re-establish embedding drift baseline after migration."
      }
    ],
    "lineage_enrichment": {
      "direct_nodes": ["pipeline::week4", "pipeline::week5"],
      "transitive_nodes": [],
      "contamination_depth": 1
    }
  },
  "migration_checklist": [
    "1. [IMMEDIATE] Notify all 2 registered subscribers of breaking change via contact email.",
    "2. [BEFORE DEPLOY] week4-cartographer (ENFORCE mode): Update node ranking logic to handle new confidence range, OR add normalization step.",
    "3. [BEFORE DEPLOY] week7-enforcer (AUDIT mode): Delete schema_snapshots/baselines.json and schema_snapshots/embedding_baselines.npz to force baseline re-establishment.",
    "4. [DEPLOY] Push updated schema with new confidence range.",
    "5. [AFTER DEPLOY] Run contracts/runner.py on new data to verify all checks pass.",
    "6. [AFTER DEPLOY] Run contracts/schema_analyzer.py to confirm evolution is tracked.",
    "7. [1 WEEK LATER] Review statistical drift thresholds with new data distribution."
  ],
  "rollback_plan": [
    "1. Revert the producer code change that modified confidence output range.",
    "2. Re-run contracts/generator.py on the original data to restore the previous contract.",
    "3. Delete the latest schema snapshot (the one with maximum=100.0).",
    "4. Verify with contracts/runner.py that all checks pass on rolled-back data.",
    "5. Notify subscribers that rollback is complete."
  ],
  "recommended_deprecation_timeline": {
    "announce": "Day 0 — Notify all registry subscribers of planned change.",
    "alias_period": "Days 1-14 — Producer outputs both old and new format. Consumer reads old.",
    "migration": "Days 14-21 — Consumers migrate to new format. Producer continues dual output.",
    "cutover": "Day 21 — Producer drops old format. Verify all consumers updated.",
    "cleanup": "Day 28 — Remove alias code. Update contracts. Refresh baselines."
  }
}
```

### 3. Per-Consumer Failure Mode Analysis

**File:** `contracts/schema_analyzer.py`

New function to analyze per-consumer impact:

```python
def analyze_consumer_failure_modes(breaking_changes, registry_subscribers):
    """For each subscriber, describe what breaks and how."""
    analyzed = []
    for sub in registry_subscribers:
        affected_fields = []
        for bc in breaking_changes:
            for bf in sub.get("breaking_fields", []):
                if bf["field"] == bc["field"] or bc["field"].endswith(bf["field"].split(".")[-1]):
                    affected_fields.append({
                        "field": bf["field"],
                        "reason": bf["reason"],
                        "change": bc["description"]
                    })
        
        if affected_fields:
            # Determine impact severity based on validation_mode
            mode = sub.get("validation_mode", "AUDIT")
            if mode == "ENFORCE":
                impact = "HIGH — pipeline will be blocked"
            elif mode == "WARN":
                impact = "MEDIUM — pipeline will warn but continue"
            else:
                impact = "LOW — logged only (AUDIT mode)"
            
            analyzed.append({
                "subscriber_id": sub["subscriber_id"],
                "contact": sub.get("contact", "unknown"),
                "validation_mode": mode,
                "breaking_fields_affected": [af["field"] for af in affected_fields],
                "failure_mode": "; ".join(af["reason"] for af in affected_fields),
                "estimated_impact": impact,
                "action_required": f"Review and update consumer logic for: {', '.join(af['field'] for af in affected_fields)}"
            })
    
    return analyzed
```

### 4. Human-Readable Diff

Add a `human_readable_diff` field to each change:

```python
def generate_human_readable_diff(change):
    """Produce a plain-English description of the change."""
    field = change["field"]
    classification = change["classification"]
    
    if change["change_type"] == "field_added":
        return f"New field '{field}' was added. {'This is BREAKING because it is required.' if classification == 'BREAKING' else 'Consumers can safely ignore it.'}"
    elif change["change_type"] == "field_removed":
        return f"Field '{field}' was removed. All consumers that depend on this field will fail."
    else:
        old = change.get("old_value", {})
        new = change.get("new_value", {})
        diffs = []
        for key in set(list(old.keys()) + list(new.keys())):
            if old.get(key) != new.get(key):
                diffs.append(f"{key}: {old.get(key)} -> {new.get(key)}")
        return f"Field '{field}' modified: {'; '.join(diffs)}. Classification: {classification}."
```

### 5. Deprecation Timeline Template

Include a recommended deprecation timeline for breaking changes, following the practitioner manual's "two-sprint deprecation minimum" guidance.

## Files to Modify

| File | Action |
|------|--------|
| `contracts/schema_analyzer.py` | MODIFY — separate migration impact file, per-consumer analysis, human-readable diffs, rollback plan, deprecation timeline |

## Acceptance Criteria

- [ ] Breaking changes produce `validation_reports/migration_impact_{contract_id}_{timestamp}.json`
- [ ] Migration impact includes `blast_radius.registry_subscribers` with per-consumer failure mode
- [ ] Each change has `human_readable_diff` field
- [ ] Migration checklist is ordered with timing tags ([IMMEDIATE], [BEFORE DEPLOY], etc.)
- [ ] Rollback plan has concrete steps referencing actual file paths
- [ ] Deprecation timeline template included for breaking changes
- [ ] `--registry` flag wires subscriber data into the report
