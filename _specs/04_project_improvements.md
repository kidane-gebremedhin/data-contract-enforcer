# Spec 04: Project Improvements to Match Requirements

## Problem

Comparing the current codebase against the project document and practitioner manual checklists, several gaps need to be addressed for full compliance.

## Gap Analysis

### Saturday Checklist Items — Current Status

| Item | Status | Gap |
|------|--------|-----|
| `contract_registry/subscriptions.yaml` — min 4 subscriptions | MISSING | No registry file exists |
| `contracts/attributor.py` — registry as primary blast radius | PARTIAL | Uses lineage only, not registry-first |
| `violation_log/violations.jsonl` — min 3 entries, 1 real, 1 injected | DONE | 7 entries present |
| `schema_snapshots/` — min 2 timestamped snapshots per contract | PARTIAL | Only 1 snapshot per contract |
| `enforcer_report/report_data.json` — recommendations reference real file paths | PARTIAL | Recommendations are generic, not file-specific |
| `--mode` flag (AUDIT/WARN/ENFORCE) on ValidationRunner | MISSING | No mode flag implemented |
| Generated contracts include `registry_subscribers` | MISSING | Only lineage downstream |
| Week 5 contract has min 6 clauses | DONE | 12 clauses present |
| Week 3 contract has min 8 clauses, confidence 0.0/1.0 | DONE | 11 clauses, confidence bounded |

## Changes Required

### 1. Add `--mode` Flag to ValidationRunner

**File:** `contracts/runner.py`

The project document requires AUDIT, WARN, and ENFORCE modes:

| Mode | Behavior |
|------|----------|
| AUDIT | Run checks, log results, never block. Default for first run. |
| WARN | Block on CRITICAL only. Warn on HIGH/MEDIUM. |
| ENFORCE | Block pipeline on any CRITICAL or HIGH violation. Quarantine data. |

Add to argparse:
```python
parser.add_argument("--mode", choices=["AUDIT", "WARN", "ENFORCE"],
                    default="AUDIT", help="Enforcement mode")
```

Add to report output:
```python
report["mode"] = args.mode
report["pipeline_action"] = determine_pipeline_action(results, args.mode)
```

```python
def determine_pipeline_action(results, mode):
    """Determine pipeline action based on mode and results."""
    has_critical = any(r["status"] == "FAIL" and r["severity"] == "CRITICAL" for r in results)
    has_high = any(r["status"] == "FAIL" and r["severity"] == "HIGH" for r in results)
    
    if mode == "AUDIT":
        return "PASS"  # never block
    elif mode == "WARN":
        return "BLOCK" if has_critical else "PASS"
    elif mode == "ENFORCE":
        return "BLOCK" if (has_critical or has_high) else "PASS"
    return "PASS"
```

### 2. Generate Second Schema Snapshot

The project requires 2+ snapshots per contract. Currently only 1 exists.

**Action:** Run generator on violated data to produce a second snapshot:
```bash
python contracts/generator.py \
  --source outputs/week3/extractions_violated.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/
```

This creates a second timestamped snapshot in `schema_snapshots/week3-document-refinery-extractions/` with different statistics (confidence range [70, 98] instead of [0.70, 0.98]).

Similarly for week5, run the generator a second time with any data modification.

### 3. Make Recommendations Reference Real File Paths

**File:** `contracts/report_generator.py`

Current recommendations are generic: "Update source pipeline to output fact_confidence as float 0.0-1.0"

Required: specific file references per the project document:
"update src/week3/extractor.py line 47 to output confidence as float 0.0-1.0 per contract week3-document-refinery-extractions clause extracted_facts.confidence.range"

Update `generate_report()` to produce file-path-specific recommendations:

```python
recommendations = []
for f in top_3:
    col = f.get("column_name", "unknown")
    check_id = f.get("check_id", "")
    contract_id = check_id.split(".")[0] if "." in check_id else ""
    
    if "confidence" in col and "range" in check_id:
        recommendations.append(
            f"Update the confidence field output in the Week 3 extraction pipeline "
            f"(contracts/generator.py or upstream extractor) to produce float 0.0-1.0 "
            f"per contract {contract_id} clause {check_id}."
        )
    elif "drift" in check_id:
        recommendations.append(
            f"Investigate statistical drift in {col}. Current z-score exceeds threshold. "
            f"Re-establish baseline in schema_snapshots/baselines.json after confirming data correctness."
        )
    else:
        recommendations.append(
            f"Fix {f.get('check_type', 'unknown')} violation in {col} "
            f"per contract clause {check_id}: {f.get('message', '')}."
        )

# Always add CI integration recommendation
recommendations.append(
    "Add contracts/runner.py --mode ENFORCE as a required CI step "
    "in your deployment pipeline before any data-consuming task."
)
recommendations.append(
    "Schedule monthly baseline refresh for statistical drift thresholds "
    "by deleting schema_snapshots/baselines.json and re-running "
    "contracts/runner.py on clean data."
)
```

### 4. Add `injection_note` to Violated Violation Records

**File:** `violation_log/violations.jsonl`

The Saturday checklist requires: "At least 1 injected with injection_note: true."

Ensure that violations from violated data include:
```json
{
  "injection_note": true,
  "injection_type": "scale_change",
  "injection_description": "fact_confidence multiplied by 100 to simulate upstream breaking change"
}
```

Update `contracts/attributor.py` to accept an `--injected` flag that adds this metadata.

### 5. Add `--registry` Flag to Attributor

**File:** `contracts/attributor.py`

```python
parser.add_argument("--registry", default="contract_registry/subscriptions.yaml",
                    help="Path to contract registry subscriptions YAML")
```

## Files to Modify

| File | Action |
|------|--------|
| `contracts/runner.py` | MODIFY — add `--mode` flag with AUDIT/WARN/ENFORCE |
| `contracts/report_generator.py` | MODIFY — file-specific recommendations |
| `contracts/attributor.py` | MODIFY — add `--registry`, `--injected` flags |
| `violation_log/violations.jsonl` | REGENERATE — include `injection_note` |
| Schema snapshots | REGENERATE — create 2nd snapshot per contract |

## Acceptance Criteria

- [ ] `--mode AUDIT|WARN|ENFORCE` works on runner.py
- [ ] Report output includes `pipeline_action` field
- [ ] 2+ timestamped snapshots per contract in `schema_snapshots/`
- [ ] Recommendations in report reference specific contract clauses and file paths
- [ ] At least 1 violation record has `injection_note: true`
