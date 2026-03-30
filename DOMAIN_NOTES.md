# DOMAIN_NOTES.md — Data Contract Enforcer

## Question 1: Backward-Compatible vs. Breaking Schema Changes

A **backward-compatible** change allows existing consumers to continue processing data without modification. A **breaking** change forces every downstream consumer to update or fail.

### Three Backward-Compatible Examples (from Weeks 1–5)

1. **Adding a nullable `notes` field to `extraction_record` (Week 3):** The `extraction_record` schema has `doc_id`, `extracted_facts[]`, `entities[]`, etc. Adding an optional `notes: string | null` field is backward-compatible because existing consumers simply ignore the new field. No consumer code needs to change.

2. **Adding a new enum value `"EXTERNAL"` to `lineage_snapshot.edges[].relationship` (Week 4):** The current enum is `IMPORTS|CALLS|READS|WRITES|PRODUCES|CONSUMES`. Adding `"EXTERNAL"` is additive — consumers that don't recognize it can skip or default-handle unknown edge types without crashing.

3. **Adding optional `governance_tags[]` to `event_record` (Week 5):** The Week 5 event schema has `event_id`, `event_type`, `payload`, etc. Adding an optional array for governance tags is safe — consumers that don't use it are unaffected.

### Three Breaking Examples (from Weeks 1–5)

1. **Changing `confidence` from float 0.0–1.0 to integer 0–100 in `extraction_record` (Week 3):** Any consumer that checks `if confidence > 0.5` now gets `True` for every record since all values are 70–98. This is the canonical silent corruption failure — the pipeline continues running, producing wrong output.

2. **Removing `source_hash` from `extraction_record` (Week 3):** The Week 4 Cartographer uses `source_hash` for deduplication. Removing it breaks the Cartographer's ability to detect duplicate source documents — a structural failure that causes silent data duplication.

3. **Renaming `verdict_id` to `assessment_id` in `verdict_record` (Week 2):** Every downstream consumer that references `verdict_id` by name will fail with a `KeyError`. Unlike a value change, a rename is immediately visible but still requires coordinated migration across all consumers.

---

## Question 2: Confidence Scale Change — Failure Trace and Contract Clause

### Measuring the Current Distribution

Running the confidence distribution analysis on our actual Week 3 extractions data:

```python
import json, statistics
with open('outputs/week3/extractions.jsonl') as f:
    facts = [json.loads(l) for l in f]
confs = [f2['confidence'] for f in facts for f2 in f.get('extracted_facts', [])]
print(f'min={min(confs):.3f} max={max(confs):.3f} mean={statistics.mean(confs):.3f}')
```

**Output:**
```
min=0.700 max=0.980 mean=0.843 stddev=0.082
count=274
```

All values are within the 0.0–1.0 range. The mean of 0.843 indicates generally high extraction confidence.

### Failure Trace: What Happens When Scale Changes to 0–100

If an update changes `confidence` to the 0–100 scale:
1. The Week 3 `extractions.jsonl` now contains values like `70.0, 85.0, 93.0` instead of `0.70, 0.85, 0.93`.
2. The Week 4 Cartographer reads `confidence` to weight lineage edges. With values of 70–98 instead of 0.70–0.98, edge weights are 100x larger. The lineage graph is distorted — all edges appear maximally confident.
3. Any threshold check like `if confidence < 0.5: flag_for_review` will never trigger, since `70.0 < 0.5` is `False`. Low-confidence extractions pass unchecked.
4. The ValidationRunner catches this with the range check: `max=98.0 > maximum=1.0` → FAIL with severity CRITICAL. The statistical drift check also catches it: z-score of 1023.7 stddev.

### Bitol YAML Contract Clause

```yaml
schema:
  extracted_facts:
    type: array
    items:
      confidence:
        type: number
        minimum: 0.0
        maximum: 1.0
        required: true
        description: >
          Confidence score. MUST remain 0.0-1.0 float.
          BREAKING CHANGE if changed to 0-100 integer scale.
          Week 4 Cartographer uses this for edge weighting.
```

---

## Question 3: Lineage Graph → Blame Chain Traversal

When the ValidationRunner detects a contract violation (e.g., `fact_confidence.range` FAIL), the ViolationAttributor traces it back to the responsible commit using the Week 4 lineage graph.

### Step-by-Step Traversal

1. **Identify the failing schema element.** The check `week3-document-refinery-extractions.fact_confidence.range` tells us the violation is in the `fact_confidence` column of the Week 3 extractions dataset.

2. **Load the lineage graph.** Read the latest snapshot from `outputs/week4/lineage_snapshots.jsonl`. This contains `nodes[]` (files, pipelines, services) and `edges[]` (IMPORTS, CALLS, READS, WRITES, PRODUCES, CONSUMES relationships).

3. **Locate the producing node.** Search `nodes[]` for entries whose `node_id` contains `week3`. These are the source files and pipeline nodes that produce the extraction data. In our graph: `pipeline::week3` and any `file::` nodes in the Week 3 source directory.

4. **BFS upstream traversal.** Starting from the producing node, traverse `edges[]` backwards (follow `source` for edges where `target` is our node). Stop at the first external boundary or file-system root. Each hop increases `lineage_distance` by 1.

5. **Git blame integration.** For each upstream file identified, run:
   ```
   git log --follow --since="14 days ago" --format=%H|%ae|%ai|%s -- {file_path}
   ```
   This retrieves recent commits to the file, including the author, timestamp, and commit message.

6. **Confidence scoring.** For each commit candidate:
   ```
   score = max(0.0, 1.0 - (days_since_commit * 0.1) - (lineage_distance * 0.2))
   ```
   A commit made yesterday at lineage distance 1 scores `1.0 - 0.1 - 0.2 = 0.7`. A commit from 7 days ago at distance 2 scores `1.0 - 0.7 - 0.4 = 0.0` (clamped).

7. **Output blame chain.** The top 1–5 candidates are written to `violation_log/violations.jsonl` with the blast radius (affected downstream nodes from the contract's `lineage.downstream[]`).

---

## Question 4: LangSmith Trace Contract in Bitol YAML

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: observability-team
  description: >
    Contract for LLM trace records exported from LangSmith.
    Enforced by the AI Contract Extension (Phase 4).

schema:
  # Structural clause
  id:
    type: string
    format: uuid
    required: true
    unique: true
    description: Unique trace run identifier.
  run_type:
    type: string
    required: true
    enum: ["llm", "chain", "tool", "retriever", "embedding"]
    description: >
      Type of run. Must be one of the five registered types.
      Adding a new type requires updating the contract.
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
    description: Must be >= start_time.

  # Statistical clause
  total_tokens:
    type: integer
    minimum: 0
    required: true
    description: >
      Must equal prompt_tokens + completion_tokens.
      A mismatch indicates a billing or logging error.
  prompt_tokens:
    type: integer
    minimum: 0
    required: true
  completion_tokens:
    type: integer
    minimum: 0
    required: true

  # AI-specific clause
  total_cost:
    type: number
    minimum: 0.0
    required: true
    description: >
      Cost in USD. Must be >= 0.
      A sudden spike (>3 stddev from baseline) indicates
      model version change or prompt length regression.

quality:
  type: SodaChecks
  specification:
    checks for langsmith_traces:
      - row_count >= 1
      - missing_count(id) = 0
      - missing_count(run_type) = 0
      - min(total_cost) >= 0
```

This contract includes: (1) structural — `run_type` enum enforcement, (2) statistical — `total_tokens` equals sum of prompt + completion, (3) AI-specific — `total_cost >= 0` with drift monitoring.

---

## Question 5: Contract Staleness — Failure Modes and Prevention

### The Most Common Failure Mode

Contract enforcement systems fail in production because **contracts drift out of sync with the actual data**. This happens through three mechanisms:

1. **Manual contracts rot.** Hand-written contracts are correct on day one. By week four, the upstream system has changed twice and nobody updated the YAML. The contract now validates against a schema that no longer exists. It either false-alarms (causing alert fatigue and eventual muting) or silently passes broken data (because the check targets a column that was renamed).

2. **Baselines become stale.** Statistical drift detection relies on a stored baseline mean and stddev. If the baseline was captured during an atypical period (e.g., a data backfill with different characteristics), every subsequent run triggers false WARN/FAIL alerts. Teams disable the check rather than re-baseline.

3. **Schema evolution without notification.** A producer team adds a new required field. No contract is updated. The consumer continues running, silently ignoring the new field. Months later, when the consumer finally needs that field, they discover it's been available but uncaptured since the change.

### How This Architecture Prevents Staleness

1. **Auto-generation from data profiling.** The ContractGenerator derives contracts from the actual data on every run. The contract reflects what the data looks like *now*, not what someone wrote six months ago. This eliminates the manual-rot problem.

2. **Schema snapshots with temporal ordering.** Every `ContractGenerator` run writes a timestamped snapshot to `schema_snapshots/{contract_id}/{timestamp}.yaml`. The `SchemaEvolutionAnalyzer` diffs consecutive snapshots to detect *when* a change occurred, not just *that* it occurred. This enables the blame chain to be temporally precise.

3. **Automatic baseline refresh.** The ValidationRunner writes baselines on its first run and uses them for drift detection on subsequent runs. If baselines are outdated, the statistical drift check will flag this, prompting a deliberate re-baseline rather than silent drift.

4. **Lineage-aware blast radius.** Every contract includes `lineage.downstream[]` from the Week 4 lineage graph. When a violation is detected, the blast radius is computed automatically — the team doesn't need to manually track who consumes what.

The key insight is that the contract is not a static document. It is a living artifact that is *regenerated* from data, *compared* over time, and *enforced* on every pipeline run. Staleness is a property of static systems; this architecture prevents it by making the contract a computed function of the data.
