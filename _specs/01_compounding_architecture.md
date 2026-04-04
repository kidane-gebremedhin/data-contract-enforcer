# Spec 01: Compounding Architecture for Violation Log and Schema Snapshots

## Problem

The project document states: "The Data Contract Enforcer's violation log and schema snapshots become first-class inputs for subsequent weeks. The Week 8 Sentinel consumes contract violation events as data quality signals alongside LLM trace quality signals."

Currently, `violation_log/violations.jsonl` and `schema_snapshots/` are produced but not structured as first-class ingestible outputs. They lack:
- A formal schema contract of their own
- Stable, documented field names that Week 8 can rely on
- A unified violation event format suitable for alert pipelines

## Changes Required

### 1. Violation Log Schema Contract

Create `generated_contracts/violation_log.yaml` — a Bitol contract for the violation log itself.

```yaml
kind: DataContract
apiVersion: v3.0.0
id: week7-violation-log
info:
  title: Week 7 Data Contract Enforcer — Violation Log
  version: 1.0.0
  owner: week7-enforcer-team
  description: >
    One record per detected contract violation. Each record includes
    a blame chain, blast radius, and severity classification.
    Designed for ingestion by Week 8 Sentinel alert pipeline.
schema:
  violation_id:
    type: string
    format: uuid
    required: true
    unique: true
    description: Unique identifier for this violation event.
  check_id:
    type: string
    required: true
    description: >
      Dot-separated contract check identifier.
      Format: {contract_id}.{field_name}.{check_type}
  detected_at:
    type: string
    format: date-time
    required: true
    description: ISO 8601 timestamp when violation was detected.
  severity:
    type: string
    enum: [CRITICAL, HIGH, MEDIUM, LOW]
    required: true
  message:
    type: string
    required: true
    description: Human-readable violation description.
  actual_value:
    type: string
    required: true
  expected:
    type: string
    required: true
  blame_chain:
    type: array
    required: true
    description: Ranked list of suspect commits (max 5).
    items:
      rank:
        type: integer
        minimum: 1
        maximum: 5
      commit_hash:
        type: string
        pattern: "^[0-9a-f]{40}$"
      author:
        type: string
      commit_timestamp:
        type: string
      commit_message:
        type: string
      confidence_score:
        type: number
        minimum: 0.0
        maximum: 1.0
  blast_radius:
    type: object
    required: true
    description: Registry-sourced blast radius with lineage enrichment.
    properties:
      source:
        type: string
        enum: [registry, lineage, both]
      direct_subscribers:
        type: array
      transitive_nodes:
        type: array
      contamination_depth:
        type: integer
        minimum: 0
      affected_pipelines:
        type: array
      estimated_records:
        type: integer
        minimum: 0
  records_failing:
    type: integer
    minimum: 0
    required: true
```

### 2. Schema Snapshots Contract

Create `generated_contracts/schema_snapshots.yaml` — a Bitol contract for snapshot metadata.

```yaml
kind: DataContract
apiVersion: v3.0.0
id: week7-schema-snapshots
info:
  title: Week 7 Data Contract Enforcer — Schema Snapshots
  version: 1.0.0
  owner: week7-enforcer-team
  description: >
    Timestamped schema snapshots enabling evolution tracking.
    Each snapshot is a full Bitol contract YAML captured at generation time.
schema:
  contract_id:
    type: string
    required: true
    description: The contract this snapshot belongs to.
  snapshot_timestamp:
    type: string
    format: date-time
    required: true
  snapshot_path:
    type: string
    required: true
    description: Relative path to the YAML snapshot file.
  schema_hash:
    type: string
    pattern: "^[a-f0-9]{64}$"
    required: true
    description: SHA-256 of the schema section for quick diff detection.
```

### 3. Update violation_log output format

**File:** `contracts/attributor.py`

Update the `write_violation` / violation output to include:
- `blast_radius.source` field (always "registry" when registry is used, "lineage" otherwise, "both" when enriched)
- `blast_radius.direct_subscribers` (from registry)
- `blast_radius.transitive_nodes` (from lineage enrichment)
- `blast_radius.contamination_depth` (integer)
- `records_failing` at top level

### 4. Schema snapshots index file

**File:** `schema_snapshots/index.jsonl`

After each generator run, append a line to `schema_snapshots/index.jsonl`:
```json
{
  "contract_id": "week3-document-refinery-extractions",
  "snapshot_timestamp": "2026-04-01T21:55:26Z",
  "snapshot_path": "schema_snapshots/week3-document-refinery-extractions/20260401_215526.yaml",
  "schema_hash": "abc123..."
}
```

This index is what Week 8 Sentinel reads to detect schema evolution events.

### 5. Update `contracts/generator.py`

In `write_snapshot()`, after copying the snapshot file:
1. Compute SHA-256 of the `schema` section of the contract
2. Append an entry to `schema_snapshots/index.jsonl`

### 6. Register both outputs in `contract_registry/subscriptions.yaml`

Add subscriptions for Week 8 consuming the violation log and schema snapshots:

```yaml
- contract_id: week7-violation-log
  subscriber_id: week8-sentinel
  fields_consumed: [violation_id, check_id, severity, detected_at, blast_radius]
  breaking_fields:
    - field: severity
      reason: alert routing depends on severity enum values
    - field: blast_radius.source
      reason: sentinel distinguishes registry vs lineage-only alerts
  validation_mode: AUDIT
  registered_at: '2026-04-03T00:00:00Z'
  contact: week8-team@org.com

- contract_id: week7-schema-snapshots
  subscriber_id: week8-sentinel
  fields_consumed: [contract_id, snapshot_timestamp, schema_hash]
  breaking_fields:
    - field: schema_hash
      reason: used for change detection in alert pipeline
  validation_mode: AUDIT
  registered_at: '2026-04-03T00:00:00Z'
  contact: week8-team@org.com
```

## Files to Create/Modify

| File | Action |
|------|--------|
| `generated_contracts/violation_log.yaml` | CREATE |
| `generated_contracts/schema_snapshots.yaml` | CREATE |
| `contracts/attributor.py` | MODIFY — update violation output schema |
| `contracts/generator.py` | MODIFY — write index entry in `write_snapshot()` |
| `contract_registry/subscriptions.yaml` | MODIFY — add Week 8 subscriptions |

## Acceptance Criteria

- [ ] `violation_log/violations.jsonl` records match the violation log contract schema
- [ ] Each generator run appends to `schema_snapshots/index.jsonl`
- [ ] Both outputs have Bitol contracts in `generated_contracts/`
- [ ] Both outputs are registered in `contract_registry/subscriptions.yaml`
- [ ] Week 8 Sentinel can ingest violation records without modification
