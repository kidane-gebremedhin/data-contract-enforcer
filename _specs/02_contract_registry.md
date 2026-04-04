# Spec 02: Contract Registry Implementation

## Problem

The project document requires a `ContractRegistry` component: "Records who subscribes to which contract and which fields they consume." Currently no `contract_registry/subscriptions.yaml` file exists. The ViolationAttributor computes blast radius solely from the contract's `lineage.downstream[]` field, which is lineage-graph-only (not registry-based).

The practitioner manual states: "Create contract_registry/subscriptions.yaml before any other code. The registry is the first deliverable because it forces you to think about consumers before you think about contracts."

## Changes Required

### 1. Create `contract_registry/subscriptions.yaml`

This is the central registry file. Minimum 4 subscriptions covering:
- Week 3 -> Week 4
- Week 4 -> Week 7 (Enforcer)
- Week 5 -> Week 7 (Enforcer)
- LangSmith -> Week 7 (Enforcer)

```yaml
# contract_registry/subscriptions.yaml
# This file is manually maintained.
# Every inter-system data dependency must be listed here.
# A subscription is a consumer's formal declaration of dependency.

subscriptions:
  # Week 3 Document Refinery -> Week 4 Brownfield Cartographer
  - contract_id: week3-document-refinery-extractions
    subscriber_id: week4-cartographer
    subscriber_team: week4
    fields_consumed: [doc_id, extracted_facts, extraction_model]
    breaking_fields:
      - field: extracted_facts.confidence
        reason: >
          Used for edge-weight ranking in lineage graph.
          Scale change (0.0-1.0 vs 0-100) breaks node ordering and
          produces incorrect transitive dependency weights.
      - field: doc_id
        reason: Primary key used as node identity in lineage graph.
    validation_mode: ENFORCE
    registered_at: '2026-04-01T09:00:00Z'
    contact: week4-team@org.com

  # Week 3 Document Refinery -> Week 7 Enforcer (AI extensions)
  - contract_id: week3-document-refinery-extractions
    subscriber_id: week7-enforcer
    subscriber_team: week7
    fields_consumed: [extracted_facts.confidence, extracted_facts.text, doc_id]
    breaking_fields:
      - field: extracted_facts.confidence
        reason: AI extension embedding drift check uses confidence for filtering.
      - field: extracted_facts.text
        reason: Embedding drift baseline computed from fact text values.
    validation_mode: AUDIT
    registered_at: '2026-04-01T09:00:00Z'
    contact: week7-team@org.com

  # Week 4 Cartographer -> Week 7 Enforcer (lineage graph for attribution)
  - contract_id: week4-brownfield-cartographer-lineage
    subscriber_id: week7-enforcer
    subscriber_team: week7
    fields_consumed: [nodes, edges, captured_at]
    breaking_fields:
      - field: edges.source
        reason: BFS traversal for blame chain depends on stable node_id format.
      - field: edges.relationship
        reason: Attribution filters on PRODUCES/WRITES/CONSUMES relationship types.
    validation_mode: ENFORCE
    registered_at: '2026-04-01T09:00:00Z'
    contact: week7-team@org.com

  # Week 5 Event Records -> Week 7 Enforcer
  - contract_id: week5-event-records
    subscriber_id: week7-enforcer
    subscriber_team: week7
    fields_consumed: [event_id, event_type, aggregate_id, sequence_number, occurred_at, recorded_at]
    breaking_fields:
      - field: event_type
        reason: Enum validation depends on registered event types.
      - field: sequence_number
        reason: Monotonicity check depends on integer sequence per aggregate.
    validation_mode: AUDIT
    registered_at: '2026-04-01T09:00:00Z'
    contact: week7-team@org.com

  # LangSmith Traces -> Week 7 Enforcer (AI extensions)
  - contract_id: langsmith-trace-records
    subscriber_id: week7-enforcer
    subscriber_team: week7
    fields_consumed: [id, run_type, total_tokens, prompt_tokens, completion_tokens, total_cost]
    breaking_fields:
      - field: run_type
        reason: AI extension filters traces by run_type enum.
      - field: total_tokens
        reason: Token arithmetic check (total = prompt + completion) depends on field presence.
    validation_mode: AUDIT
    registered_at: '2026-04-01T09:00:00Z'
    contact: week7-team@org.com

  # Week 2 Verdict Records -> Week 7 Enforcer (LLM output schema validation)
  - contract_id: week2-digital-courtroom-verdicts
    subscriber_id: week7-enforcer
    subscriber_team: week7
    fields_consumed: [verdict_id, overall_verdict, confidence]
    breaking_fields:
      - field: overall_verdict
        reason: LLM output schema violation rate check depends on PASS/FAIL/WARN enum.
    validation_mode: AUDIT
    registered_at: '2026-04-01T09:00:00Z'
    contact: week7-team@org.com

  # Week 7 Violation Log -> Week 8 Sentinel (compounding architecture)
  - contract_id: week7-violation-log
    subscriber_id: week8-sentinel
    subscriber_team: week8
    fields_consumed: [violation_id, check_id, severity, detected_at, blast_radius]
    breaking_fields:
      - field: severity
        reason: Alert routing depends on severity enum values.
      - field: blast_radius.source
        reason: Sentinel distinguishes registry vs lineage-only alerts.
    validation_mode: AUDIT
    registered_at: '2026-04-03T00:00:00Z'
    contact: week8-team@org.com

  # Week 7 Schema Snapshots -> Week 8 Sentinel
  - contract_id: week7-schema-snapshots
    subscriber_id: week8-sentinel
    subscriber_team: week8
    fields_consumed: [contract_id, snapshot_timestamp, schema_hash]
    breaking_fields:
      - field: schema_hash
        reason: Used for change detection in alert pipeline.
    validation_mode: AUDIT
    registered_at: '2026-04-03T00:00:00Z'
    contact: week8-team@org.com
```

### 2. Create Registry Loader Utility

**File:** `contracts/registry.py`

A shared module for loading and querying the registry:

```python
"""ContractRegistry: Loads and queries contract_registry/subscriptions.yaml."""

import yaml
from pathlib import Path

DEFAULT_REGISTRY_PATH = "contract_registry/subscriptions.yaml"


def load_registry(registry_path=DEFAULT_REGISTRY_PATH):
    """Load the contract registry YAML."""
    with open(registry_path) as f:
        return yaml.safe_load(f)


def get_subscribers(registry, contract_id):
    """Get all subscribers for a given contract_id."""
    return [s for s in registry.get("subscriptions", [])
            if s["contract_id"] == contract_id]


def get_breaking_field_subscribers(registry, contract_id, failing_field):
    """Get subscribers whose breaking_fields match the failing field.
    
    This is the primary blast radius query (Step 1 of attribution pipeline).
    At Tier 2, this becomes a REST call:
        GET /api/registry/subscriptions?contract_id={id}&breaking_field={field}
    """
    affected = []
    for sub in registry.get("subscriptions", []):
        if sub["contract_id"] != contract_id:
            continue
        for bf in sub.get("breaking_fields", []):
            if bf["field"] == failing_field or failing_field.startswith(bf["field"]):
                affected.append({
                    "subscriber_id": sub["subscriber_id"],
                    "subscriber_team": sub.get("subscriber_team", "unknown"),
                    "contact": sub.get("contact", "unknown"),
                    "validation_mode": sub.get("validation_mode", "AUDIT"),
                    "reason": bf["reason"],
                    "fields_consumed": sub.get("fields_consumed", [])
                })
                break
    return affected
```

### 3. Wire Registry into Generator (lineage context)

**File:** `contracts/generator.py`

Update `inject_lineage()` to also read from the registry:
- Load `contract_registry/subscriptions.yaml`
- Add `registry_subscribers` to the contract's `lineage` section
- Keep lineage graph downstream as enrichment

### 4. Wire Registry into ContractGenerator quality checks

In the `build_contract()` function, add a `registry` section to the contract output:

```yaml
registry:
  subscribers:
    - id: week4-cartographer
      breaking_fields: [extracted_facts.confidence, doc_id]
    - id: week7-enforcer
      breaking_fields: [extracted_facts.confidence, extracted_facts.text]
  note: "Blast radius uses registry_subscribers as primary source."
```

## Files to Create/Modify

| File | Action |
|------|--------|
| `contract_registry/subscriptions.yaml` | CREATE |
| `contracts/registry.py` | CREATE — shared registry loader |
| `contracts/generator.py` | MODIFY — inject registry context into contracts |
| `contracts/attributor.py` | MODIFY — use registry for blast radius (see Spec 05) |

## Acceptance Criteria

- [ ] `contract_registry/subscriptions.yaml` exists with >= 4 subscriptions
- [ ] Each subscription has `breaking_fields` with `reason`
- [ ] `contracts/registry.py` provides `get_breaking_field_subscribers()`
- [ ] Generator injects registry subscriber info into contract lineage section
- [ ] Registry covers Week 3->4, Week 4->7, Week 5->7, LangSmith->7 minimum
