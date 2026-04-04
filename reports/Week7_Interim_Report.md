# Week 7 — Data Contract Enforcer Report

**Author:** KG | **Date:** 2026-04-02 | **Project:** TRP1 Data Contract Enforcer  
**Health Score:** 70 / 100

---

## 1. Data Flow Diagram

The five TRP1 systems and their inter-system data flows, annotated with schema names:

```
┌─────────────────────┐
│   WEEK 1            │
│   Roo Code          │
│   (AI Dev Tool)     │
│                     │
│ Produces:           │
│  code generations,  │
│  refactoring output │
└─────────┬───────────┘
          │ Codebase artifacts
          ▼
┌─────────────────────┐      Audit Reports (JSON/MD)     ┌─────────────────────┐
│   WEEK 2            │─────────────────────────────────▶│   WEEK 4            │
│   Automaton Auditor │      LangSmith Traces            │   Brownfield        │
│   (Multi-Agent      │      (runs.jsonl)                │   Cartographer      │
│    Audit System)    │────────────────────────────────▶ │   (Lineage &        │
│                     │                                   │    Static Analysis) │
│ Schemas:            │                                   │                     │
│  Evidence,          │                                   │ Schemas:            │
│  JudicialOpinion,   │                                   │  ModuleNode,        │
│  AuditReport        │                                   │  DatasetNode,       │
└─────────────────────┘                                   │  TransformationNode,│
                                                          │  TypedEdge          │
┌─────────────────────┐                                   │                     │
│   WEEK 3            │   doc_id, extracted_facts         │ Produces:           │
│   Document Refinery │──────────────────────────────────▶│  module_graph.json  │
│   (5-Stage          │   (extractions.jsonl)             │  lineage_graph.json │
│    Extraction)      │                                   └──────────┬──────────┘
│                     │                                              │
│ Schemas:            │   doc_id, extracted_facts                    │ lineage_snapshots
│  DocumentProfile,   │──────────────────────┐                      │ (lineage_snapshots
│  LDU,               │                      │                      │  .jsonl)
│  ProvenanceChain,   │                      ▼                      ▼
│  fact_confidence     │            ┌─────────────────────┐
│  (0.0–1.0)          │            │   WEEK 5            │
└──────────────────────┘            │   The Ledger        │
                                    │   (Event-Sourced    │
                                    │    Loan Platform)   │
                                    │                     │
                                    │ Schemas:            │
                                    │  StoredEvent,       │
                                    │  BaseCommand,       │
                                    │  ApplicationSubmitted│
                                    │  CreditAnalysis*,   │
                                    │  FraudScreening*,   │
                                    │  ComplianceRule*,   │
                                    │  DecisionGenerated  │
                                    └─────────────────────┘
```

**Key Data Flows:**

| From | To | Schema / Data | Format |
|------|----|---------------|--------|
| Week 3 (Refinery) | Week 4 (Cartographer) | `doc_id`, `extracted_facts` (confidence, text, page_ref) | JSONL |
| Week 3 (Refinery) | Week 5 (Ledger) | `doc_id`, `extracted_facts` (confidence, text) | JSONL |
| Week 4 (Cartographer) | Week 5 (Ledger) | `lineage_snapshots` (nodes, edges, relationships) | JSONL |
| Week 2 (Auditor) | Week 4 (Cartographer) | Repository artifacts + LangSmith traces | JSON/JSONL |
| Week 1 (Roo Code) | Week 2 (Auditor) | Codebase artifacts for audit | Files |

---

## 2. Contract Coverage Table

| # | Interface | From → To | Contract Written? | Notes |
|---|-----------|-----------|:-----------------:|-------|
| 1 | Document extractions | Week 3 → Week 4 | **Yes** | `week3_document_refinery_extractions.yaml` — 11 clauses, pattern/range/enum checks, lineage to week4 downstream. dbt counterpart (`_dbt.yml`) maps all clauses including range checks via `dbt_expectations.expect_column_values_to_be_between` and pattern checks via `dbt_expectations.expect_column_values_to_match_regex`. |
| 2 | Document extractions | Week 3 → Week 5 | **Yes** | Same contract covers this flow; lineage declares week5 as downstream consumer |
| 3 | Event records | Week 5 (internal) | **Yes** | `week5_event_records.yaml` — 12 clauses, 34 event types enumerated, UUID patterns, sequence validation |
| 4 | Lineage snapshots | Week 4 → Week 5 | **Partial** | Week 4 lineage graph consumed by the enforcer for blame-chain attribution, but no standalone contract YAML for the lineage schema itself |
| 5 | Audit reports | Week 2 → Week 4 | **No** | Week 2 audit outputs (Evidence, JudicialOpinion, AuditReport) not yet formalized — Week 2 data is consumed only for LLM output validation in AI extensions |
| 6 | LangSmith traces | Week 2 → Enforcer | **Partial** | Trace records used for AI extension checks (output schema violation rate), but no formal Bitol contract — validated via `overall_verdict` enum check only |
| 7 | Codebase artifacts | Week 1 → Week 2 | **No** | Week 1 (Roo Code) is an IDE extension; its output is unstructured code, not amenable to a data contract |
| 8 | Intent records | Week 1 | **No** | `intent_records.jsonl` present but no contract generated — low priority since no downstream system consumes it in the current pipeline |
| 9 | Verdict records | Week 2 | **Partial** | Used by AI extensions for LLM output validation but no full Bitol contract — only `overall_verdict` field is schema-checked |

**Summary:** 2 full contracts, 3 partial, 3 not covered. The uncovered interfaces are either unstructured (Week 1 code output) or not yet critical to downstream consumers.

---

## 3. First Validation Run Results

### 3.1 Clean Run (Original Week 3 Data)

| Metric | Value |
|--------|-------|
| Contract validated | `week3-document-refinery-extractions` |
| Total checks | **28** |
| Passed | **28** |
| Failed | **0** |
| Warned | **0** |

All 7 check types passed on the original 55-record extraction dataset:
- **Required field checks** (11 fields): all non-null
- **Type checks**: all dtypes match contract (string, integer, float)
- **Pattern checks**: UUID and SHA-256 patterns valid
- **DateTime check**: ISO 8601 timestamps parseable
- **Range check**: `fact_confidence` in [0.70, 0.98], within contract bounds [0.0, 1.0]
- **Enum check**: `extraction_model` values match allowed set
- **Statistical drift**: all numeric columns within 3 sigma of baseline

### 3.2 Violated Run (Injected `fact_confidence` Scale Change)

A deliberate violation was injected: `fact_confidence` was multiplied by 100 (changing from 0.0–1.0 scale to 0–100 scale), simulating an upstream breaking change.

| Metric | Value |
|--------|-------|
| Contract validated | `week3-document-refinery-extractions` |
| Total checks | **31** |
| Passed | **29** |
| Failed | **2** |
| Warned | **0** |

**Violation 1 — Range Check (CRITICAL):**
- **Field:** `fact_confidence`
- **Expected:** min >= 0.0, max <= 1.0
- **Actual:** min = 70.0, max = 98.0
- **Records affected:** 274 (100% of violated dataset)
- **Blast radius:** `pipeline::week4`, `pipeline::week5`

**Violation 2 — Statistical Drift (HIGH):**
- **Field:** `fact_confidence`
- **Expected mean:** 0.843 +/- 0.082
- **Actual mean:** 84.303 (z-score = **1,023.7 sigma**)
- **Root cause:** same scale change; mean shifted by factor of 100

### 3.3 AI Extension Results

| Extension | Status | Detail |
|-----------|--------|--------|
| Embedding Drift Detection | BASELINE_SET | Drift score = 0.0 (first run) |
| Prompt Input Validation | PASS | 55/55 records valid, 0 quarantined |
| LLM Output Schema Violation Rate | PASS | 0% violation rate, trend stable |

### 3.4 Enforcer Report Summary

- **Overall health score:** 70 / 100 (deducted 20 for CRITICAL, 10 for HIGH)
- **Top recommendation:** "Update source pipeline to output `fact_confidence` as float 0.0–1.0"
- **Total checks across all runs:** 59 executed, 57 passed, 2 failed
- **Schema evolution:** No diff available yet (requires 2+ snapshots)

### 3.5 dbt Schema Mapping Coverage

| Bitol Clause | dbt Test | Coverage |
|-------------|----------|----------|
| `required: true` | `not_null` | Full |
| `unique: true` | `unique` | Full |
| `enum: [...]` | `accepted_values` | Full |
| `pattern: "^..."` | `dbt_expectations.expect_column_values_to_match_regex` | Full |
| `minimum/maximum` | `dbt_expectations.expect_column_values_to_be_between` | Full |
| `format: date-time` | `dbt_expectations.expect_column_values_to_match_regex` | Full |
| `format: uuid` | `dbt_expectations.expect_column_values_to_match_regex` | Full |
| Statistical drift | Custom macro (not native dbt) | Partial |

**Note:** The `dbt_expectations` package (calogica/dbt-expectations) is required for pattern and range tests. Install via `packages.yml`:
```yaml
packages:
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
```

---

## 4. Reflection

Writing formal data contracts for my own Week 1–5 systems was a revealing exercise. Several assumptions I had carried from development turned out to be wrong or incomplete.

**The confidence scale was an accident waiting to happen.** Week 3's extraction pipeline outputs `fact_confidence` as a 0.0–1.0 float, but nothing in the original code enforced that upper bound. When I injected a simulated upstream change (scaling to 0–100), the contract caught it instantly — a range violation flagged as CRITICAL, plus a statistical drift of over 1,000 standard deviations. Before writing the contract, I had assumed this field was "obviously" bounded. The contract proved that "obvious" is not the same as "enforced."

**Lineage was more tangled than I thought.** I assumed Week 3 fed into Week 4 cleanly and that was the end of it. The lineage graph from Week 4 revealed that Week 3's `extracted_facts` also flow into Week 5's event store through the credit analysis pipeline. A breaking change in `fact_confidence` would silently corrupt two downstream systems, not one. The blast-radius computation made this concrete: 274 records across two pipelines.

**Week 5's event schema was surprisingly well-structured.** The event-sourced architecture naturally enforced a contract-like discipline — immutable events, versioned payloads, typed enums. The contract generator found 34 distinct event types and 3 aggregate types, all cleanly enumerable. This was the easiest contract to write, which confirmed that event sourcing's upfront strictness pays dividends downstream.

**Week 2's outputs were the hardest to contract.** The multi-agent audit system produces rich but variable structures (evidence chains, judicial opinions, audit reports). The LLM output schema violation rate extension was the best I could do — checking that `overall_verdict` conforms to a small enum. Full contract coverage for LLM-generated content remains an open challenge.

**The most valuable discovery:** contracts are not just validation tools — they are documentation. The generated YAML files now serve as the single source of truth for what each system actually produces, replacing scattered comments and tribal knowledge.

---

*Report generated 2026-04-02 | Data Contract Enforcer v1.0 | Health Score: 70/100*
