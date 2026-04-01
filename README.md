# Data Contract Enforcer

Schema Integrity & Lineage Attribution System for the TRP1 Platform.

Enforces formal data contracts on inter-system data flows from Weeks 1–5, catches violations (structural and statistical), traces them to upstream commits, and generates a stakeholder-ready Enforcer Report.

## Prerequisites

```bash
pip install -r requirements.txt
```

Required: Python 3.11+, pyyaml, jsonschema, pandas, numpy, scikit-learn, gitpython, jinja2, ydata-profiling, setuptools.

---

## Phase 1 — ContractGenerator

Auto-generates Bitol YAML data contracts from JSONL data through a 5-stage pipeline (`contracts/generator.py`).

### Pipeline Stages

| Stage | What It Does | Key Function |
|-------|-------------|--------------|
| 1. Load & Flatten | Reads JSONL, explodes nested arrays (facts, scores, refs, metadata) into flat rows | `load_jsonl()`, `flatten_for_profile()` |
| 2. Profile Columns | Computes dtype, null fraction, cardinality, sample values, numeric stats (min/max/mean/percentiles/stddev) via ydata-profiling | `run_ydata_profile()`, `profile_column()` |
| 3. Generate Clauses | Translates profiles into Bitol YAML clauses with domain-aware rules (confidence 0–1, scores 1–5, UUID format, enums for cardinality ≤50) | `column_to_clause()`, `build_contract()` |
| 4. Inject Lineage | Parses Week 4 lineage graph, identifies downstream consumers, adds `lineage.downstream[]` | `inject_lineage()` |
| 5. Output Artifacts | Writes Bitol YAML + dbt `schema.yml` + timestamped schema snapshot | `generate_dbt_schema()`, `write_snapshot()` |

### Step 0: Prepare input data

If `outputs/` is not populated, run the data preparation script:

```bash
python outputs/migrate/data_prep.py
```

Expected: JSONL files created in `outputs/week1/`, `outputs/week2/`, `outputs/week3/`, `outputs/week4/`, `outputs/week5/`, and `outputs/traces/`.

### Step 1: Generate Week 3 contract

```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

Verify:
- `generated_contracts/week3_document_refinery_extractions.yaml` exists with 11 clauses
- `fact_confidence` has `minimum: 0.0`, `maximum: 1.0`
- `lineage.downstream[]` shows downstream consumers (week4, week5)
- dbt schema: `generated_contracts/week3_document_refinery_extractions_dbt.yml`
- Schema snapshot saved to `schema_snapshots/week3-document-refinery-extractions/`

### Step 2: Generate Week 5 contract

```bash
python contracts/generator.py \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-records \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

Verify:
- `generated_contracts/week5_event_records.yaml` exists with 12 clauses
- `event_type` has an `enum` clause with all 34 event types
- dbt schema: `generated_contracts/week5_event_records_dbt.yml`
- Schema snapshot saved to `schema_snapshots/week5-event-records/`

### Step 3: Validate generated contracts

Open the YAML files and check:
1. **Structural correctness** — `kind: DataContract`, `apiVersion: v3.0.0`, valid `id`
2. **Domain rules** — confidence capped 0–1, IDs have UUID format/pattern, timestamps have `date-time` format, hashes have `^[a-f0-9]{64}$` pattern
3. **Quality checks** — `row_count >= 1`, `missing_count` for required fields, min/max for confidence
4. **Lineage populated** — `downstream[]` lists consumer nodes with `breaking_if_changed` fields

---

## Phase 2 — ValidationRunner & ViolationAttributor

Validates data against contracts, then traces violations to upstream commits.

### Check Types (ValidationRunner)

The runner (`contracts/runner.py`) executes these checks against each column defined in the contract:

| Check | What It Does | Triggered By |
|-------|-------------|--------------|
| Required | No nulls in column | `required: true` |
| Type | dtype matches contract (number/integer/string/boolean) | `type:` clause |
| Range | Values within min/max bounds | `minimum:`/`maximum:` |
| Enum | Values match allowed set | `enum:` clause |
| Pattern | Regex match (UUID, SHA-256) | `pattern:` clause |
| DateTime | Parseable as ISO 8601 | `format: date-time` |
| Statistical Drift | Mean hasn't drifted >2–3 stddev from baseline | Numeric columns with baselines |

### Step 4: Run validation on clean data

```bash
python contracts/runner.py \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/clean_run.json
```

Verify:
- `validation_reports/clean_run.json` — all checks PASS
- Baselines auto-created at `schema_snapshots/baselines.json` on first run

### Step 5: Inject violation and validate again

```bash
python create_violation.py

python contracts/runner.py \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/violated_run.json
```

`create_violation.py` scales `confidence` from 0.0–1.0 to 0–100.

Verify two FAILs in the report:
1. `fact_confidence.range` — FAIL, CRITICAL (values 70–98 exceed `maximum: 1.0`)
2. `fact_confidence.statistical_drift` — FAIL, HIGH (z-score > 1000 stddev from baseline)

### Attribution Pipeline (ViolationAttributor)

The attributor (`contracts/attributor.py`) traces each FAIL to upstream commits:

| Step | What It Does |
|------|-------------|
| 1. Find upstream files | Walks Week 4 lineage graph to find producer nodes/files |
| 2. Git blame | Runs `git log --follow --since=14 days` on each upstream file |
| 3. Score candidates | Ranks commits: `score = 1.0 - (days * 0.1) - (distance * 0.2)` |
| 4. Blast radius | Reads `lineage.downstream[]` from contract for affected nodes |

### Step 6: Attribute the violation

```bash
python contracts/attributor.py \
  --violation validation_reports/violated_run.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --output violation_log/violations.jsonl
```

Verify in `violation_log/violations.jsonl`:
- Each violation has a `blame_chain[]` with ranked commit suspects (hash, author, confidence score)
- Each violation has a `blast_radius` showing affected downstream nodes
- `severity` matches what the runner detected (CRITICAL for range, HIGH for drift)

---

## Phase 3 — SchemaEvolutionAnalyzer

Diffs timestamped schema snapshots and classifies changes as BREAKING or COMPATIBLE (`contracts/schema_analyzer.py`).

### Change Classification Taxonomy

| Change | Classification | Reason |
|--------|---------------|--------|
| Add nullable column | COMPATIBLE | Consumers can ignore it |
| Add required column | BREAKING | All producers must supply it |
| Remove column | BREAKING | Consumers referencing it will fail |
| Type change | BREAKING | e.g., `number` → `string` |
| Range change (min/max) | BREAKING | e.g., `maximum: 1.0` → `maximum: 100` |
| Enum values removed | BREAKING | Consumers expecting removed values break |
| Enum values added | COMPATIBLE | Additive change |
| Pattern change | BREAKING | Validation rules change |
| Optional → required | BREAKING | Nulls no longer allowed |
| Required → optional | COMPATIBLE | Less restrictive |
| Description only | COMPATIBLE | Documentation update |

### Step 7: Run schema evolution analysis

Requires at least 2 snapshots in `schema_snapshots/{contract-id}/`. Each generator run creates a new timestamped snapshot automatically.

```bash
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution.json
```

Verify in `validation_reports/schema_evolution.json`:
- `snapshots_compared` — the two latest YAML snapshot paths
- `changes[]` — per-field diffs with `classification` (BREAKING/COMPATIBLE) and `description`
- `overall_verdict` — BREAKING if any breaking change exists, else COMPATIBLE
- `blast_radius.affected_consumers` — downstream nodes from contract lineage
- `migration_checklist` — numbered action items for each breaking change
- `rollback_plan` — numbered revert instructions

Optional: Run for Week 5 as well:

```bash
python contracts/schema_analyzer.py \
  --contract-id week5-event-records \
  --output validation_reports/schema_evolution_week5.json
```

---

## Phase 4 — AI Contract Extensions

Extends data contracts to cover AI/LLM-specific risks that traditional schema checks miss. The three extensions form a protective boundary around the LLM pipeline: semantic drift detection on text content, structural validation on LLM inputs, and schema enforcement on LLM outputs (`contracts/ai_extensions.py`).

### Extension Checks

| # | Extension | Guards | Data Source | What It Catches |
|---|-----------|--------|-------------|-----------------|
| 1 | **Embedding Drift** | Text content semantics | Week 3 `extracted_facts[].text` | Detects when the *meaning* of extracted text shifts over time (e.g., model update changes extraction style), even if schema structure stays valid |
| 2 | **Prompt Input Validation** | LLM pipeline **inputs** | Week 3 extraction records | Catches malformed records *before* they reach the LLM — missing `doc_id`, blank `source_path`, etc. Non-conforming records are quarantined to `outputs/quarantine/` |
| 3 | **LLM Output Schema Violation Rate** | LLM pipeline **outputs** | Week 2 verdict records | Catches when the LLM produces *structurally invalid* responses — `overall_verdict` must be one of `PASS`/`FAIL`/`WARN`. Tracks violation rate trend against a baseline |

**Extension 2 vs Extension 3 — Input vs Output contract:**
- Extension 2 validates the data **sent to** the LLM (extraction records). Schema: `{doc_id: required string, source_path: required string, ...}`. Failures mean the upstream data pipeline produced bad input.
- Extension 3 validates the data **returned by** the LLM (verdict records). Check: `overall_verdict in (PASS, FAIL, WARN)`. Failures mean the LLM itself is producing non-conforming structured output (e.g., returning "PASSED" instead of "PASS", or hallucinating new verdict categories).

### Step 8: Run AI contract extensions

```bash
python contracts/ai_extensions.py \
  --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json
```

Verify in `validation_reports/ai_extensions.json`:
- `checks[]` contains all three extension results
- Each check has `status` (PASS/FAIL/WARN/BASELINE_SET)
- Extension 1: `drift_score` and `threshold` (0.15)
- Extension 2: `valid`/`quarantined` counts, `quarantine_rate`, and `sample_errors` for any failures
- Extension 3: `violation_rate`, `trend` (stable/rising), and comparison against `baseline_rate`

---

## Phase 5 — Enforcer Report

Aggregates results from all prior phases into a single stakeholder-ready report with a composite health score (`contracts/report_generator.py`).

### What It Aggregates

| Source | Path | Phase |
|--------|------|-------|
| Validation reports | `validation_reports/*.json` (excl. ai_extensions, schema_evolution) | Phase 2 |
| Violation log | `violation_log/violations.jsonl` | Phase 2 |
| AI extension metrics | `validation_reports/ai_extensions.json` | Phase 4 |
| Schema evolution | `validation_reports/schema_evolution*.json` | Phase 3 |

### Health Score Formula

Starts at 100, deducts per violation severity: CRITICAL -20, HIGH -10, MEDIUM -5, LOW -1. Clamped to 0–100.

### Step 9: Generate Enforcer Report

```bash
python contracts/report_generator.py
```

Verify in `enforcer_report/report_data.json`:
- `data_health_score` between 0 and 100
- `top_violations` — plain-language summaries of the 3 most severe failures
- `ai_system_risk_assessment` — embedding drift, prompt validation, and LLM output conformance status
- `schema_changes_summary` — breaking vs. compatible change counts
- `recommendations` — up to 5 actionable items

---

## Directory Structure

```
data-contract-enforcer/
├── contracts/                    # Source code for all components
│   ├── generator.py              # Phase 1: ContractGenerator
│   ├── runner.py                 # Phase 2: ValidationRunner
│   ├── attributor.py             # Phase 2: ViolationAttributor
│   ├── schema_analyzer.py        # Phase 3: SchemaEvolutionAnalyzer
│   ├── ai_extensions.py          # Phase 4: AI Contract Extensions
│   └── report_generator.py       # Phase 5: Enforcer Report Generator
├── generated_contracts/          # Auto-generated Bitol YAML + dbt schema.yml
├── validation_reports/           # Structured validation report JSON files
├── violation_log/                # Violation records (JSONL)
├── schema_snapshots/             # Timestamped schema snapshots per contract
├── enforcer_report/              # Stakeholder report (report_data.json)
├── outputs/                      # Input data from Weeks 1–5 + LangSmith traces
├── create_violation.py           # Violation injection script
├── DOMAIN_NOTES.md               # Phase 0 domain reconnaissance
└── README.md                     # This file
```

## Data Sources

- **Week 3 (extractions):** Migrated from Document Intelligence Refinery extracted facts
- **Week 5 (events):** Migrated from APEX Ledger event stream
- **Week 4 (lineage):** Migrated from Brownfield Cartographer trace
- **Weeks 1, 2, traces:** Synthetically generated to canonical schemas (reproducible via fixed seed)

After running all steps (Steps 0–9), open `enforcer_report/report_data.json` and verify `data_health_score` is between 0 and 100.
