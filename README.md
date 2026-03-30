# Data Contract Enforcer

Schema Integrity & Lineage Attribution System for the TRP1 Platform.

Enforces formal data contracts on inter-system data flows from Weeks 1–5, catches violations (structural and statistical), traces them to upstream commits, and generates a stakeholder-ready Enforcer Report.

## Prerequisites

```bash
pip install -r requirements.txt
```

Required: Python 3.11+, pyyaml, jsonschema, pandas, numpy, scikit-learn, gitpython, jinja2.

## How to Run the Data Contract Enforcer

### Step 0: Prepare input data

If `outputs/` is not populated, run the data preparation script:

```bash
python outputs/migrate/data_prep.py
```

Expected: JSONL files created in `outputs/week1/`, `outputs/week2/`, `outputs/week3/`, `outputs/week4/`, `outputs/week5/`, and `outputs/traces/`.

### Step 1: Generate contracts

```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

python contracts/generator.py \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-records \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

Expected output:
- `generated_contracts/week3_document_refinery_extractions.yaml` (11 clauses)
- `generated_contracts/week3_document_refinery_extractions_dbt.yml`
- `generated_contracts/week5_event_records.yaml` (12 clauses)
- `generated_contracts/week5_event_records_dbt.yml`

### Step 2: Run validation (clean data)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/clean_run.json
```

Expected output: `validation_reports/clean_run.json` — all structural checks PASS.

### Step 3: Inject violation and run again

```bash
python create_violation.py

python contracts/runner.py \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --output validation_reports/violated_run.json
```

Expected output: FAIL for `fact_confidence.range` (confidence values now 70–98 instead of 0.70–0.98) and FAIL for `fact_confidence.statistical_drift` (z-score > 1000).

### Step 4: Attribute the violation

```bash
python contracts/attributor.py \
  --violation validation_reports/violated_run.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --contract generated_contracts/week3_document_refinery_extractions.yaml \
  --output violation_log/violations.jsonl
```

Expected output: `violation_log/violations.jsonl` with blame chain entries including commit hash, author, and blast radius.

### Step 5: Run schema evolution analysis

```bash
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution.json
```

Expected output: Schema diff with breaking change classifications. Requires at least 2 snapshots in `schema_snapshots/week3-document-refinery-extractions/`.

### Step 6: Run AI Contract Extensions

```bash
python contracts/ai_extensions.py \
  --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json
```

Expected output: `validation_reports/ai_extensions.json` with embedding drift score, prompt input validation results, and LLM output schema violation rate.

### Step 7: Generate Enforcer Report

```bash
python contracts/report_generator.py
```

Expected output: `enforcer_report/report_data.json` with `data_health_score` between 0 and 100. The report aggregates all validation results, violations, schema changes, and AI extension metrics.

## Directory Structure

```
data-contract-enforcer/
├── contracts/                    # Source code for all components
│   ├── generator.py              # ContractGenerator
│   ├── runner.py                 # ValidationRunner
│   ├── attributor.py             # ViolationAttributor
│   ├── schema_analyzer.py        # SchemaEvolutionAnalyzer
│   ├── ai_extensions.py          # AI Contract Extensions
│   └── report_generator.py       # Enforcer Report Generator
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

After running all steps, open `enforcer_report/report_data.json` and verify `data_health_score` is between 0 and 100.
