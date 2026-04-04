# Spec 08: Fix dbt Schema Gap and Regenerate Interim Report PDF

## Problem

The interim report notes: "the only notable gap is that dbt-style counterparts are expressed via SodaChecks rather than native dbt schemas with full mapping coverage."

Currently:
1. The generated dbt schema files (`*_dbt.yml`) only use basic tests: `not_null`, `unique`, `accepted_values`
2. Range checks (e.g., confidence 0.0-1.0) are NOT mapped to dbt tests
3. Pattern checks (UUID, SHA-256) are NOT mapped to dbt tests
4. The quality section in Bitol contracts uses `SodaChecks` format, not native dbt
5. The interim report PDF needs regeneration to reflect these fixes

## Changes Required

### 1. Full dbt Schema Generation with dbt_expectations

**File:** `contracts/generator.py` — `generate_dbt_schema()`

Replace the current minimal dbt generation with full clause mapping:

```python
def generate_dbt_schema(column_profiles, contract_id, schema_clauses):
    """Generate dbt-compatible schema.yml with full contract clause mapping.
    
    Maps all Bitol contract clauses to native dbt tests and dbt_expectations tests:
    - required -> not_null
    - unique -> unique
    - enum -> accepted_values
    - pattern -> dbt_expectations.expect_column_values_to_match_regex
    - minimum/maximum -> dbt_expectations.expect_column_values_to_be_between
    - format: date-time -> dbt_expectations.expect_column_values_to_match_regex (ISO 8601)
    - format: uuid -> dbt_expectations.expect_column_values_to_match_regex (UUID v4)
    """
    columns = []
    for col, profile in column_profiles.items():
        clause = schema_clauses.get(col, {})
        tests = []
        
        # Required -> not_null
        if clause.get("required", False):
            tests.append("not_null")
        
        # Unique
        if clause.get("unique", False) or col.endswith("_id") or col in ("id", "doc_id"):
            tests.append("unique")
        
        # Enum -> accepted_values
        if "enum" in clause:
            tests.append({
                "accepted_values": {
                    "values": clause["enum"]
                }
            })
        
        # Pattern -> regex match
        if "pattern" in clause:
            tests.append({
                "dbt_expectations.expect_column_values_to_match_regex": {
                    "regex": clause["pattern"]
                }
            })
        
        # Range -> between
        if "minimum" in clause or "maximum" in clause:
            between_config = {}
            if "minimum" in clause:
                between_config["min_value"] = clause["minimum"]
            if "maximum" in clause:
                between_config["max_value"] = clause["maximum"]
            tests.append({
                "dbt_expectations.expect_column_values_to_be_between": between_config
            })
        
        # Date-time format
        if clause.get("format") == "date-time":
            tests.append({
                "dbt_expectations.expect_column_values_to_match_regex": {
                    "regex": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"
                }
            })
        
        # UUID format (if not already covered by pattern)
        if clause.get("format") == "uuid" and "pattern" not in clause:
            tests.append({
                "dbt_expectations.expect_column_values_to_match_regex": {
                    "regex": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
                }
            })
        
        col_entry = {
            "name": col,
            "description": clause.get("description", f"Auto-generated from contract {contract_id}")
        }
        if tests:
            col_entry["tests"] = tests
        columns.append(col_entry)
    
    return {
        "version": 2,
        "models": [{
            "name": contract_id.replace("-", "_"),
            "description": (
                f"dbt schema tests auto-generated from Bitol contract {contract_id}. "
                f"Requires dbt_expectations package for pattern and range tests."
            ),
            "columns": columns
        }]
    }
```

### 2. Update generator.py to pass schema_clauses to dbt generator

Currently `generate_dbt_schema()` only receives `column_profiles`. Update the call:

```python
# In main(), after building contract:
schema_clauses = contract.get("schema", {})
dbt_schema = generate_dbt_schema(column_profiles, args.contract_id, schema_clauses)
```

### 3. Expected dbt Output for Week 3

After the fix, `generated_contracts/week3_document_refinery_extractions_dbt.yml` should contain:

```yaml
version: 2
models:
  - name: week3_document_refinery_extractions
    description: >
      dbt schema tests auto-generated from Bitol contract
      week3-document-refinery-extractions. Requires dbt_expectations
      package for pattern and range tests.
    columns:
      - name: doc_id
        description: Primary key. UUIDv4.
        tests:
          - not_null
          - unique
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[0-9a-f-]{36}$"
      - name: source_path
        description: Source file path.
        tests:
          - not_null
      - name: source_hash
        description: SHA-256 hash.
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[a-f0-9]{64}$"
      - name: extraction_model
        description: Model identifier.
        tests:
          - not_null
          - accepted_values:
              values: [claude-3-5-sonnet-20241022, claude-3-haiku-20240307]
      - name: processing_time_ms
        description: "Numeric field. Observed range [838, 2750]."
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 838
              max_value: 2750
      - name: extracted_at
        description: Extraction timestamp.
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"
      - name: fact_fact_id
        description: Fact UUID.
        tests:
          - not_null
          - unique
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[0-9a-f-]{36}$"
      - name: fact_text
        description: Extracted fact text.
        tests:
          - not_null
      - name: fact_confidence
        description: "Confidence score. Must remain 0.0-1.0 float. BREAKING if changed to 0-100."
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0.0
              max_value: 1.0
      - name: fact_page_ref
        description: Page reference.
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 3
              max_value: 19
      - name: fact_source_excerpt
        description: Source text excerpt.
        tests:
          - not_null
```

### 4. Update Interim Report Content

**File:** `reports/Week7_Interim_Report.md`

Add/update the following sections:

**In Section 2 (Contract Coverage Table):**
Add a note that dbt counterparts now use `dbt_expectations` package for full clause mapping.

**In Section 3 (Validation Results):**
Add a subsection showing the dbt mapping coverage:

```markdown
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
```

**Update Section 2 notes for contract #1:**
Change from:
> `week3_document_refinery_extractions.yaml` — 11 clauses, pattern/range/enum checks, lineage to week4 downstream

To:
> `week3_document_refinery_extractions.yaml` — 11 clauses, pattern/range/enum checks, lineage to week4 downstream. dbt counterpart (`_dbt.yml`) maps all clauses including range checks via `dbt_expectations.expect_column_values_to_be_between` and pattern checks via `dbt_expectations.expect_column_values_to_match_regex`.

### 5. Regenerate the PDF

**File:** `reports/Week7_Interim_Report.pdf`

After updating the Markdown, regenerate the PDF:

```bash
# Option 1: Use existing generate_pdf.py approach
cd /home/kg/Projects/10Academy/data-contract-enforcer
python -c "
import markdown
from weasyprint import HTML
from pathlib import Path

md_text = Path('reports/Week7_Interim_Report.md').read_text()
html_body = markdown.markdown(md_text, extensions=['tables', 'fenced_code'])

CSS = '''
@page { size: A4; margin: 2cm; }
body { font-family: DejaVu Sans, Liberation Sans, Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { font-size: 22pt; border-bottom: 2px solid #2c3e50; padding-bottom: 8px; color: #2c3e50; }
h2 { font-size: 16pt; color: #2c3e50; margin-top: 24px; border-bottom: 1px solid #bdc3c7; padding-bottom: 4px; }
h3 { font-size: 13pt; color: #34495e; margin-top: 16px; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 10pt; }
th { background-color: #2c3e50; color: white; padding: 8px 10px; text-align: left; }
td { padding: 6px 10px; border-bottom: 1px solid #ddd; }
tr:nth-child(even) { background-color: #f8f9fa; }
code { background-color: #f4f4f4; padding: 2px 4px; border-radius: 3px; font-size: 10pt; }
pre { background-color: #f4f4f4; padding: 12px; border-radius: 4px; overflow-x: auto; font-size: 9pt; line-height: 1.4; }
strong { color: #2c3e50; }
'''

full_html = f'<!DOCTYPE html><html><head><meta charset=\"utf-8\"><style>{CSS}</style></head><body>{html_body}</body></html>'
HTML(string=full_html).write_pdf('reports/Week7_Interim_Report.pdf')
print('PDF regenerated.')
"
```

## Files to Modify

| File | Action |
|------|--------|
| `contracts/generator.py` | MODIFY — full dbt clause mapping with `dbt_expectations` |
| `generated_contracts/week3_document_refinery_extractions_dbt.yml` | REGENERATE |
| `generated_contracts/week5_event_records_dbt.yml` | REGENERATE |
| `reports/Week7_Interim_Report.md` | MODIFY — add dbt mapping coverage section, update contract coverage notes |
| `reports/Week7_Interim_Report.pdf` | REGENERATE from updated Markdown |

## Acceptance Criteria

- [ ] `_dbt.yml` files include `dbt_expectations.expect_column_values_to_be_between` for range checks
- [ ] `_dbt.yml` files include `dbt_expectations.expect_column_values_to_match_regex` for pattern/UUID/datetime checks
- [ ] Each dbt column has a `description` field
- [ ] Interim report Markdown updated with dbt mapping coverage table
- [ ] Interim report PDF regenerated and reflects the fixes
- [ ] No SodaChecks-only quality definitions remain as the sole dbt representation
