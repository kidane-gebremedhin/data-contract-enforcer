# Spec 03: Contract Generation Pipeline Enforcement

## Problem

The project document specifies a 5-step Contract Generation Pipeline:
1. Structural profiling (ydata-profiling)
2. Statistical profiling (numeric distributions)
3. Lineage context injection (Week 4 graph)
4. LLM annotation (Claude for ambiguous columns)
5. dbt output (schema.yml with equivalent tests)

Currently, the generator implements Steps 1-3 and Step 5 partially. Missing:
- **Step 3 is incomplete**: lineage injection does not query the registry
- **Step 4 (LLM annotation)**: not implemented at all
- **Step 5 (dbt output)**: generates basic dbt schema but uses only `not_null`, `unique`, and `accepted_values` — does not map all contract clauses (range, pattern, relationships)
- **Registry argument**: generator does not accept `--registry` flag

## Changes Required

### 1. Add `--registry` CLI argument to generator

**File:** `contracts/generator.py`

```python
parser.add_argument("--registry", default="contract_registry/subscriptions.yaml",
                    help="Path to contract registry subscriptions YAML")
```

### 2. Update lineage injection to use registry (Step 3)

**File:** `contracts/generator.py` — `inject_lineage()`

Current behavior: only reads Week 4 lineage graph for downstream consumers.

New behavior:
1. Load lineage graph (existing)
2. Load registry via `contracts.registry.get_subscribers()`
3. Merge: registry subscribers are the **primary** downstream list
4. Lineage graph nodes are **enrichment** (transitive depth)

Updated contract lineage section:

```yaml
lineage:
  upstream: []
  downstream:
    - id: week4-cartographer
      description: Consumes doc_id and extracted_facts as node metadata
      fields_consumed: [doc_id, extracted_facts, extraction_model]
      breaking_if_changed: [extracted_facts.confidence, doc_id]
  registry_subscribers:
    - week4-cartographer
    - week7-enforcer
  note: "Blast radius uses registry_subscribers as primary source. downstream_nodes is enrichment only."
```

### 3. Implement LLM annotation (Step 4) — Optional Enhancement

**File:** `contracts/generator.py` — new function `annotate_with_llm()`

For columns where `description` is auto-generated and ambiguous:
- If `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` is set, call Claude/GPT with:
  - Column name, table name, 5 sample values, adjacent column names
  - Request: plain-English description, business rule, cross-column relationships
- Append as `llm_annotations` in the contract
- If no API key, skip gracefully with a log message

```python
def annotate_with_llm(column_profiles, contract_id):
    """Optional: Use LLM to annotate ambiguous columns."""
    try:
        from anthropic import Anthropic
        client = Anthropic()
    except (ImportError, Exception):
        print("  LLM annotation skipped (no API key or anthropic not installed)")
        return {}
    
    annotations = {}
    for col, profile in column_profiles.items():
        # Only annotate columns with generic descriptions
        if "Auto-generated" in profile.get("description", "") or "Numeric field" in profile.get("description", ""):
            # ... call LLM for annotation
            pass
    return annotations
```

### 4. Improve dbt output (Step 5) — Full Mapping

**File:** `contracts/generator.py` — `generate_dbt_schema()`

Current dbt output only maps: `not_null`, `unique`, `accepted_values`.

Required full mapping:

| Contract Clause | dbt Test |
|-----------------|----------|
| `required: true` | `not_null` |
| `unique: true` | `unique` |
| `enum: [...]` | `accepted_values: {values: [...]}` |
| `format: uuid` | `dbt_expectations.expect_column_values_to_match_regex: {regex: "^[0-9a-f-]{36}$"}` |
| `pattern: "^..."` | `dbt_expectations.expect_column_values_to_match_regex: {regex: "..."}` |
| `minimum/maximum` | `dbt_expectations.expect_column_values_to_be_between: {min_value: ..., max_value: ...}` |
| `format: date-time` | `dbt_expectations.expect_column_values_to_match_regex: {regex: "ISO8601_PATTERN"}` |
| foreign key relationships | `relationships: {to: ref('...'), field: '...'}` |

Updated `generate_dbt_schema()`:

```python
def generate_dbt_schema(column_profiles, contract_id, schema_clauses):
    """Generate dbt-compatible schema.yml with full clause mapping."""
    columns = []
    for col, profile in column_profiles.items():
        clause = schema_clauses.get(col, {})
        tests = []
        
        # Required -> not_null
        if clause.get("required", False):
            tests.append("not_null")
        
        # Unique
        if clause.get("unique", False):
            tests.append("unique")
        
        # Enum -> accepted_values
        if "enum" in clause:
            tests.append({"accepted_values": {"values": clause["enum"]}})
        
        # Pattern -> regex match (dbt_expectations)
        if "pattern" in clause:
            tests.append({
                "dbt_expectations.expect_column_values_to_match_regex": {
                    "regex": clause["pattern"]
                }
            })
        
        # Range -> between (dbt_expectations)
        if "minimum" in clause or "maximum" in clause:
            between = {}
            if "minimum" in clause:
                between["min_value"] = clause["minimum"]
            if "maximum" in clause:
                between["max_value"] = clause["maximum"]
            tests.append({
                "dbt_expectations.expect_column_values_to_be_between": between
            })
        
        # Date-time format
        if clause.get("format") == "date-time":
            tests.append({
                "dbt_expectations.expect_column_values_to_match_regex": {
                    "regex": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"
                }
            })
        
        col_entry = {"name": col, "description": clause.get("description", "")}
        if tests:
            col_entry["tests"] = tests
        columns.append(col_entry)
    
    return {
        "version": 2,
        "models": [{
            "name": contract_id.replace("-", "_"),
            "description": f"dbt schema tests generated from contract {contract_id}",
            "columns": columns
        }]
    }
```

## Files to Modify

| File | Action |
|------|--------|
| `contracts/generator.py` | MODIFY — add `--registry` flag, update lineage injection, improve dbt output, add optional LLM annotation |

## Acceptance Criteria

- [ ] Generator accepts `--registry` flag
- [ ] Lineage section includes `registry_subscribers` from subscriptions.yaml
- [ ] dbt schema.yml maps range checks to `dbt_expectations.expect_column_values_to_be_between`
- [ ] dbt schema.yml maps pattern checks to `dbt_expectations.expect_column_values_to_match_regex`
- [ ] dbt schema.yml includes `description` for each column
- [ ] LLM annotation runs if API key available, skips gracefully otherwise
