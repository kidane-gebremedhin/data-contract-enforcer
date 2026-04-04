# Data Contract Enforcer - Implementation Plan

## Context

The Data Contract Enforcer (Week 7, 10Academy TRP1) enforces schema integrity and lineage attribution across a multi-system data pipeline. Phases 1-5 are partially implemented with 8 detailed specs ready for execution. This plan sequences the remaining work respecting inter-spec dependencies.

**Current state:** Generator, Runner, AI Extensions, and Report Generator are functionally complete. Attributor and Schema Analyzer need major upgrades. Registry does not exist yet.

---

## Dependency Graph

```
Spec 02 (Registry) ─────┬──> Spec 03 (Generator Pipeline)
                         ├──> Spec 05 (Attributor Refactor)
                         ├──> Spec 01 (Compounding Architecture)
                         ├──> Spec 06 (Snapshot Discipline)
                         └──> Spec 07 (Migration Impact)

Spec 03 + 06 ──> Spec 08 (dbt + Report Regen)
Spec 02 + 03 + 05 ──> Spec 04 (Project Improvements)
All specs ──> Spec 08 (Final Artifacts)
```

---

## Phase 1: Contract Registry Foundation (Spec 02)

> Every other spec imports from the registry. Build it first.

### Task 1.1: Create `contract_registry/subscriptions.yaml`

8 subscriptions covering all producer-consumer relationships:

| Producer | Consumer | Mode |
|----------|----------|------|
| week3-document-refinery-extractions | week4-cartographer | ENFORCE |
| week3-document-refinery-extractions | week7-enforcer | AUDIT |
| week4-brownfield-cartographer-lineage | week7-enforcer | ENFORCE |
| week5-event-records | week7-enforcer | AUDIT |
| langsmith-trace-records | week7-enforcer | AUDIT |
| week2-digital-courtroom-verdicts | week7-enforcer | AUDIT |
| week7-violation-log | week8-sentinel | AUDIT |
| week7-schema-snapshots | week8-sentinel | AUDIT |

Each subscription includes: `contract_id`, `subscriber_id`, `subscriber_team`, `fields_consumed`, `breaking_fields` (with `field` + `reason`), `validation_mode`, `registered_at`, `contact`.

### Task 1.2: Create `contracts/registry.py`

Three functions:
- `load_registry(registry_path=DEFAULT_REGISTRY_PATH)` -- load YAML, return dict
- `get_subscribers(registry, contract_id)` -- filter by contract_id
- `get_breaking_field_subscribers(registry, contract_id, failing_field)` -- match breaking_fields with prefix matching for dot-notation

### Files
| File | Action |
|------|--------|
| `contract_registry/subscriptions.yaml` | CREATE |
| `contracts/registry.py` | CREATE |

### Verification
```bash
python -c "from contracts.registry import load_registry, get_subscribers; r = load_registry(); print(len(r['subscriptions']), 'subscriptions')"
# Expected: 8 subscriptions
```

---

## Phase 2: Generator Pipeline Upgrades (Specs 03, 06, 08)

> All three specs modify generator.py -- consolidate to avoid conflicts.

### Task 2.1: Add `--registry` CLI flag (Spec 03)
Add argparse argument pointing to `contract_registry/subscriptions.yaml`.

### Task 2.2: Registry-aware lineage injection (Spec 03)
Update `inject_lineage()` to call `get_subscribers()` and add `registry_subscribers` list to contract's `lineage` section.

### Task 2.3: Full dbt test mapping (Specs 03 + 08)
Replace basic dbt schema generation with complete mapping:

| Bitol Clause | dbt Test |
|-------------|----------|
| `required: true` | `not_null` |
| `unique: true` | `unique` |
| `enum: [...]` | `accepted_values` |
| `pattern: "^..."` | `dbt_expectations.expect_column_values_to_match_regex` |
| `minimum/maximum` | `dbt_expectations.expect_column_values_to_be_between` |
| `format: date-time` | `dbt_expectations.expect_column_values_to_match_regex` (ISO 8601) |
| `format: uuid` | `dbt_expectations.expect_column_values_to_match_regex` (UUID v4) |

Every dbt column gets a `description` field from the contract.

### Task 2.4: Schema snapshot indexing with SHA-256 (Spec 06)
After writing each snapshot:
1. Compute `sha256(yaml.dump(schema_section, sort_keys=True))`
2. Append entry to `schema_snapshots/index.jsonl` with `contract_id`, `snapshot_timestamp`, `snapshot_path`, `schema_hash`

### Task 2.5: Optional LLM annotation stub (Spec 03)
Add `annotate_with_llm()` that gracefully skips when no API key is set.

### Task 2.6: Regenerate all contracts and snapshots
Run generator twice per contract (clean + violated data) to produce 2+ snapshots each.

### Files
| File | Action |
|------|--------|
| `contracts/generator.py` | MODIFY |
| `generated_contracts/*.yaml` | REGENERATE |
| `generated_contracts/*_dbt.yml` | REGENERATE |
| `schema_snapshots/index.jsonl` | CREATE (auto) |

### Verification
- dbt schemas contain `dbt_expectations` tests for range/pattern fields
- `schema_snapshots/index.jsonl` has entries with 64-char hex `schema_hash`
- 2+ snapshots per contract
- Contracts contain `lineage.registry_subscribers`

---

## Phase 3: Attributor Registry-First Refactor (Spec 05)

> Major refactor from lineage-only to 4-step pipeline.

### Task 3.1: Add `--registry` and `--injected` CLI flags

### Task 3.2: Implement `map_column_to_registry_field()`
Maps flattened column names to dot-notation (e.g., `fact_confidence` -> `extracted_facts.confidence`).

### Task 3.3: Implement 4-step pipeline
1. **Registry query** -- `get_breaking_field_subscribers()` for primary blast radius
2. **Lineage BFS** -- traverse lineage graph for transitive depth enrichment
3. **Git blame** -- existing logic with minor updates (max 5 entries)
4. **Write violation** -- full schema with `blast_radius.source`, `direct_subscribers`, `transitive_nodes`, `contamination_depth`

### Task 3.4: Support `--injected` flag
When set, add `injection_note: true`, `injection_type`, `injection_description` to violation records.

### Task 3.5: Regenerate violation log
Re-run attributor for both clean and violated data.

### Files
| File | Action |
|------|--------|
| `contracts/attributor.py` | MAJOR REFACTOR |
| `violation_log/violations.jsonl` | REGENERATE |

### Verification
- Violations have `blast_radius.source` = "registry"
- `blast_radius.direct_subscribers` populated from registry
- `contamination_depth` is integer >= 0
- At least 1 violation has `injection_note: true`

---

## Phase 4: Compounding Architecture Contracts (Spec 01)

> Self-referential contracts for the enforcer's own outputs.

### Task 4.1: Create `generated_contracts/violation_log.yaml`
Bitol YAML contract defining the violation log schema (violation_id, check_id, detected_at, severity, message, blast_radius object, blame_chain array, etc.).

### Task 4.2: Create `generated_contracts/schema_snapshots.yaml`
Bitol YAML contract for snapshot metadata (contract_id, snapshot_timestamp, snapshot_path, schema_hash).

### Files
| File | Action |
|------|--------|
| `generated_contracts/violation_log.yaml` | CREATE |
| `generated_contracts/schema_snapshots.yaml` | CREATE |

### Verification
- Both files are valid Bitol YAML with `kind: DataContract`
- Violation log records conform to `violation_log.yaml` schema
- `schema_snapshots/index.jsonl` entries conform to `schema_snapshots.yaml` schema

---

## Phase 5: Runner + Schema Analyzer Upgrades (Specs 04, 06, 07)

### Task 5.1: Add `--mode` flag to runner.py (Spec 04)
- Choices: AUDIT, WARN, ENFORCE
- `determine_pipeline_action(results, mode)` returns PASS/BLOCK based on severity thresholds
- Add `mode` and `pipeline_action` to output JSON

### Task 5.2: Schema analyzer enhancements (Specs 06 + 07)
- Add `--since` and `--registry` flags
- Quick diff via schema hash comparison before full field diff
- Registry-based blast radius in migration impact
- Per-consumer failure mode analysis (intersect `breaking_fields` with detected changes)
- Human-readable diffs (plain-English descriptions)
- Separate `migration_impact_{contract_id}_{timestamp}.json` when breaking changes detected
- Migration checklist with timing tags ([IMMEDIATE], [BEFORE DEPLOY], [AFTER DEPLOY])
- Rollback plan with file paths
- Deprecation timeline template (announce -> alias -> migration -> cutover -> cleanup)

### Task 5.3: File-specific recommendations in report (Spec 04)
Update `report_generator.py` to produce recommendations referencing actual contract clauses, file paths, and tools.

### Task 5.4: Re-run pipeline
Regenerate all validation reports, schema evolution analysis, and enforcer report.

### Files
| File | Action |
|------|--------|
| `contracts/runner.py` | MODIFY |
| `contracts/schema_analyzer.py` | MODIFY |
| `contracts/report_generator.py` | MODIFY |
| `validation_reports/*.json` | REGENERATE |
| `enforcer_report/report_data.json` | REGENERATE |

### Verification
- `--mode ENFORCE` on violated data produces `pipeline_action: BLOCK`
- Migration impact file exists with per-consumer failure modes
- Enforcer report recommendations reference specific file paths

---

## Phase 6: Report Regeneration and Final Polish (Spec 08)

### Task 6.1: Update interim report
Add Section 3.5 "dbt Schema Mapping Coverage" with the mapping table. Reference `dbt_expectations` package.

### Task 6.2: Regenerate PDF
Run PDF generation with updated markdown.

### Task 6.3: End-to-end verification

**Acceptance checklist:**
- [ ] `contract_registry/subscriptions.yaml` has >= 8 subscriptions
- [ ] `contracts/registry.py` loads and queries correctly
- [ ] Generator accepts `--registry`, injects `registry_subscribers`, produces full dbt mapping
- [ ] `schema_snapshots/index.jsonl` has entries with `schema_hash`
- [ ] 2+ snapshots per contract
- [ ] Runner accepts `--mode`, produces `pipeline_action`
- [ ] Attributor uses 4-step pipeline with registry-first blast radius
- [ ] Violation records have `blast_radius.source`, `direct_subscribers`, `contamination_depth`
- [ ] At least 1 violation has `injection_note: true`
- [ ] Schema analyzer produces migration impact with per-consumer failure modes
- [ ] Enforcer report has file-specific recommendations
- [ ] dbt schemas map range/pattern/datetime to `dbt_expectations`
- [ ] `violation_log.yaml` and `schema_snapshots.yaml` contracts exist
- [ ] Interim report PDF regenerated

### Files
| File | Action |
|------|--------|
| `reports/Week7_Interim_Report.md` | MODIFY |
| `reports/Week7_Interim_Report.pdf` | REGENERATE |

---

## File Summary

### New Files (7)
- `contract_registry/subscriptions.yaml` -- Registry of all producer-consumer subscriptions
- `contracts/registry.py` -- Registry loader and query utilities
- `generated_contracts/violation_log.yaml` -- Contract for violation log output
- `generated_contracts/schema_snapshots.yaml` -- Contract for snapshot metadata
- `schema_snapshots/index.jsonl` -- Auto-generated snapshot index

### Modified Files (5)
- `contracts/generator.py` -- Registry flag, lineage injection, full dbt mapping, snapshot indexing
- `contracts/attributor.py` -- 4-step registry-first pipeline refactor
- `contracts/runner.py` -- `--mode` flag (AUDIT/WARN/ENFORCE)
- `contracts/schema_analyzer.py` -- Hash diff, registry blast radius, migration impact, deprecation timeline
- `contracts/report_generator.py` -- File-specific recommendations

### Regenerated Artifacts
- `generated_contracts/*.yaml` and `*_dbt.yml`
- `validation_reports/*.json`
- `violation_log/violations.jsonl`
- `enforcer_report/report_data.json`
- `reports/Week7_Interim_Report.md` and `.pdf`

---

## Risk Mitigations

1. **Generator is most-modified file** -- All generator changes consolidated in Phase 2
2. **Circular dependency (Spec 01 <-> Spec 02)** -- Registry created first with all 8 subscriptions; contracts created after
3. **Snapshot timestamp collision** -- Use sub-second precision in timestamps
4. **Field name mapping** -- Log warnings when no registry match found; fall back to lineage-only
5. **weasyprint dependency** -- Verify installation before Phase 6
