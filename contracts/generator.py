#!/usr/bin/env python3
"""ContractGenerator: Auto-generates Bitol YAML data contracts from JSONL data.

Usage:
    python contracts/generator.py \
        --source outputs/week3/extractions.jsonl \
        --contract-id week3-document-refinery-extractions \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output generated_contracts/
"""

import argparse
import json
import hashlib
import shutil
import uuid
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from ydata_profiling import ProfileReport


# ── Stage 1: Load and profile data ─────────────────────────────────────

def load_jsonl(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def flatten_for_profile(records):
    """Flatten nested JSONL to a flat DataFrame for profiling.
    For arrays like extracted_facts[], explode to one row per item."""
    rows = []
    for r in records:
        base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        # Handle extracted_facts (Week 3)
        if "extracted_facts" in r and isinstance(r["extracted_facts"], list):
            for fact in r.get("extracted_facts", [{}]):
                if isinstance(fact, dict):
                    row = {**base, **{f"fact_{k}": v for k, v in fact.items()
                                      if not isinstance(v, (list, dict))}}
                    rows.append(row)
            continue
        # Handle scores (Week 2)
        if "scores" in r and isinstance(r["scores"], dict):
            flat = {**base}
            for crit, vals in r["scores"].items():
                if isinstance(vals, dict):
                    flat[f"score_{crit}"] = vals.get("score")
            rows.append(flat)
            continue
        # Handle code_refs (Week 1)
        if "code_refs" in r and isinstance(r["code_refs"], list):
            for ref in r["code_refs"]:
                if isinstance(ref, dict):
                    row = {**base, **{f"ref_{k}": v for k, v in ref.items()
                                      if not isinstance(v, (list, dict))}}
                    rows.append(row)
            continue
        # Handle nodes/edges (Week 4)
        if "nodes" in r and isinstance(r["nodes"], list):
            rows.append(base)
            continue
        # Handle metadata (Week 5)
        if "metadata" in r and isinstance(r["metadata"], dict):
            flat = {**base, **{f"meta_{k}": v for k, v in r["metadata"].items()
                               if not isinstance(v, (list, dict))}}
            rows.append(flat)
            continue
        # Default: just flatten top-level non-nested
        rows.append(base)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Stage 2: Structural profiling per column ────────────────────────────

def run_ydata_profile(df):
    """Run ydata-profiling and return the description dict."""
    report = ProfileReport(df, minimal=True, title="ContractGenerator Profile")
    return report.get_description()


def profile_column(series, col_name, ydata_vars=None):
    """Profile a single column, enriched with ydata-profiling stats when available."""
    result = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": int(series.nunique()),
        "sample_values": [str(v) for v in series.dropna().unique()[:50]],
    }

    # Enrich with ydata-profiling variable description if available
    if ydata_vars and col_name in ydata_vars:
        var_desc = ydata_vars[col_name]
        result["ydata_type"] = str(var_desc.get("type", ""))

    if pd.api.types.is_numeric_dtype(series):
        s = series.dropna()
        if len(s) > 0:
            stats = {
                "min": float(s.min()), "max": float(s.max()),
                "mean": float(s.mean()), "p25": float(s.quantile(0.25)),
                "p50": float(s.quantile(0.50)), "p75": float(s.quantile(0.75)),
                "p95": float(s.quantile(0.95)), "p99": float(s.quantile(0.99)),
                "stddev": float(s.std()) if len(s) > 1 else 0.0
            }
            # Enrich with ydata stats if available
            if ydata_vars and col_name in ydata_vars:
                yd = ydata_vars[col_name]
                if "kurtosis" in yd:
                    stats["kurtosis"] = float(yd["kurtosis"])
                if "skewness" in yd:
                    stats["skewness"] = float(yd["skewness"])
            result["stats"] = stats
    return result


# ── Stage 3: Translate profiles to Bitol YAML clauses ───────────────────

def infer_type(dtype_str):
    mapping = {
        "float64": "number", "float32": "number",
        "int64": "integer", "int32": "integer",
        "bool": "boolean", "object": "string"
    }
    return mapping.get(dtype_str, "string")


def column_to_clause(profile):
    clause = {
        "type": infer_type(profile["dtype"]),
        "required": profile["null_fraction"] == 0.0
    }

    name = profile["name"]

    # Confidence fields: must be 0.0-1.0
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = "Confidence score. Must remain 0.0-1.0 float. BREAKING if changed to 0-100."

    # Score fields: must be 1-5
    if name.startswith("score_") and clause["type"] in ("integer", "number"):
        clause["minimum"] = 1
        clause["maximum"] = 5
        clause["description"] = "Rubric score. Integer 1-5."

    # ID fields
    if name.endswith("_id") or name in ("id", "doc_id", "fact_id", "intent_id",
                                         "verdict_id", "event_id", "snapshot_id"):
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-f-]{36}$"

    # Timestamp fields
    if name.endswith("_at") or name.endswith("_time"):
        clause["format"] = "date-time"

    # Hash fields
    if "hash" in name:
        clause["pattern"] = "^[a-f0-9]{64}$"
        clause["description"] = "SHA-256 hash."

    # Enum fields (low cardinality strings)
    if (profile["cardinality_estimate"] <= 50
            and profile["dtype"] == "object"
            and len(profile["sample_values"]) == profile["cardinality_estimate"]):
        clause["enum"] = profile["sample_values"]

    # Add observed range bounds for numeric columns without semantic ranges
    if "stats" in profile and "minimum" not in clause and "maximum" not in clause:
        s = profile["stats"]
        if clause["type"] in ("integer", "number"):
            clause["minimum"] = int(s["min"]) if clause["type"] == "integer" else s["min"]
            clause["maximum"] = int(s["max"]) if clause["type"] == "integer" else s["max"]

    # Add stats-based description for numeric columns
    if "stats" in profile and "description" not in clause:
        s = profile["stats"]
        clause["description"] = (
            f"Numeric field. Observed range [{s['min']}, {s['max']}], "
            f"mean={s['mean']:.2f}, stddev={s['stddev']:.2f}."
        )

    return clause


# ── Stage 4: Inject lineage context and write YAML ──────────────────────

def inject_lineage(contract, lineage_path, contract_id, registry_path=None):
    """Add lineage context from Week 4 lineage graph and contract registry."""
    try:
        with open(lineage_path) as f:
            lines = [l for l in f if l.strip()]
            snapshot = json.loads(lines[-1])  # latest snapshot

        # Determine which week this contract is for
        week_key = None
        for w in ["week1", "week2", "week3", "week4", "week5"]:
            if w in contract_id:
                week_key = w
                break

        # Find downstream consumers from lineage graph
        downstream = []
        if week_key:
            for edge in snapshot.get("edges", []):
                src = edge.get("source", "")
                tgt = edge.get("target", "")
                if week_key in src:
                    downstream.append({
                        "id": tgt,
                        "description": f"Consumes data from {week_key}",
                        "fields_consumed": ["doc_id", "extracted_facts"] if "week3" in contract_id else ["event_id", "payload"],
                        "breaking_if_changed": ["extracted_facts.confidence", "doc_id"] if "week3" in contract_id else ["event_id", "event_type"]
                    })

        contract["lineage"] = {
            "upstream": [],
            "downstream": downstream[:5]  # limit
        }
    except Exception as e:
        contract["lineage"] = {"upstream": [], "downstream": [], "error": str(e)}

    # Enrich with registry subscribers
    registry_subscribers = []
    if registry_path and Path(registry_path).exists():
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from contracts.registry import load_registry, get_subscribers
        registry = load_registry(registry_path)
        subs = get_subscribers(registry, contract_id)
        for s in subs:
            registry_subscribers.append({
                "id": s["subscriber_id"],
                "team": s.get("subscriber_team", "unknown"),
                "breaking_fields": [bf["field"] for bf in s.get("breaking_fields", [])]
            })
    contract["lineage"]["registry_subscribers"] = registry_subscribers
    if registry_subscribers:
        contract["lineage"]["note"] = "Blast radius uses registry_subscribers as primary source."

    return contract


def build_contract(column_profiles, contract_id, source_path, records_count):
    """Build a full Bitol-compatible contract YAML."""
    schema = {}
    for col, profile in column_profiles.items():
        schema[col] = column_to_clause(profile)

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": contract_id.replace("-", " ").title(),
            "version": "1.0.0",
            "owner": f"{contract_id.split('-')[0]}-team",
            "description": f"Auto-generated contract for {contract_id}. {records_count} records profiled."
        },
        "servers": {
            "local": {
                "type": "local",
                "path": str(source_path),
                "format": "jsonl"
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
            "limitations": "confidence must remain in 0.0-1.0 float range."
        },
        "schema": schema,
        "quality": {
            "type": "SodaChecks",
            "specification": {
                f"checks for {contract_id}": [
                    f"row_count >= 1",
                ] + [
                    f"missing_count({col}) = 0"
                    for col, p in column_profiles.items()
                    if p["null_fraction"] == 0.0
                ][:5] + [
                    f"min({col}) >= {p['stats']['min']:.1f}"
                    for col, p in column_profiles.items()
                    if "stats" in p and "confidence" in col
                ][:2] + [
                    f"max({col}) <= {p['stats']['max']:.1f}"
                    for col, p in column_profiles.items()
                    if "stats" in p and "confidence" in col
                ][:2]
            }
        }
    }

    return contract


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
        if "enum" in clause and clause["enum"]:
            tests.append({"accepted_values": {"values": clause["enum"]}})

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

        # Date-time format (only if no pattern already set)
        if clause.get("format") == "date-time" and "pattern" not in clause:
            tests.append({
                "dbt_expectations.expect_column_values_to_match_regex": {
                    "regex": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"
                }
            })

        # UUID format (only if no pattern already set)
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


def write_snapshot(contract, contract_id, output_dir):
    """Write timestamped schema snapshot with SHA-256 hash and index entry."""
    snapshot_dir = Path("schema_snapshots") / contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"{ts}.yaml"

    output_path = Path(output_dir) / f"{contract_id.replace('-', '_')}.yaml"
    if output_path.exists():
        shutil.copy(output_path, snapshot_path)
        print(f"  Schema snapshot saved to {snapshot_path}")

    # Compute SHA-256 of the schema section for quick diff detection
    schema_str = yaml.dump(contract.get("schema", {}), sort_keys=True)
    schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()

    # Append to schema_snapshots/index.jsonl
    index_path = Path("schema_snapshots") / "index.jsonl"
    index_entry = {
        "contract_id": contract_id,
        "snapshot_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "snapshot_path": str(snapshot_path),
        "schema_hash": schema_hash
    }
    with open(index_path, "a") as f:
        f.write(json.dumps(index_entry) + "\n")
    print(f"  Schema index updated: {index_path} (hash: {schema_hash[:12]}...)")


# ── Main ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ContractGenerator")
    parser.add_argument("--source", required=True, help="Path to JSONL source file")
    parser.add_argument("--contract-id", required=True, help="Contract identifier")
    parser.add_argument("--lineage", default=None, help="Path to lineage snapshots JSONL")
    parser.add_argument("--registry", default="contract_registry/subscriptions.yaml",
                        help="Path to contract registry subscriptions YAML")
    parser.add_argument("--output", required=True, help="Output directory for contracts")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"ContractGenerator — {args.contract_id}")
    print(f"{'='*60}")

    # Stage 1: Load and profile
    print(f"\nStage 1: Loading {args.source}...")
    records = load_jsonl(args.source)
    print(f"  Loaded {len(records)} records")
    df = flatten_for_profile(records)
    print(f"  Flattened to {len(df)} rows x {len(df.columns)} columns")
    print(f"  Columns: {list(df.columns)}")
    print(f"\n  dtypes:\n{df.dtypes.to_string()}")
    print(f"\n  describe:\n{df.describe().to_string()}")

    # Stage 2: Profile each column (enhanced with ydata-profiling)
    print(f"\nStage 2: Profiling columns with ydata-profiling...")
    ydata_desc = run_ydata_profile(df)
    ydata_vars = ydata_desc.variables if hasattr(ydata_desc, "variables") else {}
    print(f"  ydata-profiling: {len(ydata_vars)} variables analyzed")

    column_profiles = {col: profile_column(df[col], col, ydata_vars) for col in df.columns}
    for col, p in column_profiles.items():
        status = "required" if p["null_fraction"] == 0.0 else f"nullable ({p['null_fraction']:.1%} null)"
        yd_type = f", ydata_type={p['ydata_type']}" if "ydata_type" in p else ""
        print(f"  {col}: {p['dtype']} — {status}, cardinality={p['cardinality_estimate']}{yd_type}")

    # Stage 3: Build contract
    print(f"\nStage 3: Generating Bitol YAML contract...")
    contract = build_contract(column_profiles, args.contract_id, args.source, len(records))

    # Stage 4: Inject lineage + registry context
    if args.lineage:
        print(f"\nStage 4: Injecting lineage context from {args.lineage}...")
        contract = inject_lineage(contract, args.lineage, args.contract_id, args.registry)
        ds = contract.get("lineage", {}).get("downstream", [])
        rs = contract.get("lineage", {}).get("registry_subscribers", [])
        print(f"  Found {len(ds)} downstream consumers, {len(rs)} registry subscribers")

    # Write Bitol YAML
    safe_name = args.contract_id.replace("-", "_")
    yaml_path = output_dir / f"{safe_name}.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"\n  Contract written to {yaml_path}")

    # Count clauses
    clause_count = len(contract.get("schema", {}))
    print(f"  Schema clauses: {clause_count}")

    # Stage 5: dbt output
    print(f"\nStage 5: Generating dbt schema.yml...")
    schema_clauses = contract.get("schema", {})
    dbt_schema = generate_dbt_schema(column_profiles, args.contract_id, schema_clauses)
    dbt_path = output_dir / f"{safe_name}_dbt.yml"
    with open(dbt_path, "w") as f:
        yaml.dump(dbt_schema, f, default_flow_style=False, sort_keys=False)
    print(f"  dbt schema written to {dbt_path}")

    # Write schema snapshot
    write_snapshot(contract, args.contract_id, args.output)

    print(f"\n{'='*60}")
    print(f"ContractGenerator complete. {clause_count} clauses generated.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
