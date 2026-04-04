#!/usr/bin/env python3
"""ValidationRunner: Executes contract checks against a data snapshot.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3_document_refinery_extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/clean_run.json
"""

import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def flatten_for_validation(records):
    """Same flattening as generator to ensure column names match."""
    rows = []
    for r in records:
        base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        if "extracted_facts" in r and isinstance(r["extracted_facts"], list):
            for fact in r.get("extracted_facts", [{}]):
                if isinstance(fact, dict):
                    row = {**base, **{f"fact_{k}": v for k, v in fact.items()
                                      if not isinstance(v, (list, dict))}}
                    rows.append(row)
            continue
        if "scores" in r and isinstance(r["scores"], dict):
            flat = {**base}
            for crit, vals in r["scores"].items():
                if isinstance(vals, dict):
                    flat[f"score_{crit}"] = vals.get("score")
            rows.append(flat)
            continue
        if "code_refs" in r and isinstance(r["code_refs"], list):
            for ref in r["code_refs"]:
                if isinstance(ref, dict):
                    row = {**base, **{f"ref_{k}": v for k, v in ref.items()
                                      if not isinstance(v, (list, dict))}}
                    rows.append(row)
            continue
        if "nodes" in r and isinstance(r["nodes"], list):
            rows.append(base)
            continue
        if "metadata" in r and isinstance(r["metadata"], dict):
            flat = {**base, **{f"meta_{k}": v for k, v in r["metadata"].items()
                               if not isinstance(v, (list, dict))}}
            rows.append(flat)
            continue
        rows.append(base)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Check implementations ───────────────────────────────────────────────

def check_required(df, col_name, contract_id):
    """Check that a required field has no nulls."""
    if col_name not in df.columns:
        return {
            "check_id": f"{contract_id}.{col_name}.required",
            "column_name": col_name,
            "check_type": "required",
            "status": "ERROR",
            "severity": "CRITICAL",
            "actual_value": "column not found",
            "expected": "column exists with no nulls",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"Column '{col_name}' not found in data."
        }
    null_count = int(df[col_name].isna().sum())
    return {
        "check_id": f"{contract_id}.{col_name}.required",
        "column_name": col_name,
        "check_type": "required",
        "status": "PASS" if null_count == 0 else "FAIL",
        "severity": "CRITICAL" if null_count > 0 else "LOW",
        "actual_value": f"{null_count} nulls in {len(df)} records",
        "expected": "0 nulls",
        "records_failing": null_count,
        "sample_failing": [],
        "message": f"{'No' if null_count == 0 else null_count} null values found."
    }


def check_type(df, col_name, expected_type, contract_id):
    """Check column type matches contract."""
    if col_name not in df.columns:
        return {
            "check_id": f"{contract_id}.{col_name}.type",
            "column_name": col_name,
            "check_type": "type",
            "status": "ERROR",
            "severity": "CRITICAL",
            "actual_value": "column not found",
            "expected": expected_type,
            "records_failing": 0,
            "sample_failing": [],
            "message": f"Column '{col_name}' not found."
        }

    dtype = str(df[col_name].dtype)
    type_ok = False
    if expected_type == "number":
        type_ok = pd.api.types.is_numeric_dtype(df[col_name])
    elif expected_type == "integer":
        type_ok = pd.api.types.is_integer_dtype(df[col_name])
    elif expected_type == "string":
        type_ok = dtype == "object" or pd.api.types.is_string_dtype(df[col_name])
    elif expected_type == "boolean":
        type_ok = pd.api.types.is_bool_dtype(df[col_name])
    else:
        type_ok = True  # unknown type, pass

    return {
        "check_id": f"{contract_id}.{col_name}.type",
        "column_name": col_name,
        "check_type": "type",
        "status": "PASS" if type_ok else "FAIL",
        "severity": "CRITICAL" if not type_ok else "LOW",
        "actual_value": dtype,
        "expected": expected_type,
        "records_failing": 0 if type_ok else len(df),
        "sample_failing": [],
        "message": f"Type {'matches' if type_ok else 'mismatch'}: actual={dtype}, expected={expected_type}."
    }


def check_range(df, col_name, minimum, maximum, contract_id):
    """Check numeric range."""
    if col_name not in df.columns:
        return {
            "check_id": f"{contract_id}.{col_name}.range",
            "column_name": col_name,
            "check_type": "range",
            "status": "ERROR",
            "severity": "CRITICAL",
            "actual_value": "column not found",
            "expected": f"min>={minimum}, max<={maximum}",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"Column '{col_name}' not found."
        }

    series = df[col_name].dropna()
    if len(series) == 0:
        return {
            "check_id": f"{contract_id}.{col_name}.range",
            "column_name": col_name,
            "check_type": "range",
            "status": "PASS",
            "severity": "LOW",
            "actual_value": "no non-null values",
            "expected": f"min>={minimum}, max<={maximum}",
            "records_failing": 0,
            "sample_failing": [],
            "message": "No non-null values to check."
        }

    data_min = float(series.min())
    data_max = float(series.max())
    data_mean = float(series.mean())

    violations = ((series < minimum) | (series > maximum))
    fail_count = int(violations.sum())
    sample = df.loc[violations].head(5).index.tolist() if fail_count > 0 else []

    passed = data_min >= minimum and data_max <= maximum

    return {
        "check_id": f"{contract_id}.{col_name}.range",
        "column_name": col_name,
        "check_type": "range",
        "status": "PASS" if passed else "FAIL",
        "severity": "CRITICAL" if not passed else "LOW",
        "actual_value": f"min={data_min:.4f}, max={data_max:.4f}, mean={data_mean:.4f}",
        "expected": f"min>={minimum}, max<={maximum}",
        "records_failing": fail_count,
        "sample_failing": [str(s) for s in sample],
        "message": (f"Range {'OK' if passed else 'VIOLATED'}. "
                    f"Data range [{data_min:.4f}, {data_max:.4f}] vs contract [{minimum}, {maximum}]."
                    + (f" {fail_count} records out of range." if not passed else ""))
    }


def check_enum(df, col_name, allowed_values, contract_id):
    """Check enum conformance."""
    if col_name not in df.columns:
        return {
            "check_id": f"{contract_id}.{col_name}.enum",
            "column_name": col_name,
            "check_type": "enum",
            "status": "ERROR",
            "severity": "CRITICAL",
            "actual_value": "column not found",
            "expected": f"one of {allowed_values}",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"Column '{col_name}' not found."
        }

    series = df[col_name].dropna()
    non_conforming = series[~series.isin(allowed_values)]
    fail_count = len(non_conforming)
    sample = non_conforming.unique()[:5].tolist()

    return {
        "check_id": f"{contract_id}.{col_name}.enum",
        "column_name": col_name,
        "check_type": "enum",
        "status": "PASS" if fail_count == 0 else "FAIL",
        "severity": "CRITICAL" if fail_count > 0 else "LOW",
        "actual_value": f"{fail_count} non-conforming values",
        "expected": f"one of {allowed_values}",
        "records_failing": fail_count,
        "sample_failing": [str(s) for s in sample],
        "message": f"{'All' if fail_count == 0 else fail_count} values {'conform' if fail_count == 0 else 'do not conform'} to enum."
    }


def check_pattern(df, col_name, pattern, contract_id):
    """Check regex pattern for UUID/hash fields."""
    if col_name not in df.columns:
        return {
            "check_id": f"{contract_id}.{col_name}.pattern",
            "column_name": col_name,
            "check_type": "pattern",
            "status": "ERROR",
            "severity": "CRITICAL",
            "actual_value": "column not found",
            "expected": f"matches {pattern}",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"Column '{col_name}' not found."
        }

    series = df[col_name].dropna().astype(str)
    regex = re.compile(pattern)
    non_matching = series[~series.apply(lambda x: bool(regex.match(x)))]
    fail_count = len(non_matching)
    # Sample 100 if > 10000
    if len(series) > 10000:
        sample_series = series.sample(100, random_state=42)
        non_matching_sample = sample_series[~sample_series.apply(lambda x: bool(regex.match(x)))]
        fail_count = int(len(non_matching_sample) / 100 * len(series))

    return {
        "check_id": f"{contract_id}.{col_name}.pattern",
        "column_name": col_name,
        "check_type": "pattern",
        "status": "PASS" if fail_count == 0 else "FAIL",
        "severity": "CRITICAL" if fail_count > 0 else "LOW",
        "actual_value": f"{fail_count} non-matching values",
        "expected": f"matches {pattern}",
        "records_failing": fail_count,
        "sample_failing": non_matching.head(5).tolist() if fail_count > 0 else [],
        "message": f"Pattern {'OK' if fail_count == 0 else 'VIOLATED'}."
    }


def check_datetime(df, col_name, contract_id):
    """Check date-time format parseable."""
    if col_name not in df.columns:
        return {
            "check_id": f"{contract_id}.{col_name}.datetime",
            "column_name": col_name,
            "check_type": "datetime",
            "status": "ERROR",
            "severity": "CRITICAL",
            "actual_value": "column not found",
            "expected": "ISO 8601 date-time",
            "records_failing": 0,
            "sample_failing": [],
            "message": f"Column '{col_name}' not found."
        }

    series = df[col_name].dropna().astype(str)
    unparseable = 0
    samples = []
    for val in series:
        try:
            datetime.fromisoformat(val.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            unparseable += 1
            if len(samples) < 5:
                samples.append(val)

    return {
        "check_id": f"{contract_id}.{col_name}.datetime",
        "column_name": col_name,
        "check_type": "datetime",
        "status": "PASS" if unparseable == 0 else "FAIL",
        "severity": "CRITICAL" if unparseable > 0 else "LOW",
        "actual_value": f"{unparseable} unparseable values",
        "expected": "ISO 8601 date-time",
        "records_failing": unparseable,
        "sample_failing": samples,
        "message": f"{'All' if unparseable == 0 else unparseable} values {'parse' if unparseable == 0 else 'fail to parse'} as ISO 8601."
    }


def check_statistical_drift(col_name, current_mean, current_std, baselines, contract_id):
    """Check for statistical drift from baseline."""
    if col_name not in baselines:
        return None  # no baseline yet
    b = baselines[col_name]
    z_score = abs(current_mean - b["mean"]) / max(b["stddev"], 1e-9)
    if z_score > 3:
        status, severity = "FAIL", "HIGH"
        msg = f"{col_name} mean drifted {z_score:.1f} stddev from baseline"
    elif z_score > 2:
        status, severity = "WARN", "MEDIUM"
        msg = f"{col_name} mean within warning range ({z_score:.1f} stddev)"
    else:
        status, severity = "PASS", "LOW"
        msg = f"{col_name} mean stable ({z_score:.1f} stddev from baseline)"

    return {
        "check_id": f"{contract_id}.{col_name}.statistical_drift",
        "column_name": col_name,
        "check_type": "statistical_drift",
        "status": status,
        "severity": severity,
        "actual_value": f"mean={current_mean:.4f}, z_score={z_score:.2f}",
        "expected": f"mean={b['mean']:.4f} ± {b['stddev']:.4f}",
        "records_failing": 0,
        "sample_failing": [],
        "message": msg
    }


# ── Main validation logic ──────────────────────────────────────────────

def validate(contract_path, data_path):
    """Run all checks from contract against data."""
    with open(contract_path) as f:
        contract = yaml.safe_load(f)

    records = load_jsonl(data_path)
    df = flatten_for_validation(records)
    contract_id = contract.get("id", "unknown")
    schema = contract.get("schema", {})

    results = []

    # Load baselines if available
    baselines_path = Path("schema_snapshots/baselines.json")
    baselines = {}
    if baselines_path.exists():
        with open(baselines_path) as f:
            bl = json.load(f)
            baselines = bl.get("columns", {})

    for col_name, clause in schema.items():
        # Required check
        if clause.get("required", False):
            results.append(check_required(df, col_name, contract_id))

        # Type check
        if "type" in clause:
            results.append(check_type(df, col_name, clause["type"], contract_id))

        # Range check
        if "minimum" in clause or "maximum" in clause:
            minimum = clause.get("minimum", float("-inf"))
            maximum = clause.get("maximum", float("inf"))
            results.append(check_range(df, col_name, minimum, maximum, contract_id))

        # Enum check
        if "enum" in clause:
            results.append(check_enum(df, col_name, clause["enum"], contract_id))

        # Pattern check
        if "pattern" in clause:
            results.append(check_pattern(df, col_name, clause["pattern"], contract_id))

        # Date-time check
        if clause.get("format") == "date-time":
            results.append(check_datetime(df, col_name, contract_id))

    # Statistical drift checks for numeric columns
    for col in df.select_dtypes(include="number").columns:
        if col in baselines:
            drift = check_statistical_drift(
                col, float(df[col].mean()), float(df[col].std()),
                baselines, contract_id
            )
            if drift:
                results.append(drift)

    # Count statuses
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    warned = sum(1 for r in results if r["status"] == "WARN")
    errored = sum(1 for r in results if r["status"] == "ERROR")

    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "snapshot_id": sha256_file(data_path),
        "run_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": results
    }

    # Write baselines on first run (if none exist)
    if not baselines_path.exists():
        new_baselines = {}
        for col in df.select_dtypes(include="number").columns:
            s = df[col].dropna()
            if len(s) > 1:
                new_baselines[col] = {"mean": float(s.mean()), "stddev": float(s.std())}
        baselines_path.parent.mkdir(parents=True, exist_ok=True)
        with open(baselines_path, "w") as f:
            json.dump({
                "written_at": datetime.now(timezone.utc).isoformat(),
                "columns": new_baselines
            }, f, indent=2)
        print(f"  Baselines written to {baselines_path}")

    return report


def determine_pipeline_action(results, mode):
    """Determine pipeline action based on mode and check results.

    AUDIT: always PASS (log only)
    WARN: BLOCK only if CRITICAL failures exist
    ENFORCE: BLOCK if CRITICAL or HIGH failures exist
    """
    if mode == "AUDIT":
        return "PASS"

    severities = {r["severity"] for r in results if r["status"] in ("FAIL", "ERROR")}
    if mode == "WARN":
        return "BLOCK" if "CRITICAL" in severities else "PASS"
    if mode == "ENFORCE":
        return "BLOCK" if severities & {"CRITICAL", "HIGH"} else "PASS"
    return "PASS"


def main():
    parser = argparse.ArgumentParser(description="ValidationRunner")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--data", required=True, help="Path to data JSONL")
    parser.add_argument("--mode", choices=["AUDIT", "WARN", "ENFORCE"], default="AUDIT",
                        help="Validation mode: AUDIT (log only), WARN (block critical), ENFORCE (block critical+high)")
    parser.add_argument("--output", required=True, help="Path for output report JSON")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"ValidationRunner")
    print(f"{'='*60}")
    print(f"  Contract: {args.contract}")
    print(f"  Data: {args.data}")

    report = validate(args.contract, args.data)

    # Determine pipeline action based on mode
    pipeline_action = determine_pipeline_action(report.get("results", []), args.mode)
    report["mode"] = args.mode
    report["pipeline_action"] = pipeline_action

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n  Mode: {args.mode} | Pipeline action: {pipeline_action}")
    print(f"  Results: {report['total_checks']} checks — "
          f"{report['passed']} PASS, {report['failed']} FAIL, "
          f"{report['warned']} WARN, {report['errored']} ERROR")
    print(f"  Report written to {output_path}")

    # Print failures
    for r in report["results"]:
        if r["status"] in ("FAIL", "ERROR"):
            print(f"  ❌ {r['check_id']}: {r['message']}")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
