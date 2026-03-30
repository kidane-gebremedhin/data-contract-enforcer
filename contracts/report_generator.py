#!/usr/bin/env python3
"""ReportGenerator: Auto-generates the Enforcer Report from validation data.

Usage:
    python contracts/report_generator.py
"""

import json
import glob
from pathlib import Path
from datetime import datetime, timedelta, timezone


def compute_health_score(validation_reports):
    """0-100 score. Start at 100. Subtract per violation severity."""
    deductions = {"CRITICAL": 20, "HIGH": 10, "MEDIUM": 5, "LOW": 1}
    score = 100
    for report in validation_reports:
        for result in report.get("results", []):
            if result["status"] in ("FAIL", "ERROR"):
                score -= deductions.get(result.get("severity", "LOW"), 1)
    return max(0, min(100, score))


def plain_language_violation(result):
    """Convert a technical violation to plain language."""
    col = result.get("column_name", "unknown field")
    check = result.get("check_type", "unknown check")
    check_id = result.get("check_id", "")
    system = check_id.split(".")[0] if "." in check_id else "unknown system"

    return (f"The {col} field in {system} "
            f"failed its {check} check. "
            f"Expected {result.get('expected', 'N/A')} but found {result.get('actual_value', 'N/A')}. "
            f"This affects {result.get('records_failing', 'unknown')} records.")


def generate_report(reports_dir="validation_reports/", violations_dir="violation_log/",
                    ai_metrics_path="validation_reports/ai_extensions.json",
                    schema_evolution_path=None):
    """Generate the Enforcer Report from live data."""
    # Load validation reports
    reports = []
    for p in glob.glob(f"{reports_dir}*.json"):
        if "ai_extensions" in p or "schema_evolution" in p:
            continue
        try:
            with open(p) as f:
                reports.append(json.load(f))
        except Exception:
            continue

    # Load violations
    violations = []
    vlog_path = Path(f"{violations_dir}violations.jsonl")
    if vlog_path.exists():
        with open(vlog_path) as f:
            violations = [json.loads(l) for l in f if l.strip()]

    # Load AI metrics
    ai_metrics = {}
    if Path(ai_metrics_path).exists():
        with open(ai_metrics_path) as f:
            ai_metrics = json.load(f)

    # Load schema evolution
    schema_changes = {}
    evolution_files = glob.glob(f"{reports_dir}schema_evolution*.json")
    if evolution_files:
        with open(evolution_files[-1]) as f:
            schema_changes = json.load(f)

    # Compute health score
    health_score = compute_health_score(reports)

    # Find all failures
    all_failures = [r for rep in reports for r in rep.get("results", [])
                    if r["status"] in ("FAIL", "ERROR")]

    # Top 3 most severe
    severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    top_3 = sorted(
        all_failures,
        key=lambda x: severity_order.index(x.get("severity", "LOW"))
                      if x.get("severity", "LOW") in severity_order else 999
    )[:3]

    # Violation counts by severity
    total_by_severity = {
        sev: len([v for v in all_failures if v.get("severity") == sev])
        for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    }

    # AI risk assessment
    ai_assessment = "No AI extension data available."
    if ai_metrics.get("checks"):
        checks = {c["check"]: c for c in ai_metrics["checks"]}
        parts = []
        if "embedding_drift" in checks:
            ed = checks["embedding_drift"]
            parts.append(f"Embedding drift: {ed['status']} (score={ed['drift_score']})")
        if "prompt_input_validation" in checks:
            pv = checks["prompt_input_validation"]
            parts.append(f"Prompt validation: {pv['status']} ({pv['valid']}/{pv['total_records']} valid)")
        if "llm_output_schema_violation_rate" in checks:
            ov = checks["llm_output_schema_violation_rate"]
            parts.append(f"LLM output violations: {ov['status']} (rate={ov['violation_rate']}, trend={ov['trend']})")
        ai_assessment = "; ".join(parts)

    # Schema changes summary
    schema_summary = "No schema changes detected."
    if schema_changes.get("changes"):
        breaking = schema_changes.get("breaking_changes", 0)
        total = schema_changes.get("total_changes", 0)
        schema_summary = (f"{total} schema changes detected. "
                         f"{breaking} breaking, {total - breaking} compatible. "
                         f"Verdict: {schema_changes.get('overall_verdict', 'N/A')}")

    # Build recommendations
    recommendations = []
    for f in top_3:
        col = f.get("column_name", "unknown")
        check_id = f.get("check_id", "")
        if "confidence" in col and "range" in check_id:
            recommendations.append(
                f"Update source pipeline to output {col} as float 0.0-1.0 per contract clause {check_id}")
        elif "drift" in check_id:
            recommendations.append(
                f"Investigate statistical drift in {col}. Re-establish baseline after confirming data correctness.")
        else:
            recommendations.append(
                f"Fix {f.get('check_type', 'unknown')} violation in {col}: {f.get('message', '')}")

    if not recommendations:
        recommendations = ["All contracts passing. Continue monitoring for drift."]

    # Add general recommendations
    if total_by_severity.get("CRITICAL", 0) > 0:
        recommendations.append("Add contract enforcement step to CI pipeline to prevent future CRITICAL violations.")
    recommendations.append("Review schema evolution snapshots weekly to catch breaking changes early.")
    recommendations = recommendations[:5]

    now = datetime.now(timezone.utc)
    report = {
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "period": f"{(now - timedelta(days=7)).date()} to {now.date()}",
        "data_health_score": health_score,
        "health_narrative": (
            f"Score of {health_score}/100. " +
            ("No critical violations." if health_score >= 90 else
             f'{total_by_severity.get("CRITICAL", 0)} critical issues require immediate action.')
        ),
        "top_violations": [plain_language_violation(v) for v in top_3],
        "total_violations_by_severity": total_by_severity,
        "violation_count": len(violations),
        "schema_changes_summary": schema_summary,
        "ai_system_risk_assessment": ai_assessment,
        "recommendations": recommendations,
        "reports_analyzed": len(reports),
        "total_checks_run": sum(r.get("total_checks", 0) for r in reports),
        "total_passed": sum(r.get("passed", 0) for r in reports),
        "total_failed": sum(r.get("failed", 0) for r in reports)
    }

    return report


def main():
    print(f"\n{'='*60}")
    print(f"Enforcer Report Generator")
    print(f"{'='*60}")

    report = generate_report()

    output_dir = Path("enforcer_report")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "report_data.json"

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n  Data Health Score: {report['data_health_score']}/100")
    print(f"  {report['health_narrative']}")
    print(f"\n  Total checks: {report['total_checks_run']}")
    print(f"  Passed: {report['total_passed']}, Failed: {report['total_failed']}")
    print(f"  Violations logged: {report['violation_count']}")
    print(f"\n  Top violations:")
    for v in report["top_violations"]:
        print(f"    - {v}")
    print(f"\n  Recommendations:")
    for r in report["recommendations"]:
        print(f"    - {r}")
    print(f"\n  Report written to {output_path}")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
