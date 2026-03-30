#!/usr/bin/env python3
"""ViolationAttributor: Traces violations to upstream commits via lineage + git blame.

Usage:
    python contracts/attributor.py \
        --violation validation_reports/injected_violation.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --contract generated_contracts/week3_document_refinery_extractions.yaml \
        --output violation_log/violations.jsonl
"""

import argparse
import json
import subprocess
import uuid
import re
from datetime import datetime, timezone
from pathlib import Path

import yaml


def find_upstream_files(failing_column, lineage_snapshot):
    """Walk lineage graph to find files that produce the failing column."""
    # Determine which week/system the column belongs to
    column_system = None
    for w in ["week1", "week2", "week3", "week4", "week5"]:
        if w in failing_column:
            column_system = w
            break

    candidates = []
    for node in lineage_snapshot.get("nodes", []):
        nid = node.get("node_id", "")
        if column_system and column_system in nid and node.get("type") == "FILE":
            candidates.append(node.get("metadata", {}).get("path", nid))
        elif node.get("type") == "PIPELINE" and column_system and column_system in nid:
            candidates.append(nid)

    # If no file-level matches, look at edges for upstream producers
    if not candidates:
        for edge in lineage_snapshot.get("edges", []):
            tgt = edge.get("target", "")
            src = edge.get("source", "")
            if column_system and column_system in tgt:
                candidates.append(src)

    return candidates[:5]


def get_recent_commits(file_path, days=14, repo_paths=None):
    """Run git log on the file and parse structured output."""
    # Try multiple repo locations
    search_dirs = repo_paths or [
        "/home/kg/Projects/10Academy/document-intelligence-refinery",
        "/home/kg/Projects/10Academy/intelligent-rag",
        "/home/kg/Projects/10Academy/brownfield-cartographer",
        "/home/kg/Projects/10Academy/apex-ledger-starter-project",
        "/home/kg/Projects/10Academy/agentic-ledger",
        "/home/kg/Projects/10Academy/automaton-auditor",
        ".",
    ]

    for repo_dir in search_dirs:
        if not Path(repo_dir).exists():
            continue
        cmd = [
            "git", "log", "--follow",
            f"--since={days} days ago",
            "--format=%H|%ae|%ai|%s",
            "--", file_path
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=repo_dir, timeout=10)
            commits = []
            for line in result.stdout.strip().split("\n"):
                if "|" in line:
                    parts = line.split("|", 3)
                    if len(parts) >= 4:
                        commits.append({
                            "commit_hash": parts[0],
                            "author": parts[1],
                            "commit_timestamp": parts[2].strip(),
                            "commit_message": parts[3]
                        })
            if commits:
                return commits
        except Exception:
            continue

    # Fallback: return a placeholder based on current repo
    try:
        result = subprocess.run(
            ["git", "log", "-5", "--format=%H|%ae|%ai|%s"],
            capture_output=True, text=True, cwd=".", timeout=10
        )
        commits = []
        for line in result.stdout.strip().split("\n"):
            if "|" in line:
                parts = line.split("|", 3)
                if len(parts) >= 4:
                    commits.append({
                        "commit_hash": parts[0],
                        "author": parts[1],
                        "commit_timestamp": parts[2].strip(),
                        "commit_message": parts[3]
                    })
        return commits
    except Exception:
        return [{
            "commit_hash": "0" * 40,
            "author": "unknown@example.com",
            "commit_timestamp": datetime.now(timezone.utc).isoformat(),
            "commit_message": "Unable to retrieve git history"
        }]


def score_candidates(commits, violation_timestamp, lineage_distance=1):
    """Score commit candidates by temporal proximity and lineage distance."""
    scored = []
    try:
        v_time = datetime.fromisoformat(violation_timestamp.replace("Z", "+00:00"))
    except Exception:
        v_time = datetime.now(timezone.utc)

    for rank, commit in enumerate(commits[:5], start=1):
        try:
            ts = commit["commit_timestamp"]
            # Handle various git timestamp formats
            ts = re.sub(r"\s+([+-]\d{4})$", r"\1", ts)
            c_time = datetime.fromisoformat(ts.replace(" +", "+").replace(" -", "-"))
            days_diff = abs((v_time - c_time).days)
        except Exception:
            days_diff = 7

        score = max(0.0, 1.0 - (days_diff * 0.1) - (lineage_distance * 0.2))
        scored.append({
            **commit,
            "rank": rank,
            "confidence_score": round(score, 3)
        })

    return sorted(scored, key=lambda x: x["confidence_score"], reverse=True)


def compute_blast_radius(contract_path, violation_id, records_failing=0):
    """Compute blast radius from contract lineage."""
    with open(contract_path) as f:
        contract = yaml.safe_load(f)

    downstream = contract.get("lineage", {}).get("downstream", [])
    return {
        "violation_id": violation_id,
        "affected_nodes": [d["id"] for d in downstream],
        "affected_pipelines": [d["id"] for d in downstream if "pipeline" in d.get("id", "")],
        "estimated_records": records_failing
    }


def attribute_violations(report_path, lineage_path, contract_path):
    """Main attribution logic: for each FAIL, trace to upstream commit."""
    with open(report_path) as f:
        report = json.load(f)

    with open(lineage_path) as f:
        lines = [l for l in f if l.strip()]
        snapshot = json.loads(lines[-1])

    violations = []
    for result in report.get("results", []):
        if result["status"] not in ("FAIL", "ERROR"):
            continue

        violation_id = str(uuid.uuid4())
        check_id = result["check_id"]
        column_name = result.get("column_name", "unknown")

        # Step 1: Find upstream files via lineage
        upstream_files = find_upstream_files(check_id, snapshot)

        # Step 2: Git blame integration
        all_commits = []
        for fp in upstream_files:
            commits = get_recent_commits(fp)
            all_commits.extend(commits)

        # If no commits found from upstream files, get from current repo
        if not all_commits:
            all_commits = get_recent_commits(".")

        # Step 3: Score candidates
        scored = score_candidates(
            all_commits,
            report.get("run_timestamp", datetime.now(timezone.utc).isoformat()),
            lineage_distance=1
        )

        # Step 4: Build violation record
        blast = compute_blast_radius(
            contract_path, violation_id,
            result.get("records_failing", 0)
        )

        violation = {
            "violation_id": violation_id,
            "check_id": check_id,
            "detected_at": report.get("run_timestamp", datetime.now(timezone.utc).isoformat()),
            "severity": result.get("severity", "CRITICAL"),
            "message": result.get("message", ""),
            "actual_value": result.get("actual_value", ""),
            "expected": result.get("expected", ""),
            "blame_chain": scored[:5],
            "blast_radius": blast
        }
        violations.append(violation)

    return violations


def main():
    parser = argparse.ArgumentParser(description="ViolationAttributor")
    parser.add_argument("--violation", required=True, help="Path to validation report JSON")
    parser.add_argument("--lineage", required=True, help="Path to lineage snapshots JSONL")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--output", required=True, help="Output path for violations JSONL")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"ViolationAttributor")
    print(f"{'='*60}")

    violations = attribute_violations(args.violation, args.lineage, args.contract)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Append to existing file
    with open(output_path, "a") as f:
        for v in violations:
            f.write(json.dumps(v) + "\n")

    print(f"  Attributed {len(violations)} violations")
    for v in violations:
        print(f"  📍 {v['check_id']}: {v['message']}")
        if v["blame_chain"]:
            top = v["blame_chain"][0]
            print(f"     Top suspect: {top.get('author', 'unknown')} "
                  f"(commit {top.get('commit_hash', '?')[:8]}..., "
                  f"confidence={top.get('confidence_score', 0)})")
        print(f"     Blast radius: {len(v['blast_radius']['affected_nodes'])} affected nodes, "
              f"{v['blast_radius']['estimated_records']} records")

    print(f"\n  Violations appended to {output_path}")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
