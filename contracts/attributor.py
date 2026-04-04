#!/usr/bin/env python3
"""ViolationAttributor: 4-step registry-first attribution pipeline.

Pipeline:
    Step 1: Registry blast radius query (primary source)
    Step 2: Lineage traversal for transitive depth (enrichment)
    Step 3: Git blame for cause attribution
    Step 4: Write violation log

Usage:
    python contracts/attributor.py \
        --violation validation_reports/violated_run.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --contract generated_contracts/week3_document_refinery_extractions.yaml \
        --registry contract_registry/subscriptions.yaml \
        --output violation_log/violations.jsonl
"""

import argparse
import json
import subprocess
import sys
import os
import uuid
import re
from datetime import datetime, timezone
from pathlib import Path

import yaml

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from contracts.registry import load_registry, get_breaking_field_subscribers


# ── Field name mapping ────────────────────────────────────────────────

def map_column_to_registry_field(column_name):
    """Map flattened column name back to registry dot-notation.

    E.g., 'fact_confidence' -> 'extracted_facts.confidence'
          'meta_source_service' -> 'metadata.source_service'
    """
    mappings = {
        "fact_confidence": "extracted_facts.confidence",
        "fact_text": "extracted_facts.text",
        "fact_fact_id": "extracted_facts.fact_id",
        "fact_page_ref": "extracted_facts.page_ref",
        "fact_source_excerpt": "extracted_facts.source_excerpt",
        "meta_causation_id": "metadata.causation_id",
        "meta_correlation_id": "metadata.correlation_id",
        "meta_user_id": "metadata.user_id",
        "meta_source_service": "metadata.source_service",
    }
    return mappings.get(column_name, column_name)


# ── Step 1: Registry blast radius (PRIMARY) ──────────────────────────

def registry_blast_radius(contract_id, failing_field, registry_path):
    """Query registry for affected subscribers. Primary blast radius source."""
    if not Path(registry_path).exists():
        print(f"  WARNING: Registry not found at {registry_path}, falling back to lineage-only")
        return []
    registry = load_registry(registry_path)
    affected = get_breaking_field_subscribers(registry, contract_id, failing_field)
    return affected


# ── Step 2: Lineage transitive depth (ENRICHMENT) ────────────────────

def compute_transitive_depth(producer_node_id, lineage_path, max_depth=2):
    """BFS traversal of lineage graph for transitive contamination depth."""
    try:
        with open(lineage_path) as f:
            lines = [l for l in f if l.strip()]
            snapshot = json.loads(lines[-1])
    except Exception:
        return {"direct": [], "transitive": [], "max_depth": 0}

    visited, frontier, depth_map = set(), {producer_node_id}, {}

    for depth in range(1, max_depth + 1):
        next_frontier = set()
        for node in frontier:
            for edge in snapshot.get("edges", []):
                src = edge.get("source", "")
                tgt = edge.get("target", "")
                if src == node or (node in src):
                    if tgt not in visited:
                        depth_map[tgt] = depth
                        next_frontier.add(tgt)
                        visited.add(tgt)
        frontier = next_frontier

    return {
        "direct": [n for n, d in depth_map.items() if d == 1],
        "transitive": [n for n, d in depth_map.items() if d > 1],
        "max_depth": max(depth_map.values()) if depth_map else 0
    }


# ── Step 3: Git blame ────────────────────────────────────────────────

def find_upstream_files(check_id, lineage_snapshot):
    """Walk lineage graph to find files that produce the failing column."""
    column_system = None
    for w in ["week1", "week2", "week3", "week4", "week5"]:
        if w in check_id:
            column_system = w
            break

    candidates = []
    for node in lineage_snapshot.get("nodes", []):
        nid = node.get("node_id", "")
        if column_system and column_system in nid and node.get("type") == "FILE":
            candidates.append(node.get("metadata", {}).get("path", nid))
        elif node.get("type") == "PIPELINE" and column_system and column_system in nid:
            candidates.append(nid)

    if not candidates:
        for edge in lineage_snapshot.get("edges", []):
            tgt = edge.get("target", "")
            src = edge.get("source", "")
            if column_system and column_system in tgt:
                candidates.append(src)

    return candidates[:5]


def get_recent_commits(file_path, days=14, repo_paths=None):
    """Run git log on the file and parse structured output."""
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

    # Fallback: current repo recent commits
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


# ── Step 4: Write violation log ──────────────────────────────────────

def write_violation(check_result, registry_blast, lineage_enrichment, blame_chain,
                    run_timestamp, injected=False):
    """Build a violation log entry with full blast radius schema."""
    entry = {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_result["check_id"],
        "detected_at": run_timestamp,
        "severity": check_result.get("severity", "CRITICAL"),
        "message": check_result.get("message", ""),
        "actual_value": check_result.get("actual_value", ""),
        "expected": check_result.get("expected", ""),
        "blast_radius": {
            "source": "registry" if registry_blast else "lineage",
            "direct_subscribers": registry_blast,
            "transitive_nodes": lineage_enrichment.get("transitive", []),
            "contamination_depth": lineage_enrichment.get("max_depth", 0),
            "affected_pipelines": [s["subscriber_id"] for s in registry_blast],
            "estimated_records": check_result.get("records_failing", 0),
            "note": "direct_subscribers from registry; transitive_nodes from lineage graph enrichment"
        },
        "blame_chain": blame_chain[:5],
        "records_failing": check_result.get("records_failing", 0)
    }

    if injected:
        entry["injection_note"] = True
        entry["injection_type"] = "scale_change"
        entry["injection_description"] = (
            "Deliberately injected violation for contract deployment validation."
        )

    return entry


# ── Main attribution pipeline ────────────────────────────────────────

def attribute_violations(report_path, lineage_path, contract_path, registry_path, injected=False):
    """4-step attribution pipeline: registry -> lineage -> git -> write."""
    with open(report_path) as f:
        report = json.load(f)

    with open(lineage_path) as f:
        lines = [l for l in f if l.strip()]
        snapshot = json.loads(lines[-1])

    contract_id = report.get("contract_id", "unknown")
    run_timestamp = report.get("run_timestamp", datetime.now(timezone.utc).isoformat())

    # Determine week key for lineage traversal
    week_key = None
    for w in ["week1", "week2", "week3", "week4", "week5"]:
        if w in contract_id:
            week_key = w
            break
    producer_node = f"pipeline::{week_key}" if week_key else contract_id

    violations = []
    for result in report.get("results", []):
        if result["status"] not in ("FAIL", "ERROR"):
            continue

        column_name = result.get("column_name", "unknown")
        registry_field = map_column_to_registry_field(column_name)

        # Step 1: Registry blast radius query (PRIMARY)
        print(f"  Step 1: Registry query for {registry_field}...")
        reg_blast = registry_blast_radius(contract_id, registry_field, registry_path)
        print(f"    Found {len(reg_blast)} affected subscribers")

        # Step 2: Lineage traversal for enrichment
        print(f"  Step 2: Lineage BFS from {producer_node}...")
        lineage_enrichment = compute_transitive_depth(producer_node, lineage_path)
        print(f"    Direct: {len(lineage_enrichment['direct'])}, "
              f"Transitive: {len(lineage_enrichment['transitive'])}, "
              f"Max depth: {lineage_enrichment['max_depth']}")

        # Step 3: Git blame
        print(f"  Step 3: Git blame for {result['check_id']}...")
        upstream_files = find_upstream_files(result["check_id"], snapshot)
        all_commits = []
        for fp in upstream_files:
            all_commits.extend(get_recent_commits(fp))
        if not all_commits:
            all_commits = get_recent_commits(".")
        blame_chain = score_candidates(
            all_commits, run_timestamp,
            lineage_distance=lineage_enrichment.get("max_depth", 1)
        )
        print(f"    {len(blame_chain)} blame candidates scored")

        # Step 4: Build violation entry
        print(f"  Step 4: Writing violation record...")
        entry = write_violation(
            result, reg_blast, lineage_enrichment,
            blame_chain, run_timestamp, injected=injected
        )
        violations.append(entry)
        print(f"    Violation {entry['violation_id'][:8]}... severity={entry['severity']}")

    return violations


def main():
    parser = argparse.ArgumentParser(description="ViolationAttributor (4-Step Pipeline)")
    parser.add_argument("--violation", required=True, help="Path to validation report JSON")
    parser.add_argument("--lineage", required=True, help="Path to lineage snapshots JSONL")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--registry", default="contract_registry/subscriptions.yaml",
                        help="Path to contract registry subscriptions YAML")
    parser.add_argument("--injected", action="store_true",
                        help="Mark violations as intentionally injected")
    parser.add_argument("--output", required=True, help="Output path for violations JSONL")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"ViolationAttributor — 4-Step Pipeline")
    print(f"{'='*60}")

    violations = attribute_violations(
        args.violation, args.lineage, args.contract,
        args.registry, injected=args.injected
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "a") as f:
        for v in violations:
            f.write(json.dumps(v) + "\n")

    print(f"\n  Attributed {len(violations)} violations")
    for v in violations:
        src = v["blast_radius"]["source"]
        subs = len(v["blast_radius"]["direct_subscribers"])
        depth = v["blast_radius"]["contamination_depth"]
        print(f"  {v['check_id']}: severity={v['severity']}, "
              f"blast_radius.source={src}, subscribers={subs}, depth={depth}")
        if v["blame_chain"]:
            top = v["blame_chain"][0]
            print(f"     Top suspect: {top.get('author', 'unknown')} "
                  f"(commit {top.get('commit_hash', '?')[:8]}..., "
                  f"confidence={top.get('confidence_score', 0)})")

    print(f"\n  Violations appended to {output_path}")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
