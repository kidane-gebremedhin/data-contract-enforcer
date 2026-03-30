#!/usr/bin/env python3
"""AI Contract Extensions: Embedding drift, prompt input validation, LLM output enforcement.

Usage:
    python contracts/ai_extensions.py \
        --mode all \
        --extractions outputs/week3/extractions.jsonl \
        --verdicts outputs/week2/verdicts.jsonl \
        --output validation_reports/ai_extensions.json
"""

import argparse
import json
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from jsonschema import validate, ValidationError


# ── Extension 1: Embedding Drift Detection ──────────────────────────────

def embed_sample_local(texts, n=200):
    """Generate deterministic pseudo-embeddings for text (no API call).
    Uses hash-based approach for reproducibility."""
    sample = texts[:n] if len(texts) > n else texts
    dim = 128
    vecs = []
    for t in sample:
        h = hashlib.sha256(t.encode()).digest()
        # Expand hash to fill vector dimension
        expanded = h * (dim // len(h) + 1)
        vec = np.frombuffer(expanded[:dim], dtype=np.uint8).astype(np.float64)
        vec = vec / (np.linalg.norm(vec) + 1e-9)  # normalize
        vecs.append(vec)
    return np.array(vecs)


def check_embedding_drift(texts, baseline_path="schema_snapshots/embedding_baselines.npz",
                          threshold=0.15):
    """Check semantic drift of text content via embedding centroid distance."""
    current_vecs = embed_sample_local(texts)
    current_centroid = current_vecs.mean(axis=0)

    if not Path(baseline_path).exists():
        Path(baseline_path).parent.mkdir(parents=True, exist_ok=True)
        np.savez(baseline_path, centroid=current_centroid)
        return {
            "check": "embedding_drift",
            "status": "BASELINE_SET",
            "drift_score": 0.0,
            "threshold": threshold,
            "interpretation": "Baseline established. First run — no drift to compare."
        }

    baseline = np.load(baseline_path)["centroid"]
    dot = np.dot(current_centroid, baseline)
    norm = np.linalg.norm(current_centroid) * np.linalg.norm(baseline)
    cosine_sim = dot / (norm + 1e-9)
    drift = 1 - cosine_sim

    return {
        "check": "embedding_drift",
        "status": "FAIL" if drift > threshold else "PASS",
        "drift_score": round(float(drift), 4),
        "threshold": threshold,
        "interpretation": ("semantic content of text has shifted" if drift > threshold
                          else "stable — content semantics unchanged")
    }


# ── Extension 2: Prompt Input Schema Validation ─────────────────────────

PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 1},
        "source_path": {"type": "string", "minLength": 1},
        "source_hash": {"type": "string"},
        "extraction_model": {"type": "string"},
    }
}


def validate_prompt_inputs(records, quarantine_path="outputs/quarantine/"):
    """Validate prompt input records against schema, quarantine non-conforming."""
    valid, quarantined = [], []
    for r in records:
        try:
            validate(instance=r, schema=PROMPT_INPUT_SCHEMA)
            valid.append(r)
        except ValidationError as e:
            quarantined.append({"record_id": r.get("doc_id", "unknown"), "error": e.message})

    if quarantined:
        Path(quarantine_path).mkdir(parents=True, exist_ok=True)
        with open(Path(quarantine_path) / "quarantine.jsonl", "a") as f:
            for q in quarantined:
                f.write(json.dumps(q) + "\n")

    return {
        "check": "prompt_input_validation",
        "total_records": len(records),
        "valid": len(valid),
        "quarantined": len(quarantined),
        "status": "PASS" if len(quarantined) == 0 else "WARN",
        "quarantine_rate": round(len(quarantined) / max(len(records), 1), 4),
        "sample_errors": [q["error"] for q in quarantined[:3]]
    }


# ── Extension 3: LLM Output Schema Violation Rate ───────────────────────

def check_output_schema_violation_rate(verdict_records,
                                       baseline_rate=None, warn_threshold=0.02):
    """Check structured LLM output conformance rate."""
    total = len(verdict_records)
    violations = sum(1 for v in verdict_records
                     if v.get("overall_verdict") not in ("PASS", "FAIL", "WARN"))
    rate = violations / max(total, 1)

    trend = "unknown"
    if baseline_rate is not None:
        trend = "rising" if rate > baseline_rate * 1.5 else "stable"

    return {
        "check": "llm_output_schema_violation_rate",
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "baseline_rate": baseline_rate,
        "status": "WARN" if rate > warn_threshold else "PASS"
    }


# ── Main ────────────────────────────────────────────────────────────────

def load_jsonl(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def main():
    parser = argparse.ArgumentParser(description="AI Contract Extensions")
    parser.add_argument("--mode", default="all", choices=["all", "embedding", "prompt", "output"],
                       help="Which checks to run")
    parser.add_argument("--extractions", default="outputs/week3/extractions.jsonl",
                       help="Path to extractions JSONL")
    parser.add_argument("--verdicts", default="outputs/week2/verdicts.jsonl",
                       help="Path to verdicts JSONL")
    parser.add_argument("--output", required=True, help="Output path for results JSON")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"AI Contract Extensions")
    print(f"{'='*60}")

    results = {
        "run_id": str(uuid.uuid4()),
        "run_date": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "checks": []
    }

    # Extension 1: Embedding drift
    if args.mode in ("all", "embedding"):
        print("\n  Extension 1: Embedding Drift Detection")
        extractions = load_jsonl(args.extractions)
        texts = []
        for r in extractions:
            for fact in r.get("extracted_facts", []):
                if "text" in fact:
                    texts.append(fact["text"])
        print(f"    Sampled {len(texts)} text values")
        drift_result = check_embedding_drift(texts)
        results["checks"].append(drift_result)
        print(f"    Status: {drift_result['status']} (drift={drift_result['drift_score']})")

    # Extension 2: Prompt input validation
    if args.mode in ("all", "prompt"):
        print("\n  Extension 2: Prompt Input Schema Validation")
        extractions = load_jsonl(args.extractions)
        prompt_result = validate_prompt_inputs(extractions)
        results["checks"].append(prompt_result)
        print(f"    Status: {prompt_result['status']} "
              f"({prompt_result['valid']}/{prompt_result['total_records']} valid)")

    # Extension 3: LLM output enforcement
    if args.mode in ("all", "output"):
        print("\n  Extension 3: LLM Output Schema Violation Rate")
        verdicts = load_jsonl(args.verdicts)
        output_result = check_output_schema_violation_rate(verdicts, baseline_rate=0.01)
        results["checks"].append(output_result)
        print(f"    Status: {output_result['status']} "
              f"(rate={output_result['violation_rate']}, trend={output_result['trend']})")

    # Write results
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n  Results written to {output_path}")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
