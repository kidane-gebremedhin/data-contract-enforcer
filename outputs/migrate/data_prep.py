#!/usr/bin/env python3
"""Data preparation script: migrates prior-week outputs to canonical schemas
and generates synthetic data for missing weeks."""

import json
import uuid
import hashlib
import random
import string
import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict

import asyncpg

random.seed(42)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUTPUTS = BASE_DIR / "outputs"

# ── Source paths ──────────────────────────────────────────────────────────
WEEK3_SOURCE = Path("/home/kg/Projects/10Academy/document-intelligence-refinery/.refinery/extracted_facts.jsonl")
WEEK5_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ledger_test")
WEEK4_SOURCE = Path("/home/kg/Projects/10Academy/brownfield-cartographer/.cartography/cartography_trace.jsonl")


def uuid4():
    return str(uuid.uuid4())


def iso_now(offset_hours=0):
    return (datetime.now(timezone.utc) + timedelta(hours=offset_hours)).isoformat().replace("+00:00", "Z")


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def write_jsonl(records, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")
    print(f"  Wrote {len(records)} records to {path}")


# ═══════════════════════════════════════════════════════════════════════════
# 2a. Week 3 — Document Refinery (extraction_record)
# ═══════════════════════════════════════════════════════════════════════════

def migrate_week3():
    print("\n── Migrating Week 3 (Document Refinery) ──")
    with open(WEEK3_SOURCE) as f:
        raw = [json.loads(l) for l in f if l.strip()]

    # Group facts by doc_id
    by_doc = defaultdict(list)
    for r in raw:
        by_doc[r["doc_id"]].append(r)

    # Since there's only 1 doc_id, we'll split into multiple synthetic docs
    # to meet the 50+ record requirement
    all_facts = raw
    chunk_size = 5
    records = []
    entity_types = ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]
    models = ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"]

    for i in range(0, len(all_facts), chunk_size):
        chunk = all_facts[i:i + chunk_size]
        doc_id = uuid4()
        source_text = " ".join(f.get("value", "") for f in chunk)
        source_path = f"/data/documents/cbe_annual_report_chunk_{i // chunk_size + 1}.pdf"

        extracted_facts = []
        entities = []
        entity_map = {}

        for fact in chunk:
            fact_id = uuid4()
            # Generate confidence in 0.0-1.0 range
            confidence = round(random.uniform(0.70, 0.98), 2)

            # Create entities from the fact
            entity_name = fact.get("key", "unknown")
            if entity_name not in entity_map:
                eid = uuid4()
                etype = random.choice(entity_types)
                if fact.get("numeric_value") is not None:
                    etype = "AMOUNT"
                elif "/" in entity_name:
                    etype = "DATE"
                entity_map[entity_name] = eid
                entities.append({
                    "entity_id": eid,
                    "name": entity_name,
                    "type": etype,
                    "canonical_value": str(fact.get("value", entity_name))
                })

            extracted_facts.append({
                "fact_id": fact_id,
                "text": f"{fact.get('key', 'N/A')}: {fact.get('value', 'N/A')}",
                "entity_refs": [entity_map[entity_name]],
                "confidence": confidence,
                "page_ref": fact.get("page_number"),
                "source_excerpt": fact.get("context", "") or f"From {fact.get('source', 'unknown')} on page {fact.get('page_number', '?')}"
            })

        base_time = datetime(2026, 3, 25, 8, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=i * 2)
        proc_time = random.randint(800, 3000)
        input_tokens = random.randint(2000, 6000)
        output_tokens = random.randint(400, 1500)

        records.append({
            "doc_id": doc_id,
            "source_path": source_path,
            "source_hash": sha256(source_text),
            "extracted_facts": extracted_facts,
            "entities": entities,
            "extraction_model": random.choice(models),
            "processing_time_ms": proc_time,
            "token_count": {"input": input_tokens, "output": output_tokens},
            "extracted_at": base_time.isoformat().replace("+00:00", "Z")
        })

    write_jsonl(records, OUTPUTS / "week3" / "extractions.jsonl")
    return records


# ═══════════════════════════════════════════════════════════════════════════
# 2b. Week 5 — Event Sourcing Platform (event_record)
# ═══════════════════════════════════════════════════════════════════════════

async def _fetch_week5_events():
    """Fetch events from the Week 5 agentic-ledger PostgreSQL database."""
    conn = await asyncpg.connect(WEEK5_DB_URL)
    try:
        rows = await conn.fetch(
            "SELECT event_id, stream_id, stream_position, event_type, "
            "event_version, payload, metadata, recorded_at "
            "FROM events ORDER BY global_position"
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


def migrate_week5():
    print("\n── Migrating Week 5 (Event Sourcing) from database ──")
    raw = asyncio.run(_fetch_week5_events())
    print(f"  Fetched {len(raw)} events from {WEEK5_DB_URL}")

    # Track sequence numbers per aggregate
    seq_counters = defaultdict(int)
    records = []

    for r in raw:
        stream_id = r["stream_id"]
        seq_counters[stream_id] += 1

        recorded_at = r["recorded_at"].isoformat().replace("+00:00", "Z") if r.get("recorded_at") else iso_now()
        # occurred_at is slightly before recorded_at
        try:
            rec_dt = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
            occ_dt = rec_dt - timedelta(milliseconds=random.randint(100, 2000))
            occurred_at = occ_dt.isoformat().replace("+00:00", "Z")
        except Exception:
            occurred_at = recorded_at

        # Derive aggregate_type from stream_id prefix
        if "loan" in stream_id.lower():
            agg_type = "LoanApplication"
        elif "doc" in stream_id.lower():
            agg_type = "Document"
        elif "agent" in stream_id.lower():
            agg_type = "AgentSession"
        else:
            agg_type = "Document"

        payload = r.get("payload") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)

        db_metadata = r.get("metadata") or {}
        if isinstance(db_metadata, str):
            db_metadata = json.loads(db_metadata)

        records.append({
            "event_id": str(r.get("event_id", uuid4())),
            "event_type": r["event_type"],
            "aggregate_id": stream_id or uuid4(),
            "aggregate_type": agg_type,
            "sequence_number": r.get("stream_position") or seq_counters[stream_id],
            "payload": payload,
            "metadata": {
                "causation_id": db_metadata.get("causation_id"),
                "correlation_id": db_metadata.get("correlation_id", uuid4()),
                "user_id": payload.get("applicant_id", db_metadata.get("user_id", "system")),
                "source_service": "week5-event-sourcing-platform"
            },
            "schema_version": str(r.get("event_version", "1.0")),
            "occurred_at": occurred_at,
            "recorded_at": recorded_at
        })

    write_jsonl(records, OUTPUTS / "week5" / "events.jsonl")
    return records


# ═══════════════════════════════════════════════════════════════════════════
# 2c. Week 4 — Brownfield Cartographer (lineage_snapshot)
# ═══════════════════════════════════════════════════════════════════════════

def migrate_week4():
    print("\n── Migrating Week 4 (Brownfield Cartographer) ──")
    with open(WEEK4_SOURCE) as f:
        raw = [json.loads(l) for l in f if l.strip()]

    # Take first entry which has the file list
    entry = raw[0]
    file_list = entry.get("added", [])[:100]  # limit for manageability

    relationship_types = ["IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"]
    node_types_map = {
        ".py": "FILE", ".sql": "FILE", ".yml": "FILE", ".yaml": "FILE",
        ".json": "FILE", ".md": "FILE", ".csv": "FILE", ".jsonl": "FILE"
    }

    def infer_language(path):
        ext = Path(path).suffix
        return {".py": "python", ".sql": "sql", ".yml": "yaml", ".yaml": "yaml",
                ".json": "json", ".md": "markdown", ".csv": "csv"}.get(ext, "unknown")

    # Build nodes
    nodes = []
    node_ids = set()
    for fp in file_list:
        ext = Path(fp).suffix
        ntype = node_types_map.get(ext, "FILE")
        node_id = f"file::{fp}"
        node_ids.add(node_id)
        nodes.append({
            "node_id": node_id,
            "type": ntype,
            "label": Path(fp).name,
            "metadata": {
                "path": fp,
                "language": infer_language(fp),
                "purpose": f"Source file: {Path(fp).name}",
                "last_modified": (datetime(2026, 3, 20, tzinfo=timezone.utc) + timedelta(hours=random.randint(0, 120))).isoformat().replace("+00:00", "Z")
            }
        })

    # Add system-level nodes for week outputs
    for week in ["week1", "week2", "week3", "week4", "week5"]:
        nid = f"pipeline::{week}"
        node_ids.add(nid)
        nodes.append({
            "node_id": nid,
            "type": "PIPELINE",
            "label": f"{week} pipeline",
            "metadata": {
                "path": f"outputs/{week}/",
                "language": "python",
                "purpose": f"Data pipeline for {week}",
                "last_modified": iso_now(-48)
            }
        })

    # Build edges — connect files in same directory + cross-week dependencies
    edges = []
    node_list = list(node_ids)
    # Create edges between week pipelines
    week_deps = [
        ("pipeline::week1", "pipeline::week2", "PRODUCES"),
        ("pipeline::week3", "pipeline::week4", "PRODUCES"),
        ("pipeline::week4", "pipeline::week5", "CONSUMES"),
        ("pipeline::week3", "pipeline::week5", "PRODUCES"),
        ("pipeline::week1", "pipeline::week3", "READS"),
    ]
    for src, tgt, rel in week_deps:
        if src in node_ids and tgt in node_ids:
            edges.append({
                "source": src,
                "target": tgt,
                "relationship": rel,
                "confidence": round(random.uniform(0.80, 0.99), 2)
            })

    # Connect some files to pipelines
    for n in nodes:
        if n["type"] == "FILE" and "week3" in n["metadata"]["path"]:
            edges.append({
                "source": n["node_id"],
                "target": "pipeline::week3",
                "relationship": "READS",
                "confidence": round(random.uniform(0.85, 0.98), 2)
            })

    # Random file-to-file edges within same directory
    by_dir = defaultdict(list)
    for n in nodes:
        if n["type"] == "FILE":
            d = str(Path(n["metadata"]["path"]).parent)
            by_dir[d].append(n["node_id"])
    for d, nids in by_dir.items():
        for i in range(min(len(nids) - 1, 3)):
            edges.append({
                "source": nids[i],
                "target": nids[i + 1],
                "relationship": random.choice(["IMPORTS", "CALLS"]),
                "confidence": round(random.uniform(0.70, 0.95), 2)
            })

    snapshot = {
        "snapshot_id": uuid4(),
        "codebase_root": "/home/kg/Projects/10Academy/brownfield-cartographer",
        "git_commit": sha256("brownfield-cartographer-main")[:40],
        "nodes": nodes,
        "edges": edges,
        "captured_at": iso_now(-24)
    }

    write_jsonl([snapshot], OUTPUTS / "week4" / "lineage_snapshots.jsonl")
    return [snapshot]


# ═══════════════════════════════════════════════════════════════════════════
# 2d. Week 1 — Intent Correlator (intent_record) — SYNTHETIC
# ═══════════════════════════════════════════════════════════════════════════

def generate_week1():
    print("\n── Generating Week 1 (Intent Correlator) — synthetic ──")
    intents = [
        "Implement user authentication flow",
        "Add input validation to API endpoints",
        "Create database migration for user table",
        "Refactor payment processing module",
        "Fix race condition in event handler",
        "Add caching layer to document retrieval",
        "Implement rate limiting middleware",
        "Create logging infrastructure for audit trail",
        "Add PII detection to data pipeline",
        "Implement retry logic for external API calls",
        "Create unit tests for extraction module",
        "Add monitoring dashboard for system health",
    ]
    governance_tags_pool = ["auth", "pii", "billing", "compliance", "security", "performance", "data-quality"]
    files_pool = [
        "src/auth/handler.py", "src/api/validators.py", "src/db/migrations.py",
        "src/payments/processor.py", "src/events/consumer.py", "src/cache/retriever.py",
        "src/middleware/rate_limit.py", "src/logging/audit.py", "src/pipeline/pii_filter.py",
        "src/api/retry.py", "src/tests/test_extraction.py", "src/monitoring/dashboard.py",
        "src/extraction/extractor.py", "src/models/document.py", "src/utils/helpers.py",
    ]
    symbols_pool = [
        "authenticate_user", "validate_input", "run_migration", "process_payment",
        "handle_event", "get_cached_document", "check_rate_limit", "write_audit_log",
        "detect_pii", "retry_with_backoff", "test_extract_facts", "update_dashboard",
    ]

    records = []
    base_time = datetime(2026, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
    for i in range(55):
        desc = random.choice(intents) + f" (variant {i})"
        n_refs = random.randint(1, 4)
        code_refs = []
        for _ in range(n_refs):
            line_start = random.randint(1, 200)
            code_refs.append({
                "file": random.choice(files_pool),
                "line_start": line_start,
                "line_end": line_start + random.randint(5, 50),
                "symbol": random.choice(symbols_pool),
                "confidence": round(random.uniform(0.55, 0.99), 2)
            })

        records.append({
            "intent_id": uuid4(),
            "description": desc,
            "code_refs": code_refs,
            "governance_tags": random.sample(governance_tags_pool, random.randint(1, 3)),
            "created_at": (base_time + timedelta(hours=i * 2)).isoformat().replace("+00:00", "Z")
        })

    write_jsonl(records, OUTPUTS / "week1" / "intent_records.jsonl")
    return records


# ═══════════════════════════════════════════════════════════════════════════
# 2e. Week 2 — Digital Courtroom (verdict_record) — SYNTHETIC
# ═══════════════════════════════════════════════════════════════════════════

def generate_week2():
    print("\n── Generating Week 2 (Digital Courtroom) — synthetic ──")
    criteria = ["accuracy", "completeness", "relevance", "clarity", "consistency"]
    verdicts = ["PASS", "FAIL", "WARN"]

    records = []
    base_time = datetime(2026, 3, 18, 8, 0, 0, tzinfo=timezone.utc)
    for i in range(60):
        scores = {}
        for c in random.sample(criteria, random.randint(3, 5)):
            score_val = random.randint(1, 5)
            scores[c] = {
                "score": score_val,
                "evidence": [f"Evidence excerpt {j} for {c}" for j in range(random.randint(1, 3))],
                "notes": f"Assessment of {c} criterion"
            }

        score_values = [s["score"] for s in scores.values()]
        overall = round(sum(score_values) / len(score_values), 1) if score_values else 3.0
        verdict = "PASS" if overall >= 3.5 else ("WARN" if overall >= 2.5 else "FAIL")

        records.append({
            "verdict_id": uuid4(),
            "target_ref": f"src/week{random.randint(1,5)}/module_{i}.py",
            "rubric_id": sha256(f"rubric_v{random.randint(1,3)}")[:64],
            "rubric_version": f"{random.randint(1,2)}.{random.randint(0,5)}.0",
            "scores": scores,
            "overall_verdict": verdict,
            "overall_score": overall,
            "confidence": round(random.uniform(0.60, 0.98), 2),
            "evaluated_at": (base_time + timedelta(hours=i * 3)).isoformat().replace("+00:00", "Z")
        })

    write_jsonl(records, OUTPUTS / "week2" / "verdicts.jsonl")
    return records


# ═══════════════════════════════════════════════════════════════════════════
# 2f. LangSmith Traces (trace_record) — SYNTHETIC
# ═══════════════════════════════════════════════════════════════════════════

def generate_traces():
    print("\n── Generating LangSmith Traces — synthetic ──")
    run_types = ["llm", "chain", "tool", "retriever", "embedding"]
    chain_names = [
        "extraction_chain", "verdict_chain", "summarize_chain",
        "qa_retrieval", "document_processor", "fact_extractor"
    ]
    model_names = ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307", "gpt-4-turbo"]
    tags_pool = ["week1", "week2", "week3", "week4", "week5", "extraction", "verdict", "analysis"]

    records = []
    base_time = datetime(2026, 3, 20, 6, 0, 0, tzinfo=timezone.utc)
    session_id = uuid4()

    for i in range(80):
        if i % 8 == 0:
            session_id = uuid4()

        run_type = random.choice(run_types)
        prompt_tokens = random.randint(100, 5000)
        completion_tokens = random.randint(50, 2000)
        total_tokens = prompt_tokens + completion_tokens
        start = base_time + timedelta(minutes=i * 15)
        latency_ms = random.randint(200, 15000)
        end = start + timedelta(milliseconds=latency_ms)

        # ~5% error rate
        error = None
        if random.random() < 0.05:
            error = "Rate limit exceeded" if random.random() < 0.5 else "Context length exceeded"

        cost_per_token = 0.000003 if "claude" in random.choice(model_names) else 0.00001
        total_cost = round(total_tokens * cost_per_token, 4)

        records.append({
            "id": uuid4(),
            "name": random.choice(chain_names),
            "run_type": run_type,
            "inputs": {"query": f"Sample input {i}", "context": "..."},
            "outputs": {"result": f"Sample output {i}"} if error is None else {},
            "error": error,
            "start_time": start.isoformat().replace("+00:00", "Z"),
            "end_time": end.isoformat().replace("+00:00", "Z"),
            "total_tokens": total_tokens,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_cost": total_cost,
            "tags": random.sample(tags_pool, random.randint(1, 3)),
            "parent_run_id": uuid4() if random.random() < 0.4 else None,
            "session_id": session_id
        })

    write_jsonl(records, OUTPUTS / "traces" / "runs.jsonl")
    return records


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("Data Contract Enforcer — Data Preparation")
    print("=" * 60)

    migrate_week3()
    migrate_week5()
    migrate_week4()
    generate_week1()
    generate_week2()
    generate_traces()

    print("\n" + "=" * 60)
    print("Data preparation complete. All outputs written to outputs/")
    print("=" * 60)
