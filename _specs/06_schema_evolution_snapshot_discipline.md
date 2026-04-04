# Spec 06: SchemaEvolutionAnalyzer — Schema Snapshot Discipline

## Problem

The project document states: "On every ContractGenerator run, write a timestamped snapshot of the inferred schema to `schema_snapshots/{contract_id}/{timestamp}.yaml`. The SchemaEvolutionAnalyzer diffs consecutive snapshots to detect changes."

Current issues:
1. Only 1 snapshot exists per contract (need 2+ for diffs)
2. The analyzer reports `NO_DIFF_AVAILABLE` because it can't diff
3. No `--since` flag support for temporal filtering
4. No blast radius query against registry in the analyzer
5. Schema snapshots are not indexed (no `schema_snapshots/index.jsonl`)
6. No SHA-256 hash computed for quick diff detection

## Changes Required

### 1. Update Generator to Ensure Snapshot Discipline

**File:** `contracts/generator.py` — `write_snapshot()`

Current: copies the contract YAML to `schema_snapshots/{contract_id}/{timestamp}.yaml`

Updated behavior:
1. Copy contract YAML to snapshot dir (existing)
2. Compute SHA-256 of the `schema` section
3. Append entry to `schema_snapshots/index.jsonl`

```python
def write_snapshot(contract, contract_id, output_dir):
    """Write timestamped schema snapshot with index entry."""
    snapshot_dir = Path("schema_snapshots") / contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"{ts}.yaml"
    
    # Write the snapshot
    output_path = Path(output_dir) / f"{contract_id.replace('-', '_')}.yaml"
    if output_path.exists():
        shutil.copy(output_path, snapshot_path)
        print(f"  Schema snapshot saved to {snapshot_path}")
    
    # Compute schema hash for quick diff detection
    schema_str = yaml.dump(contract.get("schema", {}), sort_keys=True)
    schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()
    
    # Append to index
    index_path = Path("schema_snapshots") / "index.jsonl"
    index_entry = {
        "contract_id": contract_id,
        "snapshot_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "snapshot_path": str(snapshot_path),
        "schema_hash": schema_hash
    }
    with open(index_path, "a") as f:
        f.write(json.dumps(index_entry) + "\n")
    print(f"  Schema index updated: {index_path}")
```

### 2. Add `--since` Flag to SchemaEvolutionAnalyzer

**File:** `contracts/schema_analyzer.py`

```python
parser.add_argument("--since", default=None,
                    help="Only analyze snapshots newer than this (e.g., '7 days ago')")
```

Filter snapshots by timestamp when `--since` is provided:

```python
def filter_snapshots_by_time(snapshots, since_str):
    """Filter snapshots to only those after the given time."""
    if not since_str:
        return snapshots
    
    # Parse relative time strings like "7 days ago"
    import re
    match = re.match(r"(\d+)\s+(day|hour|minute)s?\s+ago", since_str)
    if match:
        amount, unit = int(match.group(1)), match.group(2)
        delta = timedelta(**{f"{unit}s": amount})
        cutoff = datetime.now(timezone.utc) - delta
    else:
        cutoff = datetime.fromisoformat(since_str)
    
    return [s for s in snapshots 
            if parse_snapshot_timestamp(s["timestamp"]) >= cutoff]
```

### 3. Add Registry-Based Blast Radius to Migration Impact

**File:** `contracts/schema_analyzer.py` — `generate_migration_impact()`

Currently uses `contract.lineage.downstream[]` for blast radius. Update to query registry:

```python
def generate_migration_impact(contract_id, changes, old_contract, new_contract, registry_path=None):
    """Generate migration impact report with registry blast radius."""
    breaking = [c for c in changes if c["classification"] == "BREAKING"]
    
    # Primary: registry subscribers
    registry_subscribers = []
    if registry_path and Path(registry_path).exists():
        from contracts.registry import load_registry, get_subscribers
        registry = load_registry(registry_path)
        registry_subscribers = get_subscribers(registry, contract_id)
    
    # Enrichment: lineage downstream
    downstream = new_contract.get("lineage", {}).get("downstream", [])
    
    impact = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_changes": len(changes),
        "breaking_changes": len(breaking),
        "compatible_changes": len(changes) - len(breaking),
        "overall_verdict": "BREAKING" if breaking else "COMPATIBLE",
        "changes": changes,
        "blast_radius": {
            "source": "registry" if registry_subscribers else "lineage",
            "registry_subscribers": [
                {
                    "subscriber_id": s["subscriber_id"],
                    "contact": s.get("contact", "unknown"),
                    "validation_mode": s.get("validation_mode", "AUDIT"),
                    "breaking_fields_affected": [
                        bf["field"] for bf in s.get("breaking_fields", [])
                        if any(bc["field"] == bf["field"] for bc in breaking)
                    ]
                }
                for s in registry_subscribers
            ],
            "lineage_downstream": [d.get("id", "unknown") for d in downstream],
            "consumer_count": len(registry_subscribers) or len(downstream)
        },
        "migration_checklist": [],
        "rollback_plan": []
    }
    
    # Build per-consumer migration checklist
    for i, bc in enumerate(breaking, 1):
        impact["migration_checklist"].append(
            f"{i}. [{bc['classification']}] Field '{bc['field']}': {bc['description']}. "
            f"Notify {len(registry_subscribers)} registered subscribers."
        )
        impact["rollback_plan"].append(
            f"{i}. Revert field '{bc['field']}' to previous state: "
            f"{json.dumps(bc['old_value'], default=str)}"
        )
    
    if not breaking:
        impact["migration_checklist"].append("No breaking changes. Safe to deploy.")
        impact["rollback_plan"].append("No rollback needed.")
    
    return impact
```

### 4. Add `--registry` Flag to Analyzer

```python
parser.add_argument("--registry", default="contract_registry/subscriptions.yaml",
                    help="Path to contract registry for blast radius computation")
```

### 5. Quick Diff via Schema Hash

Before doing a full diff, check `schema_hash` from the index:

```python
def quick_diff_check(contract_id):
    """Check if schema changed via hash comparison (O(1) instead of O(n))."""
    index_path = Path("schema_snapshots") / "index.jsonl"
    if not index_path.exists():
        return None  # can't determine
    
    entries = []
    with open(index_path) as f:
        for line in f:
            entry = json.loads(line)
            if entry["contract_id"] == contract_id:
                entries.append(entry)
    
    if len(entries) < 2:
        return None
    
    # Compare last two hashes
    return entries[-1]["schema_hash"] != entries[-2]["schema_hash"]
```

## Files to Modify

| File | Action |
|------|--------|
| `contracts/generator.py` | MODIFY — compute schema hash, write to index.jsonl |
| `contracts/schema_analyzer.py` | MODIFY — add `--since`, `--registry`, quick diff, registry blast radius |

## Acceptance Criteria

- [ ] Each generator run appends to `schema_snapshots/index.jsonl`
- [ ] Index entries include `schema_hash` (SHA-256 of schema section)
- [ ] `--since "7 days ago"` filters snapshots by time
- [ ] `--registry` flag wires registry into blast radius of migration impact report
- [ ] `blast_radius` in migration report includes `registry_subscribers`
- [ ] Quick diff via hash works before full field-by-field comparison
