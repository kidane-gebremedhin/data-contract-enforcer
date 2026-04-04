# Spec 05: ViolationAttributor Registry-First Blast Radius (4-Step Pipeline)

## Problem

The project document mandates: "Use the ContractRegistry as the primary blast radius source and the lineage graph as an enrichment source. This is the correct Tier 1 model."

Currently, `contracts/attributor.py`:
- Computes blast radius from `contract.lineage.downstream[]` only (lineage-graph-derived)
- Does not load `contract_registry/subscriptions.yaml`
- Does not distinguish registry-sourced vs lineage-enriched blast radius
- Does not compute transitive contamination depth

The attribution pipeline must run in 4 steps per the project document:
1. Registry blast radius query (primary source)
2. Lineage traversal for enrichment (transitive depth)
3. Git blame for cause attribution
4. Write violation log

## Changes Required

### 1. Import and use `contracts.registry`

**File:** `contracts/attributor.py`

```python
from contracts.registry import load_registry, get_breaking_field_subscribers
```

### 2. Implement Step 1: Registry Blast Radius Query

Replace `compute_blast_radius()` with registry-first logic:

```python
def registry_blast_radius(contract_id, failing_field, registry_path):
    """Step 1: Query registry for affected subscribers. PRIMARY blast radius source."""
    registry = load_registry(registry_path)
    affected = get_breaking_field_subscribers(registry, contract_id, failing_field)
    return affected
```

The `failing_field` is extracted from the check_id. For example:
- `check_id = "week3-document-refinery-extractions.fact_confidence.range"`
- `failing_field = "fact_confidence"` (but must also check as `extracted_facts.confidence` since registry uses dot notation)

Add a field name mapper:

```python
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
```

### 3. Implement Step 2: Lineage Transitive Depth (Enrichment)

```python
def compute_transitive_depth(producer_node_id, lineage_path, max_depth=2):
    """Step 2: BFS traversal of lineage graph for transitive contamination.
    
    This is ENRICHMENT, not the primary blast radius source.
    """
    with open(lineage_path) as f:
        lines = [l for l in f if l.strip()]
        snapshot = json.loads(lines[-1])
    
    visited, frontier, depth_map = set(), {producer_node_id}, {}
    
    for depth in range(1, max_depth + 1):
        next_frontier = set()
        for node in frontier:
            for edge in snapshot.get("edges", []):
                if (edge["source"] == node and 
                    edge.get("relationship") in ("PRODUCES", "WRITES", "CONSUMES")):
                    target = edge["target"]
                    if target not in visited:
                        depth_map[target] = depth
                        next_frontier.add(target)
                        visited.add(target)
        frontier = next_frontier
    
    return {
        "direct": [n for n, d in depth_map.items() if d == 1],
        "transitive": [n for n, d in depth_map.items() if d > 1],
        "max_depth": max(depth_map.values()) if depth_map else 0
    }
```

### 4. Implement Step 3: Git Blame (existing, minor updates)

The existing `get_recent_commits()` and `score_candidates()` are adequate. Minor update:
- Accept `lineage_distance` from the transitive depth computation

### 5. Implement Step 4: Write Violation Log with Full Schema

Update the violation record output to match the compounding architecture schema:

```python
def write_violation(check_result, registry_blast, lineage_enrichment, blame_chain, 
                    out_path, injected=False):
    """Step 4: Write the violation log entry with full blast radius."""
    entry = {
        "violation_id": str(uuid.uuid4()),
        "check_id": check_result["check_id"],
        "detected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
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
    
    with open(out_path, "a") as f:
        f.write(json.dumps(entry) + "\n")
    
    return entry
```

### 6. Refactor `attribute_violations()` to Use 4-Step Pipeline

```python
def attribute_violations(report_path, lineage_path, contract_path, registry_path, injected=False):
    """Main attribution: 4-step pipeline per project architecture."""
    with open(report_path) as f:
        report = json.load(f)
    
    contract_id = report.get("contract_id", "unknown")
    violations = []
    
    for result in report.get("results", []):
        if result["status"] not in ("FAIL", "ERROR"):
            continue
        
        column_name = result.get("column_name", "unknown")
        registry_field = map_column_to_registry_field(column_name)
        
        # Step 1: Registry blast radius query (PRIMARY)
        registry_blast = registry_blast_radius(contract_id, registry_field, registry_path)
        
        # Step 2: Lineage traversal for enrichment
        # Find producer node in lineage for this contract
        week_key = None
        for w in ["week1", "week2", "week3", "week4", "week5"]:
            if w in contract_id:
                week_key = w
                break
        producer_node = f"pipeline::{week_key}" if week_key else contract_id
        lineage_enrichment = compute_transitive_depth(producer_node, lineage_path)
        
        # Step 3: Git blame for cause attribution
        upstream_files = find_upstream_files(result["check_id"], ...)
        commits = []
        for fp in upstream_files:
            commits.extend(get_recent_commits(fp))
        if not commits:
            commits = get_recent_commits(".")
        blame_chain = score_candidates(
            commits, report.get("run_timestamp", ""),
            lineage_distance=lineage_enrichment.get("max_depth", 1)
        )
        
        # Step 4: Write violation log
        entry = write_violation(
            result, registry_blast, lineage_enrichment,
            blame_chain, output_path, injected=injected
        )
        violations.append(entry)
    
    return violations
```

### 7. Update CLI

```python
parser.add_argument("--registry", default="contract_registry/subscriptions.yaml",
                    help="Path to contract registry subscriptions YAML")
parser.add_argument("--injected", action="store_true",
                    help="Mark violations as intentionally injected")
```

## Files to Modify

| File | Action |
|------|--------|
| `contracts/attributor.py` | MAJOR REFACTOR — 4-step pipeline, registry-first blast radius |
| `contracts/registry.py` | CREATE (see Spec 02) — shared registry utilities |

## Acceptance Criteria

- [ ] `blast_radius.source` is "registry" when registry subscribers found
- [ ] `blast_radius.direct_subscribers` populated from `subscriptions.yaml`
- [ ] `blast_radius.transitive_nodes` populated from lineage graph BFS
- [ ] `blast_radius.contamination_depth` is an integer >= 0
- [ ] `--registry` flag accepted and used
- [ ] `--injected` flag adds `injection_note: true` to violation records
- [ ] Confidence score formula: `1.0 - (days * 0.1) - (lineage_distance * 0.2)`
- [ ] Max 5 blame chain candidates returned
