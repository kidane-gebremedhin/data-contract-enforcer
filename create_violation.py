#!/usr/bin/env python3
"""Inject a known violation: change confidence from 0.0-1.0 to 0-100 scale."""

import json

records = []
with open("outputs/week3/extractions.jsonl") as f:
    for line in f:
        r = json.loads(line)
        for fact in r.get("extracted_facts", []):
            fact["confidence"] = round(fact["confidence"] * 100, 1)
        r["doc_id"] = int(round(fact["confidence"] * 100, 0))
        records.append(r)

with open("outputs/week3/extractions_violated.jsonl", "w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")

print(f"INJECTION: confidence scale changed from 0.0-1.0 to 0-100, data_type of doc_id changed from string to integer in {len(records)} records")
print(f"Output: outputs/week3/extractions_violated.jsonl")
