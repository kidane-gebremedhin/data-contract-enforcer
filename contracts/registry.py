"""ContractRegistry: Loads and queries contract_registry/subscriptions.yaml."""

import yaml
from pathlib import Path

DEFAULT_REGISTRY_PATH = "contract_registry/subscriptions.yaml"


def load_registry(registry_path=DEFAULT_REGISTRY_PATH):
    """Load the contract registry YAML."""
    with open(registry_path) as f:
        return yaml.safe_load(f)


def get_subscribers(registry, contract_id):
    """Get all subscribers for a given contract_id."""
    return [s for s in registry.get("subscriptions", [])
            if s["contract_id"] == contract_id]


def get_breaking_field_subscribers(registry, contract_id, failing_field):
    """Get subscribers whose breaking_fields match the failing field.

    This is the primary blast radius query (Step 1 of attribution pipeline).
    Supports prefix matching for dot-notation fields (e.g., failing_field
    'extracted_facts.confidence.min' matches breaking_field 'extracted_facts.confidence').
    """
    affected = []
    for sub in registry.get("subscriptions", []):
        if sub["contract_id"] != contract_id:
            continue
        for bf in sub.get("breaking_fields", []):
            if bf["field"] == failing_field or failing_field.startswith(bf["field"]):
                affected.append({
                    "subscriber_id": sub["subscriber_id"],
                    "subscriber_team": sub.get("subscriber_team", "unknown"),
                    "contact": sub.get("contact", "unknown"),
                    "validation_mode": sub.get("validation_mode", "AUDIT"),
                    "reason": bf["reason"],
                    "fields_consumed": sub.get("fields_consumed", [])
                })
                break
    return affected
