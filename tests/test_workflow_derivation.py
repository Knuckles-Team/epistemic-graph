"""Parsed workflows derive stable identities and governance-carrying graph rows."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.workflow_derivation import (
    workflow_content_hash,
    workflow_properties,
    workflow_step_properties,
)

pytestmark = pytest.mark.no_engine


def _parsed() -> dict:
    return {
        "name": "Ship",
        "description": "Ship safely",
        "domain": "delivery",
        "tags": ["release"],
        "specialist_ids": ["agent:release"],
        "source_ref": "skill://ship",
        "steps": [
            {
                "step": 1,
                "component": "review",
                "skill_name": "review",
                "depends_on": [],
                "tools": ["git"],
                "kind": "gate",
                "condition": "on_success",
                "on_reject": "step 1",
            }
        ],
    }


def test_workflow_hash_tracks_execution_semantics_and_ignores_map_order() -> None:
    parsed = _parsed()
    baseline = workflow_content_hash(parsed)
    assert baseline == workflow_content_hash(dict(reversed(list(parsed.items()))))
    changed = _parsed()
    changed["steps"][0]["condition"] = "always"
    assert baseline != workflow_content_hash(changed)


def test_graph_properties_include_governance_and_resolved_gate_fields() -> None:
    parsed = _parsed()
    governance = {"tenant_id": "tenant-1", "classification": "internal"}
    digest = workflow_content_hash(parsed)
    definition = workflow_properties(
        parsed, governance, content_hash=digest, timestamp="2026-09-26T00:00:00Z"
    )
    step = workflow_step_properties(
        parsed["steps"][0], "workflow:ship:step:1", governance, [], "step:reject"
    )
    assert definition["content_hash"] == digest
    assert definition["source_ref"] == "skill://ship"
    assert definition["tenant_id"] == step["tenant_id"] == "tenant-1"
    assert step["kind"] == "gate"
    assert step["on_reject"] == "step:reject"
    assert "node_id" not in step
