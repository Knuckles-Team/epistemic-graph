"""Runnable skill graph material is deterministic and carries governance."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.runnable_skill_derivation import (
    classify_skill_type,
    runnable_skill_digest,
    runnable_skill_projection,
    skill_reference,
)

pytestmark = pytest.mark.no_engine


def test_runnable_skill_projection_preserves_identity_and_governance() -> None:
    governance = {
        "tenant_id": "tenant-1",
        "classification": "public",
        "external_access": {"is_public": True},
    }
    projection = runnable_skill_projection(
        name="Review Code",
        description="Review changes",
        body="Read the diff.",
        provider="MCP Server",
        disabled=False,
        mcp_server="code-server",
        skill_type="mcp_skill",
        privacy_redactions=1,
        governance=governance,
    )
    skill, resource, provenance = projection.nodes
    assert projection.resource_id == "resource:skill:review-code"
    assert skill[:2] == ("Skill", "skill:review-code")
    assert resource[:2] == ("CallableResource", projection.resource_id)
    assert provenance[:2] == (
        "Provenance",
        f"provenance:skill:{runnable_skill_digest('Read the diff.')}",
    )
    assert all(row[2]["tenant_id"] == "tenant-1" for row in projection.nodes)
    assert skill[2]["source_ref"] == skill_reference("Review Code")
    assert skill[2]["mcp_server"] == "code-server"
    assert resource[2]["system_prompt"] == "Read the diff."
    assert resource[2]["resource_type"] == "AGENT_SKILL"
    assert projection.edges == (
        ("skill:review-code", projection.resource_id, "BINDS_RUNNABLE"),
        ("skill:review-code", provenance[1], "DERIVED_FROM"),
        (projection.resource_id, provenance[1], "DERIVED_FROM"),
    )


def test_unknown_skill_type_defaults_to_atomic_and_local_skill_has_no_server() -> None:
    assert classify_skill_type("unknown") == ("skill", "skill")
    projection = runnable_skill_projection(
        name="Local",
        description="",
        body="Run",
        provider="local",
        disabled=True,
        mcp_server="",
        skill_type="unknown",
        privacy_redactions=0,
        governance={"tenant_id": "tenant-1"},
    )
    assert projection.nodes[0][2]["skill_type"] == "skill"
    assert "mcp_server" not in projection.nodes[0][2]
