from __future__ import annotations

import pytest

from epistemic_graph.generated.write_back import (
    SourceChangeSet,
    WriteBackAuthorizationDecision,
    WriteBackAuthorizationMode,
)

pytestmark = pytest.mark.no_engine


def _change_set() -> SourceChangeSet:
    value = SourceChangeSet(
        schema_version=1,
        change_set_id="change-1",
        change_set_digest="00" * 32,
        tenant_id="tenant-1",
        actor=f"principal:sha256:{'a' * 64}",
        purpose="approved maintenance",
        connector_id="connector-1",
        source_instance_id="source-1",
        entity_id="entity-1",
        field_scope=["priority"],
        base_source_version="etag-1",
        desired_patch={"priority": "high"},
        source_of_truth_rule="source_wins_outside_scope",
        field_provenance={"priority": "approval:42"},
        required_capability="ticket:update",
        policy_digest="02" * 32,
        authorization=WriteBackAuthorizationDecision(
            mode=WriteBackAuthorizationMode.PROPOSAL_APPROVAL,
            authorization_ref="approval:42",
            decision_digest="03" * 32,
            input_digest="04" * 32,
            output_digest="05" * 32,
            authorized=True,
        ),
        idempotency_key="idempotency-1",
        expires_at_ms=4_000_000_000_000,
        reconciliation_procedure="read_by_idempotency_and_version",
    )
    return value.model_copy(update={"change_set_digest": value.canonical_digest()})


def test_generated_change_set_matches_rust_digest_vectors() -> None:
    value = _change_set()
    assert value.change_set_digest == (
        "6b8533d62f86e685d1f2649e7ead46c226e037dd66d0b4ea0a7af6460828541f"
    )
    assert value.patch_digest() == (
        "66625421e4f2816d7abad2a3a0cde2c058c0be1260a04cb6c7900b6d48ba0a7e"
    )
    assert value.model_dump(mode="json")["authorization"]["mode"] == (
        "proposal_approval"
    )


def test_generated_string_enum_constructs_from_wire_value() -> None:
    assert (
        WriteBackAuthorizationMode("proposal_approval")
        is WriteBackAuthorizationMode.PROPOSAL_APPROVAL
    )
