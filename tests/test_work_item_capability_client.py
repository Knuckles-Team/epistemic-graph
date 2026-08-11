"""Pure wire-shape tests for the native WorkItem capability client surface."""

from __future__ import annotations

import pytest

from epistemic_graph.client import WorkItemClient, _work_item_capability_result

pytestmark = pytest.mark.no_engine


class _FakeGraphClient:
    def __init__(self, result: dict[str, object]) -> None:
        self.result = result
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def _send(self, method: str, params: dict[str, object]) -> dict[str, object]:
        self.calls.append((method, params))
        return self.result


@pytest.mark.asyncio
async def test_mint_and_verify_bind_only_opaque_wire_fields() -> None:
    mint_transport = _FakeGraphClient(
        {
            "schema_version": "1",
            "decision": "minted",
            "valid": True,
            "capability": b"WIC1" + b"n" * 32,
        }
    )
    client = WorkItemClient(mint_transport)  # type: ignore[arg-type]
    minted = await client.mint_capability(
        {"schema_version": "1", "work_item_id": "work-1"}
    )
    assert minted["decision"] == "minted"
    assert mint_transport.calls == [
        (
            "MintWorkItemClaimCapability",
            {"request": {"schema_version": "1", "work_item_id": "work-1"}},
        )
    ]
    assert "tenant" not in mint_transport.calls[0][1]["request"]
    assert "lease_epoch" not in mint_transport.calls[0][1]["request"]

    verify_transport = _FakeGraphClient(
        {
            "schema_version": "1",
            "decision": "unauthorized",
            "valid": False,
            "capability": None,
        }
    )
    verifier = WorkItemClient(verify_transport)  # type: ignore[arg-type]
    denied = await verifier.verify_capability(
        {
            "schema_version": "1",
            "work_item_id": "work-1",
            "capability": b"WIC1" + b"n" * 32,
        }
    )
    assert denied == {
        "schema_version": "1",
        "decision": "unauthorized",
        "valid": False,
        "capability": None,
    }


def test_capability_result_rejects_unknown_fields_and_authority_projection() -> None:
    with pytest.raises(ValueError, match="unsupported fields"):
        _work_item_capability_result(
            {
                "schema_version": "1",
                "decision": "verified",
                "valid": True,
                "capability": None,
                "lease_epoch": 1,
            },
            verify=True,
        )
    with pytest.raises(ValueError, match="must not return capability"):
        _work_item_capability_result(
            {
                "schema_version": "1",
                "decision": "verified",
                "valid": True,
                "capability": b"WIC1" + b"n" * 32,
            },
            verify=True,
        )


@pytest.mark.asyncio
async def test_capability_request_does_not_accept_public_authority_tuple() -> None:
    client = WorkItemClient(_FakeGraphClient({}))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unsupported fields"):
        await client.mint_capability(
            {
                "schema_version": "1",
                "work_item_id": "work-1",
                "lease_epoch": 1,
            }
        )
