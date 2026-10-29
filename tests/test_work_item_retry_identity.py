"""WorkItem retry keys must reach the authenticated wire envelope."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import msgpack
import pytest
from _client_fixtures import unused_reader, unused_writer

from epistemic_graph.client import EpistemicGraphClient

pytestmark = pytest.mark.no_engine


def _context() -> dict[str, object]:
    return {
        "principal": "subject-opaque",
        "tenant": "tenant-fixture",
        "audience": "engine-fixture",
        "agent_id": "agent-fixture",
        "roles": ["worker"],
        "scopes": ["work:*"],
        "policy_version": "policy-v1",
        "delegation": ["subject-opaque", "agent-fixture"],
    }


def _submit_request() -> dict[str, object]:
    return {
        "schema_version": "1",
        # A decodable RequestContext v2: the signer now derives the eg2. body
        # from the engine's own decoder, so the fixture must be a real request.
        "context": {
            "schema_version": "2",
            "request_id": "request-1",
            "subject_id": "subject-opaque",
            "tenant_id": "tenant-fixture",
            "agent_id": "agent-fixture",
            "scopes": [],
            "audience": "engine-fixture",
            "authentication_method": "local_process",
            "policy_version": "policy-v1",
            "graph": "graph-fixture",
            "placement_epoch": None,
            "trace_id": "trace-1",
            "issued_at_ms": 0,
            "expires_at_ms": 1,
        },
        "work_item_id": "work-1",
        "idempotency_key": "submit-retry-1",
        "command_digest": "a" * 64,
        "kind": "test",
        "priority": 0,
        "depends_on": [],
        "input_ref": "input:1",
        "policy_digest": "policy:1",
        "catalog_digest": "catalog:1",
        "model_digest": "model:1",
        "max_attempts": 1,
        "deadline_unix": None,
        "metadata": {},
        "provenance_refs": [],
        "max_tenant_in_flight": 1,
    }


def _claim_request() -> dict[str, object]:
    return {
        "schema_version": "1",
        "tenant_ref": "tenant-fixture",
        "work_item_id": None,
        "queue_ref": None,
        "resource_class": None,
        "fairness_group": None,
        "worker_ref": "worker-1",
        "now_ms": 1_000,
        "lease_ms": 5_000,
        "max_tenant_in_flight": 1,
    }


def _claim_empty_result() -> dict[str, object]:
    return {
        "schema_version": "1",
        "claimed": False,
        "reason": "empty",
        "work_item_id": None,
        "kind": None,
        "payload_ref": None,
        "lease_holder_ref": None,
        "lease_epoch": None,
        "fencing_token": None,
        "lease_expires_at_ms": None,
        "attempt": None,
        "max_attempts": None,
        "tenant_in_flight": 0,
        "changed_work_item_ids": [],
    }


def _submit_result() -> dict[str, object]:
    return {
        "schema_version": "1",
        "work_item_id": "work-1",
        "status": "ready",
        "created": True,
        "replayed": False,
        "command_sequence": 1,
        "idempotency_key": "submit-retry-1",
        "dependency_count": 0,
        "admitted_count": 1,
        "max_tenant_in_flight": 1,
        "outbox_id": "outbox-1",
        "command_digest": "a" * 64,
        "provenance_refs": [],
        "changed_work_item_ids": ["work-1"],
    }


def _decode_envelope(request: dict[str, Any]) -> dict[str, Any]:
    token = request["auth_token"]
    assert isinstance(token, str) and token.startswith("eg2.")
    return json.loads(bytes.fromhex(token.removeprefix("eg2.")).decode("utf-8"))


# spec: EG-DURABLE-KERNEL-R028.4
@pytest.mark.parametrize("run_id", [None, "run-fixture"])
@pytest.mark.asyncio
async def test_work_item_retries_bind_stable_keys_to_signed_wire(
    monkeypatch: pytest.MonkeyPatch,
    run_id: str | None,
) -> None:
    """Exercise WorkItemClient -> generated helper -> real signed request.

    The fake round-trip returns native-shaped results but receives the complete
    MessagePack request built by ``EpistemicGraphClient._send``. Repeating a
    Submit or Claim with a stable key therefore checks the transport boundary
    where a fresh request id previously changed the durable envelope identity.
    """

    client = EpistemicGraphClient(
        unused_reader(),
        unused_writer(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    wire: list[dict[str, Any]] = []
    results: dict[str, Any] = {
        "SubmitWorkItem": _submit_result(),
        "ClaimWorkItem": _claim_empty_result(),
        "RenewWorkItemLease": {"decision": "renewed"},
        "CasWorkItemMetadata": {
            "schema_version": "1",
            "outcome": "applied",
            "work_item_id": "work-1",
            "changed_work_item_ids": ["work-1"],
        },
    }

    async def round_trip(
        payload: bytes,
        *,
        req_id: int,
        method: str,
        required_socket_path: str | None = None,
        verified_binding: Any = None,
    ) -> dict[str, Any]:
        assert required_socket_path is None
        assert verified_binding is not None
        assert verified_binding.claims == client._effective_verified_context()
        assert verified_binding.admission is None
        request = msgpack.unpackb(payload, raw=False)
        assert isinstance(request, dict)
        wire.append(request)
        return {"id": req_id, "result": results[method]}

    async def supports(_method: str) -> bool:
        return True

    monkeypatch.setattr(client, "_roundtrip", round_trip)
    monkeypatch.setattr(client, "supports", supports)

    submit = _submit_request()
    if run_id is not None:
        submit.update(run_id=run_id, work_item_id=None)
    await client.work_items.submit(submit)
    await client.work_items.submit(submit)

    claim = _claim_request()
    await client.work_items.claim(claim, idempotency_key="claim-retry-1")
    await client.work_items.claim(claim, idempotency_key="claim-retry-1")

    await client.work_items.renew(
        tenant="tenant-fixture",
        work_item_id="work-1",
        worker_id="worker-1",
        lease_epoch=1,
        fencing_token=1,
        now_ms=1_000,
        lease_ms=5_000,
        idempotency_key="renew-retry-1",
    )
    await client.work_items.cas_metadata(
        tenant="tenant-fixture",
        work_item_id="work-1",
        expected_status=["leased"],
        now_ms=1_100,
        set_metadata={"attempt": 1},
        idempotency_key="cas-retry-1",
    )

    envelopes = [_decode_envelope(request) for request in wire]
    assert [request["method"] for request in wire] == [
        "SubmitWorkItem",
        "SubmitWorkItem",
        "ClaimWorkItem",
        "ClaimWorkItem",
        "RenewWorkItemLease",
        "CasWorkItemMetadata",
    ]
    assert [envelope["idempotency_key"] for envelope in envelopes] == [
        "submit-retry-1",
        "submit-retry-1",
        "claim-retry-1",
        "claim-retry-1",
        "renew-retry-1",
        "cas-retry-1",
    ]
    assert wire[0]["params"]["request"]["idempotency_key"] == "submit-retry-1"
    assert wire[1]["params"]["request"] == wire[0]["params"]["request"]
    assert wire[0]["params"]["request"].get("run_id") == run_id
    if run_id is not None:
        assert wire[0]["params"]["request"]["work_item_id"] is None
    assert envelopes[0]["nonce"] != envelopes[1]["nonce"]


@pytest.mark.asyncio
async def test_work_item_retry_key_validation_rejects_blank_values() -> None:
    client = EpistemicGraphClient(
        unused_reader(),
        unused_writer(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )

    with pytest.raises(ValueError, match="ClaimWorkItem.idempotency_key"):
        await client.work_items.claim(_claim_request(), idempotency_key=" ")
    with pytest.raises(ValueError, match="RenewWorkItemLease.idempotency_key"):
        await client.work_items.renew(
            tenant="tenant-fixture",
            work_item_id="work-1",
            worker_id="worker-1",
            lease_epoch=1,
            fencing_token=1,
            now_ms=1_000,
            lease_ms=5_000,
            idempotency_key="",
        )
    with pytest.raises(ValueError, match="CasWorkItemMetadata.idempotency_key"):
        await client.work_items.cas_metadata(
            tenant="tenant-fixture",
            work_item_id="work-1",
            expected_status=["leased"],
            now_ms=1_100,
            set_metadata={"attempt": 1},
            idempotency_key=" ",
        )


# spec: EG-DURABLE-KERNEL-R028.4
@pytest.mark.asyncio
async def test_run_owned_submission_keeps_closed_request_validation() -> None:
    client = EpistemicGraphClient(
        unused_reader(),
        unused_writer(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    request = {**_submit_request(), "run_id": "run-fixture", "work_item_id": None}
    with pytest.raises(ValueError, match="unsupported fields"):
        await client.work_items.submit({**request, "owner_principal": "forged"})
    with pytest.raises(TypeError, match="SubmitWorkItem.run_id"):
        await client.work_items.submit({**request, "run_id": 1})
    with pytest.raises(ValueError, match="SubmitWorkItem.run_id"):
        await client.work_items.submit({**request, "run_id": " "})


# spec: EG-DURABLE-KERNEL-R028.4
@pytest.mark.asyncio
async def test_submit_accepts_optional_conversation_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The generated request and the engine accept ``conversation``; the client must not reject it."""
    client = EpistemicGraphClient(
        unused_reader(),
        unused_writer(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    sent: list[dict[str, Any]] = []

    async def fake_send(_method: str, payload: dict[str, Any], **_kw: Any) -> Any:
        sent.append(payload)
        return SimpleNamespace(payload=_submit_result())

    async def supports(_method: str) -> bool:
        return True

    monkeypatch.setattr(client, "supports", supports)
    monkeypatch.setattr(
        "epistemic_graph.client._gen.coordination.send_submit_work_item",
        lambda _c, payload, **kw: fake_send("SubmitWorkItem", payload, **kw),
    )
    conversation = {"turn": "opaque"}
    await client.work_items.submit({**_submit_request(), "conversation": conversation})
    assert sent[0]["request"]["conversation"] == conversation
