"""Generated senders are accepted by a live engine's ``eg2.`` MAC (au-core R5).

The engine MACs the body it re-serializes from the typed ``Method`` it decoded.
These requests are sent in pydantic/alphabetical key order with serde defaults
omitted -- exactly the shapes that answered ``Authentication failed`` while the
client restated the server's field order and defaults in Python.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

import pytest
from conftest import (
    TEST_AGENT_ID,
    TEST_AUDIENCE,
    TEST_POLICY_VERSION,
    TEST_TENANT,
    request_context,
)

from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.generated.agent_component import (
    AgentComponentContentRequest,
    AgentComponentKind,
    AgentComponentOpCurrent,
    AgentComponentSearchRequest,
)
from epistemic_graph.generated.storage import (
    send_agent_component,
    send_agent_component_content,
    send_agent_component_current,
    send_agent_component_search,
)

_PLACEHOLDER = "placeholder"
_REFUSED_BEFORE_DISPATCH = ("Authentication failed", "invalid request encoding")


def _alphabetical(value: Any) -> Any:
    """Every mapping re-keyed alphabetically: never the Rust declaration order."""
    if isinstance(value, dict):
        return {key: _alphabetical(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_alphabetical(item) for item in value]
    return value


async def _client() -> EpistemicGraphClient:
    return await EpistemicGraphClient.connect(
        socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
        verified_context=request_context(),
    )


def _library_context(expected_revision: int) -> dict[str, Any]:
    principal = "principal:sha256:" + "a" * 64
    return {
        "request_id": 0,
        "principal": principal,
        "caller_principal": principal,
        "attempt_nonce": uuid.uuid4().hex * 2,
        "tenant_id": TEST_TENANT,
        "actor_scope": _PLACEHOLDER,
        "purpose_id": _PLACEHOLDER,
        "policy_revision": _PLACEHOLDER,
        "policy_digest": "sha256:" + "0" * 64,
        "policy_decision_id": _PLACEHOLDER,
        "idempotency_key": uuid.uuid4().hex,
        "expected_revision": expected_revision,
        "trace_id": None,
        "created_at_ms": 0,
    }


def _skill_draft(component_id: str) -> dict[str, Any]:
    # `classification`, `requires`, `provides` and the capability lists are
    # serde defaults and deliberately omitted.
    return {
        "component_id": component_id,
        "kind": "skill",
        "version": "1.0.0",
        "content_digest": "sha256:" + "1" * 64,
        "content_ref": None,
        "facts": {"facts": "opaque"},
        "provenance": {"origin": "native"},
        "summary": "canonical body probe",
        "attributes": {"name": "canonical-body"},
        "tenant_id": TEST_TENANT,
        "actor_scope": _PLACEHOLDER,
        "purpose_id": _PLACEHOLDER,
        "policy_digest": "sha256:" + "0" * 64,
        "source_revision": "rev-1",
        "source_revision_digest": "sha256:" + "8" * 64,
    }


@pytest.mark.asyncio
async def test_agent_component_senders_pass_the_served_mac() -> None:
    client = await _client()
    component_id = f"skill:canonical-{uuid.uuid4().hex[:12]}"
    try:
        assert await client.supports("AgentComponent")
        publish = {
            "op": "publish",
            "request": {
                "context": _library_context(0),
                "component": _skill_draft(component_id),
                "evaluation_receipt_digest": None,
            },
        }
        await send_agent_component(
            client,
            _alphabetical({"op": publish}),
            idempotency_key=f"canonical-publish-{component_id}",
        )
        current = await send_agent_component_current(
            client,
            AgentComponentOpCurrent(
                op="current", tenant_id=TEST_TENANT, component_id=component_id
            ),
        )
        assert current is not None and current.component_id == component_id
        page = await send_agent_component_search(
            client,
            AgentComponentSearchRequest(
                tenant_id=TEST_TENANT, kinds=[AgentComponentKind.SKILL], limit=10
            ),
        )
        assert component_id in {entry.component_id for entry in page.entries}
        await _content_reaches_dispatch(client, component_id)
    finally:
        await client.close()


async def _content_reaches_dispatch(
    client: EpistemicGraphClient, component_id: str
) -> None:
    """The probe component has no stored body, so the engine may refuse the
    read itself -- but only after the MAC and decode accepted the request."""
    try:
        await send_agent_component_content(
            client,
            AgentComponentContentRequest(
                tenant_id=TEST_TENANT, component_id=component_id
            ),
        )
    except RuntimeError as refused:
        assert not any(text in str(refused) for text in _REFUSED_BEFORE_DISPATCH)


def _submit_context(now_ms: int) -> dict[str, Any]:
    return {
        "schema_version": "2",
        "request_id": uuid.uuid4().hex,
        "subject_id": TEST_AGENT_ID,
        "tenant_id": TEST_TENANT,
        "agent_id": TEST_AGENT_ID,
        "scopes": [],
        "audience": TEST_AUDIENCE,
        "authentication_method": "local_process",
        "policy_version": TEST_POLICY_VERSION,
        "graph": "__commons__",
        "placement_epoch": None,
        "trace_id": uuid.uuid4().hex,
        "issued_at_ms": now_ms,
        "expires_at_ms": now_ms + 60_000,
    }


def _submit_request(work_item_id: str, now_ms: int) -> dict[str, Any]:
    return _alphabetical(
        {
            "schema_version": "1",
            "context": _submit_context(now_ms),
            "work_item_id": work_item_id,
            "idempotency_key": f"submit-{work_item_id}",
            "command_digest": uuid.uuid4().hex * 2,
            "kind": "canonical_body",
            "priority": 0,
            "depends_on": [],
            "input_ref": "input:canonical-body",
            "policy_digest": "policy:canonical-body",
            "catalog_digest": "catalog:canonical-body",
            "model_digest": "model:canonical-body",
            "max_attempts": 1,
            "deadline_unix": None,
            "metadata": {"a": 2},
            "provenance_refs": [],
            "max_tenant_in_flight": 8,
        }
    )


def _claim_request(work_item_id: str, now_ms: int) -> dict[str, Any]:
    return {
        "schema_version": "1",
        "tenant_ref": TEST_TENANT,
        "work_item_id": work_item_id,
        "queue_ref": None,
        "resource_class": None,
        "fairness_group": None,
        "worker_ref": TEST_AGENT_ID,
        "now_ms": now_ms,
        "lease_ms": 60_000,
        "max_tenant_in_flight": 8,
    }


@pytest.mark.asyncio
async def test_work_item_submit_claim_commit_pass_the_served_mac() -> None:
    """``CasWorkItemMetadata`` sends binary metadata blobs, and
    ``CommitWorkItemResult`` omits ``outcome_extension`` (a serde default the
    engine re-serializes as an explicit null between ``result_ref`` and
    ``error_ref``) -- the body the Python signer used to get wrong."""
    client = await _client()
    work_item_id = f"workitem:canonical:{uuid.uuid4().hex}"
    now_ms = int(time.time() * 1000)
    try:
        submitted = await client.work_items.submit(
            _submit_request(work_item_id, now_ms)
        )
        assert submitted["work_item_id"] == work_item_id
        claim = await client.work_items.claim(
            _claim_request(work_item_id, now_ms + 1),
            idempotency_key=f"claim-{work_item_id}",
        )
        assert claim["claimed"] is True, claim
        # The metadata blobs ride as MessagePack `bin`; the flattened Request
        # used to refuse them with "invalid request encoding".
        cas = await client.work_items.cas_metadata(
            tenant=TEST_TENANT,
            work_item_id=work_item_id,
            expected_status=["leased", "running"],
            now_ms=now_ms + 2,
            expected_lease={
                "worker_ref": TEST_AGENT_ID,
                "lease_epoch": claim["lease_epoch"],
                "fencing_token": claim["fencing_token"],
            },
            expected_metadata={"a": 2},
            set_metadata={"pending_input_response": {"answer_ref": "answer:1"}},
            idempotency_key=f"cas-{work_item_id}",
        )
        assert cas["outcome"] == "applied", cas
        committed = await client.work_items.commit_result(
            tenant=TEST_TENANT,
            work_item_id=work_item_id,
            worker_id=TEST_AGENT_ID,
            lease_epoch=claim["lease_epoch"],
            fencing_token=claim["fencing_token"],
            idempotency_key=f"commit-{work_item_id}",
            outcome="succeeded",
            now_ms=now_ms + 3,
            result_ref="result:canonical-body",
        )
        assert committed, committed
    finally:
        await client.close()
