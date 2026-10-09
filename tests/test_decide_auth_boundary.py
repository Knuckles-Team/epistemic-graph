"""EG-CONTRACT-R002.1/.2: the served ``Decide`` auth-boundary and pack journey.

``Decide`` is refused when the request's own ``tenant_id`` does not match the
tenant the engine verified the caller's session for
(``src/server/handlers/decide/stat_decide.rs::serve``, first check, before any
candidate or feature resolution runs):

    if request.tenant_id != verified.tenant() {
        return Err("ACCESS_DENIED: Decide tenant must match the verified
        request tenant".to_string());
    }

That check had no test anywhere in this repository before R002.1 -- not a
Rust unit test, and no served Python test at all exercised ``Decide`` or
``DecisionLog`` against a live engine (checked: no ``tests/served_*.{py,rs}``
file names ``Decide``). R002.1 proved the refusal side end-to-end through the
generated sender (``epistemic_graph.decision_client.DecisionClient``, over
the installed wire client, no source-tree import) against the real server
binary this suite's session fixture starts, not an in-process reasoning
check.

R002.2 (this module's second half) proves the matching authorized side of the
same boundary: a request whose ``tenant_id`` DOES match the verified session
passes the tenant check and reaches the next stage (candidate/feature
resolution), rather than being refused with the auth-boundary's own error --
and a served Agent Library pack journey (bind an importer, publish a pack
through the Blob CAS, import it, then read its status back) against the same
live engine, reusing ``tests/test_connector_pack_planted.py``'s served
helpers rather than duplicating them. The "no source-tree imports" wheel
install assertion from the full EG-CONTRACT-R002 acceptance text is left for
a further slice.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import pytest
from conftest import TEST_TENANT, request_context
from test_connector_pack_planted import IMPORTER, _bind, _import, _pack_op

from epistemic_graph import decision_client as dc
from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.testing.synthetic.packs.malformed import base_spec
from epistemic_graph.testing.synthetic.packs.spec import assemble

#: A structurally valid, semantically empty DecideRequest: the tenant check
#: in `serve()` runs before any candidate/feature resolution, so a minimal
#: declared-candidate request is enough to reach it.
_FEATURE_SCHEMA_PIN = {
    "component_id": "s",
    "kind": "feature_schema",
    "definition_digest": "sha256:x",
}


def _request_for_tenant(tenant_id: str) -> dict[str, object]:
    return dc.decide_request(
        tenant_id,
        ("test.auth-boundary", "retrieval_plan", "ordinary"),
        dc.declared([dc.DeclaredOption("opt-a")]),
        _FEATURE_SCHEMA_PIN,
    )


async def _decide_as_verified_session(tenant_id: str) -> object:
    socket_path = os.environ["GRAPH_SERVICE_SOCKET"]
    client = await EpistemicGraphClient.connect(
        socket_path=socket_path,
        verified_context=request_context(),
    )
    try:
        decision = dc.DecisionClient(client=client)
        return await decision.decide(_request_for_tenant(tenant_id))
    finally:
        await client.close()


def test_decide_refuses_a_request_tenant_that_does_not_match_the_verified_session() -> (
    None
):
    """A session verified for ``TEST_TENANT`` may not ``Decide`` for a
    different tenant named in the request body: the engine's own tenant
    check refuses it, not a client-side guard (the request is schema-valid
    and would otherwise be accepted)."""
    other_tenant = f"not-{TEST_TENANT}"
    assert other_tenant != TEST_TENANT

    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(_decide_as_verified_session(other_tenant))

    message = str(excinfo.value)
    assert "ACCESS_DENIED" in message, message
    assert "tenant" in message.lower(), message


def test_decide_passes_the_tenant_check_for_a_matching_request_tenant() -> None:
    """A session verified for ``TEST_TENANT`` MAY ``Decide`` for its own
    tenant: the auth-boundary check in ``serve()`` does not refuse it. The
    request still fails -- its feature-schema pin (``component_id: "s"``)
    names a component this test never published, so resolution refuses past
    the tenant check -- but that refusal is never the auth-boundary's own
    ``ACCESS_DENIED`` message, proving the two tenants take different paths
    through the same check."""
    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(_decide_as_verified_session(TEST_TENANT))

    message = str(excinfo.value)
    assert "ACCESS_DENIED" not in message, message
    assert "Decide tenant must match" not in message, message


async def _pack_journey(connector: str) -> dict[str, Any]:
    """Publish, import and read back one well-formed pack against the live
    engine: the served Agent Library journey named by EG-CONTRACT-R002's
    acceptance text. Reuses ``test_connector_pack_planted``'s served helpers
    (``_bind``/``_import``/``_pack_op``) instead of duplicating its
    bind/upload/import wire sequence."""
    socket_path = os.environ["GRAPH_SERVICE_SOCKET"]
    client = await EpistemicGraphClient.connect(
        socket_path=socket_path,
        verified_context=request_context(),
    )
    try:
        pack = assemble(base_spec(0, connector))
        await _bind(client, connector, IMPORTER)
        imported = await _import(client, pack, expected_head=None)
        assert imported["result"] == "imported", imported
        status_key = f"{connector}:status"
        status = await _pack_op(
            client,
            "status",
            {"tenant_id": TEST_TENANT, "connector": connector},
            status_key,
        )
        return {"imported": imported, "status": status}
    finally:
        await client.close()


def test_served_pack_publish_import_and_status_journey() -> None:
    """The Agent Library pack journey from EG-CONTRACT-R002's acceptance
    text -- publishing (bind + upload through the Blob CAS), importing, then
    reading a pack back -- against the real engine this suite's session
    fixture starts, through the generated sender, not an in-process check."""
    outcome = asyncio.run(_pack_journey("auth-boundary-journey"))

    imported = outcome["imported"]
    assert imported["receipt"]["binding_revision"] >= 1, imported

    status = outcome["status"]
    assert status["connector"] == "auth-boundary-journey", status
    assert status["head"]["pack_digest"] == imported["receipt"]["pack_digest"], (
        status,
        imported,
    )
