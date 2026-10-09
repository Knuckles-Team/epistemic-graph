"""EG-CONTRACT-R002.1: the first served ``Decide`` auth-boundary refusal test.

``Decide`` is refused when the request's own ``tenant_id`` does not match the
tenant the engine verified the caller's session for
(``src/server/handlers/decide/stat_decide.rs::serve``, first check, before any
candidate or feature resolution runs):

    if request.tenant_id != verified.tenant() {
        return Err("ACCESS_DENIED: Decide tenant must match the verified
        request tenant".to_string());
    }

That check had no test anywhere in this repository before this one -- not a
Rust unit test, and no served Python test at all exercised ``Decide`` or
``DecisionLog`` against a live engine (checked: no ``tests/served_*.{py,rs}``
file names ``Decide``). This is the first: an end-to-end proof through the
generated sender (``epistemic_graph.decision_client.DecisionClient``, over
the installed wire client, no source-tree import) against the real server
binary this suite's session fixture starts, not an in-process reasoning
check.

Only the refusal case is proven here (EG-CONTRACT-R002.1, per
SPEC-SIZING-AND-DEPENDENCIES.md Sec 5: the typed request plus its refusal
first). A follow-up slice proves the matching authorized-success path and the
"no source-tree imports" wheel-install assertion from the full EG-CONTRACT-R002
acceptance text.
"""

from __future__ import annotations

import asyncio
import os

import pytest
from conftest import TEST_TENANT, request_context

from epistemic_graph import decision_client as dc
from epistemic_graph.client import EpistemicGraphClient

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
