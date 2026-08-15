"""U-96/U-98/U-101 end-to-end, against the REAL server binary.

U-96 root cause: `agent_utilities.knowledge_graph.ontology.lifecycle.
_ensure_ontology_graph` called `tenants.create(graph_name, "Ontology")` --
`"Ontology"` is a semantic content label, not a member of the engine's closed
`GraphType` wire enum (`crates/eg-types/src/protocol.rs`: `Agent | Team |
Global | Commons`). The unsupported value failed to deserialize server-side.

U-98 root cause (why that showed up as a multi-minute TIMEOUT rather than an
immediate error): the server's undecodable-request path answered under a
synthetic correlation id `0`
(`src/server/transport.rs`, before this fix) -- which the Python client's
`_pending` map never has a future for, so the response was silently dropped
and the ORIGINAL caller starved out its full retry/timeout budget instead of
seeing the failure.

U-101 validates the closed lifecycle end-to-end: a supported type creates
quickly, an unsupported type is rejected FAST (not after a timeout), and nothing
is left half-provisioned.

This file proves the FULL chain live: (1) a supported type creates
successfully, (2) the Python client allowlist rejects an unsupported type
before any send, and (3) bypassing that client-side guard (simulating a
caller that skipped it, or a bug reintroducing it) still gets a FAST, bounded,
correlated error from the real server -- never a dropped response / timeout.
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest
from conftest import request_context

from epistemic_graph.client import EpistemicGraphClient


@pytest.mark.concept("CONCEPT:EG-KG.security.signed-request-envelope")
def test_supported_graph_type_creates_and_is_queryable(clean_graph):
    graph_name = "kf-pilot:lifecycle-contract-global"
    clean_graph.tenants.create(graph_name, "Global")
    try:
        names = {
            str(e.get("name") if isinstance(e, dict) else e)
            for e in (clean_graph.tenants.list() or [])
        }
        assert graph_name in names
    finally:
        clean_graph.tenants.delete(graph_name)


@pytest.mark.concept("CONCEPT:EG-KG.security.signed-request-envelope")
def test_unsupported_graph_type_client_raises_before_any_send(clean_graph):
    calls_before = None
    if hasattr(clean_graph, "tenants"):
        # No live call should happen -- confirm no new graph appears.
        calls_before = {
            str(e.get("name") if isinstance(e, dict) else e)
            for e in (clean_graph.tenants.list() or [])
        }
    with pytest.raises(ValueError, match="Ontology"):
        clean_graph.tenants.create("tenant__local__ontology", "Ontology")
    calls_after = {
        str(e.get("name") if isinstance(e, dict) else e)
        for e in (clean_graph.tenants.list() or [])
    }
    assert calls_after == calls_before, "no graph should have been created or attempted"


@pytest.mark.concept("CONCEPT:EG-KG.security.signed-request-envelope")
def test_bypassing_the_client_guard_still_fails_fast_and_correlated():
    """The server-side half of U-96/U-98: even a caller that skips the
    client-side allowlist (an old client, a direct wire caller, a future
    regression in the guard) gets a FAST, correlated, bounded error --
    never the multi-minute timeout the live incident actually produced.
    """
    socket_path = os.environ.get("GRAPH_SERVICE_SOCKET")
    assert socket_path is not None

    async def _run() -> tuple[str | None, float]:
        # Bounded, generous connect/call timeout: this test's OWN point is
        # that the fix makes the round trip fast, but a genuinely broken
        # server (or a stale binary without this fix) must still fail this
        # test LOUD within a bounded time rather than hang the suite forever
        # (GOC-70: size timeouts generously enough for a slow runner without
        # masking a hang). 20s is far above the fixed path's expected
        # milliseconds and far below the 242s+ the live pre-fix incident took.
        client = await EpistemicGraphClient.connect(
            socket_path=socket_path,
            verified_context=request_context(),
            timeout=20,
        )
        try:
            start = time.monotonic()
            try:
                # Bypass MultiTenantClient.create's allowlist entirely --
                # send the unsupported wire value directly, exactly as the
                # pre-fix ontology bootstrap did.
                await client._send(
                    "CreateGraph",
                    {
                        "graph_name": "tenant__local__ontology-bypass",
                        "graph_type": "Ontology",
                    },
                )
            except Exception as exc:  # noqa: BLE001 -- want the exact error/timing
                return str(exc), time.monotonic() - start
            return None, time.monotonic() - start
        finally:
            await client.close()

    error, elapsed = asyncio.run(_run())
    assert error is not None, "an unsupported graph_type unexpectedly succeeded"
    # The live U-90/U-96 incident measured 242+ seconds (three 60s native
    # retries) before this fix. A generous bound well under that, but far
    # above what a healthy local round-trip needs, proves this is a fast
    # correlated rejection, not a starved timeout that happened to also
    # raise.
    assert elapsed < 10.0, (
        f"rejection took {elapsed:.2f}s -- expected a fast correlated error, "
        "not a multi-minute timeout (the exact U-90/U-96 failure mode)"
    )
