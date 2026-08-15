"""U-96/U-98: `MultiTenantClient.create`'s `graph_type` must be validated
against the engine's closed `GraphType` wire enum (`crates/eg-types/src/
protocol.rs`: `Agent | Team | Global | Commons`) BEFORE any request is sent.

Before this fix, a semantic content label like `"Ontology"` (the live
`agent_utilities.knowledge_graph.ontology.lifecycle._ensure_ontology_graph`
defect) was sent over the wire, failed to deserialize server-side, and the
decode-failure path answered under a synthetic correlation id `0` that the
Python client's `_pending` map has no future for -- so the call silently
starved out its full multi-minute timeout/retry budget instead of failing
immediately with a clear error (see `src/server/transport.rs`'s
`recover_request_id`, added alongside this fix for the server side of the same
defect).

This test proves BOTH directions per the acceptance bar:
  * every valid type succeeds (and is sent byte-for-byte as given);
  * an invalid type raises client-side BEFORE any wire send -- proven by
    making `_send` itself raise if it is EVER called, not merely by checking
    the call count after the fact.
"""

from __future__ import annotations

import pytest

from epistemic_graph.client import VALID_GRAPH_TYPES, EpistemicGraphClient

# Pure client-side logic: `EpistemicGraphClient` is constructed over dummy
# `object()` reader/writer and `_send` is monkeypatched -- never a real
# connection. No test here needs the shared native engine.
pytestmark = pytest.mark.no_engine


def _context() -> dict[str, object]:
    return {
        "principal": "subject-opaque",
        "tenant": "tenant-fixture",
        "audience": "engine-fixture",
        "agent_id": "agent-fixture",
        "roles": ["ingestor"],
        "scopes": ["ingest:*"],
        "policy_version": "policy-v1",
        "delegation": ["subject-opaque", "agent-fixture"],
    }


def _client() -> EpistemicGraphClient:
    return EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )


def test_valid_graph_types_match_the_closed_wire_enum() -> None:
    # Pin the exact set so a future server-side enum change is caught here
    # too, not just by the round-trip proof below.
    assert VALID_GRAPH_TYPES == {"Agent", "Team", "Global", "Commons"}


@pytest.mark.asyncio
@pytest.mark.parametrize("graph_type", sorted(VALID_GRAPH_TYPES))
async def test_every_valid_graph_type_is_sent_unchanged(graph_type: str) -> None:
    client = _client()
    captured: dict[str, object] = {}

    async def capture(method, params=None, graph=None, *, idempotency_key=None):
        captured.update(method=method, params=params)
        return None

    client._send = capture  # type: ignore[method-assign]
    await client.tenants.create("tenant__local__ontology", graph_type)

    assert captured["method"] == "CreateGraph"
    assert captured["params"] == {
        "graph_name": "tenant__local__ontology",
        "graph_type": graph_type,
    }


@pytest.mark.asyncio
async def test_unsupported_graph_type_raises_before_any_send() -> None:
    client = _client()

    async def never_send(*args, **kwargs):
        raise AssertionError(
            "CreateGraph must not be sent for an unsupported graph_type -- "
            "the allowlist check must reject it before _send is reached"
        )

    client._send = never_send  # type: ignore[method-assign]

    # U-96's exact live defect: a semantic content label instead of a
    # lifecycle/isolation graph category.
    with pytest.raises(ValueError, match="Ontology"):
        await client.tenants.create("tenant__local__ontology", "Ontology")


@pytest.mark.asyncio
@pytest.mark.parametrize("graph_type", ["", "agent", "GLOBAL", "Tenant", "Commons "])
async def test_case_and_near_miss_variants_are_also_rejected(graph_type: str) -> None:
    """Not just the one live-observed value -- the allowlist is exact-match,
    not case-insensitive or fuzzy, matching the engine's own closed enum
    (`serde` derives exact-name matching with no aliases)."""
    client = _client()

    async def never_send(*args, **kwargs):
        raise AssertionError(f"must not send unsupported graph_type={graph_type!r}")

    client._send = never_send  # type: ignore[method-assign]

    with pytest.raises(ValueError):
        await client.tenants.create("g", graph_type)
