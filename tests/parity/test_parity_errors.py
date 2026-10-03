"""Known graph refusals use the same declared code on both transports.

Missing graph targets and duplicate creates are invalid arguments. Unknown or
uncoded failures still normalize to sanitized INTERNAL; the pure Rust tests in
crates/eg-pyengine/src/errors.rs retain that separate regression coverage.
These tests require the installed binding and the existing shared socket server.
"""

from __future__ import annotations

import msgpack
import pytest

from epistemic_graph.client import EngineResponseError

from ._harness import _try_send


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "params"),
    [
        ("GetNodeProperties", {"node_id": "n1"}),
        ("AddNode", {"node_id": "n1", "properties_msgpack": list(msgpack.packb({}))}),
    ],
)
async def test_missing_graph_refusal_is_classified_identically_across_transports(
    pair_factory, owner_agent_id, parity_graph, method, params
):
    """An authorized caller receives a typed missing-target refusal on either path."""
    pair = await pair_factory(owner_agent_id, parity_graph)
    for transport in (pair.socket, pair.embedded):
        result, exc = await _try_send(transport, method, params, parity_graph)
        assert result is None, f"unexpected result: {result!r}"
        assert type(exc) is EngineResponseError, f"unexpected exception: {exc!r}"
        assert exc.code == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_duplicate_graph_refusal_is_classified_identically_across_transports(
    pair_factory, owner_agent_id, parity_graph
):
    """A second independent create collides; a durable operation retry is distinct."""
    pair = await pair_factory(owner_agent_id, parity_graph)
    params = {"graph_name": parity_graph, "graph_type": "Agent"}
    for transport in (pair.socket, pair.embedded):
        _, first_exc = await _try_send(transport, "CreateGraph", params, parity_graph)
        assert first_exc is None, f"first create failed: {first_exc!r}"
        result, exc = await _try_send(transport, "CreateGraph", params, parity_graph)
        assert result is None, f"duplicate create unexpectedly returned {result!r}"
        assert type(exc) is EngineResponseError, f"unexpected exception: {exc!r}"
        assert exc.code == "INVALID_ARGUMENT"
