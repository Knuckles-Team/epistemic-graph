"""Differential parity: the graph_ops domain (Lane 1's file -- Wave 0 proves
the harness shape here first, using the prototype's 5 existing methods:
`create_graph`/`add_node`/`get_node_properties`/`has_node`/`node_count`).

Every test here needs the shared out-of-process server (started by the
parent `tests/conftest.py`'s autouse fixture, `--features full`) AND the
compiled `epistemic_graph.engine` pyo3 extension (`crates/eg-pyengine
--features python`, plus its `graph_ops()` accessor, plan §4.1) -- neither is
built in this session (Wave 0's Python half deliberately does not run
`cargo`, see the task brief). These tests are written to collect cleanly
and pass once both are built; they are NOT expected to pass in this session.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from epistemic_graph.embedded import EmbeddedTransport

# Relative import -- see `conftest.py`'s comment on the same import and
# `tests/parity/__init__.py` (BUG-CX-002): this directory is now a real
# package (`parity`), distinct from the top-level `conftest` module.
from ._harness import assert_parity, assert_rls_isolation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method",
    ["CreateGraph", "AddNode", "GetNodeProperties", "HasNode", "NodeCount"],
)
async def test_embedded_refuses_other_principal_before_graph_dispatch(
    monkeypatch, method
):
    """Every currently ported method must fail closed, including mutations
    and counts whose native implementation has no per-call identity arg.
    """
    native_calls = []

    class FakeEngine:
        def __init__(self, **kwargs):
            native_calls.append(("new", kwargs))

        def create_graph(self, name):
            native_calls.append(("create_graph", name))

    monkeypatch.setitem(
        sys.modules, "epistemic_graph.engine", SimpleNamespace(Engine=FakeEngine)
    )
    transport = EmbeddedTransport(persist_dir=":memory:", agent_id="owner")
    with pytest.raises(RuntimeError, match="^ACCESS_DENIED: embedded engine"):
        await transport._send(
            method, {"node_id": "secret"}, graph="private", agent_id="other"
        )
    assert [call[0] for call in native_calls] == ["new"]


@pytest.mark.asyncio
async def test_create_add_get_has_count_round_trip(
    pair_factory, owner_agent_id, parity_graph
):
    """The prototype's 5 methods, in the order a caller would naturally use
    them, each checked for parity between the socket and embedded transports."""
    pair = await pair_factory(owner_agent_id, parity_graph)

    await assert_parity(
        pair, "CreateGraph", {"graph_name": parity_graph, "graph_type": "Agent"}
    )

    properties = {"kind": "widget", "count": 3}
    await assert_parity(
        pair,
        "AddNode",
        {"node_id": "n1", "properties_msgpack": _pack(properties)},
        graph=parity_graph,
    )

    has_result = await assert_parity(
        pair, "HasNode", {"node_id": "n1"}, graph=parity_graph
    )
    assert has_result is True

    got_properties = await assert_parity(
        pair, "GetNodeProperties", {"node_id": "n1"}, graph=parity_graph
    )
    assert got_properties == properties

    count = await assert_parity(pair, "NodeCount", None, graph=parity_graph)
    assert count == 1


@pytest.mark.asyncio
async def test_has_node_false_for_absent_node_is_parity_checked(
    pair_factory, owner_agent_id, parity_graph
):
    """A negative case (no node written yet) -- proves parity isn't only
    checked on the happy path."""
    pair = await pair_factory(owner_agent_id, parity_graph)
    await assert_parity(
        pair, "CreateGraph", {"graph_name": parity_graph, "graph_type": "Agent"}
    )
    has_result = await assert_parity(
        pair, "HasNode", {"node_id": "does-not-exist"}, graph=parity_graph
    )
    assert has_result is False


@pytest.mark.asyncio
async def test_get_node_properties_rls_isolation(
    pair_factory, owner_agent_id, other_agent_id, parity_graph
):
    """Two-principal RLS case (plan §3.1/§4.3's proof-of-concept requirement):
    `owner` writes a node into a graph `other` (an unregistered identity) has
    no grant for; `other` must see nothing reading it back, on BOTH
    transports, while `owner` sees it on both.

    EH-635 binds the shared embedded engine to `owner`. Its request from
    `other` must fail with the same ACCESS_DENIED shape as the served graph
    ACL. The focused transport test above also checks all five method names
    so a future method cannot silently ignore the per-call identity.
    """
    owner = await pair_factory(owner_agent_id, parity_graph)
    other = await pair_factory(other_agent_id, parity_graph)

    await assert_parity(
        owner, "CreateGraph", {"graph_name": parity_graph, "graph_type": "Agent"}
    )
    await assert_parity(
        owner,
        "AddNode",
        {"node_id": "secret", "properties_msgpack": _pack({"v": "owner-only"})},
        graph=parity_graph,
    )

    await assert_rls_isolation(
        "GetNodeProperties",
        {"node_id": "secret"},
        owner=owner,
        other=other,
        graph=parity_graph,
    )


def _pack(value: dict) -> list[int]:
    """Mirrors `client.py`'s own `_pack_binary_msgpack` (`client.py:714`):
    the wire `properties_msgpack` param is a plain msgpack byte-list, not a
    Python dict -- both transports must decode it themselves."""
    import msgpack

    return list(msgpack.packb(value, use_bin_type=True))
