"""Live CEP standing-query round-trip against a live engine (CONCEPT:EG-KG.query.protocol-types,
features `streaming` + `stream`, both folded into `full`).

Proves the `StreamingClient.cep_subscribe` / `cep_poll` / `cep_unsubscribe` bindings
(added for B-1 — the engine's live CEP surface, `Method::CepSubscribe`/`CepPoll`/
`CepUnsubscribe`, was handler-complete and compiled into the default `full` build but
had zero occurrences in `epistemic_graph/client.py`) actually work end-to-end over the
SAME framed-MessagePack transport the rest of the client uses: subscribe a pattern,
cause a matching mutation to flow through the CDC hub that feeds the live NFA engine,
poll and receive the match, then unsubscribe. A subscribe call that merely returns a
subscription id proves nothing about the surface actually detecting anything — every
test below drives an ACTUAL match through the pipe.
"""

from __future__ import annotations

import os

import pytest
from conftest import request_context

from epistemic_graph.client import SyncEpistemicGraphClient


@pytest.fixture
def client():
    socket_path = os.environ["GRAPH_SERVICE_SOCKET"]
    c = SyncEpistemicGraphClient.connect(
        socket_path=socket_path, verified_context=request_context()
    )
    c.graph.clear()
    return c


def test_cep_subscribe_poll_match_unsubscribe(client):
    g = "__commons__"
    # A one-step Sequence over an unbounded (size=0 -> span check disabled) sliding
    # window: matches every event keyed "Alert" -- the CDC label of an AddNode whose
    # properties carry {"type": "Alert"} (src/server/cep.rs::feed_change / eg-core's
    # labels_of), exactly the same label semantics test_streaming_cdc.py's
    # watch/trigger tests already rely on.
    pattern = {"Sequence": [{"key": "Alert", "preds": []}]}
    window = {"Sliding": {"size": 0}}

    sub_id = client.streaming.cep_subscribe(pattern, window=window, buffer=16)
    assert isinstance(sub_id, int)

    # Nothing pending yet -- a zero-timeout poll must return immediately empty, not
    # block, proving cep_poll's "don't wait" path.
    assert client.streaming.cep_poll(sub_id, timeout_ms=0) == []

    client.nodes.add("d1", {"type": "Doc"})  # non-matching: different key
    client.nodes.add("a1", {"type": "Alert"})  # matching

    matches = client.streaming.cep_poll(sub_id, timeout_ms=5000)
    assert len(matches) == 1, f"expected exactly one CEP match, got {matches}"
    match = matches[0]
    assert len(match["events"]) == 1
    ev = match["events"][0]
    assert ev["key"] == "Alert"
    assert ev["attrs"]["node_id"] == "a1"
    assert ev["attrs"]["op"] == "add"
    assert ev["attrs"]["graph"] == g
    assert match["start_ts"] == match["end_ts"] == ev["ts"]

    # The non-matching Doc add never produced a match, and the Alert match was
    # already drained -- nothing left to poll.
    assert client.streaming.cep_poll(sub_id, timeout_ms=200) == []

    # A second matching event produces a second, independent match.
    client.nodes.add("a2", {"type": "Alert"})
    matches2 = client.streaming.cep_poll(sub_id, timeout_ms=5000)
    assert len(matches2) == 1
    assert matches2[0]["events"][0]["attrs"]["node_id"] == "a2"

    assert client.streaming.cep_unsubscribe(sub_id) is True
    # Unsubscribing an id that no longer exists reports False, not an error.
    assert client.streaming.cep_unsubscribe(sub_id) is False

    # Polling a dropped subscription is an error (CONCEPT:EG-KG.query.protocol-types docs this
    # explicitly), not a silent empty list.
    with pytest.raises(Exception):
        client.streaming.cep_poll(sub_id, timeout_ms=200)


def test_cep_within_window_prunes_late_match(client):
    g = "__commons__"
    # A two-step sequence ("Open" followed by "Close") constrained to complete
    # within 0 ts-ticks of each other via Within{within: 0} -- since the surface's
    # clock ticks once per CDC event, two DIFFERENT events can never satisfy
    # `within: 0` (end_ts - start_ts <= 0 requires start_ts == end_ts), so this
    # proves the window constraint is actually being enforced by the live engine,
    # not just accepted as a no-op.
    pattern = {
        "Within": {
            "within": 0,
            "pattern": {
                "Sequence": [
                    {"key": "Open", "preds": []},
                    {"key": "Close", "preds": []},
                ]
            },
        }
    }
    window = {"Sliding": {"size": 100}}
    sub_id = client.streaming.cep_subscribe(pattern, window=window)
    try:
        client.nodes.add("o1", {"type": "Open"})
        client.nodes.add("c1", {"type": "Close"})
        assert client.streaming.cep_poll(sub_id, timeout_ms=500) == []
    finally:
        client.streaming.cep_unsubscribe(sub_id)


def test_cep_requires_admin_scope():
    # CEP subscriptions have no mandatory graph in the wire contract, so they
    # cannot be row-projected safely -- src/server/cep.rs::try_handle fails closed
    # for anything but a verified admin (`authority.require_admin`). Prove that
    # gate is live over the wire, not just present in the handler source.
    socket_path = os.environ["GRAPH_SERVICE_SOCKET"]
    non_admin = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        verified_context=request_context(roles=["test"], scopes=["read:only"]),
    )
    try:
        with pytest.raises(Exception):
            non_admin.streaming.cep_subscribe(
                {"Sequence": [{"key": "Alert", "preds": []}]},
                window={"Sliding": {"size": 0}},
            )
    finally:
        non_admin.close()
