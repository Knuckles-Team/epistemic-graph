"""Streaming / CDC / continuous-query / watch / trigger round-trip against a live
engine (CONCEPT:KG-2.229/230, feature `streaming`, folded into `full`).

These drive the `StreamingClient` over the framed-MessagePack socket the conftest
fixture serves, proving the reactive surface works end-to-end through the SAME
transport the rest of the client uses — cursor-driven CDC reads, an incrementally-
maintained continuous query equal to a full re-run, and a watch + trigger delivery.
"""

from __future__ import annotations

import os

import pytest

from epistemic_graph.client import SyncEpistemicGraphClient


@pytest.fixture
def client():
    socket_path = os.environ.get(
        "GRAPH_SERVICE_SOCKET", "/tmp/test_epistemic_graph_local.sock"
    )
    c = SyncEpistemicGraphClient.connect(socket_path=socket_path)
    c.graph.clear()
    return c


def test_cdc_read_from_cursor_is_ordered_and_skips_seen(client):
    g = "__commons__"
    client.nodes.add("n1", {"type": "Doc"})
    client.nodes.add("n2", {"type": "Doc"})

    events = client.streaming.cdc_read(g, 0)
    assert len(events) == 2
    assert events[0]["seq"] == 0
    assert events[0]["node_id"] == "n1"
    assert events[0]["kind"] == "AddNode"
    assert events[0]["label"] == "Doc"
    assert events[1]["node_id"] == "n2"

    # Re-read from one past the last seen → nothing.
    cursor = events[-1]["seq"] + 1
    assert client.streaming.cdc_read(g, cursor) == []

    # A new write then read-from-cursor returns only it.
    client.nodes.remove("n1")
    tail = client.streaming.cdc_read(g, cursor)
    assert len(tail) == 1
    assert tail[0]["kind"] == "RemoveNode"
    assert tail[0]["node_id"] == "n1"


def test_continuous_query_incremental_equals_full_rerun(client):
    g = "__commons__"
    client.streaming.register_continuous_query("doc_count", g, "count", label="Doc")
    for n in ("a", "b", "c"):
        client.nodes.add(n, {"type": "Doc"})
    client.nodes.add("x", {"type": "Other"})
    client.nodes.remove("a")

    cq = client.streaming.read_continuous_query("doc_count")
    # Full re-run oracle: CDC-replay count of live Doc nodes.
    events = client.streaming.cdc_read(g, 0)
    live = set()
    for e in events:
        if e["label"] == "Doc" and e["kind"] == "AddNode":
            live.add(e["node_id"])
        elif e["kind"] == "RemoveNode":
            live.discard(e["node_id"])
    assert cq["value"] == float(len(live))
    assert cq["value"] == 2.0


def test_watch_and_trigger_delivery(client):
    g = "__commons__"
    client.streaming.register_trigger(
        "on_alert", g, "add", label="Alert", action={"topic": "ops"}
    )
    client.nodes.add("d1", {"type": "Doc"})  # non-matching
    client.nodes.add("a1", {"type": "Alert"})  # matching

    batch = client.streaming.watch(g, 0, label="Alert")
    assert len(batch["events"]) == 1
    assert batch["events"][0]["node_id"] == "a1"
    assert batch["next_seq"] == batch["events"][0]["seq"] + 1

    fired = client.streaming.fired_triggers(g, 0)
    assert len(fired) == 1
    assert fired[0]["trigger"] == "on_alert"
    assert fired[0]["node_id"] == "a1"
