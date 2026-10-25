"""EG-ORCH-VOCAB-R801.6 served receipt: two versions, supersession, bitemporal read.

The entry point is the generated client write through
``ApplyChangeEnvelopes`` (``changes.apply_batch`` compiles to it): one
envelope publishes version v1, the next publishes v2 naming v1 in
``supersedes_id`` alongside the real ``:supersedes`` edge and the
head-window-close CAS. The versions are then read back through the node
property reads, and the current version is picked by validity window at
each time — v1 until its window closes at v2's ``valid_from``, v2 after.
"""

from __future__ import annotations

from typing import Any

import msgpack
from _harness_envelope import draft_envelope as _envelope
from _harness_envelope import graph_name as _graph_name
from _harness_envelope import op

from epistemic_graph.client import EngineResponseError
from epistemic_graph.decision import digest_text

COMPONENT = "comp:served-prompt"
CONTENT_DIGEST_DOMAIN = "eg/harness-component-content/v1"


def content_digest(content_ref: str) -> str:
    """The vocabulary's content digest — the engine's one digest scheme."""
    return digest_text(CONTENT_DIGEST_DOMAIN, content_ref)


def version_node_id(content_ref: str) -> str:
    return f"harness-component:{COMPONENT}:{content_digest(content_ref)}"


def version_props(
    content_ref: str,
    valid_from: int,
    tx_from: int,
    supersedes_id: str | None = None,
    confidence: float = 0.8,
) -> dict[str, Any]:
    """One published version as the writer composes it: the record's fields
    plus the engine-owned write marker and the client's transaction stamp."""
    props: dict[str, Any] = {
        "_harness_component": True,
        "component_id": COMPONENT,
        "kind": "system_prompt",
        "content_ref": content_ref,
        "content_digest": content_digest(content_ref),
        "valid_from": valid_from,
        "confidence": confidence,
        "evidence_run_id": f"run:{COMPONENT}:{valid_from}",
        "applied_to_id": "agent:served",
        "tx_from": tx_from,
    }
    if supersedes_id is not None:
        props["supersedes_id"] = supersedes_id
    return props


# spec: EG-ORCH-VOCAB-R801.6
def test_two_versions_and_bitemporal_read(clean_graph):
    graph = _graph_name(clean_graph)
    v1_id = version_node_id("blob:v1")
    v2_id = version_node_id("blob:v2")

    # v1 is the component's first version; v2 supersedes it by edge — the
    # new version names the current one in `supersedes_id`, the SUPERSEDES
    # edge links the two, and the CAS closes v1's window at v2's valid_from.
    first = _envelope(
        graph,
        v1_id,
        "h2-r801:v1",
        1,
        [
            op(
                0,
                "AddNode",
                {
                    "node_id": v1_id,
                    "properties_msgpack": msgpack.packb(
                        version_props("blob:v1", valid_from=10, tx_from=100)
                    ),
                },
            )
        ],
    )
    second = _envelope(
        graph,
        v2_id,
        "h2-r801:v2",
        2,
        [
            op(
                0,
                "AddNode",
                {
                    "node_id": v2_id,
                    "properties_msgpack": msgpack.packb(
                        version_props(
                            "blob:v2",
                            valid_from=20,
                            tx_from=200,
                            supersedes_id=v1_id,
                        )
                    ),
                },
            ),
            op(
                1,
                "AddEdge",
                {
                    "source_id": v2_id,
                    "target_id": v1_id,
                    "properties_msgpack": msgpack.packb(
                        {"relationship": "supersedes", "valid_from": 20, "tx_from": 200}
                    ),
                },
            ),
            op(
                2,
                "CompareAndSetNodeFields",
                {
                    "node_id": v1_id,
                    "conditions_msgpack": msgpack.packb({"valid_to": None}),
                    "updates_msgpack": msgpack.packb({"valid_to": 20, "tx_to": 200}),
                },
            ),
        ],
    )

    results = clean_graph.changes.apply_batch([first, second])
    assert [result["status"] for result in results] == ["applied", "applied"], results

    # Both versions stay readable; v1's window is closed at v2's valid_from,
    # v2 names v1, and the supersession edge is in place (newer to older).
    props_first = clean_graph.nodes.properties(v1_id)
    props_second = clean_graph.nodes.properties(v2_id)
    assert props_first is not None and props_first["valid_to"] == 20, props_first
    assert props_second is not None and props_second["supersedes_id"] == v1_id, (
        props_second
    )
    edges = clean_graph.edges.properties(v2_id, v1_id)
    assert len(edges) == 1 and edges[0]["relationship"] == "supersedes", edges

    # The bitemporal read: the current version at each time is the newest
    # whose validity window covers it — nothing before the component was
    # published, v1 until its window closes, v2 after.
    def current_at(as_of: int) -> str | None:
        current_id: str | None = None
        current_from: int | None = None
        for node_id, props in [(v1_id, props_first), (v2_id, props_second)]:
            valid_from = props["valid_from"]
            valid_to = props.get("valid_to")
            if valid_from <= as_of and (valid_to is None or as_of < valid_to):
                if current_from is None or valid_from > current_from:
                    current_id, current_from = node_id, valid_from
        return current_id

    assert current_at(5) is None, "no version is current before the component existed"
    assert current_at(10) == v1_id
    assert current_at(15) == v1_id, "v1 stays current inside its open window"
    assert current_at(19) == v1_id
    assert current_at(20) == v2_id, "the window closed at v2's valid_from"
    assert current_at(99) == v2_id, "v2's window stays open"

    # A write that would mutate the published v1 refuses with the
    # vocabulary's immutability code, and nothing is written.
    mutated = version_props("blob:v1", valid_from=10, tx_from=300, confidence=0.9)
    attempt = _envelope(
        graph,
        v1_id,
        "h2-r801:v1-mutated",
        3,
        [
            op(
                0,
                "AddNode",
                {
                    "node_id": v1_id,
                    "properties_msgpack": msgpack.packb(mutated),
                },
            )
        ],
    )
    try:
        clean_graph.changes.apply(attempt)
    except EngineResponseError as error:
        assert error.code == "HarnessVersionImmutable", error.code
    else:
        raise AssertionError("a mutated published version must refuse")
    assert clean_graph.nodes.properties(v1_id)["confidence"] == 0.8
