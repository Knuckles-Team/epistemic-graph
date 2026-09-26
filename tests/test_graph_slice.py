"""Derived graph-slice identity and canonical-key contracts."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.graph_slice import (
    GraphSliceCapture,
    edge_only_marker_entity,
    graph_slice_digest,
    graph_slice_primary,
    validate_graph_slice,
)

pytestmark = pytest.mark.no_engine


def test_auxiliary_changes_advance_whole_slice_identity() -> None:
    entities = [
        {"id": "root", "node_type": "ProcessEvent"},
        {"id": "child", "node_type": "BusinessObject"},
    ]
    links = [{"source": "root", "target": "child", "relationship": "INVOLVES"}]
    validate_graph_slice(entities, links)
    primary = graph_slice_primary(entities, links)
    first = graph_slice_digest({"entities": entities, "relationships": links})

    assert primary["_nodes"] == [entities[1]]
    assert primary["_links"] == links
    assert "_nodes" not in entities[0]
    assert graph_slice_digest({"relationships": links, "entities": entities}) == first
    assert (
        graph_slice_digest(
            {
                "entities": entities,
                "relationships": [{**links[0], "relationship": "RETRACTS"}],
            }
        )
        != first
    )


@pytest.mark.parametrize(
    ("entities", "links"),
    [
        ([{"id": "n", "type": "ProcessEvent"}], []),
        ([{"id": "n", "node_type": ""}], []),
        ([{"id": "n", "node_type": "ProcessEvent"}], [{"type": "INVOLVES"}]),
        ([{"id": "n", "node_type": "ProcessEvent"}], [{"relationship": ""}]),
    ],
)
def test_noncanonical_type_keys_refused(
    entities: list[dict], links: list[dict]
) -> None:
    with pytest.raises(ValueError, match="canonical"):
        validate_graph_slice(entities, links)


def test_edge_only_marker_is_stable_and_source_scoped() -> None:
    links = [{"source": "a", "target": "b", "relationship": "INVOLVES"}]
    first = edge_only_marker_entity("ocel", "orders", links)
    replay = edge_only_marker_entity("ocel", "orders", links)
    other_source = edge_only_marker_entity("ocel", "returns", links)

    assert first == replay
    assert first["node_type"] == "SourceMaterialization"
    assert first["id"] != other_source["id"]
    assert "orders" not in first["id"]


def test_capture_merges_nodes_deduplicates_edges_and_orders_snapshot() -> None:
    capture = GraphSliceCapture(None)
    capture.add_node("b", "Entity", name="before")
    capture.add_node("a", node_type="Document", title="A")
    capture.add_node("b", node_type="Person", name="after")
    capture.add_edge("b", "a", "CITES", confidence=0.9)
    capture.add_edge("b", "a", "CITES", confidence=0.9)
    nodes, edges = capture.snapshot()

    assert [node["id"] for node in nodes] == ["a", "b"]
    assert nodes[1]["node_type"] == "Person"
    assert nodes[1]["name"] == "after"
    assert edges == [
        {"source": "b", "target": "a", "relationship": "CITES", "confidence": 0.9}
    ]


def test_capture_refuses_retired_type_and_relationship_keys() -> None:
    capture = GraphSliceCapture(None)
    with pytest.raises(ValueError, match="node property 'type' is retired"):
        capture.add_node("a", type="Document")
    with pytest.raises(ValueError, match="edge properties .* are retired"):
        capture.add_edge("a", "b", type="CITES")
