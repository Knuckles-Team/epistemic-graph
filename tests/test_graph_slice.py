"""Derived graph-slice identity and canonical-key contracts."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.graph_slice import (
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
