"""Extraction projection preserves graph identity and filters absent properties."""

from types import SimpleNamespace

import pytest

from epistemic_graph.materialize_derivation import project_extraction_batch

pytestmark = pytest.mark.no_engine


def test_projection_keeps_typed_identity_and_non_null_properties():
    graph_slice = project_extraction_batch(
        [
            SimpleNamespace(
                id="n1", type="BusinessProcess", props={"name": "Invoice", "x": None}
            )
        ],
        [
            SimpleNamespace(
                source="n1",
                target="n2",
                rel_type="FLOWS_TO",
                props={"condition": "ok", "x": None},
            )
        ],
    )
    assert graph_slice.entities == [
        {"id": "n1", "node_type": "BusinessProcess", "name": "Invoice"}
    ]
    assert graph_slice.relationships == [
        {"source": "n1", "target": "n2", "relationship": "FLOWS_TO", "condition": "ok"}
    ]


@pytest.mark.parametrize(
    "props",
    [{"id": "forged"}, {"node_type": "Secret"}],
)
def test_node_properties_cannot_override_identity(props):
    with pytest.raises(ValueError, match="reserved identity"):
        project_extraction_batch(
            [SimpleNamespace(id="n1", type="Thing", props=props)], []
        )


@pytest.mark.parametrize(
    "props",
    [{"source": "forged"}, {"target": "forged"}, {"relationship": "ADMIN"}],
)
def test_edge_properties_cannot_override_identity(props):
    with pytest.raises(ValueError, match="reserved identity"):
        project_extraction_batch(
            [],
            [SimpleNamespace(source="n1", target="n2", rel_type="LINK", props=props)],
        )
