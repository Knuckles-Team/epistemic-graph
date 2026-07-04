import pytest
import epistemic_graph
from epistemic_graph.client import SyncEpistemicGraphClient


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_initial_graph_dynamics(clean_graph):
    """Verify that a clean, newly instantiated EpistemicGraph is properly configured and empty."""
    assert isinstance(clean_graph, SyncEpistemicGraphClient)

    # Check that nodes and edges lists are empty
    nodes = clean_graph.nodes.list()
    edges = clean_graph.edges.list()
    assert len(nodes) == 0
    assert len(edges) == 0

    # Verify standard defaults
    assert clean_graph.nodes.has("non_existent_node") is False
    assert clean_graph.edges.has("src", "tgt") is False

    # Verify algorithms handle empty graph gracefully
    assert clean_graph.graph.topological_sort() == []
    assert clean_graph.graph.find_cycle() is None
    assert clean_graph.graph.shortest_path("A", "B") is None
    assert clean_graph.graph.blast_radius("A", 2) == []
