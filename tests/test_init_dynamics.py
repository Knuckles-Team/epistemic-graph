import epistemic_graph

def test_initial_graph_dynamics(clean_graph):
    """Verify that a clean, newly instantiated EpistemicGraph is properly configured and empty."""
    assert isinstance(clean_graph, epistemic_graph.EpistemicGraph)

    # Check that nodes and edges lists are empty
    nodes = clean_graph.get_nodes()
    edges = clean_graph.get_edges()
    assert len(nodes) == 0
    assert len(edges) == 0

    # Verify standard defaults
    assert clean_graph.has_node("non_existent_node") is False
    assert clean_graph.has_edge("src", "tgt") is False

    # Verify algorithms handle empty graph gracefully
    assert clean_graph.topological_sort() == []
    assert clean_graph.find_cycle() is None
    assert clean_graph.get_shortest_path("A", "B") is None
    assert clean_graph.get_blast_radius("A", 2) == []
