import epistemic_graph


def test_module_startup():
    """Ensure the PyO3 binary compiles successfully and can be cleanly imported in Python."""
    assert epistemic_graph is not None
    assert hasattr(epistemic_graph, "EpistemicGraph")
