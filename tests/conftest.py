import pytest
import epistemic_graph


@pytest.fixture
def clean_graph():
    """Returns a clean EpistemicGraph instance for each test case."""
    return epistemic_graph.EpistemicGraph()
