import pytest

import epistemic_graph


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_module_startup():
    """Ensure the package can be cleanly imported in Python."""
    assert epistemic_graph is not None
    from epistemic_graph.client import SyncEpistemicGraphClient

    assert SyncEpistemicGraphClient is not None
