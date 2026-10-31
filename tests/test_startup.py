import pytest

import medulla


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_module_startup():
    """Ensure the package can be cleanly imported in Python."""
    assert medulla is not None
    from medulla.client import SyncMedullaClient

    assert SyncMedullaClient is not None
