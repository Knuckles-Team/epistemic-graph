"""Object path behavior used by ontology consumers."""

import pytest

from epistemic_graph.object_path import find_object_path

# Pure unit tests over a mock graph reader -- never needs the shared native
# engine (see conftest.py's session-scoped `start_epistemic_graph_server`
# fixture, which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


class GraphReader:
    def __init__(self) -> None:
        self.paths = {("a", "b"): None, ("b", "a"): ["b", "middle", "a"]}

    def get_shortest_path(self, source: str, target: str) -> list[str] | None:
        return self.paths.get((source, target))

    def query_cypher(
        self, query: str, params: dict[str, object]
    ) -> list[dict[str, object]]:
        if "WHERE n.id IN" in query:
            return [{"id": "middle", "type": "Document", "name": "middle name"}]
        return [{"rel": "LINKED_TO", "confidence": 0.8}]


def test_reverse_path_preserves_labels_and_hops() -> None:
    result = find_object_path(GraphReader(), "a", "b")
    assert result["connected"] is True
    assert result["length"] == 2
    assert result["path"][1] == {
        "id": "middle",
        "type": "Document",
        "name": "middle name",
    }
    assert result["hops"] == [
        {"from": "a", "to": "middle", "rel": "LINKED_TO", "confidence": 0.8},
        {"from": "middle", "to": "b", "rel": "LINKED_TO", "confidence": 0.8},
    ]


def test_missing_and_identical_objects_are_distinct() -> None:
    reader = GraphReader()
    assert find_object_path(reader, "a", "missing") == {
        "source": "a",
        "target": "missing",
        "connected": False,
        "path": [],
    }
    assert find_object_path(reader, "a", "a") == {
        "source": "a",
        "target": "a",
        "connected": False,
        "error": "source and target are the same object",
    }
