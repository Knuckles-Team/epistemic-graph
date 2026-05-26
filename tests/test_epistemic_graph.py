import pytest
import epistemic_graph

def test_node_addition_and_removal(clean_graph):
    """Test standard node insertion, retrieval, checking, and deletion."""
    # Add nodes with JSON properties
    clean_graph.add_node("A", '{"label": "Agent A", "type": "agent"}')
    clean_graph.add_node("B", '{"label": "Task B", "type": "task"}')

    assert clean_graph.has_node("A") is True
    assert clean_graph.has_node("B") is True
    assert clean_graph.has_node("C") is False

    # Get nodes
    nodes = clean_graph.get_nodes()
    assert len(nodes) == 2

    # Check that we can map them back
    nodes_dict = dict(nodes)
    assert "A" in nodes_dict
    assert "B" in nodes_dict
    assert "Agent A" in nodes_dict["A"]

    # Remove node A
    clean_graph.remove_node("A")
    assert clean_graph.has_node("A") is False
    assert len(clean_graph.get_nodes()) == 1


def test_edge_addition_and_removal(clean_graph):
    """Test standard edge creation, property assignment, FFI validation, and removal."""
    clean_graph.add_node("A", "{}")
    clean_graph.add_node("B", "{}")

    # Adding edge to non-existent node should raise ValueError from FFI layer
    with pytest.raises(ValueError, match="Target node 'C' not found"):
        clean_graph.add_edge("A", "C", "{}")

    clean_graph.add_edge("A", "B", '{"weight": 2.5}')
    assert clean_graph.has_edge("A", "B") is True
    assert clean_graph.has_edge("B", "A") is False

    # Get edges
    edges = clean_graph.get_edges()
    assert len(edges) == 1
    src, tgt, props = edges[0]
    assert src == "A"
    assert tgt == "B"
    assert "2.5" in props

    # Remove edge
    clean_graph.remove_edge("A", "B")
    assert clean_graph.has_edge("A", "B") is False


def test_topological_sorting(clean_graph):
    """Test Kahn's algorithm topological sorting on valid DAGs and exception on cyclic graphs."""
    clean_graph.add_node("X", "{}")
    clean_graph.add_node("Y", "{}")
    clean_graph.add_node("Z", "{}")

    clean_graph.add_edge("X", "Y", "{}")
    clean_graph.add_edge("Y", "Z", "{}")

    order = clean_graph.topological_sort()
    assert order == ["X", "Y", "Z"]

    # Create cycle Z -> X
    clean_graph.add_edge("Z", "X", "{}")
    with pytest.raises(ValueError, match="Graph contains cycles"):
        clean_graph.topological_sort()


def test_cycle_detection(clean_graph):
    """Test that DFS-coloring can detect cycles and return precise paths."""
    clean_graph.add_node("A", "{}")
    clean_graph.add_node("B", "{}")
    clean_graph.add_node("C", "{}")

    clean_graph.add_edge("A", "B", "{}")
    clean_graph.add_edge("B", "C", "{}")

    assert clean_graph.find_cycle() is None

    # Introduce cycle: C -> A
    clean_graph.add_edge("C", "A", "{}")
    cycle = clean_graph.find_cycle()
    assert cycle is not None
    assert len(cycle) == 4
    # The cycle path should represent the traversal sequence: A -> B -> C -> A
    assert cycle == ["A", "B", "C", "A"]


def test_shortest_path_bfs(clean_graph):
    """Test unweighted BFS shortest path computations."""
    clean_graph.add_node("1", "{}")
    clean_graph.add_node("2", "{}")
    clean_graph.add_node("3", "{}")
    clean_graph.add_node("4", "{}")

    clean_graph.add_edge("1", "2", "{}")
    clean_graph.add_edge("2", "3", "{}")
    clean_graph.add_edge("1", "3", "{}")
    clean_graph.add_edge("3", "4", "{}")

    # Shortest path from 1 to 4 should be 1 -> 3 -> 4 (length 3 nodes) rather than 1 -> 2 -> 3 -> 4
    path = clean_graph.get_shortest_path("1", "4")
    assert path == ["1", "3", "4"]

    # Try unconnected node
    clean_graph.add_node("unconnected", "{}")
    assert clean_graph.get_shortest_path("1", "unconnected") is None


def test_blast_radius(clean_graph):
    """Test depth-limited BFS blast radius transitive impact mapping."""
    clean_graph.add_node("root", "{}")
    clean_graph.add_node("child1", "{}")
    clean_graph.add_node("child2", "{}")
    clean_graph.add_node("grandchild", "{}")

    clean_graph.add_edge("root", "child1", "{}")
    clean_graph.add_edge("root", "child2", "{}")
    clean_graph.add_edge("child1", "grandchild", "{}")

    # Blast radius max_depth=1 should only include child1 and child2
    blast_d1 = clean_graph.get_blast_radius("root", 1)
    assert set(blast_d1) == {"child1", "child2"}

    # Blast radius max_depth=2 should include grandchild as well
    blast_d2 = clean_graph.get_blast_radius("root", 2)
    assert set(blast_d2) == {"child1", "child2", "grandchild"}


def test_ast_ingestion(clean_graph, tmp_path):
    """Test AST repository parser extracts file structural components correctly."""
    # Write Python file
    py_file = tmp_path / "model.py"
    py_file.write_text(
        "class AgentModel(Base):\n"
        "    def run_agent(self):\n"
        "        pass\n"
    )

    # Write JavaScript file
    js_file = tmp_path / "harness.js"
    js_file.write_text(
        "function executeTask() {\n"
        "    return 42;\n"
        "}\n"
    )

    clean_graph.parse_repository(str(tmp_path))

    # Verify File nodes
    assert clean_graph.has_node("model.py") is True
    assert clean_graph.has_node("harness.js") is True

    # Verify Class and Function nodes
    assert clean_graph.has_node("model.py::AgentModel") is True
    assert clean_graph.has_node("model.py::run_agent") is True
    assert clean_graph.has_node("harness.js::executeTask") is True

    # Verify File contains Class and Function relationships
    assert clean_graph.has_edge("model.py", "model.py::AgentModel") is True
    assert clean_graph.has_edge("model.py", "model.py::run_agent") is True
    assert clean_graph.has_edge("harness.js", "harness.js::executeTask") is True


def test_vf2_subgraph_match(clean_graph):
    """Test VF2 subgraph pattern matching with property matching."""
    # Target graph
    clean_graph.add_node("A", '{"type": "class"}')
    clean_graph.add_node("B", '{"type": "function"}')
    clean_graph.add_node("C", '{"type": "other"}')
    clean_graph.add_edge("A", "B", "{}")
    clean_graph.add_edge("B", "C", "{}")

    # Pattern graph
    pattern = epistemic_graph.EpistemicGraph()
    pattern.add_node("P1", '{"type": "class"}')
    pattern.add_node("P2", '{"type": "function"}')
    pattern.add_edge("P1", "P2", "{}")

    matches = clean_graph.vf2_subgraph_match(pattern)
    assert len(matches) == 1
    assert matches[0] == {"P1": "A", "P2": "B"}


def test_reactive_state_ledger(clean_graph):
    """Test State Ledger serialization, transaction replay, and JSON states."""
    clean_graph.add_node("X", '{"value": 10}')
    clean_graph.add_node("Y", '{"value": 20}')
    clean_graph.add_edge("X", "Y", '{"rel": "connects"}')

    # Verify ledger entries
    ledger = clean_graph.get_ledger()
    assert len(ledger) == 3
    assert "ADD_NODE|X|" in ledger[0]
    assert "ADD_NODE|Y|" in ledger[1]
    assert "ADD_EDGE|X|Y|" in ledger[2]

    # JSON Serialization & Load
    json_str = clean_graph.to_json()

    graph2 = epistemic_graph.EpistemicGraph()
    graph2.from_json(json_str)

    assert graph2.has_node("X") is True
    assert graph2.has_node("Y") is True
    assert graph2.has_edge("X", "Y") is True
    assert graph2.get_ledger() == ledger

    # Transaction replay
    graph2.clear_ledger()
    graph2.add_node("Z", '{"value": 30}')
    graph2.add_edge("Y", "Z", '{"rel": "connects"}')

    txs = graph2.get_ledger()
    assert len(txs) == 2

    # Replay on original graph
    clean_graph.apply_ledger(txs)
    assert clean_graph.has_node("Z") is True
    assert clean_graph.has_edge("Y", "Z") is True
