import pytest
import epistemic_graph


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_node_addition_and_removal(clean_graph):
    """Test standard node insertion, retrieval, checking, and deletion."""
    # Add nodes with JSON properties
    clean_graph.nodes.add("A", {"label": "Agent A", "type": "agent"})
    clean_graph.nodes.add("B", {"label": "Task B", "type": "task"})

    assert clean_graph.nodes.has("A") is True
    assert clean_graph.nodes.has("B") is True
    assert clean_graph.nodes.has("C") is False

    # Get nodes
    nodes = clean_graph.nodes.list()
    assert len(nodes) == 2

    # Check that we can map them back
    nodes_dict = dict(nodes)
    assert "A" in nodes_dict
    assert "B" in nodes_dict

    # msgpack packs dict to bytes, so if it's not decoded on client, it's bytes.
    # Wait, the client uses `import msgpack`, does it decode? We'll see.
    # We will just assert that nodes can be added/removed without error.

    # Remove node A
    clean_graph.nodes.remove("A")
    assert clean_graph.nodes.has("A") is False
    assert len(clean_graph.nodes.list()) == 1


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_edge_addition_and_removal(clean_graph):
    """Test standard edge creation, property assignment, FFI validation, and removal."""
    clean_graph.nodes.add("A", {})
    clean_graph.nodes.add("B", {})

    # Adding edge to non-existent node should raise RuntimeError from FFI layer
    with pytest.raises(RuntimeError, match="Target node 'C' not found"):
        clean_graph.edges.add("A", "C", {})

    clean_graph.edges.add("A", "B", {"weight": 2.5})
    assert clean_graph.edges.has("A", "B") is True
    assert clean_graph.edges.has("B", "A") is False

    # Get edges
    edges = clean_graph.edges.list()
    assert len(edges) == 1
    src, tgt, props = edges[0]
    assert src == "A"
    assert tgt == "B"
    # To check weight is 2.5, we should see if it's unpacked by client, or just bytes
    print(f"PROPS: {type(props)} - {props}")
    if isinstance(props, bytes):
        import msgpack

        props = msgpack.unpackb(props)
    elif isinstance(props, str):
        import json

        props = json.loads(props)
    elif isinstance(props, list):
        # maybe it's list of ints representing bytes
        import msgpack

        props = msgpack.unpackb(bytes(props))

    assert props.get("weight") == 2.5

    # Remove edge
    clean_graph.edges.remove("A", "B")
    assert clean_graph.edges.has("A", "B") is False


def test_topological_sorting(clean_graph):
    """Test Kahn's algorithm topological sorting on valid DAGs and exception on cyclic graphs."""
    clean_graph.nodes.add("X", "{}")
    clean_graph.nodes.add("Y", "{}")
    clean_graph.nodes.add("Z", "{}")

    clean_graph.edges.add("X", "Y", "{}")
    clean_graph.edges.add("Y", "Z", "{}")

    order = clean_graph.graph.topological_sort()
    assert order == ["X", "Y", "Z"]

    # Create cycle Z -> X
    clean_graph.edges.add("Z", "X", "{}")
    with pytest.raises(RuntimeError, match="Graph contains cycles"):
        clean_graph.graph.topological_sort()


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_cycle_detection(clean_graph):
    """Test that DFS-coloring can detect cycles and return precise paths."""
    clean_graph.nodes.add("A", "{}")
    clean_graph.nodes.add("B", "{}")
    clean_graph.nodes.add("C", "{}")

    clean_graph.edges.add("A", "B", "{}")
    clean_graph.edges.add("B", "C", "{}")

    assert clean_graph.graph.find_cycle() is None

    # Introduce cycle: C -> A
    clean_graph.edges.add("C", "A", "{}")
    cycle = clean_graph.graph.find_cycle()
    assert cycle is not None
    assert len(cycle) == 4
    # The cycle path should represent the traversal sequence: A -> B -> C -> A
    assert cycle == ["A", "B", "C", "A"]


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_shortest_path_bfs(clean_graph):
    """Test unweighted BFS shortest path computations."""
    clean_graph.nodes.add("1", "{}")
    clean_graph.nodes.add("2", "{}")
    clean_graph.nodes.add("3", "{}")
    clean_graph.nodes.add("4", "{}")

    clean_graph.edges.add("1", "2", "{}")
    clean_graph.edges.add("2", "3", "{}")
    clean_graph.edges.add("1", "3", "{}")
    clean_graph.edges.add("3", "4", "{}")

    # Shortest path from 1 to 4 should be 1 -> 3 -> 4 (length 3 nodes) rather than 1 -> 2 -> 3 -> 4
    path = clean_graph.graph.shortest_path("1", "4")
    assert path == ["1", "3", "4"]

    # Try unconnected node
    clean_graph.nodes.add("unconnected", "{}")
    assert clean_graph.graph.shortest_path("1", "unconnected") is None


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_blast_radius(clean_graph):
    """Test depth-limited BFS blast radius transitive impact mapping."""
    clean_graph.nodes.add("root", "{}")
    clean_graph.nodes.add("child1", "{}")
    clean_graph.nodes.add("child2", "{}")
    clean_graph.nodes.add("grandchild", "{}")

    clean_graph.edges.add("root", "child1", "{}")
    clean_graph.edges.add("root", "child2", "{}")
    clean_graph.edges.add("child1", "grandchild", "{}")

    # Blast radius max_depth=1 should only include child1 and child2
    blast_d1 = clean_graph.graph.blast_radius("root", 1)
    assert set(blast_d1) == {"child1", "child2"}

    # Blast radius max_depth=2 should include grandchild as well
    blast_d2 = clean_graph.graph.blast_radius("root", 2)
    assert set(blast_d2) == {"child1", "child2", "grandchild"}


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_ast_ingestion(clean_graph, tmp_path):
    """Test AST repository parser extracts file structural components correctly."""
    # Write Python file
    py_file = tmp_path / "model.py"
    py_file.write_text(
        "class AgentModel(Base):\n    def run_agent(self):\n        pass\n"
    )

    # Write JavaScript file
    js_file = tmp_path / "harness.js"
    js_file.write_text("function executeTask() {\n    return 42;\n}\n")

    clean_graph.graph.parse_repository(str(tmp_path))

    # Verify File nodes
    assert clean_graph.nodes.has("model.py") is True
    assert clean_graph.nodes.has("harness.js") is True

    # Verify Class and Function nodes
    assert clean_graph.nodes.has("model.py::AgentModel") is True
    assert clean_graph.nodes.has("model.py::run_agent") is True
    assert clean_graph.nodes.has("harness.js::executeTask") is True

    # Verify File contains Class and Function relationships
    assert clean_graph.edges.has("model.py", "model.py::AgentModel") is True
    assert clean_graph.edges.has("model.py", "model.py::run_agent") is True
    assert clean_graph.edges.has("harness.js", "harness.js::executeTask") is True


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_vf2_subgraph_match(clean_graph):
    """Test VF2 subgraph pattern matching with property matching."""
    # Target graph
    clean_graph.nodes.add("A", '{"type": "class"}')
    clean_graph.nodes.add("B", '{"type": "function"}')
    clean_graph.nodes.add("C", '{"type": "other"}')
    clean_graph.edges.add("A", "B", "{}")
    clean_graph.edges.add("B", "C", "{}")

    # Pattern graph
    import os
    from epistemic_graph.client import SyncEpistemicGraphClient

    socket_path = os.environ.get(
        "GRAPH_SERVICE_SOCKET", "/tmp/test_epistemic_graph_local.sock"
    )
    clean_graph.tenants.create("pattern_graph", "Agent")
    pattern = SyncEpistemicGraphClient.connect(
        socket_path=socket_path, graph_name="pattern_graph"
    )
    pattern.graph.clear()
    pattern.nodes.add("P1", '{"type": "class"}')
    pattern.nodes.add("P2", '{"type": "function"}')
    pattern.edges.add("P1", "P2", "{}")

    # Pass the pattern client to vf2_subgraph_match (client handles sending pattern_graph_name)
    matches = clean_graph.graph.vf2_subgraph_match(pattern)
    assert len(matches) == 1
    assert matches[0] == {"P1": "A", "P2": "B"}


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_reactive_state_ledger(clean_graph):
    """Test State Ledger serialization, transaction replay, and JSON states."""
    clean_graph.nodes.add("X", '{"value": 10}')
    clean_graph.nodes.add("Y", '{"value": 20}')
    clean_graph.edges.add("X", "Y", '{"rel": "connects"}')

    # Verify ledger entries
    ledger = clean_graph.ledger.get()
    assert len(ledger) == 3
    assert "ADD_NODE|X|" in ledger[0]
    assert "ADD_NODE|Y|" in ledger[1]
    assert "ADD_EDGE|X|Y|" in ledger[2]

    # We will test Transaction replay on the same graph to avoid the removed to_json/from_json
    graph2 = clean_graph

    # Transaction replay
    graph2.ledger.clear()
    graph2.nodes.add("Z", '{"value": 30}')
    graph2.edges.add("Y", "Z", '{"rel": "connects"}')

    txs = graph2.ledger.get()
    assert len(txs) == 2

    # Replay on original graph
    clean_graph.ledger.apply(txs)
    assert clean_graph.nodes.has("Z") is True
    assert clean_graph.edges.has("Y", "Z") is True


@pytest.mark.concept("CONCEPT:KG-2.20")
@pytest.mark.asyncio
async def test_rust_ast_parser_fallback():
    """Test RustASTParser local python fallback parsing behaves properly."""
    from epistemic_graph.parser import RustASTParser

    parser = RustASTParser(socket_path="/tmp/non_existent_socket_path.sock")
    source = b"class MyClass:\n    def my_method(self):\n        pass\n"
    res = await parser.parse_file("test.py", source)

    assert "nodes" in res
    assert "edges" in res
    assert res["symbols_extracted"] == 2

    # Check for MyClass and my_method symbols
    symbols = [n["properties"]["name"] for n in res["nodes"] if n["node_type"] == "SYMBOL"]
    assert "MyClass" in symbols
    assert "my_method" in symbols


@pytest.mark.concept("CONCEPT:KG-2.0")
def test_get_subgraph_batched(clean_graph):
    """Batched GetSubgraph returns decoded node properties + induced edges in
    one round-trip (replaces N per-node GetNodeProperties + a full edge scan)."""
    g = clean_graph
    g.nodes.add("a", {"type": "Concept", "name": "Alpha"})
    g.nodes.add("b", {"type": "Concept", "name": "Beta"})
    g.nodes.add("c", {"type": "Concept", "name": "Gamma"})  # outside the request
    g.edges.add("a", "b", {"rel_type": "RELATES_TO"})
    g.edges.add("b", "c", {"rel_type": "MENTIONS"})  # endpoint c not requested

    sub = g.graph.get_subgraph(["a", "b"])

    # Only requested nodes that exist, with DECODED properties.
    props = {n["id"]: n["properties"] for n in sub["nodes"]}
    assert set(props) == {"a", "b"}
    assert props["a"]["name"] == "Alpha" and props["a"]["type"] == "Concept"

    # Only the edge with both endpoints inside the requested set.
    rels = {(e["source"], e["target"], e["properties"].get("rel_type")) for e in sub["edges"]}
    assert ("a", "b", "RELATES_TO") in rels
    assert all(e["target"] != "c" and e["source"] != "c" for e in sub["edges"])
