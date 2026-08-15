"""Round-trip tests for the compiled semantic reasoner (CONCEPT:EG-KG.compute.compiled-semantic-reasoner).

Exercises the live UDS path: ``client.reasoning.reason`` against the running
epistemic-graph-server (started by the conftest session fixture).

Regression guard for the wiring fix: ``run_datalog_reasoning`` /
``infer_domain_range`` / ``infer_property_chains`` exist in ``src/reasoning.rs``
but were never dispatched in ``src/server.rs`` — these tests fail if the
``RunDatalogReasoning`` handler is removed from ``dispatch_graph_op``.
"""


def _triples(result):
    """Normalise the inferred-triple payload to (subject, predicate, object) tuples."""
    return {
        (t.get("subject"), t.get("predicate"), t.get("object"))
        for t in result.get("inferred_triples", [])
    }


def test_transitive_closure_infers_edge(clean_graph):
    # a --ancestor--> b --ancestor--> c  ⇒ infer a --ancestor--> c
    clean_graph.nodes.add("a", {"type": "Person"})
    clean_graph.nodes.add("b", {"type": "Person"})
    clean_graph.nodes.add("c", {"type": "Person"})
    # Edge predicate/type rides the `relationship` property key (the convention
    # `extract_facts` in `crates/eg-compute/src/reasoning.rs` reads), not `type`
    # (which is the NODE-type key) -- see that function's `val.get("relationship")`.
    clean_graph.edges.add("a", "b", {"relationship": "ancestor"})
    clean_graph.edges.add("b", "c", {"relationship": "ancestor"})

    result = clean_graph.reasoning.reason(transitive_properties=["ancestor"])

    assert result["inferred_count"] >= 1
    # Inferred edge is materialised in-graph (side effect), not just returned.
    assert clean_graph.edges.has("a", "c") is True


def test_subclass_inheritance_infers_type(clean_graph):
    # rex : Dog, Dog ⊑ Animal  ⇒  rex : Animal
    clean_graph.nodes.add("rex", {"type": "Dog"})

    result = clean_graph.reasoning.reason(
        subclass_relations=[("Dog", "Animal")]
    )

    assert result["inferred_count"] >= 1
    triples = _triples(result)
    assert ("rex", "rdf:type", "Animal") in triples or any(
        s == "rex" and o == "Animal" for (s, _p, o) in triples
    )


def test_symmetric_property_infers_reverse(clean_graph):
    # alice --spouse--> bob, spouse symmetric  ⇒  bob --spouse--> alice
    clean_graph.nodes.add("alice", {"type": "Person"})
    clean_graph.nodes.add("bob", {"type": "Person"})
    clean_graph.edges.add("alice", "bob", {"relationship": "spouse"})

    result = clean_graph.reasoning.reason(symmetric_properties=["spouse"])

    assert result["inferred_count"] >= 1
    assert clean_graph.edges.has("bob", "alice") is True


def test_empty_rules_is_noop(clean_graph):
    # Calling with no rule sets must not error and must infer nothing.
    clean_graph.nodes.add("x", {"type": "Thing"})
    result = clean_graph.reasoning.reason()
    assert result["inferred_count"] == 0
