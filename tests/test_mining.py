"""End-to-end round-trip tests for the data-mining surface
(CONCEPT:EG-KG.mining.frequent-itemset-mining).

Exercises the full path: Python client → UDS → Rust dispatch → mining handler →
result, plus the graph-derived transaction source and KG write-back of
``:AssociationRule`` nodes. Uses the session-scoped server + ``clean_graph`` sync
client from conftest.py.
"""

import math

# The classic market-basket fixture (bread/butter/milk), 5 baskets.
BASKETS = [
    ["bread", "butter", "milk"],
    ["bread", "butter"],
    ["bread", "milk"],
    ["butter", "milk"],
    ["bread", "butter", "milk"],
]


def _find_rule(rules, antecedent, consequent):
    want_a, want_c = set(antecedent), set(consequent)
    for r in rules:
        if set(r["antecedent"]) == want_a and set(r["consequent"]) == want_c:
            return r
    return None


def test_mine_associate_explicit_transactions(clean_graph):
    res = clean_graph.mining.associate(
        BASKETS, min_support=0.4, min_confidence=0.0, algorithm="apriori"
    )
    assert res["n_transactions"] == 5
    rules = res["rules"]
    # {bread,butter} ⇒ {milk}: support=0.4, conf=2/3, lift=(2/3)/0.8
    r = _find_rule(rules, ["bread", "butter"], ["milk"])
    assert r is not None
    assert math.isclose(r["support"], 0.4, abs_tol=1e-9)
    assert math.isclose(r["confidence"], 2.0 / 3.0, abs_tol=1e-9)
    assert math.isclose(r["lift"], (2.0 / 3.0) / 0.8, abs_tol=1e-9)
    # {bread} ⇒ {butter}: conf=0.75, lift=0.9375
    r2 = _find_rule(rules, ["bread"], ["butter"])
    assert r2 is not None
    assert math.isclose(r2["confidence"], 0.75, abs_tol=1e-9)
    assert math.isclose(r2["lift"], 0.9375, abs_tol=1e-9)


def test_all_three_algorithms_agree_over_wire(clean_graph):
    """Apriori == FP-Growth == Eclat through the full RPC path (parity gate)."""
    ruleset = {}
    for algo in ("apriori", "fpgrowth", "eclat"):
        res = clean_graph.mining.associate(
            BASKETS, min_support=0.4, min_confidence=0.5, algorithm=algo
        )
        ruleset[algo] = sorted(
            (tuple(r["antecedent"]), tuple(r["consequent"])) for r in res["rules"]
        )
    assert ruleset["apriori"] == ruleset["fpgrowth"]
    assert ruleset["apriori"] == ruleset["eclat"]
    assert ruleset["apriori"]  # non-empty


def test_min_confidence_filters(clean_graph):
    loose = clean_graph.mining.associate(BASKETS, min_support=0.4, min_confidence=0.0)
    strict = clean_graph.mining.associate(BASKETS, min_support=0.4, min_confidence=0.7)
    assert strict["n_rules"] < loose["n_rules"]
    assert all(r["confidence"] >= 0.7 - 1e-12 for r in strict["rules"])


def _seed_evolution_graph(client):
    """Seed the concept↔capability co-occurrence graph (the evolution use case).

    Five ``Paper`` nodes each TOUCHES some ``Concept`` nodes and IMPLEMENTS a
    ``Capability`` node — so each paper's neighborhood is one transaction over
    {concepts ∪ capability}.
    """
    client.graph.clear()
    concepts = {"cA": "concept:cA", "cB": "concept:cB", "cC": "concept:cC"}
    caps = {"capX": "capability:capX", "capZ": "capability:capZ"}
    for cid in concepts.values():
        client.nodes.add(cid, {"type": "Concept"})
    for cid in caps.values():
        client.nodes.add(cid, {"type": "Capability"})
    papers = {
        "p1": (["cA", "cB"], "capZ"),
        "p2": (["cA", "cB"], "capZ"),
        "p3": (["cA", "cB"], "capZ"),
        "p4": (["cA", "cC"], "capX"),
        "p5": (["cB", "cC"], "capX"),
    }
    for pid, (cs, cap) in papers.items():
        client.nodes.add(pid, {"type": "Paper"})
        for c in cs:
            client.edges.add(pid, concepts[c], {"relation": "TOUCHES"})
        client.edges.add(pid, caps[cap], {"relation": "IMPLEMENTS"})


def test_graph_derived_source_and_writeback(clean_graph):
    _seed_evolution_graph(clean_graph)
    res = clean_graph.mining.associate(
        source={"node_label": "Paper", "direction": "out"},
        min_support=0.4,
        min_confidence=0.9,
        algorithm="fpgrowth",
        writeback=True,
    )
    assert res["n_transactions"] == 5
    # {concept:cA, concept:cB} ⇒ {capability:capZ} with confidence 1.0.
    r = _find_rule(res["rules"], ["concept:cA", "concept:cB"], ["capability:capZ"])
    assert r is not None, res["rules"]
    assert math.isclose(r["confidence"], 1.0, abs_tol=1e-9)
    assert math.isclose(r["support"], 0.6, abs_tol=1e-9)
    assert r["lift"] > 1.0

    # Write-back materialized :AssociationRule nodes, queryable by label.
    assert res["written_back"] > 0
    written = clean_graph.nodes.list_by_label("AssociationRule", 0)
    assert len(written) == res["written_back"]
    # The rule node carries the metrics as queryable properties.
    node_ids = [nid for nid, _ in written]
    props = clean_graph.nodes.properties(node_ids[0])
    assert props["type"] == "AssociationRule"
    for key in ("antecedent", "consequent", "support", "confidence", "lift"):
        assert key in props


def test_writeback_is_idempotent(clean_graph):
    """Re-mining the same rules re-uses deterministic node ids (no duplicates)."""
    _seed_evolution_graph(clean_graph)
    first = clean_graph.mining.associate(
        source={"node_label": "Paper", "direction": "out"},
        min_support=0.4,
        min_confidence=0.9,
        writeback=True,
    )
    clean_graph.mining.associate(
        source={"node_label": "Paper", "direction": "out"},
        min_support=0.4,
        min_confidence=0.9,
        writeback=True,
    )
    written = clean_graph.nodes.list_by_label("AssociationRule", 0)
    assert len(written) == first["written_back"]
