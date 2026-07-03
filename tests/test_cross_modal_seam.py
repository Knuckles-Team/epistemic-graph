"""Cross-modal SEAM regression — served end-to-end (CONCEPT:EG-366).

These exercise the write→read cross-modal seams through the REAL served engine (the
`clean_graph` fixture in conftest.py builds `--features full` and connects a
`SyncEpistemicGraphClient` over the socket), so they need a LIVE engine to run — they
are skipped/uncollected only if the served fixture is unavailable.

Seam 1: a node written with an embedding + a type the reasoner subsumes is BOTH
        reasoner-inferred AND vector-discoverable by the next query.
Seam 2: a SPARQL-style axiom UPDATE (DELETE+INSERT effect) is visible to BOTH a fresh
        OWL reasoning pass AND a hybrid vector retrieve over the changed set.
"""

RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
SUBCLASS = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"


def test_add_embedding_then_reason_then_discover(clean_graph):
    """Write node(type+embedding) → reason(subclass) → discover returns the fresh node.

    Proves the freshly written node is visible to BOTH the reasoner (a new inferred
    type) and the vector/keyword discovery surface, cross-transaction.
    """
    gc = clean_graph
    gc.nodes.add("rex", {"type": "Dog", "name": "Rex", "description": "a good dog"})
    gc.graph.add_embedding("rex", [1.0, 0.0, 0.0])

    # Reason: Dog ⊑ Animal ⇒ rex is inferred an Animal (materialized).
    res = gc.reasoning.reason(subclass_relations=[("Dog", "Animal")])
    assert res["inferred_count"] >= 1, "the subclass axiom produced an inference"
    inferred_objs = {t.get("object") for t in res.get("inferred_triples", [])}
    assert "Animal" in inferred_objs, f"rex inferred as Animal, got {inferred_objs}"

    # Discover: the fresh node surfaces via hybrid keyword + vector retrieval.
    out = gc.graph.discover(["dog", "rex"], [1.0, 0.0, 0.0], k=5)
    ids = {row["id"] for row in out}
    assert "rex" in ids, f"the freshly written node must be discoverable, got {ids}"


def test_sparql_update_then_hybrid_retrieve(clean_graph):
    """A SPARQL-style axiom UPDATE (re-parent Dog from Mammal to Animal) affects BOTH a
    fresh OWL reasoning pass AND a vector/hybrid retrieve over the changed set.

    The wire-native SPARQL UPDATE is the HTTP ``POST /sparql`` (application/sparql-update)
    endpoint; over the socket client the same DELETE/INSERT change set is effected with
    ``rdf.remove_triples`` + ``rdf.add_triples`` (the reusable retract/insert ops the
    UPDATE executor itself calls).
    """
    gc = clean_graph
    rex = "<http://ex/rex>"
    dog = "<http://ex/Dog>"

    # Seed: rex a Dog ; Dog ⊑ Mammal ; give rex an embedding for the vector leg.
    gc.rdf.add_triples(
        ntriples=(
            f"{rex} {RDF_TYPE} {dog} .\n"
            f"{dog} {SUBCLASS} <http://ex/Mammal> .\n"
        )
    )
    gc.graph.add_embedding("<http://ex/rex>", [1.0, 0.0, 0.0])

    # BEFORE: rex is not yet an Animal.
    before = gc.rdf.owl_reason(target_class="http://ex/Animal")
    before_animals = {i[0] for i in before.get("instances", [])}
    assert rex not in before_animals

    # UPDATE (DELETE + INSERT WHERE effect): re-parent Dog from Mammal to Animal.
    gc.rdf.remove_triples(ntriples=f"{dog} {SUBCLASS} <http://ex/Mammal> .")
    gc.rdf.add_triples(ntriples=f"{dog} {SUBCLASS} <http://ex/Animal> .")

    # AFTER — REASONING: the fresh pass infers rex is now an Animal.
    after = gc.rdf.owl_reason(target_class="http://ex/Animal")
    after_animals = {i[0] for i in after.get("instances", [])}
    assert rex in after_animals, (
        f"the axiom UPDATE must be visible to reasoning, got {after_animals}"
    )

    # AFTER — VECTOR/HYBRID: a hybrid discover over the changed set still surfaces rex,
    # so the new knowledge affects the vector-reranked retrieval too.
    out = gc.graph.discover(["animal"], [1.0, 0.0, 0.0], k=5)
    assert any(row["id"] == "<http://ex/rex>" for row in out), (
        "the re-parented node must still be vector-discoverable after the update"
    )
