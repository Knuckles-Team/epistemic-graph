//! SPARQL-UPDATE → reasoning cross-modal SEAM regression (CONCEPT:EG-KG.compute.concept-7).
//!
//! Proves the write→read seam across the RDF/OWL modality boundary: a SPARQL 1.1
//! `DELETE/INSERT … WHERE` that rewrites a TBox axiom is IMMEDIATELY visible to a fresh
//! OWL 2 (EL⁺) reasoning pass — the reasoner reads the SAME live property-graph the
//! update wrote to (a new committed snapshot), so re-parenting a class flips which
//! individuals are inferred members of a superclass. This is the cross-TRANSACTION seam
//! (write commits, fresh snapshot, read sees it) the audit calls supported; the
//! in-SAME-txn variant is the documented gap (see the pgwire ignored specs).
//!
//! The vector-`Rank`-over-the-changed-set half of this seam lives in eg-plan
//! (`vector_reasoning_consistency.rs`, `Op::Reason |> Op::Rank`) because eg-rdf carries
//! no vector store — here we pin the reasoning-visibility half end-to-end.

#![cfg(all(feature = "owl", feature = "sparql"))]

use std::collections::HashSet;

use eg_rdf::owl::{asserted_types_from_view, instances_of, tbox_triples_from_view, Reasoner};
use eg_rdf::sparql::Projection;
use eg_rdf::update::{execute_str, MapStore};

/// The (inferred) members of `class` over the store's default graph, right now.
fn members_of(store: &MapStore, class: &str) -> HashSet<String> {
    let view = store.core_of(None).analysis_snapshot();
    let triples = tbox_triples_from_view(&view);
    let mut reasoner = Reasoner::from_triples(&triples);
    let cls = reasoner.classify();
    let asserted = asserted_types_from_view(&view, None);
    instances_of(&cls, &asserted, class).into_iter().collect()
}

/// A `DELETE/INSERT … WHERE` that re-parents `Dog` from `Mammal` to `Animal` makes the
/// reasoner infer `rex` (a Dog) is an `Animal` where it previously was not — the new
/// axiom is visible to the very next classification.
#[test]
fn sparql_update_reparents_class_and_reasoner_sees_new_inference() {
    let store = MapStore::new();
    let dog = "<http://ex/Dog>".to_string();
    let animal = "http://ex/Animal";
    let mammal = "http://ex/Mammal";
    let rex = "<http://ex/rex>".to_string();

    // ── Seed: Dog ⊑ Mammal, rex a Dog. ──
    execute_str(
        "PREFIX ex: <http://ex/>
         PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         INSERT DATA { ex:Dog rdfs:subClassOf ex:Mammal . ex:rex a ex:Dog }",
        &store,
        &Projection::raw(),
    )
    .unwrap();

    // BEFORE: rex is a Mammal (inferred), NOT an Animal.
    let animal_before = members_of(&store, &format!("<{animal}>"));
    let mammal_before = members_of(&store, &format!("<{mammal}>"));
    assert!(
        !animal_before.contains(&rex),
        "rex is not yet an Animal (no Mammal⊑Animal axiom)"
    );
    assert!(mammal_before.contains(&rex), "rex is an inferred Mammal");

    // ── UPDATE: re-parent Dog from Mammal to Animal via DELETE/INSERT … WHERE. ──
    let report = execute_str(
        "PREFIX ex: <http://ex/>
         PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         DELETE { ex:Dog rdfs:subClassOf ex:Mammal }
         INSERT { ex:Dog rdfs:subClassOf ex:Animal }
         WHERE  { ex:Dog rdfs:subClassOf ex:Mammal }",
        &store,
        &Projection::raw(),
    )
    .unwrap();
    assert_eq!(report.deleted, 1, "the old subClassOf axiom was deleted");
    assert_eq!(report.inserted, 1, "the new subClassOf axiom was inserted");

    // AFTER: the fresh reasoning pass sees the new axiom — rex is now an Animal, not a Mammal.
    let animal_after = members_of(&store, &format!("<{animal}>"));
    let mammal_after = members_of(&store, &format!("<{mammal}>"));
    assert!(
        animal_after.contains(&rex),
        "the SPARQL UPDATE is visible to reasoning: rex is now an inferred Animal"
    );
    assert!(
        !mammal_after.contains(&rex),
        "rex is no longer a Mammal after re-parenting Dog"
    );
    let _ = dog; // (Dog is a class, not an individual — only `rex` is an instance.)
}
