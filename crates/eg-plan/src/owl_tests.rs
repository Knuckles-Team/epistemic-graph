//! The OWL/SPARQL compose-oracle proofs (CONCEPT:EG-KG.ontology.concept-12).
//!
//! Mirrors the [`crate::oracle`] pattern for the semantic tier: a SINGLE
//! `UnifiedQuery` plan that seeds its candidate set from `Op::Reason` (OWL-inferred
//! class members) or `Op::SparqlBgp` (SPARQL-selected ids) and then vector-`Rank`s
//! them must return the BYTE-IDENTICAL ordered id list as doing the two steps
//! SEPARATELY (the reasoner/evaluator, then an independent kNN). That identity is the
//! proof the semantic surface is "just more Ops over the one RowSet substrate" — the
//! OWL-inferred / SPARQL-selected ids flow into the SAME ranker the SQL+graph+vector
//! plan uses, with no impedance mismatch.

use std::collections::HashSet;

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};

use crate::exec::{execute, PlanCtx};
use eg_types::wire::{Op, Plan};

/// An OWL ontology with an existential-restriction + role-chain subsumption the EL
/// reasoner derives, plus four individuals — two only-INFERRED `ScholarlyWork`s, a
/// non-member, and a member that is vector-FAR. Returns the view + a SemanticStore.
///
/// TBox:  `Paper ⊑ ∃about.Topic`, `∃about.Topic ⊑ ScholarlyWork`, `Article ⊑ Paper`.
/// So `Article`/`Paper` individuals are INFERRED `ScholarlyWork` even though NO
/// explicit `ScholarlyWork` type edge exists on them — the headline EL move.
fn owl_fixture() -> (GraphView, SemanticStore) {
    let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .

ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .

ex:p1 a ex:Paper .
ex:p2 a ex:Article .
ex:p3 a ex:Topic .
ex:p4 a ex:Paper .
"#;
    let core = GraphCore::new();
    let mut iris = eg_rdf::mapping::IriStore::default();
    eg_rdf::mapping::load_triples(
        &core,
        &mut iris,
        "g",
        eg_rdf::mapping::parse_turtle(ttl).unwrap(),
    )
    .unwrap();

    // Embeddings keyed by the canonical <iri> node ids. Query ≈ [1,0,0,0]:
    // p2 closest, then p1, then p4 (far). p3 (Topic, NOT a ScholarlyWork) is vector-
    // VERY-close but must be excluded by the semantic candidate set.
    let mut semantic = SemanticStore::new();
    semantic.add_embedding("<http://example.org/p1>".into(), vec![0.90, 0.44, 0.0, 0.0]);
    semantic.add_embedding("<http://example.org/p2>".into(), vec![0.99, 0.10, 0.0, 0.0]);
    semantic.add_embedding("<http://example.org/p3>".into(), vec![1.00, 0.00, 0.0, 0.0]);
    semantic.add_embedding("<http://example.org/p4>".into(), vec![0.20, 0.97, 0.0, 0.0]);

    (core.analysis_snapshot(), semantic)
}

fn query_vec() -> Vec<f32> {
    vec![1.0, 0.0, 0.0, 0.0]
}

/// THE OWL COMPOSE ORACLE: `[Reason{ScholarlyWork} |> Rank |> Limit]` in ONE plan ==
/// (reason for the inferred members) then (an independent vector kNN over them).
#[test]
fn owl_inferred_set_then_rank_equals_separate() {
    let (view, semantic) = owl_fixture();
    let target = "<http://example.org/ScholarlyWork>";

    // ── Fused: ONE plan. Reason seeds the inferred members; Rank orders them. ──
    let plan = Plan::new(vec![
        Op::Reason {
            target_class: target.into(),
            ontology: String::new(), // classify the axioms already in the graph
        },
        Op::Rank { query: query_vec() },
        Op::Limit { k: 2 },
    ]);
    let ctx = PlanCtx::new(&view, &semantic);
    let fused = execute(&plan, &ctx).unwrap();
    let fused_ids: Vec<String> = fused.ids();

    // ── Separate: reason for the member set, THEN an independent kNN over it. ──
    let triples = eg_rdf::owl::tbox_triples_from_view(&view);
    let mut reasoner = eg_rdf::owl::Reasoner::from_triples(&triples);
    let cls = reasoner.classify();
    let asserted = eg_rdf::owl::asserted_types_from_view(&view, "http://example.org/").unwrap();
    let members: HashSet<String> = eg_rdf::owl::instances_of(&cls, &asserted, target)
        .into_iter()
        .collect();
    // Independent kNN over the whole store, keep only members, top-2.
    let ranked = semantic.semantic_search(&query_vec(), 64);
    let separate: Vec<String> = ranked
        .into_iter()
        .filter(|(id, _)| members.contains(id))
        .map(|(id, _)| id)
        .take(2)
        .collect();

    assert_eq!(
        fused_ids, separate,
        "fused OWL-Reason→Rank must equal separate reason-then-rank"
    );
    // The inferred members are exactly p1,p2,p4 (Articles/Papers); p3 (Topic) excluded
    // even though it is vector-closest — the semantic candidate set constrains rank.
    assert!(!fused_ids.contains(&"<http://example.org/p3>".to_string()));
    // Order: p2 (closest member) then p1.
    assert_eq!(
        fused_ids,
        vec![
            "<http://example.org/p2>".to_string(),
            "<http://example.org/p1>".to_string()
        ]
    );
}

/// The SPARQL compose oracle: `[SparqlBgp{?w a Paper} |> Rank |> Limit]` in ONE plan
/// == (SPARQL select the Papers) then (an independent vector kNN over them). Mirrors
/// the OWL oracle for the SPARQL source op.
#[test]
fn sparql_selected_set_then_rank_equals_separate() {
    let (view, semantic) = owl_fixture();
    let sparql = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?w WHERE { ?w a ex:Paper . }"#;

    let plan = Plan::new(vec![
        Op::SparqlBgp {
            query: sparql.into(),
            var: "w".into(),
        },
        Op::Rank { query: query_vec() },
        Op::Limit { k: 2 },
    ]);
    let ctx = PlanCtx::new(&view, &semantic);
    let fused = execute(&plan, &ctx).unwrap();
    let fused_ids = fused.ids();

    // Separate: SPARQL select the Papers, then an independent kNN.
    let res = eg_rdf::sparql::execute(
        &eg_rdf::sparql::Dataset::new(&view, Vec::new()),
        sparql,
        &eg_rdf::sparql::Projection::raw(),
        None,
    )
    .unwrap()
    .into_table();
    let members: HashSet<String> = res
        .solutions
        .iter()
        .filter_map(|s| match s.get("w") {
            Some(eg_rdf::sparql::Binding::Node(n)) => Some(n.clone()),
            _ => None,
        })
        .collect();
    let ranked = semantic.semantic_search(&query_vec(), 64);
    let separate: Vec<String> = ranked
        .into_iter()
        .filter(|(id, _)| members.contains(id))
        .map(|(id, _)| id)
        .take(2)
        .collect();

    assert_eq!(
        fused_ids, separate,
        "fused SPARQL-BGP→Rank must equal separate"
    );
    // p1 + p4 are asserted ex:Paper (p2 is an ex:Article — SPARQL matches the asserted
    // `a ex:Paper` only, NOT the subclass, which is the OWL reasoner's job). So the
    // candidate set is {p1, p4}; p1 ranks above p4 (and p2/p3 are NOT in the set).
    assert_eq!(
        fused_ids,
        vec![
            "<http://example.org/p1>".to_string(),
            "<http://example.org/p4>".to_string()
        ]
    );
}
