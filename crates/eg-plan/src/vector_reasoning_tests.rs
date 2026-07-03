//! Vector ⇄ reasoning consistency SEAM regression (CONCEPT:EG-361).
//!
//! The cross-TRANSACTION write→read seam: a freshly WRITTEN node (an embedding + a type
//! that the OWL reasoner subsumes) must be BOTH ANN-ranked AND reasoner-included by the
//! very next query — because both the reasoner (`Op::Reason`) and the vector ranker
//! (`Op::Rank`) read the SAME new committed snapshot. And an embedding UPDATE must be
//! reflected by the next ANN pass. One fused plan `[Reason{Class} |> Rank{vec}]` pushes
//! the reasoned class members INTO the ANN scan as the candidate allowlist (CONCEPT:EG-070),
//! so the fresh node can only surface if it cleared BOTH gates.
//!
//! (Placed in eg-plan, NOT eg-query as the audit's file plan sketched: eg-query links no
//! reasoner — the reason∩vector fusion only exists here, over `Op::Reason`+`Op::Rank`.)

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use serde_json::json;

use crate::exec::{PlanCtx, PlanExt};
use eg_types::wire::{Op, Plan};

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// TBox: `Article ⊑ Paper`, `Paper ⊑ ∃about.Topic`, `∃about.Topic ⊑ ScholarlyWork`; two
/// only-INFERRED `ScholarlyWork`s (`p1` Paper, `p2` Article). Returns the OPEN core (so a
/// test can WRITE a fresh node) — take `analysis_snapshot()` per read.
fn owl_core() -> GraphCore {
    let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .

ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .

ex:p1 a ex:Paper .
ex:p2 a ex:Article .
"#;
    let core = GraphCore::new();
    let mut iris = eg_rdf::mapping::IriStore::default();
    eg_rdf::mapping::load_triples(
        &core,
        &mut iris,
        "g",
        eg_rdf::mapping::parse_turtle(ttl).unwrap(),
        #[cfg(feature = "rdf-redb")]
        None,
    )
    .unwrap();
    core
}

const TARGET: &str = "<http://example.org/ScholarlyWork>";

fn reason_rank_plan(q: Vec<f32>) -> Plan {
    Plan::new(vec![
        Op::Reason {
            target_class: TARGET.into(),
            ontology: String::new(),
        },
        Op::Rank { query: q },
        Op::Limit { k: 10 },
    ])
}

/// Cross-txn write→read: INSERT a node with a type the reasoner subsumes + an embedding,
/// then the next `[Reason |> Rank]` returns it — proving the fresh node is BOTH
/// reasoner-included (inferred `ScholarlyWork`) AND ANN-ranked in ONE query.
#[test]
fn add_embedding_then_reason_then_ann_consistent() {
    let core = owl_core();
    let q = vec![1.0f32, 0.0, 0.0];
    let mut semantic = SemanticStore::new();
    semantic.add_embedding("<http://example.org/p1>".into(), vec![0.90, 0.44, 0.0]);
    semantic.add_embedding("<http://example.org/p2>".into(), vec![0.80, 0.60, 0.0]);

    // BEFORE the write: only p1,p2 are inferred members (p2 closer to q than p1).
    let before = {
        let view = core.analysis_snapshot();
        reason_rank_plan(q.clone())
            .execute(&PlanCtx::new(&view, &semantic))
            .unwrap()
            .ids()
    };
    assert_eq!(
        before,
        vec![
            "<http://example.org/p1>".to_string(),
            "<http://example.org/p2>".to_string(),
        ],
        "p1 ranks above p2 for q before the write"
    );
    assert!(!before.contains(&"<http://example.org/p9>".to_string()));

    // ── WRITE: a fresh Article (⊑ ScholarlyWork by the TBox) with a top-ranked vector. ──
    core.add_node(
        "<http://example.org/p9>".into(),
        blob(json!({ "type": "<http://example.org/Article>" })),
    );
    semantic.add_embedding("<http://example.org/p9>".into(), vec![0.999, 0.02, 0.0]);

    // AFTER: the next fused query sees p9 — reasoner-included (inferred ScholarlyWork)
    // AND ANN-ranked FIRST (closest to q). It cleared BOTH gates in one plan.
    let after = {
        let view = core.analysis_snapshot();
        reason_rank_plan(q.clone())
            .execute(&PlanCtx::new(&view, &semantic))
            .unwrap()
            .ids()
    };
    assert_eq!(
        after,
        vec![
            "<http://example.org/p9>".to_string(),
            "<http://example.org/p1>".to_string(),
            "<http://example.org/p2>".to_string(),
        ],
        "the freshly-written node is BOTH reasoned-in AND ANN-ranked (first)"
    );
}

/// An embedding UPDATE is reflected by the next ANN pass: re-embedding an existing
/// reasoned member with a nearer vector re-orders the SAME `[Reason |> Rank]` result.
#[test]
fn update_embedding_then_ann_reflects_new_vector() {
    let core = owl_core();
    let q = vec![1.0f32, 0.0, 0.0];
    let mut semantic = SemanticStore::new();
    // Initially p2 is closer to q than p1.
    semantic.add_embedding("<http://example.org/p1>".into(), vec![0.70, 0.71, 0.0]);
    semantic.add_embedding("<http://example.org/p2>".into(), vec![0.99, 0.10, 0.0]);

    let view = core.analysis_snapshot();
    let before = reason_rank_plan(q.clone())
        .execute(&PlanCtx::new(&view, &semantic))
        .unwrap()
        .ids();
    assert_eq!(
        before,
        vec![
            "<http://example.org/p2>".to_string(),
            "<http://example.org/p1>".to_string(),
        ],
        "p2 ranks above p1 before the embedding update"
    );

    // ── UPDATE p1's embedding to be the nearest to q (overwrites the prior vector). ──
    semantic.add_embedding("<http://example.org/p1>".into(), vec![1.0, 0.0, 0.0]);

    let after = reason_rank_plan(q)
        .execute(&PlanCtx::new(&view, &semantic))
        .unwrap()
        .ids();
    assert_eq!(
        after,
        vec![
            "<http://example.org/p1>".to_string(),
            "<http://example.org/p2>".to_string(),
        ],
        "the updated embedding re-orders the ANN result (p1 now first)"
    );
}
