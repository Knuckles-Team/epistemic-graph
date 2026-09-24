//! Statement row annotations: `WITH KNOWLEDGE` (EH-450) and `WITH PROOF` (EH-448).

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use eg_types::wire::{UqlProofCoverage, UqlProofStep, UqlResult, UqlRow};
use serde_json::json;

use crate::exec::PlanCtx;
use crate::uql::serve::run_statement;
use crate::uql::{parse_statement, Params};

fn rows_of(src: &str, ctx: &PlanCtx) -> Vec<UqlRow> {
    let stmt = parse_statement(src, &Params::new()).unwrap_or_else(|e| panic!("{}", e.render(src)));
    match run_statement(&stmt, ctx).unwrap() {
        UqlResult::Rows { rows, .. } | UqlResult::Profile { rows, .. } => rows,
        other => panic!("rows expected, got {other:?}"),
    }
}

fn docs() -> (eg_core::graph::GraphView, SemanticStore) {
    let core = GraphCore::new();
    for (id, year) in [("d1", 2020), ("d2", 2021)] {
        let props = json!({ "type": "Doc", "year": year, "valid_from": 5 });
        core.add_node(id.into(), rmp_serde::to_vec_named(&props).unwrap());
    }
    (core.analysis_snapshot(), SemanticStore::new())
}

#[test]
fn with_knowledge_carries_each_rows_record() {
    let (view, semantic) = docs();
    let ctx = PlanCtx::new(&view, &semantic);
    let rows = rows_of("MATCH (:Doc) WITH KNOWLEDGE (year, missing)", &ctx);
    assert_eq!(rows.len(), 2);
    for row in &rows {
        let knowledge = row.knowledge.as_ref().expect("a knowledge record");
        let year = if row.id == "d1" { 2020 } else { 2021 };
        assert_eq!(knowledge.projection, Some(json!({ "year": year })));
        assert_eq!(knowledge.valid_from, Some(5));
        assert_eq!(knowledge.confidence, 1.0);
        assert!(row.proof.is_none());
    }
    let bare = rows_of("MATCH (:Doc) |> LIMIT 1 WITH KNOWLEDGE", &ctx);
    assert_eq!(bare[0].knowledge.as_ref().unwrap().projection, None);
    let profiled = rows_of("PROFILE MATCH (:Doc) WITH KNOWLEDGE", &ctx);
    assert!(profiled.iter().all(|r| r.knowledge.is_some()));
}

#[test]
fn an_unprovable_admission_makes_the_proof_partial() {
    let (view, semantic) = docs();
    let ctx = PlanCtx::new(&view, &semantic);
    let rows = rows_of("MATCH (:Doc) |> LIMIT 5 WITH PROOF, KNOWLEDGE", &ctx);
    for row in &rows {
        let proof = row.proof.as_ref().expect("a proof");
        assert_eq!(proof.coverage, UqlProofCoverage::Partial);
        // MATCH admitted the row without a proof; LIMIT only cut rows.
        assert_eq!(
            proof.steps,
            vec![UqlProofStep::Unproved {
                stage: "MATCH (:Doc)".into()
            }]
        );
        assert!(row.knowledge.is_some());
    }
}

#[cfg(feature = "owl")]
mod owl {
    use super::*;
    use eg_types::rdf_report::SparqlProofCoverage;

    /// `Article ⊑ Paper ⊑ Work`; p1 a Paper, p2 an Article, t1 a Topic.
    fn papers() -> (eg_core::graph::GraphView, SemanticStore) {
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Paper rdfs:subClassOf ex:Work .
ex:Article rdfs:subClassOf ex:Paper .
ex:p1 a ex:Paper .
ex:p2 a ex:Article .
ex:t1 a ex:Topic .
"#;
        let core = GraphCore::new();
        let mut iris = eg_rdf::mapping::IriStore::default();
        let triples = eg_rdf::mapping::parse_turtle(ttl).unwrap();
        eg_rdf::mapping::load_triples(&core, &mut iris, "g", triples).unwrap();
        (core.analysis_snapshot(), SemanticStore::new())
    }

    #[test]
    fn a_sparql_source_proves_each_row_with_its_witness() {
        let (view, semantic) = papers();
        let ctx = PlanCtx::new(&view, &semantic);
        let rows = rows_of(
            "SPARQL 'SELECT ?w WHERE { ?w a <http://example.org/Paper> }' VAR 'w' WITH PROOF",
            &ctx,
        );
        assert_eq!(rows.len(), 1, "only p1 is asserted a Paper: {rows:?}");
        let proof = rows[0].proof.as_ref().unwrap();
        assert_eq!(proof.coverage, UqlProofCoverage::Complete);
        let [UqlProofStep::Sparql { proof: witness, .. }] = proof.steps.as_slice() else {
            panic!("one SPARQL step expected: {proof:?}")
        };
        assert_eq!(witness.coverage, SparqlProofCoverage::Complete);
        assert_eq!(witness.witnesses.len(), 1);
        assert_eq!(witness.witnesses[0].subject, rows[0].id);
    }

    #[test]
    fn a_reason_stage_proves_inferred_membership() {
        let (view, semantic) = papers();
        let ctx = PlanCtx::new(&view, &semantic);
        let rows = rows_of("REASON <http://example.org/Work> |> LIMIT 10 WITH PROOF", &ctx);
        let mut ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, ["<http://example.org/p1>", "<http://example.org/p2>"]);
        for row in &rows {
            let proof = row.proof.as_ref().unwrap();
            assert_eq!(proof.coverage, UqlProofCoverage::Complete, "{proof:?}");
            let [UqlProofStep::Reason { proof: tree, .. }] = proof.steps.as_slice() else {
                panic!("one REASON step expected: {proof:?}")
            };
            assert_eq!(tree.sub, row.id);
            assert_eq!(tree.rule, "CR-instance");
            assert!(!tree.premises.is_empty());
        }
    }
}
