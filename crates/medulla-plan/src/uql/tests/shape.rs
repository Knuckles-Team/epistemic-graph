//! `VALIDATE SHAPE` as an end-to-end UQL statement (EH-196): a program that parses
//! real UQL text runs the stage over real RDF-loaded fixtures and reports which rows
//! conform to, or violate, the named SHACL shape -- not just a direct call into the
//! stage's own executor (`exec::dispatch::shape::tests`) or a parser-only check
//! (`uql::parser::shape::tests`).

use medulla_core::compute::semantic::SemanticStore;
use medulla_core::graph::GraphCore;
use medulla_rdf::mapping::{load_triples, parse_turtle, IriStore};
use medulla_types::wire::UqlResult;

use crate::exec::PlanCtx;
use crate::uql::serve::run_statement;
use crate::uql::{parse_statement, Params};

const SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
ex:Named a sh:NodeShape ; sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"#;

/// alice and carol have `ex:name`; bob does not.
fn people() -> (medulla_core::graph::GraphView, SemanticStore) {
    let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice a ex:Person ; ex:name "Alice" .
ex:bob   a ex:Person ; ex:age "3" .
ex:carol a ex:Person ; ex:name "Carol" .
"#;
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "g", parse_turtle(ttl).unwrap()).unwrap();
    (core.analysis_snapshot(), SemanticStore::new())
}

fn ids_of(src: &str, ctx: &PlanCtx) -> Vec<String> {
    let stmt = parse_statement(src, &Params::new()).unwrap_or_else(|e| panic!("{}", e.render(src)));
    match run_statement(&stmt, ctx).unwrap() {
        UqlResult::Rows { rows, .. } => rows.into_iter().map(|row| row.id).collect(),
        other => panic!("rows expected, got {other:?}"),
    }
}

// spec: MEDULLA-FEDERATED-QUERY-R005
#[test]
fn validate_shape_reports_conformance_and_violation_through_real_uql_text() {
    let (view, semantic) = people();
    let ctx = PlanCtx::new(&view, &semantic);

    let mut conforming = ids_of(
        &format!(
            r#"REASON <http://ex/Person> |> VALIDATE SHAPE <http://ex/Named> USING '{SHAPES}' KEEP CONFORMING"#
        ),
        &ctx,
    );
    conforming.sort();
    assert_eq!(
        conforming,
        vec![
            "<http://ex/alice>".to_string(),
            "<http://ex/carol>".to_string()
        ],
        "alice and carol both carry ex:name, so they conform to the Named shape"
    );

    let violating = ids_of(
        &format!(
            r#"REASON <http://ex/Person> |> VALIDATE SHAPE <http://ex/Named> USING '{SHAPES}' KEEP VIOLATING"#
        ),
        &ctx,
    );
    assert_eq!(
        violating,
        vec!["<http://ex/bob>".to_string()],
        "bob has no ex:name, so he is the reported violation"
    );
}

#[test]
fn projected_taxon_shape_checks_native_fields_and_typed_relationships() {
    let core = GraphCore::new();
    for (id, fields) in [
        (
            "source:child",
            serde_json::json!({"type":"Taxon","scientificName":"Child","termCurie":"GBIF:1"}),
        ),
        (
            "source:parent",
            serde_json::json!({"type":"Taxon","scientificName":"Parent","termCurie":"GBIF:2"}),
        ),
        (
            "source:bad",
            serde_json::json!({"type":"Taxon","scientificName":"Bad"}),
        ),
        (
            "source:rank",
            serde_json::json!({"type":"TaxonRank","name":"species"}),
        ),
    ] {
        core.add_node(id.to_string(), rmp_serde::to_vec_named(&fields).unwrap());
    }
    for (target, relation) in [
        ("source:parent", "parentTaxon"),
        ("source:rank", "taxonRank"),
    ] {
        let edge = rmp_serde::to_vec_named(&serde_json::json!({"relationship":relation})).unwrap();
        core.add_edge("source:child".to_string(), target.to_string(), edge)
            .unwrap();
    }
    let view = core.analysis_snapshot();
    let semantic = SemanticStore::new();
    let ctx = PlanCtx::new(&view, &semantic);
    let shapes =
        include_str!("../../../../medulla-core/ontology/world_model-v1.shapes.ttl").replace('\'', "''");
    let query = format!(
        r#"MATCH (:Taxon) |> VALIDATE SHAPE <http://knuckles.team/kg#TaxonShape> USING '{shapes}' PROJECT 'http://knuckles.team/kg#' KEEP VIOLATING"#
    );
    assert_eq!(ids_of(&query, &ctx), vec!["source:bad".to_string()]);

    let bad_edge =
        rmp_serde::to_vec_named(&serde_json::json!({"relationship": "taxonRank"})).unwrap();
    core.add_edge(
        "source:child".to_string(),
        "source:bad".to_string(),
        bad_edge,
    )
    .unwrap();
    let invalid_view = core.analysis_snapshot();
    let invalid_ctx = PlanCtx::new(&invalid_view, &semantic);
    let mut violating = ids_of(&query, &invalid_ctx);
    violating.sort();
    assert_eq!(
        violating,
        vec!["source:bad".to_string(), "source:child".to_string()]
    );
}
