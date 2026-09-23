use eg_core::graph::{GraphCore, GraphView};
use eg_rdf::mapping::{load_triples, parse_turtle, IriStore};
use eg_types::wire::ShapeKeep;

use super::validate_shape;
use crate::rowset::RowSet;

const SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
ex:Named a sh:NodeShape ; sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"#;

fn people() -> GraphView {
    let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice a ex:Person ; ex:name "Alice" .
ex:bob   a ex:Person ; ex:age "3" .
ex:carol a ex:Person ; ex:name "Carol" .
"#;
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "g", parse_turtle(ttl).unwrap()).unwrap();
    core.analysis_snapshot()
}

fn rows() -> RowSet {
    RowSet::from_ids(
        ["alice", "bob", "carol"]
            .into_iter()
            .map(|who| format!("<http://ex/{who}>")),
    )
}

fn ids(rows: &RowSet) -> Vec<&str> {
    rows.rows().iter().map(|row| row.id.as_str()).collect()
}

#[test]
fn conforming_rows_are_kept_in_order() {
    let out = validate_shape(
        &people(),
        rows(),
        "<http://ex/Named>",
        SHAPES,
        ShapeKeep::Conforming,
    )
    .unwrap();
    assert_eq!(ids(&out), vec!["<http://ex/alice>", "<http://ex/carol>"]);
}

#[test]
fn violating_rows_are_the_complement() {
    let out = validate_shape(
        &people(),
        rows(),
        "http://ex/Named",
        SHAPES,
        ShapeKeep::Violating,
    )
    .unwrap();
    assert_eq!(ids(&out), vec!["<http://ex/bob>"]);
}

#[test]
fn an_undeclared_shape_or_missing_shapes_graph_is_an_error() {
    let view = people();
    let missing = validate_shape(
        &view,
        rows(),
        "<http://ex/Nope>",
        SHAPES,
        ShapeKeep::Conforming,
    );
    assert!(missing.unwrap_err().contains("not declared"));
    let no_shapes = validate_shape(
        &view,
        rows(),
        "<http://ex/Named>",
        " ",
        ShapeKeep::Conforming,
    );
    assert!(no_shapes.unwrap_err().contains("needs a shapes graph"));
}

#[test]
fn a_row_without_rdf_identity_is_refused_and_empty_input_stays_empty() {
    let view = people();
    let plain = RowSet::from_ids(["alice".to_string()]);
    let error = validate_shape(
        &view,
        plain,
        "<http://ex/Named>",
        SHAPES,
        ShapeKeep::Conforming,
    )
    .unwrap_err();
    assert!(error.contains("not an RDF resource term"), "{error}");
    let empty = validate_shape(
        &view,
        RowSet::new(),
        "<http://ex/Named>",
        SHAPES,
        ShapeKeep::Violating,
    )
    .unwrap();
    assert!(empty.is_empty());
}
