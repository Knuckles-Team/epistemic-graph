//! ICV — Integrity Constraint Validation, exercised through the eg-rdf test surface
//! (CONCEPT:EG-KG.ontology.wired-into-commit-write).
//!
//! ICV is implemented in `eg-shacl` (which sits ABOVE eg-rdf in the DAG and reuses the
//! SHACL Core shape machinery, CONCEPT:EG-KG.ontology.concept-6). This integration test drives it via a
//! DEV-dependency so the closed-world behaviour is verifiable from
//! `cargo test -p eg-rdf --features sparql`: a missing required property is a VIOLATION;
//! datatype/class/pattern/in breaches are detected with a focus node + message; a valid
//! graph conforms clean; every violation carries an explain witness; and the write guard
//! rejects a change that introduces a violation while accepting one that does not.

use eg_shacl::{check_write, graph_from_turtle, validate_icv, validate_icv_turtle};

const PREFIXES: &str = r#"
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:   <http://example.org/> .
"#;

const SHAPES: &str = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name  ; sh:minCount 1 ; sh:datatype xsd:string ] ;
    sh:property [ sh:path ex:email ; sh:maxCount 1 ] ;
    sh:property [ sh:path ex:age   ; sh:datatype xsd:integer ] .
"#;

fn icv(data: &str) -> eg_shacl::IcvReport {
    validate_icv_turtle(&format!("{PREFIXES}{SHAPES}"), &format!("{PREFIXES}{data}"))
        .expect("parse + ICV")
}

#[test]
fn eg146_icv_closed_world_and_witness_from_eg_rdf() {
    // Closed-world: no ex:name asserted ⇒ minCount VIOLATION (not open-world "unknown"),
    // and a non-integer age ⇒ datatype VIOLATION.
    let report = icv(r#"ex:bob a ex:Person ; ex:age "old" ."#);
    assert!(!report.conforms);
    assert!(report
        .violations
        .iter()
        .any(|v| v.result.constraint_component.contains("MinCount")));
    assert!(report
        .violations
        .iter()
        .any(|v| v.result.constraint_component.contains("Datatype")));
    // Every violation is self-explaining (SPARQL witness citing EG-146, anchored at focus).
    for v in &report.violations {
        assert!(v
            .witness
            .contains("CONCEPT:EG-KG.ontology.wired-into-commit-write"));
        assert!(v.witness.contains("<http://example.org/bob>"));
        assert!(v.result.message.is_some());
    }

    // A fully valid graph conforms clean.
    let good = icv(r#"ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer ."#);
    assert!(
        good.conforms,
        "valid graph conforms; got {:?}",
        good.violations
    );
}

#[test]
fn eg146_icv_check_write_guard_from_eg_rdf() {
    let shapes = graph_from_turtle(&format!("{PREFIXES}{SHAPES}")).unwrap();
    let base = graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:a a ex:Person ; ex:name "A" ."#
    ))
    .unwrap();
    assert!(validate_icv(&shapes, &base).unwrap().conforms);

    // Rejecting: a second email breaks sh:maxCount 1.
    let bad_add =
        graph_from_turtle(&format!("{PREFIXES}{}", r#"ex:a ex:email "x@y" , "z@y" ."#)).unwrap();
    let bad_add: Vec<_> = bad_add.iter().map(|t| t.into_owned()).collect();
    let rejected = check_write(&shapes, &base, &bad_add, &[]).unwrap();
    assert!(!rejected.accepted, "maxCount-breaking add must be rejected");
    assert!(!rejected.introduced.is_empty());

    // Accepting: an unrelated fact introduces no violation.
    let ok_add = graph_from_turtle(&format!("{PREFIXES}{}", r#"ex:a ex:nickname "Ay" ."#)).unwrap();
    let ok_add: Vec<_> = ok_add.iter().map(|t| t.into_owned()).collect();
    let accepted = check_write(&shapes, &base, &ok_add, &[]).unwrap();
    assert!(
        accepted.accepted,
        "clean add must be accepted; {:?}",
        accepted.introduced
    );
    assert!(accepted.introduced.is_empty());
}
