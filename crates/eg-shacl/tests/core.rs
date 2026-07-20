//! SHACL Core correctness proof (CONCEPT:EG-KG.ontology.concept-6).
//!
//! Each test parses a shapes graph + a data graph from Turtle and asserts the report's
//! `conforms` flag and (on failure) the reported constraint component.

use eg_shacl::{validate_turtle, ValidationReport};

const PREFIXES: &str = r#"
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:   <http://example.org/> .
"#;

fn run(shapes: &str, data: &str) -> ValidationReport {
    let s = format!("{PREFIXES}{shapes}");
    let d = format!("{PREFIXES}{data}");
    validate_turtle(&s, &d).expect("parse + validate")
}

/// Every result's component IRI contains `frag` for at least one result.
fn has_component(report: &ValidationReport, frag: &str) -> bool {
    report
        .results
        .iter()
        .any(|r| r.constraint_component.contains(frag))
}

// ── 1. A conforming graph ──────────────────────────────────────────────
#[test]
fn conforming_graph() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:name ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
        sh:datatype xsd:string ;
    ] .
"#;
    let data = r#"
ex:alice a ex:Person ;
    ex:name "Alice" .
"#;
    let report = run(shapes, data);
    assert!(
        report.conforms,
        "expected conforms; got {:?}",
        report.results
    );
    assert!(report.results.is_empty());
}

// ── 2. sh:minCount violation ───────────────────────────────────────────
#[test]
fn min_count_violation() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"#;
    let data = r#"ex:bob a ex:Person ."#;
    let report = run(shapes, data);
    assert!(!report.conforms);
    assert!(has_component(&report, "MinCount"));
    // The result path points at ex:name.
    assert!(report.results[0].path.as_deref() == Some("<http://example.org/name>"));
}

// ── 3. sh:maxCount violation ───────────────────────────────────────────
#[test]
fn max_count_violation() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:maxCount 1 ] .
"#;
    let data = r#"
ex:carol a ex:Person ;
    ex:name "Carol", "C." .
"#;
    let report = run(shapes, data);
    assert!(!report.conforms);
    assert!(has_component(&report, "MaxCount"));
}

// ── 4. sh:datatype mismatch ────────────────────────────────────────────
#[test]
fn datatype_mismatch() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:age ; sh:datatype xsd:integer ] .
"#;
    let data = r#"
ex:dan a ex:Person ;
    ex:age "not-a-number" .
"#;
    let report = run(shapes, data);
    assert!(!report.conforms);
    assert!(has_component(&report, "Datatype"));

    // Same shape, a good integer conforms.
    let ok = run(shapes, r#"ex:dan a ex:Person ; ex:age "42"^^xsd:integer ."#);
    assert!(ok.conforms, "integer should conform; got {:?}", ok.results);
}

// ── 5. sh:pattern failure ──────────────────────────────────────────────
#[test]
fn pattern_failure() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:code ; sh:pattern "^[A-Z]{3}$" ] .
"#;
    let bad = run(shapes, r#"ex:e a ex:Person ; ex:code "abcd" ."#);
    assert!(!bad.conforms);
    assert!(has_component(&bad, "Pattern"));

    let good = run(shapes, r#"ex:e a ex:Person ; ex:code "XYZ" ."#);
    assert!(good.conforms, "XYZ should match; got {:?}", good.results);
}

// ── 6. sh:class target + sh:in ─────────────────────────────────────────
#[test]
fn class_target_and_in() {
    let shapes = r#"
ex:LightShape a sh:NodeShape ;
    sh:targetClass ex:Light ;
    sh:property [
        sh:path ex:color ;
        sh:in ( "red" "green" "blue" ) ;
    ] .
"#;
    // "purple" is not in the allowed set ⇒ InConstraintComponent.
    let bad = run(shapes, r#"ex:l1 a ex:Light ; ex:color "purple" ."#);
    assert!(!bad.conforms);
    assert!(has_component(&bad, "In"));

    // A green light conforms; a node NOT of class ex:Light is not targeted at all.
    let good = run(
        shapes,
        r#"
ex:l2 a ex:Light ; ex:color "green" .
ex:x  a ex:Other ; ex:color "purple" .
"#,
    );
    assert!(
        good.conforms,
        "green light + untargeted node; got {:?}",
        good.results
    );
}

// sh:class constraint (value node must be an instance of a class).
#[test]
fn class_constraint_on_value() {
    let shapes = r#"
ex:OwnerShape a sh:NodeShape ;
    sh:targetClass ex:Owner ;
    sh:property [ sh:path ex:pet ; sh:class ex:Dog ] .
"#;
    let bad = run(
        shapes,
        r#"
ex:o a ex:Owner ; ex:pet ex:fluffy .
ex:fluffy a ex:Cat .
"#,
    );
    assert!(!bad.conforms);
    assert!(has_component(&bad, "Class"));

    let good = run(
        shapes,
        r#"
ex:o a ex:Owner ; ex:pet ex:rex .
ex:rex a ex:Dog .
"#,
    );
    assert!(good.conforms, "dog should conform; got {:?}", good.results);
}

// ── 7. sh:not / sh:or logical ──────────────────────────────────────────
#[test]
fn logical_not() {
    // A value node must NOT be a literal (sh:not a Literal-nodeKind shape).
    let shapes = r#"
ex:S a sh:NodeShape ;
    sh:targetNode ex:focus ;
    sh:not ex:LiteralShape .
ex:LiteralShape a sh:NodeShape ;
    sh:nodeKind sh:Literal .
"#;
    // ex:focus is an IRI ⇒ NOT a literal ⇒ conforms.
    let good = run(shapes, r#"ex:focus ex:p ex:o ."#);
    assert!(
        good.conforms,
        "iri passes sh:not-literal; got {:?}",
        good.results
    );
}

#[test]
fn logical_or() {
    // ex:val must be a string OR an integer.
    let shapes = r#"
ex:S a sh:NodeShape ;
    sh:targetClass ex:Thing ;
    sh:property [
        sh:path ex:val ;
        sh:or ( [ sh:datatype xsd:string ] [ sh:datatype xsd:integer ] ) ;
    ] .
"#;
    let good = run(shapes, r#"ex:t a ex:Thing ; ex:val "42"^^xsd:integer ."#);
    assert!(
        good.conforms,
        "integer satisfies the sh:or; got {:?}",
        good.results
    );

    let bad = run(shapes, r#"ex:t a ex:Thing ; ex:val "1.5"^^xsd:double ."#);
    assert!(!bad.conforms);
    assert!(has_component(&bad, "Or"));
}

// ── 8. Nested sh:node ──────────────────────────────────────────────────
#[test]
fn nested_node_shape() {
    // A Person's ex:address value node must itself conform to ex:AddressShape,
    // which requires an ex:zip (minCount 1, string).
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:address ;
        sh:node ex:AddressShape ;
    ] .
ex:AddressShape a sh:NodeShape ;
    sh:property [
        sh:path ex:zip ;
        sh:minCount 1 ;
        sh:datatype xsd:string ;
    ] .
"#;
    // Address has no zip ⇒ inner node shape fails ⇒ NodeConstraintComponent on the outer.
    let bad = run(
        shapes,
        r#"
ex:p a ex:Person ; ex:address ex:addr1 .
ex:addr1 ex:city "Springfield" .
"#,
    );
    assert!(!bad.conforms);
    assert!(has_component(&bad, "Node"));

    // With a zip the whole thing conforms.
    let good = run(
        shapes,
        r#"
ex:p a ex:Person ; ex:address ex:addr1 .
ex:addr1 ex:zip "12345" .
"#,
    );
    assert!(
        good.conforms,
        "address with zip conforms; got {:?}",
        good.results
    );
}

// ── nodeKind + minInclusive range + report serde ───────────────────────
#[test]
fn node_kind_and_range_and_serde() {
    let shapes = r#"
ex:S a sh:NodeShape ;
    sh:targetClass ex:Measure ;
    sh:property [ sh:path ex:v ; sh:minInclusive 0 ; sh:maxInclusive 100 ] .
"#;
    let bad = run(shapes, r#"ex:m a ex:Measure ; ex:v "150"^^xsd:integer ."#);
    assert!(!bad.conforms);
    assert!(has_component(&bad, "MaxInclusive"));

    // The report is serde-serializable and round-trips.
    let json = serde_json::to_string(&bad).unwrap();
    let back: ValidationReport = serde_json::from_str(&json).unwrap();
    assert_eq!(back, bad);
    assert!(json.contains("\"conforms\":false"));

    let good = run(shapes, r#"ex:m a ex:Measure ; ex:v "50"^^xsd:integer ."#);
    assert!(good.conforms, "50 is in [0,100]; got {:?}", good.results);
}

// ── targetSubjectsOf / targetObjectsOf + minLength ─────────────────────
#[test]
fn target_subjects_and_min_length() {
    // Any subject of ex:knows must have a name of length >= 3 (a node-shape path).
    let shapes = r#"
ex:S a sh:NodeShape ;
    sh:targetSubjectsOf ex:knows ;
    sh:property [ sh:path ex:name ; sh:minLength 3 ] .
"#;
    let bad = run(shapes, r#"ex:a ex:knows ex:b ; ex:name "Al" ."#);
    assert!(!bad.conforms);
    assert!(has_component(&bad, "MinLength"));

    let good = run(shapes, r#"ex:a ex:knows ex:b ; ex:name "Alice" ."#);
    assert!(
        good.conforms,
        "Alice is long enough; got {:?}",
        good.results
    );
}
