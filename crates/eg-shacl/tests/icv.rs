//! ICV — Integrity Constraint Validation correctness proof (CONCEPT:EG-KG.ontology.wired-into-commit-write).
//!
//! Proves the closed-world reading of SHACL shapes as DB integrity constraints:
//! a missing required property is a VIOLATION (not open-world "unknown"); datatype /
//! class / pattern / `sh:in` breaches are detected with a focus node + message; a valid
//! graph conforms clean; every violation carries a SPARQL explain witness; and the
//! [`check_write`] guard rejects a change that introduces a violation while accepting
//! one that does not.

use eg_shacl::{
    check_write, graph_from_turtle, validate_icv, validate_icv_turtle,
    validate_icv_with_inferences, IcvReport, Severity,
};

const PREFIXES: &str = r#"
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:   <http://example.org/> .
"#;

fn run(shapes: &str, data: &str) -> IcvReport {
    let s = format!("{PREFIXES}{shapes}");
    let d = format!("{PREFIXES}{data}");
    validate_icv_turtle(&s, &d).expect("parse + ICV validate")
}

fn has_component(report: &IcvReport, frag: &str) -> bool {
    report
        .violations
        .iter()
        .any(|v| v.result.constraint_component.contains(frag))
}

// ── EG-146: closed-world — a missing required property IS a violation ───────
#[test]
fn eg146_icv_closed_world_missing_required_property_is_violation() {
    // Under the CLOSED-world reading, a targeted Person with NO ex:name asserted is a
    // hard integrity violation — NOT an open-world "the name might exist elsewhere".
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"#;
    let report = run(shapes, r#"ex:bob a ex:Person ."#);
    assert!(
        !report.conforms,
        "missing required property must violate ICV"
    );
    assert!(has_component(&report, "MinCount"));

    let v = &report.violations[0];
    assert_eq!(v.result.focus_node, "<http://example.org/bob>");
    assert_eq!(v.result.path.as_deref(), Some("<http://example.org/name>"));
    assert_eq!(v.result.severity, Severity::Violation);
    assert!(v.result.message.is_some(), "human-readable message present");
}

// ── EG-146: a fully-valid graph passes clean ────────────────────────────────
#[test]
fn eg146_icv_valid_graph_conforms_clean() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:name ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string ;
    ] .
"#;
    let report = run(shapes, r#"ex:alice a ex:Person ; ex:name "Alice" ."#);
    assert!(
        report.conforms,
        "valid graph must conform; got {:?}",
        report.violations
    );
    assert!(report.violations.is_empty());
}

// ── EG-146: datatype / class / pattern / sh:in violations w/ focus + message ─
#[test]
fn eg146_icv_datatype_violation_has_focus_and_message() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:age ; sh:datatype xsd:integer ] .
"#;
    let report = run(shapes, r#"ex:dan a ex:Person ; ex:age "not-a-number" ."#);
    assert!(!report.conforms);
    assert!(has_component(&report, "Datatype"));
    let v = &report.violations[0];
    assert_eq!(v.result.focus_node, "<http://example.org/dan>");
    assert!(v.result.value.is_some(), "offending value reported");
    assert!(
        v.witness.contains("datatype(?value)"),
        "witness: {}",
        v.witness
    );
}

#[test]
fn eg146_icv_class_membership_violation() {
    let shapes = r#"
ex:OwnerShape a sh:NodeShape ;
    sh:targetClass ex:Owner ;
    sh:property [ sh:path ex:pet ; sh:class ex:Dog ] .
"#;
    let bad = run(
        shapes,
        r#"ex:o a ex:Owner ; ex:pet ex:fluffy . ex:fluffy a ex:Cat ."#,
    );
    assert!(!bad.conforms);
    assert!(has_component(&bad, "Class"));
    // The witness closes the world: NOT EXISTS an asserted rdf:type edge to the class.
    let v = bad
        .violations
        .iter()
        .find(|v| v.result.constraint_component.contains("Class"))
        .unwrap();
    assert!(
        v.witness.contains("FILTER NOT EXISTS"),
        "witness: {}",
        v.witness
    );

    let good = run(
        shapes,
        r#"ex:o a ex:Owner ; ex:pet ex:rex . ex:rex a ex:Dog ."#,
    );
    assert!(good.conforms, "dog conforms; got {:?}", good.violations);
}

#[test]
fn eg146_icv_pattern_and_in_violations() {
    let shapes = r#"
ex:CodeShape a sh:NodeShape ;
    sh:targetClass ex:Item ;
    sh:property [ sh:path ex:code ; sh:pattern "^[A-Z]{3}$" ] ;
    sh:property [ sh:path ex:color ; sh:in ( "red" "green" "blue" ) ] .
"#;
    let bad = run(
        shapes,
        r#"ex:i a ex:Item ; ex:code "abcd" ; ex:color "purple" ."#,
    );
    assert!(!bad.conforms);
    assert!(has_component(&bad, "Pattern"));
    assert!(has_component(&bad, "In"));

    let pat = bad
        .violations
        .iter()
        .find(|v| v.result.constraint_component.contains("Pattern"))
        .unwrap();
    assert!(
        pat.witness.contains("REGEX"),
        "pattern witness: {}",
        pat.witness
    );
    let inv = bad
        .violations
        .iter()
        .find(|v| v.result.constraint_component.contains("In"))
        .unwrap();
    assert!(
        inv.witness.contains("NOT IN"),
        "in witness: {}",
        inv.witness
    );

    let good = run(
        shapes,
        r#"ex:i a ex:Item ; ex:code "XYZ" ; ex:color "green" ."#,
    );
    assert!(good.conforms, "conforming item; got {:?}", good.violations);
}

// ── EG-146: every violation carries a SPARQL explain witness ────────────────
#[test]
fn eg146_icv_explain_witness_present_for_every_violation() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] ;
    sh:property [ sh:path ex:age ; sh:datatype xsd:integer ] .
"#;
    let report = run(shapes, r#"ex:bob a ex:Person ; ex:age "old" ."#);
    assert!(!report.conforms);
    assert!(!report.violations.is_empty());
    for v in &report.violations {
        assert!(!v.witness.trim().is_empty(), "witness must be non-empty");
        assert!(
            v.witness
                .contains("CONCEPT:EG-KG.ontology.wired-into-commit-write"),
            "witness cites EG-146: {}",
            v.witness
        );
        // The witness references the focus node so a user can run it as-is.
        assert!(
            v.witness.contains("<http://example.org/bob>"),
            "witness anchors the focus node: {}",
            v.witness
        );
    }
    // The minCount witness is a COUNT query showing the closed-world shortfall.
    let mc = report
        .violations
        .iter()
        .find(|v| v.result.constraint_component.contains("MinCount"))
        .unwrap();
    assert!(
        mc.witness.contains("COUNT(?value)"),
        "minCount witness: {}",
        mc.witness
    );
}

// ── EG-146: check_write guard — reject a change that introduces a violation ─
#[test]
fn eg146_icv_check_write_rejects_violating_change() {
    let shapes_ttl = format!(
        "{PREFIXES}{}",
        r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:email ; sh:maxCount 1 ] .
"#
    );
    let shapes = graph_from_turtle(&shapes_ttl).unwrap();
    // Base: Alice has one email — conforms.
    let base = graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:alice a ex:Person ; ex:email "a@x.org" ."#
    ))
    .unwrap();
    assert!(
        validate_icv(&shapes, &base).unwrap().conforms,
        "base is clean"
    );

    // Proposed: add a SECOND email → breaks sh:maxCount 1.
    let additions = eg_shacl::graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:alice ex:email "a2@x.org" ."#
    ))
    .unwrap();
    let additions: Vec<_> = additions.iter().map(|t| t.into_owned()).collect();

    let check = check_write(&shapes, &base, &additions, &[]).unwrap();
    assert!(!check.accepted, "guard must REJECT a maxCount-breaking add");
    assert!(
        !check.introduced.is_empty(),
        "introduced violation reported"
    );
    assert!(check
        .introduced
        .iter()
        .any(|v| v.result.constraint_component.contains("MaxCount")));
    // The introduced violation carries its explain witness.
    assert!(check.introduced[0]
        .witness
        .contains("CONCEPT:EG-KG.ontology.wired-into-commit-write"));
}

// ── EG-146: check_write guard — accept a non-violating change ───────────────
#[test]
fn eg146_icv_check_write_accepts_clean_change() {
    let shapes_ttl = format!(
        "{PREFIXES}{}",
        r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"#
    );
    let shapes = graph_from_turtle(&shapes_ttl).unwrap();
    let base = graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:alice a ex:Person ; ex:name "Alice" ."#
    ))
    .unwrap();

    // Add an unrelated fact — no constraint touched.
    let additions =
        graph_from_turtle(&format!("{PREFIXES}{}", r#"ex:alice ex:nickname "Al" ."#)).unwrap();
    let additions: Vec<_> = additions.iter().map(|t| t.into_owned()).collect();

    let check = check_write(&shapes, &base, &additions, &[]).unwrap();
    assert!(
        check.accepted,
        "guard must ACCEPT a clean add; {:?}",
        check.introduced
    );
    assert!(check.introduced.is_empty());
}

// ── EG-146: check_write guard — removing a required value introduces a breach ─
#[test]
fn eg146_icv_check_write_rejects_removal_that_breaks_min_count() {
    let shapes_ttl = format!(
        "{PREFIXES}{}",
        r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"#
    );
    let shapes = graph_from_turtle(&shapes_ttl).unwrap();
    let base = graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:alice a ex:Person ; ex:name "Alice" ."#
    ))
    .unwrap();

    // Remove the only ex:name → closed-world minCount breach.
    let removals =
        graph_from_turtle(&format!("{PREFIXES}{}", r#"ex:alice ex:name "Alice" ."#)).unwrap();
    let removals: Vec<_> = removals.iter().map(|t| t.into_owned()).collect();

    let check = check_write(&shapes, &base, &[], &removals).unwrap();
    assert!(
        !check.accepted,
        "removing a required value must be rejected"
    );
    assert!(check
        .introduced
        .iter()
        .any(|v| v.result.constraint_component.contains("MinCount")));
}

// ── EG-146: reasoned-view ICV — an inferred type satisfies sh:class ─────────
#[test]
fn eg146_icv_over_reasoned_view_inferred_type_satisfies_class() {
    let shapes = graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"
ex:OwnerShape a sh:NodeShape ;
    sh:targetClass ex:Owner ;
    sh:property [ sh:path ex:pet ; sh:class ex:Animal ] .
"#
    ))
    .unwrap();
    // Asserted: fluffy is a Cat, but NOT (asserted) an Animal → closed-world fails.
    let data = graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:o a ex:Owner ; ex:pet ex:fluffy . ex:fluffy a ex:Cat ."#
    ))
    .unwrap();
    assert!(
        !validate_icv(&shapes, &data).unwrap().conforms,
        "asserted-only: Cat is not an Animal, so sh:class fails closed-world"
    );

    // Reasoned view: OWL would infer `ex:fluffy a ex:Animal` (Cat ⊑ Animal). Supply that
    // inference and ICV now conforms — constraints checked against the ENTAILED model.
    let inferred =
        graph_from_turtle(&format!("{PREFIXES}{}", r#"ex:fluffy a ex:Animal ."#)).unwrap();
    let inferred: Vec<_> = inferred.iter().map(|t| t.into_owned()).collect();
    let reasoned = validate_icv_with_inferences(&shapes, &data, &inferred).unwrap();
    assert!(
        reasoned.conforms,
        "reasoned view: inferred type satisfies sh:class; got {:?}",
        reasoned.violations
    );
}

// ── EG-146: report is serde round-trippable ─────────────────────────────────
#[test]
fn eg146_icv_report_serde_roundtrip() {
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"#;
    let report = run(shapes, r#"ex:bob a ex:Person ."#);
    let json = serde_json::to_string(&report).unwrap();
    assert!(json.contains("\"conforms\":false"));
    assert!(json.contains("witness"));
    let back: IcvReport = serde_json::from_str(&json).unwrap();
    assert_eq!(back, report);
}

// ── W4.13: sh:sparql through the ICV layer — witness IS the constraint's own SELECT ─

#[test]
fn w413_icv_sparql_violation_carries_the_constraints_own_select_as_witness() {
    // sh:sparql (CONCEPT:EG-KG.ontology.concept-6): a Person's ex:age must be non-negative, checked via a
    // SPARQL FILTER rather than sh:minInclusive, so the SAME evaluator this task
    // added (crate::sparql) is what ICV's witness-building now goes through too.
    // The embedded SPARQL text uses the full <http://example.org/age> IRI (not
    // the ex: prefix) deliberately: this constraint carries no sh:prefixes/
    // sh:declare, so `ex:` would be undeclared inside the SELECT text -- exactly
    // as it should be (this crate implements the NORMATIVE sh:declare
    // resolution mechanism only, see sparql.rs's module docs).
    let shapes = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:sparql [
        sh:message "age must be non-negative" ;
        sh:select """SELECT $this ?value WHERE { $this <http://example.org/age> ?value . FILTER (?value < 0) }""" ;
    ] .
"#;
    let report = run(shapes, r#"ex:bob a ex:Person ; ex:age "-5"^^xsd:integer ."#);
    assert!(!report.conforms, "a negative age must violate");
    let v = report
        .violations
        .iter()
        .find(|v| {
            v.result
                .constraint_component
                .contains("SPARQLConstraintComponent")
        })
        .expect("an sh:sparql violation is reported");
    assert_eq!(v.result.focus_node, "<http://example.org/bob>");
    assert_eq!(
        v.result.message.as_deref(),
        Some("age must be non-negative")
    );
    // The witness is not a generic fallback query -- it is literally the shape's
    // OWN sh:select text (the strongest possible "run this to see the offending
    // data" proof for a SPARQL-based constraint).
    assert!(
        v.witness.contains("FILTER (?value < 0)"),
        "witness embeds the constraint's own SELECT verbatim: {}",
        v.witness
    );
    assert!(v
        .witness
        .contains("CONCEPT:EG-KG.ontology.wired-into-commit-write"));

    // A non-negative age does not violate.
    let good = run(
        shapes,
        r#"ex:alice a ex:Person ; ex:age "30"^^xsd:integer ."#,
    );
    assert!(good.conforms, "got {:?}", good.violations);
}

#[test]
fn w413_icv_check_write_rejects_a_change_that_breaks_an_sh_sparql_constraint() {
    // The [`check_write`] guard (the pure decision function the native-write ICV
    // extension and the RDF write guard both sit on) must reject a write that
    // introduces an sh:sparql violation exactly like it already does for the
    // structural constraint components.
    let shapes_ttl = format!(
        "{PREFIXES}{}",
        r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:sparql [
        sh:select """SELECT $this ?value WHERE { $this <http://example.org/age> ?value . FILTER (?value < 0) }""" ;
    ] .
"#
    );
    let shapes = graph_from_turtle(&shapes_ttl).unwrap();
    let base = graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:alice a ex:Person ; ex:age "30"^^xsd:integer ."#
    ))
    .unwrap();
    assert!(
        validate_icv(&shapes, &base).unwrap().conforms,
        "base is clean"
    );

    // Proposed: overwrite alice's age with a negative one (remove the old, add the new).
    let removals = eg_shacl::graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:alice ex:age "30"^^xsd:integer ."#
    ))
    .unwrap();
    let removals: Vec<_> = removals.iter().map(|t| t.into_owned()).collect();
    let additions = eg_shacl::graph_from_turtle(&format!(
        "{PREFIXES}{}",
        r#"ex:alice ex:age "-1"^^xsd:integer ."#
    ))
    .unwrap();
    let additions: Vec<_> = additions.iter().map(|t| t.into_owned()).collect();

    let check = check_write(&shapes, &base, &additions, &removals).unwrap();
    assert!(
        !check.accepted,
        "guard must REJECT a change that makes ex:age negative"
    );
    assert!(check.introduced.iter().any(|v| v
        .result
        .constraint_component
        .contains("SPARQLConstraintComponent")));
}
