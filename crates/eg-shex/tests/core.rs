//! ShEx Core correctness proof (CONCEPT:EG-133).
//!
//! Each test parses a ShExJ schema + a Turtle data graph, validates a shape map, and
//! asserts the report's `conforms` flag (and, on failure, that the reason mentions the
//! offending predicate / constraint). Schemas are inline ShExJ; data is inline Turtle.

use eg_shex::{validate_shexj, ShexReport};

const PREFIXES: &str = r#"
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:   <http://example.org/> .
"#;

fn run(schema: &str, data: &str, pairs: &[(&str, &str)]) -> ShexReport {
    let d = format!("{PREFIXES}{data}");
    validate_shexj(schema, &d, pairs).expect("parse + validate")
}

// A ShExJ Shape requiring exactly one ex:name of datatype xsd:string.
const PERSON_NAME_SCHEMA: &str = r#"{
  "type": "Schema",
  "shapes": [
    { "type": "Shape",
      "id": "http://example.org/PersonShape",
      "expression": {
        "type": "TripleConstraint",
        "predicate": "http://example.org/name",
        "valueExpr": { "type": "NodeConstraint", "datatype": "http://www.w3.org/2001/XMLSchema#string" },
        "min": 1, "max": 1
      } }
  ]
}"#;

// ── 1. A conforming node ────────────────────────────────────────────────
#[test]
fn conforming_node() {
    let report = run(
        PERSON_NAME_SCHEMA,
        r#"ex:alice ex:name "Alice" ."#,
        &[("http://example.org/alice", "http://example.org/PersonShape")],
    );
    assert!(report.conforms, "expected conforms; got {:?}", report.results);
    assert_eq!(report.results.len(), 1);
    assert!(report.results[0].conforms);
    assert!(report.results[0].reason.is_none());
}

// ── 2. Cardinality violation (missing required ex:name) ─────────────────
#[test]
fn cardinality_violation() {
    let report = run(
        PERSON_NAME_SCHEMA,
        r#"ex:bob ex:age "1" ."#,
        &[("http://example.org/bob", "http://example.org/PersonShape")],
    );
    assert!(!report.conforms);
    let reason = report.results[0].reason.as_deref().unwrap();
    assert!(reason.contains("name"), "reason was: {reason}");
    assert!(reason.contains("at least"), "reason was: {reason}");
}

// ── 2b. Cardinality violation (too many — max 1, two values) ────────────
#[test]
fn cardinality_too_many() {
    let report = run(
        PERSON_NAME_SCHEMA,
        r#"ex:carol ex:name "Carol", "C." ."#,
        &[("http://example.org/carol", "http://example.org/PersonShape")],
    );
    assert!(!report.conforms);
    assert!(report.results[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("at most"));
}

// ── 3a. Datatype violation ──────────────────────────────────────────────
#[test]
fn datatype_violation() {
    let schema = r#"{
      "type": "Schema",
      "shapes": [
        { "type": "Shape",
          "id": "http://example.org/AgeShape",
          "expression": {
            "type": "TripleConstraint",
            "predicate": "http://example.org/age",
            "valueExpr": { "type": "NodeConstraint", "datatype": "http://www.w3.org/2001/XMLSchema#integer" },
            "min": 1, "max": 1
          } }
      ]
    }"#;
    let bad = run(
        schema,
        r#"ex:dan ex:age "not-a-number" ."#,
        &[("http://example.org/dan", "http://example.org/AgeShape")],
    );
    assert!(!bad.conforms, "string age must not conform");

    let good = run(
        schema,
        r#"ex:dan ex:age "42"^^xsd:integer ."#,
        &[("http://example.org/dan", "http://example.org/AgeShape")],
    );
    assert!(good.conforms, "integer age conforms; got {:?}", good.results);
}

// ── 3b. nodeKind violation (value must be an IRI) ───────────────────────
#[test]
fn node_kind_violation() {
    let schema = r#"{
      "type": "Schema",
      "shapes": [
        { "type": "Shape",
          "id": "http://example.org/KnowsShape",
          "expression": {
            "type": "TripleConstraint",
            "predicate": "http://example.org/knows",
            "valueExpr": { "type": "NodeConstraint", "nodeKind": "iri" },
            "min": 1, "max": 1
          } }
      ]
    }"#;
    let bad = run(
        schema,
        r#"ex:a ex:knows "bob" ."#,
        &[("http://example.org/a", "http://example.org/KnowsShape")],
    );
    assert!(!bad.conforms, "a literal is not an IRI value");

    let good = run(
        schema,
        r#"ex:a ex:knows ex:b ."#,
        &[("http://example.org/a", "http://example.org/KnowsShape")],
    );
    assert!(good.conforms, "IRI value conforms; got {:?}", good.results);
}

// ── 4. Value-set constraint ─────────────────────────────────────────────
// A top-level NodeConstraint value set tested directly against the focus node — this
// exercises the value-set path itself (rather than folding into a cardinality reason).
#[test]
fn value_set_constraint() {
    let schema = r#"{
      "type": "Schema",
      "shapes": [
        { "type": "NodeConstraint",
          "id": "http://example.org/Color",
          "values": [
            "http://example.org/red",
            "http://example.org/green",
            "http://example.org/blue"
          ] }
      ]
    }"#;
    // ex:purple is not a member of the value set.
    let bad = run(
        schema,
        r#"ex:purple a ex:Color ."#,
        &[("http://example.org/purple", "http://example.org/Color")],
    );
    assert!(!bad.conforms);
    assert!(bad.results[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("value set"));

    // ex:green IS a member ⇒ conforms.
    let good = run(
        schema,
        r#"ex:green a ex:Color ."#,
        &[("http://example.org/green", "http://example.org/Color")],
    );
    assert!(good.conforms, "green is in the set; got {:?}", good.results);
}

// ── 5. EachOf vs OneOf ──────────────────────────────────────────────────
#[test]
fn each_of_requires_all() {
    // EachOf: a node must have BOTH ex:name and ex:age.
    let schema = r#"{
      "type": "Schema",
      "shapes": [
        { "type": "Shape",
          "id": "http://example.org/S",
          "expression": {
            "type": "EachOf",
            "expressions": [
              { "type": "TripleConstraint", "predicate": "http://example.org/name", "min": 1, "max": 1 },
              { "type": "TripleConstraint", "predicate": "http://example.org/age",  "min": 1, "max": 1 }
            ]
          } }
      ]
    }"#;
    let missing_age = run(
        schema,
        r#"ex:p ex:name "P" ."#,
        &[("http://example.org/p", "http://example.org/S")],
    );
    assert!(!missing_age.conforms, "EachOf requires age too");

    let both = run(
        schema,
        r#"ex:p ex:name "P" ; ex:age "3" ."#,
        &[("http://example.org/p", "http://example.org/S")],
    );
    assert!(both.conforms, "both present conforms; got {:?}", both.results);
}

#[test]
fn one_of_requires_exactly_one_branch() {
    // OneOf: a node must have ex:email OR ex:phone (alternation).
    let schema = r#"{
      "type": "Schema",
      "shapes": [
        { "type": "Shape",
          "id": "http://example.org/Contact",
          "expression": {
            "type": "OneOf",
            "expressions": [
              { "type": "TripleConstraint", "predicate": "http://example.org/email", "min": 1, "max": 1 },
              { "type": "TripleConstraint", "predicate": "http://example.org/phone", "min": 1, "max": 1 }
            ]
          } }
      ]
    }"#;
    let email_only = run(
        schema,
        r#"ex:c ex:email "a@b.c" ."#,
        &[("http://example.org/c", "http://example.org/Contact")],
    );
    assert!(email_only.conforms, "email satisfies OneOf; got {:?}", email_only.results);

    let neither = run(
        schema,
        r#"ex:c ex:fax "123" ."#,
        &[("http://example.org/c", "http://example.org/Contact")],
    );
    assert!(!neither.conforms, "neither branch ⇒ OneOf fails");
}

// ── 6. A shape reference ────────────────────────────────────────────────
#[test]
fn shape_reference() {
    // PersonShape's ex:address value must conform to the referenced AddressShape,
    // which requires an ex:zip.
    let schema = r#"{
      "type": "Schema",
      "shapes": [
        { "type": "Shape",
          "id": "http://example.org/PersonShape",
          "expression": {
            "type": "TripleConstraint",
            "predicate": "http://example.org/address",
            "valueExpr": "http://example.org/AddressShape",
            "min": 1, "max": 1
          } },
        { "type": "Shape",
          "id": "http://example.org/AddressShape",
          "expression": {
            "type": "TripleConstraint",
            "predicate": "http://example.org/zip",
            "valueExpr": { "type": "NodeConstraint", "datatype": "http://www.w3.org/2001/XMLSchema#string" },
            "min": 1, "max": 1
          } }
      ]
    }"#;
    let no_zip = run(
        schema,
        r#"
ex:p ex:address ex:addr1 .
ex:addr1 ex:city "Springfield" .
"#,
        &[("http://example.org/p", "http://example.org/PersonShape")],
    );
    assert!(!no_zip.conforms, "address without zip fails the referenced shape");

    let with_zip = run(
        schema,
        r#"
ex:p ex:address ex:addr1 .
ex:addr1 ex:zip "12345" .
"#,
        &[("http://example.org/p", "http://example.org/PersonShape")],
    );
    assert!(with_zip.conforms, "address with zip conforms; got {:?}", with_zip.results);
}

// ── 7. CLOSED shape rejects extra triples ───────────────────────────────
#[test]
fn closed_shape_rejects_extra() {
    let schema = r#"{
      "type": "Schema",
      "shapes": [
        { "type": "Shape",
          "id": "http://example.org/ClosedPerson",
          "closed": true,
          "expression": {
            "type": "TripleConstraint",
            "predicate": "http://example.org/name",
            "min": 1, "max": 1
          } }
      ]
    }"#;
    let extra = run(
        schema,
        r#"ex:p ex:name "P" ; ex:nickname "PP" ."#,
        &[("http://example.org/p", "http://example.org/ClosedPerson")],
    );
    assert!(!extra.conforms, "closed shape must reject ex:nickname");
    assert!(extra.results[0].reason.as_deref().unwrap().contains("extra"));

    let clean = run(
        schema,
        r#"ex:p ex:name "P" ."#,
        &[("http://example.org/p", "http://example.org/ClosedPerson")],
    );
    assert!(clean.conforms, "only the mentioned arc ⇒ conforms; got {:?}", clean.results);
}

// ── 7b. EXTRA lets a closed shape carry an extra predicate ──────────────
#[test]
fn closed_shape_with_extra_predicate() {
    let schema = r#"{
      "type": "Schema",
      "shapes": [
        { "type": "Shape",
          "id": "http://example.org/ClosedPerson",
          "closed": true,
          "extra": [ "http://example.org/nickname" ],
          "expression": {
            "type": "TripleConstraint",
            "predicate": "http://example.org/name",
            "min": 1, "max": 1
          } }
      ]
    }"#;
    let report = run(
        schema,
        r#"ex:p ex:name "P" ; ex:nickname "PP" ."#,
        &[("http://example.org/p", "http://example.org/ClosedPerson")],
    );
    assert!(report.conforms, "extra predicate allowed; got {:?}", report.results);
}

// ── Report is serde-serializable and round-trips ────────────────────────
#[test]
fn report_serde_round_trip() {
    let bad = run(
        PERSON_NAME_SCHEMA,
        r#"ex:bob ex:age "1" ."#,
        &[("http://example.org/bob", "http://example.org/PersonShape")],
    );
    let json = serde_json::to_string(&bad).unwrap();
    assert!(json.contains("\"conforms\":false"));
    let back: ShexReport = serde_json::from_str(&json).unwrap();
    assert_eq!(back, bad);
}
