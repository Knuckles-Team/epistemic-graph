//! ShExC (compact syntax) parser correctness proof (CONCEPT:EG-KG.compute.concept-2).
//!
//! [`round_trip_matches_programmatic_build`] is the task's acceptance test: a ShExC
//! document parses to the EXACT SAME [`Schema`] an equivalent programmatic build
//! produces (`Schema`/`ShapeExpr`/`TripleExpr`/`NodeConstraint` now derive
//! `PartialEq` for this). [`parsed_schema_validates_real_data`] proves the parsed
//! schema is not just structurally equal but functionally correct: run through
//! [`eg_shex::validate`] against real data, same as a ShExJ-built schema would be.

use std::collections::HashMap;

use eg_shex::schema::{
    NodeConstraint, NodeKind, NumericFacets, Schema, Shape, ShapeExpr, StringFacets, TripleExpr,
    ValueSetValue,
};
use eg_shex::{validate, ShapeMap};

const EX: &str = "http://example.org/";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

const SHEXC: &str = r#"
PREFIX ex: <http://example.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

START = @ex:PersonShape

ex:PersonShape CLOSED EXTRA ex:nickname {
  ex:name xsd:string ;
  ex:age xsd:integer MININCLUSIVE 0 ? ;
  ex:tag [ex:red ex:green ex:blue] * ;
  (ex:phone xsd:string | ex:fax xsd:string) ;
  ex:address @ex:AddressShape
}

ex:AddressShape {
  ex:zip xsd:string MINLENGTH 3 MAXLENGTH 10
}
"#;

fn nc_datatype(dt: &str) -> ShapeExpr {
    ShapeExpr::NodeConstraint(NodeConstraint {
        datatype: Some(dt.to_string()),
        ..Default::default()
    })
}

fn tc(predicate: &str, value_expr: ShapeExpr, min: i64, max: i64) -> TripleExpr {
    TripleExpr::TripleConstraint {
        predicate: predicate.to_string(),
        value_expr: Some(Box::new(value_expr)),
        min,
        max,
        inverse: false,
    }
}

/// The SAME schema `SHEXC` describes, built by hand through the programmatic
/// `Schema`/`ShapeExpr`/`TripleExpr`/`NodeConstraint` constructors this crate's
/// ShExJ path (and the rest of the crate) already uses.
fn programmatic_schema() -> Schema {
    let age_expr = ShapeExpr::NodeConstraint(NodeConstraint {
        datatype: Some(XSD_INTEGER.to_string()),
        numeric_facets: NumericFacets {
            mininclusive: Some(0.0),
            ..Default::default()
        },
        ..Default::default()
    });
    let tag_expr = ShapeExpr::NodeConstraint(NodeConstraint {
        values: Some(vec![
            ValueSetValue::Iri(format!("{EX}red")),
            ValueSetValue::Iri(format!("{EX}green")),
            ValueSetValue::Iri(format!("{EX}blue")),
        ]),
        ..Default::default()
    });
    let zip_expr = ShapeExpr::NodeConstraint(NodeConstraint {
        datatype: Some(XSD_STRING.to_string()),
        string_facets: StringFacets {
            minlength: Some(3),
            maxlength: Some(10),
            ..Default::default()
        },
        ..Default::default()
    });

    let person_expression = TripleExpr::EachOf(vec![
        tc(&format!("{EX}name"), nc_datatype(XSD_STRING), 1, 1),
        tc(&format!("{EX}age"), age_expr, 0, 1),
        tc(&format!("{EX}tag"), tag_expr, 0, -1),
        TripleExpr::OneOf(vec![
            tc(&format!("{EX}phone"), nc_datatype(XSD_STRING), 1, 1),
            tc(&format!("{EX}fax"), nc_datatype(XSD_STRING), 1, 1),
        ]),
        tc(
            &format!("{EX}address"),
            ShapeExpr::Ref(format!("{EX}AddressShape")),
            1,
            1,
        ),
    ]);

    let mut shapes = HashMap::new();
    shapes.insert(
        format!("{EX}PersonShape"),
        ShapeExpr::Shape(Shape {
            expression: Some(person_expression),
            closed: true,
            extra: vec![format!("{EX}nickname")],
        }),
    );
    shapes.insert(
        format!("{EX}AddressShape"),
        ShapeExpr::Shape(Shape {
            expression: Some(tc(&format!("{EX}zip"), zip_expr, 1, 1)),
            closed: false,
            extra: Vec::new(),
        }),
    );

    Schema {
        shapes,
        start: Some(ShapeExpr::Ref(format!("{EX}PersonShape"))),
    }
}

#[test]
fn round_trip_matches_programmatic_build() {
    let parsed = Schema::from_shexc(SHEXC).expect("ShExC document must parse");
    let expected = programmatic_schema();
    assert_eq!(
        parsed, expected,
        "ShExC parse must produce the identical Schema an equivalent programmatic build does"
    );
}

/// The round-trip proof above is structural; this proves the PARSED schema also
/// VALIDATES correctly (a closed shape with EXTRA, a value set, numeric/string
/// facets, a `OneOf` alternation, and a nested shape reference all actually work,
/// not just deserialize to the right shape).
#[test]
fn parsed_schema_validates_real_data() {
    let schema = Schema::from_shexc(SHEXC).expect("ShExC document must parse");

    let good_data = format!(
        r#"
@prefix ex: <{EX}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice ex:name "Alice" ; ex:age "30"^^xsd:integer ; ex:tag ex:red ;
    ex:phone "555-1234" ; ex:address ex:addr1 ; ex:nickname "Al" .
ex:addr1 ex:zip "12345" .
"#
    );
    let graph = eg_shex::graph_from_turtle(&good_data).unwrap();
    let map =
        ShapeMap::from_iri_pairs(&[("http://example.org/alice", "http://example.org/PersonShape")]);
    let report = validate(&schema, &graph, &map);
    assert!(
        report.conforms,
        "a fully valid Person must conform; got {:?}",
        report.results
    );

    // Violates: sh:closed EXTRA -- an undeclared predicate (ex:extra) on a CLOSED
    // shape not covered by EXTRA (ex:nickname) is rejected.
    let closed_violation = format!(
        r#"
@prefix ex: <{EX}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:bob ex:name "Bob" ; ex:age "40"^^xsd:integer ; ex:tag ex:blue ;
    ex:fax "555-9999" ; ex:address ex:addr2 ; ex:extra "not allowed" .
ex:addr2 ex:zip "99999" .
"#
    );
    let graph = eg_shex::graph_from_turtle(&closed_violation).unwrap();
    let map =
        ShapeMap::from_iri_pairs(&[("http://example.org/bob", "http://example.org/PersonShape")]);
    let report = validate(&schema, &graph, &map);
    assert!(
        !report.conforms,
        "an undeclared, non-EXTRA predicate on a CLOSED shape must be rejected"
    );

    // Violates: neither ex:phone nor ex:fax present -> the OneOf alternation fails.
    let no_contact = format!(
        r#"
@prefix ex: <{EX}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:carol ex:name "Carol" ; ex:age "25"^^xsd:integer ; ex:tag ex:green ;
    ex:address ex:addr3 .
ex:addr3 ex:zip "00000" .
"#
    );
    let graph = eg_shex::graph_from_turtle(&no_contact).unwrap();
    let map =
        ShapeMap::from_iri_pairs(&[("http://example.org/carol", "http://example.org/PersonShape")]);
    let report = validate(&schema, &graph, &map);
    assert!(
        !report.conforms,
        "missing both ex:phone and ex:fax must fail the OneOf alternation"
    );

    // Violates: ex:zip too short for AddressShape's MINLENGTH 3 (nested shape ref).
    let bad_address = format!(
        r#"
@prefix ex: <{EX}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:dan ex:name "Dan" ; ex:age "50"^^xsd:integer ; ex:tag ex:red ;
    ex:phone "555-0000" ; ex:address ex:addr4 .
ex:addr4 ex:zip "1" .
"#
    );
    let graph = eg_shex::graph_from_turtle(&bad_address).unwrap();
    let map =
        ShapeMap::from_iri_pairs(&[("http://example.org/dan", "http://example.org/PersonShape")]);
    let report = validate(&schema, &graph, &map);
    assert!(
        !report.conforms,
        "a zip shorter than the nested AddressShape's MINLENGTH 3 must be rejected"
    );
}

/// `BASE` + a relative shape label, `NOT`, `AND`, and a bare `IRI` node-kind
/// constraint — a second, smaller document exercising constructs the main
/// round-trip fixture above does not.
#[test]
fn base_directive_and_not_and_iri_nodekind() {
    let doc = r#"
BASE <http://example.org/>
PREFIX ex: <http://example.org/>

<NotALiteralShape> NOT LITERAL

<RefShape> IRI @<Target>
<Target> { ex:p . }
"#;
    let schema = Schema::from_shexc(doc).expect("BASE + NOT + IRI@ref must parse");
    match schema.shapes.get("http://example.org/NotALiteralShape") {
        Some(ShapeExpr::Not(inner)) => {
            assert!(matches!(
                inner.as_ref(),
                ShapeExpr::NodeConstraint(NodeConstraint {
                    node_kind: Some(NodeKind::Literal),
                    ..
                })
            ));
        }
        other => panic!("expected NOT LITERAL, got {other:?}"),
    }
    match schema.shapes.get("http://example.org/RefShape") {
        Some(ShapeExpr::And(branches)) => {
            assert_eq!(branches.len(), 2);
            assert!(matches!(
                branches[0],
                ShapeExpr::NodeConstraint(NodeConstraint {
                    node_kind: Some(NodeKind::Iri),
                    ..
                })
            ));
            assert!(matches!(&branches[1], ShapeExpr::Ref(l) if l == "http://example.org/Target"));
        }
        other => panic!("expected IRI AND @<Target>, got {other:?}"),
    }
}

/// A malformed document (an undeclared prefix) must be a clean parse error, never
/// a panic or a silently wrong (e.g. empty) schema.
#[test]
fn undeclared_prefix_is_a_parse_error_not_a_panic() {
    let err = Schema::from_shexc("ex:Foo { ex:bar xsd:string }")
        .expect_err("an undeclared `ex:`/`xsd:` prefix must be rejected");
    assert!(
        err.contains("undeclared prefix") || err.contains("ShExC"),
        "{err}"
    );
}

/// A cardinality directly on a parenthesized triple-expression GROUP has no
/// representation in this crate's `TripleExpr` model (only a leaf
/// `TripleConstraint` carries min/max) — this must fail loud, never silently
/// drop the cardinality (which would validate data that should be rejected, or
/// vice versa).
#[test]
fn group_cardinality_is_rejected_not_silently_dropped() {
    let err = Schema::from_shexc("PREFIX ex: <http://example.org/>\nex:S { (ex:a . ; ex:b .)+ }")
        .expect_err("a `+` on a parenthesized group must be a parse error");
    assert!(err.contains("cardinality"), "{err}");
}
