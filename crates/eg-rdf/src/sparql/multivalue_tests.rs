//! EH-583 — a triple pattern over a multi-valued literal property binds EVERY value.
//!
//! Loading a subject with two values of one predicate keeps the first at the ordinary
//! property key and parks the rest in the reserved multivalue cell. The evaluator used to
//! read only the ordinary key, so the second value never bound and a FILTER on it could
//! not see it. These run on a plain (non-federated) graph.

use super::{execute, Dataset, Projection};

const DATA: &str = r#"
@prefix ex: <http://example.org/> .
ex:p ex:age 40, 18 ;
     ex:name "Alice", "Alys" .
ex:q ex:age 35 .
"#;

/// Sorted bindings of `var` for `query` (the `ex:` prefix is supplied).
fn column(query: &str, var: &str) -> Vec<String> {
    let view = super::view_of_turtle(DATA);
    let text = format!("PREFIX ex: <http://example.org/>\n{query}");
    let table = execute(
        &Dataset::new(&view, Vec::new()),
        &text,
        &Projection::raw(),
        None,
    )
    .unwrap()
    .into_table();
    let mut out: Vec<String> = table
        .solutions
        .iter()
        .filter_map(|s| s.get(var).map(|b| b.as_str().to_string()))
        .collect();
    out.sort();
    out
}

#[test]
fn both_values_of_one_property_bind() {
    assert_eq!(
        column("SELECT ?n WHERE { ex:p ex:name ?n }", "n"),
        vec!["Alice", "Alys"]
    );
    assert_eq!(
        column("SELECT ?a WHERE { ex:p ex:age ?a }", "a"),
        vec!["18", "40"]
    );
}

#[test]
fn a_filter_applies_to_each_binding() {
    let over = column("SELECT ?a WHERE { ?s ex:age ?a FILTER (?a > 30) }", "a");
    assert_eq!(over, vec!["35", "40"]);
    let under = column("SELECT ?a WHERE { ?s ex:age ?a FILTER (?a < 30) }", "a");
    assert_eq!(under, vec!["18"], "the second value is filtered on its own");
}

#[test]
fn two_multi_valued_properties_join_as_a_cross_product() {
    let names = column("SELECT ?n WHERE { ex:p ex:name ?n ; ex:age ?a }", "n");
    assert_eq!(names, vec!["Alice", "Alice", "Alys", "Alys"]);
}

#[test]
fn a_constant_object_matches_a_second_value() {
    let subjects = column("SELECT ?s WHERE { ?s ex:name \"Alys\" }", "s");
    assert_eq!(subjects.len(), 1, "{subjects:?}");
}
