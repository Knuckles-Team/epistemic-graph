//! FO-04 proofs (EH-563). Every query runs twice — through a RECORDING source that honours
//! the pushed filters and through a NAIVE source that ignores them (the full-fetch oracle) —
//! and the answers must be identical. The first three queries lost solutions under the
//! previous whole-map filter pushdown; the rest assert what the pushdown now ships.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::obda::{
    run_virtual, ClosureSource, ForeignRow, ObdaCompare, ObdaFilter, ObdaSource,
    ObdaSourceRegistry, TableSource, TriplesMap, VirtualGraph,
};

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";

/// id, name, age, friend — row 4 repeats subject key 1 (a non-unique key).
fn people() -> TableSource {
    TableSource::from_records(
        ["id", "name", "age", "friend_id"].map(String::from),
        [
            vec!["1".into(), "Alice".into(), "40".into(), "2".into()],
            vec!["2".into(), "Bob".into(), "20".into(), "".into()],
            vec!["3".into(), "Carol".into(), "35".into(), "2".into()],
            vec!["1".into(), "Alys".into(), "18".into(), "".into()],
        ],
    )
}

fn vgraph() -> VirtualGraph {
    VirtualGraph::new().with_map(
        TriplesMap::new("people", "http://example.org/person/{id}")
            .with_class("http://example.org/Person")
            .add_column("http://example.org/name", "name")
            .add_typed_column(
                "http://example.org/age",
                "age",
                "http://www.w3.org/2001/XMLSchema#integer",
            )
            .add_ref(
                "http://example.org/knows",
                "http://example.org/person/{friend_id}",
            ),
    )
}

/// A source that honours pushed filters and records every scan: `(filters, rows returned)`.
struct Recording {
    table: TableSource,
    scans: Mutex<Vec<(Vec<ObdaFilter>, usize)>>,
}

impl ObdaSource for Recording {
    fn scan(
        &self,
        needed: &BTreeSet<String>,
        filters: &[ObdaFilter],
    ) -> Result<Vec<ForeignRow>, String> {
        let rows = self.table.scan(needed, filters)?;
        self.scans
            .lock()
            .unwrap()
            .push((filters.to_vec(), rows.len()));
        Ok(rows)
    }
}

/// The oracle: ignore every pushed filter and projection — the naive full fetch.
fn naive_registry() -> ObdaSourceRegistry {
    let table = people();
    let naive = ClosureSource::new(
        Vec::new(),
        move |_needed: &BTreeSet<String>, _f: &[ObdaFilter]| table.scan(&BTreeSet::new(), &[]),
    );
    let mut reg = ObdaSourceRegistry::new();
    reg.register("people", Arc::new(naive));
    reg
}

fn recording() -> Arc<Recording> {
    Arc::new(Recording {
        table: people(),
        scans: Mutex::new(Vec::new()),
    })
}

/// Sorted `var` bindings of `query` over `reg`.
fn answers(reg: &ObdaSourceRegistry, query: &str, var: &str) -> Vec<String> {
    let res = run_virtual(&vgraph(), reg, &format!("{PREFIX}{query}")).unwrap();
    let mut out: Vec<String> = res
        .solutions
        .iter()
        .filter_map(|s| s.get(var).map(|b| b.as_str().to_string()))
        .collect();
    out.sort();
    out
}

/// Run `query` pushed and naive; assert equal answers; return them with the scan log.
fn both_ways(query: &str, var: &str) -> (Vec<String>, Vec<(Vec<ObdaFilter>, usize)>) {
    let rec = recording();
    let mut reg = ObdaSourceRegistry::new();
    reg.register("people", rec.clone());
    let pushed = answers(&reg, query, var);
    let naive = answers(&naive_registry(), query, var);
    assert_eq!(pushed, naive, "pushdown changed the answer of {query}");
    let scans = rec.scans.lock().unwrap().clone();
    (pushed, scans)
}

#[test]
fn a_filter_on_one_subject_keeps_the_other_subjects_triples() {
    let (names, _) = both_ways(
        "SELECT ?n WHERE { ?a ex:age ?age . FILTER (?age > 30) ?a ex:knows ?b . ?b ex:name ?n }",
        "n",
    );
    assert_eq!(names, vec!["Bob", "Bob"], "Alice and Carol both know Bob");
}

#[test]
fn a_non_unique_subject_key_keeps_every_solution() {
    let (names, _) = both_ways(
        "SELECT ?n WHERE { ?s ex:name ?n ; ex:age ?age . FILTER (?age > 30) }",
        "n",
    );
    assert_eq!(names, vec!["Alice", "Alys", "Carol"]);
}

#[test]
fn a_predicate_shared_by_two_subjects_is_not_filtered() {
    let (ages, _) = both_ways(
        "SELECT ?y WHERE { ?a ex:age ?x . ?b ex:age ?y . FILTER (?x > 36) }",
        "y",
    );
    assert_eq!(
        ages.len(),
        4,
        "one filtered x (person/1 = 40) paired with every y: {ages:?}"
    );
}

#[test]
fn a_constant_subject_is_a_key_lookup() {
    let (names, scans) = both_ways(
        "SELECT ?n WHERE { <http://example.org/person/3> ex:name ?n }",
        "n",
    );
    assert_eq!(names, vec!["Carol"]);
    let key = scans
        .iter()
        .find(|(f, _)| {
            f.iter()
                .any(|f| f.column == "id" && f.op == ObdaCompare::Eq && f.value == "3")
        })
        .expect("the subject IRI must reach the source as id = 3");
    assert_eq!(key.1, 1, "one row moved, not the table");
}

#[test]
fn a_constant_object_is_a_column_equality() {
    let (people, scans) = both_ways("SELECT ?p WHERE { ?p ex:name \"Bob\" }", "p");
    assert_eq!(people.len(), 1);
    assert!(scans
        .iter()
        .any(|(f, rows)| *rows == 1 && f.iter().any(|f| f.column == "name" && f.value == "Bob")));
}

#[test]
fn the_semi_join_ships_only_anchor_keys() {
    let (names, scans) = both_ways(
        "SELECT ?n WHERE { ?p ex:name ?n ; ex:age ?age . FILTER (?age > 30) }",
        "n",
    );
    assert_eq!(names, vec!["Alice", "Alys", "Carol"]);
    assert_eq!(scans.len(), 2, "anchor scan + one reduced scan: {scans:?}");
    let (anchor, reduced) = (&scans[0], &scans[1]);
    assert!(anchor
        .0
        .iter()
        .any(|f| f.column == "age" && f.op == ObdaCompare::Gt));
    let semi = reduced
        .0
        .iter()
        .find(|f| f.op == ObdaCompare::In)
        .expect("the name scan is restricted to the anchor's keys");
    assert_eq!(semi.values, vec!["1", "3"]);
    assert_eq!(
        reduced.1, 3,
        "rows for keys 1 (twice) and 3 only — Bob never moves"
    );
}

#[test]
fn an_optional_or_union_use_disables_pushdown_for_that_predicate() {
    let (_, scans) = both_ways(
        "SELECT ?n WHERE { ?p ex:name ?n OPTIONAL { ?p ex:age ?age FILTER (?age > 30) } }",
        "n",
    );
    assert!(scans.iter().all(|(f, _)| f.is_empty()), "{scans:?}");
    both_ways(
        "SELECT ?n WHERE { { ?p ex:name ?n ; ex:age 40 } UNION { ?p ex:name ?n } }",
        "n",
    );
}

#[test]
fn in_membership_matches_string_values_only() {
    let f = ObdaFilter {
        column: "id".into(),
        op: ObdaCompare::In,
        value: String::new(),
        numeric: false,
        values: vec!["1".into(), "3".into()],
    };
    assert!(f.matches("3"));
    assert!(!f.matches("2"));
    assert!(!f.matches("3.0"));
}
