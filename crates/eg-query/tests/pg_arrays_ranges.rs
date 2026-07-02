//! CONCEPT:EG-104 — Postgres array/range types + common scalar/table functions.
//!
//! Verifies the drop-in surface ORMs/BI tools emit: the common scalar functions (which
//! are already in DataFusion 43 vs the `greatest`/`least`/`EXTRACT` gaps EG-104 fills),
//! `generate_series`, DataFusion's native array handling (enabled via `nested_expressions`)
//! and the pragmatic range UDFs.
#![cfg(feature = "sql")]

use eg_core::graph::GraphCore;
use eg_query::{exec_sql, QueryResult};
use serde_json::json;

/// n1/n2/n3 carrying an integer `v` (10/20/30) and a string `id`.
fn graph() -> GraphCore {
    let core = GraphCore::new();
    for (id, v) in [("n1", 10i64), ("n2", 20), ("n3", 30)] {
        core.add_node(
            id.to_string(),
            rmp_serde::to_vec_named(&json!({ "v": v })).unwrap(),
        );
    }
    core
}

fn rows(r: &QueryResult) -> Vec<Vec<serde_json::Value>> {
    r.rows
        .iter()
        .map(|b| rmp_serde::from_slice::<Vec<serde_json::Value>>(b).unwrap())
        .collect()
}

fn one(sql: &str) -> serde_json::Value {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(&snap, sql).unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let v = rows(&r);
    v[0][0].clone()
}

// ── common scalar functions already provided by DataFusion 43 (verify) ───────

#[test]
fn eg104_split_part_native() {
    assert_eq!(one("SELECT split_part('a,b,c', ',', 2) AS x"), json!("b"));
}

#[test]
fn eg104_regexp_replace_native() {
    assert_eq!(
        one("SELECT regexp_replace('foobar', 'o+', 'X') AS x"),
        json!("fXbar")
    );
}

#[test]
fn eg104_date_trunc_native() {
    // 2001-09-09T01:46:40Z truncated to the day.
    assert_eq!(
        one("SELECT date_trunc('day', to_timestamp(1000000000)) AS x"),
        json!("2001-09-09T00:00:00")
    );
}

#[test]
fn eg104_coalesce_and_nullif_native() {
    assert_eq!(one("SELECT coalesce(NULL, 2) AS x"), json!(2));
    assert_eq!(one("SELECT nullif(2, 2) AS x"), json!(null));
}

#[test]
fn eg104_string_agg_native() {
    // string_agg concatenates every id; scan order is not guaranteed, so compare as a set.
    let v = one("SELECT string_agg(id, ',') AS x FROM nodes");
    let mut parts: Vec<&str> = v.as_str().unwrap().split(',').collect();
    parts.sort_unstable();
    assert_eq!(parts, vec!["n1", "n2", "n3"]);
}

// ── functions EG-104 ADDS (greatest/least + EXTRACT desugar) ─────────────────

#[test]
fn eg104_greatest_least_int() {
    assert_eq!(one("SELECT greatest(1, 5, 3) AS x"), json!(5));
    assert_eq!(one("SELECT least(4, 2, 9) AS x"), json!(2));
}

#[test]
fn eg104_greatest_ignores_nulls() {
    // Postgres: NULLs are skipped; the result is NULL only when every arg is NULL.
    assert_eq!(one("SELECT greatest(NULL, 3, 1) AS x"), json!(3));
}

#[test]
fn eg104_greatest_float_and_least_string() {
    assert_eq!(one("SELECT greatest(1.5, 2) AS x"), json!(2.0));
    assert_eq!(one("SELECT least('b', 'a', 'c') AS x"), json!("a"));
}

#[test]
fn eg104_extract_desugars_to_date_part() {
    // EXTRACT(YEAR FROM ts) → date_part('year', ts) = 2001.
    assert_eq!(
        one("SELECT extract(YEAR FROM to_timestamp(1000000000)) AS x")
            .as_f64()
            .unwrap(),
        2001.0
    );
}

// ── generate_series table function ───────────────────────────────────────────

#[test]
fn eg104_generate_series_ascending() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(&snap, "SELECT value FROM generate_series(1, 3) ORDER BY value").unwrap();
    let v = rows(&r);
    assert_eq!(v, vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]]);
}

#[test]
fn eg104_generate_series_step_and_descending() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(&snap, "SELECT value FROM generate_series(0, 10, 5)").unwrap();
    assert_eq!(
        rows(&r),
        vec![vec![json!(0)], vec![json!(5)], vec![json!(10)]]
    );
    let r = exec_sql(&snap, "SELECT value FROM generate_series(3, 1, -1)").unwrap();
    assert_eq!(
        rows(&r),
        vec![vec![json!(3)], vec![json!(2)], vec![json!(1)]]
    );
}

// ── native array support (nested_expressions) ────────────────────────────────

#[test]
fn eg104_unnest_expands_array() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(&snap, "SELECT unnest(array[1,2,3]) AS x").unwrap();
    let v = rows(&r);
    assert_eq!(v, vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]]);
}

#[test]
fn eg104_array_length_native() {
    assert_eq!(
        one("SELECT array_length(array[1,2,3], 1) AS x").as_i64().unwrap(),
        3
    );
}

#[test]
fn eg104_any_operator_over_array() {
    assert_eq!(one("SELECT 5 = ANY(array[1,5,3]) AS x"), json!(true));
    assert_eq!(one("SELECT 7 = ANY(array[1,5,3]) AS x"), json!(false));
}

#[test]
fn eg104_any_operator_in_where() {
    // WHERE 20 = ANY(array[v]) selects exactly n2 (v = 20).
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT id FROM nodes WHERE 20 = ANY(array[v]) ORDER BY id",
    )
    .unwrap();
    assert_eq!(rows(&r), vec![vec![json!("n2")]]);
}

#[test]
fn eg104_array_has_all_any_native() {
    // pg `@>` semantics ⇒ array_has_all; `&&` ⇒ array_has_any.
    assert_eq!(
        one("SELECT array_has_all(array[1,2,3], array[2,3]) AS x"),
        json!(true)
    );
    assert_eq!(
        one("SELECT array_has_any(array[1,2,3], array[3,4]) AS x"),
        json!(true)
    );
    assert_eq!(
        one("SELECT array_has_any(array[1,2], array[7,8]) AS x"),
        json!(false)
    );
}

// ── pragmatic range types ────────────────────────────────────────────────────

#[test]
fn eg104_int4range_canonical_text() {
    assert_eq!(one("SELECT int4range(1, 5) AS x"), json!("[1,5)"));
    assert_eq!(one("SELECT tsrange(1000, 2000) AS x"), json!("[1000,2000)"));
}

#[test]
fn eg104_range_contains_point() {
    // Half-open [1,5): contains 1..4, excludes 5.
    assert_eq!(one("SELECT range_contains(int4range(1,5), 3) AS x"), json!(true));
    assert_eq!(one("SELECT range_contains(int4range(1,5), 5) AS x"), json!(false));
    // Inclusive upper bound text form `[1,5]` includes 5.
    assert_eq!(one("SELECT range_contains('[1,5]', 5) AS x"), json!(true));
}

#[test]
fn eg104_range_overlaps() {
    assert_eq!(
        one("SELECT range_overlaps(int4range(1,5), int4range(4,9)) AS x"),
        json!(true)
    );
    assert_eq!(
        one("SELECT range_overlaps(int4range(1,3), int4range(5,9)) AS x"),
        json!(false)
    );
}

#[test]
fn eg104_range_contains_and_contained_by() {
    // @> : [1,10) covers [3,5).  <@ : [3,5) covered by [1,10).
    assert_eq!(
        one("SELECT range_contains_range(int4range(1,10), int4range(3,5)) AS x"),
        json!(true)
    );
    assert_eq!(
        one("SELECT range_contained_by(int4range(3,5), int4range(1,10)) AS x"),
        json!(true)
    );
    assert_eq!(
        one("SELECT range_contains_range(int4range(3,5), int4range(1,10)) AS x"),
        json!(false)
    );
}
