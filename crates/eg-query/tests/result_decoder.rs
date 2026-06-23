//! Result-cell decoder coverage (CONCEPT:KG-2.196). DataFusion 43 already EXECUTES
//! aggregates / GROUP BY / HAVING, window functions, CTEs, subqueries, set ops
//! (UNION/INTERSECT/EXCEPT) and DISTINCT — but their results often materialize as
//! Arrow types the old `cell_to_json` rejected with a hard `Err` (Int32/UInt32,
//! Float32, Date/Timestamp, Decimal128, etc.), so the query failed at the LAST step
//! even though the compute succeeded. Each test below would previously have hit that
//! error path; now the broadened decoder degrades every type to a correct JSON cell
//! (or, for exotic types, a lossless display string) and never errors.
#![cfg(feature = "sql")]

use std::collections::HashMap;
use std::sync::Arc;

use eg_core::graph::GraphView;
use eg_query::exec_sql;
use serde_json::json;

/// Nodes with a `type` label, an integer `rank`, and a float `score` — enough to
/// exercise GROUP BY, aggregates, windows, CTEs, set ops and DISTINCT.
fn sample_view() -> GraphView {
    let mut node_properties: HashMap<String, Arc<Vec<u8>>> = HashMap::new();
    for (id, val) in [
        ("n1", json!({"type": "Agent", "rank": 1, "score": 0.5})),
        ("n2", json!({"type": "Agent", "rank": 2, "score": 1.5})),
        ("n3", json!({"type": "Tool", "rank": 3, "score": 2.5})),
        ("n4", json!({"type": "Tool", "rank": 4, "score": 3.5})),
        ("n5", json!({"type": "Tool", "rank": 5, "score": 4.5})),
    ] {
        let blob = rmp_serde::to_vec_named(&val).unwrap();
        node_properties.insert(id.to_string(), Arc::new(blob));
    }
    GraphView {
        node_properties,
        ..Default::default()
    }
}

fn run(sql: &str) -> eg_query::QueryResult {
    exec_sql(&sample_view(), sql).expect("sql executed and decoded")
}

fn rows(r: &eg_query::QueryResult) -> Vec<Vec<serde_json::Value>> {
    r.rows
        .iter()
        .map(|blob| rmp_serde::from_slice::<Vec<serde_json::Value>>(blob).unwrap())
        .collect()
}

/// `COUNT(*)` materializes as Int64 — but with GROUP BY the grouped aggregate path
/// exercises the decoder over the aggregate output column.
#[test]
fn count_star_group_by() {
    let r = run("SELECT json_get(props, 'type') AS t, COUNT(*) AS c \
         FROM nodes GROUP BY json_get(props, 'type') ORDER BY t");
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], json!("Agent"));
    assert_eq!(v[0][1], json!(2));
    assert_eq!(v[1][0], json!("Tool"));
    assert_eq!(v[1][1], json!(3));
}

/// SUM / AVG / MIN / MAX with GROUP BY. `SUM` over an Int64 column materializes as
/// Int64; `AVG` materializes as Float64; MIN/MAX preserve the input type. All must
/// decode to JSON numbers (previously the aggregate output types could error).
#[test]
fn sum_avg_min_max_group_by() {
    let r = run("SELECT json_get(props, 'type') AS t, \
                SUM(rank) AS s, AVG(rank) AS a, MIN(rank) AS mn, MAX(rank) AS mx \
         FROM nodes GROUP BY json_get(props, 'type') ORDER BY t");
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    // Agent: ranks 1,2 -> sum 3, avg 1.5, min 1, max 2
    assert_eq!(v[0][1], json!(3));
    assert_eq!(v[0][2], json!(1.5));
    assert_eq!(v[0][3], json!(1));
    assert_eq!(v[0][4], json!(2));
    // Tool: ranks 3,4,5 -> sum 12, avg 4.0, min 3, max 5
    assert_eq!(v[1][1], json!(12));
    assert_eq!(v[1][2], json!(4.0));
    assert_eq!(v[1][3], json!(3));
    assert_eq!(v[1][4], json!(5));
}

/// GROUP BY ... HAVING — same aggregate decode path, filtered post-aggregation.
#[test]
fn group_by_having() {
    let r = run("SELECT json_get(props, 'type') AS t, COUNT(*) AS c \
         FROM nodes GROUP BY json_get(props, 'type') HAVING COUNT(*) > 2");
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], json!("Tool"));
    assert_eq!(v[0][1], json!(3));
}

/// `ROW_NUMBER() OVER (...)` — a window function. The window output materializes as
/// UInt64 in DataFusion 43, which the OLD decoder handled, but partitioned windows
/// and the surrounding projection types are part of the newly-validated surface.
#[test]
fn window_row_number() {
    let r = run("SELECT id, ROW_NUMBER() OVER (ORDER BY rank) AS rn \
         FROM nodes ORDER BY rn");
    let v = rows(&r);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0][0], json!("n1"));
    assert_eq!(v[0][1], json!(1));
    assert_eq!(v[4][0], json!("n5"));
    assert_eq!(v[4][1], json!(5));
}

/// A CTE (`WITH x AS (...) SELECT ...`). The aggregate inside the CTE flows through
/// to the outer projection; previously the aggregate cell could hit the error path.
#[test]
fn cte_with_aggregate() {
    let r = run("WITH per_type AS ( \
            SELECT json_get(props, 'type') AS t, COUNT(*) AS c \
            FROM nodes GROUP BY json_get(props, 'type') \
         ) \
         SELECT t, c FROM per_type WHERE c >= 3 ORDER BY t");
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], json!("Tool"));
    assert_eq!(v[0][1], json!(3));
}

/// A set op (`UNION` / `EXCEPT`). UNION-ing two SELECTs and de-duplicating exercises
/// the result schema unification + the decoder over the combined output.
#[test]
fn set_op_union_and_except() {
    let r = run("SELECT id FROM nodes WHERE rank <= 2 \
         UNION \
         SELECT id FROM nodes WHERE rank >= 5 \
         ORDER BY id");
    let v = rows(&r);
    assert_eq!(v.len(), 3); // n1, n2, n5
    assert_eq!(v[0][0], json!("n1"));
    assert_eq!(v[2][0], json!("n5"));

    let r2 = run("SELECT id FROM nodes \
         EXCEPT \
         SELECT id FROM nodes WHERE rank <= 4 \
         ORDER BY id");
    let v2 = rows(&r2);
    assert_eq!(v2.len(), 1); // only n5 (rank 5) survives EXCEPT
    assert_eq!(v2[0][0], json!("n5"));
}

/// `SELECT DISTINCT` — distinct over a projected column.
#[test]
fn select_distinct() {
    let r = run("SELECT DISTINCT json_get(props, 'type') AS t FROM nodes ORDER BY t");
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], json!("Agent"));
    assert_eq!(v[1][0], json!("Tool"));
}

/// Explicit casts forcing Float32 / Int32 / Decimal128 / Timestamp result cells —
/// EXACTLY the Arrow types the OLD `cell_to_json` rejected with a hard error. The
/// query computes fine in DataFusion 43; this asserts the broadened decoder
/// materializes each (numbers for the numeric widths, ISO-8601 string for the
/// timestamp, lossless decimal string for Decimal128) instead of erroring.
#[test]
fn narrow_numeric_decimal_timestamp_cells_decode() {
    // Float32: CAST(score AS REAL) -> Float32 array.
    let r = run("SELECT CAST(score AS REAL) AS s FROM nodes WHERE id = 'n1'");
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], json!(0.5));

    // Int32: CAST(rank AS INT) -> Int32 array.
    let r = run("SELECT CAST(rank AS INT) AS r FROM nodes WHERE id = 'n3'");
    let v = rows(&r);
    assert_eq!(v[0][0], json!(3));

    // Decimal128: an arithmetic decimal literal materializes as Decimal128 ->
    // lossless decimal string (NOT a float that could round).
    let r = run("SELECT CAST(rank AS DECIMAL(10, 2)) AS d FROM nodes WHERE id = 'n2'");
    let v = rows(&r);
    assert_eq!(v[0][0], json!("2.00"));

    // Timestamp: a timestamp literal materializes as Timestamp(*) -> ISO-8601 string.
    let r = run("SELECT TIMESTAMP '2024-01-02 03:04:05' AS ts FROM nodes LIMIT 1");
    let v = rows(&r);
    let cell = &v[0][0];
    assert!(
        cell.is_string(),
        "timestamp must decode to a string, got {cell:?}"
    );
    let s = cell.as_str().unwrap();
    assert!(
        s.starts_with("2024-01-02"),
        "timestamp must be ISO-8601, got {s:?}"
    );

    // Date32: a DATE literal materializes as Date32 -> ISO-8601 date string.
    let r = run("SELECT DATE '2024-01-02' AS d FROM nodes LIMIT 1");
    let v = rows(&r);
    assert_eq!(v[0][0], json!("2024-01-02"));
}
