//! SQL window-function end-to-end tests (CONCEPT:EG-KG.temporal.columnar-schema-inference): `<fn>() OVER (PARTITION BY
//! … ORDER BY … <frame>)` executed through the SAME DataFusion read path as every
//! other SELECT (`exec_sql` → `SessionContext::sql`). DataFusion 54 provides the
//! window operator + the ranking/offset/aggregate window functions natively; these
//! tests assert that the classify/exec path routes them (a window SELECT is a
//! `StatementKind::Read`) and that partition → order → frame evaluation is correct.
#![cfg(feature = "sql")]

use std::collections::HashMap;
use std::sync::Arc;

use eg_core::graph::GraphView;
use eg_query::exec_sql;
use serde_json::{json, Value};

/// Nodes with a `part` partition key, an `ord` ordering key, and a numeric `val`, so
/// PARTITION BY / ORDER BY / frame behaviour is observable.
fn sample_view() -> GraphView {
    let mut node_properties: HashMap<String, Arc<Vec<u8>>> = HashMap::new();
    for (id, val) in [
        ("n1", json!({"part": "a", "ord": 1, "val": 10})),
        ("n2", json!({"part": "a", "ord": 2, "val": 20})),
        ("n3", json!({"part": "a", "ord": 3, "val": 30})),
        ("n4", json!({"part": "b", "ord": 1, "val": 100})),
        ("n5", json!({"part": "b", "ord": 2, "val": 100})),
        ("n6", json!({"part": "b", "ord": 3, "val": 300})),
    ] {
        let blob = rmp_serde::to_vec_named(&val).unwrap();
        node_properties.insert(id.to_string(), Arc::new(blob));
    }
    GraphView {
        node_properties,
        ..Default::default()
    }
}

fn run(sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    let r = exec_sql(&sample_view(), sql, &eg_query::CancellationToken::new())
        .expect("window sql executed");
    let rows = r
        .rows
        .iter()
        .map(|blob| rmp_serde::from_slice::<Vec<Value>>(blob).unwrap())
        .collect();
    (r.columns, rows)
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — ROW_NUMBER / RANK / DENSE_RANK over PARTITION BY … ORDER BY.
/// Partition `b` has a tie on `val` (n4,n5 both 100) so RANK gaps (1,1,3) while
/// DENSE_RANK does not (1,1,2), and ROW_NUMBER is always distinct (1,2,3).
#[test]
fn eg_089_window_row_number_rank_dense_rank() {
    let (_cols, rows) = run("SELECT id, \
            ROW_NUMBER() OVER (PARTITION BY part ORDER BY val, id) AS rn, \
            RANK() OVER (PARTITION BY part ORDER BY val) AS rnk, \
            DENSE_RANK() OVER (PARTITION BY part ORDER BY val) AS drnk \
         FROM nodes ORDER BY part, val, id");
    // Order: a:(10,20,30) then b:(100,100,300).
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["n1", "n2", "n3", "n4", "n5", "n6"]);
    let rn: Vec<i64> = rows.iter().map(|r| r[1].as_i64().unwrap()).collect();
    assert_eq!(rn, vec![1, 2, 3, 1, 2, 3]);
    let rnk: Vec<i64> = rows.iter().map(|r| r[2].as_i64().unwrap()).collect();
    assert_eq!(rnk, vec![1, 2, 3, 1, 1, 3]); // tie → RANK gaps
    let drnk: Vec<i64> = rows.iter().map(|r| r[3].as_i64().unwrap()).collect();
    assert_eq!(drnk, vec![1, 2, 3, 1, 1, 2]); // tie → DENSE_RANK no gap
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — offset functions LAG / LEAD / FIRST_VALUE over a partition.
#[test]
fn eg_089_window_lag_lead_first_value() {
    let (_cols, rows) = run("SELECT id, \
            LAG(val, 1, -1) OVER (PARTITION BY part ORDER BY ord) AS prev, \
            LEAD(val, 1, -1) OVER (PARTITION BY part ORDER BY ord) AS next, \
            FIRST_VALUE(val) OVER (PARTITION BY part ORDER BY ord) AS firstv \
         FROM nodes ORDER BY part, ord");
    let prev: Vec<i64> = rows.iter().map(|r| r[1].as_i64().unwrap()).collect();
    // a: [-1,10,20], b: [-1,100,100]
    assert_eq!(prev, vec![-1, 10, 20, -1, 100, 100]);
    let next: Vec<i64> = rows.iter().map(|r| r[2].as_i64().unwrap()).collect();
    // a: [20,30,-1], b: [100,300,-1]
    assert_eq!(next, vec![20, 30, -1, 100, 300, -1]);
    let firstv: Vec<i64> = rows.iter().map(|r| r[3].as_i64().unwrap()).collect();
    assert_eq!(firstv, vec![10, 10, 10, 100, 100, 100]);
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — SUM() OVER (PARTITION BY …) with NO frame → the whole-partition
/// aggregate broadcast onto every row (default frame is bounded by the partition when
/// there is no ORDER BY).
#[test]
fn eg_089_window_sum_over_partition_by() {
    let (_cols, rows) = run("SELECT id, part, \
            SUM(val) OVER (PARTITION BY part) AS part_total \
         FROM nodes ORDER BY part, ord");
    let totals: Vec<i64> = rows.iter().map(|r| r[2].as_i64().unwrap()).collect();
    // a total = 60 (×3 rows), b total = 500 (×3 rows).
    assert_eq!(totals, vec![60, 60, 60, 500, 500, 500]);
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — an explicit ROWS BETWEEN frame: a trailing running sum over the
/// current row and the one before it (`ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`).
#[test]
fn eg_089_window_rows_between_frame() {
    let (_cols, rows) = run("SELECT id, \
            SUM(val) OVER (PARTITION BY part ORDER BY ord \
                           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS trailing2 \
         FROM nodes ORDER BY part, ord");
    let trailing: Vec<i64> = rows.iter().map(|r| r[1].as_i64().unwrap()).collect();
    // a: [10, 10+20, 20+30] = [10,30,50]; b: [100, 100+100, 100+300] = [100,200,400].
    assert_eq!(trailing, vec![10, 30, 50, 100, 200, 400]);
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — the default frame when ORDER BY is present (RANGE UNBOUNDED
/// PRECEDING .. CURRENT ROW) produces a running cumulative sum; PARTITION + ORDER
/// interplay resets the accumulation at each partition boundary.
#[test]
fn eg_089_window_partition_order_interplay_running_sum() {
    let (_cols, rows) = run("SELECT id, \
            SUM(val) OVER (PARTITION BY part ORDER BY ord) AS running \
         FROM nodes ORDER BY part, ord");
    let running: Vec<i64> = rows.iter().map(|r| r[1].as_i64().unwrap()).collect();
    // a: [10,30,60]; b resets: [100,200,500].
    assert_eq!(running, vec![10, 30, 60, 100, 200, 500]);
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — the distribution ranking functions NTILE(n) / PERCENT_RANK /
/// CUME_DIST over PARTITION BY … ORDER BY, including the tie behaviour on partition
/// `b` (two rows share val=100).
#[test]
fn eg_089_window_ntile_percent_rank_cume_dist() {
    let (_cols, rows) = run("SELECT id, \
            NTILE(2) OVER (PARTITION BY part ORDER BY ord) AS bucket, \
            PERCENT_RANK() OVER (PARTITION BY part ORDER BY val) AS pr, \
            CUME_DIST() OVER (PARTITION BY part ORDER BY val) AS cd \
         FROM nodes ORDER BY part, ord");
    // NTILE(2) over 3 rows → bucket sizes [2,1] per partition.
    let bucket: Vec<i64> = rows.iter().map(|r| r[1].as_i64().unwrap()).collect();
    assert_eq!(bucket, vec![1, 1, 2, 1, 1, 2]);
    // PERCENT_RANK = (rank-1)/(n-1). a: [0,.5,1]; b (ranks 1,1,3): [0,0,1].
    let pr: Vec<f64> = rows.iter().map(|r| r[2].as_f64().unwrap()).collect();
    assert_eq!(pr, vec![0.0, 0.5, 1.0, 0.0, 0.0, 1.0]);
    // CUME_DIST = (#rows with val <= current)/n. a: [1/3,2/3,1]; b: [2/3,2/3,1].
    let cd: Vec<f64> = rows.iter().map(|r| r[3].as_f64().unwrap()).collect();
    let approx = |a: f64, b: f64| (a - b).abs() < 1e-9;
    assert!(approx(cd[0], 1.0 / 3.0) && approx(cd[1], 2.0 / 3.0) && approx(cd[2], 1.0));
    assert!(approx(cd[3], 2.0 / 3.0) && approx(cd[4], 2.0 / 3.0) && approx(cd[5], 1.0));
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — LAST_VALUE / NTH_VALUE over the FULL partition frame
/// (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`), so the frame spans
/// the whole partition rather than the default running frame.
#[test]
fn eg_089_window_last_value_nth_value_full_frame() {
    let (_cols, rows) = run("SELECT id, \
            LAST_VALUE(val) OVER (PARTITION BY part ORDER BY ord \
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lastv, \
            NTH_VALUE(val, 2) OVER (PARTITION BY part ORDER BY ord \
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second \
         FROM nodes ORDER BY part, ord");
    let lastv: Vec<i64> = rows.iter().map(|r| r[1].as_i64().unwrap()).collect();
    assert_eq!(lastv, vec![30, 30, 30, 300, 300, 300]);
    let second: Vec<i64> = rows.iter().map(|r| r[2].as_i64().unwrap()).collect();
    assert_eq!(second, vec![20, 20, 20, 100, 100, 100]);
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — the aggregate window functions AVG/MIN/MAX/COUNT OVER a partition.
#[test]
fn eg_089_window_avg_min_max_count_over_partition() {
    let (_cols, rows) = run("SELECT id, part, \
            AVG(val) OVER (PARTITION BY part) AS a, \
            MIN(val) OVER (PARTITION BY part) AS mn, \
            MAX(val) OVER (PARTITION BY part) AS mx, \
            COUNT(val) OVER (PARTITION BY part) AS c \
         FROM nodes ORDER BY part, ord");
    let avg: Vec<f64> = rows.iter().map(|r| r[2].as_f64().unwrap()).collect();
    assert_eq!(avg[0], 20.0); // a: (10+20+30)/3
    assert!((avg[3] - 500.0 / 3.0).abs() < 1e-9); // b: (100+100+300)/3
    let mn: Vec<i64> = rows.iter().map(|r| r[3].as_i64().unwrap()).collect();
    assert_eq!(mn, vec![10, 10, 10, 100, 100, 100]);
    let mx: Vec<i64> = rows.iter().map(|r| r[4].as_i64().unwrap()).collect();
    assert_eq!(mx, vec![30, 30, 30, 300, 300, 300]);
    let c: Vec<i64> = rows.iter().map(|r| r[5].as_i64().unwrap()).collect();
    assert_eq!(c, vec![3, 3, 3, 3, 3, 3]);
}

/// CONCEPT:EG-KG.temporal.columnar-schema-inference — a RANGE frame differs from a ROWS frame on ORDER-BY ties: peers
/// with an equal ORDER BY value share the same frame end. Partition `b` orders by
/// `val` with a tie at 100, so both 100-rows see the same RANGE sum (200) while a
/// ROWS frame would have given 100 then 200.
#[test]
fn eg_089_window_range_frame_ties() {
    let (_cols, rows) = run("SELECT id, part, val, \
            SUM(val) OVER (PARTITION BY part ORDER BY val \
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_sum \
         FROM nodes ORDER BY part, val, id");
    let range_sum: Vec<i64> = rows.iter().map(|r| r[3].as_i64().unwrap()).collect();
    // a distinct vals: [10,30,60]; b ties at 100 → [200,200,500].
    assert_eq!(range_sum, vec![10, 30, 60, 200, 200, 500]);
}
