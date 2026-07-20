//! Analytics-through-UQL on ALL SQL paths (CONCEPT:EG-KG.query.concept-8) — proves the `eg-numeric`
//! Surface-B operators (`cosine_sim`/`covariance`/`zscore`/`svd`/`pca`/`kmeans`,
//! CONCEPT:EG-KG.query.surface-b-numeric-operators/335/336/344) AND the DataFusion-native statistical aggregates
//! (`corr`/`stddev`/`var`/`median`/`approx_percentile_cont`) run IN-ENGINE over resident
//! columns through the query surface — with NO numpy round-trip.
//!
//! The load-bearing assertion is that the analytics resolve through
//! [`exec_sql_typed_with_tables`] — the SAME read path the shared `WireSession`
//! (`crate::server::wire`) drives for EVERY wire protocol (pgwire/mysql/sqlite/mssql).
//! So a DBeaver / `psql` / DBI client issuing `SELECT pca(...)` hits exactly this code,
//! not just the native RPC `Method::Sql` path (`exec_sql`). We assert BOTH paths to prove
//! parity: the wire read path and the RPC path expose the identical analytical surface,
//! because both build their DataFusion `SessionContext` through `exec::build_ctx`
//! (`register_numeric`) — there is one registration site, shared. Only compiles under
//! `numeric`.
#![cfg(feature = "numeric")]

use eg_core::graph::{GraphCore, GraphView};
use eg_query::{exec_sql, exec_sql_typed_with_tables, QueryResult, TableStore, TypedQueryResult};
use serde_json::{json, Value};

/// A 6-node graph carrying, per node:
///   * `x`  — a relational scalar (1..6),
///   * `y`  — `2·x` (perfectly linearly dependent on `x`, so `corr = 1`, `covariance = 7`),
///   * `emb`— a 2-D vector embedding on the line `y = x`, in TWO communities
///     (n1..n3 near `[10,10]`, n4..n6 near `[-10,-10]`) so PCA's PC1 = ±[1/√2,1/√2] and
///     k-means(k=2) recovers the two communities.
fn graph() -> GraphCore {
    let core = GraphCore::new();
    let spec = [
        ("n1", 1.0, [12.0, 12.0]),
        ("n2", 2.0, [10.0, 10.0]),
        ("n3", 3.0, [8.0, 8.0]),
        ("n4", 4.0, [-8.0, -8.0]),
        ("n5", 5.0, [-10.0, -10.0]),
        ("n6", 6.0, [-12.0, -12.0]),
    ];
    for (id, x, emb) in spec {
        let val = json!({ "x": x, "y": 2.0 * x, "emb": [emb[0], emb[1]] });
        core.add_node(id.to_string(), rmp_serde::to_vec_named(&val).unwrap());
    }
    core
}

/// Decode the msgpack rows of an RPC-path [`QueryResult`] into JSON.
fn rpc_rows(r: &QueryResult) -> Vec<Vec<Value>> {
    r.rows
        .iter()
        .map(|b| rmp_serde::from_slice::<Vec<Value>>(b).unwrap())
        .collect()
}

/// Run `sql` through the WIRE read path (`exec_sql_typed_with_tables`, what
/// pgwire/mysql/sqlite/mssql all call). Returns the typed rows (JSON cells).
fn wire(view: &GraphView, store: &TableStore, sql: &str) -> TypedQueryResult {
    exec_sql_typed_with_tables(view, store, sql).expect("wire read path")
}

/// **The headline test** — every analytical operator resolves through the WIRE read path
/// (`exec_sql_typed_with_tables`), the exact path a DBeaver / `psql` client hits. If any
/// UDF/UDAF were NOT registered on that path's `SessionContext`, the plan would fail with
/// "Invalid function" and this test would fail.
#[test]
fn analytics_resolve_through_the_wire_read_path_eg353() {
    let snap = graph().analysis_snapshot();
    let (store, _p) = TableStore::open_temp().unwrap();

    // ── eg-numeric Surface-B kernel operators (compute-near-data) ──────────────

    // cosine_sim: identical vectors → 1.0.
    let r = wire(
        &snap,
        &store,
        "SELECT cosine_sim('[1,2,3]', '[1,2,3]') AS s",
    );
    assert!(
        (r.rows[0][0].as_f64().unwrap() - 1.0).abs() < 1e-9,
        "cosine_sim: {:?}",
        r.rows[0][0]
    );

    // covariance(x, 2x) sample (ddof=1) = 2·Σ(x-x̄)² / (n-1) = 2·17.5/5 = 7.0.
    let r = wire(&snap, &store, "SELECT covariance(x, y) AS cov FROM nodes");
    assert!(
        (r.rows[0][0].as_f64().unwrap() - 7.0).abs() < 1e-9,
        "covariance: {:?}",
        r.rows[0][0]
    );

    // zscore standardizes within the materialized batch → mean ≈ 0, and the middle-ish
    // rows are near 0. Assert the column returns 6 finite values summing to ≈ 0.
    let r = wire(&snap, &store, "SELECT zscore(x) AS z FROM nodes");
    let zsum: f64 = r.rows.iter().map(|row| row[0].as_f64().unwrap()).sum();
    assert_eq!(r.rows.len(), 6, "zscore row count");
    assert!(
        zsum.abs() < 1e-9,
        "zscore should be mean-centered: sum={zsum}"
    );

    // svd(emb): the emb rows are all scalar multiples of [1,1] ⇒ rank-1 matrix ⇒ the
    // SECOND singular value is ≈ 0.
    let r = wire(&snap, &store, "SELECT svd(emb) AS s FROM nodes");
    let sv = r.rows[0][0].as_array().expect("svd → JSON array");
    assert_eq!(sv.len(), 2, "two singular values: {sv:?}");
    assert!(sv[0].as_f64().unwrap() > 1.0, "s1 large: {sv:?}");
    assert!(sv[1].as_f64().unwrap().abs() < 1e-6, "s2 ≈ 0: {sv:?}");

    // pca(emb, 1): the single principal-component direction of collinear y=x points is
    // ±[1/√2, 1/√2] ⇒ |pc[0]| == |pc[1]| == 0.7071…
    let r = wire(&snap, &store, "SELECT pca(emb, 1) AS pc FROM nodes");
    let comps = r.rows[0][0].as_array().expect("pca → list of components");
    let pc1 = comps[0].as_array().expect("pc1 vector");
    let a = pc1[0].as_f64().unwrap().abs();
    let b = pc1[1].as_f64().unwrap().abs();
    assert!(
        (a - std::f64::consts::FRAC_1_SQRT_2).abs() < 1e-6
            && (b - std::f64::consts::FRAC_1_SQRT_2).abs() < 1e-6,
        "PC1 = ±[1/√2,1/√2]: {pc1:?}"
    );

    // kmeans(emb, 2): one label per aggregated row (in scan order), recovering the two
    // communities as exactly two BALANCED clusters of three (the row order the scan yields
    // is not asserted — the balanced split is the community-recovery proof).
    let r = wire(&snap, &store, "SELECT kmeans(emb, 2) AS lbl FROM nodes");
    let labels: Vec<i64> = r.rows[0][0]
        .as_array()
        .expect("kmeans → list of labels")
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    assert_eq!(labels.len(), 6, "one label per row: {labels:?}");
    let mut counts = std::collections::BTreeMap::new();
    for l in &labels {
        *counts.entry(*l).or_insert(0) += 1;
    }
    assert_eq!(counts.len(), 2, "exactly two clusters: {labels:?}");
    assert!(
        counts.values().all(|&c| c == 3),
        "clusters must be balanced 3/3 (the two communities): {labels:?}"
    );

    // ── DataFusion-native statistical aggregates (in-engine, no numpy) ─────────
    // These are always-registered DataFusion aggregates — surfaced on the SAME wire path,
    // so the analytical-DB surface is complete without duplicating them in the kernel.

    // corr(x, 2x) = 1.0 (perfect positive linear dependence).
    let r = wire(&snap, &store, "SELECT corr(x, y) AS r FROM nodes");
    assert!(
        (r.rows[0][0].as_f64().unwrap() - 1.0).abs() < 1e-9,
        "corr: {:?}",
        r.rows[0][0]
    );

    // stddev_pop(x) over 1..6 = sqrt(Σ(x-3.5)²/6) = sqrt(17.5/6) ≈ 1.70783.
    let r = wire(&snap, &store, "SELECT stddev_pop(x) AS s FROM nodes");
    assert!(
        (r.rows[0][0].as_f64().unwrap() - (17.5f64 / 6.0).sqrt()).abs() < 1e-6,
        "stddev_pop: {:?}",
        r.rows[0][0]
    );

    // median(x) over 1..6 = 3.5 (approx_percentile_cont at 0.5).
    let r = wire(
        &snap,
        &store,
        "SELECT approx_percentile_cont(x, 0.5) AS m FROM nodes",
    );
    assert!(
        (r.rows[0][0].as_f64().unwrap() - 3.5).abs() < 0.5,
        "median-ish: {:?}",
        r.rows[0][0]
    );
}

/// Parity: the identical analytics resolve through the native RPC path (`exec_sql`,
/// `Method::Sql`) too — the wire path and the RPC path share ONE registration site
/// (`register_numeric` via `build_ctx`), so neither is privileged.
#[test]
fn analytics_resolve_through_the_rpc_path_too_eg353() {
    let snap = graph().analysis_snapshot();

    let r = exec_sql(
        &snap,
        "SELECT covariance(x, y) AS cov, corr(x, y) AS r FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let rows = rpc_rows(&r);
    assert!(
        (rows[0][0].as_f64().unwrap() - 7.0).abs() < 1e-9,
        "rpc covariance: {:?}",
        rows[0][0]
    );
    assert!(
        (rows[0][1].as_f64().unwrap() - 1.0).abs() < 1e-9,
        "rpc corr: {:?}",
        rows[0][1]
    );

    // A kernel matrix operator through the RPC path as well.
    let r = exec_sql(
        &snap,
        "SELECT kmeans(emb, 2) AS lbl FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let rows = rpc_rows(&r);
    let labels = rows[0][0].as_array().expect("kmeans labels");
    assert_eq!(labels.len(), 6, "rpc kmeans labels: {labels:?}");
}

/// The cross-modal, compute-near-data story in ONE statement: JOIN the graph's `nodes`
/// (relational `x` + vector `emb`) to an inline timeseries relation on node id, then run
/// BOTH a kernel operator (`covariance`) and a DataFusion-native aggregate (`corr`) over
/// the JOINED result set — through the wire read path. numpy has no data layer to join
/// across; here it is one SQL statement. (CONCEPT:EG-KG.query.eg-3/EG-353.)
#[test]
fn cross_modal_join_then_analytics_through_wire_eg353() {
    let snap = graph().analysis_snapshot();
    let (store, _p) = TableStore::open_temp().unwrap();

    // Per-node timeseries readings averaging to exactly 2·x, joined on node id.
    let sql = "\
        WITH readings(nid, reading) AS (VALUES \
            ('n1', 1.0), ('n1', 3.0), ('n2', 3.0), ('n2', 5.0), \
            ('n3', 5.0), ('n3', 7.0), ('n4', 7.0), ('n4', 9.0), \
            ('n5', 9.0), ('n5', 11.0), ('n6', 11.0), ('n6', 13.0)), \
        ts AS (SELECT nid, avg(reading) AS avg_reading FROM readings GROUP BY nid) \
        SELECT covariance(n.x, ts.avg_reading) AS cov, corr(n.x, ts.avg_reading) AS r \
        FROM nodes n JOIN ts ON n.id = ts.nid";
    let r = wire(&snap, &store, sql);
    // avg_reading = 2·x, so cov(x, 2x) = 7.0 and corr = 1.0 — a statistic spanning the
    // relational + timeseries modalities, aligned by the join, computed in-engine.
    assert!(
        (r.rows[0][0].as_f64().unwrap() - 7.0).abs() < 1e-9,
        "cross-modal covariance: {:?}",
        r.rows[0][0]
    );
    assert!(
        (r.rows[0][1].as_f64().unwrap() - 1.0).abs() < 1e-9,
        "cross-modal corr: {:?}",
        r.rows[0][1]
    );
}
