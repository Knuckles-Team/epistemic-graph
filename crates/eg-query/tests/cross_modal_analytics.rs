//! Cross-modal in-engine analytics (CONCEPT:EG-345) — the Analytics-Program P4 differentiator
//! that lets epistemic-graph *surpass* numpy: **join graph + vector + timeseries, then run
//! PCA / k-means / covariance over the JOINED result set IN-ENGINE** (compute-near-data, no
//! fetch-to-Python, no data-layer round-trip). numpy has no data layer, so this join→analytics
//! pipeline is impossible there; here it is one SQL statement over resident graph data.
//!
//! The dataset spans THREE modalities, all reachable from the one SQL surface:
//!   * **graph / relational** — real `GraphCore` nodes (the resident `nodes` table), each
//!     carrying a scalar attribute `x` and a 2-D embedding `emb` in its property map;
//!   * **vector** — the per-node `emb` embedding, decoded out of the node props by the same
//!     `row_to_vector` path the pgvector/ANN operators use;
//!   * **timeseries** — a per-node stream of time-stamped `reading`s (the `readings` relation),
//!     reduced to a per-node average by a windowless timeseries aggregate.
//!
//! ONE query JOINs the graph nodes to the timeseries aggregate on node id and runs the
//! kernel analytics (`pca`/`kmeans`/`covariance`, `crates/eg-query/src/sql/numeric.rs`,
//! CONCEPT:EG-329/335/336/344) over the joined columns. Only compiles under `numeric`.
#![cfg(feature = "numeric")]

use eg_core::graph::GraphCore;
use eg_query::{exec_sql, QueryResult};
use serde_json::json;

/// A 6-node graph in TWO communities, each node carrying:
///   * `x`   — a graph/relational scalar attribute (1..6),
///   * `emb` — a 2-D vector embedding stored as a JSON array prop (the VECTOR modality).
///
/// Community A (n1,n2,n3) sits near `[10,10]`, community B (n4,n5,n6) near `[-10,-10]`,
/// and **all six points lie exactly on the line y = x** — so PCA's first component is
/// exactly ±[1/√2, 1/√2] and k-means (k=2) recovers the two communities.
fn graph() -> GraphCore {
    let core = GraphCore::new();
    // (id, x, emb) — emb points chosen ON the y=x diagonal, centered per community.
    let spec = [
        ("n1", 1.0, [12.0, 12.0]),
        ("n2", 2.0, [10.0, 10.0]),
        ("n3", 3.0, [8.0, 8.0]),
        ("n4", 4.0, [-8.0, -8.0]),
        ("n5", 5.0, [-10.0, -10.0]),
        ("n6", 6.0, [-12.0, -12.0]),
    ];
    for (id, x, emb) in spec {
        let val = json!({ "x": x, "emb": [emb[0], emb[1]] });
        core.add_node(id.to_string(), rmp_serde::to_vec_named(&val).unwrap());
    }
    core
}

fn rows(r: &QueryResult) -> Vec<Vec<serde_json::Value>> {
    r.rows
        .iter()
        .map(|b| rmp_serde::from_slice::<Vec<serde_json::Value>>(b).unwrap())
        .collect()
}

/// The TIMESERIES modality as an inline relation: two time-stamped readings per node,
/// engineered so the per-node average is exactly `2·x`. As a SQL CTE it JOINs the resident
/// graph on node id — the timeseries leg of the cross-modal join.
const READINGS_CTE: &str = "\
    readings(nid, ts, reading) AS (VALUES \
        ('n1', 1, 1.0),  ('n1', 2, 3.0), \
        ('n2', 1, 3.0),  ('n2', 2, 5.0), \
        ('n3', 1, 5.0),  ('n3', 2, 7.0), \
        ('n4', 1, 7.0),  ('n4', 2, 9.0), \
        ('n5', 1, 9.0),  ('n5', 2, 11.0), \
        ('n6', 1, 11.0), ('n6', 2, 13.0)), \
    ts AS (SELECT nid, avg(reading) AS avg_reading FROM readings GROUP BY nid)";

/// **The cross-modal covariance** (CONCEPT:EG-345): `covariance` over a column from the
/// GRAPH modality (`x`) and a column from the TIMESERIES modality (`avg_reading`), aligned
/// by the graph⋈timeseries JOIN — a statistic that spans two modalities and is computed
/// entirely in-engine over the joined rows.
///
/// avg_reading = 2·x by construction, so cov(x, avg_reading) = 2·var_sample(x). For
/// x = 1..6, var_sample = 3.5 ⇒ cov = 7.0 exactly. numpy could only do this AFTER pulling
/// both modalities out into arrays and aligning them by hand.
#[test]
fn cross_modal_covariance_graph_join_timeseries() {
    let snap = graph().analysis_snapshot();
    let sql = format!(
        "WITH {READINGS_CTE} \
         SELECT covariance(json_get_f64(n.props, 'x'), t.avg_reading) AS xcov \
         FROM nodes n JOIN ts t ON n.id = t.nid"
    );
    let r = exec_sql(&snap, &sql).unwrap();
    let v = rows(&r);
    assert!(
        (v[0][0].as_f64().unwrap() - 7.0).abs() < 1e-9,
        "cross-modal cov(x, avg_reading) should be 7.0, got {:?}",
        v[0][0]
    );
}

/// **k-means over the joined VECTOR column** (CONCEPT:EG-345/EG-344): the join brings graph
/// nodes together with their timeseries average; `kmeans` then clusters the joined rows'
/// `emb` vectors in-engine. The two communities (near `[10,10]` / `[-10,-10]`) must split
/// into two balanced clusters of three — the "cluster the joined result in-engine" proof.
#[test]
fn cross_modal_kmeans_over_join() {
    let snap = graph().analysis_snapshot();
    let sql = format!(
        "WITH {READINGS_CTE} \
         SELECT kmeans(json_get(n.props, 'emb'), 2) AS clusters \
         FROM nodes n JOIN ts t ON n.id = t.nid"
    );
    let r = exec_sql(&snap, &sql).unwrap();
    let v = rows(&r);
    let labels: Vec<i64> = v[0][0]
        .as_array()
        .expect("kmeans → JSON array of Int64 labels")
        .iter()
        .map(|x| x.as_i64().unwrap())
        .collect();
    assert_eq!(labels.len(), 6, "one label per joined row: {labels:?}");
    // Exactly two clusters, balanced 3 / 3 (the two communities).
    let mut counts = std::collections::HashMap::new();
    for l in &labels {
        *counts.entry(*l).or_insert(0) += 1;
    }
    assert_eq!(counts.len(), 2, "expected 2 clusters: {labels:?}");
    assert!(
        counts.values().all(|&c| c == 3),
        "clusters must be balanced 3/3: {labels:?}"
    );
}

/// **PCA over the joined VECTOR column** (CONCEPT:EG-345): the first principal component of
/// the joined `emb` vectors. Every point lies on y = x, so PC1 is exactly ±[1/√2, 1/√2]
/// (all variance is along the [1,1] diagonal) — asserted on the in-engine join→analytics
/// result.
#[test]
fn cross_modal_pca_over_join() {
    let snap = graph().analysis_snapshot();
    let sql = format!(
        "WITH {READINGS_CTE} \
         SELECT pca(json_get(n.props, 'emb'), 1) AS pcs \
         FROM nodes n JOIN ts t ON n.id = t.nid"
    );
    let r = exec_sql(&snap, &sql).unwrap();
    let v = rows(&r);
    let pcs = v[0][0].as_array().expect("pca → list of components");
    assert_eq!(pcs.len(), 1, "{pcs:?}");
    let pc0 = pcs[0].as_array().expect("component → vector");
    assert_eq!(pc0.len(), 2, "{pc0:?}");
    let inv_sqrt2 = 1.0 / std::f64::consts::SQRT_2;
    let a = pc0[0].as_f64().unwrap();
    let b = pc0[1].as_f64().unwrap();
    assert!((a.abs() - inv_sqrt2).abs() < 1e-9, "pc0[0]: {pc0:?}");
    assert!((b.abs() - inv_sqrt2).abs() < 1e-9, "pc0[1]: {pc0:?}");
    // Direction [1,1] (same sign), not [1,-1].
    assert!(a * b > 0.0, "PC1 should lie on y=x: {pc0:?}");
}

/// **All three modalities, ONE statement** (CONCEPT:EG-345): graph ⋈ timeseries, with the
/// vector analytic (`kmeans`) and the cross-modal statistic (`covariance`) side by side in a
/// single projection over the joined result — the full "impossible in numpy" pipeline.
#[test]
fn cross_modal_join_pca_kmeans_covariance_one_query() {
    let snap = graph().analysis_snapshot();
    let sql = format!(
        "WITH {READINGS_CTE} \
         SELECT kmeans(json_get(n.props, 'emb'), 2) AS clusters, \
                covariance(json_get_f64(n.props, 'x'), t.avg_reading) AS xcov \
         FROM nodes n JOIN ts t ON n.id = t.nid"
    );
    let r = exec_sql(&snap, &sql).unwrap();
    let v = rows(&r);
    // Clustering result (vector modality over the join).
    let labels: Vec<i64> = v[0][0]
        .as_array()
        .expect("clusters list")
        .iter()
        .map(|x| x.as_i64().unwrap())
        .collect();
    assert_eq!(labels.len(), 6);
    let distinct: std::collections::HashSet<i64> = labels.iter().copied().collect();
    assert_eq!(distinct.len(), 2, "2 clusters: {labels:?}");
    // Cross-modal covariance (graph scalar × timeseries aggregate over the join).
    assert!(
        (v[0][1].as_f64().unwrap() - 7.0).abs() < 1e-9,
        "xcov should be 7.0, got {:?}",
        v[0][1]
    );
}
