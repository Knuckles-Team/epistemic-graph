//! M4 (CONCEPT:EG-KG.query.version-keyed-cache): edges table + JOINs, differentiated UDFs (epistemic_decay
//! scalar, pagerank/betweenness table functions, var/cvar finance aggregates), and
//! the version-keyed schema cache.
#![cfg(feature = "sql")]

use eg_core::graph::GraphCore;
use eg_query::{exec_sql, exec_sql_cached, QueryResult, SqlCache};
use serde_json::json;

/// Build a small graph as a `GraphCore` (real petgraph topology + edge props), then
/// hand back its `analysis_snapshot()` GraphView plus the OCC `version()`.
///
/// Topology: n1 -> n2 -> n3, n1 -> n3 (a tiny DAG so pagerank/betweenness are
/// non-trivial). Each node carries a `type` + `rank`; each edge carries a `weight`.
fn graph() -> GraphCore {
    let core = GraphCore::new();
    for (id, val) in [
        ("n1", json!({"type": "Agent", "rank": 1})),
        ("n2", json!({"type": "Agent", "rank": 2})),
        ("n3", json!({"type": "Tool", "rank": 3})),
    ] {
        core.add_node(id.to_string(), rmp_serde::to_vec_named(&val).unwrap());
    }
    for (s, d, w) in [("n1", "n2", 1.0), ("n2", "n3", 2.0), ("n1", "n3", 0.5)] {
        core.add_edge(
            s.to_string(),
            d.to_string(),
            rmp_serde::to_vec_named(&json!({ "weight": w })).unwrap(),
        )
        .unwrap();
    }
    core
}

fn rows(r: &QueryResult) -> Vec<Vec<serde_json::Value>> {
    r.rows
        .iter()
        .map(|b| rmp_serde::from_slice::<Vec<serde_json::Value>>(b).unwrap())
        .collect()
}

#[test]
fn edges_table_exposes_topology() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT src, dst, rel FROM edges ORDER BY src, dst",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        r.columns,
        vec!["src".to_string(), "dst".to_string(), "rel".to_string()]
    );
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    // first row n1->n2, rel is the engine's "src:dst" edge weight string.
    assert_eq!(v[0][0], json!("n1"));
    assert_eq!(v[0][1], json!("n2"));
    assert_eq!(v[0][2], json!("n1:n2"));
}

#[test]
fn join_nodes_to_edges() {
    let snap = graph().analysis_snapshot();
    // Join each node to its OUTGOING edges; n1 has two (n2, n3), n2 has one (n3).
    let r = exec_sql(
        &snap,
        "SELECT nodes.id, edges.dst FROM nodes JOIN edges ON nodes.id = edges.src \
         ORDER BY nodes.id, edges.dst",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0], vec![json!("n1"), json!("n2")]);
    assert_eq!(v[1], vec![json!("n1"), json!("n3")]);
    assert_eq!(v[2], vec![json!("n2"), json!("n3")]);
}

#[test]
fn join_reaches_edge_props_via_json_get() {
    let snap = graph().analysis_snapshot();
    // json_get_f64 over the edges.props escape-hatch recovers the stored weight.
    let r = exec_sql(
        &snap,
        "SELECT edges.src, json_get_f64(edges.props, 'weight') AS w \
         FROM nodes JOIN edges ON nodes.id = edges.src WHERE edges.dst = 'n3' \
         ORDER BY edges.src",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0], vec![json!("n1"), json!(0.5)]);
    assert_eq!(v[1], vec![json!("n2"), json!(2.0)]);
}

#[test]
fn epistemic_decay_scalar() {
    let snap = graph().analysis_snapshot();
    // now = valid_from + 30 days (1 half-life) -> confidence halves; the second row
    // has now == valid_from -> unchanged.
    let day: f64 = 86_400.0;
    let r = exec_sql(
        &snap,
        &format!(
            "SELECT epistemic_decay(1.0, 0.0, {}) AS half, epistemic_decay(0.8, 100.0, 100.0) AS same",
            30.0 * day
        ),
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let v = rows(&r);
    let half = v[0][0].as_f64().unwrap();
    assert!((half - 0.5).abs() < 1e-9, "30d half-life: {half}");
    assert_eq!(v[0][1], json!(0.8));
}

#[test]
fn pagerank_table_function_joins_against_nodes() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT n.id, p.score FROM nodes n JOIN pagerank() p ON n.id = p.id \
         ORDER BY p.score DESC",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    // n3 is the sink (two in-edges) so it must carry the most rank.
    assert_eq!(v[0][0], json!("n3"));
    // all scores are positive and n3 strictly dominates n1 (the source).
    assert!(v.iter().all(|row| row[1].as_f64().unwrap() > 0.0));
    let n3 = v[0][1].as_f64().unwrap();
    let n1 = v.iter().find(|row| row[0] == json!("n1")).unwrap()[1]
        .as_f64()
        .unwrap();
    assert!(n3 > n1, "sink n3 ({n3}) outranks source n1 ({n1})");
}

#[test]
fn betweenness_table_function() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT id, score FROM betweenness() ORDER BY id",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    // every node id appears exactly once with a numeric score.
    assert_eq!(v[0][0], json!("n1"));
    assert!(v.iter().all(|row| row[1].as_f64().is_some()));
}

#[test]
fn version_keyed_cache_hits_then_invalidates() {
    let core = graph();
    let cache = SqlCache::new();

    let v0 = core.version();
    let snap0 = core.analysis_snapshot();
    let r0 = exec_sql_cached(&snap0, v0, &cache, "SELECT COUNT(*) AS c FROM nodes").unwrap();
    assert_eq!(rows(&r0)[0][0], json!(3));

    // CACHE HIT: same version, but a DIFFERENT (here: empty) view. Because the cache
    // is keyed on `version`, it must serve the v0 tables (3 nodes), NOT re-scan the
    // empty view. This proves the batches are reused, not rebuilt.
    let empty = GraphCore::new().analysis_snapshot();
    let r_hit = exec_sql_cached(&empty, v0, &cache, "SELECT COUNT(*) AS c FROM nodes").unwrap();
    assert_eq!(
        rows(&r_hit)[0][0],
        json!(3),
        "stale-but-same-version query must hit the v0 cache"
    );

    // MUTATE: add a node the way the real write path does (write + mark_dirty bumps
    // the OCC version), then re-snapshot and re-query with the new version.
    core.add_node(
        "n4".to_string(),
        rmp_serde::to_vec_named(&json!({"type": "Agent", "rank": 4})).unwrap(),
    );
    core.mark_dirty();
    let v1 = core.version();
    assert!(v1 > v0, "mutation bumped the version: {v0} -> {v1}");

    let snap1 = core.analysis_snapshot();
    let r1 = exec_sql_cached(&snap1, v1, &cache, "SELECT COUNT(*) AS c FROM nodes").unwrap();
    assert_eq!(
        rows(&r1)[0][0],
        json!(4),
        "new version must invalidate the cache and re-scan"
    );
}

#[cfg(feature = "finance")]
#[test]
fn var_cvar_finance_aggregates() {
    use eg_core::graph::GraphCore;
    // A graph whose nodes carry a `ret` (daily return) column to aggregate over.
    let core = GraphCore::new();
    for (id, ret) in [
        ("a", -0.05),
        ("b", -0.02),
        ("c", 0.01),
        ("d", 0.03),
        ("e", 0.04),
    ] {
        core.add_node(
            id.to_string(),
            rmp_serde::to_vec_named(&json!({ "ret": ret })).unwrap(),
        );
    }
    let snap = core.analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT var(ret) AS v, cvar(ret) AS cv FROM nodes",
        &eg_query::CancellationToken::new(),
    )
    .unwrap();
    let v = rows(&r);
    let var = v[0][0].as_f64().unwrap();
    let cvar = v[0][1].as_f64().unwrap();
    // historical_var/cvar at 95% over this sample: worst loss dominates the tail, so
    // both are positive (losses are reported as positive magnitudes) and cvar >= var.
    assert!(var > 0.0, "var positive: {var}");
    assert!(cvar >= var, "cvar >= var: {cvar} >= {var}");
}
