//! Cache-coherence SEAM regression (CONCEPT:EG-363).
//!
//! The audit's cache finding: the SQL result cache is **version-keyed** — a write bumps
//! the OCC `version()`, so the next query at the new version MISSES and re-scans, and a
//! query at the old version HITS (serves the pinned batches). (The `eg-kvcache`
//! token-hash cache is content-addressed with NO data-version hook — a different, LLM-KV
//! concern — so the *data* cache that must invalidate on write is this one.)
//!
//! Each proof runs the cache ON (`exec_sql_cached`) vs OFF (`exec_sql`) and asserts
//! byte-identical rows, plus explicit hit/miss + fresh-row assertions across a mutation.

#![cfg(feature = "sql")]

use eg_core::graph::GraphCore;
use eg_query::{exec_sql, exec_sql_cached, QueryResult, SqlCache};
use serde_json::json;

/// n1,n2,n3 (types Agent/Agent/Tool). Returned as a `GraphCore` so a test can mutate it
/// and bump the OCC `version()` the cache keys on.
fn graph() -> GraphCore {
    let core = GraphCore::new();
    for (id, ty, rank) in [("n1", "Agent", 1i64), ("n2", "Agent", 2), ("n3", "Tool", 3)] {
        core.add_node(
            id.to_string(),
            rmp_serde::to_vec_named(&json!({ "type": ty, "rank": rank })).unwrap(),
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

/// Populate the cache at v0, mutate (SQL-style write via the graph core + `mark_dirty`),
/// then re-run: the new version must MISS the cache and return FRESH rows, while a query
/// pinned at the OLD version still HITS the stale-but-valid batches.
#[test]
fn result_cache_invalidates_on_mutation() {
    let core = graph();
    let cache = SqlCache::new();
    let sql = "SELECT id FROM nodes WHERE type = 'Agent' ORDER BY id";

    // ── populate at v0 ──
    let v0 = core.version();
    let r0 = exec_sql_cached(&core.analysis_snapshot(), v0, &cache, sql).unwrap();
    assert_eq!(rows(&r0), vec![vec![json!("n1")], vec![json!("n2")]]);

    // ── HIT: same version, a DIFFERENT (empty) view. Version-keyed ⇒ serves v0 rows,
    //    NOT a re-scan of the empty view (proves the batches are reused). ──
    let empty = GraphCore::new().analysis_snapshot();
    let r_hit = exec_sql_cached(&empty, v0, &cache, sql).unwrap();
    assert_eq!(
        rows(&r_hit),
        vec![vec![json!("n1")], vec![json!("n2")]],
        "stale-but-same-version query HITS the v0 cache"
    );

    // ── MUTATE: add a fresh Agent (the write path bumps the OCC version). ──
    core.add_node(
        "n4".to_string(),
        rmp_serde::to_vec_named(&json!({"type":"Agent","rank":4})).unwrap(),
    );
    core.mark_dirty();
    let v1 = core.version();
    assert!(v1 > v0, "mutation bumped the version: {v0} -> {v1}");

    // ── MISS at v1: cache invalidated ⇒ re-scan ⇒ the fresh row appears. ──
    let r1 = exec_sql_cached(&core.analysis_snapshot(), v1, &cache, sql).unwrap();
    assert_eq!(
        rows(&r1),
        vec![vec![json!("n1")], vec![json!("n2")], vec![json!("n4")]],
        "new version MISSES the cache and returns the fresh row"
    );
}

/// Cache ON vs OFF is transparent: the version-keyed cached path returns byte-identical
/// rows to the uncached path — the cache is a pure performance optimization, never a
/// correctness change, across BOTH a stable state and after a mutation.
#[test]
fn result_cache_on_vs_off_identical() {
    let core = graph();
    let cache = SqlCache::new();
    let sql = "SELECT id, rank FROM nodes ORDER BY id";

    let off = rows(&exec_sql(&core.analysis_snapshot(), sql).unwrap());
    let on =
        rows(&exec_sql_cached(&core.analysis_snapshot(), core.version(), &cache, sql).unwrap());
    assert_eq!(on, off, "cache ON must equal cache OFF (stable state)");

    // Mutate, then re-check ON == OFF at the new version (both see the fresh row).
    core.add_node(
        "n4".to_string(),
        rmp_serde::to_vec_named(&json!({"type":"Agent","rank":4})).unwrap(),
    );
    core.mark_dirty();
    let off2 = rows(&exec_sql(&core.analysis_snapshot(), sql).unwrap());
    let on2 =
        rows(&exec_sql_cached(&core.analysis_snapshot(), core.version(), &cache, sql).unwrap());
    assert_eq!(on2, off2, "cache ON must equal cache OFF (post-mutation)");
    assert_eq!(on2.len(), 4, "the fresh row is visible on both paths");
}
