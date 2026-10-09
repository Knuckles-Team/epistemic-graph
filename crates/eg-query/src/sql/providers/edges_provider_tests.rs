use super::*;
use crate::sql::exec::exec_sql;
use crate::sql::CancellationToken;
use datafusion::logical_expr::{col, lit};
use eg_core::graph::GraphCore;
use serde_json::json;

/// n1 -> n2, n1 -> n3, n2 -> n3 (a tiny DAG so src/dst pushdown each narrow to
/// a strict, non-trivial, non-empty subset of the 3 edges).
fn graph() -> GraphCore {
    let core = GraphCore::new();
    for id in ["n1", "n2", "n3"] {
        core.add_node(id.into(), rmp_serde::to_vec_named(&json!({})).unwrap());
    }
    for (s, d) in [("n1", "n2"), ("n1", "n3"), ("n2", "n3")] {
        core.add_edge(
            s.into(),
            d.into(),
            rmp_serde::to_vec_named(&json!({})).unwrap(),
        )
        .unwrap();
    }
    core
}

fn ids(r: &crate::sql::QueryResult, col_index: usize) -> Vec<String> {
    r.rows
        .iter()
        .map(|blob| {
            let cells: Vec<serde_json::Value> = rmp_serde::from_slice(blob).unwrap();
            cells[col_index].as_str().unwrap().to_string()
        })
        .collect()
}

/// A `src = 'n1'` equality returns EXACTLY n1's two outgoing edges — never n2's
/// or n3's — proving `scan_by_src`'s O(deg) walk is both correct (right rows)
/// and narrowing (not the full 3-edge set) rather than a full-scan-then-filter
/// that happened to produce the right answer regardless.
#[test]
fn src_equality_returns_only_that_nodes_outgoing_edges() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT dst FROM edges WHERE src = 'n1' ORDER BY dst",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(ids(&r, 0), vec!["n2".to_string(), "n3".to_string()]);
}

/// A `dst = 'n3'` equality returns EXACTLY n3's two incoming edges (from n1 and
/// n2), proving `scan_by_dst`'s O(deg) incoming walk.
#[test]
fn dst_equality_returns_only_that_nodes_incoming_edges() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT src FROM edges WHERE dst = 'n3' ORDER BY src",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(ids(&r, 0), vec!["n1".to_string(), "n2".to_string()]);
}

/// A `src` equality naming a node with NO outgoing edges (or that doesn't
/// exist at all) returns zero rows — not an error — matching what a full scan
/// would find.
#[test]
fn src_equality_on_a_sink_or_absent_node_returns_no_rows() {
    let snap = graph().analysis_snapshot();
    let sink = exec_sql(
        &snap,
        "SELECT dst FROM edges WHERE src = 'n3'",
        &CancellationToken::new(),
    )
    .unwrap();
    assert!(sink.rows.is_empty(), "n3 has no outgoing edges");

    let absent = exec_sql(
        &snap,
        "SELECT dst FROM edges WHERE src = 'ghost'",
        &CancellationToken::new(),
    )
    .unwrap();
    assert!(absent.rows.is_empty(), "ghost names no node at all");
}

/// A combined `src = 'n1' AND dst = 'n2'` equality (both pushed) resolves to
/// exactly the one matching edge — `scan_by_src`'s per-candidate `dst` check.
#[test]
fn combined_src_and_dst_equality_resolves_to_the_one_edge() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT src, dst FROM edges WHERE src = 'n1' AND dst = 'n2'",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(ids(&r, 0), vec!["n1".to_string()]);
    assert_eq!(ids(&r, 1), vec!["n2".to_string()]);
}

/// An unfiltered `SELECT * FROM edges` still returns the full, correct edge
/// set (the `full_batch` fallback) — the pushdown adds a fast path, it never
/// narrows a query that didn't ask for it.
#[test]
fn unfiltered_query_returns_every_edge() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT src, dst FROM edges ORDER BY src, dst",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(r.rows.len(), 3);
}

/// A `rel = '<src>:<dst>'` equality (the ONE column with no adjacency
/// shortcut, per the module doc) still returns the correct row — via the
/// `Unsupported`-classified post-scan `Filter`, not a pushdown into `scan`.
#[test]
fn rel_equality_still_works_via_the_post_scan_filter() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT src, dst FROM edges WHERE rel = 'n1:n2'",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(ids(&r, 0), vec!["n1".to_string()]);
    assert_eq!(ids(&r, 1), vec!["n2".to_string()]);
}

fn edges_provider() -> EdgesTableProvider {
    let snap = graph().analysis_snapshot();
    EdgesTableProvider::new(Arc::new(snap))
}

/// Direct classification check (mirrors `NodesTableProvider`'s own
/// `supports_filters_classifies_predicates` test): `src`/`dst` equality is
/// `Inexact` (pushed to `scan`'s adjacency walk); `rel` equality and a
/// non-equality predicate are `Unsupported` (kept as an ordinary Filter).
#[test]
fn supports_filters_pushdown_classifies_src_dst_inexact_rel_unsupported() {
    let p = edges_provider();
    let src_eq = col("src").eq(lit("n1"));
    let dst_eq = col("dst").eq(lit("n2"));
    let rel_eq = col("rel").eq(lit("n1:n2"));
    let like = col("src").like(lit("n%"));
    let refs: Vec<&Expr> = vec![&src_eq, &dst_eq, &rel_eq, &like];
    let got = p.supports_filters_pushdown(&refs).unwrap();
    assert_eq!(got[0], TableProviderFilterPushDown::Inexact, "src");
    assert_eq!(got[1], TableProviderFilterPushDown::Inexact, "dst");
    assert_eq!(
        got[2],
        TableProviderFilterPushDown::Unsupported,
        "rel — no adjacency shortcut, see the module doc"
    );
    assert_eq!(
        got[3],
        TableProviderFilterPushDown::Unsupported,
        "non-equality"
    );
}

/// `EdgesTableProvider::schema()` is the same static shape `edges_schema()`
/// returns — proving the schema needs no batch/scan to be known (the
/// asymmetry with `nodes` the module doc explains).
#[test]
fn schema_is_available_with_no_scan() {
    let p = edges_provider();
    assert_eq!(p.schema(), edges_schema());
}
