//! The separate-surfaces oracle — the correctness proof for the fused planner.
//!
//! [`separate_surfaces`] runs the canonical cross-modal query the SILOED way: three
//! independent surface calls (a SQL filter, then an independent graph BFS, then an
//! independent vector kNN) wired by the CALLER — exactly as the system does today
//! across three round-trips. The fused [`crate::Plan::execute`] must return the
//! BYTE-IDENTICAL ordered id list. That identity is the proof the unification is
//! real and not a re-implementation: the fused plan threads DataFusion's id output
//! into the same BFS into the same kNN, just without the round-trips.

use std::collections::HashSet;

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphView;

use crate::algebra::Pred;
use crate::exec::{bfs_reached, where_clause};
use crate::rowset::RowSet;

/// The canonical fused query — "nodes of `label` matching `preds` (relational),
/// traverse `rel` for `min..=max` hops (graph), rank the reached set by similarity
/// to `query` (vector), top-`k`" — executed as three siloed surface calls. Used as
/// the correctness oracle for [`crate::Plan::execute`].
///
/// Takes the full query as positional args to mirror the three siloed surface calls
/// a caller wires today (a struct would obscure the 1:1 correspondence).
#[allow(clippy::too_many_arguments)]
pub fn separate_surfaces(
    view: &GraphView,
    semantic: &SemanticStore,
    label: &str,
    preds: &[Pred],
    rel: &str,
    min: usize,
    max: usize,
    query: &[f32],
    k: usize,
) -> Result<RowSet, String> {
    // Surface 1 — SQL: nodes WHERE type = label AND preds, through real DataFusion.
    let mut all_preds = vec![Pred::Eq {
        prop: "type".into(),
        value: label.into(),
    }];
    all_preds.extend_from_slice(preds);
    let where_sql = where_clause(&all_preds)?;
    let sql = format!("SELECT id FROM nodes WHERE {where_sql}");
    let filtered = filter_ids(view, &sql)?;

    // Surface 2 — graph: BFS along `rel` from the filtered set.
    let reached = bfs_reached(view, &filtered, rel, min, max);
    let reached_set: HashSet<&str> = reached.iter().map(String::as_str).collect();

    // Surface 3 — vector: kNN, keep reached, top-k.
    let want = (reached_set.len() * 4).max(reached_set.len() + 32).max(k);
    let ranked = semantic.semantic_search(query, want);
    let scored: Vec<(String, f32)> = ranked
        .into_iter()
        .filter(|(id, _)| reached_set.contains(id.as_str()))
        .collect();
    Ok(RowSet::from_scored(scored).limit(k))
}

/// Run a `SELECT id FROM nodes WHERE …` through real DataFusion and decode the ids.
fn filter_ids(view: &GraphView, sql: &str) -> Result<Vec<String>, String> {
    let result = eg_query::exec_sql(view, sql, &eg_query::CancellationToken::new())?;
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let cells: Vec<serde_json::Value> = eg_types::msgpack::decode_bounded(
            row,
            eg_types::msgpack::MsgpackLimits::new(
                eg_types::msgpack::MAX_PROPERTY_BYTES,
                eg_types::msgpack::MAX_PROPERTY_ITEMS,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| "decode oracle row failed".to_string())?;
        match cells.first().and_then(|v| v.as_str()) {
            Some(id) => out.push(id.to_string()),
            None => return Err("oracle filter row had no string id cell".into()),
        }
    }
    Ok(out)
}
