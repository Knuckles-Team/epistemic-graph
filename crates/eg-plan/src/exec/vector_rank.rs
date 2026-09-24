//! Vector `RANK` over a candidate set (EH-564/EH-565,
//! CONCEPT:EG-KG.query.filtered-vector-rank).
//!
//! A `Rank` ranks the candidates its input carries. Two physical paths:
//!
//! * **Exact** — the candidates' own embeddings are scored by cosine
//!   ([`SemanticStore::exact_rank_candidates`]), O(|candidates| · dim). Used whenever
//!   the whole candidate order is wanted, and for any candidate set up to
//!   [`EXACT_RANK_MAX`] when only a top `k` is wanted.
//! * **Filtered ANN** — for a larger candidate set feeding a `Limit k`, the ANN walk
//!   runs with the candidates as its allowlist, asking for `k · OVERSAMPLE` hits and
//!   widening by `OVERSAMPLE` each round. A walk that still strands candidates after
//!   [`ANN_ROUNDS`] rounds is topped up exactly, so `k` rows survive whenever `k`
//!   candidates hold an embedding (EH-564: a traversal-then-rank query returned 7–9
//!   of 10).
//!
//! The previous single path asked the filtered walk for `|candidates|` hits, which
//! both cost more than scoring the candidates (the walk's beam grew to the candidate
//! count) and could strand candidates the walk never reached.

use std::collections::HashSet;

use eg_core::compute::semantic::SemanticStore;

use crate::algebra::Op;
use crate::exec::PlanCtx;
use crate::rowset::RowSet;

/// The largest candidate set a top-`k` rank scores exactly. Above it the filtered ANN
/// walk is cheaper; the crossover was measured with `benches/filtered_rank.rs` (see
/// the EH-565 evidence in the lane WRAPUP).
pub(crate) const EXACT_RANK_MAX: usize = 4_096;

/// Oversampling factor of the filtered ANN walk, applied again on every widening round.
const OVERSAMPLE: usize = 4;

/// Widening rounds before the exact top-up.
const ANN_ROUNDS: usize = 3;

/// `RANK BY ~query` over every candidate (the whole order).
pub(crate) fn rank_op(ctx: &PlanCtx, query: &[f32], input: RowSet) -> Result<RowSet, String> {
    rank_top(ctx, query, input, None)
}

/// `Some((query, k))` when `ops[at]` is a vector `Rank` immediately followed by
/// `Limit k`, so only the top `k` of the ranking is ever read.
pub(crate) fn rank_then_limit(ops: &[Op], at: usize) -> Option<(&[f32], usize)> {
    match (ops.get(at)?, ops.get(at + 1)?) {
        (Op::Rank { query }, Op::Limit { k }) => Some((query.as_slice(), *k)),
        _ => None,
    }
}

/// `RANK BY ~query |> LIMIT k` as one physical step. The trailing `Limit` still runs
/// and is then a no-op.
pub(crate) fn rank_limit(
    ctx: &PlanCtx,
    query: &[f32],
    input: RowSet,
    k: usize,
) -> Result<RowSet, String> {
    rank_top(ctx, query, input, Some(k))
}

fn rank_top(
    ctx: &PlanCtx,
    query: &[f32],
    input: RowSet,
    limit: Option<usize>,
) -> Result<RowSet, String> {
    let candidates = input.id_set();
    if candidates.is_empty() {
        return Ok(RowSet::new());
    }
    check_width(ctx.semantic, query)?;
    let want = limit.map_or(candidates.len(), |k| k.min(candidates.len()));
    let scored = ranked(ctx.semantic, query, &candidates, want, EXACT_RANK_MAX);
    Ok(RowSet::from_scored(scored))
}

/// F4 (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard): a query whose width differs
/// from the store's embedding width is a typed error, not a silent empty ranking.
fn check_width(semantic: &SemanticStore, query: &[f32]) -> Result<(), String> {
    let store_dim = semantic.dim();
    if store_dim == 0 || query.len() == store_dim {
        return Ok(());
    }
    Err(format!(
        "RANK BY ~{query:?}: query vector dimension mismatch — expected {store_dim} (the \
         store's embedding dimension), got {}",
        query.len()
    ))
}

/// The best `want` candidates, by the path the candidate count selects (see the module
/// docs). `exact_max` is the exact-path ceiling ([`EXACT_RANK_MAX`] when serving).
pub(crate) fn ranked(
    semantic: &SemanticStore,
    query: &[f32],
    candidates: &HashSet<&str>,
    want: usize,
    exact_max: usize,
) -> Vec<(String, f32)> {
    if candidates.len() <= exact_max || want >= candidates.len() {
        return semantic.exact_rank_candidates(query, candidates.iter().copied(), want);
    }
    let allow = |id: &str| candidates.contains(id);
    let mut fetch = want.saturating_mul(OVERSAMPLE);
    for _ in 0..ANN_ROUNDS {
        let mut hits = semantic.semantic_search_filtered(query, fetch, allow);
        if hits.len() >= want {
            hits.truncate(want);
            return hits;
        }
        fetch = fetch.saturating_mul(OVERSAMPLE);
    }
    semantic.exact_rank_candidates(query, candidates.iter().copied(), want)
}
