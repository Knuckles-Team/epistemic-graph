//! Recall / precision self-evaluation harness (CONCEPT:EG-297).
//!
//! Measures how much of the EXACT top-k an approximate search recovered, so the
//! IVF-PQ tier can be *measured* and *tuned* (nprobe, refine_factor, m, nlist)
//! against a ground truth instead of guessed. All functions take pre-ranked id
//! lists (nearest-first), so they are agnostic to how each side was produced:
//!
//!   * [`recall_at_k`] — the headline: fraction of the exact top-k that appears in
//!     the ANN top-k. `1.0` ⇔ the ANN missed nothing.
//!   * [`precision_at_k`] — fraction of the ANN top-k that is actually in the exact
//!     top-k (equals recall when both lists have exactly `k` entries, but distinct
//!     when the ANN returns fewer/more).
//!   * [`average_precision`] / [`mean_average_precision`] — rank-sensitive quality:
//!     rewards putting true neighbours EARLY, not just present.
//!   * [`evaluate_recall`] — an end-to-end driver over a query set, an [`IvfPq`] ANN
//!     index, and a [`FlatIndex`] ground truth, reporting mean/min recall@k, MAP,
//!     and precision in one [`RecallReport`].

use crate::flat::{FlatIndex, Metric};
use crate::ivfpq::{IvfPq, SearchParams};
use std::collections::HashSet;

/// Recall@k (CONCEPT:EG-297): of the exact top-k neighbours, the fraction the ANN's
/// top-k also returned — `|ann_topk ∩ exact_topk| / |exact_topk|`. Both inputs are
/// truncated to their first `k` ids before intersecting. Returns `1.0` when the
/// exact top-k is empty (nothing to miss), so an empty ground truth never drags a
/// mean down. Range `[0, 1]`; `1.0` = perfect recall.
pub fn recall_at_k(ann_results: &[u64], exact_results: &[u64], k: usize) -> f64 {
    let exact: HashSet<u64> = exact_results.iter().take(k).copied().collect();
    if exact.is_empty() {
        return 1.0;
    }
    let ann: HashSet<u64> = ann_results.iter().take(k).copied().collect();
    let hit = exact.intersection(&ann).count();
    hit as f64 / exact.len() as f64
}

/// Precision@k (CONCEPT:EG-297): of the ANN's top-k, the fraction that is truly in
/// the exact top-k — `|ann_topk ∩ exact_topk| / |ann_topk|`. Distinct from recall
/// only when the two lists differ in length; returns `1.0` for an empty ANN list.
pub fn precision_at_k(ann_results: &[u64], exact_results: &[u64], k: usize) -> f64 {
    let ann: Vec<u64> = ann_results.iter().take(k).copied().collect();
    if ann.is_empty() {
        return 1.0;
    }
    let exact: HashSet<u64> = exact_results.iter().take(k).copied().collect();
    let hit = ann.iter().filter(|id| exact.contains(id)).count();
    hit as f64 / ann.len() as f64
}

/// Average precision of one ranked ANN list against the exact top-k relevant set
/// (CONCEPT:EG-297). Walks the ANN ids in rank order; at each position that holds a
/// relevant id, accumulates the running precision, then divides by the number of
/// relevant items. Rank-sensitive: surfacing true neighbours earlier scores higher.
/// Returns `1.0` when there are no relevant items.
pub fn average_precision(ann_results: &[u64], exact_results: &[u64], k: usize) -> f64 {
    let relevant: HashSet<u64> = exact_results.iter().take(k).copied().collect();
    if relevant.is_empty() {
        return 1.0;
    }
    let mut hits = 0usize;
    let mut sum_prec = 0.0f64;
    for (rank, id) in ann_results.iter().take(k).enumerate() {
        if relevant.contains(id) {
            hits += 1;
            sum_prec += hits as f64 / (rank + 1) as f64;
        }
    }
    sum_prec / relevant.len() as f64
}

/// Mean average precision over many `(ann, exact)` query pairs (CONCEPT:EG-297) —
/// the mean of [`average_precision`] across the query set. `0.0` for an empty set.
pub fn mean_average_precision(pairs: &[(Vec<u64>, Vec<u64>)], k: usize) -> f64 {
    if pairs.is_empty() {
        return 0.0;
    }
    let sum: f64 = pairs
        .iter()
        .map(|(ann, exact)| average_precision(ann, exact, k))
        .sum();
    sum / pairs.len() as f64
}

/// The aggregate quality of an ANN index vs. exact ground truth (CONCEPT:EG-297),
/// produced by [`evaluate_recall`].
#[derive(Clone, Debug, PartialEq)]
pub struct RecallReport {
    /// The `k` recall/precision were measured at.
    pub k: usize,
    /// Number of queries evaluated.
    pub n_queries: usize,
    /// Mean recall@k across the query set (the headline number). `[0, 1]`.
    pub mean_recall: f64,
    /// The single WORST query's recall@k — the tail an average hides.
    pub min_recall: f64,
    /// Mean precision@k across the query set.
    pub mean_precision: f64,
    /// Mean average precision (rank-sensitive) across the query set.
    pub mean_average_precision: f64,
}

/// End-to-end recall driver (CONCEPT:EG-297): for each query, take the ANN top-k
/// from `ann.search` and the EXACT top-k from `truth.search(metric)`, then report
/// mean/min recall@k, mean precision@k, and MAP across the whole query set. This is
/// the measurement loop for tuning ANN parameters (`sp`, `m`, `nlist`) against a
/// known-correct baseline — a flat brute-force [`FlatIndex`] over the same vectors.
pub fn evaluate_recall(
    ann: &IvfPq,
    truth: &FlatIndex,
    queries: &[Vec<f32>],
    k: usize,
    sp: SearchParams,
    metric: Metric,
) -> RecallReport {
    let mut sum_recall = 0.0f64;
    let mut min_recall = 1.0f64;
    let mut sum_precision = 0.0f64;
    let mut pairs: Vec<(Vec<u64>, Vec<u64>)> = Vec::with_capacity(queries.len());

    for q in queries {
        let ann_ids: Vec<u64> = ann.search(q, k, sp).into_iter().map(|r| r.id).collect();
        let exact_ids: Vec<u64> = truth
            .search(q, k, metric)
            .into_iter()
            .map(|r| r.id)
            .collect();
        let r = recall_at_k(&ann_ids, &exact_ids, k);
        sum_recall += r;
        if r < min_recall {
            min_recall = r;
        }
        sum_precision += precision_at_k(&ann_ids, &exact_ids, k);
        pairs.push((ann_ids, exact_ids));
    }

    let nq = queries.len();
    let denom = nq.max(1) as f64;
    RecallReport {
        k,
        n_queries: nq,
        mean_recall: sum_recall / denom,
        min_recall: if nq == 0 { 1.0 } else { min_recall },
        mean_precision: sum_precision / denom,
        mean_average_precision: mean_average_precision(&pairs, k),
    }
}
