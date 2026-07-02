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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivfpq::IvfPqParams;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn eg297_recall_is_one_when_ann_equals_exact() {
        let exact = vec![1u64, 2, 3, 4, 5];
        let ann = vec![1u64, 2, 3, 4, 5];
        assert_eq!(recall_at_k(&ann, &exact, 5), 1.0);
    }

    #[test]
    fn eg297_recall_is_order_insensitive() {
        // Recall is set-based: the SAME ids in a different order is still perfect.
        assert_eq!(recall_at_k(&[3, 1, 2], &[1, 2, 3], 3), 1.0);
    }

    #[test]
    fn eg297_recall_below_one_when_ann_misses_a_neighbor() {
        // ANN dropped the true neighbour `5`, substituting a non-neighbour.
        let exact = vec![1u64, 2, 3, 4, 5];
        let ann = vec![1u64, 2, 3, 4, 99];
        assert_eq!(recall_at_k(&ann, &exact, 5), 0.8);
        // Missing two of five → 0.6.
        assert_eq!(recall_at_k(&[1, 2, 3, 98, 99], &exact, 5), 0.6);
    }

    #[test]
    fn eg297_recall_truncates_both_lists_to_k() {
        // Only the top-k of each side counts.
        let exact = vec![1u64, 2, 3, 4, 5, 6];
        let ann = vec![1u64, 2, 3, 7, 8, 9];
        // top-3 exact {1,2,3} all present in top-3 ann → 1.0.
        assert_eq!(recall_at_k(&ann, &exact, 3), 1.0);
    }

    #[test]
    fn eg297_precision_distinct_from_recall() {
        // ANN returns 4, only 2 of which are true top-4 neighbours.
        let exact = vec![1u64, 2, 3, 4];
        let ann = vec![1u64, 99, 2, 98];
        assert_eq!(precision_at_k(&ann, &exact, 4), 0.5);
        assert_eq!(recall_at_k(&ann, &exact, 4), 0.5);
        // With fewer relevant, precision and recall diverge:
        let exact2 = vec![1u64, 2];
        // ann top-4 has 2 relevant of 4 returned → precision 0.5; of 2 relevant, both
        // found → recall 1.0.
        assert_eq!(precision_at_k(&[1, 2, 98, 99], &exact2, 4), 0.5);
        assert_eq!(recall_at_k(&[1, 2, 98, 99], &exact2, 4), 1.0);
    }

    #[test]
    fn eg297_average_precision_is_rank_sensitive() {
        // Relevant {1,2,3,4}; ANN hits at ranks 1 and 3.
        //   AP = (1/1 + 2/3) / 4.
        let exact = vec![1u64, 2, 3, 4];
        let ann = vec![1u64, 99, 2, 98];
        let ap = average_precision(&ann, &exact, 4);
        let expected = (1.0 + 2.0 / 3.0) / 4.0;
        assert!((ap - expected).abs() < 1e-9, "ap={ap} expected={expected}");
        // Same hits EARLIER (ranks 1,2) scores strictly higher.
        let ann_early = vec![1u64, 2, 98, 99];
        assert!(average_precision(&ann_early, &exact, 4) > ap);
    }

    #[test]
    fn eg297_mean_average_precision_over_pairs() {
        let pairs = vec![
            (vec![1u64, 2], vec![1u64, 2]),  // AP = 1.0
            (vec![9u64, 1], vec![1u64, 2]),  // relevant{1,2}; hit rank2 → (1/2)/2 = 0.25
        ];
        let m = mean_average_precision(&pairs, 2);
        assert!((m - (1.0 + 0.25) / 2.0).abs() < 1e-9, "map={m}");
        assert_eq!(mean_average_precision(&[], 5), 0.0);
    }

    #[test]
    fn eg297_empty_ground_truth_recall_is_one() {
        assert_eq!(recall_at_k(&[1, 2], &[], 5), 1.0);
        assert_eq!(average_precision(&[1, 2], &[], 5), 1.0);
    }

    /// Clustered synthetic vectors — the regime IVF-PQ is designed for.
    fn clustered(n: usize, dim: usize, ncl: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let centers: Vec<Vec<f32>> = (0..ncl)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect())
            .collect();
        (0..n)
            .map(|_| {
                let c = &centers[rng.gen_range(0..ncl)];
                (0..dim).map(|j| c[j] + (rng.gen::<f32>() - 0.5) * 0.2).collect()
            })
            .collect()
    }

    #[test]
    fn eg297_evaluate_recall_driver_high_when_ann_tracks_exact() {
        // End-to-end: a real IVF-PQ ANN measured against a FlatIndex ground truth
        // over the SAME vectors. With strong params (probe every cell + refine) the
        // ANN should track the exact top-10 closely.
        let dim = 32;
        let n = 2000;
        let data = clustered(n, dim, 20, 3);
        let params = IvfPqParams {
            dim,
            nlist: 32,
            m: dim / 4,
            kmeans_iters: 15,
            opq_iters: 4,
            seed: 7,
        };
        let mut ann = IvfPq::train(&params, &data);
        let items: Vec<(u64, Vec<f32>)> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.clone()))
            .collect();
        ann.add(&items);

        let mut truth = FlatIndex::new(dim);
        truth.add(&items);

        let mut qrng = ChaCha8Rng::seed_from_u64(101);
        let queries: Vec<Vec<f32>> = (0..30).map(|_| data[qrng.gen_range(0..n)].clone()).collect();

        let sp = SearchParams {
            nprobe: 32, // probe every cell → recall is bounded by PQ/refine, not IVF
            refine: true,
            refine_factor: 16,
        };
        let rep = evaluate_recall(&ann, &truth, &queries, 10, sp, Metric::L2);
        assert_eq!(rep.k, 10);
        assert_eq!(rep.n_queries, 30);
        assert!(rep.mean_recall <= 1.0 && rep.mean_recall >= 0.85, "mean_recall={}", rep.mean_recall);
        assert!(rep.min_recall <= rep.mean_recall + 1e-9);
        assert!(rep.mean_average_precision > 0.0 && rep.mean_average_precision <= 1.0);
        assert!(rep.mean_precision >= 0.0 && rep.mean_precision <= 1.0);
    }

    #[test]
    fn eg297_evaluate_recall_drops_below_one_when_ann_is_starved() {
        // Starve the ANN: probe a SINGLE cell so true neighbours in other cells are
        // structurally unreachable → recall must fall below 1.0 (a known miss),
        // proving the harness measures degradation, not a constant.
        let dim = 32;
        let n = 3000;
        let data = clustered(n, dim, 40, 5);
        let params = IvfPqParams {
            dim,
            nlist: 64,
            m: dim / 4,
            kmeans_iters: 12,
            opq_iters: 3,
            seed: 7,
        };
        let mut ann = IvfPq::train(&params, &data);
        let items: Vec<(u64, Vec<f32>)> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (i as u64, v.clone()))
            .collect();
        ann.add(&items);
        let mut truth = FlatIndex::new(dim);
        truth.add(&items);

        let mut qrng = ChaCha8Rng::seed_from_u64(202);
        let queries: Vec<Vec<f32>> = (0..40).map(|_| data[qrng.gen_range(0..n)].clone()).collect();
        let starved = SearchParams {
            nprobe: 1,
            refine: true,
            refine_factor: 4,
        };
        let rep = evaluate_recall(&ann, &truth, &queries, 10, starved, Metric::L2);
        assert!(
            rep.mean_recall < 1.0,
            "a single-probe ANN must miss some exact neighbours: mean_recall={}",
            rep.mean_recall
        );
    }
}
