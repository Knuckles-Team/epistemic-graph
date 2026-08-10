// CONCEPT:EG-KG.mining.retrieval-quality — Precision/recall/MRR/NDCG over retrieval traces.
//
// Pure-Rust, dependency-light: given a set of stored RETRIEVAL TRACES — each the
// ranked list of ids a query actually retrieved plus the ground-truth relevant
// ids for that query — compute the standard IR quality metrics:
//
//   * **Precision@k** — of the top `k` retrieved ids, what fraction are relevant.
//   * **Recall@k**    — of all relevant ids, what fraction appear in the top `k`.
//   * **MRR**         — mean reciprocal rank of the FIRST relevant hit (`0` if
//     none of the top `k` are relevant) — rewards ranking a relevant result
//     EARLY, complementing precision/recall's rank-agnostic view.
//   * **NDCG@k** (GOC-08) — Normalized Discounted Cumulative Gain: like MRR, rank-
//     sensitive (a relevant hit at rank 1 counts more than one at rank 10), but
//     unlike MRR it credits EVERY relevant hit in the top `k`, not just the first.
//     `RetrievalTrace.relevant` is unweighted (a set, not graded judgments), so
//     the per-position gain is binary (`1` if `retrieved[i]` is relevant, else
//     `0`) — the standard reduction of DCG to binary relevance, not an invented
//     variant. `DCG@k = Σ gain_i / log2(i + 2)` (`i` is the 0-based rank, so the
//     top result divides by `log2(2) = 1`); `IDCG@k` is the DCG of the best
//     possible ordering (all relevant ids first, up to `min(k, |relevant|)`).
//     `NDCG@k = DCG@k / IDCG@k`, in `[0, 1]`; `1.0` = every relevant id the query
//     could possibly surface within the cutoff is surfaced, earliest first.
//
// averaged across every trace with a non-empty `relevant` set (a trace with no
// declared ground truth contributes no signal and is skipped, not scored `0`).
// No fabricated aggregate: the single scalar the epistemic writeback anchors its
// claim confidence to is the **F1** of the averaged precision/recall — the
// standard harmonic-mean combination, not a new invented formula.

/// One stored retrieval trace: what was retrieved (ranked) vs. what was actually relevant.
#[derive(Debug, Clone, PartialEq)]
pub struct RetrievalTrace {
    pub retrieved: Vec<String>,
    pub relevant: Vec<String>,
}

/// The aggregate quality report over a batch of traces.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetrievalQuality {
    pub precision_at_k: f64,
    pub recall_at_k: f64,
    pub mrr: f64,
    /// Harmonic mean of `precision_at_k` and `recall_at_k`; `0.0` when both are `0.0`.
    pub f1: f64,
    /// Mean Normalized Discounted Cumulative Gain at `k` (GOC-08) — see module docs.
    pub ndcg_at_k: f64,
    pub n_queries: usize,
    pub k: usize,
}

/// DCG@`cutoff` for one ranked list under binary relevance (CONCEPT:EG-KG.mining.retrieval-quality,
/// GOC-08): `Σ_{i=0}^{cutoff-1} gain(top[i]) / log2(i + 2)`. `relevant_set` decides
/// `gain` (`1.0` if the id is relevant, else `0.0`).
fn dcg(top: &[String], relevant_set: &std::collections::HashSet<&String>) -> f64 {
    top.iter()
        .enumerate()
        .map(|(rank, id)| {
            let gain = if relevant_set.contains(id) { 1.0 } else { 0.0 };
            gain / (rank as f64 + 2.0).log2()
        })
        .sum()
}

/// Ideal DCG@`cutoff` under binary relevance (GOC-08): the best possible ordering
/// puts all `n_relevant` relevant ids first, so it is just `dcg` of `n_relevant`
/// leading `1`s truncated to `cutoff` — no ranked list needed.
fn idcg(cutoff: usize, n_relevant: usize) -> f64 {
    (0..cutoff.min(n_relevant))
        .map(|rank| 1.0 / (rank as f64 + 2.0).log2())
        .sum()
}

/// Evaluate `traces` at cutoff `k` (CONCEPT:EG-KG.mining.retrieval-quality).
/// `k == 0` is treated as "no cutoff" (use the FULL retrieved list per trace).
pub fn evaluate(traces: &[RetrievalTrace], k: usize) -> RetrievalQuality {
    use std::collections::HashSet;

    let mut sum_precision = 0.0f64;
    let mut sum_recall = 0.0f64;
    let mut sum_rr = 0.0f64;
    let mut sum_ndcg = 0.0f64;
    let mut n = 0usize;

    for t in traces {
        if t.relevant.is_empty() {
            continue; // no ground truth, no signal
        }
        let cutoff = if k == 0 {
            t.retrieved.len()
        } else {
            k.min(t.retrieved.len())
        };
        let top: &[String] = &t.retrieved[..cutoff];
        let relevant_set: HashSet<&String> = t.relevant.iter().collect();

        let hits = top.iter().filter(|id| relevant_set.contains(id)).count();
        let precision = if cutoff > 0 {
            hits as f64 / cutoff as f64
        } else {
            0.0
        };
        let recall = hits as f64 / t.relevant.len() as f64;
        let rr = top
            .iter()
            .position(|id| relevant_set.contains(id))
            .map(|pos| 1.0 / (pos + 1) as f64)
            .unwrap_or(0.0);
        let ideal = idcg(cutoff, t.relevant.len());
        let ndcg = if ideal > 0.0 {
            dcg(top, &relevant_set) / ideal
        } else {
            0.0 // cutoff == 0 (empty retrieved list): nothing possible to rank, no signal
        };

        sum_precision += precision;
        sum_recall += recall;
        sum_rr += rr;
        sum_ndcg += ndcg;
        n += 1;
    }

    if n == 0 {
        return RetrievalQuality {
            precision_at_k: 0.0,
            recall_at_k: 0.0,
            mrr: 0.0,
            f1: 0.0,
            ndcg_at_k: 0.0,
            n_queries: 0,
            k,
        };
    }

    let precision_at_k = sum_precision / n as f64;
    let recall_at_k = sum_recall / n as f64;
    let mrr = sum_rr / n as f64;
    let ndcg_at_k = sum_ndcg / n as f64;
    let f1 = if precision_at_k + recall_at_k > 0.0 {
        2.0 * precision_at_k * recall_at_k / (precision_at_k + recall_at_k)
    } else {
        0.0
    };

    RetrievalQuality {
        precision_at_k,
        recall_at_k,
        mrr,
        f1,
        ndcg_at_k,
        n_queries: n,
        k,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace(retrieved: &[&str], relevant: &[&str]) -> RetrievalTrace {
        RetrievalTrace {
            retrieved: retrieved.iter().map(|s| s.to_string()).collect(),
            relevant: relevant.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn perfect_retrieval_yields_perfect_scores() {
        let traces = vec![trace(&["a", "b"], &["a", "b"])];
        let q = evaluate(&traces, 2);
        assert!((q.precision_at_k - 1.0).abs() < 1e-9);
        assert!((q.recall_at_k - 1.0).abs() < 1e-9);
        assert!((q.mrr - 1.0).abs() < 1e-9);
        assert!((q.f1 - 1.0).abs() < 1e-9);
        assert!(
            (q.ndcg_at_k - 1.0).abs() < 1e-9,
            "the retrieved order already IS the ideal order"
        );
    }

    /// GOC-08: NDCG's distinguishing behaviour vs. MRR — it credits EVERY relevant
    /// hit in the cutoff, not just the first. Two rankings with the same first-hit
    /// rank (so identical MRR) but different SECOND-hit placement must score
    /// differently on NDCG.
    #[test]
    fn ndcg_credits_every_relevant_hit_not_just_the_first() {
        // Both relevant ids present, correct order (best possible) → NDCG == 1.0.
        let best = vec![trace(&["a", "b", "x"], &["a", "b"])];
        let q_best = evaluate(&best, 3);
        assert!((q_best.ndcg_at_k - 1.0).abs() < 1e-9);

        // Same first hit (rank 1, so identical MRR == 1.0) but the second relevant
        // id is pushed to the LAST position — DCG loses the rank-2 gain the ideal
        // ordering would have banked, so NDCG must be strictly less than 1.0 even
        // though MRR is unchanged.
        let worse = vec![trace(&["a", "x", "b"], &["a", "b"])];
        let q_worse = evaluate(&worse, 3);
        assert!(
            (q_worse.mrr - 1.0).abs() < 1e-9,
            "first hit still at rank 1"
        );
        assert!(
            q_worse.ndcg_at_k < q_best.ndcg_at_k,
            "delaying the SECOND relevant hit must lower NDCG even though MRR does not move: \
             best={}, worse={}",
            q_best.ndcg_at_k,
            q_worse.ndcg_at_k
        );
        assert!(q_worse.ndcg_at_k < 1.0);
    }

    /// GOC-08: when there are more relevant ids than the cutoff can ever surface,
    /// the IDEAL ranking is also capped at `k` — NDCG must still reach `1.0` for a
    /// ranking that fills the cutoff entirely with relevant ids, rather than being
    /// unfairly penalized for relevant ids beyond `k` it could never have shown.
    #[test]
    fn ndcg_ideal_is_capped_at_the_cutoff_not_all_relevant_ids() {
        let traces = vec![trace(&["a", "b"], &["a", "b", "c", "d", "e"])];
        let q = evaluate(&traces, 2);
        assert!(
            (q.ndcg_at_k - 1.0).abs() < 1e-9,
            "top-2 is entirely relevant ids in the best possible order for a k=2 \
             cutoff — must score 1.0 regardless of the 3 relevant ids k=2 could \
             never have surfaced: got {}",
            q.ndcg_at_k
        );
    }

    #[test]
    fn mrr_rewards_early_relevant_hits() {
        let traces = vec![trace(&["irrelevant", "a"], &["a"])];
        let q = evaluate(&traces, 2);
        assert!((q.mrr - 0.5).abs() < 1e-9); // first hit at rank 2
    }

    #[test]
    fn no_relevant_hits_yields_zero_precision_recall_mrr() {
        let traces = vec![trace(&["x", "y"], &["a"])];
        let q = evaluate(&traces, 2);
        assert_eq!(q.precision_at_k, 0.0);
        assert_eq!(q.recall_at_k, 0.0);
        assert_eq!(q.mrr, 0.0);
        assert_eq!(q.ndcg_at_k, 0.0);
    }

    /// GOC-08 edge case: a trace with ground truth but an EMPTY retrieved list
    /// (cutoff == 0) must report NDCG `0.0`, not `NaN` from a `0.0 / 0.0` IDCG
    /// division — mirrors `precision_at_k`'s existing `cutoff > 0` guard.
    #[test]
    fn ndcg_is_zero_not_nan_for_an_empty_retrieved_list() {
        let traces = vec![trace(&[], &["a"])];
        let q = evaluate(&traces, 5);
        assert_eq!(
            q.n_queries, 1,
            "ground truth is present, so this trace counts"
        );
        assert_eq!(q.ndcg_at_k, 0.0);
        assert!(!q.ndcg_at_k.is_nan());
    }

    #[test]
    fn traces_without_ground_truth_are_skipped_not_zeroed() {
        let traces = vec![
            trace(&["a"], &[]), // no ground truth ⇒ skipped
            trace(&["a"], &["a"]),
        ];
        let q = evaluate(&traces, 1);
        assert_eq!(q.n_queries, 1);
        assert!((q.precision_at_k - 1.0).abs() < 1e-9);
    }

    #[test]
    fn empty_batch_yields_zeroed_report_not_panic() {
        let q = evaluate(&[], 5);
        assert_eq!(q.n_queries, 0);
        assert_eq!(q.f1, 0.0);
    }

    #[test]
    fn k_zero_uses_the_full_retrieved_list() {
        let traces = vec![trace(&["irrelevant", "a", "b"], &["a", "b"])];
        let q = evaluate(&traces, 0);
        assert!((q.recall_at_k - 1.0).abs() < 1e-9);
    }
}
