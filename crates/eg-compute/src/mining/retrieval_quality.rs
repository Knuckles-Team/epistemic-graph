// CONCEPT:EG-KG.mining.retrieval-quality — Precision/recall/MRR over retrieval traces.
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
    pub n_queries: usize,
    pub k: usize,
}

/// Evaluate `traces` at cutoff `k` (CONCEPT:EG-KG.mining.retrieval-quality).
/// `k == 0` is treated as "no cutoff" (use the FULL retrieved list per trace).
pub fn evaluate(traces: &[RetrievalTrace], k: usize) -> RetrievalQuality {
    use std::collections::HashSet;

    let mut sum_precision = 0.0f64;
    let mut sum_recall = 0.0f64;
    let mut sum_rr = 0.0f64;
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

        sum_precision += precision;
        sum_recall += recall;
        sum_rr += rr;
        n += 1;
    }

    if n == 0 {
        return RetrievalQuality {
            precision_at_k: 0.0,
            recall_at_k: 0.0,
            mrr: 0.0,
            f1: 0.0,
            n_queries: 0,
            k,
        };
    }

    let precision_at_k = sum_precision / n as f64;
    let recall_at_k = sum_recall / n as f64;
    let mrr = sum_rr / n as f64;
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
