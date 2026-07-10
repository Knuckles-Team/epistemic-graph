// CONCEPT:EG-KG.mining.root-cause — Anomaly propagation to a most-likely upstream cause.
//
// Pure-Rust, dependency-light: given a directed, weighted DEPENDENCY graph
// (`cause -> effect`, e.g. "service A calls service B" or "upstream sensor feeds
// downstream gauge") and a per-node anomaly score (from the existing `anomaly`
// family, or any other `[0,∞)` score), find — for one SYMPTOM node already known
// to be anomalous — the most likely upstream ROOT CAUSE.
//
// The search walks BACKWARD from the symptom along the reversed dependency edges
// (a predecessor of the symptom is a candidate cause of it), bounded to
// `max_hops`, accumulating for each reached ancestor a **responsibility score**:
//
//   `responsibility(ancestor) = anomaly_score(ancestor) * path_weight * decay^hops`
//
// where `path_weight` is the product of the edge weights along the best
// (hop-levelized) path back to `ancestor` (edge weight ∈ `[0,1]` — "how strongly
// does this dependency propagate a fault" — reused as-is, no fabricated units)
// and `decay` (∈ `(0,1]`, mirroring PageRank's damping factor) discounts more
// distant ancestors, since a fault's evidence weakens with propagation distance.
// The ancestor with the highest responsibility is the ROOT CAUSE candidate.
//
// Honest scope: this is a GREEDY, hop-levelized propagation (an ancestor's best
// path is fixed the first hop level at which it is reached), not a global search
// over every possible path — the same complexity trade-off `anomaly`'s Isolation
// Forest and `cluster`'s DBSCAN make elsewhere in this crate (approximate,
// bounded, deterministic, documented).

/// One candidate ancestor and its responsibility score.
#[derive(Debug, Clone, PartialEq)]
pub struct RootCauseCandidate {
    pub node: usize,
    pub score: f64,
    pub hops: usize,
}

/// The full ranked candidate list for one symptom (CONCEPT:EG-KG.mining.root-cause).
#[derive(Debug, Clone, PartialEq)]
pub struct RootCauseResult {
    pub symptom: usize,
    /// Sorted by descending `score` (ties broken by ascending node index);
    /// excludes the symptom itself.
    pub candidates: Vec<RootCauseCandidate>,
}

impl RootCauseResult {
    /// The single most-likely root cause, if any ancestor was reached.
    pub fn best(&self) -> Option<&RootCauseCandidate> {
        self.candidates.first()
    }
}

/// Find the most-likely upstream root cause of `symptom` (CONCEPT:EG-KG.mining.root-cause).
///
/// `edges` is `(cause, effect, weight)` — `weight` is clamped to `[0,1]`.
/// `anomaly_scores[i]` is node `i`'s own anomaly score (negative values clamp to
/// `0.0` — "not anomalous"). `decay` is clamped to `(0,1]` (values `<= 0` fall
/// back to `1.0`, no decay).
pub fn find_root_cause(
    n: usize,
    edges: &[(usize, usize, f64)],
    anomaly_scores: &[f64],
    symptom: usize,
    max_hops: usize,
    decay: f64,
) -> RootCauseResult {
    let decay = if decay > 0.0 { decay.min(1.0) } else { 1.0 };
    if symptom >= n {
        return RootCauseResult {
            symptom,
            candidates: Vec::new(),
        };
    }

    let mut best_path_weight = vec![0.0f64; n];
    let mut best_hops = vec![usize::MAX; n];
    best_path_weight[symptom] = 1.0;
    best_hops[symptom] = 0;

    for hop in 1..=max_hops.max(1).min(n.max(1)) {
        let mut updated = false;
        for &(cause, effect, w) in edges {
            if cause >= n || effect >= n {
                continue;
            }
            if best_hops[effect] != hop - 1 {
                continue; // only extend the exact previous frontier (levelized BFS)
            }
            let w = w.clamp(0.0, 1.0);
            let cand = best_path_weight[effect] * w;
            if best_hops[cause] == usize::MAX || cand > best_path_weight[cause] {
                best_path_weight[cause] = cand;
                best_hops[cause] = hop;
                updated = true;
            }
        }
        if !updated {
            break;
        }
    }

    let mut candidates: Vec<RootCauseCandidate> = (0..n)
        .filter(|&i| i != symptom && best_hops[i] != usize::MAX)
        .map(|i| {
            let decay_factor = decay.powi(best_hops[i] as i32);
            let score = anomaly_scores.get(i).copied().unwrap_or(0.0).max(0.0)
                * best_path_weight[i]
                * decay_factor;
            RootCauseCandidate {
                node: i,
                score,
                hops: best_hops[i],
            }
        })
        .collect();
    candidates.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.node.cmp(&b.node))
    });
    RootCauseResult { symptom, candidates }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_direct_upstream_cause() {
        // 0 -> 1 -> 2 (symptom); node 0 is highly anomalous, node 1 is not.
        let edges = vec![(0, 1, 1.0), (1, 2, 1.0)];
        let scores = vec![5.0, 0.1, 0.2]; // symptom's own score irrelevant to search
        let out = find_root_cause(3, &edges, &scores, 2, 5, 0.9);
        let best = out.best().expect("a root cause is found");
        assert_eq!(best.node, 0, "node 0's high anomaly score should win despite 2 hops");
    }

    #[test]
    fn decay_favors_closer_ancestors_when_scores_tie() {
        // Two candidates with the SAME anomaly score at different depths:
        // 0 -> 1 -> 3 (symptom, node 0 is 2 hops away) and 2 -> 3 (node 2 is 1 hop away).
        let edges = vec![(0, 1, 1.0), (1, 3, 1.0), (2, 3, 1.0)];
        let scores = vec![1.0, 0.0, 1.0, 0.0];
        let out = find_root_cause(4, &edges, &scores, 3, 5, 0.5);
        // node 2 (1 hop, decay^1=0.5) beats node 0 (2 hops, decay^2=0.25) at equal raw score.
        assert_eq!(out.best().unwrap().node, 2);
    }

    #[test]
    fn max_hops_bounds_the_search() {
        let edges = vec![(0, 1, 1.0), (1, 2, 1.0), (2, 3, 1.0)];
        let scores = vec![9.0, 0.0, 0.0, 0.0];
        let out = find_root_cause(4, &edges, &scores, 3, 1, 1.0); // only 1 hop allowed
        assert!(
            out.candidates.iter().all(|c| c.node != 0),
            "node 0 is 3 hops away, must be excluded by max_hops=1"
        );
    }

    #[test]
    fn no_ancestors_yields_empty_candidates() {
        let out = find_root_cause(2, &[], &[1.0, 1.0], 0, 3, 0.9);
        assert!(out.candidates.is_empty());
    }
}
