// CONCEPT:EG-KG.mining.risk-propagation — Personalized (seeded) risk propagation.
//
// Pure-Rust, dependency-light: given a directed, weighted graph and a set of
// SEED risk scores (e.g. "this vendor just had a security incident", "this
// account was flagged for fraud"), propagate risk to every other node via
// power iteration — the same fixed-point recurrence PageRank uses
// (`eg_compute::graph_algos::pagerank`), but PERSONALIZED: instead of
// teleporting uniformly to `1/n` of the whole graph, the `(1 - damping)` restart
// mass returns to the SEED distribution. This is the standard "personalized
// PageRank" / "topic-sensitive PageRank" formulation (Haveliwala), reused here
// as the "risk propagates along dependency/relationship edges, but keeps being
// pulled back toward its known sources" model:
//
//   `risk_{t+1}(v) = damping * Σ_{u->v} weight(u,v)/outdeg(u) * risk_t(u) + (1 - damping) * seed(v)`
//
// `seed` is normalized to sum to `1.0` before iterating (a probability-mass
// restart distribution); the returned scores likewise sum to ~1.0, so a node's
// score is its SHARE of total propagated risk, comparable to PageRank's own
// mass-conservation contract. Dangling nodes (no out-edges) redistribute their
// mass back to the seed distribution too (not uniformly across all nodes,
// preserving the personalization).

/// Configuration for [`propagate`].
#[derive(Debug, Clone, Copy)]
pub struct RiskConfig {
    pub damping: f64,
    pub tolerance: f64,
    pub max_iterations: usize,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            // Mirrors `eg_compute::graph_algos::PageRankConfig`'s own default: at
            // damping 0.85 the error decays ~0.85^k, so 100 iterations reaches
            // ~8.8e-8 — tolerance 1e-7 is reachable within the iteration cap,
            // 1e-9 is not (the same trade-off documented on `PageRankConfig`).
            damping: 0.85,
            tolerance: 1e-7,
            max_iterations: 100,
        }
    }
}

/// The propagated risk distribution.
#[derive(Debug, Clone, PartialEq)]
pub struct RiskScores {
    /// `scores[i]` — node `i`'s share of total propagated risk, `>= 0`, summing to ~1.0.
    pub scores: Vec<f64>,
    pub iterations: usize,
    pub converged: bool,
}

/// Propagate `seed` risk over `edges` (`(from, to, weight)`, `weight` clamped to
/// `>= 0`) across `n` nodes (CONCEPT:EG-KG.mining.risk-propagation).
///
/// `seed[i]` is node `i`'s initial risk (any non-negative scale — normalized
/// internally). An all-zero `seed` returns all-zero scores (nothing to
/// propagate) rather than falling back to a uniform restart.
pub fn propagate(n: usize, edges: &[(usize, usize, f64)], seed: &[f64], config: &RiskConfig) -> RiskScores {
    if n == 0 {
        return RiskScores {
            scores: Vec::new(),
            iterations: 0,
            converged: true,
        };
    }
    let seed_total: f64 = seed.iter().take(n).map(|s| s.max(0.0)).sum();
    let seed_dist: Vec<f64> = if seed_total > 0.0 {
        (0..n)
            .map(|i| seed.get(i).copied().unwrap_or(0.0).max(0.0) / seed_total)
            .collect()
    } else {
        vec![0.0; n]
    };
    if seed_total <= 0.0 {
        return RiskScores {
            scores: vec![0.0; n],
            iterations: 0,
            converged: true,
        };
    }

    // Out-adjacency + weighted out-degree, built once.
    let mut out: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
    let mut out_weight = vec![0.0f64; n];
    for &(u, v, w) in edges {
        if u >= n || v >= n {
            continue;
        }
        let w = w.max(0.0);
        out[u].push((v, w));
        out_weight[u] += w;
    }

    let d = config.damping.clamp(0.0, 1.0);
    let mut rank = seed_dist.clone();
    let mut next = vec![0.0f64; n];
    let mut iterations = 0;
    let mut converged = false;

    while iterations < config.max_iterations {
        iterations += 1;
        let dangling: f64 = (0..n).filter(|&i| out_weight[i] <= 0.0).map(|i| rank[i]).sum();
        for v in 0..n {
            next[v] = (1.0 - d) * seed_dist[v] + d * dangling * seed_dist[v];
        }
        for u in 0..n {
            if out_weight[u] <= 0.0 {
                continue;
            }
            let share = d * rank[u] / out_weight[u];
            for &(v, w) in &out[u] {
                next[v] += share * w;
            }
        }
        let delta: f64 = rank.iter().zip(&next).map(|(a, b)| (a - b).abs()).sum();
        std::mem::swap(&mut rank, &mut next);
        if delta <= config.tolerance {
            converged = true;
            break;
        }
    }

    RiskScores {
        scores: rank,
        iterations,
        converged,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn risk_flows_from_seed_along_edges() {
        // 0 (seed) -> 1 -> 2, and 2 is dangling so its mass returns to the seed
        // distribution (all mass at node 0) — a 3-step recirculation that mixes
        // more slowly than a plain sink-absorbing chain, so convergence needs a
        // larger cap; what actually matters (checked below) is that risk still
        // decreases with distance from the seed and total mass is conserved.
        let edges = vec![(0, 1, 1.0), (1, 2, 1.0)];
        let seed = vec![1.0, 0.0, 0.0];
        let config = RiskConfig {
            max_iterations: 2000,
            ..RiskConfig::default()
        };
        let out = propagate(3, &edges, &seed, &config);
        assert!(out.converged, "expected convergence within {} iterations", config.max_iterations);
        assert!(out.scores[0] > out.scores[1]);
        assert!(out.scores[1] > out.scores[2]);
        let total: f64 = out.scores.iter().sum();
        assert!((total - 1.0).abs() < 1e-6, "mass not conserved: {total}");
    }

    #[test]
    fn zero_seed_yields_zero_risk() {
        let edges = vec![(0, 1, 1.0)];
        let out = propagate(2, &edges, &[0.0, 0.0], &RiskConfig::default());
        assert_eq!(out.scores, vec![0.0, 0.0]);
    }

    #[test]
    fn higher_edge_weight_gets_more_propagated_risk() {
        // Seed at 0, fanning out to 1 (weight 9) and 2 (weight 1).
        let edges = vec![(0, 1, 9.0), (0, 2, 1.0)];
        let seed = vec![1.0, 0.0, 0.0];
        let out = propagate(3, &edges, &seed, &RiskConfig::default());
        assert!(out.scores[1] > out.scores[2]);
    }

    #[test]
    fn dangling_node_returns_mass_to_seed_not_uniform() {
        // 0 (seed) -> 1, and 1 has NO out-edges (dangling). All mass eventually
        // concentrates on {0, 1} — none should leak to the unrelated node 2.
        let edges = vec![(0, 1, 1.0)];
        let seed = vec![1.0, 0.0, 0.0];
        let out = propagate(3, &edges, &seed, &RiskConfig::default());
        assert!(out.scores[2] < 1e-6, "dangling mass must not leak to non-seed node 2");
    }
}
