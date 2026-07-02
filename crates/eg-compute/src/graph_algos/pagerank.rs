// CONCEPT:EG-144 — PageRank via power iteration (Neo4j GDS `gds.pageRank` parity).

use super::graph::AdjacencyGraph;
use std::hash::Hash;

/// Configuration for [`pagerank`]. Damping 0.85 and tolerance 1e-7 mirror Neo4j
/// GDS; the iteration cap is raised to 100 so the L1 tolerance is actually
/// reached on typical graphs (with damping 0.85 the error decays ~0.85ᵏ, so 20
/// iterations only reaches ~4e-2). CONCEPT:EG-144
#[derive(Debug, Clone, Copy)]
pub struct PageRankConfig {
    /// Damping factor `d` (probability of following an edge vs teleporting).
    pub damping: f64,
    /// L1 convergence tolerance: stop when `Σ|rank_new − rank_old| ≤ tol`.
    pub tolerance: f64,
    /// Hard iteration cap.
    pub max_iterations: usize,
}

impl Default for PageRankConfig {
    fn default() -> Self {
        Self {
            damping: 0.85,
            tolerance: 1e-7,
            max_iterations: 100,
        }
    }
}

/// Result of a PageRank run. CONCEPT:EG-144
#[derive(Debug, Clone)]
pub struct PageRankResult<N> {
    /// `(node, rank)` pairs in node (sorted-id) order; ranks sum to ~1.0.
    pub scores: Vec<(N, f64)>,
    /// Iterations actually performed.
    pub iterations: usize,
    /// Whether the L1 tolerance was reached before the cap.
    pub converged: bool,
}

/// PageRank via power iteration over a directed, weighted graph.
///
/// Weighted contribution: a node distributes its rank to out-neighbours in
/// proportion to edge weight (falling back to uniform when unweighted). Dangling
/// nodes (no out-edges) redistribute their whole rank uniformly across all nodes,
/// so the total rank mass is conserved at 1.0 every iteration.
///
/// Deterministic: no RNG; iteration order is the graph's sorted index order.
///
/// Complexity: `O(k · (V + E))` for `k` iterations. CONCEPT:EG-144
pub fn pagerank<N>(graph: &AdjacencyGraph<N>, config: &PageRankConfig) -> PageRankResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 {
        return PageRankResult {
            scores: Vec::new(),
            iterations: 0,
            converged: true,
        };
    }

    let d = config.damping;
    let base = (1.0 - d) / n as f64;
    let inv_n = 1.0 / n as f64;

    // Pre-compute weighted out-degrees.
    let out_weight: Vec<f64> = (0..n).map(|i| graph.weighted_out_degree(i)).collect();

    let mut rank = vec![inv_n; n];
    let mut next = vec![0.0f64; n];
    let mut iterations = 0;
    let mut converged = false;

    while iterations < config.max_iterations {
        iterations += 1;

        // Dangling mass: nodes with no outgoing edges spill their rank uniformly.
        let dangling: f64 = (0..n)
            .filter(|&i| out_weight[i] <= 0.0)
            .map(|i| rank[i])
            .sum();
        let dangling_share = d * dangling * inv_n;

        for slot in next.iter_mut() {
            *slot = base + dangling_share;
        }
        for u in 0..n {
            let ow = out_weight[u];
            if ow <= 0.0 {
                continue;
            }
            let ru = rank[u];
            for &(v, w) in graph.out_edges(u) {
                next[v] += d * ru * (w / ow);
            }
        }

        let delta: f64 = (0..n).map(|i| (next[i] - rank[i]).abs()).sum();
        std::mem::swap(&mut rank, &mut next);

        if delta <= config.tolerance {
            converged = true;
            break;
        }
    }

    PageRankResult {
        scores: graph.label_scores(&rank),
        iterations,
        converged,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rank on a known 4-node directed graph. Classic example: 1→2, 1→3, 2→3,
    /// 3→1, 4→3 (node ids a,b,c,d ≡ 1,2,3,4). Node c (3) is the strongest sink;
    /// d (4) — only outgoing, only teleport in — is weakest.
    #[test]
    fn eg144_pagerank_ranks_known_four_node_graph() {
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("a", "c", 1.0),
            ("b", "c", 1.0),
            ("c", "a", 1.0),
            ("d", "c", 1.0),
        ]);
        let res = pagerank(&g, &PageRankConfig::default());
        let m: std::collections::HashMap<&str, f64> =
            res.scores.iter().map(|(k, v)| (*k, *v)).collect();

        // Mass conservation.
        let total: f64 = res.scores.iter().map(|(_, v)| v).sum();
        assert!(
            (total - 1.0).abs() < 1e-6,
            "ranks must sum to 1, got {total}"
        );

        // c is the highest-ranked (everyone points at it).
        assert!(m["c"] > m["a"]);
        assert!(m["a"] > m["b"]);
        // d only teleports in → lowest.
        assert!(m["d"] < m["b"]);
        assert!(res.converged);
    }

    #[test]
    fn eg144_pagerank_symmetric_ring_is_uniform() {
        // A ↔-style directed ring a→b→c→a is fully symmetric ⇒ equal ranks.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("c", "a", 1.0)]);
        let res = pagerank(&g, &PageRankConfig::default());
        let vals: Vec<f64> = res.scores.iter().map(|(_, v)| *v).collect();
        for w in vals.windows(2) {
            assert!((w[0] - w[1]).abs() < 1e-6);
        }
    }

    #[test]
    fn eg144_pagerank_dangling_conserves_mass() {
        // b is a dangling sink (no out-edges): mass must not leak.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
        let res = pagerank(&g, &PageRankConfig::default());
        let total: f64 = res.scores.iter().map(|(_, v)| v).sum();
        assert!((total - 1.0).abs() < 1e-6, "got {total}");
    }

    #[test]
    fn eg144_pagerank_empty_graph() {
        let g: AdjacencyGraph<&str> =
            AdjacencyGraph::from_adjacency(Vec::<(&str, Vec<(&str, f64)>)>::new());
        let res = pagerank(&g, &PageRankConfig::default());
        assert!(res.scores.is_empty());
        assert!(res.converged);
    }
}
