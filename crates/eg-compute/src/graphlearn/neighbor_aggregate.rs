// CONCEPT:EG-KG.graphlearn.neighbor-aggregation — 1-hop message-passing (lite).
//
// The cheapest "graph learning" ingredient: refine each node's feature vector by
// mixing in the mean of its 1-hop neighbours' vectors — the minimum-viable
// message-passing step a KAN edge function needs before it has anything
// graph-structural to consume beyond raw pairwise similarity (see the synergy
// report §3-3):
//
//     x'_v = α · x_v + (1 − α) · mean( x_u : u ∈ N(v) )
//
// Pure vector arithmetic, `O(deg)` per node, deterministic. Isolated nodes (no
// neighbours) keep their own vector. Optionally the neighbour weight is itself a
// learnable `KanEdgeFn` of the edge feature (the "the edge IS the computation"
// idea) — exposed via [`aggregate_1hop_weighted`].

use crate::graph_algos::AdjacencyGraph;
use std::hash::Hash;

use super::edge_fn::KanEdgeFn;

/// Refine per-node feature rows by one round of mean neighbour aggregation.
///
/// `feats[i]` is the feature vector of node index `i` (must be parallel to the
/// graph's compact index order and all the same width). Returns refined rows in
/// the same order. `alpha ∈ [0, 1]` is the self-retention weight (`1.0` ⇒ identity,
/// `0.0` ⇒ pure neighbour mean). CONCEPT:EG-KG.graphlearn.neighbor-aggregation
pub fn aggregate_1hop<N>(graph: &AdjacencyGraph<N>, feats: &[Vec<f64>], alpha: f64) -> Vec<Vec<f64>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = feats.len();
    if n == 0 {
        return Vec::new();
    }
    let dim = feats[0].len();
    let mut out = Vec::with_capacity(n);
    for v in 0..n {
        let neighbors = graph.undirected_neighbors(v);
        if neighbors.is_empty() {
            out.push(feats[v].clone());
            continue;
        }
        let mut mean = vec![0.0; dim];
        for &u in &neighbors {
            for (m, f) in mean.iter_mut().zip(feats[u].iter()) {
                *m += *f;
            }
        }
        let inv = 1.0 / neighbors.len() as f64;
        let mut row = vec![0.0; dim];
        for k in 0..dim {
            row[k] = alpha * feats[v][k] + (1.0 - alpha) * (mean[k] * inv);
        }
        out.push(row);
    }
    out
}

/// Refine per-node feature rows where each neighbour's contribution is weighted by
/// a learnable `KanEdgeFn` of the edge weight (instead of a uniform mean). This is
/// the concrete "the edge weight IS a learned univariate function of resident edge
/// features" primitive from the synergy report §3-3. Weights are softmax-free simple
/// normalisation of `max(edge_fn(w), 0)`; a node with no positive-weighted neighbour
/// falls back to its own vector. CONCEPT:EG-KG.graphlearn.neighbor-aggregation
pub fn aggregate_1hop_weighted<N>(
    graph: &AdjacencyGraph<N>,
    feats: &[Vec<f64>],
    edge_fn: &KanEdgeFn,
    alpha: f64,
) -> Vec<Vec<f64>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = feats.len();
    if n == 0 {
        return Vec::new();
    }
    let dim = feats[0].len();
    let mut out = Vec::with_capacity(n);
    for v in 0..n {
        // Collect undirected neighbour weights (sum both directions).
        let mut weighted: Vec<(usize, f64)> = Vec::new();
        for &(t, w) in graph.out_edges(v) {
            if t != v {
                weighted.push((t, w));
            }
        }
        for &(s, w) in graph.in_edges(v) {
            if s != v {
                weighted.push((s, w));
            }
        }
        if weighted.is_empty() {
            out.push(feats[v].clone());
            continue;
        }
        let mut agg = vec![0.0; dim];
        let mut total = 0.0;
        for &(u, w) in &weighted {
            let gate = edge_fn.eval(w).max(0.0);
            if gate <= 0.0 {
                continue;
            }
            total += gate;
            for (a, f) in agg.iter_mut().zip(feats[u].iter()) {
                *a += gate * *f;
            }
        }
        if total <= 0.0 {
            out.push(feats[v].clone());
            continue;
        }
        let inv = 1.0 / total;
        let mut row = vec![0.0; dim];
        for k in 0..dim {
            row[k] = alpha * feats[v][k] + (1.0 - alpha) * (agg[k] * inv);
        }
        out.push(row);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_when_alpha_one() {
        let g = AdjacencyGraph::from_unweighted_edges([(0u32, 1u32), (1, 2)]);
        let feats = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![2.0, 2.0]];
        let out = aggregate_1hop(&g, &feats, 1.0);
        assert_eq!(out, feats);
    }

    #[test]
    fn pure_mean_when_alpha_zero() {
        // Path 0-1-2 (undirected). Node 1's neighbours are 0 and 2.
        let g = AdjacencyGraph::from_unweighted_edges([(0u32, 1u32), (1, 2)]);
        let feats = vec![vec![1.0], vec![0.0], vec![3.0]];
        let out = aggregate_1hop(&g, &feats, 0.0);
        // node 1 ⇒ mean(1.0, 3.0) = 2.0
        assert!((out[1][0] - 2.0).abs() < 1e-12);
        // node 0's only neighbour is 1 ⇒ 0.0
        assert!((out[0][0] - 0.0).abs() < 1e-12);
    }

    #[test]
    fn isolated_node_keeps_own_vector() {
        let g = AdjacencyGraph::from_adjacency([
            (0u32, vec![(1u32, 1.0)]),
            (1, vec![(0, 1.0)]),
            (2, vec![]), // isolated
        ]);
        let feats = vec![vec![1.0], vec![2.0], vec![9.0]];
        let out = aggregate_1hop(&g, &feats, 0.5);
        assert!((out[2][0] - 9.0).abs() < 1e-12);
    }

    #[test]
    fn weighted_aggregation_gates_by_edge_fn() {
        let g = AdjacencyGraph::from_edges([(0u32, 1u32, 1.0), (0, 2, 5.0)]);
        let feats = vec![vec![0.0], vec![1.0], vec![10.0]];
        // A linear-ish edge fn (T_1 ≈ tanh(w)) up-weights the heavier edge (to node 2).
        let ef = KanEdgeFn::chebyshev(vec![0.5, 0.5]);
        let out = aggregate_1hop_weighted(&g, &feats, &ef, 0.0);
        // node 0 pulled toward node 2 (heavier gate) ⇒ > mean(1,10)=5.5.
        assert!(out[0][0] > 5.5);
    }
}
