// CONCEPT:EG-144 — Centrality: degree centrality + betweenness (Brandes'
// algorithm). Neo4j GDS `gds.degree` / `gds.betweenness` parity.

use super::graph::AdjacencyGraph;
use std::collections::VecDeque;
use std::hash::Hash;

/// Which degree to score. CONCEPT:EG-144
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegreeKind {
    /// In-degree (number of incoming edges).
    In,
    /// Out-degree (number of outgoing edges).
    Out,
    /// In + out degree.
    Total,
}

/// Degree centrality: the chosen degree normalised by `(n − 1)` (so a node
/// adjacent to every other scores 1.0). Returns `(node, score)` in node order.
///
/// Complexity: `O(V + E)`. CONCEPT:EG-144
pub fn degree_centrality<N>(graph: &AdjacencyGraph<N>, kind: DegreeKind) -> Vec<(N, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let denom = if n > 1 { (n - 1) as f64 } else { 1.0 };
    let scores: Vec<f64> = (0..n)
        .map(|i| {
            let d = match kind {
                DegreeKind::In => graph.in_degree(i),
                DegreeKind::Out => graph.out_degree(i),
                DegreeKind::Total => graph.in_degree(i) + graph.out_degree(i),
            };
            d as f64 / denom
        })
        .collect();
    graph.label_scores(&scores)
}

/// Betweenness centrality via **Brandes' algorithm** (unweighted, BFS-based).
///
/// For each source, a BFS builds the shortest-path DAG (counting path
/// multiplicities `σ`), then dependencies are accumulated back-to-front. When
/// `directed` is false the graph is symmetrised and the raw scores are halved
/// (each unordered pair is otherwise counted twice).
///
/// Deterministic: neighbour iteration follows sorted index order. Returns
/// `(node, score)` in node order.
///
/// Complexity: `O(V · E)` time, `O(V + E)` space. CONCEPT:EG-144
pub fn betweenness_centrality<N>(graph: &AdjacencyGraph<N>, directed: bool) -> Vec<(N, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let mut bc = vec![0.0f64; n];

    // Neighbour list closure (directed out-edges, or undirected union).
    let neighbors = |v: usize| -> Vec<usize> {
        if directed {
            graph.out_edges(v).iter().map(|(t, _)| *t).collect()
        } else {
            graph.undirected_neighbors(v)
        }
    };

    for s in 0..n {
        let mut stack: Vec<usize> = Vec::new();
        let mut pred: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut sigma = vec![0.0f64; n];
        let mut dist = vec![-1i64; n];
        sigma[s] = 1.0;
        dist[s] = 0;

        let mut queue: VecDeque<usize> = VecDeque::new();
        queue.push_back(s);
        while let Some(v) = queue.pop_front() {
            stack.push(v);
            for w in neighbors(v) {
                if dist[w] < 0 {
                    dist[w] = dist[v] + 1;
                    queue.push_back(w);
                }
                if dist[w] == dist[v] + 1 {
                    sigma[w] += sigma[v];
                    pred[w].push(v);
                }
            }
        }

        let mut delta = vec![0.0f64; n];
        while let Some(w) = stack.pop() {
            for &v in &pred[w] {
                delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
            }
            if w != s {
                bc[w] += delta[w];
            }
        }
    }

    if !directed {
        for b in bc.iter_mut() {
            *b /= 2.0;
        }
    }
    graph.label_scores(&bc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_degree_centrality_normalised() {
        // Undirected-style star: center 'c' out to 3 leaves.
        let g = AdjacencyGraph::from_edges([("c", "l1", 1.0), ("c", "l2", 1.0), ("c", "l3", 1.0)]);
        let out = degree_centrality(&g, DegreeKind::Out);
        let m: std::collections::HashMap<&str, f64> = out.iter().map(|(k, v)| (*k, *v)).collect();
        // n=4 ⇒ denom 3; center has out-degree 3 ⇒ 1.0.
        assert!((m["c"] - 1.0).abs() < 1e-9);
        assert!((m["l1"] - 0.0).abs() < 1e-9);

        let inc = degree_centrality(&g, DegreeKind::In);
        let mi: std::collections::HashMap<&str, f64> = inc.iter().map(|(k, v)| (*k, *v)).collect();
        assert!((mi["l1"] - (1.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn eg144_betweenness_star_center_is_high_leaves_zero() {
        // Undirected star: center on every shortest path between leaf pairs.
        let g =
            AdjacencyGraph::from_unweighted_edges([("c", "a"), ("c", "b"), ("c", "d"), ("c", "e")]);
        let bc = betweenness_centrality(&g, false);
        let m: std::collections::HashMap<&str, f64> = bc.iter().map(|(k, v)| (*k, *v)).collect();
        // 4 leaves ⇒ C(4,2)=6 unordered pairs all routed through center.
        assert!((m["c"] - 6.0).abs() < 1e-9, "center bc={}", m["c"]);
        for leaf in ["a", "b", "d", "e"] {
            assert!((m[leaf] - 0.0).abs() < 1e-9);
        }
    }

    #[test]
    fn eg144_betweenness_path_middle_is_highest() {
        // Undirected path a-b-c-d-e. Middle c should score highest.
        let g =
            AdjacencyGraph::from_unweighted_edges([("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")]);
        let bc = betweenness_centrality(&g, false);
        let m: std::collections::HashMap<&str, f64> = bc.iter().map(|(k, v)| (*k, *v)).collect();
        // Analytic betweenness of an internal node at position i (1-based) in a
        // 5-path: b=3, c=4, d=3, endpoints 0.
        assert!((m["c"] - 4.0).abs() < 1e-9, "c={}", m["c"]);
        assert!((m["b"] - 3.0).abs() < 1e-9, "b={}", m["b"]);
        assert!((m["a"] - 0.0).abs() < 1e-9);
        assert!(m["c"] > m["b"]);
    }
}
