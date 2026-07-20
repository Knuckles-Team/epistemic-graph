// CONCEPT:EG-KG.compute.node-similarity — Node similarity: Jaccard + cosine over neighbour sets.
// Neo4j GDS `gds.nodeSimilarity` parity.

use super::graph::AdjacencyGraph;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;

/// A similarity edge between two nodes. CONCEPT:EG-KG.compute.node-similarity
#[derive(Debug, Clone)]
pub struct SimilarityPair<N> {
    /// First node (the smaller-id endpoint).
    pub a: N,
    /// Second node.
    pub b: N,
    /// Similarity score in `[0, 1]`.
    pub score: f64,
}

/// Which relationship set forms each node's "neighbour" vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Outgoing neighbours (GDS default).
    Out,
    /// Incoming neighbours.
    In,
    /// Union of both directions (undirected view).
    Undirected,
}

/// Sorted `(neighbor, weight)` list for a node under the chosen direction; the
/// undirected view sums both directions' weights.
fn neighbor_vec<N>(graph: &AdjacencyGraph<N>, i: usize, dir: Direction) -> Vec<(usize, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    match dir {
        Direction::Out => graph.out_edges(i).to_vec(),
        Direction::In => graph.in_edges(i).to_vec(),
        Direction::Undirected => {
            let mut m: HashMap<usize, f64> = HashMap::new();
            for &(t, w) in graph.out_edges(i) {
                *m.entry(t).or_insert(0.0) += w;
            }
            for &(s, w) in graph.in_edges(i) {
                *m.entry(s).or_insert(0.0) += w;
            }
            let mut v: Vec<(usize, f64)> = m.into_iter().collect();
            v.sort_unstable_by_key(|(k, _)| *k);
            v
        }
    }
}

/// Prepare each node's neighbor vector once for an all-pairs query. Directed
/// views borrow the graph's already-sorted adjacency without copying; only the
/// synthesized undirected view owns merged rows.
fn prepared_neighbors<N>(graph: &AdjacencyGraph<N>, dir: Direction) -> Vec<Cow<'_, [(usize, f64)]>>
where
    N: Clone + Eq + Hash + Ord,
{
    (0..graph.node_count())
        .map(|index| match dir {
            Direction::Out => Cow::Borrowed(graph.out_edges(index)),
            Direction::In => Cow::Borrowed(graph.in_edges(index)),
            Direction::Undirected => Cow::Owned(neighbor_vec(graph, index, dir)),
        })
        .collect()
}

fn neighbor_intersection(a: &[(usize, f64)], b: &[(usize, f64)]) -> usize {
    let (mut i, mut j, mut intersection) = (0, 0, 0);
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                intersection += 1;
                i += 1;
                j += 1;
            }
        }
    }
    intersection
}

fn jaccard_from_neighbors(a: &[(usize, f64)], b: &[(usize, f64)]) -> f64 {
    let intersection = neighbor_intersection(a, b);
    let union = a.len() + b.len() - intersection;
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn neighbor_norm(neighbors: &[(usize, f64)]) -> f64 {
    neighbors
        .iter()
        .map(|(_, weight)| weight * weight)
        .sum::<f64>()
        .sqrt()
}

fn cosine_from_neighbors(a: &[(usize, f64)], b: &[(usize, f64)], norm_a: f64, norm_b: f64) -> f64 {
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    let (mut i, mut j) = (0, 0);
    let mut dot = 0.0;
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                dot += a[i].1 * b[j].1;
                i += 1;
                j += 1;
            }
        }
    }
    dot / (norm_a * norm_b)
}

/// **Jaccard** similarity of two nodes' neighbour *sets* (weights ignored):
/// `|N(a) ∩ N(b)| / |N(a) ∪ N(b)|`. Two nodes with no neighbours score 0.
///
/// Complexity: `O(deg(a) + deg(b))`. CONCEPT:EG-KG.compute.node-similarity
pub fn jaccard_similarity<N>(graph: &AdjacencyGraph<N>, a: usize, b: usize, dir: Direction) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    let na = neighbor_vec(graph, a, dir);
    let nb = neighbor_vec(graph, b, dir);
    jaccard_from_neighbors(&na, &nb)
}

/// **Cosine** similarity of two nodes' weighted neighbour *vectors*:
/// `(a · b) / (‖a‖ ‖b‖)` over the shared neighbour space. For unit weights this
/// reduces to `|N(a) ∩ N(b)| / √(|N(a)|·|N(b)|)`.
///
/// Complexity: `O(deg(a) + deg(b))`. CONCEPT:EG-KG.compute.node-similarity
pub fn cosine_similarity<N>(graph: &AdjacencyGraph<N>, a: usize, b: usize, dir: Direction) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    let va = neighbor_vec(graph, a, dir);
    let vb = neighbor_vec(graph, b, dir);
    cosine_from_neighbors(&va, &vb, neighbor_norm(&va), neighbor_norm(&vb))
}

/// Which metric an all-pairs sweep uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Set-based Jaccard.
    Jaccard,
    /// Weighted cosine.
    Cosine,
}

fn neighbor_score_cmp(left: &(usize, f64), right: &(usize, f64)) -> std::cmp::Ordering {
    right
        .1
        .partial_cmp(&left.1)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left.0.cmp(&right.0))
}

/// All-pairs node similarity above a cutoff. Returns each unordered pair
/// `(a < b)` whose score `> cutoff`, sorted by descending score then ascending
/// node ids (deterministic).
///
/// Complexity: `O(V² · d̄)` naïvely. CONCEPT:EG-KG.compute.node-similarity
pub fn all_pairs_similarity<N>(
    graph: &AdjacencyGraph<N>,
    metric: Metric,
    dir: Direction,
    cutoff: f64,
) -> Vec<SimilarityPair<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let neighbors = prepared_neighbors(graph, dir);
    let norms: Vec<f64> = if metric == Metric::Cosine {
        neighbors.iter().map(|row| neighbor_norm(row)).collect()
    } else {
        Vec::new()
    };
    let mut out: Vec<(usize, usize, f64)> = Vec::new();
    for a in 0..n {
        for b in (a + 1)..n {
            let s = match metric {
                Metric::Jaccard => jaccard_from_neighbors(&neighbors[a], &neighbors[b]),
                Metric::Cosine => {
                    cosine_from_neighbors(&neighbors[a], &neighbors[b], norms[a], norms[b])
                }
            };
            if s > cutoff {
                out.push((a, b, s));
            }
        }
    }
    // Descending score, then ascending (a, b) for stable ordering.
    out.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.0.cmp(&y.0))
            .then_with(|| x.1.cmp(&y.1))
    });
    out.into_iter()
        .map(|(a, b, score)| SimilarityPair {
            a: graph.node_at(a).clone(),
            b: graph.node_at(b).clone(),
            score,
        })
        .collect()
}

/// Per-node top-`k` nearest-neighbour similarity edges (CONCEPT:EG-KG.compute.node-similarity),
/// `gds.knn` parity. Distinct from [`all_pairs_similarity`]'s GLOBAL cutoff sweep
/// (`gds.nodeSimilarity`): each node independently keeps its `top_k` best-scoring
/// OTHER nodes (score `> cutoff`), then the directed per-node results are folded
/// into undirected pairs (keeping the max of the two directional scores). This
/// engine computes the exact top-`k` via a full sweep rather than Neo4j's
/// approximate KNN-descent sampling — exact and deterministic, at `O(V²·d̄)`
/// instead of KNN-descent's sub-quadratic approximate cost; fine at the node
/// counts this engine targets.
///
/// Complexity: `O(V² · d̄)`. Returns pairs sorted by descending score then
/// ascending ids.
pub fn knn_similarity<N>(
    graph: &AdjacencyGraph<N>,
    metric: Metric,
    dir: Direction,
    top_k: usize,
    cutoff: f64,
) -> Vec<SimilarityPair<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let k = top_k.max(1);
    let neighbors = prepared_neighbors(graph, dir);
    let norms: Vec<f64> = if metric == Metric::Cosine {
        neighbors.iter().map(|row| neighbor_norm(row)).collect()
    } else {
        Vec::new()
    };
    let mut pair_best: HashMap<(usize, usize), f64> = HashMap::new();
    for a in 0..n {
        let mut scored: Vec<(usize, f64)> = (0..n)
            .filter(|&b| b != a)
            .map(|b| {
                let s = match metric {
                    Metric::Jaccard => jaccard_from_neighbors(&neighbors[a], &neighbors[b]),
                    Metric::Cosine => {
                        cosine_from_neighbors(&neighbors[a], &neighbors[b], norms[a], norms[b])
                    }
                };
                (b, s)
            })
            .filter(|&(_, s)| s > cutoff)
            .collect();
        if scored.len() > k {
            scored.select_nth_unstable_by(k, neighbor_score_cmp);
            scored.truncate(k);
        }
        // The public result is ordered, but the discarded V-k neighbors are not:
        // only sort the exact selected prefix under the established score/id order.
        scored.sort_by(neighbor_score_cmp);
        for (b, s) in scored {
            let key = if a < b { (a, b) } else { (b, a) };
            let e = pair_best.entry(key).or_insert(f64::MIN);
            if s > *e {
                *e = s;
            }
        }
    }
    let mut out: Vec<(usize, usize, f64)> =
        pair_best.into_iter().map(|((a, b), s)| (a, b, s)).collect();
    out.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.0.cmp(&y.0))
            .then_with(|| x.1.cmp(&y.1))
    });
    out.into_iter()
        .map(|(a, b, score)| SimilarityPair {
            a: graph.node_at(a).clone(),
            b: graph.node_at(b).clone(),
            score,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_jaccard_overlapping_neighbors() {
        // a→{x,y,z}, b→{y,z,w}. Intersection {y,z}=2, union {w,x,y,z}=4 ⇒ 0.5.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("a", "z", 1.0),
            ("b", "y", 1.0),
            ("b", "z", 1.0),
            ("b", "w", 1.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((jaccard_similarity(&g, a, b, Direction::Out) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn eg144_jaccard_identical_and_disjoint() {
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "p", 1.0),
            ("c", "q", 1.0),
        ]);
        let (a, b, c) = (
            g.index_of(&"a").unwrap(),
            g.index_of(&"b").unwrap(),
            g.index_of(&"c").unwrap(),
        );
        assert!((jaccard_similarity(&g, a, b, Direction::Out) - 1.0).abs() < 1e-9);
        assert!((jaccard_similarity(&g, a, c, Direction::Out) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn eg144_cosine_unit_weights_matches_formula() {
        // a→{x,y,z}, b→{y,z,w}: inter=2, |a|=|b|=3 ⇒ 2/√(3·3)=2/3.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("a", "z", 1.0),
            ("b", "y", 1.0),
            ("b", "z", 1.0),
            ("b", "w", 1.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((cosine_similarity(&g, a, b, Direction::Out) - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn eg144_cosine_respects_weights() {
        // Same target, proportional weight vectors ⇒ cosine 1.0.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 2.0),
            ("b", "x", 2.0),
            ("b", "y", 4.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((cosine_similarity(&g, a, b, Direction::Out) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn prepared_pair_scores_match_pointwise_semantics() {
        let g = AdjacencyGraph::from_edges([
            ("a", "a", 0.5),
            ("a", "b", 1.0),
            ("b", "a", 2.0),
            ("b", "c", 3.0),
            ("c", "a", 4.0),
        ]);
        for direction in [Direction::Out, Direction::In, Direction::Undirected] {
            let prepared = prepared_neighbors(&g, direction);
            let norms: Vec<f64> = prepared.iter().map(|row| neighbor_norm(row)).collect();
            for a in 0..g.node_count() {
                for b in 0..g.node_count() {
                    assert_eq!(
                        jaccard_from_neighbors(&prepared[a], &prepared[b]),
                        jaccard_similarity(&g, a, b, direction)
                    );
                    let prepared_cosine =
                        cosine_from_neighbors(&prepared[a], &prepared[b], norms[a], norms[b]);
                    let pointwise_cosine = cosine_similarity(&g, a, b, direction);
                    assert!((prepared_cosine - pointwise_cosine).abs() < 1e-12);
                }
            }
        }
    }

    #[test]
    fn knn_similarity_keeps_top_k_per_node() {
        // a/b share {x,y} (jaccard 1.0); c only shares {x} with each (jaccard 0.5).
        // With top_k=1: a's + b's mutual best pick is each other (1.0); c's best
        // pick is a (0.5, ascending-id tie-break over the equally-scored b) — but
        // NOT b's pick (b's own top-1 is a, at a strictly higher score than c),
        // so (b, c) never appears.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "x", 1.0),
        ]);
        let pairs = knn_similarity(&g, Metric::Jaccard, Direction::Out, 1, 0.0);
        assert!(pairs
            .iter()
            .any(|p| p.a == "a" && p.b == "b" && (p.score - 1.0).abs() < 1e-9));
        assert!(pairs
            .iter()
            .any(|p| p.a == "a" && p.b == "c" && (p.score - 0.5).abs() < 1e-9));
        assert!(!pairs
            .iter()
            .any(|p| (p.a == "b" && p.b == "c") || (p.a == "c" && p.b == "b")));
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn knn_similarity_empty_graph_is_empty() {
        let g: AdjacencyGraph<String> =
            AdjacencyGraph::from_adjacency(Vec::<(String, Vec<(String, f64)>)>::new());
        assert!(knn_similarity(&g, Metric::Jaccard, Direction::Out, 5, 0.0).is_empty());
    }

    #[test]
    fn eg144_all_pairs_similarity_ranked() {
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "y", 1.0),
        ]);
        let pairs = all_pairs_similarity(&g, Metric::Jaccard, Direction::Out, 0.0);
        // a & b share {x,y} ⇒ top pair with score 1.0.
        assert_eq!(pairs[0].a, "a");
        assert_eq!(pairs[0].b, "b");
        assert!((pairs[0].score - 1.0).abs() < 1e-9);
    }
}
