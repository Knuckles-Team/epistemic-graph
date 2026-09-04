use super::super::graph::AdjacencyGraph;
use super::{
    cosine_from_neighbors, jaccard_from_neighbors, neighbor_norm, neighbor_vec, prepared_neighbors,
    Direction, Metric, SimilarityPair,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;

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
    finish_similarity_pairs(graph, out)
}

/// Score node `a` against every other node under `metric`, keep only scores
/// `> cutoff`, and truncate to the top-`k` (descending score, ascending id).
/// Pure extraction from [`knn_similarity`]'s per-node loop body -- identical
/// scoring/filter/truncate/sort logic, no behaviour change; pulled out solely
/// to keep [`knn_similarity`]'s own cyclomatic complexity within the repo's
/// gate cap. CONCEPT:EG-KG.compute.node-similarity
fn top_k_for_node(
    a: usize,
    n: usize,
    k: usize,
    cutoff: f64,
    metric: Metric,
    neighbors: &[Cow<'_, [(usize, f64)]>],
    norms: &[f64],
) -> Vec<(usize, f64)> {
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
    scored
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
        let scored = top_k_for_node(a, n, k, cutoff, metric, &neighbors, &norms);
        for (b, s) in scored {
            let key = if a < b { (a, b) } else { (b, a) };
            let e = pair_best.entry(key).or_insert(f64::MIN);
            if s > *e {
                *e = s;
            }
        }
    }
    finish_similarity_pairs(
        graph,
        pair_best.into_iter().map(|((a, b), score)| (a, b, score)),
    )
}

pub(super) fn finish_similarity_pairs<N>(
    graph: &AdjacencyGraph<N>,
    pairs: impl IntoIterator<Item = (usize, usize, f64)>,
) -> Vec<SimilarityPair<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let mut out: Vec<(usize, usize, f64)> = pairs.into_iter().collect();
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
