use super::super::graph::AdjacencyGraph;
use super::types::Direction;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;

/// Sorted `(neighbor, weight)` list for a node under the chosen direction; the
/// undirected view sums both directions' weights.
pub(super) fn neighbor_vec<N>(
    graph: &AdjacencyGraph<N>,
    i: usize,
    dir: Direction,
) -> Vec<(usize, f64)>
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
pub(super) fn prepared_neighbors<N>(
    graph: &AdjacencyGraph<N>,
    dir: Direction,
) -> Vec<Cow<'_, [(usize, f64)]>>
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

pub(super) fn neighbor_intersection(a: &[(usize, f64)], b: &[(usize, f64)]) -> usize {
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

pub(super) fn jaccard_from_neighbors(a: &[(usize, f64)], b: &[(usize, f64)]) -> f64 {
    let intersection = neighbor_intersection(a, b);
    let union = a.len() + b.len() - intersection;
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

pub(super) fn neighbor_norm(neighbors: &[(usize, f64)]) -> f64 {
    neighbors
        .iter()
        .map(|(_, weight)| weight * weight)
        .sum::<f64>()
        .sqrt()
}

pub(super) fn cosine_from_neighbors(
    a: &[(usize, f64)],
    b: &[(usize, f64)],
    norm_a: f64,
    norm_b: f64,
) -> f64 {
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
