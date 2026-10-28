//! Deterministic weighted global min-cut (Stoer-Wagner).
//!
//! The graph algorithms expose stable node ordering through [`AdjacencyGraph`].
//! This implementation keeps that ordering for every tie break and canonicalizes
//! the two sides of the cut, so a repeated run produces the same community ids.

use super::AdjacencyGraph;
use std::hash::Hash;

/// The two sides and weight of a global undirected cut.
#[derive(Debug, Clone, PartialEq)]
pub struct MincutPartition<N> {
    /// Sum of the weights crossing between `left` and `right`.
    pub cut_weight: f64,
    /// The lexicographically smaller side after node-index canonicalization.
    pub left: Vec<N>,
    /// The other side of the cut.
    pub right: Vec<N>,
}

/// Find a minimum global cut using the deterministic Stoer-Wagner algorithm.
///
/// Directed input is symmetrized with [`AdjacencyGraph::undirected_weighted_adjacency`].
/// Negative and non-finite edge weights are refused because they do not define a
/// valid non-negative cut. Graphs with fewer than two nodes have no partition.
/// Node IDs are sorted by `AdjacencyGraph`, and equal-weight choices retain the
/// lowest index; the returned sides are therefore stable across repeated runs.
///
/// Complexity is `O(V^3)` time and `O(V^2)` memory for the dense phase matrix.
pub fn stoer_wagner<N>(graph: &AdjacencyGraph<N>) -> Option<MincutPartition<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n < 2 {
        return None;
    }

    let adjacency = graph.undirected_weighted_adjacency();
    let mut weights = vec![vec![0.0; n]; n];
    for (u, edges) in adjacency.iter().enumerate() {
        for &(v, weight) in edges {
            if !weight.is_finite() || weight < 0.0 {
                return None;
            }
            if u != v {
                weights[u][v] = weight;
            }
        }
    }

    let mut active = vec![true; n];
    let mut groups: Vec<Vec<usize>> = (0..n).map(|index| vec![index]).collect();
    let mut remaining = n;
    let mut best_weight = f64::INFINITY;
    let mut best_side: Option<Vec<usize>> = None;

    while remaining > 1 {
        let mut added = vec![false; n];
        let mut connection = vec![0.0_f64; n];
        let mut previous = None;
        let mut last = None;

        for _ in 0..remaining {
            let selected = (0..n)
                .filter(|&index| active[index] && !added[index])
                .max_by(|&a, &b| {
                    connection[a]
                        .total_cmp(&connection[b])
                        .then_with(|| b.cmp(&a))
                })?;
            previous = last;
            last = Some(selected);
            added[selected] = true;

            for neighbor in 0..n {
                if active[neighbor] && !added[neighbor] {
                    connection[neighbor] += weights[selected][neighbor];
                }
            }
        }

        let sink = last?;
        let source = previous?;
        let side = canonical_side(&groups[sink], n);
        let cut_weight = connection[sink];
        if cut_weight < best_weight
            || (cut_weight.total_cmp(&best_weight).is_eq()
                && best_side.as_ref().is_none_or(|current| side < *current))
        {
            best_weight = cut_weight;
            best_side = Some(side);
        }

        let moved = std::mem::take(&mut groups[sink]);
        groups[source].extend(moved);
        groups[source].sort_unstable();
        active[sink] = false;
        remaining -= 1;

        for neighbor in 0..n {
            if active[neighbor] && neighbor != source {
                weights[source][neighbor] += weights[sink][neighbor];
                weights[neighbor][source] = weights[source][neighbor];
            }
        }
    }

    let left_indices = best_side?;
    let left_set: std::collections::HashSet<usize> = left_indices.iter().copied().collect();
    let right_indices: Vec<usize> = (0..n).filter(|index| !left_set.contains(index)).collect();
    Some(MincutPartition {
        cut_weight: best_weight,
        left: left_indices
            .into_iter()
            .map(|index| graph.node_at(index).clone())
            .collect(),
        right: right_indices
            .into_iter()
            .map(|index| graph.node_at(index).clone())
            .collect(),
    })
}

/// Canonicalize a phase side against its complement so partition orientation is
/// independent of which endpoint Stoer-Wagner selected as the sink.
fn canonical_side(side: &[usize], node_count: usize) -> Vec<usize> {
    let mut selected = side.to_vec();
    selected.sort_unstable();
    let selected_set: std::collections::HashSet<usize> = selected.iter().copied().collect();
    let complement: Vec<usize> = (0..node_count)
        .filter(|index| !selected_set.contains(index))
        .collect();
    if selected <= complement {
        selected
    } else {
        complement
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bridged_triangles() -> AdjacencyGraph<&'static str> {
        AdjacencyGraph::from_edges([
            ("a", "b", 3.0),
            ("b", "a", 3.0),
            ("a", "c", 3.0),
            ("c", "a", 3.0),
            ("b", "c", 3.0),
            ("c", "b", 3.0),
            ("d", "e", 3.0),
            ("e", "d", 3.0),
            ("d", "f", 3.0),
            ("f", "d", 3.0),
            ("e", "f", 3.0),
            ("f", "e", 3.0),
            ("c", "d", 0.1),
            ("d", "c", 0.1),
        ])
    }

    #[test]
    fn mincut_returns_stable_ids_for_a_planted_partition() {
        let graph = bridged_triangles();
        let first = stoer_wagner(&graph).expect("two-node cut exists");
        let second = stoer_wagner(&graph).expect("repeat cut exists");
        assert_eq!(first, second);
        assert_eq!(first.left, vec!["a", "b", "c"]);
        assert_eq!(first.right, vec!["d", "e", "f"]);
        assert!((first.cut_weight - 0.2).abs() < 1e-12);
    }

    #[test]
    fn mincut_refuses_invalid_weights_and_trivial_graphs() {
        let singleton = AdjacencyGraph::from_adjacency([("only", Vec::<(&str, f64)>::new())]);
        assert!(stoer_wagner(&singleton).is_none());
        let invalid = AdjacencyGraph::from_edges([("a", "b", -1.0)]);
        assert!(stoer_wagner(&invalid).is_none());
    }
}
