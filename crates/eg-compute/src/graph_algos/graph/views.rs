use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use super::storage::GraphStorage;

/// Derived and undirected views over compact graph storage.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct GraphViews<N> {
    pub(super) storage: GraphStorage<N>,
}

impl<N> GraphViews<N>
where
    N: Clone + Eq + Hash + Ord,
{
    /// Weighted out-degree (sum of outgoing edge weights).
    #[inline]
    pub fn weighted_out_degree(&self, idx: usize) -> f64 {
        self.storage.out[idx].iter().map(|(_, w)| *w).sum()
    }

    /// Number of outgoing edges.
    #[inline]
    pub fn out_degree(&self, idx: usize) -> usize {
        self.storage.out[idx].len()
    }

    /// Number of incoming edges.
    #[inline]
    pub fn in_degree(&self, idx: usize) -> usize {
        self.storage.inc[idx].len()
    }

    /// Undirected neighbour set of a node as a sorted, de-duplicated index list
    /// (union of out- and in-neighbours, self excluded). Used by the undirected
    /// algorithms (similarity, WCC).
    pub fn undirected_neighbors(&self, idx: usize) -> Vec<usize> {
        let out = &self.storage.out[idx];
        let inc = &self.storage.inc[idx];
        let mut neighbors: Vec<usize> = Vec::with_capacity(out.len() + inc.len());
        neighbors.extend(
            out.iter()
                .filter_map(|&(target, _)| (target != idx).then_some(target)),
        );
        neighbors.extend(
            inc.iter()
                .filter_map(|&(source, _)| (source != idx).then_some(source)),
        );
        neighbors.sort_unstable();
        neighbors.dedup();
        neighbors
    }

    /// Build a symmetric undirected weighted adjacency (each undirected edge's
    /// weight is the sum of both directions; self-loops kept once). Returned as
    /// per-node sorted `(neighbor, weight)` lists over compact indices. Shared by
    /// Louvain and weighted undirected measures.
    pub fn undirected_weighted_adjacency(&self) -> Vec<Vec<(usize, f64)>> {
        let n = self.storage.nodes.len();
        let mut maps: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n];
        for (u, edges) in self.storage.out.iter().enumerate() {
            for &(v, weight) in edges {
                if u == v {
                    *maps[u].entry(u).or_insert(0.0) += weight;
                } else {
                    *maps[u].entry(v).or_insert(0.0) += weight;
                    *maps[v].entry(u).or_insert(0.0) += weight;
                }
            }
        }
        maps.into_iter().map(super::storage::sorted_edges).collect()
    }

    /// Map an index-keyed score vector back to `(N, score)` pairs in node order.
    pub(crate) fn label_scores(&self, scores: &[f64]) -> Vec<(N, f64)> {
        self.storage
            .nodes
            .iter()
            .cloned()
            .zip(scores.iter().copied())
            .collect()
    }

    /// Map compact-index communities to sorted `Vec<Vec<N>>`, communities
    /// ordered by their smallest member for determinism.
    pub(crate) fn label_partition(&self, membership: &[usize]) -> Vec<Vec<N>> {
        let mut groups: BTreeMap<usize, Vec<N>> = BTreeMap::new();
        for (index, &community) in membership.iter().enumerate() {
            groups
                .entry(community)
                .or_default()
                .push(self.storage.nodes[index].clone());
        }
        groups
            .into_values()
            .map(|mut members| {
                members.sort();
                members
            })
            .collect()
    }
}
