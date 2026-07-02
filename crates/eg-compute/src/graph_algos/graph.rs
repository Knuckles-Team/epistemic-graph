// CONCEPT:EG-144 — Graph data-science algorithms (Neo4j GDS parity).
//
// A standalone, generic adjacency-graph value the graph-algorithms operate on.
// It is deliberately decoupled from the live engine (`GraphView`/petgraph) so the
// algorithms are unit-testable against known small graphs without a running store.
//
// The graph is *directed and weighted*. Undirected algorithms (WCC, Louvain,
// undirected betweenness/similarity) symmetrise on demand. Node identity is
// generic over `N: Clone + Eq + Hash + Ord`; nodes are assigned dense internal
// indices `0..n` in **sorted `N` order**, which is the sole source of the stable
// tie-breaking every algorithm in this module relies on for determinism.

use std::collections::HashMap;
use std::hash::Hash;

/// A dense, directed, weighted adjacency graph over generic node ids `N`.
///
/// Built once from an adjacency list or an edge list; internal algorithms then
/// run over compact `usize` indices (`0..node_count`) and map results back to `N`
/// at the boundary. Parallel edges between the same ordered pair are **summed**.
///
/// CONCEPT:EG-144
#[derive(Debug, Clone)]
pub struct AdjacencyGraph<N> {
    /// Compact index → node id. Sorted, unique — the determinism anchor.
    nodes: Vec<N>,
    /// Node id → compact index.
    index: HashMap<N, usize>,
    /// Outgoing edges per node, each sorted by target index, weights merged.
    out: Vec<Vec<(usize, f64)>>,
    /// Incoming edges per node, each sorted by source index, weights merged.
    inc: Vec<Vec<(usize, f64)>>,
}

impl<N> AdjacencyGraph<N>
where
    N: Clone + Eq + Hash + Ord,
{
    /// Build from an adjacency list: `(node, [(neighbor, weight), ...])`.
    ///
    /// Every id that appears — as a source *or* only as a neighbor — becomes a
    /// node. Duplicate `(src, dst)` edges have their weights summed. This is the
    /// primary constructor the tests use.
    ///
    /// Complexity: O(E log E) for the per-node neighbour sort, O(V log V) for the
    /// stable node ordering. CONCEPT:EG-144
    pub fn from_adjacency<I, J>(adjacency: I) -> Self
    where
        I: IntoIterator<Item = (N, J)>,
        J: IntoIterator<Item = (N, f64)>,
    {
        // Materialise so we can two-pass (collect ids, then edges).
        let rows: Vec<(N, Vec<(N, f64)>)> = adjacency
            .into_iter()
            .map(|(n, nbrs)| (n, nbrs.into_iter().collect()))
            .collect();

        let mut ids: Vec<N> = Vec::new();
        for (src, nbrs) in &rows {
            ids.push(src.clone());
            for (dst, _) in nbrs {
                ids.push(dst.clone());
            }
        }
        ids.sort();
        ids.dedup();

        let index: HashMap<N, usize> = ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, n)| (n, i))
            .collect();

        let n = ids.len();
        let mut out_maps: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n];
        let mut inc_maps: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n];

        for (src, nbrs) in rows {
            let si = index[&src];
            for (dst, w) in nbrs {
                let ti = index[&dst];
                *out_maps[si].entry(ti).or_insert(0.0) += w;
                *inc_maps[ti].entry(si).or_insert(0.0) += w;
            }
        }

        let out = out_maps.into_iter().map(sorted_edges).collect();
        let inc = inc_maps.into_iter().map(sorted_edges).collect();

        Self {
            nodes: ids,
            index,
            out,
            inc,
        }
    }

    /// Build from a flat weighted edge list `(src, dst, weight)`.
    /// CONCEPT:EG-144
    pub fn from_edges<I>(edges: I) -> Self
    where
        I: IntoIterator<Item = (N, N, f64)>,
    {
        let mut grouped: HashMap<N, Vec<(N, f64)>> = HashMap::new();
        let mut order: Vec<N> = Vec::new();
        for (s, d, w) in edges {
            if !grouped.contains_key(&s) {
                order.push(s.clone());
            }
            grouped.entry(s).or_default().push((d, w));
        }
        // Preserve deterministic construction; from_adjacency re-sorts anyway.
        let adjacency: Vec<(N, Vec<(N, f64)>)> = order
            .into_iter()
            .map(|s| {
                let nbrs = grouped.remove(&s).unwrap_or_default();
                (s, nbrs)
            })
            .collect();
        Self::from_adjacency(adjacency)
    }

    /// Build from an *unweighted* edge list (every edge weight = 1.0).
    /// CONCEPT:EG-144
    pub fn from_unweighted_edges<I>(edges: I) -> Self
    where
        I: IntoIterator<Item = (N, N)>,
    {
        Self::from_edges(edges.into_iter().map(|(s, d)| (s, d, 1.0)))
    }

    /// Number of nodes.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of directed edges (after parallel-edge merge).
    #[inline]
    pub fn edge_count(&self) -> usize {
        self.out.iter().map(Vec::len).sum()
    }

    /// The nodes in compact-index order (i.e. sorted `N`).
    #[inline]
    pub fn nodes(&self) -> &[N] {
        &self.nodes
    }

    /// Compact index of a node id, if present.
    #[inline]
    pub fn index_of(&self, node: &N) -> Option<usize> {
        self.index.get(node).copied()
    }

    /// Node id at a compact index.
    #[inline]
    pub fn node_at(&self, idx: usize) -> &N {
        &self.nodes[idx]
    }

    /// Outgoing `(neighbor_idx, weight)` edges, sorted by neighbor index.
    #[inline]
    pub fn out_edges(&self, idx: usize) -> &[(usize, f64)] {
        &self.out[idx]
    }

    /// Incoming `(source_idx, weight)` edges, sorted by source index.
    #[inline]
    pub fn in_edges(&self, idx: usize) -> &[(usize, f64)] {
        &self.inc[idx]
    }

    /// Weighted out-degree (sum of outgoing edge weights).
    #[inline]
    pub fn weighted_out_degree(&self, idx: usize) -> f64 {
        self.out[idx].iter().map(|(_, w)| *w).sum()
    }

    /// Number of outgoing edges.
    #[inline]
    pub fn out_degree(&self, idx: usize) -> usize {
        self.out[idx].len()
    }

    /// Number of incoming edges.
    #[inline]
    pub fn in_degree(&self, idx: usize) -> usize {
        self.inc[idx].len()
    }

    /// Undirected neighbour set of a node as a sorted, de-duplicated index list
    /// (union of out- and in-neighbours, self excluded). Used by the undirected
    /// algorithms (similarity, WCC). CONCEPT:EG-144
    pub fn undirected_neighbors(&self, idx: usize) -> Vec<usize> {
        let mut v: Vec<usize> = Vec::with_capacity(self.out[idx].len() + self.inc[idx].len());
        for &(t, _) in &self.out[idx] {
            if t != idx {
                v.push(t);
            }
        }
        for &(s, _) in &self.inc[idx] {
            if s != idx {
                v.push(s);
            }
        }
        v.sort_unstable();
        v.dedup();
        v
    }

    /// Build a symmetric undirected weighted adjacency (each undirected edge's
    /// weight is the sum of both directions; self-loops kept once). Returned as
    /// per-node sorted `(neighbor, weight)` lists over compact indices. Shared by
    /// Louvain and weighted undirected measures. CONCEPT:EG-144
    pub fn undirected_weighted_adjacency(&self) -> Vec<Vec<(usize, f64)>> {
        let n = self.node_count();
        let mut maps: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n];
        for u in 0..n {
            for &(v, w) in &self.out[u] {
                if u == v {
                    *maps[u].entry(u).or_insert(0.0) += w;
                } else {
                    *maps[u].entry(v).or_insert(0.0) += w;
                    *maps[v].entry(u).or_insert(0.0) += w;
                }
            }
        }
        maps.into_iter().map(sorted_edges).collect()
    }

    /// Map an index-keyed score vector back to `(N, score)` pairs in node order.
    pub(crate) fn label_scores(&self, scores: &[f64]) -> Vec<(N, f64)> {
        self.nodes
            .iter()
            .cloned()
            .zip(scores.iter().copied())
            .collect()
    }

    /// Map compact-index communities to sorted `Vec<Vec<N>>`, communities ordered
    /// by their smallest member for determinism.
    pub(crate) fn label_partition(&self, membership: &[usize]) -> Vec<Vec<N>> {
        use std::collections::BTreeMap;
        let mut groups: BTreeMap<usize, Vec<N>> = BTreeMap::new();
        for (i, &c) in membership.iter().enumerate() {
            groups.entry(c).or_default().push(self.nodes[i].clone());
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

/// Turn an index→weight map into a sorted `(idx, weight)` edge list.
fn sorted_edges(m: HashMap<usize, f64>) -> Vec<(usize, f64)> {
    let mut v: Vec<(usize, f64)> = m.into_iter().collect();
    v.sort_unstable_by_key(|(i, _)| *i);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_graph_builds_dense_sorted_indices() {
        // Nodes given out of order; indices must follow sorted id order.
        let g = AdjacencyGraph::from_edges([("c", "a", 1.0), ("a", "b", 2.0), ("b", "c", 3.0)]);
        assert_eq!(g.node_count(), 3);
        assert_eq!(g.nodes(), &["a", "b", "c"]);
        assert_eq!(g.index_of(&"a"), Some(0));
        assert_eq!(g.index_of(&"c"), Some(2));
        assert_eq!(g.edge_count(), 3);
    }

    #[test]
    fn eg144_graph_merges_parallel_edges() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("a", "b", 2.5)]);
        assert_eq!(g.edge_count(), 1);
        let a = g.index_of(&"a").unwrap();
        assert_eq!(g.out_edges(a), &[(g.index_of(&"b").unwrap(), 3.5)]);
    }

    #[test]
    fn eg144_graph_neighbor_only_nodes_are_registered() {
        // "z" only appears as a target — must still be a node.
        let g = AdjacencyGraph::from_adjacency([("a", vec![("z", 1.0)])]);
        assert_eq!(g.node_count(), 2);
        assert!(g.index_of(&"z").is_some());
        assert_eq!(g.out_degree(g.index_of(&"z").unwrap()), 0);
    }

    #[test]
    fn eg144_undirected_neighbors_union_both_directions() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("c", "a", 1.0)]);
        let a = g.index_of(&"a").unwrap();
        let nbrs: Vec<&str> = g
            .undirected_neighbors(a)
            .into_iter()
            .map(|i| *g.node_at(i))
            .collect();
        assert_eq!(nbrs, vec!["b", "c"]);
    }
}
