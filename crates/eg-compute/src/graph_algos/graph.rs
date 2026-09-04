// CONCEPT:EG-KG.compute.graph-data-science-algorithms — Graph data-science algorithms (Neo4j GDS parity).
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
use std::ops::Deref;

#[path = "graph/storage.rs"]
mod storage;
#[path = "graph/topology.rs"]
mod topology;
#[path = "graph/views.rs"]
mod views;

use storage::{sorted_edges, GraphStorage};
pub use topology::GraphTopology;
pub use views::GraphViews;

/// A dense, directed, weighted adjacency graph over generic node ids `N`.
///
/// Built once from an adjacency list or an edge list; internal algorithms then
/// run over compact `usize` indices (`0..node_count`) and map results back to `N`
/// at the boundary. Parallel edges between the same ordered pair are **summed**.
/// Stable topology and derived views are exposed through transparent deref
/// capability layers, keeping the public method surface unchanged.
///
/// CONCEPT:EG-KG.compute.graph-data-science-algorithms
#[derive(Debug, Clone)]
pub struct AdjacencyGraph<N> {
    topology: GraphTopology<N>,
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
    /// stable node ordering. CONCEPT:EG-KG.compute.graph-data-science-algorithms
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
            topology: GraphTopology::from_storage(GraphStorage {
                nodes: ids,
                index,
                out,
                inc,
            }),
        }
    }

    /// Build from a flat weighted edge list `(src, dst, weight)`.
    /// CONCEPT:EG-KG.compute.graph-data-science-algorithms
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
    /// CONCEPT:EG-KG.compute.graph-data-science-algorithms
    pub fn from_unweighted_edges<I>(edges: I) -> Self
    where
        I: IntoIterator<Item = (N, N)>,
    {
        Self::from_edges(edges.into_iter().map(|(s, d)| (s, d, 1.0)))
    }
}

impl<N> Deref for AdjacencyGraph<N> {
    type Target = GraphTopology<N>;

    fn deref(&self) -> &Self::Target {
        &self.topology
    }
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
