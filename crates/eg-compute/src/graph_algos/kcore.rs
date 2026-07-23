// CONCEPT:EG-KG.compute.k-core-decomposition — k-core decomposition via degeneracy peeling
// (Batagelj–Zaversnik / Matula–Beck). Neo4j GDS `gds.kcore` parity.

use super::graph::AdjacencyGraph;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::Hash;

// Min-heap entry ordered by (degree, node) — mirrors `shortest_path::HeapItem`'s
// reversed-Ord-for-min-heap + ascending-node-id tie-break idiom, so peeling order
// (hence every result) is deterministic.
#[derive(PartialEq, Eq)]
struct CoreHeapItem {
    degree: usize,
    node: usize,
}
impl Ord for CoreHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .degree
            .cmp(&self.degree)
            .then_with(|| other.node.cmp(&self.node))
    }
}
impl PartialOrd for CoreHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// k-core decomposition: each node's **core number** (coreness) — the largest
/// `k` such that the node belongs to a `k`-core (a maximal subgraph in which
/// every member has undirected degree ≥ `k` within that subgraph).
///
/// Computed by degeneracy ("peeling") over the undirected symmetrisation: the
/// current minimum-degree node is repeatedly removed, its core number set to
/// `max(k_so_far, its degree at removal)`, and its remaining neighbours'
/// degrees decremented — the classical Batagelj–Zaversnik algorithm. Uses a
/// binary heap with lazy (stale-entry-skipping) decrease-key, the same idiom
/// [`super::shortest_path::dijkstra`] uses, so ties always peel the
/// smallest-degree, then smallest-id node first (deterministic).
///
/// Returns `(node, core_number)` in node order.
///
/// Complexity: `O((V + E) log V)`. CONCEPT:EG-KG.compute.k-core-decomposition
pub fn k_core<N>(graph: &AdjacencyGraph<N>) -> Vec<(N, u64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let nbrs: Vec<Vec<usize>> = (0..n).map(|i| graph.undirected_neighbors(i)).collect();
    let mut cur_degree: Vec<usize> = nbrs.iter().map(Vec::len).collect();
    let mut removed = vec![false; n];
    let mut core = vec![0u64; n];

    let mut heap: BinaryHeap<CoreHeapItem> = BinaryHeap::with_capacity(n);
    for (v, deg) in cur_degree.iter().enumerate() {
        heap.push(CoreHeapItem {
            degree: *deg,
            node: v,
        });
    }

    let mut k_so_far = 0u64;
    while let Some(CoreHeapItem { degree: d, node: v }) = heap.pop() {
        if removed[v] || d > cur_degree[v] {
            continue; // stale heap entry (already removed, or degree since decreased)
        }
        removed[v] = true;
        k_so_far = k_so_far.max(d as u64);
        core[v] = k_so_far;
        for &nb in &nbrs[v] {
            if !removed[nb] {
                cur_degree[nb] -= 1;
                heap.push(CoreHeapItem {
                    degree: cur_degree[nb],
                    node: nb,
                });
            }
        }
    }

    graph.nodes().iter().cloned().zip(core).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn eg144_kcore_triangle_plus_pendant() {
        // Triangle {a,b,c} (core 2) with a pendant d attached only to a (core 1).
        let g =
            AdjacencyGraph::from_unweighted_edges([("a", "b"), ("b", "c"), ("a", "c"), ("a", "d")]);
        let core = k_core(&g);
        let m: HashMap<&str, u64> = core.into_iter().collect();
        assert_eq!(m["a"], 2);
        assert_eq!(m["b"], 2);
        assert_eq!(m["c"], 2);
        assert_eq!(m["d"], 1);
    }

    #[test]
    fn eg144_kcore_k4_clique_is_a_3_core() {
        let g = AdjacencyGraph::from_unweighted_edges([
            ("a", "b"),
            ("a", "c"),
            ("a", "d"),
            ("b", "c"),
            ("b", "d"),
            ("c", "d"),
        ]);
        let core = k_core(&g);
        for (_, c) in core {
            assert_eq!(c, 3, "every K4 member has core number 3");
        }
    }

    #[test]
    fn eg144_kcore_bridge_between_two_triangles_stays_core_2() {
        // Two triangles joined by one bridge edge: the bridge endpoints have
        // degree 3 but their CORE remains 2 (removing the bridge alone still
        // leaves each triangle a valid 2-core; the 3rd edge doesn't sustain a
        // higher core since peeling removes degree-2 non-bridge members first,
        // dropping the bridge endpoints back to degree 2 before they'd ever
        // qualify for a 3-core).
        let g = AdjacencyGraph::from_unweighted_edges([
            ("a", "b"),
            ("b", "c"),
            ("a", "c"),
            ("c", "d"),
            ("d", "e"),
            ("d", "f"),
            ("e", "f"),
        ]);
        let core = k_core(&g);
        for (_, c) in core {
            assert_eq!(c, 2);
        }
    }

    #[test]
    fn eg144_kcore_isolated_node_is_zero() {
        let g = AdjacencyGraph::from_adjacency([("a", vec![("b", 1.0)]), ("lonely", vec![])]);
        let core = k_core(&g);
        let m: HashMap<&str, u64> = core.into_iter().collect();
        assert_eq!(m["lonely"], 0);
        assert_eq!(m["a"], 1);
    }

    #[test]
    fn eg144_kcore_empty_graph() {
        let g: AdjacencyGraph<&str> =
            AdjacencyGraph::from_adjacency(Vec::<(&str, Vec<(&str, f64)>)>::new());
        assert!(k_core(&g).is_empty());
    }
}
