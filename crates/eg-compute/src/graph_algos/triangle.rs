// CONCEPT:EG-KG.compute.triangle-counting — Triangle counting + local clustering coefficient over the
// undirected symmetrisation of the graph. Neo4j GDS `gds.triangleCount` /
// `gds.localClusteringCoefficient` parity.

use super::graph::AdjacencyGraph;
use std::hash::Hash;

/// Per-node undirected neighbour lists, sorted (shared by both functions below).
fn undirected_adjacency<N>(graph: &AdjacencyGraph<N>) -> Vec<Vec<usize>>
where
    N: Clone + Eq + Hash + Ord,
{
    (0..graph.node_count())
        .map(|i| graph.undirected_neighbors(i))
        .collect()
}

/// Count how many triangles each node participates in, over the undirected
/// symmetrisation (edge direction and weight are ignored — a triangle is a
/// purely structural, unweighted notion here, matching GDS's default).
///
/// For each node `v`, every pair of `v`'s neighbours `(a, b)` with `a < b` that
/// are ALSO neighbours of each other closes one triangle `{v, a, b}`; running
/// this from every node's own perspective counts each triangle exactly once at
/// EACH of its three vertices (i.e. `counts[v]` really is "triangles touching
/// v", not "triangles times something"). Returns `(node, count)` in node order.
///
/// Complexity: `O(Σ_v deg(v)²)`, bounded by `O(V · d_max²)` (equivalently
/// `O(V·E)` worst case) — the same complexity class GDS's own (non-approximate)
/// triangle-count default cites. CONCEPT:EG-KG.compute.triangle-counting
pub fn triangle_count<N>(graph: &AdjacencyGraph<N>) -> Vec<(N, u64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let nbrs = undirected_adjacency(graph);
    let mut counts = vec![0u64; n];
    for v in 0..n {
        let nv = &nbrs[v];
        for i in 0..nv.len() {
            for j in (i + 1)..nv.len() {
                let (a, b) = (nv[i], nv[j]);
                if nbrs[a].binary_search(&b).is_ok() {
                    counts[v] += 1;
                }
            }
        }
    }
    graph.nodes().iter().cloned().zip(counts).collect()
}

/// The local clustering coefficient of every node: `LCC(v) = 2·triangles(v) /
/// (deg(v)·(deg(v)−1))` over the undirected degree — the fraction of `v`'s
/// neighbour PAIRS that are themselves connected. `0.0` when `deg(v) < 2` (no
/// possible triangle). Returns `(node, coefficient)` in node order.
///
/// Reuses [`triangle_count`]'s per-node counts over the SAME undirected
/// neighbour structure, so the two are always consistent with each other.
///
/// Complexity: `O(Σ_v deg(v)²)`, same as [`triangle_count`].
/// CONCEPT:EG-KG.compute.triangle-counting
pub fn local_clustering_coefficient<N>(graph: &AdjacencyGraph<N>) -> Vec<(N, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let nbrs = undirected_adjacency(graph);
    let triangles = triangle_count(graph);
    (0..n)
        .map(|v| {
            let deg = nbrs[v].len();
            let lcc = if deg < 2 {
                0.0
            } else {
                let t = triangles[v].1 as f64;
                2.0 * t / (deg as f64 * (deg as f64 - 1.0))
            };
            (graph.node_at(v).clone(), lcc)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn eg144_triangle_count_single_triangle_plus_isolated_node() {
        let g = AdjacencyGraph::from_adjacency([
            ("a", vec![("b", 1.0), ("c", 1.0)]),
            ("b", vec![("c", 1.0)]),
            ("d", vec![]),
        ]);
        let counts = triangle_count(&g);
        let m: HashMap<&str, u64> = counts.into_iter().collect();
        assert_eq!(m["a"], 1);
        assert_eq!(m["b"], 1);
        assert_eq!(m["c"], 1);
        assert_eq!(m["d"], 0);
    }

    #[test]
    fn eg144_triangle_count_bowtie_center_counts_both_triangles() {
        // Two triangles {a,b,c} and {a,d,e} sharing vertex `a` (a "bowtie").
        let g = AdjacencyGraph::from_unweighted_edges([
            ("a", "b"),
            ("b", "c"),
            ("a", "c"),
            ("a", "d"),
            ("d", "e"),
            ("a", "e"),
        ]);
        let counts = triangle_count(&g);
        let m: HashMap<&str, u64> = counts.into_iter().collect();
        assert_eq!(m["a"], 2, "center touches both triangles");
        assert_eq!(m["b"], 1);
        assert_eq!(m["c"], 1);
        assert_eq!(m["d"], 1);
        assert_eq!(m["e"], 1);
    }

    #[test]
    fn eg144_triangle_count_direction_and_weight_are_ignored() {
        // A directed 3-cycle a->b->c->a is still one undirected triangle.
        let g = AdjacencyGraph::from_edges([("a", "b", 5.0), ("b", "c", 0.1), ("c", "a", 2.0)]);
        let counts = triangle_count(&g);
        for (_, c) in counts {
            assert_eq!(c, 1);
        }
    }

    #[test]
    fn eg144_lcc_full_triangle_is_one() {
        let g = AdjacencyGraph::from_unweighted_edges([("a", "b"), ("b", "c"), ("a", "c")]);
        let lcc = local_clustering_coefficient(&g);
        for (_, v) in lcc {
            assert!((v - 1.0).abs() < 1e-12);
        }
    }

    #[test]
    fn eg144_lcc_star_center_and_leaves_are_zero() {
        // Center has 3 leaves, no leaf-leaf edges: no closed wedge anywhere.
        let g = AdjacencyGraph::from_unweighted_edges([("c", "l1"), ("c", "l2"), ("c", "l3")]);
        let lcc = local_clustering_coefficient(&g);
        let m: HashMap<&str, f64> = lcc.into_iter().collect();
        assert!((m["c"] - 0.0).abs() < 1e-12, "center deg=3, no leaf edges");
        assert!(
            (m["l1"] - 0.0).abs() < 1e-12,
            "leaf deg=1 < 2 ⇒ 0 by convention"
        );
    }

    #[test]
    fn eg144_lcc_cross_check_hand_computed_path_of_four() {
        // Path a-b-c-d: b has neighbours {a,c} (not connected) ⇒ LCC(b)=0;
        // same for c. Endpoints have degree 1 ⇒ 0 by convention.
        let g = AdjacencyGraph::from_unweighted_edges([("a", "b"), ("b", "c"), ("c", "d")]);
        let lcc = local_clustering_coefficient(&g);
        for (_, v) in lcc {
            assert!((v - 0.0).abs() < 1e-12);
        }
    }

    #[test]
    fn eg144_lcc_partial_closure_hand_computed() {
        // Center `v` has 3 neighbours a,b,c; only the pair (a,b) is itself an
        // edge. deg(v)=3 ⇒ possible pairs = 3; closed = 1 ⇒ LCC = 2*1/(3*2) = 1/3.
        let g =
            AdjacencyGraph::from_unweighted_edges([("v", "a"), ("v", "b"), ("v", "c"), ("a", "b")]);
        let lcc = local_clustering_coefficient(&g);
        let m: HashMap<&str, f64> = lcc.into_iter().collect();
        assert!((m["v"] - 1.0 / 3.0).abs() < 1e-9, "got {}", m["v"]);
    }
}
