// CONCEPT:EG-KG.compute.k1-coloring — Greedy graph coloring (Welsh–Powell largest-degree-first).
// Neo4j GDS `gds.k1coloring` parity: a PROPER coloring (no two adjacent nodes
// share a color), heuristically minimizing the color count (finding the true
// minimum — the chromatic number — is NP-hard; this is the same greedy-heuristic
// scope GDS's own K1Coloring documents).

use super::graph::AdjacencyGraph;
use std::collections::BTreeSet;
use std::hash::Hash;

/// Greedily colors every node so that no two nodes joined by an (undirected)
/// edge share a color, using the **Welsh–Powell** heuristic: process nodes in
/// DESCENDING undirected-degree order (ties broken by ascending node id — both
/// choices deterministic), assigning each the smallest color not already used
/// by an already-colored neighbour. Processing high-degree nodes first tends to
/// use fewer colors than a naive ascending-id order.
///
/// Returns `(node, color)` in node order; colors are dense `0..k` for some
/// `k` ≤ the graph's max degree + 1 (the greedy bound).
///
/// Complexity: `O(V log V + Σ_v deg(v))`, i.e. `O(V log V + E)`.
/// CONCEPT:EG-KG.compute.k1-coloring
pub fn k1_coloring<N>(graph: &AdjacencyGraph<N>) -> Vec<(N, u64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let nbrs: Vec<Vec<usize>> = (0..n).map(|i| graph.undirected_neighbors(i)).collect();

    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| nbrs[b].len().cmp(&nbrs[a].len()).then(a.cmp(&b)));

    const UNCOLORED: u64 = u64::MAX;
    let mut color = vec![UNCOLORED; n];
    for &v in &order {
        let used: BTreeSet<u64> = nbrs[v]
            .iter()
            .filter_map(|&nb| {
                let c = color[nb];
                (c != UNCOLORED).then_some(c)
            })
            .collect();
        let mut c = 0u64;
        while used.contains(&c) {
            c += 1;
        }
        color[v] = c;
    }

    graph.nodes().iter().cloned().zip(color).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// The genuine correctness property: no two ADJACENT nodes share a color.
    fn assert_proper_coloring(g: &AdjacencyGraph<&str>, colors: &HashMap<&str, u64>) {
        for i in 0..g.node_count() {
            for &j in &g.undirected_neighbors(i) {
                let (u, v) = (*g.node_at(i), *g.node_at(j));
                assert_ne!(
                    colors[u], colors[v],
                    "adjacent nodes {u} and {v} share color {}",
                    colors[u]
                );
            }
        }
    }

    #[test]
    fn eg144_coloring_triangle_needs_exactly_three_colors() {
        let g = AdjacencyGraph::from_unweighted_edges([("a", "b"), ("b", "c"), ("a", "c")]);
        let colors: HashMap<&str, u64> = k1_coloring(&g).into_iter().collect();
        assert_proper_coloring(&g, &colors);
        let distinct: std::collections::BTreeSet<u64> = colors.values().copied().collect();
        assert_eq!(
            distinct.len(),
            3,
            "K3 needs exactly 3 colors, got {colors:?}"
        );
    }

    #[test]
    fn eg144_coloring_k4_needs_exactly_four_colors() {
        let g = AdjacencyGraph::from_unweighted_edges([
            ("a", "b"),
            ("a", "c"),
            ("a", "d"),
            ("b", "c"),
            ("b", "d"),
            ("c", "d"),
        ]);
        let colors: HashMap<&str, u64> = k1_coloring(&g).into_iter().collect();
        assert_proper_coloring(&g, &colors);
        let distinct: std::collections::BTreeSet<u64> = colors.values().copied().collect();
        assert_eq!(distinct.len(), 4);
    }

    #[test]
    fn eg144_coloring_bipartite_star_needs_only_two_colors() {
        let g = AdjacencyGraph::from_unweighted_edges([
            ("center", "l1"),
            ("center", "l2"),
            ("center", "l3"),
        ]);
        let colors: HashMap<&str, u64> = k1_coloring(&g).into_iter().collect();
        assert_proper_coloring(&g, &colors);
        let distinct: std::collections::BTreeSet<u64> = colors.values().copied().collect();
        assert_eq!(distinct.len(), 2, "a star is bipartite: 2 colors suffice");
        assert_eq!(colors["l1"], colors["l2"]);
        assert_eq!(colors["l2"], colors["l3"]);
        assert_ne!(colors["center"], colors["l1"]);
    }

    #[test]
    fn eg144_coloring_no_edges_all_share_color_zero() {
        let g: AdjacencyGraph<&str> =
            AdjacencyGraph::from_adjacency([("a", vec![]), ("b", vec![]), ("c", vec![])]);
        let colors: HashMap<&str, u64> = k1_coloring(&g).into_iter().collect();
        for c in colors.values() {
            assert_eq!(*c, 0);
        }
    }

    #[test]
    fn eg144_coloring_is_deterministic_across_runs() {
        let g = AdjacencyGraph::from_unweighted_edges([
            ("a", "b"),
            ("b", "c"),
            ("c", "d"),
            ("d", "a"),
            ("a", "c"),
        ]);
        let r1 = k1_coloring(&g);
        let r2 = k1_coloring(&g);
        assert_eq!(r1, r2);
    }
}
