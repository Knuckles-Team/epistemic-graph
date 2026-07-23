// CONCEPT:EG-KG.compute.yens-k-shortest-paths — Yen's algorithm for the k shortest LOOPLESS
// (simple) paths between two nodes. Neo4j GDS `gds.shortestPath.yens` parity.

use super::graph::AdjacencyGraph;
use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};
use std::hash::Hash;

/// One ranked path from [`yen_k_shortest_paths`]: a node-label sequence
/// (source→target order) and its total cost. CONCEPT:EG-KG.compute.yens-k-shortest-paths
#[derive(Debug, Clone, PartialEq)]
pub struct RankedPath<N> {
    pub nodes: Vec<N>,
    pub cost: f64,
}

// Min-heap entry for the internal restricted single-path search — same
// reversed-Ord + ascending-node-id tie-break idiom as `shortest_path::HeapItem`.
#[derive(PartialEq)]
struct YenHeapItem {
    dist: f64,
    node: usize,
}
impl Eq for YenHeapItem {}
impl Ord for YenHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.node.cmp(&self.node))
    }
}
impl PartialOrd for YenHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Dijkstra restricted to skip a set of blocked nodes/edges entirely — the
/// "spur search" primitive Yen's algorithm re-runs for every candidate. A
/// fresh, self-contained search rather than mutating [`AdjacencyGraph`] (which
/// is an immutable, once-built value everywhere else in this crate).
fn restricted_shortest_path<N>(
    graph: &AdjacencyGraph<N>,
    source: usize,
    target: usize,
    blocked_nodes: &BTreeSet<usize>,
    blocked_edges: &BTreeSet<(usize, usize)>,
) -> Option<(Vec<usize>, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    if blocked_nodes.contains(&source) || blocked_nodes.contains(&target) {
        return None;
    }
    let n = graph.node_count();
    let mut dist: Vec<Option<f64>> = vec![None; n];
    let mut prev: Vec<Option<usize>> = vec![None; n];
    dist[source] = Some(0.0);

    let mut heap = BinaryHeap::new();
    heap.push(YenHeapItem {
        dist: 0.0,
        node: source,
    });
    while let Some(YenHeapItem { dist: d, node: u }) = heap.pop() {
        if matches!(dist[u], Some(best) if d > best) {
            continue;
        }
        if u == target {
            let mut path = vec![u];
            let mut cur = u;
            while cur != source {
                cur = prev[cur]?;
                path.push(cur);
            }
            path.reverse();
            return Some((path, d));
        }
        for &(v, w) in graph.out_edges(u) {
            if blocked_nodes.contains(&v) || blocked_edges.contains(&(u, v)) {
                continue;
            }
            let nd = d + w;
            let better = match dist[v] {
                None => true,
                Some(old) => nd < old,
            };
            if better {
                dist[v] = Some(nd);
                prev[v] = Some(u);
                heap.push(YenHeapItem { dist: nd, node: v });
            }
        }
    }
    None
}

fn path_cost<N>(graph: &AdjacencyGraph<N>, path: &[usize]) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    path.windows(2)
        .map(|w| edge_weight(graph, w[0], w[1]))
        .sum()
}

fn edge_weight<N>(graph: &AdjacencyGraph<N>, u: usize, v: usize) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    graph
        .out_edges(u)
        .binary_search_by_key(&v, |&(t, _)| t)
        .map(|i| graph.out_edges(u)[i].1)
        .unwrap_or(0.0)
}

/// The `k` shortest LOOPLESS (simple, no repeated node) paths from `source` to
/// `target`, by total weight, via **Yen's algorithm**: the first path is a
/// plain shortest path; each subsequent path is found by, for every node
/// along the previous path, "spurring" off a new restricted search that
/// blocks (a) the edges already used to leave that exact root prefix by any
/// path already found, and (b) every node earlier in the root prefix (so the
/// spur cannot loop back through it) — the best such candidate across every
/// spur point becomes the next result.
///
/// Returns UP TO `k` paths (fewer if that many distinct simple paths don't
/// exist), sorted by ascending cost, ties broken by lexicographically-smallest
/// node-index sequence (deterministic, no `HashSet`/`HashMap` iteration-order
/// dependence anywhere in the algorithm). Empty if `target` is unreachable
/// from `source` at all.
///
/// Complexity: `O(k·V·(V+E)logV)` — `k` rounds, each up to `V` spur searches,
/// each an `O((V+E)logV)` restricted Dijkstra; the standard textbook bound for
/// Yen's algorithm. CONCEPT:EG-KG.compute.yens-k-shortest-paths
pub fn yen_k_shortest_paths<N>(
    graph: &AdjacencyGraph<N>,
    source: usize,
    target: usize,
    k: usize,
) -> Vec<RankedPath<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if source >= n || target >= n || k == 0 {
        return Vec::new();
    }

    let empty_nodes = BTreeSet::new();
    let empty_edges = BTreeSet::new();
    let Some(first) = restricted_shortest_path(graph, source, target, &empty_nodes, &empty_edges)
    else {
        return Vec::new();
    };

    let mut a: Vec<(Vec<usize>, f64)> = vec![first];
    // Candidate pool: a small `Vec`, sorted explicitly each round rather than a
    // `BinaryHeap` — `k` and the candidate count per round are both small in
    // practice, and a plain sort keeps the tie-break trivially deterministic.
    let mut b: Vec<(f64, Vec<usize>)> = Vec::new();

    while a.len() < k {
        let prev_path = a.last().unwrap().0.clone();
        for i_spur in 0..prev_path.len().saturating_sub(1) {
            let spur_node = prev_path[i_spur];
            let root_path = &prev_path[..=i_spur];

            let mut blocked_edges: BTreeSet<(usize, usize)> = BTreeSet::new();
            for (path, _) in &a {
                if path.len() > i_spur + 1 && &path[..=i_spur] == root_path {
                    blocked_edges.insert((path[i_spur], path[i_spur + 1]));
                }
            }
            let blocked_nodes: BTreeSet<usize> = root_path[..i_spur].iter().copied().collect();

            if let Some((spur_path, _)) =
                restricted_shortest_path(graph, spur_node, target, &blocked_nodes, &blocked_edges)
            {
                let mut total_path = root_path[..i_spur].to_vec();
                total_path.extend(spur_path);
                let total_cost = path_cost(graph, &total_path);
                let already_in_a = a.iter().any(|(p, _)| *p == total_path);
                let already_in_b = b.iter().any(|(_, p)| *p == total_path);
                if !already_in_a && !already_in_b {
                    b.push((total_cost, total_path));
                }
            }
        }
        if b.is_empty() {
            break;
        }
        b.sort_by(|x, y| {
            x.0.partial_cmp(&y.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| x.1.cmp(&y.1))
        });
        let (cost, path) = b.remove(0);
        a.push((path, cost));
    }

    a.into_iter()
        .map(|(path, cost)| RankedPath {
            nodes: path.into_iter().map(|i| graph.node_at(i).clone()).collect(),
            cost,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_yen_diamond_with_shortcut_ranks_three_known_paths() {
        // a-b-d (cost 2) < a-c-d (cost 3) < direct a-d (cost 5). Hand-verified
        // by simulating Yen's own spur logic (see module test doc history).
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "d", 1.0),
            ("a", "c", 2.0),
            ("c", "d", 1.0),
            ("a", "d", 5.0),
        ]);
        let (source, target) = (g.index_of(&"a").unwrap(), g.index_of(&"d").unwrap());
        let paths = yen_k_shortest_paths(&g, source, target, 3);
        assert_eq!(paths.len(), 3);
        assert_eq!(paths[0].nodes, vec!["a", "b", "d"]);
        assert!((paths[0].cost - 2.0).abs() < 1e-9);
        assert_eq!(paths[1].nodes, vec!["a", "c", "d"]);
        assert!((paths[1].cost - 3.0).abs() < 1e-9);
        assert_eq!(paths[2].nodes, vec!["a", "d"]);
        assert!((paths[2].cost - 5.0).abs() < 1e-9);
        // Ascending cost order.
        assert!(paths[0].cost < paths[1].cost);
        assert!(paths[1].cost < paths[2].cost);
    }

    #[test]
    fn eg144_yen_returns_fewer_than_k_when_only_one_simple_path_exists() {
        // A bare chain a-b-c: only one simple path exists at all.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0)]);
        let (source, target) = (g.index_of(&"a").unwrap(), g.index_of(&"c").unwrap());
        let paths = yen_k_shortest_paths(&g, source, target, 5);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes, vec!["a", "b", "c"]);
    }

    #[test]
    fn eg144_yen_paths_are_loopless_and_distinct() {
        // A small grid-like graph with several alternate routes.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("a", "c", 1.0),
            ("b", "d", 1.0),
            ("c", "d", 1.0),
            ("b", "c", 1.0),
            ("d", "e", 1.0),
            ("c", "e", 2.0),
        ]);
        let (source, target) = (g.index_of(&"a").unwrap(), g.index_of(&"e").unwrap());
        let paths = yen_k_shortest_paths(&g, source, target, 10);
        assert!(!paths.is_empty());

        let mut seen: std::collections::HashSet<Vec<&str>> = std::collections::HashSet::new();
        for p in &paths {
            // Loopless: no repeated node in a single path.
            let unique: std::collections::HashSet<&&str> = p.nodes.iter().collect();
            assert_eq!(
                unique.len(),
                p.nodes.len(),
                "path has a repeated node: {:?}",
                p.nodes
            );
            // Distinct across the whole result set.
            assert!(
                seen.insert(p.nodes.clone()),
                "duplicate path returned: {:?}",
                p.nodes
            );
            // Genuinely starts/ends at source/target.
            assert_eq!(p.nodes.first(), Some(&"a"));
            assert_eq!(p.nodes.last(), Some(&"e"));
        }
        // Non-decreasing cost order.
        for w in paths.windows(2) {
            assert!(w[0].cost <= w[1].cost + 1e-12);
        }
    }

    #[test]
    fn eg144_yen_unreachable_target_is_empty() {
        let g = AdjacencyGraph::from_adjacency([("a", vec![("b", 1.0)]), ("island", vec![])]);
        let (source, target) = (g.index_of(&"a").unwrap(), g.index_of(&"island").unwrap());
        assert!(yen_k_shortest_paths(&g, source, target, 3).is_empty());
    }

    #[test]
    fn eg144_yen_k_zero_is_empty() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
        let (source, target) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!(yen_k_shortest_paths(&g, source, target, 0).is_empty());
    }
}
