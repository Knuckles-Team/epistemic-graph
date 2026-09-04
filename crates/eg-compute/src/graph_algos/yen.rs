// CONCEPT:EG-KG.compute.yens-k-shortest-paths — Yen's algorithm for the k shortest LOOPLESS
// (simple) paths between two nodes. Neo4j GDS `gds.shortestPath.yens` parity.

use super::graph::AdjacencyGraph;
use super::shortest_path::MinHeapItem;
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

/// Dijkstra restricted to skip a set of blocked nodes/edges entirely — the
/// "spur search" primitive Yen's algorithm re-runs for every candidate. A
/// fresh, self-contained search rather than mutating [`AdjacencyGraph`] (which
/// is an immutable, once-built value everywhere else in this crate).
fn restricted_shortest_path<N>(
    graph: &AdjacencyGraph<N>,
    source: usize,
    target: usize,
    blocked: &Blocked<'_>,
) -> Option<(Vec<usize>, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    if blocked.nodes.contains(&source) || blocked.nodes.contains(&target) {
        return None;
    }
    let n = graph.node_count();
    let mut dist: Vec<Option<f64>> = vec![None; n];
    let mut prev: Vec<Option<usize>> = vec![None; n];
    dist[source] = Some(0.0);

    let mut heap = BinaryHeap::new();
    heap.push(MinHeapItem {
        dist: 0.0,
        node: source,
    });
    while let Some(MinHeapItem { dist: d, node: u }) = heap.pop() {
        if matches!(dist[u], Some(best) if d > best) {
            continue;
        }
        if u == target {
            return reconstruct_path(source, u, &prev).map(|path| (path, d));
        }
        relax_neighbors(graph, u, d, blocked, &mut dist, &mut prev, &mut heap);
    }
    None
}

fn reconstruct_path(
    source: usize,
    target: usize,
    previous: &[Option<usize>],
) -> Option<Vec<usize>> {
    let mut path = vec![target];
    let mut current = target;
    while current != source {
        current = previous[current]?;
        path.push(current);
    }
    path.reverse();
    Some(path)
}

/// The exclusion set one Yen spur search runs against: the root-path nodes and
/// the edges already committed by an accepted path. The two always travel
/// together, so they are passed as one value.
struct Blocked<'a> {
    nodes: &'a BTreeSet<usize>,
    edges: &'a BTreeSet<(usize, usize)>,
}

fn relax_neighbors<N>(
    graph: &AdjacencyGraph<N>,
    source: usize,
    source_distance: f64,
    blocked: &Blocked<'_>,
    distances: &mut [Option<f64>],
    predecessors: &mut [Option<usize>],
    heap: &mut BinaryHeap<MinHeapItem>,
) where
    N: Clone + Eq + Hash + Ord,
{
    for &(target, weight) in graph.out_edges(source) {
        if blocked.nodes.contains(&target) || blocked.edges.contains(&(source, target)) {
            continue;
        }
        let candidate = source_distance + weight;
        let better = match distances[target] {
            None => true,
            Some(old) => candidate < old,
        };
        if better {
            distances[target] = Some(candidate);
            predecessors[target] = Some(source);
            heap.push(MinHeapItem {
                dist: candidate,
                node: target,
            });
        }
    }
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
    let unblocked = Blocked {
        nodes: &empty_nodes,
        edges: &empty_edges,
    };
    let Some(first) = restricted_shortest_path(graph, source, target, &unblocked) else {
        return Vec::new();
    };

    let mut a: Vec<(Vec<usize>, f64)> = vec![first];
    // Candidate pool: a small `Vec`, sorted explicitly each round rather than a
    // `BinaryHeap` — `k` and the candidate count per round are both small in
    // practice, and a plain sort keeps the tie-break trivially deterministic.
    let mut b: Vec<(f64, Vec<usize>)> = Vec::new();

    while a.len() < k {
        let prev_path = a.last().unwrap().0.clone();
        collect_spur_candidates(graph, &a, &prev_path, target, &mut b);
        let Some((cost, path)) = take_best_candidate(&mut b) else {
            break;
        };
        a.push((path, cost));
    }

    a.into_iter()
        .map(|(path, cost)| RankedPath {
            nodes: path.into_iter().map(|i| graph.node_at(i).clone()).collect(),
            cost,
        })
        .collect()
}

fn collect_spur_candidates<N>(
    graph: &AdjacencyGraph<N>,
    accepted: &[(Vec<usize>, f64)],
    previous_path: &[usize],
    target: usize,
    candidates: &mut Vec<(f64, Vec<usize>)>,
) where
    N: Clone + Eq + Hash + Ord,
{
    for spur_index in 0..previous_path.len().saturating_sub(1) {
        let Some(candidate) = spur_candidate(graph, accepted, previous_path, spur_index, target)
        else {
            continue;
        };
        if !contains_path(accepted, candidates, &candidate.1) {
            candidates.push(candidate);
        }
    }
}

fn spur_candidate<N>(
    graph: &AdjacencyGraph<N>,
    accepted: &[(Vec<usize>, f64)],
    previous_path: &[usize],
    spur_index: usize,
    target: usize,
) -> Option<(f64, Vec<usize>)>
where
    N: Clone + Eq + Hash + Ord,
{
    let spur_node = previous_path[spur_index];
    let root_path = &previous_path[..=spur_index];
    let blocked_edges = blocked_edges_for_prefix(accepted, root_path, spur_index);
    let blocked_nodes = root_path[..spur_index].iter().copied().collect();
    let (spur_path, _) = restricted_shortest_path(
        graph,
        spur_node,
        target,
        &Blocked {
            nodes: &blocked_nodes,
            edges: &blocked_edges,
        },
    )?;
    let mut total_path = root_path[..spur_index].to_vec();
    total_path.extend(spur_path);
    let total_cost = path_cost(graph, &total_path);
    Some((total_cost, total_path))
}

fn blocked_edges_for_prefix(
    accepted: &[(Vec<usize>, f64)],
    root_path: &[usize],
    spur_index: usize,
) -> BTreeSet<(usize, usize)> {
    accepted
        .iter()
        .filter(|(path, _)| path.len() > spur_index + 1 && &path[..=spur_index] == root_path)
        .map(|(path, _)| (path[spur_index], path[spur_index + 1]))
        .collect()
}

fn contains_path(
    accepted: &[(Vec<usize>, f64)],
    candidates: &[(f64, Vec<usize>)],
    path: &[usize],
) -> bool {
    accepted.iter().any(|(known, _)| known == path)
        || candidates.iter().any(|(_, known)| known == path)
}

fn take_best_candidate(candidates: &mut Vec<(f64, Vec<usize>)>) -> Option<(f64, Vec<usize>)> {
    if candidates.is_empty() {
        return None;
    }
    candidates.sort_by(|left, right| {
        left.0
            .partial_cmp(&right.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.1.cmp(&right.1))
    });
    Some(candidates.remove(0))
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
