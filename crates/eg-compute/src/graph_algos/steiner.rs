// CONCEPT:EG-KG.compute.steiner-tree — Steiner tree via the classical MST-based
// 2-approximation (Kou, Markowsky & Berman 1981). Neo4j GDS `gds.steinerTree` parity.
//
// Given a `root` and a set of required `terminals`, finds a low-weight tree
// connecting all of them (possibly through extra, non-terminal "Steiner
// points") over the graph's UNDIRECTED symmetrisation (Steiner tree is a
// classical undirected-graph problem, matching the convention
// [`super::louvain::louvain`]/[`super::leiden::leiden`] already use for
// structural algorithms in this crate).
//
// **The Kou–Markowsky–Berman (KMB) algorithm, exactly as implemented here:**
//
//   1. Compute all-pairs shortest paths BETWEEN every terminal (one Dijkstra
//      run per terminal over the undirected symmetrisation).
//   2. Build the **metric closure**: a complete graph over the terminals whose
//      edge weight is that pairwise shortest-path DISTANCE.
//   3. Take a Minimum Spanning Tree of the metric closure (Kruskal's
//      algorithm).
//   4. **Expand** each MST edge back into its real shortest PATH in the
//      original graph, and take the UNION of all those paths' edges as a
//      subgraph (a real edge counted once even if multiple expanded paths
//      reuse it).
//   5. Take an MST of THAT union subgraph — this is what turns a
//      possibly-cyclic union of overlapping paths into a genuine TREE.
//   6. Iteratively prune any non-terminal LEAF (a dead-end that doesn't help
//      connect two terminals) until none remain.
//
// **Approximation ratio: at most 2× the true Steiner-tree optimum** — the
// well-known, classical result for this exact construction (Kou, Markowsky &
// Berman, "A fast algorithm for Steiner trees", *Acta Informatica* 15(2),
// 1981). **Honest limitation, by design, not a bug:** step 2's metric closure
// only ever sees PAIRWISE shortest distances between terminals — it cannot
// discover a shared Steiner point that is not on any single pair's own
// shortest path but would still be cheaper for connecting all of them
// together (a classical example is tested below). That gap between "cheapest
// per pair" and "cheapest overall" is exactly why this is a 2-approximation
// and not an exact algorithm.

use super::components::UnionFind;
use super::graph::AdjacencyGraph;
use super::shortest_path::MinHeapItem;
use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::hash::Hash;

/// One tree row: `(node, parent, parent-edge weight)` — `parent`/`weight` are
/// `None` for the root. CONCEPT:EG-KG.compute.steiner-tree
type TreeNode<N> = (N, Option<N>, Option<f64>);

/// Result of a [`steiner_tree`] run. CONCEPT:EG-KG.compute.steiner-tree
#[derive(Debug, Clone)]
pub struct SteinerTreeResult<N> {
    /// Every node included in the tree — `root` itself plus every reached
    /// terminal plus any Steiner points needed to connect them — each paired
    /// with its parent (the neighbour closer to `root`) and that parent
    /// edge's weight. `root` maps to `(root, None, None)`. Sorted by node.
    pub nodes: Vec<TreeNode<N>>,
    /// Sum of the tree's edge weights.
    pub total_weight: f64,
    /// Requested terminals that turned out unreachable from `root` (omitted
    /// from the tree rather than failing the whole call — the same
    /// "no match ⇒ degrade gracefully" contract the rest of this engine uses).
    pub unreached_terminals: Vec<N>,
}

/// Dijkstra over a raw symmetric weighted adjacency (the undirected
/// symmetrisation), returning `(distance, predecessor)` arrays.
fn dijkstra_raw(
    adj: &[Vec<(usize, f64)>],
    source: usize,
) -> (Vec<Option<f64>>, Vec<Option<usize>>) {
    let n = adj.len();
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
        for &(v, w) in &adj[u] {
            let nd = d + w;
            let better = match dist[v] {
                None => true,
                Some(old) => nd < old,
            };
            if better {
                dist[v] = Some(nd);
                prev[v] = Some(u);
                heap.push(MinHeapItem { dist: nd, node: v });
            }
        }
    }
    (dist, prev)
}

fn edge_weight_undirected(adj: &[Vec<(usize, f64)>], u: usize, v: usize) -> f64 {
    adj[u]
        .binary_search_by_key(&v, |&(t, _)| t)
        .map(|i| adj[u][i].1)
        .unwrap_or(0.0)
}

/// Iteratively remove any non-terminal, non-root leaf until none remain.
fn prune_non_terminal_leaves(
    tree_edges: &mut Vec<(usize, usize, f64)>,
    terminals: &BTreeSet<usize>,
    root: usize,
) {
    loop {
        let mut degree: HashMap<usize, usize> = HashMap::new();
        for &(a, b, _) in tree_edges.iter() {
            *degree.entry(a).or_insert(0) += 1;
            *degree.entry(b).or_insert(0) += 1;
        }
        let is_prunable_leaf = |n: usize| {
            degree.get(&n).copied().unwrap_or(0) == 1 && n != root && !terminals.contains(&n)
        };
        let before = tree_edges.len();
        tree_edges.retain(|&(a, b, _)| !(is_prunable_leaf(a) || is_prunable_leaf(b)));
        if tree_edges.len() == before {
            break;
        }
    }
}

/// BFS-orient the final undirected tree edges into (node, parent, edge
/// weight) rows rooted at `root`, sorted by node for determinism.
fn orient_tree<N>(
    graph: &AdjacencyGraph<N>,
    tree_edges: &[(usize, usize, f64)],
    root: usize,
) -> (Vec<TreeNode<N>>, f64)
where
    N: Clone + Eq + Hash + Ord,
{
    let mut adj: HashMap<usize, Vec<(usize, f64)>> = HashMap::new();
    let mut total = 0.0;
    for &(a, b, w) in tree_edges {
        adj.entry(a).or_default().push((b, w));
        adj.entry(b).or_default().push((a, w));
        total += w;
    }

    let mut visited: BTreeSet<usize> = BTreeSet::new();
    let mut out: Vec<(N, Option<N>, Option<f64>)> = Vec::new();
    let mut queue: VecDeque<usize> = VecDeque::new();
    queue.push_back(root);
    visited.insert(root);
    out.push((graph.node_at(root).clone(), None, None));

    while let Some(u) = queue.pop_front() {
        let mut nbrs = adj.get(&u).cloned().unwrap_or_default();
        nbrs.sort_by_key(|&(v, _)| v); // deterministic expansion order
        for (v, w) in nbrs {
            if visited.insert(v) {
                out.push((
                    graph.node_at(v).clone(),
                    Some(graph.node_at(u).clone()),
                    Some(w),
                ));
                queue.push_back(v);
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    (out, total)
}

/// Steiner tree connecting `root` and every reachable node in `terminals` —
/// see the module doc for the exact KMB construction and its 2-approximation
/// ratio. Unreachable terminals are reported, not fatal.
///
/// Complexity: `O(T·(V+E)logV + T²logT)` where `T = |terminals ∪ {root}|` —
/// `T` Dijkstra runs plus two Kruskal MST passes over at-most-`T²`-edge and
/// at-most-`(T·pathlen)`-edge graphs respectively; the standard textbook bound
/// for the KMB construction. CONCEPT:EG-KG.compute.steiner-tree
pub fn steiner_tree<N>(
    graph: &AdjacencyGraph<N>,
    root: usize,
    terminals: &[usize],
) -> SteinerTreeResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 || root >= n {
        return SteinerTreeResult {
            nodes: Vec::new(),
            total_weight: 0.0,
            unreached_terminals: Vec::new(),
        };
    }

    let mut wanted: BTreeSet<usize> = terminals.iter().copied().filter(|&t| t < n).collect();
    wanted.insert(root);

    if wanted.len() <= 1 {
        return SteinerTreeResult {
            nodes: vec![(graph.node_at(root).clone(), None, None)],
            total_weight: 0.0,
            unreached_terminals: Vec::new(),
        };
    }

    let adj = graph.undirected_weighted_adjacency();
    let (dist_from, prev_from) = terminal_shortest_paths(&adj, &wanted);
    let (reachable, unreached) = reachable_terminals(root, &wanted, &dist_from);

    if reachable.len() <= 1 {
        return SteinerTreeResult {
            nodes: vec![(graph.node_at(root).clone(), None, None)],
            total_weight: 0.0,
            unreached_terminals: unreached
                .into_iter()
                .map(|i| graph.node_at(i).clone())
                .collect(),
        };
    }

    let closure_mst = metric_closure_mst(n, &reachable, &dist_from);
    let union_edges = expand_closure_paths(&adj, &prev_from, &closure_mst);
    let mut tree_edges = union_mst(n, union_edges);

    let terminal_set: BTreeSet<usize> = reachable.iter().copied().collect();
    prune_non_terminal_leaves(&mut tree_edges, &terminal_set, root);

    let (nodes, total_weight) = orient_tree(graph, &tree_edges, root);
    SteinerTreeResult {
        nodes,
        total_weight,
        unreached_terminals: unreached
            .into_iter()
            .map(|i| graph.node_at(i).clone())
            .collect(),
    }
}

/// Per-terminal Dijkstra output: distance rows and predecessor rows, each keyed
/// by the terminal they were computed from.
type TerminalPaths = (
    HashMap<usize, Vec<Option<f64>>>,
    HashMap<usize, Vec<Option<usize>>>,
);

fn terminal_shortest_paths(
    adj: &[Vec<(usize, f64)>],
    terminals: &BTreeSet<usize>,
) -> TerminalPaths {
    let mut distances = HashMap::new();
    let mut predecessors = HashMap::new();
    for &terminal in terminals {
        let (distance, predecessor) = dijkstra_raw(adj, terminal);
        distances.insert(terminal, distance);
        predecessors.insert(terminal, predecessor);
    }
    (distances, predecessors)
}

fn reachable_terminals(
    root: usize,
    wanted: &BTreeSet<usize>,
    distances: &HashMap<usize, Vec<Option<f64>>>,
) -> (Vec<usize>, Vec<usize>) {
    let root_distances = &distances[&root];
    let mut reachable = Vec::new();
    let mut unreached = Vec::new();
    for &terminal in wanted {
        if terminal == root || root_distances[terminal].is_some() {
            reachable.push(terminal);
        } else {
            unreached.push(terminal);
        }
    }
    (reachable, unreached)
}

fn metric_closure_mst(
    node_count: usize,
    terminals: &[usize],
    distances: &HashMap<usize, Vec<Option<f64>>>,
) -> Vec<(usize, usize)> {
    let mut closure_edges = Vec::new();
    for i in 0..terminals.len() {
        for j in (i + 1)..terminals.len() {
            let (a, b) = (terminals[i], terminals[j]);
            if let Some(distance) = distances[&a][b] {
                closure_edges.push((distance, a, b));
            }
        }
    }
    sort_weighted_edges(&mut closure_edges);
    kruskal_pairs(node_count, closure_edges)
}

fn sort_weighted_edges(edges: &mut [(f64, usize, usize)]) {
    edges.sort_by(|left, right| {
        left.0
            .partial_cmp(&right.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
}

fn kruskal_pairs(node_count: usize, edges: Vec<(f64, usize, usize)>) -> Vec<(usize, usize)> {
    let mut union_find = UnionFind::new(node_count);
    let mut tree = Vec::new();
    for (_weight, left, right) in edges {
        if union_find.find(left) != union_find.find(right) {
            union_find.union(left, right);
            tree.push((left, right));
        }
    }
    tree
}

fn expand_closure_paths(
    adj: &[Vec<(usize, f64)>],
    predecessors: &HashMap<usize, Vec<Option<usize>>>,
    closure_mst: &[(usize, usize)],
) -> HashMap<(usize, usize), f64> {
    let mut union_edges = HashMap::new();
    for &(source, target) in closure_mst {
        let previous = &predecessors[&source];
        let mut current = target;
        while current != source {
            let predecessor =
                previous[current].expect("connected within the same component by construction");
            let edge = if predecessor < current {
                (predecessor, current)
            } else {
                (current, predecessor)
            };
            union_edges.insert(edge, edge_weight_undirected(adj, predecessor, current));
            current = predecessor;
        }
    }
    union_edges
}

fn union_mst(
    node_count: usize,
    union_edges: HashMap<(usize, usize), f64>,
) -> Vec<(usize, usize, f64)> {
    let mut edges: Vec<(f64, usize, usize)> = union_edges
        .into_iter()
        .map(|((left, right), weight)| (weight, left, right))
        .collect();
    sort_weighted_edges(&mut edges);
    let mut union_find = UnionFind::new(node_count);
    let mut tree = Vec::new();
    for (weight, left, right) in edges {
        if union_find.find(left) != union_find.find(right) {
            union_find.union(left, right);
            tree.push((left, right, weight));
        }
    }
    tree
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_steiner_star_is_its_own_three_direct_spokes() {
        // Hub h with 3 terminal spokes of DIFFERENT weight — the optimal tree
        // is trivially the 3 direct edges themselves.
        let g = AdjacencyGraph::from_edges([("h", "t1", 3.0), ("h", "t2", 4.0), ("h", "t3", 5.0)]);
        let h = g.index_of(&"h").unwrap();
        let t1 = g.index_of(&"t1").unwrap();
        let t2 = g.index_of(&"t2").unwrap();
        let t3 = g.index_of(&"t3").unwrap();
        let res = steiner_tree(&g, h, &[t1, t2, t3]);

        assert!(
            (res.total_weight - 12.0).abs() < 1e-9,
            "{}",
            res.total_weight
        );
        assert!(res.unreached_terminals.is_empty());
        let m: HashMap<&str, (Option<&str>, Option<f64>)> =
            res.nodes.into_iter().map(|(n, p, w)| (n, (p, w))).collect();
        assert_eq!(m["h"], (None, None));
        assert_eq!(m["t1"], (Some("h"), Some(3.0)));
        assert_eq!(m["t2"], (Some("h"), Some(4.0)));
        assert_eq!(m["t3"], (Some("h"), Some(5.0)));
    }

    #[test]
    fn eg144_steiner_routes_through_a_point_that_is_on_the_shortest_path() {
        // The ONLY way to connect t1/t2/t3 is via hub h (no direct edges) —
        // proves the algorithm genuinely discovers and uses a non-terminal
        // Steiner point when it lies on the pairwise shortest paths.
        let g = AdjacencyGraph::from_edges([("h", "t1", 6.0), ("h", "t2", 6.0), ("h", "t3", 6.0)]);
        let t1 = g.index_of(&"t1").unwrap();
        let t2 = g.index_of(&"t2").unwrap();
        let t3 = g.index_of(&"t3").unwrap();
        let res = steiner_tree(&g, t1, &[t2, t3]);

        assert!(
            (res.total_weight - 18.0).abs() < 1e-9,
            "{}",
            res.total_weight
        );
        let ids: std::collections::HashSet<&str> = res.nodes.iter().map(|(n, _, _)| *n).collect();
        assert!(ids.contains("h"), "the Steiner point must be included");
        assert_eq!(ids.len(), 4);
    }

    #[test]
    fn eg144_steiner_is_a_2_approximation_not_exact_by_design() {
        // The classical KMB counterexample: pairwise DIRECT edges (10 each)
        // are individually cheaper than routing through hub h (6+6=12 per
        // pair), so the metric closure — which only ever sees PAIRWISE
        // distances — picks 2 direct edges (total 20) and never discovers
        // that routing ALL THREE through h costs only 18 in total. This is
        // the documented, honest gap that makes KMB a 2-approximation rather
        // than an exact algorithm.
        let g = AdjacencyGraph::from_edges([
            ("t1", "t2", 10.0),
            ("t2", "t3", 10.0),
            ("t1", "t3", 10.0),
            ("t1", "h", 6.0),
            ("t2", "h", 6.0),
            ("t3", "h", 6.0),
        ]);
        let t1 = g.index_of(&"t1").unwrap();
        let t2 = g.index_of(&"t2").unwrap();
        let t3 = g.index_of(&"t3").unwrap();
        let res = steiner_tree(&g, t1, &[t2, t3]);

        // This implementation finds the 20-cost direct-edge tree (verifying
        // the DOCUMENTED, real behaviour of the construction)...
        assert!(
            (res.total_weight - 20.0).abs() < 1e-9,
            "{}",
            res.total_weight
        );
        // ...which is provably NOT optimal (the true optimum, via the hub, is
        // 18) yet is still comfortably within the documented 2× bound.
        let true_optimum = 18.0;
        assert!(res.total_weight > true_optimum, "this case must be inexact");
        assert!(res.total_weight <= 2.0 * true_optimum + 1e-9);
    }

    #[test]
    fn eg144_steiner_unreachable_terminal_is_reported_not_fatal() {
        let g = AdjacencyGraph::from_adjacency([("root", vec![("t1", 1.0)]), ("island", vec![])]);
        let root = g.index_of(&"root").unwrap();
        let t1 = g.index_of(&"t1").unwrap();
        let island = g.index_of(&"island").unwrap();
        let res = steiner_tree(&g, root, &[t1, island]);

        assert_eq!(res.unreached_terminals, vec!["island"]);
        assert!((res.total_weight - 1.0).abs() < 1e-9);
        let ids: std::collections::HashSet<&str> = res.nodes.iter().map(|(n, _, _)| *n).collect();
        assert!(!ids.contains("island"));
        assert!(ids.contains("root") && ids.contains("t1"));
    }

    #[test]
    fn eg144_steiner_no_terminals_is_just_the_root() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
        let a = g.index_of(&"a").unwrap();
        let res = steiner_tree(&g, a, &[]);
        assert_eq!(res.nodes, vec![("a", None, None)]);
        assert_eq!(res.total_weight, 0.0);
    }
}
