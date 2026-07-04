// CONCEPT:EG-KG.compute.graph-data-science-algorithms — Single-source weighted shortest path (Dijkstra) + all-pairs
// via repeated Dijkstra. Neo4j GDS `gds.shortestPath.dijkstra` parity.

use super::graph::AdjacencyGraph;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::Hash;

/// Single-source shortest-path result over compact indices. CONCEPT:EG-KG.compute.graph-data-science-algorithms
#[derive(Debug, Clone)]
pub struct DijkstraResult<N> {
    source: usize,
    /// `dist[i]` = cost of the cheapest path source→i, or `None` if unreachable.
    dist: Vec<Option<f64>>,
    /// `prev[i]` = predecessor of `i` on that path (for reconstruction).
    prev: Vec<Option<usize>>,
    /// Node labels, kept so path reconstruction can return `N`.
    labels: Vec<N>,
}

impl<N> DijkstraResult<N>
where
    N: Clone,
{
    /// Distance to a target index (`None` if unreachable / out of range).
    pub fn distance_to(&self, target: usize) -> Option<f64> {
        self.dist.get(target).copied().flatten()
    }

    /// `(node, distance)` pairs for every reachable node, in node order.
    pub fn distances(&self) -> Vec<(N, f64)> {
        self.labels
            .iter()
            .zip(self.dist.iter())
            .filter_map(|(n, d)| d.map(|d| (n.clone(), d)))
            .collect()
    }

    /// Reconstruct the shortest path source→target as node labels, or `None` if
    /// the target is unreachable.
    pub fn path_to(&self, target: usize) -> Option<Vec<N>> {
        self.dist.get(target).copied().flatten()?;
        let mut path = Vec::new();
        let mut cur = target;
        loop {
            path.push(self.labels[cur].clone());
            if cur == self.source {
                break;
            }
            cur = self.prev[cur]?;
        }
        path.reverse();
        Some(path)
    }
}

// Min-heap entry ordered by (distance, node) — the node tie-break makes the
// traversal deterministic.
#[derive(PartialEq)]
struct HeapItem {
    dist: f64,
    node: usize,
}
impl Eq for HeapItem {}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed for a min-heap on distance; node id breaks ties (also reversed
        // so the smallest id is popped first).
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.node.cmp(&self.node))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Dijkstra single-source shortest paths over non-negative edge weights.
///
/// Panics-free on negative weights but results are undefined (Dijkstra's
/// precondition); callers with negative weights want Bellman–Ford instead.
///
/// Deterministic: equal-distance frontier nodes are settled in ascending index
/// order.
///
/// Complexity: `O((V + E) log V)`. CONCEPT:EG-KG.compute.graph-data-science-algorithms
pub fn dijkstra<N>(graph: &AdjacencyGraph<N>, source: usize) -> DijkstraResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let mut dist: Vec<Option<f64>> = vec![None; n];
    let mut prev: Vec<Option<usize>> = vec![None; n];

    if source < n {
        dist[source] = Some(0.0);
        let mut heap = BinaryHeap::new();
        heap.push(HeapItem {
            dist: 0.0,
            node: source,
        });
        while let Some(HeapItem { dist: d, node: u }) = heap.pop() {
            // Skip stale heap entries.
            if let Some(best) = dist[u] {
                if d > best {
                    continue;
                }
            }
            for &(v, w) in graph.out_edges(u) {
                let nd = d + w;
                let better = match dist[v] {
                    None => true,
                    Some(old) => nd < old,
                };
                if better {
                    dist[v] = Some(nd);
                    prev[v] = Some(u);
                    heap.push(HeapItem { dist: nd, node: v });
                }
            }
        }
    }

    DijkstraResult {
        source,
        dist,
        prev,
        labels: graph.nodes().to_vec(),
    }
}

/// Convenience: shortest path between two node *labels*, returned as labels.
/// `None` if either endpoint is absent or the target is unreachable.
/// CONCEPT:EG-KG.compute.graph-data-science-algorithms
pub fn shortest_path<N>(graph: &AdjacencyGraph<N>, source: &N, target: &N) -> Option<Vec<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let s = graph.index_of(source)?;
    let t = graph.index_of(target)?;
    dijkstra(graph, s).path_to(t)
}

/// All-pairs shortest-path distance matrix via repeated Dijkstra.
///
/// `matrix[i][j]` = cost source-i → node-j (`None` if unreachable). Row/column
/// order is the graph's node (sorted-id) order.
///
/// Complexity: `O(V · (V + E) log V)`. CONCEPT:EG-KG.compute.graph-data-science-algorithms
pub fn all_pairs_shortest_paths<N>(graph: &AdjacencyGraph<N>) -> Vec<Vec<Option<f64>>>
where
    N: Clone + Eq + Hash + Ord,
{
    (0..graph.node_count())
        .map(|s| dijkstra(graph, s).dist)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_dijkstra_weighted_path() {
        // a→b (1) →c (2) →d (1) = 4, vs direct a→d (10). Cheapest = via chain.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 2.0),
            ("c", "d", 1.0),
            ("a", "d", 10.0),
        ]);
        let a = g.index_of(&"a").unwrap();
        let res = dijkstra(&g, a);
        let d = g.index_of(&"d").unwrap();
        assert_eq!(res.distance_to(d), Some(4.0));
        assert_eq!(res.path_to(d), Some(vec!["a", "b", "c", "d"]));
    }

    #[test]
    fn eg144_dijkstra_picks_cheaper_of_two_routes() {
        // a→b→d cost 2+2=4 vs a→c→d cost 1+1=2. Shortest = via c.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 2.0),
            ("b", "d", 2.0),
            ("a", "c", 1.0),
            ("c", "d", 1.0),
        ]);
        let path = shortest_path(&g, &"a", &"d").unwrap();
        assert_eq!(path, vec!["a", "c", "d"]);
    }

    #[test]
    fn eg144_dijkstra_unreachable_is_none() {
        // "island" is an isolated node with no edges.
        let g = AdjacencyGraph::from_adjacency([("a", vec![("b", 1.0)]), ("island", vec![])]);
        let a = g.index_of(&"a").unwrap();
        let island = g.index_of(&"island").unwrap();
        let res = dijkstra(&g, a);
        assert_eq!(res.distance_to(island), None);
        assert!(res.path_to(island).is_none());
    }

    #[test]
    fn eg144_all_pairs_distance_matrix() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0)]);
        let (a, b, c) = (
            g.index_of(&"a").unwrap(),
            g.index_of(&"b").unwrap(),
            g.index_of(&"c").unwrap(),
        );
        let m = all_pairs_shortest_paths(&g);
        assert_eq!(m[a][c], Some(2.0));
        assert_eq!(m[b][c], Some(1.0));
        assert_eq!(m[c][a], None); // directed, no back path
    }
}
