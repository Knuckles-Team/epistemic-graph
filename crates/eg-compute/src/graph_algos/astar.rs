// CONCEPT:EG-KG.compute.astar-search — A* shortest path with a caller-supplied heuristic.
// Neo4j GDS `gds.shortestPath.astar` parity.

use super::graph::AdjacencyGraph;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::Hash;

// Min-heap entry ordered by (f = g+h, node) — mirrors `shortest_path::HeapItem`'s
// reversed-Ord + ascending-node-id tie-break idiom. Carries `g` alongside `f` so
// a popped entry can be checked for staleness against the CURRENT best `g_score`
// (an entry pushed before a later, cheaper path to the same node was found) —
// this makes the search correct even for an admissible-but-INCONSISTENT
// heuristic (a node may be "reopened" if a cheaper path surfaces later), not
// just the more common consistent case.
#[derive(PartialEq)]
struct AStarHeapItem {
    f: f64,
    g: f64,
    node: usize,
}
impl Eq for AStarHeapItem {}
impl Ord for AStarHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .f
            .partial_cmp(&self.f)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.node.cmp(&self.node))
    }
}
impl PartialOrd for AStarHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A* shortest path from `source` to `target`, guided by a caller-supplied
/// `heuristic(node) -> estimated remaining cost to target`.
///
/// **Precondition (caller's responsibility, like GDS's own A*):** the
/// heuristic should be *admissible* — never overestimate the true remaining
/// cost — for the returned path to be guaranteed optimal. A non-admissible
/// heuristic does not panic or infinite-loop; it simply may return a
/// non-optimal (but still valid) path. `heuristic = |_| 0.0` makes this
/// degrade to exactly [`super::shortest_path::dijkstra`] (zero is trivially
/// admissible, and carries no information at all) — see the module tests for
/// this exact cross-check.
///
/// Returns `(path, total_cost)` as node labels in source→target order, or
/// `None` if `target` is unreachable (or either index is out of range).
///
/// Complexity: `O((V+E)logV)` worst case — the same bound as `dijkstra`; a good
/// heuristic prunes the PRACTICAL search space but the worst-case bound is
/// unchanged (documented honestly, matching how this crate documents other
/// worst-case bounds regardless of typical-case speedups).
/// CONCEPT:EG-KG.compute.astar-search
pub fn a_star<N>(
    graph: &AdjacencyGraph<N>,
    source: usize,
    target: usize,
    heuristic: impl Fn(usize) -> f64,
) -> Option<(Vec<N>, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if source >= n || target >= n {
        return None;
    }

    let mut g_score: Vec<Option<f64>> = vec![None; n];
    let mut prev: Vec<Option<usize>> = vec![None; n];
    g_score[source] = Some(0.0);

    let mut heap = BinaryHeap::new();
    heap.push(AStarHeapItem {
        f: heuristic(source),
        g: 0.0,
        node: source,
    });

    while let Some(AStarHeapItem { f: _, g, node: u }) = heap.pop() {
        if matches!(g_score[u], Some(best) if g > best) {
            continue; // stale heap entry: a cheaper path to `u` was found since
        }
        if u == target {
            let mut path = Vec::new();
            let mut cur = u;
            loop {
                path.push(graph.node_at(cur).clone());
                if cur == source {
                    break;
                }
                cur = prev[cur]?;
            }
            path.reverse();
            return Some((path, g));
        }
        for &(v, w) in graph.out_edges(u) {
            let tentative = g + w;
            let better = match g_score[v] {
                None => true,
                Some(old) => tentative < old,
            };
            if better {
                g_score[v] = Some(tentative);
                prev[v] = Some(u);
                heap.push(AStarHeapItem {
                    f: tentative + heuristic(v),
                    g: tentative,
                    node: v,
                });
            }
        }
    }
    None
}

/// Great-circle distance between two `(lat, lon)` points in DEGREES, via the
/// haversine formula, in kilometres. A standard caller-supplied `a_star`
/// heuristic when edge weights are real-world distances in the SAME unit
/// (km): haversine never overestimates the true shortest path between two
/// points (it IS the shortest possible distance on a sphere) — exactly the
/// admissibility `a_star` needs for its optimality guarantee.
/// CONCEPT:EG-KG.compute.astar-search
pub fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0088;
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let d_phi = (lat2 - lat1).to_radians();
    let d_lambda = (lon2 - lon1).to_radians();
    let a = (d_phi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (d_lambda / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().clamp(0.0, 1.0).asin();
    EARTH_RADIUS_KM * c
}

#[cfg(test)]
mod tests {
    use super::super::shortest_path::dijkstra;
    use super::*;

    #[test]
    fn eg144_astar_haversine_zero_distance_and_quarter_globe() {
        assert!((haversine_km(40.0, -74.0, 40.0, -74.0) - 0.0).abs() < 1e-9);
        // Two equator points 90 degrees of longitude apart ⇒ a quarter of the
        // Earth's circumference, ≈ π·R/2 ≈ 10007.5 km.
        let quarter = haversine_km(0.0, 0.0, 0.0, 90.0);
        assert!((quarter - 10007.5).abs() < 1.0, "got {quarter}");
    }

    #[test]
    fn eg144_astar_matches_dijkstra_with_admissible_haversine_heuristic() {
        // Four points along the equator, 1 degree of longitude apart (≈111.19
        // km each): a chain a-b-c-d (150 each way, total 450) PLUS a direct
        // a-d shortcut (400) — a genuine choice between two routes, like
        // dijkstra's own "picks cheaper of two routes" test. Haversine to the
        // target is admissible at every node (verified: 333.6≤400 at a,
        // 222.4≤300 at b, 111.2≤150 at c), so A* is guaranteed to find the
        // SAME optimum as dijkstra.
        let coords: [(&str, f64, f64); 4] = [
            ("a", 0.0, 0.0),
            ("b", 0.0, 1.0),
            ("c", 0.0, 2.0),
            ("d", 0.0, 3.0),
        ];
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 150.0),
            ("b", "c", 150.0),
            ("c", "d", 150.0),
            ("a", "d", 400.0),
        ]);
        let coord_of = |idx: usize| {
            let id = *g.node_at(idx);
            coords
                .iter()
                .find(|(n, _, _)| *n == id)
                .map(|(_, lat, lon)| (*lat, *lon))
                .unwrap()
        };
        let source = g.index_of(&"a").unwrap();
        let target = g.index_of(&"d").unwrap();
        let (t_lat, t_lon) = coord_of(target);
        let heuristic = |v: usize| {
            let (lat, lon) = coord_of(v);
            haversine_km(lat, lon, t_lat, t_lon)
        };

        let (path, cost) = a_star(&g, source, target, heuristic).expect("reachable");
        let dijkstra_res = dijkstra(&g, source);
        assert_eq!(Some(cost), dijkstra_res.distance_to(target));
        assert_eq!(Some(path.clone()), dijkstra_res.path_to(target));
        // The direct 1-edge shortcut (400) beats the 3-edge chain (450).
        assert_eq!(path, vec!["a", "d"]);
        assert!((cost - 400.0).abs() < 1e-9);
    }

    #[test]
    fn eg144_astar_zero_heuristic_degrades_exactly_to_dijkstra() {
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 2.0),
            ("b", "d", 2.0),
            ("a", "c", 1.0),
            ("c", "d", 1.0),
        ]);
        let (source, target) = (g.index_of(&"a").unwrap(), g.index_of(&"d").unwrap());
        let (path, cost) = a_star(&g, source, target, |_| 0.0).expect("reachable");
        let dr = dijkstra(&g, source);
        assert_eq!(Some(cost), dr.distance_to(target));
        assert_eq!(Some(path), dr.path_to(target));
    }

    #[test]
    fn eg144_astar_unreachable_target_is_none() {
        let g = AdjacencyGraph::from_adjacency([("a", vec![("b", 1.0)]), ("island", vec![])]);
        let (source, target) = (g.index_of(&"a").unwrap(), g.index_of(&"island").unwrap());
        assert!(a_star(&g, source, target, |_| 0.0).is_none());
    }

    #[test]
    fn eg144_astar_out_of_range_index_is_none() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
        assert!(a_star(&g, 0, 99, |_| 0.0).is_none());
        assert!(a_star(&g, 99, 0, |_| 0.0).is_none());
    }
}
