//! **Weighted spatial-network routing** — shortest path, isochrones and TSP tours
//! (CONCEPT:EG-266).
//!
//! The logistics / urban-planning surface needs to *route over a network*: a road graph, a
//! delivery grid, a transit map. This module models a weighted spatial network ([`Network`])
//! — nodes are [`Point`] locations, edges are weighted directed segments — and answers the
//! three questions a routing engine is asked:
//!
//! * **Shortest path** — [`Network::dijkstra`] (uniform-cost) and [`Network::astar`]
//!   (A\* with a caller-supplied admissible heuristic; [`Network::astar_greatcircle`] wires
//!   the great-circle / Haversine heuristic for lon/lat networks). Both return a [`Path`]
//!   (ordered node ids + total cost).
//! * **Isochrone** — [`Network::isochrone`]: the set of nodes reachable from a source within
//!   a cost *budget* (a one-to-many Dijkstra stopped at the budget), the backbone of
//!   "everything within 15 minutes' drive".
//! * **TSP tour** — [`solve_tsp`]: a nearest-neighbour seed refined by 2-opt, over a
//!   distance matrix ([`distance_matrix`] builds one from points with any metric); the
//!   classic vehicle-routing / multi-stop-delivery ordering heuristic.
//!
//! Pure-Rust, dependency-free (only a `BinaryHeap` from `std`), reusing EG-256's geodesic
//! [`haversine_distance`](crate::geodesic::haversine_distance) for great-circle costs/heuristics.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::geodesic::haversine_distance;
use crate::geometry::Point;

/// A weighted directed edge to node `to` with non-negative `weight` (CONCEPT:EG-266).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Edge {
    pub to: usize,
    pub weight: f64,
}

/// A weighted spatial network (CONCEPT:EG-266): nodes carry a [`Point`] location, edges are
/// directed weighted links held in an adjacency list. Build it with [`Network::add_node`]
/// and [`Network::add_edge`] / [`Network::add_undirected_edge`], then route over it.
#[derive(Clone, Debug, Default)]
pub struct Network {
    locations: Vec<Point>,
    adjacency: Vec<Vec<Edge>>,
}

impl Network {
    /// An empty network.
    pub fn new() -> Self {
        Self {
            locations: Vec::new(),
            adjacency: Vec::new(),
        }
    }

    /// Add a node at `location`, returning its id (`0`-based, dense).
    pub fn add_node(&mut self, location: Point) -> usize {
        let id = self.locations.len();
        self.locations.push(location);
        self.adjacency.push(Vec::new());
        id
    }

    /// The number of nodes.
    pub fn node_count(&self) -> usize {
        self.locations.len()
    }

    /// Is the network empty (no nodes)?
    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    /// The location of node `id` (panics if out of range).
    pub fn location(&self, id: usize) -> Point {
        self.locations[id]
    }

    /// The out-edges of node `id`.
    pub fn edges(&self, id: usize) -> &[Edge] {
        &self.adjacency[id]
    }

    /// Add a **directed** weighted edge `from → to`. `weight` must be non-negative for
    /// Dijkstra/A\* to be correct; negative weights are rejected (returns `Err`).
    pub fn add_edge(&mut self, from: usize, to: usize, weight: f64) -> Result<(), String> {
        if from >= self.locations.len() || to >= self.locations.len() {
            return Err(format!("edge {from}->{to} references an unknown node"));
        }
        if weight < 0.0 || weight.is_nan() {
            return Err(format!("edge {from}->{to} has a negative/NaN weight {weight}"));
        }
        self.adjacency[from].push(Edge { to, weight });
        Ok(())
    }

    /// Add an **undirected** weighted edge (two directed edges `a↔b`).
    pub fn add_undirected_edge(&mut self, a: usize, b: usize, weight: f64) -> Result<(), String> {
        self.add_edge(a, b, weight)?;
        self.add_edge(b, a, weight)
    }

    /// Add an undirected edge whose weight is the **great-circle distance** (metres) between
    /// the two nodes' lon/lat locations (CONCEPT:EG-266) — the convenient way to weight a
    /// geographic network so A\*'s Haversine heuristic is admissible.
    pub fn add_undirected_geodesic(&mut self, a: usize, b: usize) -> Result<(), String> {
        let w = haversine_distance(&self.locations[a], &self.locations[b]);
        self.add_undirected_edge(a, b, w)
    }

    // ── shortest path ─────────────────────────────────────────────────────────────────

    /// **Dijkstra** shortest path from `source` to `target` (CONCEPT:EG-266). Returns the
    /// least-cost [`Path`], or `None` if `target` is unreachable. Uniform-cost search with a
    /// binary-heap frontier; requires non-negative edge weights (enforced at insert time).
    pub fn dijkstra(&self, source: usize, target: usize) -> Option<Path> {
        self.search(source, Some(target), f64::INFINITY)
            .and_then(|(dist, prev)| reconstruct(source, target, &dist, &prev))
    }

    /// **A\*** shortest path (CONCEPT:EG-266) using the caller-supplied `heuristic`
    /// (an estimated remaining cost from a node's [`Point`] to the target). The heuristic
    /// must be **admissible** (never over-estimate) for the result to be optimal.
    pub fn astar(
        &self,
        source: usize,
        target: usize,
        heuristic: impl Fn(&Point) -> f64,
    ) -> Option<Path> {
        let n = self.locations.len();
        if source >= n || target >= n {
            return None;
        }
        let mut dist = vec![f64::INFINITY; n];
        let mut prev = vec![usize::MAX; n];
        dist[source] = 0.0;
        let mut heap = BinaryHeap::new();
        heap.push(Frontier {
            priority: heuristic(&self.locations[source]),
            cost: 0.0,
            node: source,
        });
        while let Some(Frontier { cost, node, .. }) = heap.pop() {
            if node == target {
                return reconstruct(source, target, &dist, &prev);
            }
            if cost > dist[node] {
                continue; // stale heap entry
            }
            for e in &self.adjacency[node] {
                let nd = cost + e.weight;
                if nd < dist[e.to] {
                    dist[e.to] = nd;
                    prev[e.to] = node;
                    heap.push(Frontier {
                        priority: nd + heuristic(&self.locations[e.to]),
                        cost: nd,
                        node: e.to,
                    });
                }
            }
        }
        None
    }

    /// **A\*** with the built-in great-circle (Haversine) heuristic to the target's location
    /// (CONCEPT:EG-266) — admissible when edge weights are geodesic distances in metres.
    pub fn astar_greatcircle(&self, source: usize, target: usize) -> Option<Path> {
        if target >= self.locations.len() {
            return None;
        }
        let goal = self.locations[target];
        self.astar(source, target, |p| haversine_distance(p, &goal))
    }

    // ── isochrone ─────────────────────────────────────────────────────────────────────

    /// **Isochrone** (CONCEPT:EG-266): every node reachable from `source` with a total path
    /// cost `≤ budget`, each paired with its shortest-path cost, ascending by cost. A
    /// one-to-many Dijkstra pruned at the budget — "everything within N minutes/metres".
    pub fn isochrone(&self, source: usize, budget: f64) -> Vec<(usize, f64)> {
        let Some((dist, _)) = self.search(source, None, budget) else {
            return Vec::new();
        };
        let mut out: Vec<(usize, f64)> = dist
            .iter()
            .enumerate()
            .filter(|(_, &d)| d.is_finite() && d <= budget)
            .map(|(i, &d)| (i, d))
            .collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        out
    }

    /// Shortest-path cost from `source` to every node (CONCEPT:EG-266); `None`/`inf` for
    /// unreachable nodes. Handy for building a network distance matrix.
    pub fn shortest_path_costs(&self, source: usize) -> Vec<f64> {
        self.search(source, None, f64::INFINITY)
            .map(|(d, _)| d)
            .unwrap_or_else(|| vec![f64::INFINITY; self.locations.len()])
    }

    /// Core Dijkstra loop. Stops early if `target` is popped; prunes any node whose cost
    /// exceeds `max_cost` (used by the isochrone budget). Returns `(dist, prev)` or `None`
    /// when `source` is out of range.
    fn search(
        &self,
        source: usize,
        target: Option<usize>,
        max_cost: f64,
    ) -> Option<(Vec<f64>, Vec<usize>)> {
        let n = self.locations.len();
        if source >= n {
            return None;
        }
        let mut dist = vec![f64::INFINITY; n];
        let mut prev = vec![usize::MAX; n];
        dist[source] = 0.0;
        let mut heap = BinaryHeap::new();
        heap.push(Frontier {
            priority: 0.0,
            cost: 0.0,
            node: source,
        });
        while let Some(Frontier { cost, node, .. }) = heap.pop() {
            if cost > dist[node] {
                continue;
            }
            if Some(node) == target {
                break;
            }
            for e in &self.adjacency[node] {
                let nd = cost + e.weight;
                if nd <= max_cost && nd < dist[e.to] {
                    dist[e.to] = nd;
                    prev[e.to] = node;
                    heap.push(Frontier {
                        priority: nd,
                        cost: nd,
                        node: e.to,
                    });
                }
            }
        }
        Some((dist, prev))
    }
}

/// A routed path (CONCEPT:EG-266): the ordered node ids from source to target and the total
/// accumulated `cost`.
#[derive(Clone, Debug, PartialEq)]
pub struct Path {
    pub nodes: Vec<usize>,
    pub cost: f64,
}

/// Rebuild the node sequence from a `prev` predecessor array; `None` if `target` was never
/// reached.
fn reconstruct(source: usize, target: usize, dist: &[f64], prev: &[usize]) -> Option<Path> {
    if !dist[target].is_finite() {
        return None;
    }
    let mut nodes = vec![target];
    let mut cur = target;
    while cur != source {
        let p = prev[cur];
        if p == usize::MAX {
            return None;
        }
        nodes.push(p);
        cur = p;
    }
    nodes.reverse();
    Some(Path {
        nodes,
        cost: dist[target],
    })
}

/// A priority-queue frontier entry ordered so [`BinaryHeap`] (a max-heap) pops the
/// **smallest** `priority` first. NaN sorts last.
struct Frontier {
    priority: f64,
    cost: f64,
    node: usize,
}
impl PartialEq for Frontier {
    fn eq(&self, o: &Self) -> bool {
        self.priority == o.priority
    }
}
impl Eq for Frontier {}
impl PartialOrd for Frontier {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Frontier {
    fn cmp(&self, o: &Self) -> Ordering {
        o.priority
            .partial_cmp(&self.priority)
            .unwrap_or(Ordering::Equal)
    }
}

// ── TSP (nearest-neighbour + 2-opt) ────────────────────────────────────────────────────

/// Build a full distance matrix over `points` using `metric` (CONCEPT:EG-266). Symmetric
/// when `metric` is; the diagonal is zero. Pass [`haversine_distance`] for a geographic
/// tour or [`Point::distance`] for a planar one.
pub fn distance_matrix(points: &[Point], metric: impl Fn(&Point, &Point) -> f64) -> Vec<Vec<f64>> {
    let n = points.len();
    let mut m = vec![vec![0.0; n]; n];
    for i in 0..n {
        for j in (i + 1)..n {
            let d = metric(&points[i], &points[j]);
            m[i][j] = d;
            m[j][i] = d;
        }
    }
    m
}

/// The total length of `tour` over distance matrix `dist`, **including** the return leg to
/// the start (a closed tour) (CONCEPT:EG-266).
pub fn tour_length(tour: &[usize], dist: &[Vec<f64>]) -> f64 {
    if tour.len() < 2 {
        return 0.0;
    }
    let mut total = 0.0;
    for w in tour.windows(2) {
        total += dist[w[0]][w[1]];
    }
    total + dist[tour[tour.len() - 1]][tour[0]] // close the loop
}

/// A **nearest-neighbour** TSP tour from `start` over distance matrix `dist` (CONCEPT:EG-266):
/// greedily hop to the closest unvisited node. Fast seed, typically ~25% above optimal.
pub fn nearest_neighbour_tour(dist: &[Vec<f64>], start: usize) -> Vec<usize> {
    let n = dist.len();
    if n == 0 {
        return Vec::new();
    }
    let mut visited = vec![false; n];
    let mut tour = Vec::with_capacity(n);
    let mut cur = start.min(n - 1);
    visited[cur] = true;
    tour.push(cur);
    for _ in 1..n {
        let mut best = usize::MAX;
        let mut best_d = f64::INFINITY;
        for (j, &v) in visited.iter().enumerate() {
            if !v && dist[cur][j] < best_d {
                best_d = dist[cur][j];
                best = j;
            }
        }
        if best == usize::MAX {
            break;
        }
        visited[best] = true;
        tour.push(best);
        cur = best;
    }
    tour
}

/// Improve a closed `tour` in place with **2-opt** (CONCEPT:EG-266): repeatedly reverse a
/// segment whenever doing so shortens the tour, until no improving move remains. Returns the
/// improved tour (never longer than the input).
pub fn two_opt(mut tour: Vec<usize>, dist: &[Vec<f64>]) -> Vec<usize> {
    let n = tour.len();
    if n < 4 {
        return tour;
    }
    let mut improved = true;
    while improved {
        improved = false;
        // Consider reversing tour[i..=k]; edges (i-1,i) and (k,k+1) become (i-1,k) and (i,k+1).
        for i in 1..(n - 1) {
            for k in (i + 1)..n {
                let a = tour[i - 1];
                let b = tour[i];
                let c = tour[k];
                let d = tour[(k + 1) % n]; // wraps to start to include the closing leg
                let before = dist[a][b] + dist[c][d];
                let after = dist[a][c] + dist[b][d];
                if after + 1e-12 < before {
                    tour[i..=k].reverse();
                    improved = true;
                }
            }
        }
    }
    tour
}

/// Solve a TSP over `points` from `start`: build a Haversine distance matrix, seed with
/// nearest-neighbour, refine with 2-opt (CONCEPT:EG-266). Returns the tour order and its
/// closed length in metres. For a planar tour, build the matrix with [`distance_matrix`] and
/// call [`nearest_neighbour_tour`] + [`two_opt`] directly.
pub fn solve_tsp(points: &[Point], start: usize) -> (Vec<usize>, f64) {
    let dist = distance_matrix(points, haversine_distance);
    let seed = nearest_neighbour_tour(&dist, start);
    let tour = two_opt(seed, &dist);
    let len = tour_length(&tour, &dist);
    (tour, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 4-node "diamond" network. Shortest 0→3 is via node 1 or 2 (cost 2), not the direct
    /// edge (cost 5).
    ///
    /// ```text
    ///      1
    ///    /   \
    ///   0 --5-- 3
    ///    \   /
    ///      2
    /// ```
    fn diamond() -> Network {
        let mut g = Network::new();
        let n0 = g.add_node(Point::new(0.0, 0.0));
        let n1 = g.add_node(Point::new(1.0, 1.0));
        let n2 = g.add_node(Point::new(1.0, -1.0));
        let n3 = g.add_node(Point::new(2.0, 0.0));
        g.add_undirected_edge(n0, n1, 1.0).unwrap();
        g.add_undirected_edge(n1, n3, 1.0).unwrap();
        g.add_undirected_edge(n0, n2, 1.0).unwrap();
        g.add_undirected_edge(n2, n3, 1.0).unwrap();
        g.add_undirected_edge(n0, n3, 5.0).unwrap(); // tempting but long direct edge
        g
    }

    #[test]
    fn eg266_dijkstra_finds_least_cost_path() {
        let g = diamond();
        let p = g.dijkstra(0, 3).expect("path 0->3");
        assert_eq!(p.cost, 2.0, "two-hop beats the direct edge");
        assert_eq!(p.nodes.first(), Some(&0));
        assert_eq!(p.nodes.last(), Some(&3));
        assert_eq!(p.nodes.len(), 3, "exactly one intermediate hop");
    }

    #[test]
    fn eg266_astar_matches_dijkstra_optimum() {
        // A\* with a zero heuristic is Dijkstra; with an admissible heuristic it stays optimal.
        let g = diamond();
        let dj = g.dijkstra(0, 3).unwrap();
        let a0 = g.astar(0, 3, |_| 0.0).unwrap();
        assert_eq!(a0.cost, dj.cost);
        // Euclidean straight-line heuristic to node 3 (admissible for these unit costs).
        let goal = g.location(3);
        let ah = g.astar(0, 3, |p| p.distance(&goal)).unwrap();
        assert_eq!(ah.cost, dj.cost);
    }

    #[test]
    fn eg266_astar_greatcircle_on_geodesic_network() {
        // A path of lon/lat nodes weighted by great-circle distance; A\* great-circle
        // heuristic must return the same optimal cost as Dijkstra.
        let mut g = Network::new();
        let a = g.add_node(Point::new(0.0, 0.0));
        let b = g.add_node(Point::new(1.0, 0.0));
        let c = g.add_node(Point::new(2.0, 0.0));
        let d = g.add_node(Point::new(1.0, 1.0));
        g.add_undirected_geodesic(a, b).unwrap();
        g.add_undirected_geodesic(b, c).unwrap();
        g.add_undirected_geodesic(a, d).unwrap();
        g.add_undirected_geodesic(d, c).unwrap();
        let dj = g.dijkstra(a, c).unwrap();
        let ast = g.astar_greatcircle(a, c).unwrap();
        assert!((dj.cost - ast.cost).abs() < 1e-6, "A* == Dijkstra cost");
        assert_eq!(ast.nodes.first(), Some(&a));
        assert_eq!(ast.nodes.last(), Some(&c));
    }

    #[test]
    fn eg266_unreachable_target_is_none() {
        let mut g = Network::new();
        let a = g.add_node(Point::new(0.0, 0.0));
        let _b = g.add_node(Point::new(1.0, 0.0)); // island, no edges
        let c = g.add_node(Point::new(2.0, 0.0));
        g.add_undirected_edge(a, c, 1.0).unwrap();
        assert!(g.dijkstra(a, 1).is_none());
    }

    #[test]
    fn eg266_isochrone_reachable_set_within_budget() {
        // Line graph 0-1-2-3-4, each hop cost 1. Budget 2 reaches nodes 0,1,2 only.
        let mut g = Network::new();
        for i in 0..5 {
            g.add_node(Point::new(i as f64, 0.0));
        }
        for i in 0..4 {
            g.add_undirected_edge(i, i + 1, 1.0).unwrap();
        }
        let iso = g.isochrone(0, 2.0);
        let ids: Vec<usize> = iso.iter().map(|(i, _)| *i).collect();
        assert_eq!(ids, vec![0, 1, 2], "only nodes within cost 2");
        assert_eq!(iso[0].1, 0.0);
        assert_eq!(iso[2].1, 2.0);
        // A bigger budget reaches everything.
        assert_eq!(g.isochrone(0, 10.0).len(), 5);
    }

    #[test]
    fn eg266_negative_weight_rejected() {
        let mut g = Network::new();
        g.add_node(Point::new(0.0, 0.0));
        g.add_node(Point::new(1.0, 0.0));
        assert!(g.add_edge(0, 1, -1.0).is_err());
        assert!(g.add_edge(0, 5, 1.0).is_err()); // unknown node
    }

    #[test]
    fn eg266_two_opt_improves_over_nearest_neighbour() {
        // A unit square with a deliberately bad start order. The optimal tour visits the
        // square in perimeter order (length 4); a crossing tour is longer. 2-opt must not be
        // worse than the NN seed and must reach the optimal 4.0 here.
        let pts = vec![
            Point::new(0.0, 0.0),
            Point::new(1.0, 0.0),
            Point::new(1.0, 1.0),
            Point::new(0.0, 1.0),
        ];
        let dist = distance_matrix(&pts, |a, b| a.distance(b));
        // A crossing (bowtie) tour 0-2-1-3 has length 2√2 + 2 ≈ 4.83.
        let crossing = vec![0usize, 2, 1, 3];
        let crossing_len = tour_length(&crossing, &dist);
        assert!(crossing_len > 4.0 + 1e-9);

        let seed = nearest_neighbour_tour(&dist, 0);
        let seed_len = tour_length(&seed, &dist);
        let opt = two_opt(seed.clone(), &dist);
        let opt_len = tour_length(&opt, &dist);
        assert!(opt_len <= seed_len + 1e-12, "2-opt never worsens the tour");
        assert!((opt_len - 4.0).abs() < 1e-9, "reaches the optimal perimeter tour");
        // 2-opt on the crossing tour also untangles it to 4.0.
        let fixed = two_opt(crossing, &dist);
        assert!((tour_length(&fixed, &dist) - 4.0).abs() < 1e-9);
    }

    #[test]
    fn eg266_solve_tsp_geodesic_visits_all_points() {
        // Four cities; the solver returns a permutation of all of them with finite length.
        let cities = vec![
            Point::new(-0.1278, 51.5074), // London
            Point::new(2.3522, 48.8566),  // Paris
            Point::new(4.9041, 52.3676),  // Amsterdam
            Point::new(13.4050, 52.5200), // Berlin
        ];
        let (tour, len) = solve_tsp(&cities, 0);
        assert_eq!(tour.len(), 4);
        let mut sorted = tour.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2, 3], "every city visited once");
        assert!(len.is_finite() && len > 0.0);
    }
}
