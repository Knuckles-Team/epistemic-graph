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
//!
//! ## Turn restrictions & time-dependent weights (CONCEPT:EG-312)
//!
//! Real road networks are not plain weighted graphs: a manoeuvre *through* a junction has a
//! cost of its own (a no-left-turn is banned, a u-turn is expensive), and an edge's cost
//! depends on *when* you traverse it (rush-hour traffic, opening hours). EG-312 layers two
//! **additive** capabilities on top of EG-266 without touching the plain-weight API:
//!
//! * **Turn costs / restrictions** — a [`TurnCost`] model (penalty for a
//!   `(from → via → to)` transition; `INFINITY` bans it). [`Network::dijkstra_with_turns`]
//!   / [`Network::astar_with_turns`] route over an *edge-expanded* state space
//!   (`(prev_node, node)` states) so the search consults the turn cost at every junction and
//!   still returns an optimal [`Path`]. [`TurnRestrictions`] is a ready table-driven model
//!   with a configurable u-turn penalty.
//! * **Time-dependent edge weights** — a [`TimeCost`] model (`(from, to, base_weight,
//!   t_depart) → cost`) so an edge costs more at rush hour or is closed outside opening
//!   hours. [`Network::shortest_path_time_dependent`] runs a time-dependent Dijkstra
//!   (label = earliest arrival) that picks different routes for different departure times.
//!   [`TrafficProfile`] is a ready piecewise time-window model.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

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
            return Err(format!(
                "edge {from}->{to} has a negative/NaN weight {weight}"
            ));
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

    // ── turn restrictions / turn costs (CONCEPT:EG-312) ─────────────────────────────────

    /// **Dijkstra honouring turn costs** (CONCEPT:EG-312). Like [`Network::dijkstra`] but the
    /// cost of every manoeuvre `(prev → node → next)` is charged from the `turns` model, so a
    /// banned turn (cost `INFINITY`) is never taken and a penalised turn (e.g. a u-turn) is
    /// avoided when a cheaper legal route exists. The optimal legal [`Path`] or `None`.
    ///
    /// Implemented via **edge-based expansion**: the search state is `(prev_node, node)` (the
    /// directed edge just travelled), so the same node can legitimately be re-entered from a
    /// different predecessor (needed when a turn restriction forces a detour or a u-turn).
    pub fn dijkstra_with_turns<T: TurnCost>(
        &self,
        source: usize,
        target: usize,
        turns: &T,
    ) -> Option<Path> {
        self.search_turns(source, target, turns, |_| 0.0)
    }

    /// **A\*** honouring turn costs (CONCEPT:EG-312) with a caller-supplied admissible
    /// `heuristic` (estimated remaining cost from a node's [`Point`] to the target). Same
    /// turn-aware edge expansion as [`Network::dijkstra_with_turns`].
    pub fn astar_with_turns<T: TurnCost>(
        &self,
        source: usize,
        target: usize,
        turns: &T,
        heuristic: impl Fn(&Point) -> f64,
    ) -> Option<Path> {
        self.search_turns(source, target, turns, heuristic)
    }

    /// **A\*** honouring turn costs with the built-in great-circle heuristic (CONCEPT:EG-312) —
    /// admissible when edge weights are geodesic distances in metres (see
    /// [`Network::astar_greatcircle`]).
    pub fn astar_greatcircle_with_turns<T: TurnCost>(
        &self,
        source: usize,
        target: usize,
        turns: &T,
    ) -> Option<Path> {
        if target >= self.locations.len() {
            return None;
        }
        let goal = self.locations[target];
        self.search_turns(source, target, turns, move |p| haversine_distance(p, &goal))
    }

    /// Core turn-aware Dijkstra/A\* (CONCEPT:EG-312). State = `(prev_node, node)`; the turn
    /// cost `turns(prev, node, next)` is added when relaxing `node → next` (skipped when the
    /// state is the start, which has no prior edge). A turn cost of `INFINITY`/NaN prunes the
    /// move. Deterministic: relaxation order follows the heap's `(priority, …)` ordering and
    /// the adjacency-list order, both fixed by construction.
    fn search_turns<T: TurnCost>(
        &self,
        source: usize,
        target: usize,
        turns: &T,
        heuristic: impl Fn(&Point) -> f64,
    ) -> Option<Path> {
        let n = self.locations.len();
        if source >= n || target >= n {
            return None;
        }
        // `usize::MAX` as `prev` marks the start state: no prior edge, so no turn is charged.
        let start = (usize::MAX, source);
        let mut dist: HashMap<(usize, usize), f64> = HashMap::new();
        let mut prev_state: HashMap<(usize, usize), (usize, usize)> = HashMap::new();
        dist.insert(start, 0.0);
        let mut heap = BinaryHeap::new();
        heap.push(TurnFrontier {
            priority: heuristic(&self.locations[source]),
            cost: 0.0,
            prev: usize::MAX,
            node: source,
        });
        while let Some(TurnFrontier {
            cost, prev, node, ..
        }) = heap.pop()
        {
            if node == target {
                return reconstruct_turns((prev, node), cost, &prev_state);
            }
            if cost > dist.get(&(prev, node)).copied().unwrap_or(f64::INFINITY) {
                continue; // stale heap entry
            }
            for e in &self.adjacency[node] {
                let tc = if prev == usize::MAX {
                    0.0
                } else {
                    turns.turn_cost(prev, node, e.to)
                };
                if !tc.is_finite() {
                    continue; // banned turn (INFINITY) or NaN
                }
                let nd = cost + e.weight + tc;
                let key = (node, e.to);
                if nd < dist.get(&key).copied().unwrap_or(f64::INFINITY) {
                    dist.insert(key, nd);
                    prev_state.insert(key, (prev, node));
                    heap.push(TurnFrontier {
                        priority: nd + heuristic(&self.locations[e.to]),
                        cost: nd,
                        prev: node,
                        node: e.to,
                    });
                }
            }
        }
        None
    }

    // ── time-dependent / time-window edge weights (CONCEPT:EG-312) ──────────────────────

    /// **Time-dependent shortest path** (CONCEPT:EG-312): the least-cost [`Path`] from
    /// `source` to `target` when departing `source` at time `t_start`, where each edge's cost
    /// is a function of the moment it is entered (traffic profiles, opening hours). The `cost`
    /// model receives `(from, to, base_weight, t_depart)` and returns the realised traversal
    /// cost (travel time); a non-finite/negative result closes the edge at that instant.
    ///
    /// Runs a **time-dependent Dijkstra** whose label is the earliest known arrival time at
    /// each node — optimal under the standard **FIFO / no-overtaking** assumption (departing an
    /// edge later never yields an earlier arrival). The returned [`Path::cost`] is the total
    /// elapsed travel time (`arrival − t_start`). Departing at different `t_start` values can
    /// therefore select different routes.
    pub fn shortest_path_time_dependent<C: TimeCost>(
        &self,
        source: usize,
        target: usize,
        t_start: f64,
        cost: &C,
    ) -> Option<Path> {
        let n = self.locations.len();
        if source >= n || target >= n {
            return None;
        }
        let mut arrival = vec![f64::INFINITY; n];
        let mut prev = vec![usize::MAX; n];
        arrival[source] = t_start;
        let mut heap = BinaryHeap::new();
        heap.push(Frontier {
            priority: t_start,
            cost: t_start,
            node: source,
        });
        while let Some(Frontier {
            cost: t_now, node, ..
        }) = heap.pop()
        {
            if node == target {
                break;
            }
            if t_now > arrival[node] {
                continue; // stale heap entry
            }
            for e in &self.adjacency[node] {
                let travel = cost.traverse_cost(node, e.to, e.weight, t_now);
                if !travel.is_finite() || travel < 0.0 {
                    continue; // edge closed at this instant (e.g. outside opening hours)
                }
                let arr = t_now + travel;
                if arr < arrival[e.to] {
                    arrival[e.to] = arr;
                    prev[e.to] = node;
                    heap.push(Frontier {
                        priority: arr,
                        cost: arr,
                        node: e.to,
                    });
                }
            }
        }
        if !arrival[target].is_finite() {
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
            cost: arrival[target] - t_start,
        })
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

// ── turn-cost model (CONCEPT:EG-312) ────────────────────────────────────────────────────

/// A **turn-cost model** (CONCEPT:EG-312): the extra cost of the manoeuvre that, having
/// arrived at junction `via` from `from`, leaves `via` toward `to`. Returning `f64::INFINITY`
/// **bans** the turn (e.g. a no-left-turn); a finite value is added to the path cost (e.g. a
/// u-turn penalty or a signalised-junction delay). Any `Fn(usize, usize, usize) -> f64` is a
/// turn-cost model, and [`TurnRestrictions`] is a ready table-driven one.
pub trait TurnCost {
    /// The added cost of the turn `from → via → to` (`INFINITY` = banned).
    fn turn_cost(&self, from: usize, via: usize, to: usize) -> f64;
}

impl<F: Fn(usize, usize, usize) -> f64> TurnCost for F {
    fn turn_cost(&self, from: usize, via: usize, to: usize) -> f64 {
        (self)(from, via, to)
    }
}

/// A table-driven [`TurnCost`] model (CONCEPT:EG-312): explicit per-turn penalties/bans plus a
/// blanket **u-turn penalty** applied to any `from → via → from` manoeuvre. Explicit table
/// entries take precedence over the u-turn default, so a specific u-turn can be individually
/// allowed, penalised or banned. Build with [`TurnRestrictions::new`] then [`ban`] /
/// [`penalize`] / [`with_uturn_penalty`].
///
/// [`ban`]: TurnRestrictions::ban
/// [`penalize`]: TurnRestrictions::penalize
/// [`with_uturn_penalty`]: TurnRestrictions::with_uturn_penalty
#[derive(Clone, Debug, Default)]
pub struct TurnRestrictions {
    table: HashMap<(usize, usize, usize), f64>,
    uturn_penalty: f64,
}

impl TurnRestrictions {
    /// An empty model — no restrictions and a zero u-turn penalty (CONCEPT:EG-312).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the blanket u-turn penalty (builder form) charged to any `from → via → from`
    /// manoeuvre lacking an explicit table entry (CONCEPT:EG-312).
    pub fn with_uturn_penalty(mut self, penalty: f64) -> Self {
        self.uturn_penalty = penalty;
        self
    }

    /// Set the blanket u-turn penalty in place (CONCEPT:EG-312).
    pub fn set_uturn_penalty(&mut self, penalty: f64) -> &mut Self {
        self.uturn_penalty = penalty;
        self
    }

    /// **Ban** the turn `from → via → to` (cost `INFINITY`) — CONCEPT:EG-312.
    pub fn ban(&mut self, from: usize, via: usize, to: usize) -> &mut Self {
        self.table.insert((from, via, to), f64::INFINITY);
        self
    }

    /// **Penalise** the turn `from → via → to` by `cost` (added to the path) — CONCEPT:EG-312.
    pub fn penalize(&mut self, from: usize, via: usize, to: usize, cost: f64) -> &mut Self {
        self.table.insert((from, via, to), cost);
        self
    }
}

impl TurnCost for TurnRestrictions {
    fn turn_cost(&self, from: usize, via: usize, to: usize) -> f64 {
        if let Some(&c) = self.table.get(&(from, via, to)) {
            return c; // explicit entry wins
        }
        if from == to {
            return self.uturn_penalty; // a u-turn back down the edge we came from
        }
        0.0
    }
}

/// A turn-aware priority-queue entry (CONCEPT:EG-312): like [`Frontier`] but the search state
/// is the directed edge `(prev, node)` just travelled, so a node can be re-entered from a
/// different predecessor. Ordered so the smallest `priority` pops first.
struct TurnFrontier {
    priority: f64,
    cost: f64,
    prev: usize,
    node: usize,
}
impl PartialEq for TurnFrontier {
    fn eq(&self, o: &Self) -> bool {
        self.priority == o.priority
    }
}
impl Eq for TurnFrontier {}
impl PartialOrd for TurnFrontier {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for TurnFrontier {
    fn cmp(&self, o: &Self) -> Ordering {
        o.priority
            .partial_cmp(&self.priority)
            .unwrap_or(Ordering::Equal)
    }
}

/// Rebuild the node sequence for a turn-aware search (CONCEPT:EG-312) by walking the
/// `(prev, node)` predecessor-state chain back to the start state.
fn reconstruct_turns(
    winning: (usize, usize),
    cost: f64,
    prev_state: &HashMap<(usize, usize), (usize, usize)>,
) -> Option<Path> {
    let mut nodes = Vec::new();
    let mut st = winning;
    loop {
        nodes.push(st.1);
        match prev_state.get(&st) {
            Some(&p) => st = p,
            None => break, // start state (prev == usize::MAX): its node is the source
        }
    }
    nodes.reverse();
    Some(Path { nodes, cost })
}

// ── time-dependent edge-cost model (CONCEPT:EG-312) ─────────────────────────────────────

/// A **time-dependent edge-cost model** (CONCEPT:EG-312): the realised cost (travel time) of
/// traversing edge `from → to` (base weight `base_weight`) when it is entered at `t_depart`.
/// Returning a non-finite or negative value closes the edge at that instant (e.g. outside
/// opening hours). Any `Fn(usize, usize, f64, f64) -> f64` is a model, and [`TrafficProfile`]
/// is a ready piecewise time-window one.
pub trait TimeCost {
    /// The traversal cost of `from → to` (base `base_weight`) departing at `t_depart`.
    fn traverse_cost(&self, from: usize, to: usize, base_weight: f64, t_depart: f64) -> f64;
}

impl<F: Fn(usize, usize, f64, f64) -> f64> TimeCost for F {
    fn traverse_cost(&self, from: usize, to: usize, base_weight: f64, t_depart: f64) -> f64 {
        (self)(from, to, base_weight, t_depart)
    }
}

/// A piecewise **time-window** [`TimeCost`] model (CONCEPT:EG-312) — a traffic / opening-hours
/// profile. Each directed edge `(from, to)` may carry `[start, end)` windows with a cost
/// **multiplier** on the base weight (e.g. `3.0` for rush-hour congestion, `INFINITY` to close
/// the edge outside opening hours). Windows are tested in insertion order and the first match
/// wins (deterministic); outside every window the base weight applies unchanged.
/// One `(start, end, multiplier)` time-window on an edge (CONCEPT:EG-312).
type TimeWindow = (f64, f64, f64);

#[derive(Clone, Debug, Default)]
pub struct TrafficProfile {
    windows: HashMap<(usize, usize), Vec<TimeWindow>>,
}

impl TrafficProfile {
    /// An empty profile (every edge at its base weight for all time) — CONCEPT:EG-312.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a `[start, end)` window on directed edge `from → to` multiplying the base weight by
    /// `multiplier` for departures inside it (CONCEPT:EG-312). `INFINITY` closes the edge in
    /// that window. Returns `&mut self` for chaining.
    pub fn add_window(
        &mut self,
        from: usize,
        to: usize,
        start: f64,
        end: f64,
        multiplier: f64,
    ) -> &mut Self {
        self.windows
            .entry((from, to))
            .or_default()
            .push((start, end, multiplier));
        self
    }
}

impl TimeCost for TrafficProfile {
    fn traverse_cost(&self, from: usize, to: usize, base_weight: f64, t_depart: f64) -> f64 {
        if let Some(ws) = self.windows.get(&(from, to)) {
            for &(start, end, mult) in ws {
                if t_depart >= start && t_depart < end {
                    return base_weight * mult;
                }
            }
        }
        base_weight
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
        assert!(
            (opt_len - 4.0).abs() < 1e-9,
            "reaches the optimal perimeter tour"
        );
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

    // ── turn restrictions / turn costs (CONCEPT:EG-312) ─────────────────────────────────

    /// The [`TurnRestrictions`] model (CONCEPT:EG-312): explicit bans/penalties win over the
    /// blanket u-turn penalty; unrestricted turns are free.
    #[test]
    fn eg312_turn_restrictions_model_lookup() {
        let mut tr = TurnRestrictions::new().with_uturn_penalty(4.0);
        tr.ban(0, 1, 2).penalize(3, 4, 5, 1.5);
        assert!(tr.turn_cost(0, 1, 2).is_infinite(), "banned turn");
        assert_eq!(tr.turn_cost(3, 4, 5), 1.5, "explicit penalty");
        assert_eq!(tr.turn_cost(0, 1, 0), 4.0, "blanket u-turn penalty");
        assert_eq!(tr.turn_cost(0, 1, 3), 0.0, "unrestricted turn is free");
        // An explicit entry overrides the u-turn default for that specific manoeuvre.
        tr.penalize(7, 8, 7, 0.0);
        assert_eq!(tr.turn_cost(7, 8, 7), 0.0, "u-turn explicitly allowed");
    }

    /// A banned turn forces a strictly longer *legal* path than the turn-free optimum
    /// (CONCEPT:EG-312). Plain Dijkstra takes `0→1→2` (cost 2); banning the turn `0→1→2`
    /// forces the detour `0→3→2` (cost 3).
    ///
    /// ```text
    ///   0 --1-- 1 --1-- 2      (banned turn at node 1: 0→1→2)
    ///    \             /
    ///     1          2
    ///      \       /
    ///        \-- 3 --/
    /// ```
    #[test]
    fn eg312_turn_restriction_forces_longer_legal_path() {
        let mut g = Network::new();
        for i in 0..4 {
            g.add_node(Point::new(i as f64, 0.0));
        }
        g.add_undirected_edge(0, 1, 1.0).unwrap();
        g.add_undirected_edge(1, 2, 1.0).unwrap();
        g.add_undirected_edge(0, 3, 1.0).unwrap();
        g.add_undirected_edge(3, 2, 2.0).unwrap();

        // Turn-free optimum.
        let plain = g.dijkstra(0, 2).unwrap();
        assert_eq!(plain.cost, 2.0);
        assert_eq!(plain.nodes, vec![0, 1, 2]);

        // Ban the turn 0→1→2: the router must take the longer legal detour.
        let mut tr = TurnRestrictions::new();
        tr.ban(0, 1, 2);
        let legal = g.dijkstra_with_turns(0, 2, &tr).unwrap();
        assert_eq!(
            legal.cost, 3.0,
            "detour costs more than the banned direct route"
        );
        assert_eq!(legal.nodes, vec![0, 3, 2], "goes around via node 3");

        // A\* with turns must agree with Dijkstra-with-turns.
        let a = g.astar_with_turns(0, 2, &tr, |_| 0.0).unwrap();
        assert_eq!(a.cost, legal.cost);
        assert_eq!(a.nodes, legal.nodes);
    }

    /// A no-left-turn at a junction is worked around either by a **u-turn** (charged the u-turn
    /// penalty) or by a longer **detour**; the u-turn penalty decides which the router picks
    /// (CONCEPT:EG-312). Small penalty → u-turn route `0→1→4→1→2`; large penalty → detour
    /// `0→1→3→2`.
    #[test]
    fn eg312_uturn_penalty_changes_route_choice() {
        let mut g = Network::new();
        for i in 0..5 {
            g.add_node(Point::new(i as f64, 0.0));
        }
        g.add_undirected_edge(0, 1, 1.0).unwrap(); // approach
        g.add_undirected_edge(1, 2, 1.0).unwrap(); // the "left" turn target
        g.add_undirected_edge(1, 4, 1.0).unwrap(); // straight ahead (u-turn point)
        g.add_undirected_edge(1, 3, 2.0).unwrap(); // detour leg
        g.add_undirected_edge(3, 2, 2.0).unwrap(); // detour leg

        // No left turn: arriving at 1 from 0, cannot go straight to 2. U-turns are free here.
        let ban_left = |from: usize, via: usize, to: usize| -> f64 {
            if (from, via, to) == (0, 1, 2) {
                f64::INFINITY
            } else {
                0.0
            }
        };
        // Sanity: with the ban but *free* u-turns, the router makes a u-turn (cost 4).
        let free_uturn = g.dijkstra_with_turns(0, 2, &ban_left).unwrap();
        assert_eq!(free_uturn.cost, 4.0);
        assert_eq!(free_uturn.nodes, vec![0, 1, 4, 1, 2], "u-turn at node 4");

        // Cheap u-turn (0.5): u-turn route (4.5) still beats the detour (5.0).
        let mut cheap = TurnRestrictions::new().with_uturn_penalty(0.5);
        cheap.ban(0, 1, 2);
        let via_uturn = g.dijkstra_with_turns(0, 2, &cheap).unwrap();
        assert_eq!(via_uturn.cost, 4.5);
        assert_eq!(via_uturn.nodes, vec![0, 1, 4, 1, 2]);

        // Expensive u-turn (10): the detour (5.0) now wins.
        let mut pricey = TurnRestrictions::new().with_uturn_penalty(10.0);
        pricey.ban(0, 1, 2);
        let via_detour = g.dijkstra_with_turns(0, 2, &pricey).unwrap();
        assert_eq!(via_detour.cost, 5.0);
        assert_eq!(via_detour.nodes, vec![0, 1, 3, 2], "goes around the block");
    }

    /// Turn-aware routing with no restrictions reproduces plain Dijkstra (CONCEPT:EG-312) — the
    /// additive layer is a pure superset.
    #[test]
    fn eg312_turns_with_empty_model_match_plain_dijkstra() {
        let g = diamond();
        let plain = g.dijkstra(0, 3).unwrap();
        let turns = g
            .dijkstra_with_turns(0, 3, &TurnRestrictions::new())
            .unwrap();
        assert_eq!(plain.cost, turns.cost);
        assert_eq!(plain.nodes, turns.nodes);
    }

    // ── time-dependent / time-window edge weights (CONCEPT:EG-312) ──────────────────────

    /// A two-route network where departure time selects the path (CONCEPT:EG-312). Route A
    /// (`0→1→3`, base 2) is fast off-peak but jams 5× during `[8, 9)`; route B (`0→2→3`, base
    /// 3.2) is steady. Departing at `t=0` picks A; departing at `t=8` picks B.
    #[test]
    fn eg312_time_dependent_picks_different_paths_by_departure_time() {
        let mut g = Network::new();
        for i in 0..4 {
            g.add_node(Point::new(i as f64, 0.0));
        }
        g.add_undirected_edge(0, 1, 1.0).unwrap();
        g.add_undirected_edge(1, 3, 1.0).unwrap(); // route A: fast highway
        g.add_undirected_edge(0, 2, 1.6).unwrap();
        g.add_undirected_edge(2, 3, 1.6).unwrap(); // route B: steady backroad

        let mut traffic = TrafficProfile::new();
        traffic.add_window(0, 1, 8.0, 9.0, 5.0); // rush-hour jam on the highway
        traffic.add_window(1, 3, 8.0, 9.0, 5.0);

        // Off-peak departure: highway is fastest.
        let off_peak = g.shortest_path_time_dependent(0, 3, 0.0, &traffic).unwrap();
        assert_eq!(off_peak.nodes, vec![0, 1, 3]);
        assert_eq!(off_peak.cost, 2.0);

        // Rush-hour departure: highway jams (0→1 costs 5, total 6) so the backroad wins.
        let rush = g.shortest_path_time_dependent(0, 3, 8.0, &traffic).unwrap();
        assert_eq!(rush.nodes, vec![0, 2, 3], "avoids the jammed highway");
        assert!((rush.cost - 3.2).abs() < 1e-9);
    }

    /// A closed edge (opening-hours window with an `INFINITY` multiplier) is skipped when
    /// departing inside the closure, forcing an alternate route (CONCEPT:EG-312).
    #[test]
    fn eg312_time_window_closes_edge_outside_opening_hours() {
        let mut g = Network::new();
        for i in 0..4 {
            g.add_node(Point::new(i as f64, 0.0));
        }
        g.add_undirected_edge(0, 1, 1.0).unwrap();
        g.add_undirected_edge(1, 3, 1.0).unwrap(); // preferred route via a gated road
        g.add_undirected_edge(0, 2, 5.0).unwrap();
        g.add_undirected_edge(2, 3, 5.0).unwrap(); // long always-open alternate

        let mut hours = TrafficProfile::new();
        hours.add_window(0, 1, 15.0, 1e9, f64::INFINITY); // 0→1 closed after t=15

        // Before closure: fast route open.
        let open = g.shortest_path_time_dependent(0, 3, 0.0, &hours).unwrap();
        assert_eq!(open.nodes, vec![0, 1, 3]);
        assert_eq!(open.cost, 2.0);

        // After closure: 0→1 is shut, must take the long alternate.
        let shut = g.shortest_path_time_dependent(0, 3, 20.0, &hours).unwrap();
        assert_eq!(shut.nodes, vec![0, 2, 3], "detours around the closed road");
        assert_eq!(shut.cost, 10.0);
    }

    /// A constant [`TimeCost`] (base weight for all time) reproduces plain Dijkstra
    /// (CONCEPT:EG-312).
    #[test]
    fn eg312_constant_time_cost_matches_plain_dijkstra() {
        let g = diamond();
        let plain = g.dijkstra(0, 3).unwrap();
        let const_cost = |_from: usize, _to: usize, w: f64, _dep: f64| w;
        let td = g
            .shortest_path_time_dependent(0, 3, 0.0, &const_cost)
            .unwrap();
        assert_eq!(plain.nodes, td.nodes);
        assert_eq!(plain.cost, td.cost);
    }
}
