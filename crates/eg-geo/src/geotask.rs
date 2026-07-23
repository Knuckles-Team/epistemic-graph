//! **Map-based task tracking** — geolocated tasks + spatial assignment queries
//! (CONCEPT:EG-KG.domains.geo-task).
//!
//! The logistics / field-service surface tracks *work on a map*: deliveries, inspections,
//! service calls — each pinned to a location, each with a status and an optional service
//! area. This module gives that a pure-library model ([`GeoTask`]) plus the spatial queries
//! a dispatcher needs, backed by EG-263's durable STR R-tree ([`crate::strtree`]):
//!
//! * **Spatial selection** — [`GeoTaskIndex::tasks_in_bbox`] (a map viewport / region query)
//!   and [`GeoTaskIndex::tasks_in_polygon`] (an arbitrary zone), both R-tree-pruned.
//! * **Proximity** — [`GeoTaskIndex::nearest`] (k-nearest tasks to a point, R-tree
//!   branch-and-bound) and [`GeoTaskIndex::nearest_task`].
//! * **Assignment** — [`GeoTaskIndex::assign_nearest_task`] (nearest task to a resource),
//!   [`nearest_resource`] (nearest resource to a task) and [`GeoTaskIndex::greedy_assign`]
//!   (a whole fleet → pending tasks, nearest-first).
//! * **Service-area** — [`GeoTaskIndex::tasks_covering`]: which tasks' `service_area`
//!   polygon covers a query point.
//!
//! [`GeoTask`] is a plain serde struct: the caller owns persistence (it can store a task as
//! a typed value in the redb per-graph store) — this layer does NOT touch eg-core's node
//! model. The R-tree indexes each task's **location** (a zero-area box), so the index is a
//! pure spatial accelerator a caller rebuilds (or persists, via the STR R-tree's serde) as
//! tasks change.

use serde::{Deserialize, Serialize};

use crate::geometry::{Bbox, Geometry, Point, Polygon};
use crate::strtree::StrTree;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

/// The lifecycle status of a [`GeoTask`] (CONCEPT:EG-KG.domains.geo-task). `Pending` tasks are the ones the
/// assignment queries hand out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Unassigned, awaiting a resource — the assignable state.
    Pending,
    /// Assigned to a resource but not yet started.
    Assigned,
    /// Work in progress.
    InProgress,
    /// Finished.
    Done,
    /// Cancelled / withdrawn.
    Cancelled,
}

impl TaskStatus {
    /// Is this task assignable (i.e. `Pending`)?
    pub fn is_pending(&self) -> bool {
        matches!(self, TaskStatus::Pending)
    }
}

/// A geolocated unit of work (CONCEPT:EG-KG.domains.geo-task): a stable `id`, a map `location`, a `status`
/// and an optional `service_area` geometry (the region the task covers — e.g. a delivery
/// zone). Plain serde so the caller can persist it however it likes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeoTask {
    pub id: u64,
    pub location: Point,
    pub status: TaskStatus,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub service_area: Option<Geometry>,
}

impl GeoTask {
    /// A pending task at `location` with no service area.
    pub fn new(id: u64, location: Point) -> Self {
        Self {
            id,
            location,
            status: TaskStatus::Pending,
            service_area: None,
        }
    }

    /// This task with an explicit status (builder style).
    pub fn with_status(mut self, status: TaskStatus) -> Self {
        self.status = status;
        self
    }

    /// This task with a service-area geometry (builder style).
    pub fn with_service_area(mut self, area: Geometry) -> Self {
        self.service_area = Some(area);
        self
    }

    /// Does this task's `service_area` cover `point`? `false` when there is no area, or the
    /// area is not an areal (polygonal) geometry.
    pub fn covers(&self, point: &Point) -> bool {
        self.service_area
            .as_ref()
            .is_some_and(|g| geometry_covers_point(g, point))
    }
}

/// A resource-to-task assignment result (CONCEPT:EG-KG.domains.geo-task): the resource's index (into the
/// caller's slice), the assigned task's index (into the index's task list) and the planar
/// distance between them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Assignment {
    pub resource: usize,
    pub task: usize,
    pub distance: f64,
}

/// A spatial index over a set of [`GeoTask`]s (CONCEPT:EG-KG.domains.geo-task): owns the tasks and an STR
/// R-tree over their locations. Rebuild it (cheap, bulk-loaded) whenever the task set
/// changes, or persist the tasks + tree via serde.
#[derive(Clone, Debug)]
pub struct GeoTaskIndex {
    tasks: Vec<GeoTask>,
    tree: StrTree,
}

impl GeoTaskIndex {
    /// Build the index over `tasks`, indexing each task by its (zero-area) location box.
    pub fn build(tasks: Vec<GeoTask>) -> Self {
        let boxes: Vec<(usize, Bbox)> = tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (i, point_box(&t.location)))
            .collect();
        let tree = StrTree::build(&boxes);
        Self { tasks, tree }
    }

    /// The indexed tasks.
    pub fn tasks(&self) -> &[GeoTask] {
        &self.tasks
    }

    /// The number of indexed tasks.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// The task at internal index `i`.
    pub fn task(&self, i: usize) -> &GeoTask {
        &self.tasks[i]
    }

    // ── spatial selection ───────────────────────────────────────────────────────────────

    /// Every task whose location lies within `bbox` (CONCEPT:EG-KG.domains.geo-task) — a map-viewport /
    /// region query, R-tree pruned. Order unspecified.
    pub fn tasks_in_bbox(&self, bbox: &Bbox) -> Vec<&GeoTask> {
        self.tree
            .query_bbox(bbox)
            .into_iter()
            .map(|i| &self.tasks[i])
            .collect()
    }

    /// Every task whose location lies inside `polygon` (CONCEPT:EG-KG.domains.geo-task): the R-tree prunes to
    /// the polygon's bounding box, then an exact point-in-polygon test (hole-aware) filters.
    pub fn tasks_in_polygon(&self, polygon: &Polygon) -> Vec<&GeoTask> {
        let Some(bbox) = Geometry::Polygon(polygon.clone()).bbox() else {
            return Vec::new();
        };
        self.tree
            .query_bbox(&bbox)
            .into_iter()
            .map(|i| &self.tasks[i])
            .filter(|t| polygon.contains_point(&t.location))
            .collect()
    }

    // ── proximity ───────────────────────────────────────────────────────────────────────

    /// The up-to-`k` tasks nearest to `point`, nearest first, each with its planar distance
    /// (CONCEPT:EG-KG.domains.geo-task). Best-first branch-and-bound over the R-tree.
    pub fn nearest(&self, point: &Point, k: usize) -> Vec<(&GeoTask, f64)> {
        self.tree
            .nearest(point, k)
            .into_iter()
            .map(|(i, d)| (&self.tasks[i], d))
            .collect()
    }

    /// The single nearest task to `point` (CONCEPT:EG-KG.domains.geo-task), or `None` when the index is empty.
    pub fn nearest_task(&self, point: &Point) -> Option<(&GeoTask, f64)> {
        self.nearest(point, 1).into_iter().next()
    }

    // ── assignment ──────────────────────────────────────────────────────────────────────

    /// The nearest **pending** task to a `resource` location (CONCEPT:EG-KG.domains.geo-task) — the "give
    /// this driver their next job" query. Scans nearest-first and returns the first pending
    /// hit; `None` if no pending task exists.
    pub fn assign_nearest_task(&self, resource: &Point) -> Option<(&GeoTask, f64)> {
        // Grow k until a pending task is found or the whole set is exhausted.
        let mut k = 8.min(self.tasks.len().max(1));
        loop {
            let hits = self.nearest(resource, k);
            if let Some(hit) = hits.iter().find(|(t, _)| t.status.is_pending()) {
                return Some(*hit);
            }
            if hits.len() >= self.tasks.len() {
                return None; // saw everything, nothing pending
            }
            k = (k * 2).min(self.tasks.len());
        }
    }

    /// Greedily assign a fleet of `resources` to **pending** tasks, nearest pair first
    /// (CONCEPT:EG-KG.domains.geo-task): repeatedly take the closest (resource, unassigned-pending-task)
    /// pair until resources or pending tasks run out. Each resource and each task is used at
    /// most once. Returns the [`Assignment`]s (planar distance).
    pub fn greedy_assign(&self, resources: &[Point]) -> Vec<Assignment> {
        // Candidate pending task indices.
        let pending: Vec<usize> = self
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, t)| t.status.is_pending())
            .map(|(i, _)| i)
            .collect();
        // All (distance, resource, task) pairs, sorted ascending by distance.
        let mut pairs: Vec<(f64, usize, usize)> = Vec::new();
        for (ri, r) in resources.iter().enumerate() {
            for &ti in &pending {
                pairs.push((r.distance(&self.tasks[ti].location), ri, ti));
            }
        }
        pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        let mut used_res = vec![false; resources.len()];
        let mut used_task = std::collections::HashSet::new();
        let mut out = Vec::new();
        for (d, ri, ti) in pairs {
            if used_res[ri] || used_task.contains(&ti) {
                continue;
            }
            used_res[ri] = true;
            used_task.insert(ti);
            out.push(Assignment {
                resource: ri,
                task: ti,
                distance: d,
            });
        }
        out
    }

    // ── service area ────────────────────────────────────────────────────────────────────

    /// Every task whose `service_area` covers `point` (CONCEPT:EG-KG.domains.geo-task) — "which delivery
    /// zones include this address". A linear scan (service areas are arbitrary polygons, not
    /// point-indexed); tasks without an areal service area never match.
    pub fn tasks_covering(&self, point: &Point) -> Vec<&GeoTask> {
        self.tasks.iter().filter(|t| t.covers(point)).collect()
    }
}

/// The nearest resource (by planar distance) to a task `location` (CONCEPT:EG-KG.domains.geo-task): the
/// "who should take this job" query. Returns the resource's index into `resources` and the
/// distance, or `None` when `resources` is empty.
pub fn nearest_resource(location: &Point, resources: &[Point]) -> Option<(usize, f64)> {
    resources
        .iter()
        .enumerate()
        .map(|(i, r)| (i, r.distance(location)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
}

/// A zero-area bounding box at a point (how a task location enters the R-tree).
fn point_box(p: &Point) -> Bbox {
    Bbox::new(p.x, p.y, p.x, p.y)
}

/// Does an areal geometry cover `point`? Handles `Polygon` / `MultiPolygon` (hole-aware,
/// boundary-inclusive); every other geometry kind is treated as non-areal ⇒ `false`.
fn geometry_covers_point(g: &Geometry, point: &Point) -> bool {
    match g {
        Geometry::Polygon(pg) => pg.contains_point(point),
        Geometry::MultiPolygon(pgs) => pgs.iter().any(|pg| pg.contains_point(point)),
        Geometry::GeometryCollection(gs) => gs.iter().any(|g| geometry_covers_point(g, point)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::LineString;

    fn sample_tasks() -> Vec<GeoTask> {
        vec![
            GeoTask::new(1, Point::new(0.0, 0.0)),
            GeoTask::new(2, Point::new(10.0, 10.0)),
            GeoTask::new(3, Point::new(5.0, 5.0)).with_status(TaskStatus::Done),
            GeoTask::new(4, Point::new(2.0, 3.0)),
            GeoTask::new(5, Point::new(-4.0, 8.0)),
        ]
    }

    fn ids(tasks: &[&GeoTask]) -> Vec<u64> {
        let mut v: Vec<u64> = tasks.iter().map(|t| t.id).collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn eg267_tasks_in_bbox_via_rtree() {
        let idx = GeoTaskIndex::build(sample_tasks());
        // Box around the origin cluster.
        let hits = idx.tasks_in_bbox(&Bbox::new(-1.0, -1.0, 6.0, 6.0));
        assert_eq!(ids(&hits), vec![1, 3, 4], "tasks inside the query box");
        // Empty region.
        assert!(idx
            .tasks_in_bbox(&Bbox::new(100.0, 100.0, 200.0, 200.0))
            .is_empty());
    }

    #[test]
    fn eg267_tasks_in_polygon_exact_filter() {
        let idx = GeoTaskIndex::build(sample_tasks());
        // A triangle covering the lower-left tasks but not (10,10).
        let tri = Polygon::new(
            LineString::new(vec![
                Point::new(-1.0, -1.0),
                Point::new(8.0, -1.0),
                Point::new(-1.0, 8.0),
                Point::new(-1.0, -1.0),
            ]),
            Vec::new(),
        );
        let hits = idx.tasks_in_polygon(&tri);
        // Hypotenuse is x+y=7: (0,0) and (2,3) are inside; (5,5) [x+y=10], (10,10) and
        // (-4,8) are outside.
        assert_eq!(ids(&hits), vec![1, 4]);
    }

    #[test]
    fn eg267_nearest_n_tasks_to_point() {
        let idx = GeoTaskIndex::build(sample_tasks());
        let near = idx.nearest(&Point::new(0.0, 0.0), 2);
        assert_eq!(near.len(), 2);
        assert_eq!(near[0].0.id, 1, "task at origin is nearest");
        assert_eq!(near[0].1, 0.0);
        // Second nearest is (2,3) at distance sqrt(13) ≈ 3.606.
        assert_eq!(near[1].0.id, 4);
        assert!((near[1].1 - 13f64.sqrt()).abs() < 1e-9);
    }

    #[test]
    fn eg267_assign_nearest_pending_task_skips_non_pending() {
        let idx = GeoTaskIndex::build(sample_tasks());
        // Nearest to (5,5) is task 3 (Done) — assignment must skip it and pick a pending one.
        let (t, _d) = idx.assign_nearest_task(&Point::new(5.0, 5.0)).unwrap();
        assert!(t.status.is_pending());
        assert_ne!(t.id, 3, "the Done task is not assignable");
        assert_eq!(t.id, 4, "nearest pending is (2,3)");
    }

    #[test]
    fn eg267_nearest_resource_to_task() {
        let resources = vec![
            Point::new(0.0, 0.0),
            Point::new(9.0, 9.0),
            Point::new(-5.0, 7.0),
        ];
        // Task at (10,10): nearest resource is index 1 (9,9).
        let (ri, d) = nearest_resource(&Point::new(10.0, 10.0), &resources).unwrap();
        assert_eq!(ri, 1);
        assert!((d - 2f64.sqrt()).abs() < 1e-9);
        assert!(nearest_resource(&Point::new(0.0, 0.0), &[]).is_none());
    }

    #[test]
    fn eg267_greedy_fleet_assignment_is_one_to_one_nearest_first() {
        let idx = GeoTaskIndex::build(sample_tasks());
        // Two drivers near two different pending tasks.
        let resources = vec![Point::new(0.1, 0.1), Point::new(9.5, 9.5)];
        let assigns = idx.greedy_assign(&resources);
        assert_eq!(assigns.len(), 2, "both resources get a task");
        // Resource 0 → task id 1 (origin); resource 1 → task id 2 (10,10).
        let by_res: std::collections::HashMap<usize, u64> = assigns
            .iter()
            .map(|a| (a.resource, idx.task(a.task).id))
            .collect();
        assert_eq!(by_res[&0], 1);
        assert_eq!(by_res[&1], 2);
        // No task assigned twice.
        let mut task_ids: Vec<usize> = assigns.iter().map(|a| a.task).collect();
        task_ids.sort_unstable();
        task_ids.dedup();
        assert_eq!(task_ids.len(), 2);
    }

    #[test]
    fn eg267_service_area_covering_query() {
        // A task whose service area is a square delivery zone.
        let zone = Geometry::Polygon(Polygon::new(
            LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(10.0, 0.0),
                Point::new(10.0, 10.0),
                Point::new(0.0, 10.0),
                Point::new(0.0, 0.0),
            ]),
            Vec::new(),
        ));
        let tasks = vec![
            GeoTask::new(1, Point::new(5.0, 5.0)).with_service_area(zone),
            GeoTask::new(2, Point::new(50.0, 50.0)), // no service area
        ];
        let idx = GeoTaskIndex::build(tasks);
        let covering = idx.tasks_covering(&Point::new(3.0, 4.0));
        assert_eq!(ids(&covering), vec![1], "point inside zone 1 only");
        assert!(idx.tasks_covering(&Point::new(20.0, 20.0)).is_empty());
    }

    #[test]
    fn eg267_geotask_serde_round_trip() {
        // A task persists via serde (the caller owns storage) and reloads identically.
        let t = GeoTask::new(7, Point::new(1.5, -2.5))
            .with_status(TaskStatus::InProgress)
            .with_service_area(Geometry::Polygon(Polygon::new(
                LineString::new(vec![
                    Point::new(0.0, 0.0),
                    Point::new(3.0, 0.0),
                    Point::new(3.0, 3.0),
                    Point::new(0.0, 0.0),
                ]),
                Vec::new(),
            )));
        let s = serde_json::to_string(&t).unwrap();
        let back: GeoTask = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }
}
