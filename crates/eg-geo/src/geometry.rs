//! The geometry value model (CONCEPT:EG-083): `Point` / `LineString` / `Polygon`
//! behind one [`Geometry`] enum, each with an axis-aligned bounding box ([`Bbox`]).
//!
//! Coordinates are planar `f64` `(x, y)` (longitude/latitude or a projected plane —
//! the predicates in [`crate::predicates`] are Euclidean/planar for v1; a geodesic
//! metric is a documented follow-up). Every type derives serde so a `Geometry`
//! persists as a typed value in the engine's redb per-graph store.

use serde::{Deserialize, Serialize};

/// A single planar coordinate `(x, y)`.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    /// Euclidean distance to another point.
    pub fn distance(&self, o: &Point) -> f64 {
        let dx = self.x - o.x;
        let dy = self.y - o.y;
        (dx * dx + dy * dy).sqrt()
    }
}

/// An ordered chain of coordinates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LineString {
    pub points: Vec<Point>,
}

impl LineString {
    pub fn new(points: Vec<Point>) -> Self {
        Self { points }
    }

    /// The line's segments as `(a, b)` endpoint pairs (empty for < 2 points).
    pub fn segments(&self) -> impl Iterator<Item = (Point, Point)> + '_ {
        self.points.windows(2).map(|w| (w[0], w[1]))
    }
}

/// A polygon defined by a single exterior ring (a closed [`LineString`]). Interior
/// rings / holes are a documented follow-up (v1 models the exterior ring only), as
/// is multi-polygon.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Polygon {
    /// The exterior ring. Conventionally closed (first == last coordinate); the
    /// point-in-polygon test tolerates an unclosed ring too.
    pub exterior: LineString,
}

impl Polygon {
    pub fn new(exterior: LineString) -> Self {
        Self { exterior }
    }

    /// Ray-casting point-in-polygon (even-odd rule). Points exactly on an edge are
    /// treated as inside (boundary-inclusive), matching the OGC `within`/`contains`
    /// intent for the common case. Planar.
    pub fn contains_point(&self, p: &Point) -> bool {
        let ring = &self.exterior.points;
        if ring.len() < 3 {
            return false;
        }
        // Boundary check first (inclusive).
        for (a, b) in self.exterior.segments() {
            if point_on_segment(p, &a, &b) {
                return true;
            }
        }
        let mut inside = false;
        let n = ring.len();
        let mut j = n - 1;
        for i in 0..n {
            let pi = ring[i];
            let pj = ring[j];
            let intersect = ((pi.y > p.y) != (pj.y > p.y))
                && (p.x < (pj.x - pi.x) * (p.y - pi.y) / (pj.y - pi.y) + pi.x);
            if intersect {
                inside = !inside;
            }
            j = i;
        }
        inside
    }
}

/// One spatial value: a point, a line, or a polygon.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Geometry {
    Point(Point),
    LineString(LineString),
    Polygon(Polygon),
}

impl Geometry {
    /// The axis-aligned bounding box of this geometry (`None` for an empty geometry).
    pub fn bbox(&self) -> Option<Bbox> {
        let pts: &[Point] = match self {
            Geometry::Point(p) => std::slice::from_ref(p),
            Geometry::LineString(l) => &l.points,
            Geometry::Polygon(pg) => &pg.exterior.points,
        };
        Bbox::of_points(pts)
    }

    /// Every vertex of the geometry (used by the vertex-set predicates).
    pub fn points(&self) -> &[Point] {
        match self {
            Geometry::Point(p) => std::slice::from_ref(p),
            Geometry::LineString(l) => &l.points,
            Geometry::Polygon(pg) => &pg.exterior.points,
        }
    }
}

/// An axis-aligned bounding box `[minx, miny, maxx, maxy]`. The R-tree index and the
/// wire `Op::SpatialScan { bbox }` both speak this box.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bbox {
    pub minx: f64,
    pub miny: f64,
    pub maxx: f64,
    pub maxy: f64,
}

impl Bbox {
    pub fn new(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Self {
        Self {
            minx,
            miny,
            maxx,
            maxy,
        }
    }

    /// The bbox from a raw `[minx, miny, maxx, maxy]` wire array.
    pub fn from_array(a: [f64; 4]) -> Self {
        Self::new(a[0], a[1], a[2], a[3])
    }

    /// The bounding box enclosing all `pts` (`None` when empty).
    pub fn of_points(pts: &[Point]) -> Option<Bbox> {
        let first = pts.first()?;
        let mut b = Bbox::new(first.x, first.y, first.x, first.y);
        for p in &pts[1..] {
            b.minx = b.minx.min(p.x);
            b.miny = b.miny.min(p.y);
            b.maxx = b.maxx.max(p.x);
            b.maxy = b.maxy.max(p.y);
        }
        Some(b)
    }

    /// Grow this box to also cover `o`.
    pub fn union(&mut self, o: &Bbox) {
        self.minx = self.minx.min(o.minx);
        self.miny = self.miny.min(o.miny);
        self.maxx = self.maxx.max(o.maxx);
        self.maxy = self.maxy.max(o.maxy);
    }

    /// Do the two boxes overlap (touching edges count)?
    pub fn intersects(&self, o: &Bbox) -> bool {
        self.minx <= o.maxx && self.maxx >= o.minx && self.miny <= o.maxy && self.maxy >= o.miny
    }

    /// Does this box fully contain `o`?
    pub fn contains(&self, o: &Bbox) -> bool {
        self.minx <= o.minx && self.miny <= o.miny && self.maxx >= o.maxx && self.maxy >= o.maxy
    }

    /// Center of the box.
    pub fn center(&self) -> (f64, f64) {
        ((self.minx + self.maxx) / 2.0, (self.miny + self.maxy) / 2.0)
    }
}

/// Euclidean distance from point `p` to the segment `a→b` (planar). Also the primitive
/// the boundary-inclusive point-in-polygon test uses.
pub fn point_segment_distance(p: &Point, a: &Point, b: &Point) -> f64 {
    let dx = b.x - a.x;
    let dy = b.y - a.y;
    let len2 = dx * dx + dy * dy;
    if len2 == 0.0 {
        return p.distance(a);
    }
    // Projection parameter t of p onto the segment, clamped to [0, 1].
    let t = (((p.x - a.x) * dx + (p.y - a.y) * dy) / len2).clamp(0.0, 1.0);
    let proj = Point::new(a.x + t * dx, a.y + t * dy);
    p.distance(&proj)
}

/// Is `p` on the segment `a→b` (within a tiny planar epsilon)?
pub fn point_on_segment(p: &Point, a: &Point, b: &Point) -> bool {
    point_segment_distance(p, a, b) <= 1e-9
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbox_of_geometry() {
        let ls = Geometry::LineString(LineString::new(vec![
            Point::new(1.0, 2.0),
            Point::new(5.0, -3.0),
            Point::new(0.0, 4.0),
        ]));
        assert_eq!(ls.bbox(), Some(Bbox::new(0.0, -3.0, 5.0, 4.0)));
    }

    #[test]
    fn bbox_intersect_and_contain() {
        let a = Bbox::new(0.0, 0.0, 10.0, 10.0);
        let b = Bbox::new(5.0, 5.0, 15.0, 15.0);
        let c = Bbox::new(2.0, 2.0, 3.0, 3.0);
        assert!(a.intersects(&b));
        assert!(a.contains(&c));
        assert!(!a.contains(&b));
        assert!(!a.intersects(&Bbox::new(20.0, 20.0, 30.0, 30.0)));
    }

    #[test]
    fn point_in_polygon_raycast() {
        // A unit square.
        let sq = Polygon::new(LineString::new(vec![
            Point::new(0.0, 0.0),
            Point::new(4.0, 0.0),
            Point::new(4.0, 4.0),
            Point::new(0.0, 4.0),
            Point::new(0.0, 0.0),
        ]));
        assert!(sq.contains_point(&Point::new(2.0, 2.0))); // interior
        assert!(sq.contains_point(&Point::new(0.0, 2.0))); // on edge (inclusive)
        assert!(!sq.contains_point(&Point::new(5.0, 2.0))); // outside
    }

    #[test]
    fn point_to_segment() {
        let a = Point::new(0.0, 0.0);
        let b = Point::new(10.0, 0.0);
        assert_eq!(point_segment_distance(&Point::new(5.0, 3.0), &a, &b), 3.0);
        assert_eq!(point_segment_distance(&Point::new(-4.0, 0.0), &a, &b), 4.0); // clamps to a
    }
}
