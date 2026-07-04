//! The geometry value model: single geometries `Point` / `LineString` / `Polygon`
//! (CONCEPT:EG-KG.ontology.singles-concept) plus the **full OGC collection model** `MultiPoint` /
//! `MultiLineString` / `MultiPolygon` / `GeometryCollection` and polygon **interior
//! rings (holes)** (CONCEPT:EG-KG.domains.geometry-collections), all behind one [`Geometry`] enum, each with an
//! axis-aligned bounding box ([`Bbox`]).
//!
//! Coordinates are planar `f64` `(x, y)` (longitude/latitude or a projected plane —
//! the predicates in [`crate::predicates`] are Euclidean/planar; the geodesic metrics
//! in [`crate::geodesic`] (CONCEPT:EG-KG.ontology.concept-8) treat `x = lon`, `y = lat` in degrees).
//! Every type derives serde so a `Geometry` persists as a typed value in the engine's
//! redb per-graph store.

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

/// A polygon defined by an exterior ring (a closed [`LineString`]) plus zero or more
/// **interior rings (holes)** (CONCEPT:EG-KG.domains.geometry-collections). Backward-compatible: [`Polygon::new`]
/// still takes only the exterior (holes default empty); [`Polygon::with_interiors`]
/// adds holes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Polygon {
    /// The exterior ring. Conventionally closed (first == last coordinate); the
    /// point-in-polygon test tolerates an unclosed ring too (implicit wrap edge).
    pub exterior: LineString,
    /// Interior rings (holes). A point inside the exterior but inside any hole is
    /// **outside** the polygon. Empty for a solid polygon.
    #[serde(default)]
    pub interiors: Vec<LineString>,
}

impl Polygon {
    /// A solid polygon from just its exterior ring (no holes). Unchanged EG-083 API.
    pub fn new(exterior: LineString) -> Self {
        Self {
            exterior,
            interiors: Vec::new(),
        }
    }

    /// A polygon with an exterior ring and interior rings (holes) (CONCEPT:EG-KG.domains.geometry-collections).
    pub fn with_interiors(exterior: LineString, interiors: Vec<LineString>) -> Self {
        Self {
            exterior,
            interiors,
        }
    }

    /// Ray-casting point-in-polygon (even-odd rule), hole-aware (CONCEPT:EG-KG.domains.geometry-collections): a
    /// point is inside iff it is inside the exterior ring AND not strictly inside any
    /// interior ring. Points exactly on the exterior boundary — or on a hole boundary —
    /// are treated as inside (boundary-inclusive), matching the OGC `within`/`contains`
    /// intent for the common case. Planar.
    pub fn contains_point(&self, p: &Point) -> bool {
        // Outside the exterior (on-edge counts as inside) ⇒ not contained.
        if !ring_contains(&self.exterior.points, p, true) {
            return false;
        }
        // Strictly inside a hole ⇒ outside the polygon; on a hole edge ⇒ still inside.
        for hole in &self.interiors {
            if ring_contains(&hole.points, p, false) {
                return false;
            }
        }
        true
    }
}

/// One spatial value: a single geometry, a homogeneous multi-geometry, or a mixed
/// collection (CONCEPT:EG-KG.ontology.singles-concept for the singles, CONCEPT:EG-KG.domains.geometry-collections for the collections).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Geometry {
    Point(Point),
    LineString(LineString),
    Polygon(Polygon),
    MultiPoint(Vec<Point>),
    MultiLineString(Vec<LineString>),
    MultiPolygon(Vec<Polygon>),
    GeometryCollection(Vec<Geometry>),
}

/// A borrowed reference to one atomic (single) primitive of a geometry — the flattened
/// unit the planar predicates operate on. Internal to the crate.
pub(crate) enum Prim<'a> {
    Point(&'a Point),
    Line(&'a LineString),
    Poly(&'a Polygon),
}

impl Geometry {
    /// The axis-aligned bounding box of this geometry (`None` for an empty geometry).
    /// A multi/collection's bbox is the union of its parts' boxes (CONCEPT:EG-KG.domains.geometry-collections).
    pub fn bbox(&self) -> Option<Bbox> {
        match self {
            Geometry::Point(p) => Bbox::of_points(std::slice::from_ref(p)),
            Geometry::LineString(l) => Bbox::of_points(&l.points),
            Geometry::Polygon(pg) => Bbox::of_points(&pg.exterior.points),
            Geometry::MultiPoint(ps) => Bbox::of_points(ps),
            Geometry::MultiLineString(ls) => union_bbox(ls.iter().flat_map(|l| l.points.iter())),
            Geometry::MultiPolygon(pgs) => {
                union_bbox(pgs.iter().flat_map(|pg| pg.exterior.points.iter()))
            }
            Geometry::GeometryCollection(gs) => {
                let mut acc: Option<Bbox> = None;
                for g in gs {
                    if let Some(b) = g.bbox() {
                        match &mut acc {
                            Some(a) => a.union(&b),
                            None => acc = Some(b),
                        }
                    }
                }
                acc
            }
        }
    }

    /// The vertices of the FIRST (or only) ring/chain of a *single* geometry
    /// (`Point`/`LineString`/`Polygon`-exterior). Retained for EG-083 backward
    /// compatibility; composite geometries return an empty slice — use the internal
    /// [`Geometry::primitives`] flattening (or [`Geometry::all_vertices`]) instead.
    pub fn points(&self) -> &[Point] {
        match self {
            Geometry::Point(p) => std::slice::from_ref(p),
            Geometry::LineString(l) => &l.points,
            Geometry::Polygon(pg) => &pg.exterior.points,
            _ => &[],
        }
    }

    /// Flatten this geometry into its atomic single primitives (recursing through multis
    /// and collections), appending borrowed refs to `out` (CONCEPT:EG-KG.domains.geometry-collections).
    pub(crate) fn primitives<'a>(&'a self, out: &mut Vec<Prim<'a>>) {
        match self {
            Geometry::Point(p) => out.push(Prim::Point(p)),
            Geometry::LineString(l) => out.push(Prim::Line(l)),
            Geometry::Polygon(pg) => out.push(Prim::Poly(pg)),
            Geometry::MultiPoint(ps) => ps.iter().for_each(|p| out.push(Prim::Point(p))),
            Geometry::MultiLineString(ls) => ls.iter().for_each(|l| out.push(Prim::Line(l))),
            Geometry::MultiPolygon(pgs) => pgs.iter().for_each(|pg| out.push(Prim::Poly(pg))),
            Geometry::GeometryCollection(gs) => gs.iter().for_each(|g| g.primitives(out)),
        }
    }

    /// Every vertex of the geometry, across all parts and (for polygons) interior rings.
    pub fn all_vertices(&self) -> Vec<Point> {
        let mut prims = Vec::new();
        self.primitives(&mut prims);
        let mut out = Vec::new();
        for p in &prims {
            match p {
                Prim::Point(q) => out.push(**q),
                Prim::Line(l) => out.extend_from_slice(&l.points),
                Prim::Poly(pg) => {
                    out.extend_from_slice(&pg.exterior.points);
                    for h in &pg.interiors {
                        out.extend_from_slice(&h.points);
                    }
                }
            }
        }
        out
    }
}

/// Bounding box of an iterator of points (`None` when empty).
fn union_bbox<'a>(pts: impl Iterator<Item = &'a Point>) -> Option<Bbox> {
    let mut acc: Option<Bbox> = None;
    for p in pts {
        match &mut acc {
            Some(b) => {
                b.minx = b.minx.min(p.x);
                b.miny = b.miny.min(p.y);
                b.maxx = b.maxx.max(p.x);
                b.maxy = b.maxy.max(p.y);
            }
            None => acc = Some(Bbox::new(p.x, p.y, p.x, p.y)),
        }
    }
    acc
}

/// Ray-cast even-odd point-in-ring over a ring treated as **closed** (implicit wrap
/// edge). `boundary_inside` decides how an on-edge point is classified (exterior rings
/// pass `true`; holes pass `false` so a point on a hole edge stays inside the polygon).
/// Fewer than 3 vertices ⇒ not contained.
fn ring_contains(ring: &[Point], p: &Point, boundary_inside: bool) -> bool {
    let n = ring.len();
    if n < 3 {
        return false;
    }
    // Boundary check first (over the closed ring, including the wrap edge).
    for i in 0..n {
        let a = ring[i];
        let b = ring[(i + 1) % n];
        if point_on_segment(p, &a, &b) {
            return boundary_inside;
        }
    }
    let mut inside = false;
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
    fn point_in_polygon_with_hole() {
        // 10×10 square with a 4×4 hole in the middle (CONCEPT:EG-KG.domains.geometry-collections).
        let exterior = LineString::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);
        let hole = LineString::new(vec![
            Point::new(3.0, 3.0),
            Point::new(7.0, 3.0),
            Point::new(7.0, 7.0),
            Point::new(3.0, 7.0),
            Point::new(3.0, 3.0),
        ]);
        let pg = Polygon::with_interiors(exterior, vec![hole]);
        assert!(pg.contains_point(&Point::new(1.0, 1.0))); // in the ring, outside hole
        assert!(!pg.contains_point(&Point::new(5.0, 5.0))); // inside the hole → outside
        assert!(pg.contains_point(&Point::new(3.0, 5.0))); // on the hole edge → inside
        assert!(!pg.contains_point(&Point::new(11.0, 5.0))); // outside exterior
    }

    #[test]
    fn multi_bbox_is_union_of_parts() {
        let mp = Geometry::MultiPoint(vec![
            Point::new(0.0, 0.0),
            Point::new(5.0, -2.0),
            Point::new(-1.0, 3.0),
        ]);
        assert_eq!(mp.bbox(), Some(Bbox::new(-1.0, -2.0, 5.0, 3.0)));

        let mpoly = Geometry::MultiPolygon(vec![
            Polygon::new(LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(1.0, 0.0),
                Point::new(1.0, 1.0),
                Point::new(0.0, 0.0),
            ])),
            Polygon::new(LineString::new(vec![
                Point::new(10.0, 10.0),
                Point::new(12.0, 10.0),
                Point::new(12.0, 12.0),
                Point::new(10.0, 10.0),
            ])),
        ]);
        assert_eq!(mpoly.bbox(), Some(Bbox::new(0.0, 0.0, 12.0, 12.0)));
    }

    #[test]
    fn point_to_segment() {
        let a = Point::new(0.0, 0.0);
        let b = Point::new(10.0, 0.0);
        assert_eq!(point_segment_distance(&Point::new(5.0, 3.0), &a, &b), 3.0);
        assert_eq!(point_segment_distance(&Point::new(-4.0, 0.0), &a, &b), 4.0);
        // clamps to a
    }
}
