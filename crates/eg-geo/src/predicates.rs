//! Planar spatial predicates (CONCEPT:EG-083): `within`, `intersects`, `distance`,
//! extended (CONCEPT:EG-257) to recurse over multi-geometries / collections and to
//! honour polygon interior rings (holes).
//!
//! All metrics are **Euclidean / planar** (coordinates are treated as a flat plane).
//! The geodesic (great-circle / ellipsoidal) metrics for true lon/lat inputs live in
//! [`crate::geodesic`] (CONCEPT:EG-256); the predicate signatures here are unchanged so
//! eg-plan's `geo` executor keeps compiling.
//!
//! Composite geometries flatten to atomic [`Prim`]s: a multi's predicate recurses over
//! its parts (within = every part of `a` within `b`; intersects = any part-pair
//! intersects; distance = min over part-pairs), and a polygon-with-holes is "inside"
//! only in its exterior AND outside every hole.

use crate::geometry::{point_on_segment, point_segment_distance, Geometry, Point, Prim};

/// Euclidean distance between two geometries: the MINIMUM distance between any part of
/// `a` and any part of `b` (0 when they intersect). Recurses over multis/collections.
pub fn distance(a: &Geometry, b: &Geometry) -> f64 {
    if intersects(a, b) {
        return 0.0;
    }
    let (pa, pb) = (prims(a), prims(b));
    let mut best = f64::INFINITY;
    for x in &pa {
        for y in &pb {
            best = best.min(prim_distance(x, y));
            if best == 0.0 {
                return 0.0;
            }
        }
    }
    best
}

/// Is geometry `a` spatially WITHIN geometry `b`? (Every part of `a` lies inside `b`.)
///
/// Planar, boundary-inclusive, vertex-based (matching EG-083 v1 semantics, now
/// recursing over composite `b`): every vertex of `a` must lie in *some* primitive of
/// `b` — inside a polygon (hole-aware), on a line, or equal to a point.
pub fn within(a: &Geometry, b: &Geometry) -> bool {
    let bp = prims(b);
    if bp.is_empty() {
        return false;
    }
    let verts = a.all_vertices();
    if verts.is_empty() {
        return false;
    }
    verts.iter().all(|p| bp.iter().any(|q| point_in_prim(p, q)))
}

/// Do geometries `a` and `b` share any point (planar)? Fast bbox reject first, then an
/// exact primitive-pair test (point-in-polygon, segment crossing, vertex containment).
pub fn intersects(a: &Geometry, b: &Geometry) -> bool {
    match (a.bbox(), b.bbox()) {
        (Some(ba), Some(bb)) if !ba.intersects(&bb) => return false,
        (Some(_), Some(_)) => {}
        _ => return false, // an empty geometry intersects nothing
    }
    let (pa, pb) = (prims(a), prims(b));
    for x in &pa {
        for y in &pb {
            if prim_intersect(x, y) {
                return true;
            }
        }
    }
    false
}

// ── primitive-level kernels ──────────────────────────────────────────────────────

/// Flatten a geometry into its atomic single primitives.
fn prims(g: &Geometry) -> Vec<Prim<'_>> {
    let mut out = Vec::new();
    g.primitives(&mut out);
    out
}

/// The boundary segments of a primitive (empty for a point). For a polygon this is the
/// exterior ring PLUS every interior (hole) ring, each treated as closed.
fn prim_segments<'a>(prim: &Prim<'a>) -> Vec<(Point, Point)> {
    match prim {
        Prim::Point(_) => Vec::new(),
        Prim::Line(l) => l.segments().collect(),
        Prim::Poly(pg) => {
            let mut segs = Vec::new();
            for ring in std::iter::once(&pg.exterior).chain(pg.interiors.iter()) {
                let n = ring.points.len();
                for i in 0..n.saturating_sub(1) {
                    segs.push((ring.points[i], ring.points[i + 1]));
                }
                // implicit wrap edge if the ring isn't explicitly closed
                if n >= 3 && ring.points[0] != ring.points[n - 1] {
                    segs.push((ring.points[n - 1], ring.points[0]));
                }
            }
            segs
        }
    }
}

/// Every vertex of a primitive (polygon = exterior + all hole rings).
fn prim_vertices<'a>(prim: &Prim<'a>) -> Vec<Point> {
    match prim {
        Prim::Point(p) => vec![**p],
        Prim::Line(l) => l.points.clone(),
        Prim::Poly(pg) => {
            let mut v = pg.exterior.points.clone();
            for h in &pg.interiors {
                v.extend_from_slice(&h.points);
            }
            v
        }
    }
}

/// Does point `p` touch primitive `prim` (equal to a point, on a line, or inside a
/// hole-aware polygon)?
fn point_in_prim(p: &Point, prim: &Prim<'_>) -> bool {
    match prim {
        Prim::Point(q) => p == *q,
        Prim::Line(l) => l.segments().any(|(a, b)| point_on_segment(p, &a, &b)),
        Prim::Poly(pg) => pg.contains_point(p),
    }
}

/// Minimum Euclidean distance from a point to a primitive.
fn point_to_prim(p: &Point, prim: &Prim<'_>) -> f64 {
    match prim {
        Prim::Point(q) => p.distance(q),
        Prim::Line(l) => l
            .segments()
            .map(|(a, b)| point_segment_distance(p, &a, &b))
            .fold(f64::INFINITY, f64::min),
        Prim::Poly(pg) => {
            if pg.contains_point(p) {
                0.0
            } else {
                prim_segments(prim)
                    .iter()
                    .map(|(a, b)| point_segment_distance(p, a, b))
                    .fold(f64::INFINITY, f64::min)
            }
        }
    }
}

/// Minimum Euclidean distance between two primitives.
fn prim_distance(x: &Prim<'_>, y: &Prim<'_>) -> f64 {
    let mut best = f64::INFINITY;
    for v in prim_vertices(x) {
        best = best.min(point_to_prim(&v, y));
    }
    for v in prim_vertices(y) {
        best = best.min(point_to_prim(&v, x));
    }
    best
}

/// Do two primitives share any point (planar)?
fn prim_intersect(x: &Prim<'_>, y: &Prim<'_>) -> bool {
    // Any vertex of one lying in/on the other (handles point cases + full nesting).
    if prim_vertices(x).iter().any(|v| point_in_prim(v, y))
        || prim_vertices(y).iter().any(|v| point_in_prim(v, x))
    {
        return true;
    }
    // Boundary segment crossing.
    for (a1, a2) in prim_segments(x) {
        for (b1, b2) in prim_segments(y) {
            if seg_seg_intersect(&a1, &a2, &b1, &b2) {
                return true;
            }
        }
    }
    false
}

/// Orientation of the ordered triple `(a, b, c)`: >0 CCW, <0 CW, 0 collinear.
fn orient(a: &Point, b: &Point, c: &Point) -> f64 {
    (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)
}

/// Do segments `p1→p2` and `p3→p4` intersect (proper or at an endpoint)?
fn seg_seg_intersect(p1: &Point, p2: &Point, p3: &Point, p4: &Point) -> bool {
    let d1 = orient(p3, p4, p1);
    let d2 = orient(p3, p4, p2);
    let d3 = orient(p1, p2, p3);
    let d4 = orient(p1, p2, p4);
    if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    {
        return true;
    }
    // Collinear-overlap / touching endpoints.
    (d1 == 0.0 && point_on_segment(p1, p3, p4))
        || (d2 == 0.0 && point_on_segment(p2, p3, p4))
        || (d3 == 0.0 && point_on_segment(p3, p1, p2))
        || (d4 == 0.0 && point_on_segment(p4, p1, p2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{LineString, Polygon};

    fn square() -> Geometry {
        Geometry::Polygon(Polygon::new(LineString::new(vec![
            Point::new(0.0, 0.0),
            Point::new(4.0, 0.0),
            Point::new(4.0, 4.0),
            Point::new(0.0, 4.0),
            Point::new(0.0, 0.0),
        ])))
    }

    #[test]
    fn point_within_polygon() {
        let inside = Geometry::Point(Point::new(2.0, 2.0));
        let outside = Geometry::Point(Point::new(9.0, 9.0));
        assert!(within(&inside, &square()));
        assert!(!within(&outside, &square()));
    }

    #[test]
    fn intersects_point_polygon_and_segments() {
        // point inside
        assert!(intersects(&Geometry::Point(Point::new(1.0, 1.0)), &square()));
        // point far away
        assert!(!intersects(
            &Geometry::Point(Point::new(50.0, 50.0)),
            &square()
        ));
        // a line crossing the square boundary
        let crossing = Geometry::LineString(LineString::new(vec![
            Point::new(-1.0, 2.0),
            Point::new(5.0, 2.0),
        ]));
        assert!(intersects(&crossing, &square()));
        // a line entirely outside
        let away = Geometry::LineString(LineString::new(vec![
            Point::new(10.0, 10.0),
            Point::new(20.0, 20.0),
        ]));
        assert!(!intersects(&away, &square()));
    }

    #[test]
    fn distance_zero_when_contained_else_gap() {
        // point inside → 0
        assert_eq!(distance(&Geometry::Point(Point::new(2.0, 2.0)), &square()), 0.0);
        // point 6 to the right of the square's right edge (x=4) at y=2 → x=10 ⇒ 6
        let d = distance(&Geometry::Point(Point::new(10.0, 2.0)), &square());
        assert!((d - 6.0).abs() < 1e-9, "expected 6.0 got {d}");
        // two points
        let d2 = distance(
            &Geometry::Point(Point::new(0.0, 0.0)),
            &Geometry::Point(Point::new(3.0, 4.0)),
        );
        assert!((d2 - 5.0).abs() < 1e-9);
    }

    #[test]
    fn point_in_polygon_with_hole_predicate() {
        // 10×10 square with a central 4×4 hole (CONCEPT:EG-257).
        let ext = LineString::new(vec![
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
        let holed = Geometry::Polygon(Polygon::with_interiors(ext, vec![hole]));
        // Inside the ring but outside the hole → within; distance 0.
        let in_ring = Geometry::Point(Point::new(1.0, 1.0));
        assert!(within(&in_ring, &holed));
        assert!(intersects(&in_ring, &holed));
        // In the hole → NOT within; a positive distance to the polygon.
        let in_hole = Geometry::Point(Point::new(5.0, 5.0));
        assert!(!within(&in_hole, &holed));
        assert!(!intersects(&in_hole, &holed));
        let d = distance(&in_hole, &holed);
        assert!(d > 0.0 && (d - 2.0).abs() < 1e-9, "hole-center distance {d}");
    }

    #[test]
    fn multipoint_within_and_intersects() {
        // All parts inside → within; any part inside → intersects.
        let all_in = Geometry::MultiPoint(vec![Point::new(1.0, 1.0), Point::new(3.0, 3.0)]);
        assert!(within(&all_in, &square()));
        let some_out = Geometry::MultiPoint(vec![Point::new(1.0, 1.0), Point::new(9.0, 9.0)]);
        assert!(!within(&some_out, &square()));
        assert!(intersects(&some_out, &square())); // one part is inside
        let all_out = Geometry::MultiPoint(vec![Point::new(9.0, 9.0), Point::new(20.0, 20.0)]);
        assert!(!intersects(&all_out, &square()));
    }

    #[test]
    fn multipolygon_distance_takes_nearest_part() {
        let mpoly = Geometry::MultiPolygon(vec![
            Polygon::new(LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(2.0, 0.0),
                Point::new(2.0, 2.0),
                Point::new(0.0, 2.0),
                Point::new(0.0, 0.0),
            ])),
            Polygon::new(LineString::new(vec![
                Point::new(100.0, 100.0),
                Point::new(102.0, 100.0),
                Point::new(102.0, 102.0),
                Point::new(100.0, 100.0),
            ])),
        ]);
        // A point at (3,1) is 1.0 from the near square's right edge.
        let d = distance(&Geometry::Point(Point::new(3.0, 1.0)), &mpoly);
        assert!((d - 1.0).abs() < 1e-9, "nearest-part distance {d}");
    }
}
