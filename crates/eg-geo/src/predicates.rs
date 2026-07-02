//! Planar spatial predicates (CONCEPT:EG-083): `within`, `intersects`, `distance`.
//!
//! All metrics are **Euclidean / planar** for v1 (coordinates are treated as a flat
//! plane). A geodesic (great-circle / ellipsoidal) metric for true lon/lat inputs is
//! a documented follow-up — the predicate signatures stay the same, only the distance
//! kernel changes.

use crate::geometry::{point_segment_distance, Geometry, Point};

/// Euclidean distance between two geometries: the MINIMUM distance between any part of
/// `a` and any part of `b` (0 when they intersect). Points, line segments and polygon
/// exterior rings are all reduced to point/segment pairs.
pub fn distance(a: &Geometry, b: &Geometry) -> f64 {
    // Containment ⇒ zero distance (a point inside a polygon, etc.).
    if intersects(a, b) {
        return 0.0;
    }
    let a_pts = a.points();
    let b_pts = b.points();
    let mut best = f64::INFINITY;

    // vertex(a) → segment(b) and vertex(b) → segment(a): the min distance between two
    // polylines is always attained at a vertex-to-segment pair.
    for p in a_pts {
        best = best.min(point_to_geometry(p, b));
    }
    for p in b_pts {
        best = best.min(point_to_geometry(p, a));
    }
    best
}

/// Minimum Euclidean distance from a point to any part of a geometry.
fn point_to_geometry(p: &Point, g: &Geometry) -> f64 {
    match g {
        Geometry::Point(q) => p.distance(q),
        Geometry::LineString(l) => l
            .segments()
            .map(|(a, b)| point_segment_distance(p, &a, &b))
            .fold(f64::INFINITY, f64::min),
        Geometry::Polygon(pg) => {
            if pg.contains_point(p) {
                0.0
            } else {
                pg.exterior
                    .segments()
                    .map(|(a, b)| point_segment_distance(p, &a, &b))
                    .fold(f64::INFINITY, f64::min)
            }
        }
    }
}

/// Is geometry `a` spatially WITHIN geometry `b`? (Every part of `a` lies inside `b`.)
///
/// v1 semantics (planar, boundary-inclusive):
/// * `b` is a `Polygon` — every vertex of `a` is inside `b` (exact for points; a
///   vertex-inclusion approximation for lines/polygons, documented as a follow-up to
///   full segment-clipping containment).
/// * `b` is a `Point` — `a` is within `b` iff `a` is that same point.
/// * `b` is a `LineString` — every vertex of `a` lies on `b` (rare; kept exact).
pub fn within(a: &Geometry, b: &Geometry) -> bool {
    match b {
        Geometry::Polygon(pg) => a.points().iter().all(|p| pg.contains_point(p)),
        Geometry::Point(q) => a.points().iter().all(|p| p == q),
        Geometry::LineString(l) => a.points().iter().all(|p| {
            l.segments()
                .any(|(s, e)| crate::geometry::point_on_segment(p, &s, &e))
        }),
    }
}

/// Do geometries `a` and `b` share any point (planar)? Fast bbox reject first, then an
/// exact test per type pair (point-in-polygon, segment crossing, vertex containment).
pub fn intersects(a: &Geometry, b: &Geometry) -> bool {
    // Cheap bbox reject.
    match (a.bbox(), b.bbox()) {
        (Some(ba), Some(bb)) if !ba.intersects(&bb) => return false,
        (Some(_), Some(_)) => {}
        _ => return false, // an empty geometry intersects nothing
    }
    match (a, b) {
        (Geometry::Point(p), Geometry::Point(q)) => p == q,
        (Geometry::Point(p), other) | (other, Geometry::Point(p)) => {
            point_intersects_geometry(p, other)
        }
        // line/polygon vs line/polygon: any boundary segment crossing, OR one vertex
        // contained in the other (handles fully-nested cases).
        _ => {
            if segments_cross(a, b) {
                return true;
            }
            a.points().iter().any(|p| point_intersects_geometry(p, b))
                || b.points().iter().any(|p| point_intersects_geometry(p, a))
        }
    }
}

/// Does point `p` touch geometry `g` (on it, or inside a polygon)?
fn point_intersects_geometry(p: &Point, g: &Geometry) -> bool {
    match g {
        Geometry::Point(q) => p == q,
        Geometry::LineString(l) => l
            .segments()
            .any(|(a, b)| crate::geometry::point_on_segment(p, &a, &b)),
        Geometry::Polygon(pg) => pg.contains_point(p),
    }
}

/// Does any boundary segment of `a` cross any boundary segment of `b`?
fn segments_cross(a: &Geometry, b: &Geometry) -> bool {
    let a_pts = a.points();
    let b_pts = b.points();
    for wa in a_pts.windows(2) {
        for wb in b_pts.windows(2) {
            if seg_seg_intersect(&wa[0], &wa[1], &wb[0], &wb[1]) {
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
    (d1 == 0.0 && crate::geometry::point_on_segment(p1, p3, p4))
        || (d2 == 0.0 && crate::geometry::point_on_segment(p2, p3, p4))
        || (d3 == 0.0 && crate::geometry::point_on_segment(p3, p1, p2))
        || (d4 == 0.0 && crate::geometry::point_on_segment(p4, p1, p2))
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
        // point 6 to the right of the square's right edge (x=4) at y=2 → 6-4 = 2? point x=10
        let d = distance(&Geometry::Point(Point::new(10.0, 2.0)), &square());
        assert!((d - 6.0).abs() < 1e-9, "expected 6.0 got {d}");
        // two points
        let d2 = distance(
            &Geometry::Point(Point::new(0.0, 0.0)),
            &Geometry::Point(Point::new(3.0, 4.0)),
        );
        assert!((d2 - 5.0).abs() < 1e-9);
    }
}
