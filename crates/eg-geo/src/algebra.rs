//! Constructive geometry algebra (CONCEPT:EG-KG.ontology.concept-9) — the operations that *derive a new
//! geometry* from existing ones, the workhorses of urban-planning / logistics GIS:
//!
//! * [`buffer`] — grow a geometry outward by a distance (a rounded convex buffer).
//! * [`convex_hull`] — the smallest convex polygon enclosing every vertex (Andrew's
//!   monotone chain).
//! * [`simplify`] — vertex reduction via the Douglas–Peucker algorithm.
//! * [`centroid`] — the area- / length- / point-weighted centre of mass.
//! * [`intersection`] / [`union`] / [`difference`] — polygon boolean algebra.
//!
//! All **pure-Rust, no C dep**. The boolean set-ops are a *documented practical subset*:
//! [`intersection`] is exact Sutherland–Hodgman (correct when the CLIP polygon is convex —
//! the overwhelmingly common query-window case); [`union`] returns the convex union (the
//! convex hull of both, exact when the true union is convex); [`difference`] is exact for
//! the disjoint and fully-covered cases and conservatively returns the subject for a
//! partial non-convex overlap. A general Weiler–Atherton clipper is the documented
//! follow-up.

use crate::geometry::{point_segment_distance, Geometry, LineString, Point, Polygon};

/// Grow `g` outward by `dist` (CONCEPT:EG-KG.ontology.concept-9), returning a convex buffer polygon: the
/// convex hull of a disk of radius `dist` (16-gon) placed at every vertex — i.e. the
/// Minkowski sum of the vertices' convex hull with that disk. Its bounding box is exactly
/// the input's grown by `dist` on every side. A non-positive `dist` degenerates to
/// [`convex_hull`]. (Concave/rounded-per-vertex buffering is the documented follow-up.)
pub fn buffer(g: &Geometry, dist: f64) -> Geometry {
    if dist <= 0.0 {
        return convex_hull(g);
    }
    const K: usize = 16;
    let mut cloud = Vec::new();
    for v in g.all_vertices() {
        for i in 0..K {
            let theta = (i as f64) * std::f64::consts::TAU / (K as f64);
            cloud.push(Point::new(
                v.x + dist * theta.cos(),
                v.y + dist * theta.sin(),
            ));
        }
    }
    hull_geometry(convex_hull_points(cloud))
}

/// The convex hull of every vertex of `g` (CONCEPT:EG-KG.ontology.concept-9), via Andrew's monotone-chain
/// algorithm. Returns a `Polygon` (≥ 3 hull points), a `LineString` (2 collinear/coincident
/// points), a `Point` (1), or an empty `GeometryCollection` (0).
pub fn convex_hull(g: &Geometry) -> Geometry {
    hull_geometry(convex_hull_points(g.all_vertices()))
}

/// Douglas–Peucker vertex reduction of `g` at tolerance `tol` (CONCEPT:EG-KG.ontology.concept-9): every
/// linestring / polygon ring is simplified, dropping vertices that lie within `tol` of the
/// retained chain. Points/multipoints pass through unchanged; polygon rings stay closed.
pub fn simplify(g: &Geometry, tol: f64) -> Geometry {
    let simp_line = |l: &LineString| LineString::new(douglas_peucker(&l.points, tol));
    let simp_ring = |l: &LineString| {
        let mut pts = douglas_peucker(&l.points, tol);
        // keep the ring closed if the source was closed
        if let (Some(&f), Some(&last)) = (l.points.first(), pts.last()) {
            if l.points.first() == l.points.last() && Some(&f) != pts.last() {
                let _ = last;
                pts.push(f);
            }
        }
        LineString::new(pts)
    };
    let simp_poly = |pg: &Polygon| {
        Polygon::new(
            simp_ring(&pg.exterior),
            pg.interiors.iter().map(simp_ring).collect(),
        )
    };
    match g {
        Geometry::Point(_) | Geometry::MultiPoint(_) => g.clone(),
        Geometry::LineString(l) => Geometry::LineString(simp_line(l)),
        Geometry::Polygon(pg) => Geometry::Polygon(simp_poly(pg)),
        Geometry::MultiLineString(ls) => {
            Geometry::MultiLineString(ls.iter().map(&simp_line).collect())
        }
        Geometry::MultiPolygon(pgs) => Geometry::MultiPolygon(pgs.iter().map(&simp_poly).collect()),
        Geometry::GeometryCollection(gs) => {
            Geometry::GeometryCollection(gs.iter().map(|g| simplify(g, tol)).collect())
        }
    }
}

/// The centroid (centre of mass) of `g` (CONCEPT:EG-KG.ontology.concept-9): area-weighted over polygons
/// (holes subtracted), else length-weighted over linestrings, else the arithmetic mean of
/// the points. `None` for an empty geometry (or a zero-area/zero-length degenerate that
/// falls through to no vertices).
pub fn centroid(g: &Geometry) -> Option<Point> {
    let mut prims = Vec::new();
    g.primitives(&mut prims);

    // Polygons: area-weighted.
    let mut area_w = 0.0;
    let (mut ax, mut ay) = (0.0, 0.0);
    for p in &prims {
        if let crate::geometry::Prim::Poly(pg) = p {
            if let Some((c, a)) = polygon_centroid(pg) {
                area_w += a;
                ax += c.x * a;
                ay += c.y * a;
            }
        }
    }
    if area_w > 0.0 {
        return Some(Point::new(ax / area_w, ay / area_w));
    }

    // Linestrings: length-weighted midpoints.
    let mut len_w = 0.0;
    let (mut lx, mut ly) = (0.0, 0.0);
    for p in &prims {
        if let crate::geometry::Prim::Line(l) = p {
            for (a, b) in l.segments() {
                let len = a.distance(&b);
                len_w += len;
                lx += (a.x + b.x) / 2.0 * len;
                ly += (a.y + b.y) / 2.0 * len;
            }
        }
    }
    if len_w > 0.0 {
        return Some(Point::new(lx / len_w, ly / len_w));
    }

    // Fallback: arithmetic mean of every vertex.
    let vs = g.all_vertices();
    if vs.is_empty() {
        return None;
    }
    let n = vs.len() as f64;
    let (sx, sy) = vs
        .iter()
        .fold((0.0, 0.0), |(sx, sy), p| (sx + p.x, sy + p.y));
    Some(Point::new(sx / n, sy / n))
}

/// The polygon intersection of `a` and `b` (CONCEPT:EG-KG.ontology.concept-9) via Sutherland–Hodgman,
/// clipping `a`'s exterior ring against `b`'s exterior ring. **Exact when `b` (the clip)
/// is convex** — the common query-window case; a general clipper is the follow-up. `None`
/// when either lacks a polygon ring or the clipped result is empty (< 3 vertices).
pub fn intersection(a: &Geometry, b: &Geometry) -> Option<Geometry> {
    let subject = first_polygon_ring(a)?;
    let clip = ensure_ccw(first_polygon_ring(b)?);
    let out = sutherland_hodgman(strip_closing(&subject), &clip);
    if out.len() < 3 {
        return None;
    }
    Some(closed_polygon(out))
}

/// The (convex) union of `a` and `b` (CONCEPT:EG-KG.ontology.concept-9): the convex hull of both geometries'
/// vertices — exact when the true union is convex, an over-approximation otherwise
/// (documented; a general union is the follow-up).
pub fn union(a: &Geometry, b: &Geometry) -> Geometry {
    let mut vs = a.all_vertices();
    vs.extend(b.all_vertices());
    hull_geometry(convex_hull_points(vs))
}

/// The difference `a − b` (CONCEPT:EG-KG.ontology.concept-9), a documented subset: exact when `a` and `b`
/// are disjoint (returns `a`) or `a` is fully covered by `b` (returns `None`, empty). For
/// a partial non-convex overlap it conservatively returns `a` (the general Weiler–Atherton
/// subtraction is the follow-up).
pub fn difference(a: &Geometry, b: &Geometry) -> Option<Geometry> {
    if crate::predicates::disjoint(a, b) {
        return Some(a.clone());
    }
    if crate::predicates::covers(b, a) {
        return None; // a lies entirely within b ⇒ nothing left
    }
    Some(a.clone())
}

// ── internals ────────────────────────────────────────────────────────────────────

/// Recursive Douglas–Peucker over a coordinate chain.
fn douglas_peucker(points: &[Point], tol: f64) -> Vec<Point> {
    let n = points.len();
    if n < 3 {
        return points.to_vec();
    }
    let (first, last) = (points[0], points[n - 1]);
    let mut idx = 0;
    let mut dmax = 0.0;
    for (i, p) in points.iter().enumerate().take(n - 1).skip(1) {
        let d = point_segment_distance(p, &first, &last);
        if d > dmax {
            dmax = d;
            idx = i;
        }
    }
    if dmax > tol {
        let mut left = douglas_peucker(&points[..=idx], tol);
        let right = douglas_peucker(&points[idx..], tol);
        left.pop(); // drop the shared vertex
        left.extend(right);
        left
    } else {
        vec![first, last]
    }
}

/// Andrew's monotone-chain convex hull; returns the hull vertices CCW, NOT closed.
fn convex_hull_points(mut pts: Vec<Point>) -> Vec<Point> {
    pts.sort_by(|a, b| {
        a.x.partial_cmp(&b.x)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.y.partial_cmp(&b.y).unwrap_or(std::cmp::Ordering::Equal))
    });
    pts.dedup();
    let n = pts.len();
    if n < 3 {
        return pts;
    }
    let cross =
        |o: &Point, a: &Point, b: &Point| (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
    let mut lower: Vec<Point> = Vec::new();
    for p in &pts {
        while lower.len() >= 2 && cross(&lower[lower.len() - 2], &lower[lower.len() - 1], p) <= 0.0
        {
            lower.pop();
        }
        lower.push(*p);
    }
    let mut upper: Vec<Point> = Vec::new();
    for p in pts.iter().rev() {
        while upper.len() >= 2 && cross(&upper[upper.len() - 2], &upper[upper.len() - 1], p) <= 0.0
        {
            upper.pop();
        }
        upper.push(*p);
    }
    lower.pop();
    upper.pop();
    lower.extend(upper);
    lower
}

/// Wrap a set of (CCW, unclosed) hull points into the appropriate geometry.
fn hull_geometry(mut ring: Vec<Point>) -> Geometry {
    match ring.len() {
        0 => Geometry::GeometryCollection(Vec::new()),
        1 => Geometry::Point(ring[0]),
        2 => Geometry::LineString(LineString::new(ring)),
        _ => {
            ring.push(ring[0]); // close
            Geometry::Polygon(Polygon::new(LineString::new(ring), Vec::new()))
        }
    }
}

/// The signed area of a ring (shoelace); positive when CCW.
fn signed_area(ring: &[Point]) -> f64 {
    let n = ring.len();
    if n < 3 {
        return 0.0;
    }
    let mut a = 0.0;
    for i in 0..n {
        let p = ring[i];
        let q = ring[(i + 1) % n];
        a += p.x * q.y - q.x * p.y;
    }
    a / 2.0
}

/// Centroid + absolute area of a polygon (exterior minus holes), or `None` if degenerate.
fn polygon_centroid(pg: &Polygon) -> Option<(Point, f64)> {
    let (ec, ea) = ring_centroid(&pg.exterior.points)?;
    let mut area = ea;
    let (mut cx, mut cy) = (ec.x * ea, ec.y * ea);
    for h in &pg.interiors {
        if let Some((hc, ha)) = ring_centroid(&h.points) {
            area -= ha;
            cx -= hc.x * ha;
            cy -= hc.y * ha;
        }
    }
    if area <= 0.0 {
        return None;
    }
    Some((Point::new(cx / area, cy / area), area))
}

/// Centroid + ABSOLUTE area of a single ring (orientation-independent), or `None` for a
/// zero-area/degenerate ring.
fn ring_centroid(ring: &[Point]) -> Option<(Point, f64)> {
    let n = ring.len();
    if n < 3 {
        return None;
    }
    let mut a = 0.0;
    let (mut cx, mut cy) = (0.0, 0.0);
    for i in 0..n {
        let p = ring[i];
        let q = ring[(i + 1) % n];
        let cross = p.x * q.y - q.x * p.y;
        a += cross;
        cx += (p.x + q.x) * cross;
        cy += (p.y + q.y) * cross;
    }
    if a.abs() < 1e-12 {
        return None;
    }
    // cx/cy carry the sign of `a`; dividing by 3a cancels it ⇒ orientation-independent.
    Some((Point::new(cx / (3.0 * a), cy / (3.0 * a)), (a / 2.0).abs()))
}

/// The exterior ring (closed) of the first polygon in a geometry, if any.
fn first_polygon_ring(g: &Geometry) -> Option<Vec<Point>> {
    match g {
        Geometry::Polygon(pg) => Some(pg.exterior.points.clone()),
        Geometry::MultiPolygon(pgs) => pgs.first().map(|pg| pg.exterior.points.clone()),
        Geometry::GeometryCollection(gs) => gs.iter().find_map(first_polygon_ring),
        _ => None,
    }
}

/// A ring with any explicit closing-duplicate vertex removed.
fn strip_closing(ring: &[Point]) -> Vec<Point> {
    let mut v = ring.to_vec();
    if v.len() >= 2 && v.first() == v.last() {
        v.pop();
    }
    v
}

/// Ensure a ring is oriented counter-clockwise (unclosed, CCW), for Sutherland–Hodgman.
fn ensure_ccw(ring: Vec<Point>) -> Vec<Point> {
    let mut open = strip_closing(&ring);
    if signed_area(&open) < 0.0 {
        open.reverse();
    }
    open
}

/// Close a CCW hull/clip result into a `Polygon` geometry.
fn closed_polygon(mut ring: Vec<Point>) -> Geometry {
    if ring.first() != ring.last() {
        if let Some(&f) = ring.first() {
            ring.push(f);
        }
    }
    Geometry::Polygon(Polygon::new(LineString::new(ring), Vec::new()))
}

/// Sutherland–Hodgman polygon clipping: clip `subject` (unclosed) against the convex,
/// CCW `clip` ring. Returns the clipped vertices (unclosed).
fn sutherland_hodgman(subject: Vec<Point>, clip: &[Point]) -> Vec<Point> {
    let cn = clip.len();
    let mut output = subject;
    for i in 0..cn {
        if output.is_empty() {
            break;
        }
        let a = clip[i];
        let b = clip[(i + 1) % cn];
        let input = std::mem::take(&mut output);
        let m = input.len();
        for j in 0..m {
            let cur = input[j];
            let prev = input[(j + m - 1) % m];
            let cur_in = inside(&cur, &a, &b);
            let prev_in = inside(&prev, &a, &b);
            if cur_in {
                if !prev_in {
                    output.push(edge_intersect(&prev, &cur, &a, &b));
                }
                output.push(cur);
            } else if prev_in {
                output.push(edge_intersect(&prev, &cur, &a, &b));
            }
        }
    }
    output
}

/// Is `p` on the inside (left) of the directed clip edge `a→b` (CCW clip)?
fn inside(p: &Point, a: &Point, b: &Point) -> bool {
    (b.x - a.x) * (p.y - a.y) - (b.y - a.y) * (p.x - a.x) >= 0.0
}

/// The intersection point of segment `p1→p2` with the (infinite) clip line `a→b`.
fn edge_intersect(p1: &Point, p2: &Point, a: &Point, b: &Point) -> Point {
    let (a1, b1, c1) = line_coeffs(a, b);
    let (a2, b2, c2) = line_coeffs(p1, p2);
    let det = a1 * b2 - a2 * b1;
    if det.abs() < 1e-12 {
        return *p2; // near-parallel — degenerate, snap to the far endpoint
    }
    Point::new((b2 * c1 - b1 * c2) / det, (a1 * c2 - a2 * c1) / det)
}

/// `(A, B, C)` for the line `A·x + B·y = C` through two points.
fn line_coeffs(p: &Point, q: &Point) -> (f64, f64, f64) {
    let a = q.y - p.y;
    let b = p.x - q.x;
    let c = a * p.x + b * p.y;
    (a, b, c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::Bbox;

    fn pts(v: &[(f64, f64)]) -> Vec<Point> {
        v.iter().map(|&(x, y)| Point::new(x, y)).collect()
    }

    #[test]
    fn convex_hull_of_point_set() {
        // A square's four corners plus an interior point — hull is the square (interior
        // point dropped): 4 distinct corners + closing vertex = 5.
        let g = Geometry::MultiPoint(pts(&[
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (2.0, 2.0), // interior — must NOT be on the hull
        ]));
        let hull = convex_hull(&g);
        if let Geometry::Polygon(pg) = &hull {
            let ring = &pg.exterior.points;
            assert_eq!(ring.len(), 5, "square hull = 4 corners + close");
            assert!(
                !ring.contains(&Point::new(2.0, 2.0)),
                "interior point excluded"
            );
        } else {
            panic!("expected polygon hull, got {hull:?}");
        }
    }

    #[test]
    fn buffer_grows_bbox_by_distance() {
        let g = Geometry::Point(Point::new(5.0, 5.0));
        let b = buffer(&g, 2.0);
        let bb = b.bbox().unwrap();
        // The 16-gon includes the 0/90/180/270° samples ⇒ bbox is exactly ±dist.
        let want = Bbox::new(3.0, 3.0, 7.0, 7.0);
        assert!((bb.minx - want.minx).abs() < 1e-9, "minx {bb:?}");
        assert!((bb.miny - want.miny).abs() < 1e-9, "miny {bb:?}");
        assert!((bb.maxx - want.maxx).abs() < 1e-9, "maxx {bb:?}");
        assert!((bb.maxy - want.maxy).abs() < 1e-9, "maxy {bb:?}");
    }

    #[test]
    fn simplify_reduces_vertices() {
        // A near-straight chain with a tiny bump: DP drops the near-collinear vertices.
        let g = Geometry::LineString(LineString::new(pts(&[
            (0.0, 0.0),
            (1.0, 0.01),
            (2.0, -0.01),
            (3.0, 0.0),
            (4.0, 0.0),
        ])));
        let s = simplify(&g, 0.1);
        if let Geometry::LineString(l) = &s {
            assert!(
                l.points.len() < 5,
                "expected fewer than 5 vertices, got {}",
                l.points.len()
            );
            assert_eq!(l.points.first(), Some(&Point::new(0.0, 0.0)));
            assert_eq!(l.points.last(), Some(&Point::new(4.0, 0.0)));
        } else {
            panic!("expected linestring");
        }
    }

    #[test]
    fn centroid_of_square() {
        let sq = Geometry::Polygon(Polygon::new(
            LineString::new(pts(&[
                (0.0, 0.0),
                (4.0, 0.0),
                (4.0, 4.0),
                (0.0, 4.0),
                (0.0, 0.0),
            ])),
            Vec::new(),
        ));
        let c = centroid(&sq).unwrap();
        assert!(
            (c.x - 2.0).abs() < 1e-9 && (c.y - 2.0).abs() < 1e-9,
            "centroid {c:?}"
        );
        // Point centroid is itself.
        assert_eq!(
            centroid(&Geometry::Point(Point::new(7.0, 9.0))),
            Some(Point::new(7.0, 9.0))
        );
    }

    #[test]
    fn polygon_intersection_overlap() {
        // Square [0,0]-[4,4] intersected with (convex) clip [2,2]-[6,6] ⇒ [2,2]-[4,4].
        let a = Geometry::Polygon(Polygon::new(
            LineString::new(pts(&[
                (0.0, 0.0),
                (4.0, 0.0),
                (4.0, 4.0),
                (0.0, 4.0),
                (0.0, 0.0),
            ])),
            Vec::new(),
        ));
        let b = Geometry::Polygon(Polygon::new(
            LineString::new(pts(&[
                (2.0, 2.0),
                (6.0, 2.0),
                (6.0, 6.0),
                (2.0, 6.0),
                (2.0, 2.0),
            ])),
            Vec::new(),
        ));
        let inter = intersection(&a, &b).expect("non-empty intersection");
        let bb = inter.bbox().unwrap();
        assert!((bb.minx - 2.0).abs() < 1e-9, "minx {bb:?}");
        assert!((bb.miny - 2.0).abs() < 1e-9, "miny {bb:?}");
        assert!((bb.maxx - 4.0).abs() < 1e-9, "maxx {bb:?}");
        assert!((bb.maxy - 4.0).abs() < 1e-9, "maxy {bb:?}");
        // The intersection polygon has the expected area (2×2 = 4).
        let (_, area) = polygon_centroid(match &inter {
            Geometry::Polygon(pg) => pg,
            _ => panic!("expected polygon"),
        })
        .unwrap();
        assert!((area - 4.0).abs() < 1e-9, "intersection area {area}");
    }

    #[test]
    fn union_and_difference_subset() {
        let a = Geometry::Polygon(Polygon::new(
            LineString::new(pts(&[
                (0.0, 0.0),
                (2.0, 0.0),
                (2.0, 2.0),
                (0.0, 2.0),
                (0.0, 0.0),
            ])),
            Vec::new(),
        ));
        let far = Geometry::Polygon(Polygon::new(
            LineString::new(pts(&[
                (10.0, 10.0),
                (12.0, 10.0),
                (12.0, 12.0),
                (10.0, 10.0),
            ])),
            Vec::new(),
        ));
        // Convex union of two disjoint boxes spans both (hull bbox 0..12).
        let u = union(&a, &far);
        let ub = u.bbox().unwrap();
        assert!(
            (ub.minx).abs() < 1e-9 && (ub.maxx - 12.0).abs() < 1e-9,
            "union bbox {ub:?}"
        );
        // Disjoint difference returns the subject; covered difference is empty.
        assert!(difference(&a, &far).is_some());
        let big = Geometry::Polygon(Polygon::new(
            LineString::new(pts(&[
                (-1.0, -1.0),
                (5.0, -1.0),
                (5.0, 5.0),
                (-1.0, 5.0),
                (-1.0, -1.0),
            ])),
            Vec::new(),
        ));
        assert!(difference(&a, &big).is_none(), "a fully within big ⇒ empty");
    }
}
