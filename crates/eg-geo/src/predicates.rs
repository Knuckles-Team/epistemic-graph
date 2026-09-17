//! Planar spatial predicates (CONCEPT:EG-KG.ontology.singles-concept): `within`, `intersects`, `distance`,
//! extended (CONCEPT:EG-KG.domains.geometry-collections) to recurse over multi-geometries / collections and to
//! honour polygon interior rings (holes).
//!
//! All metrics are **Euclidean / planar** (coordinates are treated as a flat plane).
//! The geodesic (great-circle / ellipsoidal) metrics for true lon/lat inputs live in
//! [`crate::geodesic`] (CONCEPT:EG-KG.ontology.concept-8); the predicate signatures here are unchanged so
//! eg-plan's `geo` executor keeps compiling.
//!
//! Composite geometries flatten to atomic [`Prim`]s: a multi's predicate recurses over
//! its parts (within = every part of `a` within `b`; intersects = any part-pair
//! intersects; distance = min over part-pairs), and a polygon-with-holes is "inside"
//! only in its exterior AND outside every hole.

use crate::algebra::signed_area;
use crate::geometry::{
    point_on_segment, point_segment_distance, Geometry, LineString, Point, Polygon, Prim,
};

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
        Prim::Poly(pg) => sided_edges(pg)
            .into_iter()
            .map(|(p, q, _)| (p, q))
            .collect(),
    }
}

/// Every ring edge of a polygon (exterior, then each hole ring, each treated as closed)
/// with whether the polygon's interior lies to the LEFT of it: an exterior ring's
/// interior is on its left when counter-clockwise, a hole ring's when clockwise.
fn sided_edges(pg: &Polygon) -> Vec<(Point, Point, bool)> {
    let mut edges = Vec::new();
    for (i, ring) in std::iter::once(&pg.exterior)
        .chain(pg.interiors.iter())
        .enumerate()
    {
        let left = (signed_area(&ring.points) > 0.0) == (i == 0);
        let n = ring.points.len();
        edges.extend(ring.points.windows(2).map(|w| (w[0], w[1], left)));
        // implicit wrap edge if the ring isn't explicitly closed
        if n >= 3 && ring.points[0] != ring.points[n - 1] {
            edges.push((ring.points[n - 1], ring.points[0], left));
        }
    }
    edges
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

/// Orientation of the ordered triple `(a, b, c)`: >0 CCW, <0 CW, 0 collinear. The
/// crate's one 2D cross-product authority — the sign also answers "is `c` left of the
/// directed edge `a→b`", which is how [`crate::algebra`] clips against a convex window.
pub(crate) fn orient(a: &Point, b: &Point, c: &Point) -> f64 {
    (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)
}

/// Do segments `p1→p2` and `p3→p4` intersect (proper or at an endpoint)?
fn seg_seg_intersect(p1: &Point, p2: &Point, p3: &Point, p4: &Point) -> bool {
    let [d1, d2, d3, d4] = segment_orientations(p1, p2, p3, p4);
    if opposite_sides(d1, d2) && opposite_sides(d3, d4) {
        return true;
    }
    // Collinear-overlap / touching endpoints.
    collinear_on_segment(d1, p1, p3, p4)
        || collinear_on_segment(d2, p2, p3, p4)
        || collinear_on_segment(d3, p3, p1, p2)
        || collinear_on_segment(d4, p4, p1, p2)
}

/// The orientations of each segment's endpoints against the other segment's line:
/// `p1`, `p2` against `p3→p4`, then `p3`, `p4` against `p1→p2`.
fn segment_orientations(p1: &Point, p2: &Point, p3: &Point, p4: &Point) -> [f64; 4] {
    [
        orient(p3, p4, p1),
        orient(p3, p4, p2),
        orient(p1, p2, p3),
        orient(p1, p2, p4),
    ]
}

/// Two orientation values strictly on opposite sides of a directed line.
fn opposite_sides(a: f64, b: f64) -> bool {
    (a > 0.0 && b < 0.0) || (a < 0.0 && b > 0.0)
}

/// `p` is collinear with segment `a→b` (its orientation value `d` is zero) and lies on it.
fn collinear_on_segment(d: f64, p: &Point, a: &Point, b: &Point) -> bool {
    d == 0.0 && point_on_segment(p, a, b)
}

// ── DE-9IM topological relations (CONCEPT:EG-KG.ontology.de-9im-relations) ────────────────────────────────
//
// The full OGC/DE-9IM relation set beyond EG-083's `within`/`intersects`, computed over
// the flattened primitives (multis/collections) honouring polygon interior rings. All
// planar. These are a **documented practical subset**: they are exact for the common
// GIS cases (point/line/polygon pairs) and use a strict-interior test + proper segment
// crossing rather than materialising the full 9-intersection matrix. The key building
// block is [`interiors_intersect`] — do the *interiors* of `a` and `b` share a point —
// which distinguishes `touches` (boundary-only) from `contains`/`overlaps`/`crosses`.

/// `a` DISJOINT `b`: they share no point at all. The exact complement of [`intersects`].
pub fn disjoint(a: &Geometry, b: &Geometry) -> bool {
    !intersects(a, b)
}

/// `a` COVERS `b`: every point of `b` lies in `a` (boundary-inclusive). Here the
/// vertex-based [`within`] of `b` in `a` — the boundary-tolerant superset of [`contains`]
/// (no interior-intersection requirement).
pub fn covers(a: &Geometry, b: &Geometry) -> bool {
    within(b, a)
}

/// `a` CONTAINS `b`: `b` is covered by `a` AND their interiors intersect (so a point on
/// only `a`'s boundary is *covered* but not *contained*, matching DE-9IM).
pub fn contains(a: &Geometry, b: &Geometry) -> bool {
    covers(a, b) && interiors_intersect(a, b)
}

/// `a` EQUALS `b`: geometrically equal point-sets — mutual [`within`] (planar,
/// vertex-based; robust for equal/re-ordered rings of the same shape).
pub fn equals(a: &Geometry, b: &Geometry) -> bool {
    within(a, b) && within(b, a)
}

/// `a` TOUCHES `b`: they intersect only on their boundaries — they share a point but
/// their *interiors* do not meet (adjacent polygons, a line ending on a polygon edge).
pub fn touches(a: &Geometry, b: &Geometry) -> bool {
    intersects(a, b) && !interiors_intersect(a, b)
}

/// `a` OVERLAPS `b`: same dimension (both lines or both polygons), interiors meet, and
/// neither covers the other — and the shared piece has the SAME dimension as the inputs
/// (areal overlap for polygons; a collinear sub-segment for lines, NOT a mere crossing
/// point — that is [`crosses`]).
pub fn overlaps(a: &Geometry, b: &Geometry) -> bool {
    let (da, db) = (dim(a), dim(b));
    if da != db || da < 1 {
        return false;
    }
    if covers(a, b) || covers(b, a) {
        return false;
    }
    match da {
        2 => interiors_intersect(a, b), // any interior meeting of areas ⇒ areal overlap
        1 => lines_share_subsegment(a, b), // collinear overlap of positive length
        _ => false,
    }
}

/// `a` CROSSES `b`: their interiors meet in a piece of LOWER dimension than the higher
/// input (a line through a polygon, two lines meeting at a point), neither covers the
/// other, and it is not an equal-dimension [`overlaps`].
pub fn crosses(a: &Geometry, b: &Geometry) -> bool {
    interiors_intersect(a, b) && !covers(a, b) && !covers(b, a) && !overlaps(a, b)
}

/// Do the BOUNDARIES of `a` and `b` share at least one point — the DE-9IM `B(a) ∩ B(b)`
/// cell (CONCEPT:EG-KG.ontology.concept-7)?
///
/// The boundary is the polygon's rings (exterior + holes) or a linestring's chain
/// segments; a point has an empty boundary (so it never boundary-intersects). This is the
/// single extra cell needed on top of the EG-258 predicate set to separate the
/// **tangential** from the **non-tangential** part relations of the RCC8 / Egenhofer
/// families (TPP vs NTPP, `ehCoveredBy`/`ehCovers` vs `ehInside`/`ehContains`): a proper
/// part whose boundary meets its container's boundary is *tangential*, otherwise
/// *non-tangential*. Exact for the common polygon cases; segment-based like the sibling
/// predicates. (CONCEPT:EG-KG.ontology.concept-7)
pub fn boundaries_intersect(a: &Geometry, b: &Geometry) -> bool {
    segments_meet(
        &boundary_segments(a),
        &boundary_segments(b),
        seg_seg_intersect,
    )
}

/// Every boundary segment of a geometry, across all its primitives (a point contributes
/// none, a polygon contributes its exterior and hole rings).
fn boundary_segments(g: &Geometry) -> Vec<(Point, Point)> {
    prims(g).iter().flat_map(prim_segments).collect()
}

/// Every segment of a geometry's LINE primitives only — polygons and points contribute
/// nothing, because a shared sub-segment of two areal boundaries is not a line overlap.
fn line_segments(g: &Geometry) -> Vec<(Point, Point)> {
    prims(g)
        .iter()
        .filter_map(|p| match p {
            Prim::Line(l) => Some(l.segments().collect::<Vec<_>>()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Does any segment of `a` and any segment of `b` satisfy `hit`? The shared segment-pair
/// scan behind the segment-level predicates.
fn segments_meet(
    a: &[(Point, Point)],
    b: &[(Point, Point)],
    hit: impl Fn(&Point, &Point, &Point, &Point) -> bool,
) -> bool {
    a.iter()
        .any(|(a1, a2)| b.iter().any(|(b1, b2)| hit(a1, a2, b1, b2)))
}

/// Topological dimension of a geometry: 0 (point), 1 (line), 2 (polygon) — the MAX over
/// its flattened primitives; `-1` for an empty geometry.
fn dim(g: &Geometry) -> i8 {
    let mut d = -1i8;
    for p in &prims(g) {
        let pd = match p {
            Prim::Point(_) => 0,
            Prim::Line(_) => 1,
            Prim::Poly(_) => 2,
        };
        d = d.max(pd);
    }
    d
}

/// Do the INTERIORS of `a` and `b` share a point? The DE-9IM `I(a) ∩ I(b) ≠ ∅` cell,
/// evaluated per flattened-primitive pair.
fn interiors_intersect(a: &Geometry, b: &Geometry) -> bool {
    let (pa, pb) = (prims(a), prims(b));
    for x in &pa {
        for y in &pb {
            if prim_interiors_intersect(x, y) {
                return true;
            }
        }
    }
    false
}

/// Do two atomic primitives' interiors meet? Cased on their dimensions.
fn prim_interiors_intersect(x: &Prim<'_>, y: &Prim<'_>) -> bool {
    match (x, y) {
        (Prim::Point(p), Prim::Point(q)) => **p == **q,
        (Prim::Point(p), Prim::Line(l)) | (Prim::Line(l), Prim::Point(p)) => {
            line_interior_has_point(l, p)
        }
        (Prim::Point(p), Prim::Poly(pg)) | (Prim::Poly(pg), Prim::Point(p)) => {
            strict_inside_poly(p, pg)
        }
        (Prim::Line(a), Prim::Line(b)) => lines_interiors_meet(a, b),
        (Prim::Line(l), Prim::Poly(pg)) | (Prim::Poly(pg), Prim::Line(l)) => {
            line_enters_poly(l, pg)
        }
        (Prim::Poly(a), Prim::Poly(b)) => polys_interiors_meet(a, b),
    }
}

/// Is `p` strictly inside polygon `pg` — inside AND not on any (exterior or hole) edge?
fn strict_inside_poly(p: &Point, pg: &Polygon) -> bool {
    if !pg.contains_point(p) {
        return false;
    }
    let prim = Prim::Poly(pg);
    !prim_segments(&prim)
        .iter()
        .any(|(a, b)| point_on_segment(p, a, b))
}

/// Is `p` on the INTERIOR of linestring `l` — on the chain but not at either endpoint
/// (interior vertices count)?
fn line_interior_has_point(l: &LineString, p: &Point) -> bool {
    let n = l.points.len();
    if n < 2 {
        return false;
    }
    if *p == l.points[0] || *p == l.points[n - 1] {
        return false; // a line's two endpoints are its boundary, not its interior
    }
    l.segments().any(|(a, b)| point_on_segment(p, &a, &b))
}

/// Do two linestrings' interiors meet — a proper crossing of any segment pair, or a
/// vertex of one lying on the interior of the other?
fn lines_interiors_meet(a: &LineString, b: &LineString) -> bool {
    for (a1, a2) in a.segments() {
        for (b1, b2) in b.segments() {
            if seg_proper_cross(&a1, &a2, &b1, &b2) {
                return true;
            }
        }
    }
    for v in &a.points {
        if line_interior_has_point(b, v) && line_interior_has_point(a, v) {
            return true;
        }
    }
    for v in &b.points {
        if line_interior_has_point(a, v) && line_interior_has_point(b, v) {
            return true;
        }
    }
    false
}

/// Does linestring `l`'s interior meet polygon `pg`'s interior — a vertex strictly inside,
/// a piece of a segment (between the polygon vertices it touches) passing through the
/// interior, or a segment properly crossing the boundary?
fn line_enters_poly(l: &LineString, pg: &Polygon) -> bool {
    for p in &l.points {
        if strict_inside_poly(p, pg) {
            return true;
        }
    }
    let boundary = prim_segments(&Prim::Poly(pg));
    for (a, b) in l.segments() {
        if segment_passes_through_interior(&a, &b, pg) {
            return true;
        }
        for (c, d) in &boundary {
            if seg_proper_cross(&a, &b, c, d) {
                return true;
            }
        }
    }
    false
}

/// Do two polygons' interiors meet — their boundaries properly crossing (partial areal
/// overlap), a boundary piece of one passing through the other's interior (which also
/// covers a vertex strictly inside), or a shared boundary piece with both interiors on
/// the same side of it (identical or coincident-edged polygons)?
fn polys_interiors_meet(a: &Polygon, b: &Polygon) -> bool {
    let (sa, sb) = (prim_segments(&Prim::Poly(a)), prim_segments(&Prim::Poly(b)));
    segments_meet(&sa, &sb, seg_proper_cross)
        || sa
            .iter()
            .any(|(p, q)| segment_passes_through_interior(p, q, b))
        || sb
            .iter()
            .any(|(p, q)| segment_passes_through_interior(p, q, a))
        || boundaries_share_an_interior_side(a, b)
}

/// Does segment `p→q`, cut at every vertex of `pg` lying on it, have a piece whose
/// midpoint is strictly inside `pg`? Without a proper boundary crossing, each piece is
/// wholly inside, wholly outside, or wholly on `pg`'s boundary, so its midpoint decides.
fn segment_passes_through_interior(p: &Point, q: &Point, pg: &Polygon) -> bool {
    let (dx, dy) = (q.x - p.x, q.y - p.y);
    let along = |v: &Point| (v.x - p.x) * dx + (v.y - p.y) * dy;
    let mut stops: Vec<Point> = prim_vertices(&Prim::Poly(pg))
        .into_iter()
        .filter(|v| point_on_segment(v, p, q))
        .collect();
    stops.extend([*p, *q]);
    stops.sort_by(|u, v| along(u).total_cmp(&along(v)));
    stops.windows(2).any(|w| {
        let mid = Point::new((w[0].x + w[1].x) / 2.0, (w[0].y + w[1].y) / 2.0);
        strict_inside_poly(&mid, pg)
    })
}

/// Do `a` and `b` share a collinear boundary piece with BOTH interiors on the same side
/// of it? (Opposite sides is boundary-only contact: adjacent polygons, or a polygon
/// filling another's hole.)
fn boundaries_share_an_interior_side(a: &Polygon, b: &Polygon) -> bool {
    let edges_b = sided_edges(b);
    sided_edges(a).iter().any(|&(p, q, a_left)| {
        edges_b.iter().any(|&(c, d, b_left)| {
            let same_direction = (q.x - p.x) * (d.x - c.x) + (q.y - p.y) * (d.y - c.y) > 0.0;
            collinear_overlap(&p, &q, &c, &d) && (a_left == b_left) == same_direction
        })
    })
}

/// Do two linestrings share a collinear sub-segment of positive length (an areal-1D
/// overlap, the `overlaps` criterion for lines)?
fn lines_share_subsegment(a: &Geometry, b: &Geometry) -> bool {
    segments_meet(&line_segments(a), &line_segments(b), collinear_overlap)
}

/// Are segments `a→b` and `c→d` collinear and overlapping over a positive length?
fn collinear_overlap(p1: &Point, p2: &Point, p3: &Point, p4: &Point) -> bool {
    if orient(p1, p2, p3) != 0.0 || orient(p1, p2, p4) != 0.0 {
        return false;
    }
    let (dx, dy) = (p2.x - p1.x, p2.y - p1.y);
    let along = |p: &Point| if dx.abs() >= dy.abs() { p.x } else { p.y };
    let (mut a_lo, mut a_hi) = (along(p1), along(p2));
    if a_lo > a_hi {
        std::mem::swap(&mut a_lo, &mut a_hi);
    }
    let (mut b_lo, mut b_hi) = (along(p3), along(p4));
    if b_lo > b_hi {
        std::mem::swap(&mut b_lo, &mut b_hi);
    }
    a_lo.max(b_lo) - a_hi.min(b_hi) < -1e-9
}

/// Do segments `p1→p2` and `p3→p4` cross PROPERLY — at a single point interior to both
/// (all four orientations strictly non-zero with opposite signs)?
fn seg_proper_cross(p1: &Point, p2: &Point, p3: &Point, p4: &Point) -> bool {
    let [d1, d2, d3, d4] = segment_orientations(p1, p2, p3, p4);
    opposite_sides(d1, d2) && opposite_sides(d3, d4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{LineString, Polygon};

    fn square() -> Geometry {
        Geometry::Polygon(Polygon::new(
            LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(4.0, 0.0),
                Point::new(4.0, 4.0),
                Point::new(0.0, 4.0),
                Point::new(0.0, 0.0),
            ]),
            Vec::new(),
        ))
    }

    #[test]
    fn point_within_polygon() {
        let inside = Geometry::Point(Point::new(2.0, 2.0));
        let outside = Geometry::Point(Point::new(9.0, 9.0));
        assert!(within(&inside, &square()));
        assert!(!within(&outside, &square()));
    }

    /// CONCEPT:EG-KG.ontology.concept-7: `boundaries_intersect` is true when two polygons' rings share a
    /// point (a boundary-tangential inner square, and an edge-abutting neighbour), and
    /// false for a strictly-interior square (whose ring never meets the container's) — the
    /// exact cell that separates tangential from non-tangential part relations. (Uses the
    /// `poly` fixture helper defined later in this test module.)
    #[test]
    fn eg155_boundaries_intersect_tangential_vs_interior() {
        let big = poly(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        // Corner square shares two edges with `big`'s boundary.
        let corner = poly(&[(0.0, 0.0), (5.0, 0.0), (5.0, 5.0), (0.0, 5.0), (0.0, 0.0)]);
        assert!(boundaries_intersect(&big, &corner));
        // Edge-abutting neighbour touches along x=10.
        let abut = poly(&[
            (10.0, 0.0),
            (12.0, 0.0),
            (12.0, 10.0),
            (10.0, 10.0),
            (10.0, 0.0),
        ]);
        assert!(boundaries_intersect(&big, &abut));
        // Strictly-interior square: rings never meet.
        let inner = poly(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0), (2.0, 2.0)]);
        assert!(!boundaries_intersect(&big, &inner));
        // A point has an empty boundary ⇒ never boundary-intersects.
        assert!(!boundaries_intersect(
            &big,
            &Geometry::Point(Point::new(5.0, 5.0))
        ));
    }

    #[test]
    fn intersects_point_polygon_and_segments() {
        // point inside
        assert!(intersects(
            &Geometry::Point(Point::new(1.0, 1.0)),
            &square()
        ));
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
        assert_eq!(
            distance(&Geometry::Point(Point::new(2.0, 2.0)), &square()),
            0.0
        );
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
        // 10×10 square with a central 4×4 hole (CONCEPT:EG-KG.domains.geometry-collections).
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
        let holed = Geometry::Polygon(Polygon::new(ext, vec![hole]));
        // Inside the ring but outside the hole → within; distance 0.
        let in_ring = Geometry::Point(Point::new(1.0, 1.0));
        assert!(within(&in_ring, &holed));
        assert!(intersects(&in_ring, &holed));
        // In the hole → NOT within; a positive distance to the polygon.
        let in_hole = Geometry::Point(Point::new(5.0, 5.0));
        assert!(!within(&in_hole, &holed));
        assert!(!intersects(&in_hole, &holed));
        let d = distance(&in_hole, &holed);
        assert!(
            d > 0.0 && (d - 2.0).abs() < 1e-9,
            "hole-center distance {d}"
        );
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
            Polygon::new(
                LineString::new(vec![
                    Point::new(0.0, 0.0),
                    Point::new(2.0, 0.0),
                    Point::new(2.0, 2.0),
                    Point::new(0.0, 2.0),
                    Point::new(0.0, 0.0),
                ]),
                Vec::new(),
            ),
            Polygon::new(
                LineString::new(vec![
                    Point::new(100.0, 100.0),
                    Point::new(102.0, 100.0),
                    Point::new(102.0, 102.0),
                    Point::new(100.0, 100.0),
                ]),
                Vec::new(),
            ),
        ]);
        // A point at (3,1) is 1.0 from the near square's right edge.
        let d = distance(&Geometry::Point(Point::new(3.0, 1.0)), &mpoly);
        assert!((d - 1.0).abs() < 1e-9, "nearest-part distance {d}");
    }

    // ── DE-9IM relations (CONCEPT:EG-KG.ontology.de-9im-relations) ────────────────────────────────────────

    fn poly(pts: &[(f64, f64)]) -> Geometry {
        Geometry::Polygon(Polygon::new(
            LineString::new(pts.iter().map(|&(x, y)| Point::new(x, y)).collect()),
            Vec::new(),
        ))
    }
    fn line(pts: &[(f64, f64)]) -> Geometry {
        Geometry::LineString(LineString::new(
            pts.iter().map(|&(x, y)| Point::new(x, y)).collect(),
        ))
    }

    #[test]
    fn disjoint_and_equals() {
        let a = square();
        let far = poly(&[
            (20.0, 20.0),
            (24.0, 20.0),
            (24.0, 24.0),
            (20.0, 24.0),
            (20.0, 20.0),
        ]);
        assert!(disjoint(&a, &far));
        assert!(!disjoint(&a, &a));
        assert!(equals(&a, &square()));
        assert!(!equals(&a, &far));
    }

    #[test]
    fn contains_covers_point() {
        let sq = square(); // 0,0 → 4,4
        let interior = Geometry::Point(Point::new(2.0, 2.0));
        let on_edge = Geometry::Point(Point::new(0.0, 2.0));
        // Strictly-inside point: contained AND covered.
        assert!(contains(&sq, &interior));
        assert!(covers(&sq, &interior));
        // Boundary point: covered but NOT contained (interiors don't meet).
        assert!(covers(&sq, &on_edge));
        assert!(!contains(&sq, &on_edge));
    }

    #[test]
    fn touches_adjacent_polygons() {
        // Two unit squares sharing the edge x=4: they touch (boundary-only), not overlap.
        let a = square();
        let b = poly(&[(4.0, 0.0), (8.0, 0.0), (8.0, 4.0), (4.0, 4.0), (4.0, 0.0)]);
        assert!(touches(&a, &b));
        assert!(!overlaps(&a, &b));
        assert!(!contains(&a, &b));
        assert!(intersects(&a, &b));
    }

    #[test]
    fn overlaps_partial_polygons() {
        // Two squares overlapping in a quadrant: overlaps, not touches, neither contains.
        let a = square(); // 0,0 → 4,4
        let b = poly(&[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0), (2.0, 2.0)]);
        assert!(overlaps(&a, &b));
        assert!(!touches(&a, &b));
        assert!(!contains(&a, &b));
        assert!(!equals(&a, &b));
    }

    /// A polygon's DE-9IM surface relation to `b`, as `[contains(a,b), within(a,b),
    /// touches, overlaps, crosses, equals]`.
    fn areal_relations(a: &Geometry, b: &Geometry) -> [bool; 6] {
        [
            contains(a, b),
            within(a, b),
            touches(a, b),
            overlaps(a, b),
            crosses(a, b),
            equals(a, b),
        ]
    }

    #[test]
    fn identical_polygons_contain_each_other_and_do_not_touch() {
        // Same ring, and the same ring in the opposite winding: no vertex is strictly
        // inside the other and no edge properly crosses, but the interiors coincide.
        let a = square();
        let reversed = poly(&[(0.0, 0.0), (0.0, 4.0), (4.0, 4.0), (4.0, 0.0), (0.0, 0.0)]);
        let equal = [true, true, false, false, false, true];
        assert_eq!(areal_relations(&a, &square()), equal);
        assert_eq!(areal_relations(&a, &reversed), equal);
        assert_eq!(areal_relations(&reversed, &a), equal);
    }

    #[test]
    fn coincident_edges_with_shared_interior_are_not_touches() {
        let a = square(); // 0,0 → 4,4
                          // Lower half: every vertex on `a`'s boundary, its top edge through `a`'s interior.
        let half = poly(&[(0.0, 0.0), (4.0, 0.0), (4.0, 2.0), (0.0, 2.0), (0.0, 0.0)]);
        assert_eq!(
            areal_relations(&a, &half),
            [true, false, false, false, false, false]
        );
        assert_eq!(
            areal_relations(&half, &a),
            [false, true, false, false, false, false]
        );
        // Inscribed triangle: all vertices on `a`'s corners, the diagonal inside.
        let triangle = poly(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 0.0)]);
        assert_eq!(
            areal_relations(&a, &triangle),
            [true, false, false, false, false, false]
        );
        // A strip sharing two side lines and overlapping `a`'s upper half.
        let strip = poly(&[(0.0, 2.0), (4.0, 2.0), (4.0, 6.0), (0.0, 6.0), (0.0, 2.0)]);
        assert_eq!(
            areal_relations(&a, &strip),
            [false, false, false, true, false, false]
        );
    }

    /// Mutation coverage for `polys_interiors_meet`'s boundary-through-interior check
    /// (M1): `a`'s edge (0,2)→(4,2) enters `square()` exactly at the midpoints of its
    /// left and right edges — not at a corner and not properly crossing (both
    /// endpoints lie ON `square()`'s boundary, giving a zero orientation there rather
    /// than a strict sign change either side) — and `a` shares no collinear edge with
    /// `square()` at all. So the only way the two interiors are seen to meet is the
    /// through-interior check on that one segment; drop it and `overlaps` misses this
    /// case entirely.
    #[test]
    fn boundary_edge_through_interior_with_no_crossing_or_shared_side_overlaps() {
        let a = poly(&[
            (0.0, 2.0),
            (4.0, 2.0),
            (4.5, -3.0),
            (-0.5, -3.0),
            (-0.5, 2.0),
            (0.0, 2.0),
        ]);
        let b = square();
        assert_eq!(
            areal_relations(&a, &b),
            [false, false, false, true, false, false]
        );
    }

    #[test]
    fn shared_boundary_with_interiors_on_opposite_sides_still_touches() {
        // A polygon exactly filling another's hole shares the whole hole ring, with the
        // interiors on opposite sides of it (hole ring wound either way).
        let outer = [(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0), (0.0, 0.0)];
        let hole_ccw = [(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0), (2.0, 2.0)];
        let mut hole_cw = hole_ccw;
        hole_cw.reverse();
        let ring = |pts: &[(f64, f64)]| {
            LineString::new(pts.iter().map(|&(x, y)| Point::new(x, y)).collect())
        };
        let plug = poly(&hole_ccw);
        for hole in [hole_ccw, hole_cw] {
            let donut = Geometry::Polygon(Polygon::new(ring(&outer), vec![ring(&hole)]));
            assert_eq!(
                areal_relations(&donut, &plug),
                [false, false, true, false, false, false]
            );
            let same_donut = Geometry::Polygon(Polygon::new(ring(&outer), vec![ring(&hole)]));
            assert!(contains(&donut, &same_donut) && !touches(&donut, &same_donut));
        }
    }

    #[test]
    fn line_through_polygon_corners_crosses_its_interior() {
        // The segment's own midpoint is outside the square and it only meets the
        // boundary at two corners, yet it runs along the square's diagonal.
        let diagonal = line(&[(-5.0, -5.0), (100.0, 100.0)]);
        assert!(crosses(&diagonal, &square()));
        assert!(!touches(&diagonal, &square()));
        // Along an edge only: boundary contact, not an interior crossing.
        let along_edge = line(&[(-5.0, 0.0), (100.0, 0.0)]);
        assert!(touches(&along_edge, &square()));
        assert!(!crosses(&along_edge, &square()));
    }

    #[test]
    fn crosses_line_through_polygon_and_line_line() {
        // A line passing straight through the square crosses it (dim 1 vs 2).
        let l = line(&[(-1.0, 2.0), (5.0, 2.0)]);
        assert!(crosses(&l, &square()));
        assert!(!overlaps(&l, &square()));
        // Two lines meeting at a single interior point cross (intersection dim 0 < 1).
        let h = line(&[(-2.0, 0.0), (2.0, 0.0)]);
        let v = line(&[(0.0, -2.0), (0.0, 2.0)]);
        assert!(crosses(&h, &v));
        assert!(!overlaps(&h, &v));
    }

    #[test]
    fn overlaps_collinear_lines_not_crosses() {
        // Two collinear segments sharing a sub-segment overlap (1-D), do NOT cross.
        let a = line(&[(0.0, 0.0), (4.0, 0.0)]);
        let b = line(&[(2.0, 0.0), (6.0, 0.0)]);
        assert!(overlaps(&a, &b));
        assert!(!crosses(&a, &b));
    }

    #[test]
    fn seg_seg_intersect_proper_touching_collinear_and_disjoint() {
        let p = |x: f64, y: f64| Point::new(x, y);
        let hit = |a: (f64, f64), b: (f64, f64), c: (f64, f64), d: (f64, f64)| {
            seg_seg_intersect(&p(a.0, a.1), &p(b.0, b.1), &p(c.0, c.1), &p(d.0, d.1))
        };
        // Proper crossing, in both orientations of each segment.
        assert!(hit((0.0, 0.0), (2.0, 2.0), (0.0, 2.0), (2.0, 0.0)));
        assert!(hit((2.0, 2.0), (0.0, 0.0), (2.0, 0.0), (0.0, 2.0)));
        // Straddling lines whose segments stop short of each other.
        assert!(!hit((0.0, 0.0), (1.0, 1.0), (0.0, 4.0), (4.0, 0.0)));
        assert!(!hit((0.0, 4.0), (4.0, 0.0), (0.0, 0.0), (1.0, 1.0)));
        // Parallel, disjoint.
        assert!(!hit((0.0, 0.0), (2.0, 0.0), (0.0, 1.0), (2.0, 1.0)));
        // Each endpoint touching the other segment (d1, d2, d3, d4 == 0 in turn).
        assert!(hit((1.0, 0.0), (1.0, 5.0), (0.0, 0.0), (2.0, 0.0)));
        assert!(hit((1.0, 5.0), (1.0, 0.0), (0.0, 0.0), (2.0, 0.0)));
        assert!(hit((0.0, 0.0), (2.0, 0.0), (1.0, 0.0), (1.0, 5.0)));
        assert!(hit((0.0, 0.0), (2.0, 0.0), (1.0, 5.0), (1.0, 0.0)));
        // Collinear: overlapping, sharing one endpoint, and disjoint.
        assert!(hit((0.0, 0.0), (3.0, 0.0), (2.0, 0.0), (5.0, 0.0)));
        assert!(hit((0.0, 0.0), (2.0, 0.0), (2.0, 0.0), (5.0, 0.0)));
        assert!(!hit((0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (5.0, 0.0)));
        // Orientation zero on the carrier line but the endpoint lies outside the segment.
        assert!(!hit((3.0, 0.0), (3.0, 5.0), (0.0, 0.0), (2.0, 0.0)));
    }
}
