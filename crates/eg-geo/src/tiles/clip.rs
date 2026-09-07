//! Tile-boundary clipping for MVT encoding (CONCEPT:EG-KG.domains.map-tiles).
//!
//! A tile only carries the geometry that falls inside its own bounds, so every feature is
//! clipped before it is quantised to the tile grid. Points are tested directly, lines are
//! clipped segment-by-segment with Cohen-Sutherland, and rings are clipped against the
//! four tile half-planes with Sutherland-Hodgman. Dependency-free and pure planar `f64`,
//! like the rest of the crate.

use crate::geometry::{Bbox, Point};

/// Is `p` inside `b` (bounds inclusive)? The whole clip for a point feature.
pub(super) fn bounds_contains(b: &Bbox, p: &Point) -> bool {
    p.x >= b.minx && p.x <= b.maxx && p.y >= b.miny && p.y <= b.maxy
}

/// Cohen–Sutherland outcode for a point against the rectangular `bounds`.
fn outcode(p: &Point, b: &Bbox) -> u8 {
    let mut c = 0u8;
    if p.x < b.minx {
        c |= 1;
    } else if p.x > b.maxx {
        c |= 2;
    }
    if p.y < b.miny {
        c |= 4;
    } else if p.y > b.maxy {
        c |= 8;
    }
    c
}

/// Where segment `a→b` crosses the `bounds` edge indicated by outcode bit `out`
/// (exactly one of the 4 Cohen–Sutherland region bits).
fn clip_boundary_crossing(a: &Point, b: &Point, bx: &Bbox, out: u8) -> Point {
    let (x, y);
    if out & 8 != 0 {
        x = a.x + (b.x - a.x) * (bx.maxy - a.y) / (b.y - a.y);
        y = bx.maxy;
    } else if out & 4 != 0 {
        x = a.x + (b.x - a.x) * (bx.miny - a.y) / (b.y - a.y);
        y = bx.miny;
    } else if out & 2 != 0 {
        y = a.y + (b.y - a.y) * (bx.maxx - a.x) / (b.x - a.x);
        x = bx.maxx;
    } else {
        y = a.y + (b.y - a.y) * (bx.minx - a.x) / (b.x - a.x);
        x = bx.minx;
    }
    Point::new(x, y)
}

/// Clip one segment `a→b` to `bounds` (Cohen–Sutherland). `None` if fully outside.
fn clip_segment(mut a: Point, mut b: Point, bx: &Bbox) -> Option<(Point, Point)> {
    let mut ca = outcode(&a, bx);
    let mut cb = outcode(&b, bx);
    loop {
        if ca | cb == 0 {
            return Some((a, b)); // both inside
        }
        if ca & cb != 0 {
            return None; // both share an outside region
        }
        let out = if ca != 0 { ca } else { cb };
        if out == ca {
            a = clip_boundary_crossing(&a, &b, bx, out);
            ca = outcode(&a, bx);
        } else {
            b = clip_boundary_crossing(&a, &b, bx, out);
            cb = outcode(&b, bx);
        }
    }
}

/// Clip a polyline to `bounds`, yielding the surviving contiguous parts (CONCEPT:EG-KG.domains.map-tiles).
pub(super) fn clip_line(pts: &[Point], bounds: &Bbox) -> Vec<Vec<Point>> {
    let mut parts: Vec<Vec<Point>> = Vec::new();
    if pts.len() < 2 {
        // A degenerate 1-point "line" survives iff inside.
        if pts.len() == 1 && bounds_contains(bounds, &pts[0]) {
            parts.push(vec![pts[0]]);
        }
        return parts;
    }
    let mut cur: Vec<Point> = Vec::new();
    for w in pts.windows(2) {
        if let Some((a, b)) = clip_segment(w[0], w[1], bounds) {
            if cur.is_empty() {
                cur.push(a);
            } else if cur.last() != Some(&a) {
                // A break in continuity → start a new part.
                parts.push(std::mem::take(&mut cur));
                cur.push(a);
            }
            cur.push(b);
        } else if !cur.is_empty() {
            parts.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        parts.push(cur);
    }
    parts
}

/// Clip a polygon ring to `bounds` (Sutherland–Hodgman against the 4 tile edges).
/// Returns the clipped ring (open, no closing duplicate); empty if fully outside.
pub(super) fn clip_polygon(ring: &[Point], b: &Bbox) -> Vec<Point> {
    if ring.len() < 3 {
        return Vec::new();
    }
    // Drop a closing duplicate so the algorithm sees a clean vertex set.
    let mut poly: Vec<Point> = ring.to_vec();
    if poly.first() == poly.last() && poly.len() > 1 {
        poly.pop();
    }
    // Clip successively against each of the four tile half-planes.
    poly = clip_halfplane(poly, |p| p.x >= b.minx, |a, c| intersect_x(a, c, b.minx));
    poly = clip_halfplane(poly, |p| p.x <= b.maxx, |a, c| intersect_x(a, c, b.maxx));
    poly = clip_halfplane(poly, |p| p.y >= b.miny, |a, c| intersect_y(a, c, b.miny));
    poly = clip_halfplane(poly, |p| p.y <= b.maxy, |a, c| intersect_y(a, c, b.maxy));
    poly
}

/// One Sutherland–Hodgman pass: clip ring `input` against a single half-plane defined by
/// `inside` (membership) and `isect` (edge intersection of a crossing segment).
fn clip_halfplane(
    input: Vec<Point>,
    inside: impl Fn(&Point) -> bool,
    isect: impl Fn(&Point, &Point) -> Point,
) -> Vec<Point> {
    if input.is_empty() {
        return input;
    }
    let n = input.len();
    let mut out = Vec::with_capacity(n + 1);
    for i in 0..n {
        let cur = input[i];
        let prev = input[(i + n - 1) % n];
        let cur_in = inside(&cur);
        let prev_in = inside(&prev);
        if cur_in {
            if !prev_in {
                out.push(isect(&prev, &cur));
            }
            out.push(cur);
        } else if prev_in {
            out.push(isect(&prev, &cur));
        }
    }
    out
}

/// Intersection of segment `a→b` with the vertical line `x = xc`.
fn intersect_x(a: &Point, b: &Point, xc: f64) -> Point {
    let t = if (b.x - a.x).abs() < f64::MIN_POSITIVE {
        0.0
    } else {
        (xc - a.x) / (b.x - a.x)
    };
    Point::new(xc, a.y + (b.y - a.y) * t)
}

/// Intersection of segment `a→b` with the horizontal line `y = yc`.
fn intersect_y(a: &Point, b: &Point, yc: f64) -> Point {
    let t = if (b.y - a.y).abs() < f64::MIN_POSITIVE {
        0.0
    } else {
        (yc - a.y) / (b.y - a.y)
    };
    Point::new(a.x + (b.x - a.x) * t, yc)
}
