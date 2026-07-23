//! Geodesic (round-earth) metrics on WGS84 (CONCEPT:EG-KG.ontology.concept-8).
//!
//! The predicates in [`crate::predicates`] are **planar** (Euclidean) — correct for a
//! projected plane but wrong for raw lon/lat. This module adds the real-world GIS
//! metrics alongside them, still **pure-Rust with no C/GEOS/PROJ dependency**:
//!
//! * [`haversine_distance`] — great-circle distance on a mean-radius sphere (fast,
//!   ~0.3 % worst-case error vs the ellipsoid).
//! * [`vincenty_distance`] — Vincenty's *inverse* formula on the WGS84 ellipsoid
//!   (accurate to sub-millimetre; iterative).
//! * [`geodesic_area`] / [`geodesic_ring_area`] — signed spherical polygon area
//!   (Chamberlain–Duquette), holes subtracted.
//!
//! Every coordinate is a [`Point`] carrying **`x = longitude`, `y = latitude` in
//! degrees** (matching the crate's WKT `x y` order); distances are **metres** and
//! areas **square metres**. A geometry's CRS tag (EG-255) is what selects
//! planar-vs-geodesic at the query layer; this module is the geodesic kernel.

use crate::geometry::{Point, Polygon};

/// WGS84 semi-major axis (equatorial radius), metres.
pub const WGS84_A: f64 = 6_378_137.0;
/// WGS84 flattening `f = (a - b) / a`.
pub const WGS84_F: f64 = 1.0 / 298.257_223_563;
/// WGS84 semi-minor axis (polar radius), metres: `b = a (1 - f)`.
pub const WGS84_B: f64 = WGS84_A * (1.0 - WGS84_F);
/// Mean earth radius used by the great-circle metric, metres (IUGG `R1 = (2a+b)/3`).
pub const EARTH_MEAN_RADIUS: f64 = 6_371_008.771_415_059;

/// Great-circle (Haversine) distance in metres between two lon/lat points (degrees),
/// on a sphere of [`EARTH_MEAN_RADIUS`]. Numerically stable near antipodes and for tiny
/// separations (the `asin(sqrt(a))` form). Order of arguments is irrelevant.
pub fn haversine_distance(a: &Point, b: &Point) -> f64 {
    let lat1 = a.y.to_radians();
    let lat2 = b.y.to_radians();
    let dlat = (b.y - a.y).to_radians();
    let dlon = (b.x - a.x).to_radians();

    let sin_dlat = (dlat / 2.0).sin();
    let sin_dlon = (dlon / 2.0).sin();
    let h = sin_dlat * sin_dlat + lat1.cos() * lat2.cos() * sin_dlon * sin_dlon;
    let c = 2.0 * h.sqrt().clamp(0.0, 1.0).asin();
    EARTH_MEAN_RADIUS * c
}

/// Vincenty *inverse* distance in metres between two lon/lat points (degrees) on the
/// WGS84 ellipsoid — accurate to a fraction of a millimetre. Iterates to convergence
/// (≤ ~1e-12 rad, capped at 200 iterations). Falls back to the coincident-point (0.0)
/// answer; for a (rare) near-antipodal non-convergence it returns the best iterate,
/// which is within a few metres — good enough for GIS filtering.
pub fn vincenty_distance(a: &Point, b: &Point) -> f64 {
    // Coincident points: short-circuit (the formula's L == 0 also yields 0, but this
    // avoids a needless loop and any 0/0 in cos(sigma)).
    if (a.x - b.x).abs() < 1e-12 && (a.y - b.y).abs() < 1e-12 {
        return 0.0;
    }

    let f = WGS84_F;
    let big_a2 = WGS84_A; // semi-major
    let big_b = WGS84_B; // semi-minor

    let lat1 = a.y.to_radians();
    let lat2 = b.y.to_radians();
    let l = (b.x - a.x).to_radians(); // difference in longitude

    // Reduced latitudes (latitude on the auxiliary sphere).
    let u1 = ((1.0 - f) * lat1.tan()).atan();
    let u2 = ((1.0 - f) * lat2.tan()).atan();
    let (sin_u1, cos_u1) = (u1.sin(), u1.cos());
    let (sin_u2, cos_u2) = (u2.sin(), u2.cos());

    let mut lambda = l;
    let mut cos_sq_alpha = 0.0;
    let mut sin_sigma = 0.0;
    let mut cos_sigma = 0.0;
    let mut cos2_sigma_m = 0.0;
    let mut sigma = 0.0;

    for _ in 0..200 {
        let sin_lambda = lambda.sin();
        let cos_lambda = lambda.cos();
        sin_sigma = ((cos_u2 * sin_lambda).powi(2)
            + (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lambda).powi(2))
        .sqrt();
        if sin_sigma == 0.0 {
            return 0.0; // coincident
        }
        cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lambda;
        sigma = sin_sigma.atan2(cos_sigma);
        let sin_alpha = cos_u1 * cos_u2 * sin_lambda / sin_sigma;
        cos_sq_alpha = 1.0 - sin_alpha * sin_alpha;
        cos2_sigma_m = if cos_sq_alpha != 0.0 {
            cos_sigma - 2.0 * sin_u1 * sin_u2 / cos_sq_alpha
        } else {
            0.0 // equatorial line
        };
        let c = f / 16.0 * cos_sq_alpha * (4.0 + f * (4.0 - 3.0 * cos_sq_alpha));
        let lambda_prev = lambda;
        lambda = l
            + (1.0 - c)
                * f
                * sin_alpha
                * (sigma
                    + c * sin_sigma
                        * (cos2_sigma_m
                            + c * cos_sigma * (-1.0 + 2.0 * cos2_sigma_m * cos2_sigma_m)));
        if (lambda - lambda_prev).abs() < 1e-12 {
            break;
        }
    }

    let u_sq = cos_sq_alpha * (big_a2 * big_a2 - big_b * big_b) / (big_b * big_b);
    let big_a = 1.0 + u_sq / 16384.0 * (4096.0 + u_sq * (-768.0 + u_sq * (320.0 - 175.0 * u_sq)));
    let big_bb = u_sq / 1024.0 * (256.0 + u_sq * (-128.0 + u_sq * (74.0 - 47.0 * u_sq)));
    let delta_sigma = big_bb
        * sin_sigma
        * (cos2_sigma_m
            + big_bb / 4.0
                * (cos_sigma * (-1.0 + 2.0 * cos2_sigma_m * cos2_sigma_m)
                    - big_bb / 6.0
                        * cos2_sigma_m
                        * (-3.0 + 4.0 * sin_sigma * sin_sigma)
                        * (-3.0 + 4.0 * cos2_sigma_m * cos2_sigma_m)));

    big_b * big_a * (sigma - delta_sigma)
}

/// Absolute geodesic area of a polygon in square metres (WGS84 mean-radius sphere),
/// **holes subtracted** from the exterior. Orientation-independent (absolute value).
pub fn geodesic_area(poly: &Polygon) -> f64 {
    let mut area = geodesic_ring_area(&poly.exterior.points).abs();
    for hole in &poly.interiors {
        area -= geodesic_ring_area(&hole.points).abs();
    }
    area.max(0.0)
}

/// Signed spherical area of a single ring in square metres (Chamberlain–Duquette).
/// Positive for a counter-clockwise ring, negative for clockwise; `< 3` distinct
/// vertices ⇒ 0. The ring need not be explicitly closed (the wrap edge is implied).
pub fn geodesic_ring_area(ring: &[Point]) -> f64 {
    let n = ring.len();
    if n < 3 {
        return 0.0;
    }
    let mut total = 0.0;
    for i in 0..n {
        let p1 = &ring[i];
        let p2 = &ring[(i + 1) % n];
        let lon1 = p1.x.to_radians();
        let lon2 = p2.x.to_radians();
        let lat1 = p1.y.to_radians();
        let lat2 = p2.y.to_radians();
        total += (lon2 - lon1) * (2.0 + lat1.sin() + lat2.sin());
    }
    total * EARTH_MEAN_RADIUS * EARTH_MEAN_RADIUS / 2.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::LineString;

    fn pt(lon: f64, lat: f64) -> Point {
        Point::new(lon, lat)
    }

    #[test]
    fn haversine_known_city_pairs() {
        // London (lon -0.1278, lat 51.5074) → Paris (lon 2.3522, lat 48.8566) ≈ 343 km.
        let d = haversine_distance(&pt(-0.1278, 51.5074), &pt(2.3522, 48.8566));
        assert!(
            (d - 343_500.0).abs() < 3_000.0,
            "London-Paris haversine {d}"
        );

        // New York → Los Angeles ≈ 3936 km.
        let d2 = haversine_distance(&pt(-74.0060, 40.7128), &pt(-118.2437, 34.0522));
        assert!((d2 - 3_936_000.0).abs() < 20_000.0, "NY-LA haversine {d2}");

        // Symmetry + coincidence.
        assert_eq!(haversine_distance(&pt(10.0, 20.0), &pt(10.0, 20.0)), 0.0);
        let a = pt(5.0, 5.0);
        let b = pt(-3.0, 40.0);
        assert!((haversine_distance(&a, &b) - haversine_distance(&b, &a)).abs() < 1e-6);
    }

    #[test]
    fn vincenty_known_city_pairs_and_agreement() {
        // Vincenty ellipsoidal London→Paris ≈ 343.9 km.
        let d = vincenty_distance(&pt(-0.1278, 51.5074), &pt(2.3522, 48.8566));
        assert!((d - 343_900.0).abs() < 2_000.0, "London-Paris vincenty {d}");

        // NY → LA ≈ 3944 km (ellipsoidal).
        let d2 = vincenty_distance(&pt(-74.0060, 40.7128), &pt(-118.2437, 34.0522));
        assert!((d2 - 3_944_000.0).abs() < 15_000.0, "NY-LA vincenty {d2}");

        // Vincenty and Haversine agree to within ~0.5 % on a mid-latitude pair.
        let a = pt(-0.1278, 51.5074);
        let b = pt(2.3522, 48.8566);
        let (v, h) = (vincenty_distance(&a, &b), haversine_distance(&a, &b));
        assert!(((v - h) / v).abs() < 0.005, "vincenty {v} vs haversine {h}");

        assert_eq!(vincenty_distance(&pt(1.0, 1.0), &pt(1.0, 1.0)), 0.0);
    }

    #[test]
    fn geodesic_area_matches_planar_estimate() {
        // Cross-check the spherical polygon area against an INDEPENDENT planar-on-sphere
        // estimate for a small 1°×1° cell at mid-latitude (45°), where curvature error
        // is tiny: width ≈ Δλ·R·cos φ, height ≈ Δφ·R (arc lengths on the mean-radius
        // sphere). The Chamberlain–Duquette formula must agree to well under 1 %.
        // (A pole-touching polygon is intentionally avoided — longitude is degenerate at
        // the pole, where this formula, like PostGIS's, breaks down.)
        let ring = vec![pt(0.0, 45.0), pt(1.0, 45.0), pt(1.0, 46.0), pt(0.0, 46.0)];
        let area = geodesic_ring_area(&ring).abs();
        let one_deg = std::f64::consts::PI / 180.0;
        let height = one_deg * EARTH_MEAN_RADIUS;
        let width = one_deg * EARTH_MEAN_RADIUS * (45.5_f64.to_radians()).cos();
        let estimate = width * height;
        assert!(
            (area - estimate).abs() / estimate < 0.01,
            "cell area {area} vs planar estimate {estimate}"
        );
        assert!(area > 0.0);
    }

    #[test]
    fn geodesic_area_subtracts_holes() {
        // A 10°×10° box near the equator with a 2°×2° hole punched in it: area must
        // drop by roughly the hole's share and stay positive.
        let ext = LineString::new(vec![
            pt(0.0, 0.0),
            pt(10.0, 0.0),
            pt(10.0, 10.0),
            pt(0.0, 10.0),
            pt(0.0, 0.0),
        ]);
        let hole = LineString::new(vec![
            pt(4.0, 4.0),
            pt(6.0, 4.0),
            pt(6.0, 6.0),
            pt(4.0, 6.0),
            pt(4.0, 4.0),
        ]);
        let solid = Polygon::new(ext.clone(), Vec::new());
        let holed = Polygon::new(ext, vec![hole]);
        let a_solid = geodesic_area(&solid);
        let a_holed = geodesic_area(&holed);
        assert!(a_holed > 0.0);
        assert!(
            a_holed < a_solid,
            "holed {a_holed} should be < solid {a_solid}"
        );
        // The 2°×2° hole is ~4/100 of the 10°×10° box.
        let frac = (a_solid - a_holed) / a_solid;
        assert!((0.02..0.06).contains(&frac), "hole fraction {frac}");
    }
}
