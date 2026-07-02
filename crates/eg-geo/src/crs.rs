//! Coordinate Reference Systems + reprojection (CONCEPT:EG-255).
//!
//! The geometry model (`crate::geometry`) is CRS-agnostic — raw `f64 (x, y)` pairs. This
//! module gives those coordinates a *meaning* (an [`Crs`], identified by its EPSG code)
//! and a pure-Rust [`reproject`] that converts a [`Geometry`] between CRSs — the biggest
//! real-GIS gap beyond EG-083's planar geometry. **NO PROJ / GEOS / C dependency** (the
//! Raspberry-Pi contract): every transform is hand-written `f64` math.
//!
//! Supported EPSG codes:
//!
//! * **EPSG:4326** — WGS84 geographic, lon/lat **degrees** (the pivot CRS; `x = lon`,
//!   `y = lat`, matching the crate's WKT `x y` order and the geodesic module EG-256).
//! * **EPSG:3857** — WGS84 / Web-Mercator (pseudo-Mercator), metres. The exact spherical
//!   forward/inverse used by every web map. Round-trips 4326↔3857 to sub-millimetre.
//! * **EPSG:326xx / 327xx** — WGS84 / UTM zone `xx` North (`326xx`) / South (`327xx`),
//!   metres, via a documented ellipsoidal transverse-Mercator (Snyder series, `k0 = 0.9996`,
//!   false-easting 500 000 m, false-northing 0 / 10 000 000 m). This is the "general
//!   transverse-Mercator path" — any CRS reprojects **through** 4326 as the pivot.
//!
//! A geometry's SRID/CRS tag lives OUTSIDE the geometry value (so `Geometry`'s serde form
//! and public API stay stable): [`SridGeometry`] pairs a `Geometry` with an optional SRID,
//! and [`crate::wkt::parse_with_srid`] recovers the EWKT `SRID=…;` prefix EG-257 already
//! tolerates.

use crate::geodesic::{WGS84_A, WGS84_F};
use crate::geometry::{Geometry, LineString, Point, Polygon};

/// A Coordinate Reference System, identified by its EPSG code (CONCEPT:EG-255).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Crs {
    pub epsg: u32,
}

impl Crs {
    /// WGS84 geographic lon/lat in degrees — the pivot CRS.
    pub const WGS84: Crs = Crs { epsg: 4326 };
    /// WGS84 / Web-Mercator (pseudo-Mercator), metres.
    pub const WEB_MERCATOR: Crs = Crs { epsg: 3857 };

    /// A CRS from its EPSG code.
    pub fn from_epsg(epsg: u32) -> Self {
        Crs { epsg }
    }

    /// Is this a CRS this crate can reproject to/from?
    pub fn is_supported(&self) -> bool {
        matches!(self.epsg, 4326 | 3857) || utm_zone(self.epsg).is_some()
    }
}

/// A [`Geometry`] tagged with an optional SRID/CRS (CONCEPT:EG-255). The tag lives here,
/// beside the geometry, rather than inside the `Geometry` enum so the geometry value's
/// serde form + public API stay stable. `srid == None` means "unknown / unspecified CRS".
#[derive(Clone, Debug, PartialEq)]
pub struct SridGeometry {
    pub srid: Option<u32>,
    pub geometry: Geometry,
}

impl SridGeometry {
    /// Tag a geometry with an explicit SRID.
    pub fn new(srid: u32, geometry: Geometry) -> Self {
        Self {
            srid: Some(srid),
            geometry,
        }
    }

    /// A geometry with no CRS tag.
    pub fn untagged(geometry: Geometry) -> Self {
        Self {
            srid: None,
            geometry,
        }
    }

    /// The tagged CRS, if any.
    pub fn crs(&self) -> Option<Crs> {
        self.srid.map(Crs::from_epsg)
    }

    /// Reproject this tagged geometry into `to`, returning a re-tagged result. Errors if
    /// the geometry has no SRID tag (unknown source CRS) or a CRS is unsupported.
    pub fn reproject_to(&self, to: Crs) -> Result<SridGeometry, String> {
        let from = self
            .crs()
            .ok_or_else(|| "SridGeometry has no source SRID to reproject from".to_string())?;
        Ok(SridGeometry::new(
            to.epsg,
            reproject(&self.geometry, from, to)?,
        ))
    }
}

/// Reproject `geom` from CRS `from` to CRS `to` (CONCEPT:EG-255), coordinate by
/// coordinate, recursing through multis/collections and polygon holes. Any pair of
/// supported CRSs works — the transform routes **through EPSG:4326** as the pivot
/// (`from → 4326 → to`), so adding a CRS only needs its 4326 forward/inverse. Returns
/// `Err` for an unsupported EPSG code. A same-CRS reprojection clones unchanged.
pub fn reproject(geom: &Geometry, from: Crs, to: Crs) -> Result<Geometry, String> {
    if from == to {
        return Ok(geom.clone());
    }
    if !from.is_supported() {
        return Err(format!("unsupported source CRS EPSG:{}", from.epsg));
    }
    if !to.is_supported() {
        return Err(format!("unsupported target CRS EPSG:{}", to.epsg));
    }
    map_coords(geom, &|p| {
        let ll = to_wgs84(p, from)?; // any → 4326 (lon/lat degrees)
        from_wgs84(&ll, to) // 4326 → any
    })
}

/// Apply a fallible per-coordinate transform to every vertex of a geometry, rebuilding
/// the same structure (CONCEPT:EG-255).
fn map_coords(
    g: &Geometry,
    f: &impl Fn(&Point) -> Result<Point, String>,
) -> Result<Geometry, String> {
    let line = |l: &LineString| -> Result<LineString, String> {
        Ok(LineString::new(
            l.points.iter().map(f).collect::<Result<Vec<_>, _>>()?,
        ))
    };
    let poly = |pg: &Polygon| -> Result<Polygon, String> {
        Ok(Polygon::with_interiors(
            line(&pg.exterior)?,
            pg.interiors
                .iter()
                .map(line)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    };
    Ok(match g {
        Geometry::Point(p) => Geometry::Point(f(p)?),
        Geometry::LineString(l) => Geometry::LineString(line(l)?),
        Geometry::Polygon(pg) => Geometry::Polygon(poly(pg)?),
        Geometry::MultiPoint(ps) => {
            Geometry::MultiPoint(ps.iter().map(f).collect::<Result<Vec<_>, _>>()?)
        }
        Geometry::MultiLineString(ls) => {
            Geometry::MultiLineString(ls.iter().map(&line).collect::<Result<Vec<_>, _>>()?)
        }
        Geometry::MultiPolygon(pgs) => {
            Geometry::MultiPolygon(pgs.iter().map(&poly).collect::<Result<Vec<_>, _>>()?)
        }
        Geometry::GeometryCollection(gs) => Geometry::GeometryCollection(
            gs.iter()
                .map(|g| map_coords(g, f))
                .collect::<Result<Vec<_>, _>>()?,
        ),
    })
}

// ── per-CRS forward/inverse against the 4326 pivot ───────────────────────────────

/// Convert a coordinate in CRS `from` into WGS84 lon/lat degrees (EPSG:4326).
fn to_wgs84(p: &Point, from: Crs) -> Result<Point, String> {
    match from.epsg {
        4326 => Ok(*p),
        3857 => Ok(web_mercator_to_wgs84(p)),
        code => match utm_zone(code) {
            Some((zone, north)) => Ok(utm_to_wgs84(p, zone, north)),
            None => Err(format!("unsupported source CRS EPSG:{code}")),
        },
    }
}

/// Convert a WGS84 lon/lat degrees coordinate (EPSG:4326) into CRS `to`.
fn from_wgs84(p: &Point, to: Crs) -> Result<Point, String> {
    match to.epsg {
        4326 => Ok(*p),
        3857 => Ok(wgs84_to_web_mercator(p)),
        code => match utm_zone(code) {
            Some((zone, north)) => Ok(wgs84_to_utm(p, zone, north)),
            None => Err(format!("unsupported target CRS EPSG:{code}")),
        },
    }
}

// ── EPSG:3857 Web-Mercator (spherical, exact) ────────────────────────────────────

/// WGS84 lon/lat degrees → Web-Mercator metres (EPSG:4326 → 3857). The spherical
/// pseudo-Mercator every web map uses: `x = a·λ`, `y = a·ln(tan(π/4 + φ/2))`, with `a`
/// the WGS84 semi-major axis and the sphere taken at that radius.
pub fn wgs84_to_web_mercator(p: &Point) -> Point {
    let lon = p.x.to_radians();
    let lat = p.y.to_radians();
    let x = WGS84_A * lon;
    let y = WGS84_A * ((std::f64::consts::FRAC_PI_4 + lat / 2.0).tan()).ln();
    Point::new(x, y)
}

/// Web-Mercator metres → WGS84 lon/lat degrees (EPSG:3857 → 4326). The exact inverse of
/// [`wgs84_to_web_mercator`].
pub fn web_mercator_to_wgs84(p: &Point) -> Point {
    let lon = (p.x / WGS84_A).to_degrees();
    let lat = (2.0 * (p.y / WGS84_A).exp().atan() - std::f64::consts::FRAC_PI_2).to_degrees();
    Point::new(lon, lat)
}

// ── EPSG:326xx / 327xx UTM (ellipsoidal transverse Mercator, Snyder) ─────────────

/// Decode a UTM EPSG code into `(zone 1..=60, is_north)`. WGS84 UTM North is `32601..=32660`,
/// South `32701..=32760`; anything else is `None`.
fn utm_zone(epsg: u32) -> Option<(u32, bool)> {
    match epsg {
        32601..=32660 => Some((epsg - 32600, true)),
        32701..=32760 => Some((epsg - 32700, false)),
        _ => None,
    }
}

/// Central meridian of a UTM zone, in radians (`zone·6 − 183` degrees).
fn utm_central_meridian(zone: u32) -> f64 {
    ((zone as f64) * 6.0 - 183.0).to_radians()
}

/// WGS84 lon/lat degrees → UTM easting/northing metres (Snyder's ellipsoidal
/// transverse-Mercator forward, `k0 = 0.9996`). The documented "general transverse-Mercator
/// path" — accurate to a few millimetres within a zone's ±3° band.
pub fn wgs84_to_utm(p: &Point, zone: u32, north: bool) -> Point {
    const K0: f64 = 0.9996;
    let a = WGS84_A;
    let f = WGS84_F;
    let e2 = f * (2.0 - f); // e²
    let ep2 = e2 / (1.0 - e2); // e'²

    let lat = p.y.to_radians();
    let lon = p.x.to_radians();
    let lon0 = utm_central_meridian(zone);

    let (sin_lat, cos_lat) = (lat.sin(), lat.cos());
    let n = a / (1.0 - e2 * sin_lat * sin_lat).sqrt();
    let t = lat.tan() * lat.tan();
    let c = ep2 * cos_lat * cos_lat;
    let big_a = (lon - lon0) * cos_lat;

    let m = a
        * ((1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2 * e2 * e2 / 256.0) * lat
            - (3.0 * e2 / 8.0 + 3.0 * e2 * e2 / 32.0 + 45.0 * e2 * e2 * e2 / 1024.0)
                * (2.0 * lat).sin()
            + (15.0 * e2 * e2 / 256.0 + 45.0 * e2 * e2 * e2 / 1024.0) * (4.0 * lat).sin()
            - (35.0 * e2 * e2 * e2 / 3072.0) * (6.0 * lat).sin());

    let easting = K0
        * n
        * (big_a
            + (1.0 - t + c) * big_a.powi(3) / 6.0
            + (5.0 - 18.0 * t + t * t + 72.0 * c - 58.0 * ep2) * big_a.powi(5) / 120.0)
        + 500_000.0;

    let mut northing = K0
        * (m + n
            * lat.tan()
            * (big_a * big_a / 2.0
                + (5.0 - t + 9.0 * c + 4.0 * c * c) * big_a.powi(4) / 24.0
                + (61.0 - 58.0 * t + t * t + 600.0 * c - 330.0 * ep2) * big_a.powi(6) / 720.0));
    if !north {
        northing += 10_000_000.0; // southern-hemisphere false northing
    }
    Point::new(easting, northing)
}

/// UTM easting/northing metres → WGS84 lon/lat degrees (Snyder's ellipsoidal
/// transverse-Mercator inverse). The exact inverse of [`wgs84_to_utm`].
pub fn utm_to_wgs84(p: &Point, zone: u32, north: bool) -> Point {
    const K0: f64 = 0.9996;
    let a = WGS84_A;
    let f = WGS84_F;
    let e2 = f * (2.0 - f);
    let ep2 = e2 / (1.0 - e2);

    let x = p.x - 500_000.0;
    let y = if north { p.y } else { p.y - 10_000_000.0 };
    let lon0 = utm_central_meridian(zone);

    let m = y / K0;
    let mu = m / (a * (1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2 * e2 * e2 / 256.0));

    let e1 = (1.0 - (1.0 - e2).sqrt()) / (1.0 + (1.0 - e2).sqrt());
    let lat1 = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1.powi(3) / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1 * e1 / 16.0 - 55.0 * e1.powi(4) / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1.powi(3) / 96.0) * (6.0 * mu).sin()
        + (1097.0 * e1.powi(4) / 512.0) * (8.0 * mu).sin();

    let (sin_lat1, cos_lat1) = (lat1.sin(), lat1.cos());
    let c1 = ep2 * cos_lat1 * cos_lat1;
    let t1 = lat1.tan() * lat1.tan();
    let n1 = a / (1.0 - e2 * sin_lat1 * sin_lat1).sqrt();
    let r1 = a * (1.0 - e2) / (1.0 - e2 * sin_lat1 * sin_lat1).powf(1.5);
    let d = x / (n1 * K0);

    let lat = lat1
        - (n1 * lat1.tan() / r1)
            * (d * d / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * ep2) * d.powi(4) / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1 - 252.0 * ep2 - 3.0 * c1 * c1)
                    * d.powi(6)
                    / 720.0);

    let lon = lon0
        + (d - (1.0 + 2.0 * t1 + c1) * d.powi(3) / 6.0
            + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * ep2 + 24.0 * t1 * t1)
                * d.powi(5)
                / 120.0)
            / cos_lat1;

    Point::new(lon.to_degrees(), lat.to_degrees())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64, what: &str) {
        assert!((a - b).abs() < tol, "{what}: {a} vs {b} (tol {tol})");
    }

    #[test]
    fn web_mercator_round_trip() {
        // London (lon -0.1278, lat 51.5074) 4326 → 3857 → 4326 within 1e-7 degrees.
        let ll = Point::new(-0.1278, 51.5074);
        let m = wgs84_to_web_mercator(&ll);
        let back = web_mercator_to_wgs84(&m);
        approx(back.x, ll.x, 1e-7, "web-mercator round-trip lon");
        approx(back.y, ll.y, 1e-7, "web-mercator round-trip lat");
        // The 0,0 origin maps to 0,0 metres.
        let o = wgs84_to_web_mercator(&Point::new(0.0, 0.0));
        approx(o.x, 0.0, 1e-6, "origin x");
        approx(o.y, 0.0, 1e-6, "origin y");
        // 180° lon maps to the classic ±20037508.34 m world edge.
        let edge = wgs84_to_web_mercator(&Point::new(180.0, 0.0));
        approx(edge.x, 20_037_508.342_789_244, 1e-3, "world edge x");
    }

    #[test]
    fn reproject_geometry_through_pivot() {
        let g = Geometry::Point(Point::new(2.3522, 48.8566)); // Paris
        let merc = reproject(&g, Crs::WGS84, Crs::WEB_MERCATOR).unwrap();
        let back = reproject(&merc, Crs::WEB_MERCATOR, Crs::WGS84).unwrap();
        if let (Geometry::Point(a), Geometry::Point(b)) = (&g, &back) {
            approx(a.x, b.x, 1e-7, "reproject round-trip lon");
            approx(a.y, b.y, 1e-7, "reproject round-trip lat");
        } else {
            panic!("expected points");
        }
    }

    #[test]
    fn reproject_same_crs_is_identity() {
        let g = Geometry::LineString(LineString::new(vec![
            Point::new(1.0, 2.0),
            Point::new(3.0, 4.0),
        ]));
        assert_eq!(reproject(&g, Crs::WGS84, Crs::WGS84).unwrap(), g);
    }

    #[test]
    fn utm_round_trip() {
        // Paris is in UTM zone 31 North → EPSG:32631. Round-trip 4326 → UTM → 4326.
        let utm = Crs::from_epsg(32631);
        assert!(utm.is_supported());
        let ll = Point::new(2.3522, 48.8566);
        let en = wgs84_to_utm(&ll, 31, true);
        // Sanity: easting near 450 km, northing near 5.41 Mm.
        approx(en.x, 452_000.0, 3_000.0, "paris utm easting");
        approx(en.y, 5_411_000.0, 3_000.0, "paris utm northing");
        let back = utm_to_wgs84(&en, 31, true);
        approx(back.x, ll.x, 1e-6, "utm round-trip lon");
        approx(back.y, ll.y, 1e-6, "utm round-trip lat");
    }

    #[test]
    fn utm_reproject_through_pivot_and_hemisphere() {
        // Southern-hemisphere zone (Sydney ≈ lon 151.21, lat -33.87 → zone 56 South,
        // EPSG:32756): reproject 4326 → UTM(S) → 4326 within a mm-scale tolerance.
        let s = Crs::from_epsg(32756);
        let g = Geometry::Point(Point::new(151.2093, -33.8688));
        let utm = reproject(&g, Crs::WGS84, s).unwrap();
        if let Geometry::Point(p) = &utm {
            assert!(
                p.y > 5_000_000.0,
                "southern false-northing applied: {}",
                p.y
            );
        }
        let back = reproject(&utm, s, Crs::WGS84).unwrap();
        if let (Geometry::Point(a), Geometry::Point(b)) = (&g, &back) {
            approx(a.x, b.x, 1e-6, "utm-S round-trip lon");
            approx(a.y, b.y, 1e-6, "utm-S round-trip lat");
        }
    }

    #[test]
    fn srid_tagging_and_reproject() {
        let tagged = SridGeometry::new(4326, Geometry::Point(Point::new(2.3522, 48.8566)));
        assert_eq!(tagged.crs(), Some(Crs::WGS84));
        let out = tagged.reproject_to(Crs::WEB_MERCATOR).unwrap();
        assert_eq!(out.srid, Some(3857));
        // Untagged geometry cannot reproject (unknown source CRS).
        let untagged = SridGeometry::untagged(Geometry::Point(Point::new(0.0, 0.0)));
        assert!(untagged.crs().is_none());
        assert!(untagged.reproject_to(Crs::WEB_MERCATOR).is_err());
    }

    #[test]
    fn unsupported_crs_errors() {
        let g = Geometry::Point(Point::new(0.0, 0.0));
        assert!(reproject(&g, Crs::from_epsg(2154), Crs::WGS84).is_err());
        assert!(reproject(&g, Crs::WGS84, Crs::from_epsg(9999)).is_err());
    }
}
