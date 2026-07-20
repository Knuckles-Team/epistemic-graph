//! CRS **registry** + generic affine / Helmert transforms (CONCEPT:EG-KG.domains.geo-registry).
//!
//! [`crate::crs`] (CONCEPT:EG-KG.domains.coordinate-reference-system) gave the engine the hard-coded WGS84 ⇄ Web-Mercator ⇄
//! UTM math. EG-262 layers a *registry* on top: an [`CrsRegistry`] keyed by EPSG code that
//! resolves a code to a [`CrsDef`] — one of the built-in projected systems **or** a generic
//! **affine** / **Helmert** parameter set defined relative to the WGS84 lon/lat pivot. This
//! lets a caller register an arbitrary local/engineering CRS (a survey grid, a floor-plan,
//! a scanned-map georeference) as an [`Affine`] and then [`CrsRegistry::transform`] any
//! geometry between it and every other registered CRS — the PostGIS `ST_Transform` idiom,
//! pure-Rust, NO PROJ/GEOS/C dependency (the Raspberry-Pi contract).
//!
//! Every transform routes **through EPSG:4326** as the pivot (`from → 4326 → to`), reusing
//! the EG-255 forward/inverse for the built-ins, so adding a CRS only needs its mapping to
//! and from lon/lat degrees.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::crs::{utm_to_wgs84, web_mercator_to_wgs84, wgs84_to_utm, wgs84_to_web_mercator, Crs};
use crate::geometry::{Geometry, LineString, Point, Polygon};

/// A 2-D affine transform mapping `(x, y) → (x', y')` (CONCEPT:EG-KG.domains.geo-registry):
///
/// ```text
/// x' = a·x + b·y + xoff
/// y' = d·x + e·y + yoff
/// ```
///
/// This is the general 6-parameter affine — translation, scale, rotation, shear and
/// reflection are all special cases (see the constructors). A CRS registered as
/// [`CrsDef::Affine`] stores the affine that maps **WGS84 lon/lat degrees → that CRS**
/// (its forward); the inverse ([`Affine::inverse`]) recovers lon/lat.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Affine {
    pub a: f64,
    pub b: f64,
    pub xoff: f64,
    pub d: f64,
    pub e: f64,
    pub yoff: f64,
}

impl Affine {
    /// The identity transform (`x' = x`, `y' = y`).
    pub const IDENTITY: Affine = Affine {
        a: 1.0,
        b: 0.0,
        xoff: 0.0,
        d: 0.0,
        e: 1.0,
        yoff: 0.0,
    };

    /// A pure translation by `(tx, ty)`.
    pub fn translation(tx: f64, ty: f64) -> Affine {
        Affine {
            a: 1.0,
            b: 0.0,
            xoff: tx,
            d: 0.0,
            e: 1.0,
            yoff: ty,
        }
    }

    /// A pure (possibly anisotropic) scaling by `(sx, sy)` about the origin.
    pub fn scale(sx: f64, sy: f64) -> Affine {
        Affine {
            a: sx,
            b: 0.0,
            xoff: 0.0,
            d: 0.0,
            e: sy,
            yoff: 0.0,
        }
    }

    /// A rotation by `theta` radians about the origin (counter-clockwise).
    pub fn rotation(theta: f64) -> Affine {
        let (s, c) = theta.sin_cos();
        Affine {
            a: c,
            b: -s,
            xoff: 0.0,
            d: s,
            e: c,
            yoff: 0.0,
        }
    }

    /// A **4-parameter 2-D Helmert similarity** (CONCEPT:EG-KG.domains.geo-registry): uniform `scale`, a
    /// `rotation` (radians, CCW) and a `(tx, ty)` translation — a conformal transform
    /// (angles preserved), the planar special case of the classic Helmert datum shift and
    /// exactly what georeferences a scanned map or ties two survey grids together:
    ///
    /// ```text
    /// x' = tx + scale·( cosθ·x − sinθ·y )
    /// y' = ty + scale·( sinθ·x + cosθ·y )
    /// ```
    pub fn helmert(scale: f64, rotation: f64, tx: f64, ty: f64) -> Affine {
        let (s, c) = rotation.sin_cos();
        Affine {
            a: scale * c,
            b: -scale * s,
            xoff: tx,
            d: scale * s,
            e: scale * c,
            yoff: ty,
        }
    }

    /// Apply the transform to a single coordinate.
    pub fn apply(&self, p: &Point) -> Point {
        Point::new(
            self.a * p.x + self.b * p.y + self.xoff,
            self.d * p.x + self.e * p.y + self.yoff,
        )
    }

    /// The determinant of the linear part (`a·e − b·d`); zero ⇒ non-invertible (degenerate).
    pub fn determinant(&self) -> f64 {
        self.a * self.e - self.b * self.d
    }

    /// The inverse transform, or `None` when the linear part is singular (determinant 0).
    pub fn inverse(&self) -> Option<Affine> {
        let det = self.determinant();
        if det == 0.0 {
            return None;
        }
        let inv = 1.0 / det;
        // Inverse linear part.
        let ia = self.e * inv;
        let ib = -self.b * inv;
        let id = -self.d * inv;
        let ie = self.a * inv;
        // Inverse translation: -(L⁻¹ · offset).
        let ixoff = -(ia * self.xoff + ib * self.yoff);
        let iyoff = -(id * self.xoff + ie * self.yoff);
        Some(Affine {
            a: ia,
            b: ib,
            xoff: ixoff,
            d: id,
            e: ie,
            yoff: iyoff,
        })
    }

    /// Compose: the transform that applies `self` first, then `after`
    /// (`after ∘ self`). `p.transform(self).transform(after) == p.transform(self.then(after))`.
    pub fn then(&self, after: &Affine) -> Affine {
        Affine {
            a: after.a * self.a + after.b * self.d,
            b: after.a * self.b + after.b * self.e,
            xoff: after.a * self.xoff + after.b * self.yoff + after.xoff,
            d: after.d * self.a + after.e * self.d,
            e: after.d * self.b + after.e * self.e,
            yoff: after.d * self.xoff + after.e * self.yoff + after.yoff,
        }
    }
}

/// How the registry resolves an EPSG code to concrete forward/inverse math (CONCEPT:EG-KG.domains.geo-registry).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum CrsDef {
    /// EPSG:4326 — WGS84 geographic lon/lat degrees (the pivot; identity to itself).
    Wgs84,
    /// EPSG:3857 — WGS84 / Web-Mercator (spherical pseudo-Mercator), metres.
    WebMercator,
    /// A WGS84 / UTM zone (`zone` 1..=60, `north` hemisphere flag), metres.
    Utm { zone: u32, north: bool },
    /// A CRS defined by an [`Affine`] mapping **WGS84 lon/lat degrees → this CRS** (its
    /// forward). Its inverse recovers lon/lat — so a generic local/engineering grid, a
    /// georeferenced raster or a Helmert-tied survey frame all round-trip through the pivot.
    Affine(Affine),
}

/// A registry mapping EPSG codes to [`CrsDef`]s (CONCEPT:EG-KG.domains.geo-registry). Seed it with
/// [`CrsRegistry::standard`] (the EG-255 built-ins) and [`CrsRegistry::register`] any
/// custom affine/Helmert CRS, then [`CrsRegistry::transform`] geometries between any two
/// registered codes.
#[derive(Clone, Debug, Default)]
pub struct CrsRegistry {
    defs: HashMap<u32, CrsDef>,
}

impl CrsRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self {
            defs: HashMap::new(),
        }
    }

    /// A registry pre-loaded with the EG-255 built-ins: EPSG:4326 (WGS84) and EPSG:3857
    /// (Web-Mercator). UTM codes (`326xx`/`327xx`) resolve *algorithmically* even without an
    /// explicit entry (see [`CrsRegistry::resolve`]), so only the two geographic/global
    /// systems need seeding.
    pub fn standard() -> Self {
        let mut r = Self::new();
        r.register(4326, CrsDef::Wgs84);
        r.register(3857, CrsDef::WebMercator);
        r
    }

    /// Register (or replace) the definition for an EPSG code.
    pub fn register(&mut self, epsg: u32, def: CrsDef) {
        self.defs.insert(epsg, def);
    }

    /// Register a custom CRS defined by an affine map **from WGS84 lon/lat degrees**
    /// (convenience over [`CrsRegistry::register`] + [`CrsDef::Affine`]).
    pub fn register_affine(&mut self, epsg: u32, from_wgs84: Affine) {
        self.register(epsg, CrsDef::Affine(from_wgs84));
    }

    /// Resolve an EPSG code to its [`CrsDef`]: an explicit registration wins; otherwise a
    /// UTM code (`326xx` North / `327xx` South) resolves algorithmically. `None` when unknown.
    pub fn resolve(&self, epsg: u32) -> Option<CrsDef> {
        if let Some(d) = self.defs.get(&epsg) {
            return Some(*d);
        }
        match epsg {
            4326 => Some(CrsDef::Wgs84),
            3857 => Some(CrsDef::WebMercator),
            32601..=32660 => Some(CrsDef::Utm {
                zone: epsg - 32600,
                north: true,
            }),
            32701..=32760 => Some(CrsDef::Utm {
                zone: epsg - 32700,
                north: false,
            }),
            _ => None,
        }
    }

    /// Is `epsg` resolvable by this registry?
    pub fn contains(&self, epsg: u32) -> bool {
        self.resolve(epsg).is_some()
    }

    /// Convert one coordinate in CRS `def` into WGS84 lon/lat degrees (the pivot).
    fn to_wgs84(def: &CrsDef, p: &Point) -> Result<Point, String> {
        Ok(match def {
            CrsDef::Wgs84 => *p,
            CrsDef::WebMercator => web_mercator_to_wgs84(p),
            CrsDef::Utm { zone, north } => utm_to_wgs84(p, *zone, *north),
            CrsDef::Affine(fwd) => fwd
                .inverse()
                .ok_or_else(|| "affine CRS is non-invertible (singular)".to_string())?
                .apply(p),
        })
    }

    /// Convert one WGS84 lon/lat degrees coordinate (the pivot) into CRS `def`.
    fn from_wgs84(def: &CrsDef, p: &Point) -> Point {
        match def {
            CrsDef::Wgs84 => *p,
            CrsDef::WebMercator => wgs84_to_web_mercator(p),
            CrsDef::Utm { zone, north } => wgs84_to_utm(p, *zone, *north),
            CrsDef::Affine(fwd) => fwd.apply(p),
        }
    }

    /// Transform `geom` from EPSG `from` to EPSG `to` (CONCEPT:EG-KG.domains.geo-registry), routing every
    /// coordinate through the WGS84 pivot (`from → 4326 → to`) and rebuilding the same
    /// geometry structure (recursing through multis/collections and polygon holes). This is
    /// the `ST_Transform` entry point. Same-code transforms clone unchanged; an unregistered
    /// code errors.
    pub fn transform(&self, geom: &Geometry, from: u32, to: u32) -> Result<Geometry, String> {
        if from == to {
            return Ok(geom.clone());
        }
        let from_def = self
            .resolve(from)
            .ok_or_else(|| format!("unregistered source CRS EPSG:{from}"))?;
        let to_def = self
            .resolve(to)
            .ok_or_else(|| format!("unregistered target CRS EPSG:{to}"))?;
        map_coords(geom, &|p| {
            let ll = Self::to_wgs84(&from_def, p)?;
            Ok(Self::from_wgs84(&to_def, &ll))
        })
    }
}

/// Transform a geometry through a standard [`CrsRegistry`] (the EG-255 built-ins + UTM)
/// — the one-shot `ST_Transform` convenience (CONCEPT:EG-KG.domains.geo-registry). For custom affine/Helmert
/// CRSs, build a [`CrsRegistry`], `register_affine`, and call [`CrsRegistry::transform`].
pub fn st_transform(geom: &Geometry, from: Crs, to: Crs) -> Result<Geometry, String> {
    CrsRegistry::standard().transform(geom, from.epsg, to.epsg)
}

/// Apply a fallible per-coordinate transform to every vertex, rebuilding the structure
/// (CONCEPT:EG-KG.domains.geo-registry). Mirrors `crate::crs::map_coords` but kept local so the registry owns
/// its own error strings.
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
        Ok(Polygon::new(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64, what: &str) {
        assert!((a - b).abs() < tol, "{what}: {a} vs {b} (tol {tol})");
    }

    #[test]
    fn eg262_affine_apply_and_inverse_round_trip() {
        // A rotate-scale-translate affine; apply then inverse must return the original.
        let t = Affine::helmert(2.5, 0.7, 100.0, -50.0);
        let p = Point::new(3.0, 4.0);
        let fwd = t.apply(&p);
        let back = t.inverse().unwrap().apply(&fwd);
        approx(back.x, p.x, 1e-9, "affine inverse x");
        approx(back.y, p.y, 1e-9, "affine inverse y");
    }

    #[test]
    fn eg262_affine_compose_matches_sequential() {
        let s = Affine::scale(2.0, 3.0);
        let r = Affine::rotation(0.5);
        let p = Point::new(1.0, 1.0);
        let seq = r.apply(&s.apply(&p)); // s first, then r
        let composed = s.then(&r).apply(&p);
        approx(composed.x, seq.x, 1e-12, "compose x");
        approx(composed.y, seq.y, 1e-12, "compose y");
    }

    #[test]
    fn eg262_singular_affine_has_no_inverse() {
        // Collapses the plane to a line → determinant 0 → non-invertible.
        let degenerate = Affine {
            a: 1.0,
            b: 2.0,
            xoff: 0.0,
            d: 2.0,
            e: 4.0,
            yoff: 0.0,
        };
        assert_eq!(degenerate.determinant(), 0.0);
        assert!(degenerate.inverse().is_none());
        // A registry transform through a singular affine CRS surfaces the error.
        let mut reg = CrsRegistry::standard();
        reg.register_affine(90000, degenerate);
        let g = Geometry::Point(Point::new(1.0, 2.0));
        assert!(reg.transform(&g, 90000, 4326).is_err());
    }

    #[test]
    fn eg262_registry_transform_4326_to_3857_round_trip() {
        // ST_Transform through the registry matches the EG-255 path, and round-trips.
        let reg = CrsRegistry::standard();
        let g = Geometry::Point(Point::new(2.3522, 48.8566)); // Paris
        let merc = reg.transform(&g, 4326, 3857).unwrap();
        let back = reg.transform(&merc, 3857, 4326).unwrap();
        if let (Geometry::Point(a), Geometry::Point(b)) = (&g, &back) {
            approx(a.x, b.x, 1e-7, "registry round-trip lon");
            approx(a.y, b.y, 1e-7, "registry round-trip lat");
        } else {
            panic!("expected points");
        }
    }

    #[test]
    fn eg262_registry_resolves_utm_algorithmically() {
        let reg = CrsRegistry::standard();
        assert_eq!(
            reg.resolve(32631),
            Some(CrsDef::Utm {
                zone: 31,
                north: true
            })
        );
        assert_eq!(
            reg.resolve(32756),
            Some(CrsDef::Utm {
                zone: 56,
                north: false
            })
        );
        assert!(reg.resolve(2154).is_none()); // Lambert-93 not registered
    }

    #[test]
    fn eg262_custom_affine_crs_round_trips_through_pivot() {
        // A local engineering grid defined as a Helmert similarity FROM lon/lat degrees.
        // Register it, transform WGS84 → local → WGS84 within tolerance.
        let mut reg = CrsRegistry::standard();
        let local = Affine::helmert(1000.0, 0.1, 12345.0, -6789.0);
        reg.register_affine(100001, local);
        let g = Geometry::LineString(LineString::new(vec![
            Point::new(-0.1278, 51.5074),
            Point::new(2.3522, 48.8566),
        ]));
        let projected = reg.transform(&g, 4326, 100001).unwrap();
        let back = reg.transform(&projected, 100001, 4326).unwrap();
        if let (Geometry::LineString(a), Geometry::LineString(b)) = (&g, &back) {
            for (pa, pb) in a.points.iter().zip(&b.points) {
                approx(pa.x, pb.x, 1e-9, "custom affine round-trip lon");
                approx(pa.y, pb.y, 1e-9, "custom affine round-trip lat");
            }
        } else {
            panic!("expected line strings");
        }
    }

    #[test]
    fn eg262_st_transform_helper_and_unregistered_errors() {
        let g = Geometry::Point(Point::new(2.3522, 48.8566));
        assert!(st_transform(&g, Crs::WGS84, Crs::WEB_MERCATOR).is_ok());
        // 2154 (Lambert-93) is not in the standard registry.
        assert!(st_transform(&g, Crs::from_epsg(2154), Crs::WGS84).is_err());
    }
}
