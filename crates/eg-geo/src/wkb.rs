//! **WKB — Well-Known Binary** reader/writer (CONCEPT:EG-KG.domains.geojson-gpx-formats).
//!
//! The OGC/ISO binary sibling of the [`crate::wkt`] text codec: a compact byte encoding that
//! every spatial database (PostGIS, GEOS, GDAL) speaks. Hand-rolled here — NO C/GEOS dep — so
//! an eg-geo [`Geometry`] round-trips to and from the exact bytes a PostGIS `ST_AsBinary`
//! emits. Also supports **EWKB** (the PostGIS extension) with an embedded SRID via
//! [`to_ewkb`] / [`from_wkb_srid`], tying WKB to [`crate::crs::SridGeometry`].
//!
//! Encoding (per geometry, recursively):
//! * 1 byte **byte order**: `1` = little-endian (NDR, what [`to_wkb`] writes), `0` = big-endian
//!   (XDR, tolerated on read).
//! * 4-byte `u32` **geometry type**: Point 1, LineString 2, Polygon 3, MultiPoint 4,
//!   MultiLineString 5, MultiPolygon 6, GeometryCollection 7. The EWKB SRID flag `0x2000_0000`
//!   (with a following 4-byte SRID) is read and stripped; [`to_ewkb`] sets it.
//! * then the coordinates: a Point is two `f64`s; counted rings / parts otherwise.

use crate::geometry::{Geometry, LineString, Point, Polygon};

const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_MULTIPOINT: u32 = 4;
const WKB_MULTILINESTRING: u32 = 5;
const WKB_MULTIPOLYGON: u32 = 6;
const WKB_GEOMETRYCOLLECTION: u32 = 7;
/// EWKB high bit signalling an embedded 4-byte SRID follows the type word.
const EWKB_SRID_FLAG: u32 = 0x2000_0000;

/// Serialise a geometry to standard OGC WKB, little-endian (NDR) (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn to_wkb(geom: &Geometry) -> Vec<u8> {
    let mut out = Vec::new();
    write_geom(&mut out, geom, None);
    out
}

/// Serialise a geometry to **EWKB** with an embedded `srid` (PostGIS extension, little-endian)
/// (CONCEPT:EG-KG.domains.geojson-gpx-formats). The SRID rides only on the top-level geometry, matching PostGIS.
pub fn to_ewkb(geom: &Geometry, srid: u32) -> Vec<u8> {
    let mut out = Vec::new();
    write_geom(&mut out, geom, Some(srid));
    out
}

/// Parse a geometry from WKB or EWKB bytes, discarding any SRID (CONCEPT:EG-KG.domains.geojson-gpx-formats).
pub fn from_wkb(bytes: &[u8]) -> Result<Geometry, String> {
    Ok(from_wkb_srid(bytes)?.1)
}

/// Parse a geometry from WKB/EWKB, also returning the embedded SRID if the EWKB flag was set
/// (CONCEPT:EG-KG.domains.geojson-gpx-formats) — the natural bridge to [`crate::crs::SridGeometry`].
pub fn from_wkb_srid(bytes: &[u8]) -> Result<(Option<u32>, Geometry), String> {
    let mut c = Cursor::new(bytes);
    let g = read_geom(&mut c)?;
    Ok((c.last_srid, g))
}

// ── writer ───────────────────────────────────────────────────────────────────────

fn write_geom(out: &mut Vec<u8>, geom: &Geometry, srid: Option<u32>) {
    out.push(1); // NDR little-endian
    let base = match geom {
        Geometry::Point(_) => WKB_POINT,
        Geometry::LineString(_) => WKB_LINESTRING,
        Geometry::Polygon(_) => WKB_POLYGON,
        Geometry::MultiPoint(_) => WKB_MULTIPOINT,
        Geometry::MultiLineString(_) => WKB_MULTILINESTRING,
        Geometry::MultiPolygon(_) => WKB_MULTIPOLYGON,
        Geometry::GeometryCollection(_) => WKB_GEOMETRYCOLLECTION,
    };
    let ty = if srid.is_some() {
        base | EWKB_SRID_FLAG
    } else {
        base
    };
    out.extend_from_slice(&ty.to_le_bytes());
    if let Some(s) = srid {
        out.extend_from_slice(&s.to_le_bytes());
    }
    match geom {
        Geometry::Point(p) => write_point(out, p),
        Geometry::LineString(l) => write_line(out, l),
        Geometry::Polygon(pg) => write_poly(out, pg),
        Geometry::MultiPoint(ps) => {
            out.extend_from_slice(&(ps.len() as u32).to_le_bytes());
            for p in ps {
                write_geom(out, &Geometry::Point(*p), None);
            }
        }
        Geometry::MultiLineString(ls) => {
            out.extend_from_slice(&(ls.len() as u32).to_le_bytes());
            for l in ls {
                write_geom(out, &Geometry::LineString(l.clone()), None);
            }
        }
        Geometry::MultiPolygon(pgs) => {
            out.extend_from_slice(&(pgs.len() as u32).to_le_bytes());
            for pg in pgs {
                write_geom(out, &Geometry::Polygon(pg.clone()), None);
            }
        }
        Geometry::GeometryCollection(gs) => {
            out.extend_from_slice(&(gs.len() as u32).to_le_bytes());
            for g in gs {
                write_geom(out, g, None);
            }
        }
    }
}

fn write_point(out: &mut Vec<u8>, p: &Point) {
    out.extend_from_slice(&p.x.to_le_bytes());
    out.extend_from_slice(&p.y.to_le_bytes());
}

fn write_line(out: &mut Vec<u8>, l: &LineString) {
    out.extend_from_slice(&(l.points.len() as u32).to_le_bytes());
    for p in &l.points {
        write_point(out, p);
    }
}

fn write_poly(out: &mut Vec<u8>, pg: &Polygon) {
    let n_rings = 1 + pg.interiors.len();
    out.extend_from_slice(&(n_rings as u32).to_le_bytes());
    write_ring(out, &pg.exterior);
    for r in &pg.interiors {
        write_ring(out, r);
    }
}

fn write_ring(out: &mut Vec<u8>, r: &LineString) {
    out.extend_from_slice(&(r.points.len() as u32).to_le_bytes());
    for p in &r.points {
        write_point(out, p);
    }
}

// ── reader ───────────────────────────────────────────────────────────────────────

/// A byte cursor tracking position, the current geometry's endianness, and the last SRID seen.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
    little: bool,
    last_srid: Option<u32>,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            little: true,
            last_srid: None,
        }
    }

    fn u8(&mut self) -> Result<u8, String> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| "WKB: unexpected end of input".to_string())?;
        self.pos += 1;
        Ok(b)
    }

    fn u32(&mut self) -> Result<u32, String> {
        let end = self.pos + 4;
        let bytes: [u8; 4] = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| "WKB: truncated u32".to_string())?
            .try_into()
            .unwrap();
        self.pos = end;
        Ok(if self.little {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        })
    }

    fn f64(&mut self) -> Result<f64, String> {
        let end = self.pos + 8;
        let bytes: [u8; 8] = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| "WKB: truncated f64".to_string())?
            .try_into()
            .unwrap();
        self.pos = end;
        Ok(if self.little {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }
}

fn read_geom(c: &mut Cursor) -> Result<Geometry, String> {
    let order = c.u8()?;
    c.little = match order {
        1 => true,
        0 => false,
        other => return Err(format!("WKB: bad byte-order flag {other}")),
    };
    let raw_type = c.u32()?;
    if raw_type & EWKB_SRID_FLAG != 0 {
        c.last_srid = Some(c.u32()?);
    }
    let ty = raw_type & 0x00FF_FFFF; // strip EWKB flag bits (SRID/Z/M)
    match ty {
        WKB_POINT => Ok(Geometry::Point(read_point(c)?)),
        WKB_LINESTRING => Ok(Geometry::LineString(read_line(c)?)),
        WKB_POLYGON => Ok(Geometry::Polygon(read_poly(c)?)),
        WKB_MULTIPOINT => {
            let n = c.u32()? as usize;
            let mut ps = Vec::with_capacity(n);
            for _ in 0..n {
                match read_geom(c)? {
                    Geometry::Point(p) => ps.push(p),
                    _ => return Err("WKB: MultiPoint member is not a Point".to_string()),
                }
            }
            Ok(Geometry::MultiPoint(ps))
        }
        WKB_MULTILINESTRING => {
            let n = c.u32()? as usize;
            let mut ls = Vec::with_capacity(n);
            for _ in 0..n {
                match read_geom(c)? {
                    Geometry::LineString(l) => ls.push(l),
                    _ => return Err("WKB: MultiLineString member is not a LineString".to_string()),
                }
            }
            Ok(Geometry::MultiLineString(ls))
        }
        WKB_MULTIPOLYGON => {
            let n = c.u32()? as usize;
            let mut pgs = Vec::with_capacity(n);
            for _ in 0..n {
                match read_geom(c)? {
                    Geometry::Polygon(pg) => pgs.push(pg),
                    _ => return Err("WKB: MultiPolygon member is not a Polygon".to_string()),
                }
            }
            Ok(Geometry::MultiPolygon(pgs))
        }
        WKB_GEOMETRYCOLLECTION => {
            let n = c.u32()? as usize;
            let mut gs = Vec::with_capacity(n);
            for _ in 0..n {
                gs.push(read_geom(c)?);
            }
            Ok(Geometry::GeometryCollection(gs))
        }
        other => Err(format!("WKB: unknown geometry type {other}")),
    }
}

fn read_point(c: &mut Cursor) -> Result<Point, String> {
    let x = c.f64()?;
    let y = c.f64()?;
    Ok(Point::new(x, y))
}

fn read_line(c: &mut Cursor) -> Result<LineString, String> {
    let n = c.u32()? as usize;
    let mut pts = Vec::with_capacity(n);
    for _ in 0..n {
        pts.push(read_point(c)?);
    }
    Ok(LineString::new(pts))
}

fn read_poly(c: &mut Cursor) -> Result<Polygon, String> {
    let n_rings = c.u32()? as usize;
    if n_rings == 0 {
        return Ok(Polygon::new(LineString::new(Vec::new())));
    }
    let exterior = read_line(c)?;
    let mut interiors = Vec::with_capacity(n_rings - 1);
    for _ in 1..n_rings {
        interiors.push(read_line(c)?);
    }
    Ok(Polygon::with_interiors(exterior, interiors))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(g: Geometry) {
        let bytes = to_wkb(&g);
        let back = from_wkb(&bytes).expect("parse WKB");
        assert_eq!(g, back, "WKB round-trip mismatch");
    }

    #[test]
    fn eg264_wkb_round_trip_point_line_polygon() {
        round_trip(Geometry::Point(Point::new(30.0, 10.0)));
        round_trip(Geometry::LineString(LineString::new(vec![
            Point::new(30.0, 10.0),
            Point::new(10.0, 30.0),
            Point::new(40.0, 40.0),
        ])));
        // Polygon with a hole.
        round_trip(Geometry::Polygon(Polygon::with_interiors(
            LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(10.0, 0.0),
                Point::new(10.0, 10.0),
                Point::new(0.0, 10.0),
                Point::new(0.0, 0.0),
            ]),
            vec![LineString::new(vec![
                Point::new(3.0, 3.0),
                Point::new(7.0, 3.0),
                Point::new(7.0, 7.0),
                Point::new(3.0, 7.0),
                Point::new(3.0, 3.0),
            ])],
        )));
    }

    #[test]
    fn eg264_wkb_round_trip_multis_and_collection() {
        round_trip(Geometry::MultiPoint(vec![
            Point::new(1.0, 2.0),
            Point::new(3.0, 4.0),
        ]));
        round_trip(Geometry::MultiLineString(vec![
            LineString::new(vec![Point::new(0.0, 0.0), Point::new(1.0, 1.0)]),
            LineString::new(vec![Point::new(2.0, 2.0), Point::new(3.0, 3.0)]),
        ]));
        round_trip(Geometry::MultiPolygon(vec![Polygon::new(LineString::new(
            vec![
                Point::new(0.0, 0.0),
                Point::new(1.0, 0.0),
                Point::new(1.0, 1.0),
                Point::new(0.0, 0.0),
            ],
        ))]));
        round_trip(Geometry::GeometryCollection(vec![
            Geometry::Point(Point::new(5.0, 5.0)),
            Geometry::LineString(LineString::new(vec![
                Point::new(0.0, 0.0),
                Point::new(9.0, 9.0),
            ])),
        ]));
    }

    #[test]
    fn eg264_ewkb_carries_srid() {
        let g = Geometry::Point(Point::new(2.3522, 48.8566));
        let bytes = to_ewkb(&g, 4326);
        let (srid, back) = from_wkb_srid(&bytes).expect("parse EWKB");
        assert_eq!(srid, Some(4326));
        assert_eq!(back, g);
        // Standard WKB carries no SRID.
        assert_eq!(from_wkb_srid(&to_wkb(&g)).unwrap().0, None);
    }

    #[test]
    fn eg264_wkb_parses_big_endian_point() {
        // XDR (big-endian) POINT(1 2): order=0, type=1, then two big-endian f64s.
        let mut bytes = vec![0u8];
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&1.0f64.to_be_bytes());
        bytes.extend_from_slice(&2.0f64.to_be_bytes());
        assert_eq!(
            from_wkb(&bytes).unwrap(),
            Geometry::Point(Point::new(1.0, 2.0))
        );
    }

    #[test]
    fn eg264_wkb_rejects_truncated_and_bad_order() {
        assert!(from_wkb(&[]).is_err());
        assert!(from_wkb(&[1, 1, 0, 0]).is_err()); // truncated type
        assert!(from_wkb(&[9, 1, 0, 0, 0]).is_err()); // bad byte-order flag
    }
}
