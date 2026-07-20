//! **Shapefile** reader — `.shp` geometry + `.dbf` attributes + `.shx` index
//! (CONCEPT:EG-KG.domains.geo-formats).
//!
//! The ESRI Shapefile is *the* legacy GIS interchange format: still the default export of
//! ArcGIS, QGIS, `ogr2ogr`, and virtually every open-data portal. A "shapefile" is really a
//! trio of side-car files sharing a base name:
//!
//! * **`.shp`** — the main file: a 100-byte header then variable-length geometry *records*
//!   (Point / PolyLine / Polygon / their Multi forms), big-endian record headers, little-endian
//!   geometry payloads.
//! * **`.dbf`** — a dBASE III/IV table: one fixed-width ASCII row of *attributes* per geometry.
//! * **`.shx`** — a fixed-stride offset index into `.shp` (offset + length per record) enabling
//!   random access; optional for a sequential read.
//!
//! This is a *hand-rolled* pure-Rust **reader** — NO GDAL/OGR/shapelib C dep, matching the
//! [`crate::wkb`]/[`crate::gpx`] house style. It parses the binary `.shp` and the `.dbf`
//! attribute table into eg-geo [`Geometry`]s paired with attribute maps ([`ShapeRecord`]).
//! Coordinates are `(x, y)` = `(lon, lat)` exactly as stored (shapefiles carry no axis
//! swap). A **writer** is the lower-value path (every tool still *reads* the format we would
//! emit) and is a documented B4 follow-up; see the module note at the bottom.
//!
//! Geometry mapping (shape type → eg-geo):
//! * `Null` (0)        → skipped (yields `None` geometry on the record).
//! * `Point` (1)       → [`Geometry::Point`].
//! * `MultiPoint` (8)  → [`Geometry::MultiPoint`].
//! * `PolyLine` (3)    → [`Geometry::LineString`] (1 part) or [`Geometry::MultiLineString`].
//! * `Polygon` (5)     → [`Geometry::Polygon`] (1 exterior) or [`Geometry::MultiPolygon`];
//!   rings are classified by signed area (clockwise = exterior, counter-clockwise = hole per
//!   the ESRI spec) and holes are assigned to the containing exterior via point-in-ring.
//!
//! The `Z`/`M` measured variants (11/13/15/18, 21/…) are read as their 2-D `(x, y)` core — the
//! trailing Z/M arrays are skipped — so a PointZ still yields a planar [`Geometry::Point`].

use crate::geometry::{Geometry, LineString, Point, Polygon};

// ── ESRI shape type codes (little-endian i32 at the head of each .shp record) ────────
const SHP_NULL: i32 = 0;
const SHP_POINT: i32 = 1;
const SHP_POLYLINE: i32 = 3;
const SHP_POLYGON: i32 = 5;
const SHP_MULTIPOINT: i32 = 8;
const SHP_POINT_Z: i32 = 11;
const SHP_POLYLINE_Z: i32 = 13;
const SHP_POLYGON_Z: i32 = 15;
const SHP_MULTIPOINT_Z: i32 = 18;
const SHP_POINT_M: i32 = 21;
const SHP_POLYLINE_M: i32 = 23;
const SHP_POLYGON_M: i32 = 25;
const SHP_MULTIPOINT_M: i32 = 28;

// ── public value model ───────────────────────────────────────────────────────────────

/// One typed `.dbf` attribute cell (CONCEPT:EG-KG.domains.geo-formats). dBASE stores every field as fixed-width
/// ASCII with a one-char type; this preserves that type while trimming the fixed-width padding.
#[derive(Clone, Debug, PartialEq)]
pub enum DbfValue {
    /// `C` — character/text (padding-trimmed).
    Character(String),
    /// `N`/`F` — numeric/float; `None` for a blank cell.
    Numeric(Option<f64>),
    /// `L` — logical: `T/t/Y/y` → `true`, `F/f/N/n` → `false`, `?`/blank → `None`.
    Logical(Option<bool>),
    /// `D` — date, kept as the raw 8-char `YYYYMMDD` string (empty for a blank cell).
    Date(String),
}

/// One record of a shapefile: a geometry (absent for a `Null` shape) plus its ordered `.dbf`
/// attributes as `(field_name, value)` pairs (CONCEPT:EG-KG.domains.geo-formats). Order matches the `.dbf` field
/// descriptors.
#[derive(Clone, Debug, PartialEq)]
pub struct ShapeRecord {
    pub geometry: Option<Geometry>,
    pub attributes: Vec<(String, DbfValue)>,
}

impl ShapeRecord {
    /// Look up an attribute value by (case-sensitive) field name.
    pub fn attr(&self, name: &str) -> Option<&DbfValue> {
        self.attributes
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v)
    }
}

/// A parsed `.shx` offset index (CONCEPT:EG-KG.domains.geo-formats): one `(offset_bytes, length_bytes)` per record,
/// converted from the on-disk 16-bit-word units. Enables random `.shp` access; the sequential
/// reader does not require it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShxIndex {
    /// `(byte offset into .shp of the record header, byte length of the record content)`.
    pub records: Vec<(usize, usize)>,
}

// ── .shp geometry reader ───────────────────────────────────────────────────────────

/// A little/big-endian byte cursor over a shapefile buffer (CONCEPT:EG-KG.domains.geo-formats).
struct Cur<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cur<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn at(buf: &'a [u8], pos: usize) -> Self {
        Self { buf, pos }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| "shp: offset overflow".to_string())?;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| "shp: unexpected end of input".to_string())?;
        self.pos = end;
        Ok(s)
    }

    fn i32_be(&mut self) -> Result<i32, String> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32_le(&mut self) -> Result<i32, String> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn f64_le(&mut self) -> Result<f64, String> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

/// Parse a `.shp` main file into its geometries (CONCEPT:EG-KG.domains.geo-formats). A `Null` shape yields a
/// `None` slot so the geometry list stays 1:1 with the record order (and the `.dbf` rows).
pub fn read_shp(bytes: &[u8]) -> Result<Vec<Option<Geometry>>, String> {
    if bytes.len() < 100 {
        return Err("shp: file shorter than the 100-byte header".to_string());
    }
    let mut c = Cur::new(bytes);
    let file_code = c.i32_be()?;
    if file_code != 9994 {
        return Err(format!("shp: bad file code {file_code} (expected 9994)"));
    }
    // Skip 5 unused i32 (bytes 4..24) + the file-length word (bytes 24..28); we read records
    // until the buffer is exhausted rather than trusting the header length.
    c.pos = 100;
    let mut out = Vec::new();
    // Each record: 8-byte header (record number + content length, both big-endian words) then
    // the geometry payload of `content_len * 2` bytes.
    while c.pos + 8 <= bytes.len() {
        let _rec_no = c.i32_be()?;
        let content_words = c.i32_be()?;
        if content_words < 0 {
            return Err("shp: negative record content length".to_string());
        }
        let content_bytes = content_words as usize * 2;
        let start = c.pos;
        let end = start
            .checked_add(content_bytes)
            .ok_or_else(|| "shp: record length overflow".to_string())?;
        if end > bytes.len() {
            return Err("shp: record content runs past end of file".to_string());
        }
        let mut rc = Cur::at(bytes, start);
        out.push(read_shape(&mut rc)?);
        c.pos = end; // advance exactly by the declared content length
    }
    Ok(out)
}

/// Read one geometry from the start of a record's content (its little-endian shape-type word
/// followed by the type-specific payload).
fn read_shape(c: &mut Cur) -> Result<Option<Geometry>, String> {
    let ty = c.i32_le()?;
    match ty {
        SHP_NULL => Ok(None),
        SHP_POINT | SHP_POINT_Z | SHP_POINT_M => {
            let x = c.f64_le()?;
            let y = c.f64_le()?;
            Ok(Some(Geometry::Point(Point::new(x, y))))
        }
        SHP_MULTIPOINT | SHP_MULTIPOINT_Z | SHP_MULTIPOINT_M => {
            read_bbox(c)?;
            let n = c.i32_le()? as usize;
            let mut pts = Vec::with_capacity(n);
            for _ in 0..n {
                pts.push(Point::new(c.f64_le()?, c.f64_le()?));
            }
            Ok(Some(Geometry::MultiPoint(pts)))
        }
        SHP_POLYLINE | SHP_POLYLINE_Z | SHP_POLYLINE_M => {
            let parts = read_parts(c)?;
            Ok(Some(if parts.len() == 1 {
                Geometry::LineString(LineString::new(parts.into_iter().next().unwrap()))
            } else {
                Geometry::MultiLineString(parts.into_iter().map(LineString::new).collect())
            }))
        }
        SHP_POLYGON | SHP_POLYGON_Z | SHP_POLYGON_M => {
            let parts = read_parts(c)?;
            Ok(Some(assemble_polygon(parts)))
        }
        other => Err(format!("shp: unsupported shape type {other}")),
    }
}

/// Skip a 4-double bounding box (`Xmin Ymin Xmax Ymax`).
fn read_bbox(c: &mut Cur) -> Result<(), String> {
    for _ in 0..4 {
        c.f64_le()?;
    }
    Ok(())
}

/// Read the shared PolyLine/Polygon body: bounding box, `NumParts`, `NumPoints`, the part-start
/// index array, then the flat point array — returning one `Vec<Point>` per part.
fn read_parts(c: &mut Cur) -> Result<Vec<Vec<Point>>, String> {
    read_bbox(c)?;
    let n_parts = c.i32_le()?;
    let n_points = c.i32_le()?;
    if n_parts < 0 || n_points < 0 {
        return Err("shp: negative part/point count".to_string());
    }
    let n_parts = n_parts as usize;
    let n_points = n_points as usize;
    let mut starts = Vec::with_capacity(n_parts);
    for _ in 0..n_parts {
        starts.push(c.i32_le()? as usize);
    }
    let mut all = Vec::with_capacity(n_points);
    for _ in 0..n_points {
        all.push(Point::new(c.f64_le()?, c.f64_le()?));
    }
    // Slice the flat point array by the part-start indices.
    let mut parts = Vec::with_capacity(n_parts);
    for i in 0..n_parts {
        let s = starts[i];
        let e = if i + 1 < n_parts {
            starts[i + 1]
        } else {
            n_points
        };
        if s > e || e > n_points {
            return Err("shp: part index out of range".to_string());
        }
        parts.push(all[s..e].to_vec());
    }
    Ok(parts)
}

/// Signed area of a ring (shoelace). Positive = counter-clockwise, negative = clockwise —
/// matching the ESRI rule that exterior rings are clockwise and holes counter-clockwise.
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

/// Assemble shapefile polygon rings (CONCEPT:EG-KG.domains.geo-formats) into a [`Geometry::Polygon`] (single
/// exterior) or [`Geometry::MultiPolygon`] (multiple exteriors). Clockwise rings
/// (`signed_area < 0`) are exteriors; counter-clockwise rings are holes assigned to the
/// exterior whose ring contains the hole's first vertex.
fn assemble_polygon(parts: Vec<Vec<Point>>) -> Geometry {
    let mut exteriors: Vec<Vec<Point>> = Vec::new();
    let mut holes: Vec<Vec<Point>> = Vec::new();
    for ring in parts {
        if signed_area(&ring) < 0.0 {
            exteriors.push(ring);
        } else {
            holes.push(ring);
        }
    }
    // Degenerate: no clockwise ring — treat every ring as its own exterior (lenient).
    if exteriors.is_empty() {
        let polys: Vec<Polygon> = holes
            .into_iter()
            .map(|r| Polygon::new(LineString::new(r), Vec::new()))
            .collect();
        return if polys.len() == 1 {
            Geometry::Polygon(polys.into_iter().next().unwrap())
        } else {
            Geometry::MultiPolygon(polys)
        };
    }
    let mut polys: Vec<Polygon> = exteriors
        .into_iter()
        .map(|r| Polygon::new(LineString::new(r), Vec::new()))
        .collect();
    for hole in holes {
        let anchor = hole.first().copied();
        // Find the exterior ring that contains the hole's first vertex.
        let mut target = 0usize;
        if let Some(a) = anchor {
            for (i, pg) in polys.iter().enumerate() {
                let ring = Polygon::new(pg.exterior.clone(), Vec::new());
                if ring.contains_point(&a) {
                    target = i;
                    break;
                }
            }
        }
        polys[target].interiors.push(LineString::new(hole));
    }
    if polys.len() == 1 {
        Geometry::Polygon(polys.into_iter().next().unwrap())
    } else {
        Geometry::MultiPolygon(polys)
    }
}

// ── .shx index reader ────────────────────────────────────────────────────────────────

/// Parse a `.shx` index file into byte offsets/lengths (CONCEPT:EG-KG.domains.geo-formats). Same 100-byte header
/// as `.shp`, then 8-byte records of `(offset, content_length)` big-endian 16-bit words.
pub fn read_shx(bytes: &[u8]) -> Result<ShxIndex, String> {
    if bytes.len() < 100 {
        return Err("shx: file shorter than the 100-byte header".to_string());
    }
    let mut c = Cur::at(bytes, 100);
    let mut idx = ShxIndex::default();
    while c.pos + 8 <= bytes.len() {
        let offset_words = c.i32_be()?;
        let length_words = c.i32_be()?;
        if offset_words < 0 || length_words < 0 {
            return Err("shx: negative offset/length".to_string());
        }
        idx.records
            .push((offset_words as usize * 2, length_words as usize * 2));
    }
    Ok(idx)
}

// ── .dbf attribute-table reader ───────────────────────────────────────────────────────

/// A `.dbf` field descriptor: name, one-char type, and fixed byte length (CONCEPT:EG-KG.domains.geo-formats).
#[derive(Clone, Debug, PartialEq)]
pub struct DbfField {
    pub name: String,
    pub ty: char,
    pub length: usize,
}

/// Parse a `.dbf` dBASE table into `(field descriptors, rows-of-values)` (CONCEPT:EG-KG.domains.geo-formats).
/// Deleted records (leading `*`) are skipped.
pub fn read_dbf(bytes: &[u8]) -> Result<(Vec<DbfField>, Vec<Vec<DbfValue>>), String> {
    if bytes.len() < 32 {
        return Err("dbf: file shorter than the 32-byte header".to_string());
    }
    let num_records = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let header_len = u16::from_le_bytes(bytes[8..10].try_into().unwrap()) as usize;
    let record_len = u16::from_le_bytes(bytes[10..12].try_into().unwrap()) as usize;
    if header_len < 33 || header_len > bytes.len() {
        return Err("dbf: implausible header length".to_string());
    }

    // Field descriptors: 32 bytes each from offset 32 until the 0x0D terminator.
    let mut fields = Vec::new();
    let mut off = 32;
    while off + 32 <= header_len {
        if bytes[off] == 0x0D {
            break; // header terminator
        }
        let name_bytes = &bytes[off..off + 11];
        let name_end = name_bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(name_bytes.len());
        let name = String::from_utf8_lossy(&name_bytes[..name_end])
            .trim()
            .to_string();
        let ty = bytes[off + 11] as char;
        let length = bytes[off + 16] as usize;
        fields.push(DbfField { name, ty, length });
        off += 32;
    }

    // Records begin at header_len; each is a 1-byte deletion flag + fixed-width field cells.
    let mut rows = Vec::new();
    let mut rpos = header_len;
    for _ in 0..num_records {
        if rpos + record_len > bytes.len() {
            break; // tolerate a truncated tail
        }
        let flag = bytes[rpos];
        let row = &bytes[rpos + 1..rpos + record_len];
        rpos += record_len;
        if flag == b'*' {
            continue; // deleted record
        }
        let mut cells = Vec::with_capacity(fields.len());
        let mut fpos = 0;
        for f in &fields {
            let raw = row.get(fpos..fpos + f.length).unwrap_or(&[]);
            fpos += f.length;
            cells.push(parse_cell(f.ty, raw));
        }
        rows.push(cells);
    }
    Ok((fields, rows))
}

/// Decode one fixed-width dBASE cell by its field type into a [`DbfValue`].
fn parse_cell(ty: char, raw: &[u8]) -> DbfValue {
    let s = String::from_utf8_lossy(raw);
    let t = s.trim();
    match ty {
        'N' | 'F' => {
            if t.is_empty() {
                DbfValue::Numeric(None)
            } else {
                DbfValue::Numeric(t.parse::<f64>().ok())
            }
        }
        'L' => {
            let b = match t.chars().next() {
                Some('T') | Some('t') | Some('Y') | Some('y') => Some(true),
                Some('F') | Some('f') | Some('N') | Some('n') => Some(false),
                _ => None,
            };
            DbfValue::Logical(b)
        }
        'D' => DbfValue::Date(t.to_string()),
        // Character and any unknown type fall back to trimmed text.
        _ => DbfValue::Character(t.to_string()),
    }
}

// ── combined shapefile reader ──────────────────────────────────────────────────────

/// Read a shapefile from its `.shp` geometry bytes and (optional) `.dbf` attribute bytes into
/// [`ShapeRecord`]s (CONCEPT:EG-KG.domains.geo-formats). Geometry order is preserved 1:1 with the `.dbf` rows; if
/// `dbf` is `None` (or has fewer rows) the missing rows get empty attribute lists.
pub fn read_shapefile(shp: &[u8], dbf: Option<&[u8]>) -> Result<Vec<ShapeRecord>, String> {
    let geoms = read_shp(shp)?;
    let (fields, rows) = match dbf {
        Some(d) => read_dbf(d)?,
        None => (Vec::new(), Vec::new()),
    };
    let mut out = Vec::with_capacity(geoms.len());
    for (i, geometry) in geoms.into_iter().enumerate() {
        let attributes = match rows.get(i) {
            Some(cells) => fields
                .iter()
                .zip(cells.iter())
                .map(|(f, v)| (f.name.clone(), v.clone()))
                .collect(),
            None => Vec::new(),
        };
        out.push(ShapeRecord {
            geometry,
            attributes,
        });
    }
    Ok(out)
}

// FOLLOW-UP (B4, CONCEPT:EG-KG.domains.geo-formats): a shapefile *writer* (emit .shp/.shx/.dbf) is deferred — the
// reader is the high-value ingest path (every GIS tool still reads the WKB/GeoJSON we already
// emit). The writer would re-serialise geometries back into big-endian record headers +
// little-endian payloads and re-pack fixed-width dBASE rows; tracked as a follow-up.

#[cfg(test)]
mod tests {
    use super::*;

    // ── little hand-built .shp/.dbf fixture builders ────────────────────────────────

    /// Build a `.shp` main file from already-encoded record *content* blobs (each starting with
    /// the little-endian shape-type word), wrapping the 100-byte header + big-endian record
    /// headers around them.
    fn build_shp(contents: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Vec::new();
        for (i, content) in contents.iter().enumerate() {
            body.extend_from_slice(&((i as i32) + 1).to_be_bytes()); // record number (1-based)
            body.extend_from_slice(&((content.len() / 2) as i32).to_be_bytes()); // words
            body.extend_from_slice(content);
        }
        let total_words = (100 + body.len()) / 2;
        let mut out = Vec::new();
        out.extend_from_slice(&9994i32.to_be_bytes()); // file code
        out.extend_from_slice(&[0u8; 20]); // 5 unused i32
        out.extend_from_slice(&(total_words as i32).to_be_bytes()); // file length (words)
        out.extend_from_slice(&1000i32.to_le_bytes()); // version
        out.extend_from_slice(&SHP_POINT.to_le_bytes()); // shape type (nominal)
        out.extend_from_slice(&[0u8; 64]); // bbox (Xmin..Mmax) — unused by the reader
        out.extend_from_slice(&body);
        out
    }

    fn point_content(x: f64, y: f64) -> Vec<u8> {
        let mut c = Vec::new();
        c.extend_from_slice(&SHP_POINT.to_le_bytes());
        c.extend_from_slice(&x.to_le_bytes());
        c.extend_from_slice(&y.to_le_bytes());
        c
    }

    /// A Polygon record content from rings (first ring should be CW exterior, later ones CCW
    /// holes). Encodes bbox + NumParts + NumPoints + part-index array + flat points.
    fn polygon_content(rings: &[Vec<Point>]) -> Vec<u8> {
        let mut c = Vec::new();
        c.extend_from_slice(&SHP_POLYGON.to_le_bytes());
        // bbox from all points
        let all: Vec<Point> = rings.iter().flatten().copied().collect();
        let (mut minx, mut miny, mut maxx, mut maxy) = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
        for p in &all {
            minx = minx.min(p.x);
            miny = miny.min(p.y);
            maxx = maxx.max(p.x);
            maxy = maxy.max(p.y);
        }
        for v in [minx, miny, maxx, maxy] {
            c.extend_from_slice(&v.to_le_bytes());
        }
        c.extend_from_slice(&(rings.len() as i32).to_le_bytes()); // NumParts
        c.extend_from_slice(&(all.len() as i32).to_le_bytes()); // NumPoints
        let mut start = 0i32;
        for r in rings {
            c.extend_from_slice(&start.to_le_bytes());
            start += r.len() as i32;
        }
        for p in &all {
            c.extend_from_slice(&p.x.to_le_bytes());
            c.extend_from_slice(&p.y.to_le_bytes());
        }
        c
    }

    /// Build a minimal `.dbf` with `C`/`N` fields and rows of already-formatted cell strings.
    fn build_dbf(fields: &[(&str, char, usize)], rows: &[Vec<String>]) -> Vec<u8> {
        let header_len = 32 + fields.len() * 32 + 1;
        let record_len = 1 + fields.iter().map(|f| f.2).sum::<usize>();
        let mut out = Vec::new();
        out.push(0x03); // dBASE III
        out.extend_from_slice(&[26, 6, 2]); // last-update date (arbitrary)
        out.extend_from_slice(&(rows.len() as u32).to_le_bytes()); // num records
        out.extend_from_slice(&(header_len as u16).to_le_bytes());
        out.extend_from_slice(&(record_len as u16).to_le_bytes());
        out.extend_from_slice(&[0u8; 20]); // reserved
        for (name, ty, len) in fields {
            let mut nb = [0u8; 11];
            let bytes = name.as_bytes();
            nb[..bytes.len().min(11)].copy_from_slice(&bytes[..bytes.len().min(11)]);
            out.extend_from_slice(&nb);
            out.push(*ty as u8);
            out.extend_from_slice(&[0u8; 4]); // displacement
            out.push(*len as u8);
            out.push(0); // decimals
            out.extend_from_slice(&[0u8; 14]); // reserved
        }
        out.push(0x0D); // header terminator
        for row in rows {
            out.push(b' '); // not-deleted flag
            for ((_, _, len), cell) in fields.iter().zip(row.iter()) {
                let mut buf = vec![b' '; *len];
                let cb = cell.as_bytes();
                let n = cb.len().min(*len);
                // dBASE right-justifies numbers, left-justifies text; the reader trims either.
                buf[..n].copy_from_slice(&cb[..n]);
                out.extend_from_slice(&buf);
            }
        }
        out
    }

    #[test]
    fn eg306_shapefile_reads_points_with_dbf_attributes() {
        // Two Point records + a two-field .dbf (Character NAME, Numeric POP).
        let shp = build_shp(&[
            point_content(2.3522, 48.8566),
            point_content(-0.1278, 51.5074),
        ]);
        let dbf = build_dbf(
            &[("NAME", 'C', 10), ("POP", 'N', 8)],
            &[
                vec!["Paris".into(), "2161000".into()],
                vec!["London".into(), "8982000".into()],
            ],
        );
        let recs = read_shapefile(&shp, Some(&dbf)).expect("read shapefile");
        assert_eq!(recs.len(), 2);
        assert_eq!(
            recs[0].geometry,
            Some(Geometry::Point(Point::new(2.3522, 48.8566)))
        );
        assert_eq!(
            recs[0].attr("NAME"),
            Some(&DbfValue::Character("Paris".into()))
        );
        assert_eq!(
            recs[0].attr("POP"),
            Some(&DbfValue::Numeric(Some(2_161_000.0)))
        );
        assert_eq!(
            recs[1].geometry,
            Some(Geometry::Point(Point::new(-0.1278, 51.5074)))
        );
        assert_eq!(
            recs[1].attr("NAME"),
            Some(&DbfValue::Character("London".into()))
        );
    }

    #[test]
    fn eg306_shapefile_reads_polygon_with_hole() {
        // 10×10 CW exterior + 4×4 CCW hole → Geometry::Polygon with one interior ring.
        let exterior = vec![
            Point::new(0.0, 0.0),
            Point::new(0.0, 10.0),
            Point::new(10.0, 10.0),
            Point::new(10.0, 0.0),
            Point::new(0.0, 0.0),
        ]; // clockwise (signed area < 0)
        let hole = vec![
            Point::new(3.0, 3.0),
            Point::new(7.0, 3.0),
            Point::new(7.0, 7.0),
            Point::new(3.0, 7.0),
            Point::new(3.0, 3.0),
        ]; // counter-clockwise (signed area > 0)
        assert!(signed_area(&exterior) < 0.0);
        assert!(signed_area(&hole) > 0.0);
        let shp = build_shp(&[polygon_content(&[exterior.clone(), hole.clone()])]);
        let recs = read_shapefile(&shp, None).expect("read shapefile");
        assert_eq!(recs.len(), 1);
        match &recs[0].geometry {
            Some(Geometry::Polygon(pg)) => {
                assert_eq!(pg.exterior.points, exterior);
                assert_eq!(pg.interiors.len(), 1);
                assert_eq!(pg.interiors[0].points, hole);
                // The reconstructed polygon behaves hole-aware.
                assert!(pg.contains_point(&Point::new(1.0, 1.0)));
                assert!(!pg.contains_point(&Point::new(5.0, 5.0)));
            }
            other => panic!("expected Polygon, got {other:?}"),
        }
    }

    #[test]
    fn eg306_shapefile_multipolygon_two_exteriors() {
        // Two separate CW exteriors → MultiPolygon.
        let a = vec![
            Point::new(0.0, 0.0),
            Point::new(0.0, 1.0),
            Point::new(1.0, 1.0),
            Point::new(1.0, 0.0),
            Point::new(0.0, 0.0),
        ];
        let b = vec![
            Point::new(10.0, 10.0),
            Point::new(10.0, 12.0),
            Point::new(12.0, 12.0),
            Point::new(12.0, 10.0),
            Point::new(10.0, 10.0),
        ];
        let shp = build_shp(&[polygon_content(&[a, b])]);
        let recs = read_shapefile(&shp, None).unwrap();
        match &recs[0].geometry {
            Some(Geometry::MultiPolygon(ps)) => assert_eq!(ps.len(), 2),
            other => panic!("expected MultiPolygon, got {other:?}"),
        }
    }

    #[test]
    fn eg306_dbf_typed_fields_and_deleted_records() {
        // Logical + Date + a deleted row.
        let mut dbf = build_dbf(
            &[("FLAG", 'L', 1), ("WHEN", 'D', 8)],
            &[vec!["T".into(), "20260101".into()]],
        );
        // Append a second (deleted) record manually to exercise the '*' skip.
        let record_len = 1 + 1 + 8;
        let mut deleted = vec![b'*'];
        deleted.extend_from_slice(b"F");
        deleted.extend_from_slice(b"20260202");
        assert_eq!(deleted.len(), record_len);
        dbf.extend_from_slice(&deleted);
        // Bump the record count in the header (bytes 4..8) from 1 to 2.
        dbf[4..8].copy_from_slice(&2u32.to_le_bytes());

        let (fields, rows) = read_dbf(&dbf).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].ty, 'L');
        assert_eq!(rows.len(), 1, "deleted record must be skipped");
        assert_eq!(rows[0][0], DbfValue::Logical(Some(true)));
        assert_eq!(rows[0][1], DbfValue::Date("20260101".into()));
    }

    #[test]
    fn eg306_shx_index_offsets() {
        // Header + two 8-byte index records: (offset_words, length_words).
        let mut shx = vec![0u8; 100];
        shx[0..4].copy_from_slice(&9994i32.to_be_bytes());
        shx.extend_from_slice(&50i32.to_be_bytes()); // offset 50 words = 100 bytes
        shx.extend_from_slice(&10i32.to_be_bytes()); // length 10 words = 20 bytes
        shx.extend_from_slice(&64i32.to_be_bytes());
        shx.extend_from_slice(&10i32.to_be_bytes());
        let idx = read_shx(&shx).unwrap();
        assert_eq!(idx.records, vec![(100, 20), (128, 20)]);
    }

    #[test]
    fn eg306_shp_rejects_bad_file_code() {
        let mut bad = vec![0u8; 100];
        bad[0..4].copy_from_slice(&1234i32.to_be_bytes());
        assert!(read_shp(&bad).is_err());
        assert!(read_shp(&[]).is_err());
    }
}
