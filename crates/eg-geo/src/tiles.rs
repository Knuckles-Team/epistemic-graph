//! **Web-map tiling** — XYZ / TMS tile addressing + Mapbox Vector Tile encoding
//! (CONCEPT:EG-265).
//!
//! The GIS / logistics / urban-planning surface needs to *serve maps*. This module gives
//! the engine the two halves of a tile server, pure-Rust and dependency-free (the
//! Raspberry-Pi contract, though eg-geo itself rides the `geo` serving tiers, not `pi`):
//!
//! * **Tile addressing** — the slippy-map `z/x/y` grid over Web-Mercator (EPSG:3857).
//!   [`Tile`] converts between a tile index and its bounds ([`Tile::bounds`] in Mercator
//!   metres, [`Tile::bounds_lonlat`] in WGS84 degrees), [`lonlat_to_tile`] finds the tile
//!   covering a coordinate, and [`Tile::flip_y`] / [`Tile::to_tms`] / [`Tile::to_xyz`]
//!   convert between the **XYZ** convention (Google/OSM, `y = 0` at the north edge) and the
//!   **TMS** convention (OSGeo, `y = 0` at the south edge). Reuses EG-262's Web-Mercator
//!   forward/inverse ([`crate::crs`]) so the projection math lives in one place.
//!
//! * **Mapbox Vector Tile (MVT) encoding** — [`encode_mvt`] serialises a set of eg-geo
//!   [`Geometry`] features, clipped to a tile's extent, into the MVT 2.1 protobuf wire
//!   format at the standard 4096 [`DEFAULT_EXTENT`]. The protobuf framing (varints, field
//!   tags, packed geometry command/parameter integers) is **hand-rolled** — NO `prost`,
//!   NO protobuf-c, no build-script codegen. A matching [`decode_mvt`] parses the bytes
//!   back (used by the tests and handy for round-tripping), returning the decoded geometry
//!   command stream per feature.
//!
//! The whole module is additive and dependency-free; it does not touch the wire algebra or
//! any serving/protocol file.

use crate::crs::web_mercator_to_wgs84;
use crate::geodesic::WGS84_A;
use crate::geometry::{Bbox, Geometry, LineString, Point, Polygon};

/// Half the Web-Mercator world extent in metres (`π · a`, `a` = WGS84 semi-major axis).
/// The projected world spans `[-ORIGIN_SHIFT, +ORIGIN_SHIFT]` on both axes (EPSG:3857).
pub const ORIGIN_SHIFT: f64 = std::f64::consts::PI * WGS84_A;

/// The standard MVT tile extent (grid resolution) — 4096 units per tile edge (MVT §4.1).
pub const DEFAULT_EXTENT: u32 = 4096;

// ── tile addressing ──────────────────────────────────────────────────────────────────

/// A single map tile in the slippy-map `z/x/y` grid over Web-Mercator (CONCEPT:EG-265).
///
/// `x` runs west→east `0..2^z`. `y` runs **north→south** in the **XYZ** convention (the
/// default here; Google/OSM/Mapbox) and **south→north** in **TMS**. Use [`Tile::to_tms`]
/// / [`Tile::to_xyz`] (or [`Tile::flip_y`]) to convert; the two share the same `z`/`x`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Tile {
    pub z: u32,
    pub x: u32,
    pub y: u32,
}

impl Tile {
    /// A tile from its `z`/`x`/`y` index (XYZ convention).
    pub fn new(z: u32, x: u32, y: u32) -> Self {
        Self { z, x, y }
    }

    /// The number of tiles per axis at this zoom (`2^z`).
    pub fn tiles_per_axis(z: u32) -> u64 {
        1u64 << z
    }

    /// Flip the `y` index between the XYZ and TMS conventions (CONCEPT:EG-265):
    /// `y' = 2^z − 1 − y`. The transform is its own inverse.
    pub fn flip_y(&self) -> Tile {
        let n = Self::tiles_per_axis(self.z);
        Tile {
            z: self.z,
            x: self.x,
            y: (n - 1 - self.y as u64) as u32,
        }
    }

    /// This tile's `y` re-expressed in the **TMS** convention (CONCEPT:EG-265). Assumes
    /// `self` is XYZ; TMS `y` counts from the south edge.
    pub fn to_tms(&self) -> Tile {
        self.flip_y()
    }

    /// This tile's `y` re-expressed in the **XYZ** convention (CONCEPT:EG-265). Assumes
    /// `self` is TMS. Since the flip is an involution this is identical to [`Tile::to_tms`],
    /// but the two names document intent at the call site.
    pub fn to_xyz(&self) -> Tile {
        self.flip_y()
    }

    /// The tile's bounds as a Web-Mercator (EPSG:3857) [`Bbox`] in metres (CONCEPT:EG-265).
    /// XYZ convention: `y = 0` is the northern-most row (largest `maxy`).
    pub fn bounds(&self) -> Bbox {
        let n = Self::tiles_per_axis(self.z) as f64;
        let span = 2.0 * ORIGIN_SHIFT / n; // metres per tile edge
        let minx = -ORIGIN_SHIFT + self.x as f64 * span;
        let maxx = minx + span;
        // XYZ y grows southward, so row y starts just below the top (ORIGIN_SHIFT).
        let maxy = ORIGIN_SHIFT - self.y as f64 * span;
        let miny = maxy - span;
        Bbox::new(minx, miny, maxx, maxy)
    }

    /// The tile's bounds as a WGS84 lon/lat (EPSG:4326) [`Bbox`] in **degrees**
    /// (CONCEPT:EG-265). Computed by inverse-projecting the Mercator corners.
    pub fn bounds_lonlat(&self) -> Bbox {
        let m = self.bounds();
        let sw = web_mercator_to_wgs84(&Point::new(m.minx, m.miny));
        let ne = web_mercator_to_wgs84(&Point::new(m.maxx, m.maxy));
        Bbox::new(sw.x, sw.y, ne.x, ne.y)
    }
}

/// The XYZ tile index `(x, y)` covering `(lon, lat)` degrees at zoom `z` (CONCEPT:EG-265) —
/// the standard slippy-map formula. Longitude is wrapped/clamped into `0..2^z`; latitude is
/// clamped to the Web-Mercator limit (≈ ±85.0511°) so poles map to the edge rows.
pub fn lonlat_to_tile(lon: f64, lat: f64, z: u32) -> (u32, u32) {
    let n = Tile::tiles_per_axis(z) as f64;
    let xf = (lon + 180.0) / 360.0 * n;
    // Clamp latitude into the projectable band before taking the log.
    let lat = lat.clamp(-85.051_128_779_806_59, 85.051_128_779_806_59);
    let lat_rad = lat.to_radians();
    let yf = (1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / std::f64::consts::PI) / 2.0 * n;
    let max = (n as u64 - 1) as f64;
    let x = xf.floor().clamp(0.0, max) as u32;
    let y = yf.floor().clamp(0.0, max) as u32;
    (x, y)
}

// ── Mapbox Vector Tile (MVT) encoding ──────────────────────────────────────────────────

/// A tag value attached to an MVT feature (MVT §4.4 `Layer.Value`) — the subset of the
/// spec's oneof this encoder emits. Keys are deduplicated per layer.
#[derive(Clone, Debug, PartialEq)]
pub enum MvtValue {
    String(String),
    Float(f64),
    Int(i64),
    Bool(bool),
}

/// One feature to encode into an MVT layer (CONCEPT:EG-265): a stable `id`, an eg-geo
/// [`Geometry`] (in the SAME CRS as the tile bounds passed to [`encode_mvt`] — normally
/// Web-Mercator metres), and optional `properties`.
#[derive(Clone, Debug, PartialEq)]
pub struct MvtFeature {
    pub id: u64,
    pub geometry: Geometry,
    pub properties: Vec<(String, MvtValue)>,
}

impl MvtFeature {
    /// A feature with just an id and geometry (no properties).
    pub fn new(id: u64, geometry: Geometry) -> Self {
        Self {
            id,
            geometry,
            properties: Vec::new(),
        }
    }
}

/// One MVT layer (CONCEPT:EG-265): a name, an integer `extent` (grid resolution) and its
/// features. Use [`DEFAULT_EXTENT`] (4096) unless you have a reason not to.
#[derive(Clone, Debug, PartialEq)]
pub struct MvtLayer {
    pub name: String,
    pub extent: u32,
    pub features: Vec<MvtFeature>,
}

impl MvtLayer {
    /// A layer at the standard 4096 extent.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            extent: DEFAULT_EXTENT,
            features: Vec::new(),
        }
    }
}

/// MVT geometry-type enum values (MVT §4.3.4).
const GEOM_UNKNOWN: u32 = 0;
const GEOM_POINT: u32 = 1;
const GEOM_LINESTRING: u32 = 2;
const GEOM_POLYGON: u32 = 3;

/// MVT geometry command ids (MVT §4.3.5.2).
const CMD_MOVETO: u32 = 1;
const CMD_LINETO: u32 = 2;
const CMD_CLOSEPATH: u32 = 7;

/// Encode a set of layers into an MVT tile blob (CONCEPT:EG-265). Each feature's geometry
/// is clipped to `tile_bounds` and mapped into that layer's integer `extent` grid (origin
/// top-left, `y` down, per the MVT spec). `tile_bounds` is normally `tile.bounds()`
/// (Web-Mercator metres) and the feature geometries must be in the same CRS.
///
/// The output is the raw protobuf wire bytes — hand-encoded varints + field tags, no codegen.
pub fn encode_mvt(tile_bounds: &Bbox, layers: &[MvtLayer]) -> Vec<u8> {
    let mut out = Vec::new();
    for layer in layers {
        let body = encode_layer(tile_bounds, layer);
        // Tile.layers = field 3, wire type 2 (length-delimited embedded message).
        write_tag(&mut out, 3, 2);
        write_varint(&mut out, body.len() as u64);
        out.extend_from_slice(&body);
    }
    out
}

/// Encode one [`MvtLayer`] into its protobuf message bytes.
fn encode_layer(tile_bounds: &Bbox, layer: &MvtLayer) -> Vec<u8> {
    let extent = if layer.extent == 0 {
        DEFAULT_EXTENT
    } else {
        layer.extent
    };
    let mut keys: Vec<String> = Vec::new();
    let mut values: Vec<MvtValue> = Vec::new();
    let mut feature_bodies: Vec<Vec<u8>> = Vec::new();

    for f in &layer.features {
        let (geom_type, commands) = encode_geometry(&f.geometry, tile_bounds, extent);
        if commands.is_empty() || geom_type == GEOM_UNKNOWN {
            continue; // fully clipped away / unsupported
        }
        let mut fb = Vec::new();
        // Feature.id = field 1, varint.
        write_tag(&mut fb, 1, 0);
        write_varint(&mut fb, f.id);
        // Feature.tags = field 2, packed varint (repeated key_idx, value_idx).
        if !f.properties.is_empty() {
            let mut tags = Vec::new();
            for (k, v) in &f.properties {
                let ki = intern(&mut keys, k.clone());
                let vi = intern_value(&mut values, v.clone());
                write_varint(&mut tags, ki as u64);
                write_varint(&mut tags, vi as u64);
            }
            write_tag(&mut fb, 2, 2);
            write_varint(&mut fb, tags.len() as u64);
            fb.extend_from_slice(&tags);
        }
        // Feature.type = field 3, varint.
        write_tag(&mut fb, 3, 0);
        write_varint(&mut fb, geom_type as u64);
        // Feature.geometry = field 4, packed varint command stream.
        let mut geom = Vec::new();
        for c in &commands {
            write_varint(&mut geom, *c as u64);
        }
        write_tag(&mut fb, 4, 2);
        write_varint(&mut fb, geom.len() as u64);
        fb.extend_from_slice(&geom);

        feature_bodies.push(fb);
    }

    let mut out = Vec::new();
    // Layer.name = field 1, string.
    write_tag(&mut out, 1, 2);
    write_varint(&mut out, layer.name.len() as u64);
    out.extend_from_slice(layer.name.as_bytes());
    // Layer.features = field 2, embedded messages.
    for fb in &feature_bodies {
        write_tag(&mut out, 2, 2);
        write_varint(&mut out, fb.len() as u64);
        out.extend_from_slice(fb);
    }
    // Layer.keys = field 3, strings.
    for k in &keys {
        write_tag(&mut out, 3, 2);
        write_varint(&mut out, k.len() as u64);
        out.extend_from_slice(k.as_bytes());
    }
    // Layer.values = field 4, embedded Value messages.
    for v in &values {
        let vb = encode_value(v);
        write_tag(&mut out, 4, 2);
        write_varint(&mut out, vb.len() as u64);
        out.extend_from_slice(&vb);
    }
    // Layer.extent = field 5, varint.
    write_tag(&mut out, 5, 0);
    write_varint(&mut out, extent as u64);
    // Layer.version = field 15, varint (MVT 2.1).
    write_tag(&mut out, 15, 0);
    write_varint(&mut out, 2);
    out
}

/// Encode an MVT `Value` message (MVT §4.4). One field per variant.
fn encode_value(v: &MvtValue) -> Vec<u8> {
    let mut out = Vec::new();
    match v {
        MvtValue::String(s) => {
            write_tag(&mut out, 1, 2); // string_value
            write_varint(&mut out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        MvtValue::Float(f) => {
            write_tag(&mut out, 3, 0); // double_value (field 3, 64-bit) — use fixed64
                                       // double_value is field 3, wire type 1 (64-bit). Correct the tag:
            out.pop();
            out.push((3 << 3) | 1);
            out.extend_from_slice(&f.to_le_bytes());
        }
        MvtValue::Int(i) => {
            write_tag(&mut out, 4, 0); // int_value (varint)
            write_varint(&mut out, *i as u64);
        }
        MvtValue::Bool(b) => {
            write_tag(&mut out, 7, 0); // bool_value (varint)
            write_varint(&mut out, u64::from(*b));
        }
    }
    out
}

/// Encode a single geometry into an MVT command stream (CONCEPT:EG-265), clipping to
/// `bounds` and mapping into the `extent` grid. Returns `(geom_type, command_integers)`.
fn encode_geometry(g: &Geometry, bounds: &Bbox, extent: u32) -> (u32, Vec<u32>) {
    match g {
        Geometry::Point(p) => encode_points(std::slice::from_ref(p), bounds, extent),
        Geometry::MultiPoint(ps) => encode_points(ps, bounds, extent),
        Geometry::LineString(l) => encode_lines(std::slice::from_ref(l), bounds, extent),
        Geometry::MultiLineString(ls) => encode_lines(ls, bounds, extent),
        Geometry::Polygon(pg) => encode_polys(std::slice::from_ref(pg), bounds, extent),
        Geometry::MultiPolygon(pgs) => encode_polys(pgs, bounds, extent),
        // A collection encodes its first supported member's kind (MVT features are single-type).
        Geometry::GeometryCollection(gs) => {
            for g in gs {
                let (t, cmds) = encode_geometry(g, bounds, extent);
                if !cmds.is_empty() {
                    return (t, cmds);
                }
            }
            (GEOM_UNKNOWN, Vec::new())
        }
    }
}

/// Map a projected coordinate into the integer tile grid (origin top-left, y down).
fn to_grid(p: &Point, bounds: &Bbox, extent: u32) -> (i32, i32) {
    let e = extent as f64;
    let w = (bounds.maxx - bounds.minx).max(f64::MIN_POSITIVE);
    let h = (bounds.maxy - bounds.miny).max(f64::MIN_POSITIVE);
    let gx = ((p.x - bounds.minx) / w * e).round() as i32;
    let gy = ((bounds.maxy - p.y) / h * e).round() as i32; // flip y downward
    (gx, gy)
}

fn encode_points(ps: &[Point], bounds: &Bbox, extent: u32) -> (u32, Vec<u32>) {
    // Keep only points inside the tile bounds (point clipping = a membership test).
    let kept: Vec<(i32, i32)> = ps
        .iter()
        .filter(|p| bounds_contains(bounds, p))
        .map(|p| to_grid(p, bounds, extent))
        .collect();
    if kept.is_empty() {
        return (GEOM_UNKNOWN, Vec::new());
    }
    let mut cmds = Vec::new();
    let mut cursor = (0i32, 0i32);
    cmds.push(command(CMD_MOVETO, kept.len() as u32));
    for (gx, gy) in kept {
        cmds.push(zigzag(gx - cursor.0));
        cmds.push(zigzag(gy - cursor.1));
        cursor = (gx, gy);
    }
    (GEOM_POINT, cmds)
}

fn encode_lines(lines: &[LineString], bounds: &Bbox, extent: u32) -> (u32, Vec<u32>) {
    let mut cmds = Vec::new();
    let mut cursor = (0i32, 0i32);
    for l in lines {
        for part in clip_line(&l.points, bounds) {
            if part.len() < 2 {
                continue;
            }
            let grid: Vec<(i32, i32)> = part.iter().map(|p| to_grid(p, bounds, extent)).collect();
            emit_moveto(&mut cmds, &mut cursor, grid[0]);
            emit_lineto(&mut cmds, &mut cursor, &grid[1..]);
        }
    }
    if cmds.is_empty() {
        (GEOM_UNKNOWN, Vec::new())
    } else {
        (GEOM_LINESTRING, cmds)
    }
}

fn encode_polys(polys: &[Polygon], bounds: &Bbox, extent: u32) -> (u32, Vec<u32>) {
    let mut cmds = Vec::new();
    let mut cursor = (0i32, 0i32);
    for pg in polys {
        encode_ring(&pg.exterior.points, bounds, extent, &mut cmds, &mut cursor);
        for hole in &pg.interiors {
            encode_ring(&hole.points, bounds, extent, &mut cmds, &mut cursor);
        }
    }
    if cmds.is_empty() {
        (GEOM_UNKNOWN, Vec::new())
    } else {
        (GEOM_POLYGON, cmds)
    }
}

/// Encode one polygon ring: clip (Sutherland-Hodgman) to the tile, drop any closing
/// duplicate, then MoveTo + LineTo(k-1) + ClosePath.
fn encode_ring(
    ring: &[Point],
    bounds: &Bbox,
    extent: u32,
    cmds: &mut Vec<u32>,
    cursor: &mut (i32, i32),
) {
    let clipped = clip_polygon(ring, bounds);
    if clipped.len() < 3 {
        return;
    }
    let grid: Vec<(i32, i32)> = clipped.iter().map(|p| to_grid(p, bounds, extent)).collect();
    emit_moveto(cmds, cursor, grid[0]);
    emit_lineto(cmds, cursor, &grid[1..]);
    cmds.push(command(CMD_CLOSEPATH, 1));
}

fn emit_moveto(cmds: &mut Vec<u32>, cursor: &mut (i32, i32), to: (i32, i32)) {
    cmds.push(command(CMD_MOVETO, 1));
    cmds.push(zigzag(to.0 - cursor.0));
    cmds.push(zigzag(to.1 - cursor.1));
    *cursor = to;
}

fn emit_lineto(cmds: &mut Vec<u32>, cursor: &mut (i32, i32), pts: &[(i32, i32)]) {
    if pts.is_empty() {
        return;
    }
    cmds.push(command(CMD_LINETO, pts.len() as u32));
    for &(gx, gy) in pts {
        cmds.push(zigzag(gx - cursor.0));
        cmds.push(zigzag(gy - cursor.1));
        *cursor = (gx, gy);
    }
}

/// Pack an MVT `CommandInteger` = `(id & 0x7) | (count << 3)` (MVT §4.3.5.2).
fn command(id: u32, count: u32) -> u32 {
    (id & 0x7) | (count << 3)
}

/// Zig-zag encode a signed grid delta into an MVT `ParameterInteger` (MVT §4.3.5.3).
fn zigzag(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

/// Zig-zag decode (inverse of [`zigzag`]).
fn unzigzag(u: u32) -> i32 {
    ((u >> 1) as i32) ^ -((u & 1) as i32)
}

fn bounds_contains(b: &Bbox, p: &Point) -> bool {
    p.x >= b.minx && p.x <= b.maxx && p.y >= b.miny && p.y <= b.maxy
}

// ── clipping (dependency-free) ─────────────────────────────────────────────────────────

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
        if out == ca {
            a = Point::new(x, y);
            ca = outcode(&a, bx);
        } else {
            b = Point::new(x, y);
            cb = outcode(&b, bx);
        }
    }
}

/// Clip a polyline to `bounds`, yielding the surviving contiguous parts (CONCEPT:EG-265).
fn clip_line(pts: &[Point], bounds: &Bbox) -> Vec<Vec<Point>> {
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
fn clip_polygon(ring: &[Point], b: &Bbox) -> Vec<Point> {
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

// ── protobuf varint / tag primitives (hand-rolled) ─────────────────────────────────────

/// Append a protobuf field tag `(field << 3) | wire_type` as a varint.
fn write_tag(out: &mut Vec<u8>, field: u32, wire: u32) {
    write_varint(out, ((field << 3) | wire) as u64);
}

/// Append a base-128 LEB varint.
fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

/// Intern a key into the layer key table, returning its index.
fn intern(keys: &mut Vec<String>, k: String) -> usize {
    if let Some(i) = keys.iter().position(|e| *e == k) {
        i
    } else {
        keys.push(k);
        keys.len() - 1
    }
}

/// Intern a value into the layer value table, returning its index.
fn intern_value(values: &mut Vec<MvtValue>, v: MvtValue) -> usize {
    if let Some(i) = values.iter().position(|e| *e == v) {
        i
    } else {
        values.push(v);
        values.len() - 1
    }
}

// ── decoding (round-trip / inspection) ─────────────────────────────────────────────────

/// One geometry command decoded from an MVT feature (CONCEPT:EG-265): the command id
/// (`1` MoveTo, `2` LineTo, `7` ClosePath) plus the absolute grid points it moved the
/// cursor through (empty for ClosePath).
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedCommand {
    pub id: u32,
    pub points: Vec<(i32, i32)>,
}

/// A decoded MVT feature (CONCEPT:EG-265).
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedFeature {
    pub id: u64,
    pub geom_type: u32,
    pub commands: Vec<DecodedCommand>,
}

/// A decoded MVT layer (CONCEPT:EG-265).
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedLayer {
    pub name: String,
    pub extent: u32,
    pub version: u32,
    pub features: Vec<DecodedFeature>,
}

/// Parse an MVT tile blob back into layers/features/commands (CONCEPT:EG-265). The inverse
/// of [`encode_mvt`] for the geometry command stream — used to verify encodings and to
/// round-trip. Returns `Err` on a malformed/truncated blob.
pub fn decode_mvt(bytes: &[u8]) -> Result<Vec<DecodedLayer>, String> {
    let mut layers = Vec::new();
    let mut r = Reader::new(bytes);
    while !r.at_end() {
        let (field, wire) = r.read_tag()?;
        if field == 3 && wire == 2 {
            let body = r.read_len_delimited()?;
            layers.push(decode_layer(body)?);
        } else {
            r.skip(wire)?;
        }
    }
    Ok(layers)
}

fn decode_layer(bytes: &[u8]) -> Result<DecodedLayer, String> {
    let mut name = String::new();
    let mut extent = DEFAULT_EXTENT;
    let mut version = 1;
    let mut feats = Vec::new();
    let mut r = Reader::new(bytes);
    while !r.at_end() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, 2) => {
                name = String::from_utf8(r.read_len_delimited()?.to_vec())
                    .map_err(|e| format!("MVT: layer name not utf8: {e}"))?;
            }
            (2, 2) => feats.push(decode_feature(r.read_len_delimited()?)?),
            (5, 0) => extent = r.read_varint()? as u32,
            (15, 0) => version = r.read_varint()? as u32,
            _ => r.skip(wire)?,
        }
    }
    Ok(DecodedLayer {
        name,
        extent,
        version,
        features: feats,
    })
}

fn decode_feature(bytes: &[u8]) -> Result<DecodedFeature, String> {
    let mut id = 0u64;
    let mut geom_type = GEOM_UNKNOWN;
    let mut commands = Vec::new();
    let mut r = Reader::new(bytes);
    while !r.at_end() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, 0) => id = r.read_varint()?,
            (3, 0) => geom_type = r.read_varint()? as u32,
            (4, 2) => {
                let raw = r.read_len_delimited()?;
                commands = decode_commands(raw)?;
            }
            _ => r.skip(wire)?,
        }
    }
    Ok(DecodedFeature {
        id,
        geom_type,
        commands,
    })
}

fn decode_commands(bytes: &[u8]) -> Result<Vec<DecodedCommand>, String> {
    let mut out = Vec::new();
    let mut r = Reader::new(bytes);
    let mut cursor = (0i32, 0i32);
    while !r.at_end() {
        let cmd = r.read_varint()? as u32;
        let id = cmd & 0x7;
        let count = cmd >> 3;
        if id == CMD_CLOSEPATH {
            out.push(DecodedCommand {
                id,
                points: Vec::new(),
            });
            continue;
        }
        let mut pts = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let dx = unzigzag(r.read_varint()? as u32);
            let dy = unzigzag(r.read_varint()? as u32);
            cursor = (cursor.0 + dx, cursor.1 + dy);
            pts.push(cursor);
        }
        out.push(DecodedCommand { id, points: pts });
    }
    Ok(out)
}

/// A tiny protobuf byte cursor.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn at_end(&self) -> bool {
        self.pos >= self.buf.len()
    }
    fn read_varint(&mut self) -> Result<u64, String> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            if self.pos >= self.buf.len() {
                return Err("MVT: truncated varint".to_string());
            }
            let byte = self.buf[self.pos];
            self.pos += 1;
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 64 {
                return Err("MVT: varint too long".to_string());
            }
        }
        Ok(result)
    }
    fn read_tag(&mut self) -> Result<(u32, u32), String> {
        let t = self.read_varint()?;
        Ok(((t >> 3) as u32, (t & 0x7) as u32))
    }
    fn read_len_delimited(&mut self) -> Result<&'a [u8], String> {
        let len = self.read_varint()? as usize;
        if self.pos + len > self.buf.len() {
            return Err("MVT: truncated length-delimited field".to_string());
        }
        let s = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(s)
    }
    /// Skip a field of the given wire type (0 varint, 1 fixed64, 2 len-delimited, 5 fixed32).
    fn skip(&mut self, wire: u32) -> Result<(), String> {
        match wire {
            0 => {
                self.read_varint()?;
            }
            1 => self.advance(8)?,
            2 => {
                let len = self.read_varint()? as usize;
                self.advance(len)?;
            }
            5 => self.advance(4)?,
            other => return Err(format!("MVT: unknown wire type {other}")),
        }
        Ok(())
    }
    fn advance(&mut self, n: usize) -> Result<(), String> {
        if self.pos + n > self.buf.len() {
            return Err("MVT: truncated field".to_string());
        }
        self.pos += n;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64, what: &str) {
        assert!((a - b).abs() < tol, "{what}: {a} vs {b} (tol {tol})");
    }

    #[test]
    fn eg265_tile_bounds_lonlat_round_trip_with_lonlat_to_tile() {
        // The tile covering a coordinate must have bounds that enclose that coordinate,
        // and the tile index must round-trip through lonlat_to_tile.
        for z in [0u32, 1, 5, 12, 18] {
            let (lon, lat) = (2.3522, 48.8566); // Paris
            let (x, y) = lonlat_to_tile(lon, lat, z);
            let t = Tile::new(z, x, y);
            let b = t.bounds_lonlat();
            assert!(
                lon >= b.minx - 1e-9 && lon <= b.maxx + 1e-9,
                "z{z}: lon {lon} not in [{},{}]",
                b.minx,
                b.maxx
            );
            assert!(
                lat >= b.miny - 1e-9 && lat <= b.maxy + 1e-9,
                "z{z}: lat {lat} not in [{},{}]",
                b.miny,
                b.maxy
            );
            // Re-deriving the tile from the SW-ish interior point returns the same index.
            let (mx, my) = ((b.minx + b.maxx) / 2.0, (b.miny + b.maxy) / 2.0);
            assert_eq!(lonlat_to_tile(mx, my, z), (x, y), "z{z} centre re-tile");
        }
    }

    #[test]
    fn eg265_tile_bounds_mercator_matches_world_extent() {
        // z0 is the whole world; z1 quadrants tile the world with no gaps/overlap.
        let world = Tile::new(0, 0, 0).bounds();
        approx(world.minx, -ORIGIN_SHIFT, 1e-3, "z0 minx");
        approx(world.maxx, ORIGIN_SHIFT, 1e-3, "z0 maxx");
        approx(world.miny, -ORIGIN_SHIFT, 1e-3, "z0 miny");
        approx(world.maxy, ORIGIN_SHIFT, 1e-3, "z0 maxy");

        // Top-left z1 tile (0,0) is the NW quadrant.
        let nw = Tile::new(1, 0, 0).bounds();
        approx(nw.minx, -ORIGIN_SHIFT, 1e-3, "nw minx");
        approx(nw.maxx, 0.0, 1e-3, "nw maxx");
        approx(nw.maxy, ORIGIN_SHIFT, 1e-3, "nw maxy (north)");
        approx(nw.miny, 0.0, 1e-3, "nw miny");
    }

    #[test]
    fn eg265_xyz_tms_y_flip_is_involution() {
        // XYZ y=0 (north) ⇔ TMS y=2^z-1; the flip is its own inverse.
        let z = 4;
        let t = Tile::new(z, 3, 0); // north-most XYZ row
        let tms = t.to_tms();
        assert_eq!(tms.y, (1 << z) - 1, "north XYZ row → top TMS row");
        assert_eq!(tms.to_xyz(), t, "flip round-trips");
        // A north XYZ tile and its TMS twin describe the SAME geographic bounds.
        let a = t.bounds();
        let b = Tile::new(z, tms.x, tms.to_xyz().y).bounds();
        assert_eq!(a, b);
    }

    #[test]
    fn eg265_mvt_point_encodes_expected_commands() {
        // A single point at the tile centre → one MoveTo(1) to grid (2048, 2048).
        let t = Tile::new(12, 2048, 1362);
        let bounds = t.bounds();
        let (cx, cy) = bounds.center();
        let layer = MvtLayer {
            name: "pts".into(),
            extent: DEFAULT_EXTENT,
            features: vec![MvtFeature::new(42, Geometry::Point(Point::new(cx, cy)))],
        };
        let blob = encode_mvt(&bounds, &[layer]);
        let decoded = decode_mvt(&blob).expect("decode");
        assert_eq!(decoded.len(), 1);
        let l = &decoded[0];
        assert_eq!(l.name, "pts");
        assert_eq!(l.extent, 4096);
        assert_eq!(l.version, 2);
        assert_eq!(l.features.len(), 1);
        let f = &l.features[0];
        assert_eq!(f.id, 42);
        assert_eq!(f.geom_type, GEOM_POINT);
        assert_eq!(f.commands.len(), 1);
        assert_eq!(f.commands[0].id, CMD_MOVETO);
        // Centre maps to the middle of the 4096 grid.
        assert_eq!(f.commands[0].points, vec![(2048, 2048)]);
    }

    #[test]
    fn eg265_mvt_linestring_commands_moveto_then_lineto() {
        let t = Tile::new(10, 0, 0);
        let b = t.bounds();
        // A short line well inside the tile: from 1/4 to 3/4 across.
        let p0 = Point::new(
            b.minx + (b.maxx - b.minx) * 0.25,
            b.miny + (b.maxy - b.miny) * 0.5,
        );
        let p1 = Point::new(
            b.minx + (b.maxx - b.minx) * 0.5,
            b.miny + (b.maxy - b.miny) * 0.5,
        );
        let p2 = Point::new(
            b.minx + (b.maxx - b.minx) * 0.75,
            b.miny + (b.maxy - b.miny) * 0.5,
        );
        let line = Geometry::LineString(LineString::new(vec![p0, p1, p2]));
        let layer = MvtLayer {
            name: "roads".into(),
            extent: DEFAULT_EXTENT,
            features: vec![MvtFeature::new(7, line)],
        };
        let blob = encode_mvt(&b, &[layer]);
        let f = &decode_mvt(&blob).unwrap()[0].features[0];
        assert_eq!(f.geom_type, GEOM_LINESTRING);
        assert_eq!(f.commands.len(), 2, "one MoveTo + one LineTo");
        assert_eq!(f.commands[0].id, CMD_MOVETO);
        assert_eq!(f.commands[0].points, vec![(1024, 2048)]);
        assert_eq!(f.commands[1].id, CMD_LINETO);
        assert_eq!(f.commands[1].points, vec![(2048, 2048), (3072, 2048)]);
    }

    #[test]
    fn eg265_mvt_polygon_closepath_and_properties() {
        let t = Tile::new(8, 0, 0);
        let b = t.bounds();
        let f = |fx: f64, fy: f64| {
            Point::new(
                b.minx + (b.maxx - b.minx) * fx,
                b.maxy - (b.maxy - b.miny) * fy, // fy=0 → top
            )
        };
        // A square in the upper-left quadrant (closed ring).
        let ring = LineString::new(vec![
            f(0.25, 0.25),
            f(0.5, 0.25),
            f(0.5, 0.5),
            f(0.25, 0.5),
            f(0.25, 0.25),
        ]);
        let mut feat = MvtFeature::new(3, Geometry::Polygon(Polygon::new(ring)));
        feat.properties
            .push(("kind".into(), MvtValue::String("park".into())));
        feat.properties.push(("area".into(), MvtValue::Int(1200)));
        let layer = MvtLayer {
            name: "landuse".into(),
            extent: DEFAULT_EXTENT,
            features: vec![feat],
        };
        let blob = encode_mvt(&b, &[layer]);
        let f = &decode_mvt(&blob).unwrap()[0].features[0];
        assert_eq!(f.geom_type, GEOM_POLYGON);
        // MoveTo(1), LineTo(3), ClosePath.
        assert_eq!(f.commands.len(), 3);
        assert_eq!(f.commands[0].id, CMD_MOVETO);
        assert_eq!(f.commands[0].points.len(), 1);
        assert_eq!(f.commands[1].id, CMD_LINETO);
        assert_eq!(f.commands[1].points.len(), 3);
        assert_eq!(f.commands[2].id, CMD_CLOSEPATH);
        assert!(f.commands[2].points.is_empty());
    }

    #[test]
    fn eg265_mvt_clips_geometry_outside_tile() {
        // A point outside the tile bounds is dropped (clipped) → no feature emitted.
        let t = Tile::new(10, 0, 0);
        let b = t.bounds();
        let outside = Point::new(b.maxx + 1_000_000.0, b.maxy + 1_000_000.0);
        let layer = MvtLayer {
            name: "pts".into(),
            extent: DEFAULT_EXTENT,
            features: vec![MvtFeature::new(1, Geometry::Point(outside))],
        };
        let blob = encode_mvt(&b, &[layer]);
        let decoded = decode_mvt(&blob).unwrap();
        assert_eq!(decoded[0].features.len(), 0, "outside point clipped away");
    }

    #[test]
    fn eg265_zigzag_round_trips() {
        for n in [-2048i32, -1, 0, 1, 2047, 100000, -100000] {
            assert_eq!(unzigzag(zigzag(n)), n, "zigzag {n}");
        }
    }
}
