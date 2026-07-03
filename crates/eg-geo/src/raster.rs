//! **Raster tile pyramids** — a georeferenced coverage grid resampled into a
//! slippy-map XYZ/TMS tile pyramid (CONCEPT:EG-338 build, CONCEPT:EG-339 fetch).
//!
//! EG-265 gave the engine the *vector* half of a tile server ([`crate::tiles`]:
//! XYZ/TMS addressing + hand-rolled Mapbox Vector Tiles). This module adds the
//! **raster** half: take engine-resident coverage data (a [`Raster`] — a bbox +
//! `width`×`height` grid of `bands` bytes, e.g. an ingested GeoTIFF/GeoParquet
//! coverage decoded to bands) and produce map tiles by resampling per XYZ tile:
//!
//! * [`Raster`] — the minimal georeferenced grid: a Web-Mercator (EPSG:3857) [`Bbox`]
//!   plus a pixel-interleaved `data` buffer (`width * height * bands` bytes, row 0 =
//!   north/top). serde-serialisable so a coverage persists as a typed value in the
//!   engine's redb per-graph store, exactly like a [`crate::Geometry`].
//! * [`Raster::tile`] (CONCEPT:EG-339) — fetch a single `z/x/y` tile: a
//!   [`TILE_SIZE`]×[`TILE_SIZE`] [`RasterTile`] nearest-neighbour-resampled from the
//!   source over the tile's Web-Mercator bounds ([`crate::tiles::Tile::bounds`]).
//!   Correctly downsamples as `z` decreases and upsamples as it increases; source
//!   pixels outside the coverage read as the `nodata` value (transparent for RGBA).
//! * [`Raster::build_pyramid`] (CONCEPT:EG-338) — the batch op: for every zoom
//!   `z_min..=z_max`, emit each XYZ tile that intersects the coverage. Returns a
//!   [`Pyramid`] (an ordered `(Tile, RasterTile)` list) with per-zoom tile counts.
//! * [`RasterTile::to_png`] / [`decode_png`] — a **hand-rolled, dependency-free**
//!   PNG codec (8-bit grayscale / GA / RGB / RGBA), using *stored* (uncompressed)
//!   DEFLATE blocks + hand-computed CRC-32 / Adler-32. NO `image`, NO `png`, NO `flate2`
//!   — the same "no codegen, no C" ethos as EG-265's hand-rolled MVT protobuf, so the
//!   module adds **zero new dependencies** and stays trivially inside the Pi contract
//!   (eg-geo is already out of the `pi` tier). Raw band tiles are available directly
//!   as [`RasterTile::data`] for callers that don't want PNG.
//!
//! The whole module is additive and dependency-free; it does not touch the wire algebra
//! or any serving/protocol file. Like [`crate::tiles::encode_mvt`], it is a pure eg-geo
//! library capability the `geo`-tier executor / a geo serving surface can call.

use crate::geometry::Bbox;
use crate::tiles::{Tile, ORIGIN_SHIFT};
use serde::{Deserialize, Serialize};

/// The edge length (pixels) of a rendered raster tile — the slippy-map standard 256×256.
pub const TILE_SIZE: u32 = 256;

/// A minimal georeferenced coverage grid (CONCEPT:EG-338): a Web-Mercator (EPSG:3857)
/// [`Bbox`] over a `width`×`height` pixel grid with `bands` 8-bit bands per pixel.
///
/// `data` is **pixel-interleaved, row-major**: the byte for pixel `(col, row)` band `b`
/// is `data[(row * width + col) * bands + b]`. **Row 0 is the northern-most row** (largest
/// `maxy`) — the image convention, matching XYZ tiles. `bands` is 1 (grayscale), 2
/// (grayscale+alpha), 3 (RGB) or 4 (RGBA); other counts still resample but only PNG-encode
/// for those four.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Raster {
    /// Georeferenced extent in Web-Mercator (EPSG:3857) metres.
    pub bbox: Bbox,
    /// Columns (west→east).
    pub width: u32,
    /// Rows (north→south; row 0 = north).
    pub height: u32,
    /// Bands per pixel.
    pub bands: u32,
    /// Pixel-interleaved, row-major band bytes (`width * height * bands` long).
    pub data: Vec<u8>,
}

/// Resampling options for [`Raster::tile`] / [`Raster::build_pyramid`] (CONCEPT:EG-339).
/// The default `nodata` is `0` — transparent / zero fill outside the coverage.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResampleOptions {
    /// The band byte written where a tile pixel falls outside the source coverage.
    /// For RGBA this is normally `0` (fully transparent).
    pub nodata: u8,
}

/// A single rendered raster tile (CONCEPT:EG-339): a [`TILE_SIZE`]×[`TILE_SIZE`] pixel
/// grid with the same band count as its source [`Raster`], pixel-interleaved row-major
/// (row 0 = north, matching the XYZ tile). PNG-encode with [`RasterTile::to_png`] or read
/// the raw bands from [`RasterTile::data`].
#[derive(Clone, Debug, PartialEq)]
pub struct RasterTile {
    pub tile: Tile,
    pub size: u32,
    pub bands: u32,
    pub data: Vec<u8>,
}

impl RasterTile {
    /// The band byte for tile pixel `(col, row)` band `b` (row 0 = north).
    #[inline]
    pub fn pixel(&self, col: u32, row: u32, b: u32) -> u8 {
        let idx = ((row * self.size + col) * self.bands + b) as usize;
        self.data[idx]
    }
}

/// An ordered raster tile pyramid over `z_min..=z_max` (CONCEPT:EG-338), plus the number
/// of tiles emitted at each zoom (`counts[i]` is for zoom `z_min + i`).
#[derive(Clone, Debug, PartialEq)]
pub struct Pyramid {
    pub z_min: u32,
    pub z_max: u32,
    /// Every emitted tile, grouped by ascending zoom then row-major within a zoom.
    pub tiles: Vec<(Tile, RasterTile)>,
    /// Tile count per zoom level (`counts[z - z_min]`).
    pub counts: Vec<usize>,
}

// ── tile addressing over the coverage ──────────────────────────────────────────────────

/// Web-Mercator span (metres) of one tile edge at zoom `z`.
#[inline]
fn tile_span(z: u32) -> f64 {
    2.0 * ORIGIN_SHIFT / (Tile::tiles_per_axis(z) as f64)
}

/// Clamp a tile index into `0..2^z`.
#[inline]
fn clamp_idx(v: f64, z: u32) -> u32 {
    let max = (Tile::tiles_per_axis(z) - 1) as f64;
    v.floor().clamp(0.0, max) as u32
}

impl Raster {
    /// The inclusive XYZ tile-index range `(tx_min, tx_max, ty_min, ty_max)` whose tiles
    /// intersect this coverage at zoom `z` (CONCEPT:EG-338). The upper (east / south)
    /// edges are treated as *exclusive* by an epsilon shrink, so a coverage whose extent
    /// lands exactly on a tile boundary does NOT spuriously spill into the next tile.
    pub fn tile_range(&self, z: u32) -> (u32, u32, u32, u32) {
        let span = tile_span(z);
        let eps = span * 1e-9;
        // x grows eastward from -ORIGIN_SHIFT.
        let tx_of = |x: f64| (x + ORIGIN_SHIFT) / span;
        // XYZ y grows southward from +ORIGIN_SHIFT (north).
        let ty_of = |y: f64| (ORIGIN_SHIFT - y) / span;
        let tx_min = clamp_idx(tx_of(self.bbox.minx), z);
        let tx_max = clamp_idx(tx_of(self.bbox.maxx - eps), z);
        let ty_min = clamp_idx(ty_of(self.bbox.maxy - eps), z); // north edge → smallest ty
        let ty_max = clamp_idx(ty_of(self.bbox.miny + eps), z); // south edge → largest ty
        (tx_min, tx_max, ty_min, ty_max)
    }

    /// Nearest-neighbour sample of band `b` at a Web-Mercator coordinate, or `nodata` when
    /// the point is outside the coverage.
    #[inline]
    fn sample(&self, x: f64, y: f64, b: u32, nodata: u8) -> u8 {
        if x < self.bbox.minx || x >= self.bbox.maxx || y <= self.bbox.miny || y > self.bbox.maxy {
            return nodata;
        }
        let fx = (x - self.bbox.minx) / (self.bbox.maxx - self.bbox.minx);
        // Row 0 is north (maxy); y decreases southward.
        let fy = (self.bbox.maxy - y) / (self.bbox.maxy - self.bbox.miny);
        let col = ((fx * self.width as f64).floor() as i64).clamp(0, self.width as i64 - 1) as u32;
        let row =
            ((fy * self.height as f64).floor() as i64).clamp(0, self.height as i64 - 1) as u32;
        let idx = ((row * self.width + col) * self.bands + b) as usize;
        self.data[idx]
    }

    /// Fetch a single `z/x/y` tile, nearest-neighbour-resampled from the coverage over the
    /// tile's Web-Mercator bounds (CONCEPT:EG-339). The result is [`TILE_SIZE`]×[`TILE_SIZE`]
    /// with the source band count; pixels outside the coverage take `opts.nodata`.
    pub fn tile(&self, z: u32, x: u32, y: u32, opts: &ResampleOptions) -> RasterTile {
        let tile = Tile::new(z, x, y);
        let b = tile.bounds();
        let size = TILE_SIZE;
        let bands = self.bands;
        let mut data = vec![opts.nodata; (size * size * bands) as usize];
        let sx = (b.maxx - b.minx) / size as f64;
        let sy = (b.maxy - b.miny) / size as f64;
        for row in 0..size {
            // Pixel-centre Mercator Y; row 0 = north (maxy).
            let wy = b.maxy - (row as f64 + 0.5) * sy;
            for col in 0..size {
                let wx = b.minx + (col as f64 + 0.5) * sx;
                let base = ((row * size + col) * bands) as usize;
                for band in 0..bands {
                    data[base + band as usize] = self.sample(wx, wy, band, opts.nodata);
                }
            }
        }
        RasterTile {
            tile,
            size,
            bands,
            data,
        }
    }

    /// Build the full tile pyramid over `z_min..=z_max` (CONCEPT:EG-338): every XYZ tile
    /// intersecting the coverage at each zoom, resampled via [`Raster::tile`]. Panics if
    /// `z_min > z_max`.
    pub fn build_pyramid(&self, z_min: u32, z_max: u32, opts: &ResampleOptions) -> Pyramid {
        assert!(z_min <= z_max, "z_min must be <= z_max");
        let mut tiles = Vec::new();
        let mut counts = Vec::with_capacity((z_max - z_min + 1) as usize);
        for z in z_min..=z_max {
            let (tx0, tx1, ty0, ty1) = self.tile_range(z);
            let mut n = 0usize;
            for ty in ty0..=ty1 {
                for tx in tx0..=tx1 {
                    let t = Tile::new(z, tx, ty);
                    tiles.push((t, self.tile(z, tx, ty, opts)));
                    n += 1;
                }
            }
            counts.push(n);
        }
        Pyramid {
            z_min,
            z_max,
            tiles,
            counts,
        }
    }
}

// ── hand-rolled, dependency-free PNG codec ─────────────────────────────────────────────
//
// Mirrors EG-265's hand-rolled MVT protobuf: no `image`/`png`/`flate2`, no codegen, no C.
// We emit a valid PNG using *stored* (BTYPE=00, uncompressed) DEFLATE blocks wrapped in a
// zlib stream, with hand-computed CRC-32 (chunk) and Adler-32 (zlib). This keeps eg-geo's
// dependency set unchanged (serde only) so the crate stays trivially Pi-safe.

const PNG_SIG: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];

/// PNG colour type for a given band count (`None` if unsupported for encoding).
fn png_color_type(bands: u32) -> Option<u8> {
    match bands {
        1 => Some(0), // grayscale
        2 => Some(4), // grayscale + alpha
        3 => Some(2), // RGB
        4 => Some(6), // RGBA
        _ => None,
    }
}

fn bands_of_color_type(ct: u8) -> Option<u32> {
    match ct {
        0 => Some(1),
        4 => Some(2),
        2 => Some(3),
        6 => Some(4),
        _ => None,
    }
}

/// Standard CRC-32 (reflected, poly 0xEDB88320) used by PNG chunks.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in bytes {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Adler-32 checksum used by the zlib wrapper.
fn adler32(bytes: &[u8]) -> u32 {
    const MOD: u32 = 65521;
    let (mut a, mut b) = (1u32, 0u32);
    for &byte in bytes {
        a = (a + byte as u32) % MOD;
        b = (b + a) % MOD;
    }
    (b << 16) | a
}

/// Wrap raw bytes in a zlib stream of *stored* DEFLATE blocks (no compression).
fn zlib_store(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() + raw.len() / 65535 * 5 + 8);
    out.push(0x78); // CMF: deflate, 32K window
    out.push(0x01); // FLG: (0x7801 % 31 == 0)
    let mut i = 0usize;
    if raw.is_empty() {
        // A single empty final stored block.
        out.push(0x01);
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&(!0u16).to_le_bytes());
    }
    while i < raw.len() {
        let len = (raw.len() - i).min(0xFFFF);
        let final_block = i + len >= raw.len();
        out.push(if final_block { 0x01 } else { 0x00 });
        out.extend_from_slice(&(len as u16).to_le_bytes());
        out.extend_from_slice(&(!(len as u16)).to_le_bytes());
        out.extend_from_slice(&raw[i..i + len]);
        i += len;
    }
    out.extend_from_slice(&adler32(raw).to_be_bytes());
    out
}

/// Append a PNG chunk (`len | type | data | crc`) to `out`.
fn png_chunk(out: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(data);
    let mut crc_in = Vec::with_capacity(4 + data.len());
    crc_in.extend_from_slice(kind);
    crc_in.extend_from_slice(data);
    out.extend_from_slice(&crc32(&crc_in).to_be_bytes());
}

impl RasterTile {
    /// Encode this tile as a PNG (CONCEPT:EG-339) — hand-rolled, dependency-free. Supports
    /// 1/2/3/4 bands (grayscale / GA / RGB / RGBA). Returns `None` for other band counts
    /// (read [`RasterTile::data`] for raw band tiles instead).
    pub fn to_png(&self) -> Option<Vec<u8>> {
        encode_png(self.size, self.size, self.bands, &self.data)
    }
}

/// Encode a pixel-interleaved, row-major 8-bit image as PNG bytes (CONCEPT:EG-339).
/// `data` must be `width * height * bands` long. Uses filter type 0 (None) on every row.
pub fn encode_png(width: u32, height: u32, bands: u32, data: &[u8]) -> Option<Vec<u8>> {
    let color_type = png_color_type(bands)?;
    if data.len() != (width * height * bands) as usize {
        return None;
    }
    let mut out = Vec::new();
    out.extend_from_slice(&PNG_SIG);

    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.push(8); // bit depth
    ihdr.push(color_type);
    ihdr.push(0); // compression
    ihdr.push(0); // filter
    ihdr.push(0); // interlace
    png_chunk(&mut out, b"IHDR", &ihdr);

    // Raw scanlines: each row prefixed with filter byte 0 (None).
    let stride = (width * bands) as usize;
    let mut raw = Vec::with_capacity((height as usize) * (stride + 1));
    for row in 0..height as usize {
        raw.push(0);
        let off = row * stride;
        raw.extend_from_slice(&data[off..off + stride]);
    }
    png_chunk(&mut out, b"IDAT", &zlib_store(&raw));
    png_chunk(&mut out, b"IEND", &[]);
    Some(out)
}

/// Decode a PNG produced by [`encode_png`] (CONCEPT:EG-339) back into
/// `(width, height, bands, pixel-interleaved data)`. Deliberately supports only the subset
/// this module emits: 8-bit depth, non-interlaced, filter-0 scanlines, and zlib *stored*
/// DEFLATE blocks. Returns `None` on any deviation. Mirrors EG-265's `decode_mvt` — a
/// genuine round-trip check for the tests, and handy for consumers.
pub fn decode_png(bytes: &[u8]) -> Option<(u32, u32, u32, Vec<u8>)> {
    if bytes.len() < 8 || bytes[..8] != PNG_SIG {
        return None;
    }
    let mut pos = 8usize;
    let mut width = 0u32;
    let mut height = 0u32;
    let mut bands = 0u32;
    let mut idat = Vec::new();
    while pos + 8 <= bytes.len() {
        let len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().ok()?) as usize;
        let kind = &bytes[pos + 4..pos + 8];
        let data_start = pos + 8;
        let data_end = data_start.checked_add(len)?;
        if data_end + 4 > bytes.len() {
            return None;
        }
        let data = &bytes[data_start..data_end];
        match kind {
            b"IHDR" => {
                if len != 13 || data[8] != 8 || data[12] != 0 {
                    return None; // 8-bit, non-interlaced only
                }
                width = u32::from_be_bytes(data[0..4].try_into().ok()?);
                height = u32::from_be_bytes(data[4..8].try_into().ok()?);
                bands = bands_of_color_type(data[9])?;
            }
            b"IDAT" => idat.extend_from_slice(data),
            b"IEND" => break,
            _ => {}
        }
        pos = data_end + 4; // skip CRC
    }
    if width == 0 || height == 0 || bands == 0 {
        return None;
    }
    let raw = zlib_inflate_stored(&idat)?;
    // Strip the per-row filter byte (must be 0 = None).
    let stride = (width * bands) as usize;
    if raw.len() != (height as usize) * (stride + 1) {
        return None;
    }
    let mut out = Vec::with_capacity((width * height * bands) as usize);
    for row in 0..height as usize {
        let off = row * (stride + 1);
        if raw[off] != 0 {
            return None; // only filter 0
        }
        out.extend_from_slice(&raw[off + 1..off + 1 + stride]);
    }
    Some((width, height, bands, out))
}

/// Inflate a zlib stream that uses only *stored* DEFLATE blocks (the subset [`zlib_store`]
/// emits). Verifies the trailing Adler-32.
fn zlib_inflate_stored(z: &[u8]) -> Option<Vec<u8>> {
    if z.len() < 6 {
        return None;
    }
    // Skip the 2-byte zlib header; the last 4 bytes are the Adler-32.
    let body = &z[2..z.len() - 4];
    let expect = u32::from_be_bytes(z[z.len() - 4..].try_into().ok()?);
    let mut out = Vec::new();
    let mut i = 0usize;
    loop {
        if i >= body.len() {
            return None;
        }
        let header = body[i];
        let bfinal = header & 1;
        let btype = (header >> 1) & 0b11;
        if btype != 0 {
            return None; // stored blocks only
        }
        i += 1;
        if i + 4 > body.len() {
            return None;
        }
        let len = u16::from_le_bytes(body[i..i + 2].try_into().ok()?) as usize;
        let nlen = u16::from_le_bytes(body[i + 2..i + 4].try_into().ok()?);
        if nlen != !(len as u16) {
            return None;
        }
        i += 4;
        if i + len > body.len() {
            return None;
        }
        out.extend_from_slice(&body[i..i + len]);
        i += len;
        if bfinal == 1 {
            break;
        }
    }
    if adler32(&out) != expect {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic 256×256 RGBA coverage aligned to XYZ tile (z=1, x=0, y=0) — the NW
    /// world quadrant. Fill: opaque red everywhere, except the NW (top-left) source pixel
    /// (col=0, row=0) which is opaque green, so the round-trip also proves orientation.
    fn synthetic() -> Raster {
        let bbox = Tile::new(1, 0, 0).bounds();
        let (w, h, bands) = (256u32, 256u32, 4u32);
        let mut data = vec![0u8; (w * h * bands) as usize];
        for row in 0..h {
            for col in 0..w {
                let base = ((row * w + col) * bands) as usize;
                if row == 0 && col == 0 {
                    data[base..base + 4].copy_from_slice(&[0, 255, 0, 255]); // green NW pixel
                } else {
                    data[base..base + 4].copy_from_slice(&[255, 0, 0, 255]); // red
                }
            }
        }
        Raster {
            bbox,
            width: w,
            height: h,
            bands,
            data,
        }
    }

    #[test]
    fn tile_counts_per_zoom_quadruple() {
        // Coverage == one z=1 tile. z=1 → 1 tile, z=2 → 2×2, z=3 → 4×4.
        let r = synthetic();
        let opts = ResampleOptions::default();
        let pyr = r.build_pyramid(1, 3, &opts);
        assert_eq!(pyr.counts, vec![1, 4, 16]);
        assert_eq!(pyr.tiles.len(), 1 + 4 + 16);
        // Every emitted tile is TILE_SIZE² with the source band count.
        for (_, t) in &pyr.tiles {
            assert_eq!(t.size, TILE_SIZE);
            assert_eq!(t.bands, 4);
            assert_eq!(t.data.len(), (TILE_SIZE * TILE_SIZE * 4) as usize);
        }
    }

    #[test]
    fn tile_range_edges_do_not_spill() {
        let r = synthetic();
        assert_eq!(r.tile_range(1), (0, 0, 0, 0));
        assert_eq!(r.tile_range(2), (0, 1, 0, 1));
        assert_eq!(r.tile_range(3), (0, 3, 0, 3));
    }

    #[test]
    fn z1_tile_matches_source_pixels() {
        // At z=1 the tile extent equals the coverage extent and both are 256px, so the
        // nearest-neighbour map is identity: NW pixel green, interior red.
        let r = synthetic();
        let t = r.tile(1, 0, 0, &ResampleOptions::default());
        assert_eq!(
            [
                t.pixel(0, 0, 0),
                t.pixel(0, 0, 1),
                t.pixel(0, 0, 2),
                t.pixel(0, 0, 3)
            ],
            [0, 255, 0, 255],
            "NW tile pixel should be the green source corner (orientation preserved)"
        );
        assert_eq!(
            [
                t.pixel(128, 128, 0),
                t.pixel(128, 128, 1),
                t.pixel(128, 128, 2),
                t.pixel(128, 128, 3)
            ],
            [255, 0, 0, 255],
            "interior tile pixel should be red"
        );
    }

    #[test]
    fn downsample_and_nodata() {
        // z=2 subtile (0,0) covers the NW quarter of the coverage → contains the green
        // corner at its own NW pixel. A tile OUTSIDE the coverage is all nodata.
        let r = synthetic();
        let opts = ResampleOptions::default();
        let sub = r.tile(2, 0, 0, &opts);
        assert_eq!(sub.pixel(0, 0, 1), 255); // green channel of NW corner
        assert_eq!(sub.pixel(0, 0, 3), 255); // opaque

        // Tile (1,1,1) is the SE world quadrant — disjoint from the NW coverage.
        let outside = r.tile(1, 1, 1, &opts);
        assert!(
            outside.data.iter().all(|&b| b == 0),
            "a tile outside the coverage must be all nodata (transparent)"
        );
    }

    #[test]
    fn png_round_trip() {
        let r = synthetic();
        let t = r.tile(1, 0, 0, &ResampleOptions::default());
        let png = t.to_png().expect("RGBA encodes");
        assert_eq!(&png[..8], &PNG_SIG, "valid PNG signature");
        let (w, h, bands, data) = decode_png(&png).expect("round-trips");
        assert_eq!((w, h, bands), (TILE_SIZE, TILE_SIZE, 4));
        assert_eq!(data, t.data, "decoded pixels equal the tile pixels");
        // Known band value survives the PNG round-trip (NW pixel = index 0).
        assert_eq!(&data[0..4], &[0, 255, 0, 255]);
    }

    #[test]
    fn png_round_trip_grayscale_large() {
        // Exercises multi-block stored DEFLATE (>65535 raw bytes) on a 1-band tile.
        let bbox = Tile::new(0, 0, 0).bounds();
        let (w, h) = (256u32, 256u32);
        let mut data = vec![0u8; (w * h) as usize];
        for (i, px) in data.iter_mut().enumerate() {
            *px = (i % 251) as u8;
        }
        let r = Raster {
            bbox,
            width: w,
            height: h,
            bands: 1,
            data,
        };
        let t = r.tile(0, 0, 0, &ResampleOptions::default());
        let png = t.to_png().expect("grayscale encodes");
        let (dw, dh, db, dd) = decode_png(&png).expect("round-trips");
        assert_eq!((dw, dh, db), (TILE_SIZE, TILE_SIZE, 1));
        assert_eq!(dd, t.data);
        assert!(t.data.len() > 65535, "forces >1 stored DEFLATE block");
    }

    #[test]
    fn raster_serde_round_trip() {
        // A Raster persists as a typed value (redb per-graph store), like a Geometry.
        let r = synthetic();
        let json = serde_json::to_string(&r).unwrap();
        let back: Raster = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }
}
