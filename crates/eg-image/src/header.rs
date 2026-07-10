//! Dependency-free header parsing: real `width`/`height` read straight out of a PNG
//! `IHDR` chunk or a JPEG `SOFn` marker — NOT a pixel decode. Mirrors `eg-geo::raster`'s
//! hand-rolled PNG chunk walk (same idiom, byte-for-byte the same PNG signature/chunk
//! framing), scoped down to "read the header, stop" so this crate never needs the
//! zlib-inflate step a full pixel decode would (the Pi-contract discipline: no image
//! crate, no C codec, in the default build).

/// The 8-byte PNG signature every valid PNG file starts with.
const PNG_SIG: [u8; 8] = [0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'];

/// Read `(width, height)` from a PNG's leading `IHDR` chunk. Returns `None` if the
/// bytes don't start with the PNG signature or the first chunk isn't a well-formed
/// `IHDR` (per the PNG spec, `IHDR` is always the first chunk). Does NOT validate the
/// CRC or touch any `IDAT`/pixel data.
pub fn read_png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 8 + 8 + 13 || bytes[..8] != PNG_SIG {
        return None;
    }
    let len = u32::from_be_bytes(bytes[8..12].try_into().ok()?);
    let kind = &bytes[12..16];
    if kind != b"IHDR" || len != 13 {
        return None;
    }
    let data = &bytes[16..16 + 13];
    let width = u32::from_be_bytes(data[0..4].try_into().ok()?);
    let height = u32::from_be_bytes(data[4..8].try_into().ok()?);
    if width == 0 || height == 0 {
        return None;
    }
    Some((width, height))
}

/// Read `(width, height)` from a JPEG's `SOFn` (Start Of Frame) marker segment. Walks
/// the marker chain from the `SOI` (`FFD8`) forward, skipping marker segments that
/// carry no frame dimensions, until it finds a `SOFn` (baseline/progressive/etc,
/// excluding the DHT/DAC/JPG "SOF-shaped" codes that aren't actually frame headers) or
/// hits `SOS` (start of scan, meaning no SOF was present) / runs out of bytes.
pub fn read_jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 4 || bytes[0] != 0xFF || bytes[1] != 0xD8 {
        return None;
    }
    let mut pos = 2usize;
    while pos + 2 <= bytes.len() {
        if bytes[pos] != 0xFF {
            // Padding fill bytes (0xFF) between markers are legal; a non-0xFF byte
            // here means the stream isn't marker-aligned — bail out.
            pos += 1;
            continue;
        }
        let marker = bytes[pos + 1];
        pos += 2;
        // Markers with no payload: SOI/EOI, RSTn, and the (rare) TEM marker.
        if marker == 0xD8 || marker == 0xD9 || (0xD0..=0xD7).contains(&marker) || marker == 0x01 {
            continue;
        }
        if marker == 0xDA {
            // Start of Scan reached — no SOF was found before the entropy-coded data.
            return None;
        }
        if pos + 2 > bytes.len() {
            return None;
        }
        let seg_len = u16::from_be_bytes(bytes[pos..pos + 2].try_into().ok()?) as usize;
        if seg_len < 2 || pos + seg_len > bytes.len() {
            return None;
        }
        // SOF0..SOF15 except DHT(C4)/JPG(C8)/DAC(CC), which share the C0..CF range but
        // are not frame headers.
        let is_sof = matches!(marker, 0xC0..=0xC3 | 0xC5..=0xC7 | 0xC9..=0xCB | 0xCD..=0xCF);
        if is_sof {
            let payload = &bytes[pos + 2..pos + seg_len];
            if payload.len() < 5 {
                return None;
            }
            let height = u16::from_be_bytes(payload[1..3].try_into().ok()?) as u32;
            let width = u16::from_be_bytes(payload[3..5].try_into().ok()?) as u32;
            if width == 0 || height == 0 {
                return None;
            }
            return Some((width, height));
        }
        pos += seg_len;
    }
    None
}

/// Deterministic 128-bit FNV-1a content hash, rendered as 32-char lowercase hex — the
/// SAME construction `eg_tensor::store::content_hash` uses for its blob CAS keys
/// (duplicated here rather than shared, since `eg-image` must not depend on
/// `eg-tensor`: both are DAG-parallel leaf crates). Portable + stable across
/// processes/platforms, which a content-addressed `blob_ref` requires.
pub fn content_hash(bytes: &[u8]) -> String {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let mut h = OFFSET;
    for &b in bytes {
        h ^= b as u128;
        h = h.wrapping_mul(PRIME);
    }
    format!("{h:032x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal, structurally valid PNG (signature + one 13-byte `IHDR` chunk). The
    /// CRC bytes are dummy — `read_png_dimensions` never checks them (it only reads
    /// the header, not the pixel/CRC data), so this is a faithful minimal fixture.
    fn png_fixture(width: u32, height: u32) -> Vec<u8> {
        let mut b = PNG_SIG.to_vec();
        b.extend_from_slice(&13u32.to_be_bytes()); // chunk length
        b.extend_from_slice(b"IHDR");
        b.extend_from_slice(&width.to_be_bytes());
        b.extend_from_slice(&height.to_be_bytes());
        b.extend_from_slice(&[8, 0, 0, 0, 0]); // bit depth, color type, compression, filter, interlace
        b.extend_from_slice(&[0, 0, 0, 0]); // dummy CRC
        b
    }

    #[test]
    fn png_dimensions_are_read_from_a_real_ihdr_chunk() {
        let bytes = png_fixture(640, 480);
        assert_eq!(read_png_dimensions(&bytes), Some((640, 480)));
    }

    #[test]
    fn png_rejects_wrong_signature() {
        let mut bytes = png_fixture(10, 10);
        bytes[0] = 0x00;
        assert_eq!(read_png_dimensions(&bytes), None);
    }

    #[test]
    fn png_rejects_zero_dimension() {
        let bytes = png_fixture(0, 10);
        assert_eq!(read_png_dimensions(&bytes), None);
    }

    /// A minimal baseline JPEG: `SOI` + one `SOF0` marker segment declaring
    /// width=3/height=2, no scan data needed since we stop at SOF.
    fn jpeg_fixture(width: u16, height: u16) -> Vec<u8> {
        let mut b = vec![0xFF, 0xD8]; // SOI
        b.push(0xFF);
        b.push(0xC0); // SOF0
        let payload_len: u16 = 2 + 1 + 2 + 2 + 1 + 3; // len bytes + precision + h + w + numcomp + 1 component(3 bytes)
        b.extend_from_slice(&payload_len.to_be_bytes());
        b.push(8); // precision
        b.extend_from_slice(&height.to_be_bytes());
        b.extend_from_slice(&width.to_be_bytes());
        b.push(1); // num components
        b.extend_from_slice(&[1, 0x11, 0]); // component id, sampling, quant table
        b
    }

    #[test]
    fn jpeg_dimensions_are_read_from_a_real_sof0_marker() {
        let bytes = jpeg_fixture(3, 2);
        assert_eq!(read_jpeg_dimensions(&bytes), Some((3, 2)));
    }

    #[test]
    fn jpeg_rejects_missing_soi() {
        let mut bytes = jpeg_fixture(4, 4);
        bytes[1] = 0x00;
        assert_eq!(read_jpeg_dimensions(&bytes), None);
    }

    #[test]
    fn jpeg_returns_none_when_sos_precedes_any_sof() {
        // SOI followed immediately by SOS (0xDA) with no SOF in between.
        let bytes = vec![0xFF, 0xD8, 0xFF, 0xDA, 0x00, 0x02];
        assert_eq!(read_jpeg_dimensions(&bytes), None);
    }

    #[test]
    fn content_hash_is_deterministic_and_distinguishes_bytes() {
        let a = content_hash(b"hello");
        let b = content_hash(b"hello");
        let c = content_hash(b"world");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 32);
    }
}
