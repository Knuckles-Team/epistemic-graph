//! A from-scratch, dependency-free PNG encoder (D-VZ-1 lane V3a).
//!
//! Emits a spec-valid PNG (RFC 2083): the 8-byte signature, `IHDR`
//! (8-bit RGBA, no interlace), one `IDAT` chunk wrapping a zlib stream (RFC
//! 1950) built from **stored** (uncompressed) deflate blocks (RFC 1951 §3.2.4),
//! and `IEND`. Every chunk carries the real CRC-32 the format requires. Stored
//! blocks mean larger files than a compressing encoder would produce, not a
//! smaller/wrong format — any standard PNG decoder reads this byte-for-byte
//! correctly. Kept hand-rolled specifically to add **zero** new crates.io
//! dependencies to this optional, default-off lane (see the crate's `Cargo.toml`
//! doc comment).

use crate::raster::Canvas;

const PNG_SIGNATURE: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
const MAX_STORED_BLOCK: usize = 65535;

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    crc ^ 0xFFFF_FFFF
}

fn adler32(data: &[u8]) -> u32 {
    const MOD_ADLER: u32 = 65521;
    let (mut a, mut b) = (1u32, 0u32);
    for &byte in data {
        a = (a + byte as u32) % MOD_ADLER;
        b = (b + a) % MOD_ADLER;
    }
    (b << 16) | a
}

/// RFC 1951 §3.2.4 stored (non-compressed) deflate blocks, chunked to the
/// format's 65535-byte-per-block ceiling.
fn deflate_stored(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + data.len() / MAX_STORED_BLOCK * 5 + 5);
    if data.is_empty() {
        out.push(0x01); // BFINAL=1, BTYPE=00
        out.extend(0u16.to_le_bytes());
        out.extend(0xFFFFu16.to_le_bytes());
        return out;
    }
    let mut offset = 0;
    while offset < data.len() {
        let end = (offset + MAX_STORED_BLOCK).min(data.len());
        let is_final = end == data.len();
        let len = (end - offset) as u16;
        out.push(if is_final { 0x01 } else { 0x00 });
        out.extend(len.to_le_bytes());
        out.extend((!len).to_le_bytes());
        out.extend_from_slice(&data[offset..end]);
        offset = end;
    }
    out
}

/// RFC 1950 zlib stream: a 2-byte header (`0x78 0x01` — deflate, 32K window,
/// fastest-level flag; `(0x78*256+0x01) % 31 == 0`, the check the format
/// requires), the deflate payload, and a big-endian Adler-32 trailer.
fn zlib_wrap(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 6);
    out.push(0x78);
    out.push(0x01);
    out.extend(deflate_stored(data));
    out.extend(adler32(data).to_be_bytes());
    out
}

fn write_chunk(out: &mut Vec<u8>, chunk_type: &[u8; 4], data: &[u8]) {
    out.extend((data.len() as u32).to_be_bytes());
    out.extend(chunk_type);
    out.extend(data);
    let mut crc_input = Vec::with_capacity(4 + data.len());
    crc_input.extend(chunk_type);
    crc_input.extend(data);
    out.extend(crc32(&crc_input).to_be_bytes());
}

/// Encode `canvas` (RGBA8) to a complete, valid PNG byte stream.
pub fn encode(canvas: &Canvas) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(PNG_SIGNATURE);

    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend(canvas.width.to_be_bytes());
    ihdr.extend(canvas.height.to_be_bytes());
    ihdr.push(8); // bit depth
    ihdr.push(6); // color type: RGBA
    ihdr.push(0); // compression method
    ihdr.push(0); // filter method
    ihdr.push(0); // interlace method: none
    write_chunk(&mut out, b"IHDR", &ihdr);

    let width = canvas.width as usize;
    let height = canvas.height as usize;
    let mut raw = Vec::with_capacity(height * (1 + width * 4));
    for y in 0..height {
        raw.push(0u8); // per-scanline filter type: None
        let start = y * width * 4;
        raw.extend_from_slice(&canvas.pixels[start..start + width * 4]);
    }
    write_chunk(&mut out, b"IDAT", &zlib_wrap(&raw));
    write_chunk(&mut out, b"IEND", &[]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_bytes_start_with_the_png_signature() {
        let canvas = Canvas::new(2, 2, [255, 0, 0, 255]);
        let bytes = encode(&canvas);
        assert_eq!(&bytes[0..8], &PNG_SIGNATURE);
    }

    #[test]
    fn ihdr_carries_the_real_dimensions() {
        let canvas = Canvas::new(37, 19, [0, 0, 0, 255]);
        let bytes = encode(&canvas);
        // IHDR chunk: [len(4)][type(4)="IHDR"][width(4)][height(4)]...
        let width = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let height = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
        assert_eq!(width, 37);
        assert_eq!(height, 19);
    }

    #[test]
    fn every_chunk_crc_is_valid() {
        let canvas = Canvas::new(4, 4, [1, 2, 3, 255]);
        let bytes = encode(&canvas);
        let mut offset = 8; // past signature
        while offset < bytes.len() {
            let len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            let chunk_type = &bytes[offset + 4..offset + 8];
            let data = &bytes[offset + 8..offset + 8 + len];
            let expected_crc = u32::from_be_bytes(
                bytes[offset + 8 + len..offset + 12 + len]
                    .try_into()
                    .unwrap(),
            );
            let mut crc_input = Vec::new();
            crc_input.extend(chunk_type);
            crc_input.extend(data);
            assert_eq!(
                crc32(&crc_input),
                expected_crc,
                "bad CRC for chunk {:?}",
                String::from_utf8_lossy(chunk_type)
            );
            offset += 12 + len;
        }
    }

    #[test]
    fn adler32_matches_a_known_vector() {
        // "Wikipedia" -> Adler-32 0x11E60398 (a widely quoted reference vector).
        assert_eq!(adler32(b"Wikipedia"), 0x11E6_0398);
    }

    #[test]
    fn large_canvas_spans_multiple_stored_deflate_blocks() {
        // width*height*4 + height > 65535 forces deflate_stored to emit more
        // than one block -- proves the >64KB chunking path, not just the
        // single-block happy path the small fixtures above exercise.
        let canvas = Canvas::new(200, 200, [0, 0, 0, 255]); // 200*200*4 = 160_000 bytes
        let bytes = encode(&canvas);
        assert_eq!(&bytes[0..8], &PNG_SIGNATURE);
        assert!(bytes.len() > 160_000);
    }
}
