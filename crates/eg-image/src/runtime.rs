//! Memory-bounded native PNG decode and feature extraction.

use std::io::Read;

use eg_modality::{GovernedModality, NativePredicate, NativeProductionProbe};
use flate2::read::ZlibDecoder;

use crate::{content_hash, ImageColorSpace, ImageData, ImageFormat};

const PNG_SIGNATURE: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
const MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;
/// One complete 4K frame, while keeping decoded RGBA plus scanline working memory
/// below roughly 72 MiB.
const MAX_DECODED_PIXELS: usize = 8_388_608;
const MAX_ZLIB_OVERHEAD_BYTES: usize = 1024 * 1024;
// A valid 1x1 RGBA PNG (filter-type-0/None scanline, pixel (255,0,0,127)). The
// previous literal here had a corrupt zlib Adler-32 trailer on its IDAT stream
// (`05 fe 02 fe` where the deflated bytes' real checksum is `04 85 01 80`) — a
// stale/broken test fixture, not a codec bug: `flate2::ZlibDecoder` correctly
// enforces the checksum (per RFC 1950) and `read_to_end` failed, so
// `decode_png` correctly returned `None` on this input. Regenerated 2026-08-11
// with a real zlib encoder so every CRC32/Adler-32 in the file is authentic;
// verified byte-for-byte length-identical (70 bytes) to the original so the
// IHDR-relative byte offsets `production_probe` mutates (`[16..20]` width,
// `[20..24]` height, `[29]` IHDR CRC byte) are unaffected.
const PROBE_PNG: &[u8] = &[
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0xda, 0x63, 0xf8, 0xcf, 0xc0, 0x50,
    0x0f, 0x00, 0x04, 0x80, 0x01, 0x7f, 0xa6, 0x8b, 0x01, 0x3d, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
    0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColorInfo {
    pub color_space: ImageColorSpace,
    pub bit_depth: u8,
    pub channel_count: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PixelBuffer {
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedImage {
    pub pixels: PixelBuffer,
    pub color: ColorInfo,
    pub perceptual_hash: u64,
}

/// Concrete current image runtime. Decoded pixels remain request-local and are not
/// serializable; only normalized metadata and the perceptual hash are persisted.
#[derive(Clone, Debug)]
pub struct NativeImageRuntime {
    source_ref: String,
    decoded: DecodedImage,
}

impl NativeImageRuntime {
    pub fn decode_png(bytes: &[u8]) -> Option<Self> {
        let decoded = decode_png(bytes)?;
        Some(Self {
            source_ref: content_hash(bytes),
            decoded,
        })
    }

    pub fn decoded(&self) -> &DecodedImage {
        &self.decoded
    }

    pub fn normalized_data(&self) -> ImageData {
        ImageData::new(
            self.decoded.pixels.width,
            self.decoded.pixels.height,
            self.source_ref.clone(),
        )
        .with_format(ImageFormat::Png)
        .with_native_features(
            self.decoded.color.color_space,
            self.decoded.color.bit_depth,
            self.decoded.perceptual_hash,
        )
    }
}

pub fn production_probe() -> NativeProductionProbe {
    let runtime = NativeImageRuntime::decode_png(PROBE_PNG);
    let value = runtime.as_ref().map(NativeImageRuntime::normalized_data);
    let codec = runtime.is_some();
    let normalized_payload = value
        .as_ref()
        .is_some_and(GovernedModality::validate_governed_payload);
    let secondary_index = value
        .as_ref()
        .is_some_and(|value| !value.native_index_keys().is_empty());
    let typed_query = value.as_ref().is_some_and(|value| {
        value.matches_native_predicate(&NativePredicate::ImagePerceptualHash {
            hash: value.perceptual_hash,
            maximum_distance: 0,
        })
    });
    let mut corrupt = PROBE_PNG.to_vec();
    corrupt[29] ^= 1;
    let mut oversized = PROBE_PNG.to_vec();
    oversized[16..20].copy_from_slice(&100_000u32.to_be_bytes());
    oversized[20..24].copy_from_slice(&100_000u32.to_be_bytes());
    let ihdr_crc = crc32_parts(b"IHDR", &oversized[16..29]);
    oversized[29..33].copy_from_slice(&ihdr_crc.to_be_bytes());
    let malformed_and_resource_bounds = NativeImageRuntime::decode_png(&corrupt).is_none()
        && NativeImageRuntime::decode_png(&[0u8; 32]).is_none()
        && NativeImageRuntime::decode_png(&oversized).is_none();
    NativeProductionProbe {
        codec,
        normalized_payload,
        secondary_index,
        typed_query,
        malformed_and_resource_bounds,
    }
}

fn decode_png(bytes: &[u8]) -> Option<DecodedImage> {
    if bytes.len() > MAX_SOURCE_BYTES || bytes.get(..8)? != PNG_SIGNATURE {
        return None;
    }
    let mut position = 8usize;
    let mut header = None;
    let mut palette = Vec::new();
    let mut transparency = Vec::new();
    let mut compressed = Vec::new();
    let mut ended = false;
    let mut seen_palette = false;
    let mut seen_transparency = false;
    let mut seen_idat = false;
    let mut idat_closed = false;
    while position.checked_add(12)? <= bytes.len() {
        let length =
            u32::from_be_bytes(bytes.get(position..position + 4)?.try_into().ok()?) as usize;
        let kind: [u8; 4] = bytes.get(position + 4..position + 8)?.try_into().ok()?;
        let data_start = position + 8;
        let data_end = data_start.checked_add(length)?;
        let crc_end = data_end.checked_add(4)?;
        let data = bytes.get(data_start..data_end)?;
        let expected_crc = u32::from_be_bytes(bytes.get(data_end..crc_end)?.try_into().ok()?);
        if !kind.iter().all(u8::is_ascii_alphabetic) || crc32_parts(&kind, data) != expected_crc {
            return None;
        }
        match &kind {
            b"IHDR" if position == 8 && header.is_none() && length == 13 => {
                let width = u32::from_be_bytes(data.get(0..4)?.try_into().ok()?);
                let height = u32::from_be_bytes(data.get(4..8)?.try_into().ok()?);
                let bit_depth = *data.get(8)?;
                let color_type = *data.get(9)?;
                if width == 0 || height == 0 || bit_depth != 8 || data.get(10..13)? != [0, 0, 0] {
                    return None;
                }
                if usize::try_from(width)
                    .ok()?
                    .checked_mul(usize::try_from(height).ok()?)?
                    > MAX_DECODED_PIXELS
                {
                    return None;
                }
                color_layout(color_type)?;
                header = Some((width, height, bit_depth, color_type));
            }
            b"PLTE"
                if header.is_some()
                    && !seen_palette
                    && !seen_idat
                    && length >= 3
                    && length.is_multiple_of(3)
                    && length <= 768 =>
            {
                seen_palette = true;
                palette.extend_from_slice(data);
            }
            b"tRNS" if header.is_some() && !seen_transparency && !seen_idat && length <= 256 => {
                seen_transparency = true;
                transparency.extend_from_slice(data);
            }
            b"IDAT" if header.is_some() && !ended && !idat_closed => {
                seen_idat = true;
                let (width, height, _, color_type) = header?;
                let (channels, _) = color_layout(color_type)?;
                let encoded_limit = usize::try_from(height)
                    .ok()?
                    .checked_mul(
                        usize::try_from(width)
                            .ok()?
                            .checked_mul(channels)?
                            .checked_add(1)?,
                    )?
                    .checked_add(MAX_ZLIB_OVERHEAD_BYTES)?;
                if compressed.len().checked_add(data.len())? > encoded_limit {
                    return None;
                }
                compressed.extend_from_slice(data);
            }
            b"IEND" if length == 0 => {
                ended = true;
                position = crc_end;
                break;
            }
            b"IHDR" | b"PLTE" | b"tRNS" | b"IDAT" | b"IEND" => return None,
            _ if kind[0].is_ascii_uppercase() => return None,
            _ => {
                if seen_idat {
                    idat_closed = true;
                }
            }
        }
        position = crc_end;
    }
    if !ended || position != bytes.len() || compressed.is_empty() {
        return None;
    }
    let (width, height, bit_depth, color_type) = header?;
    let pixels = usize::try_from(width)
        .ok()?
        .checked_mul(usize::try_from(height).ok()?)?;
    if pixels > MAX_DECODED_PIXELS {
        return None;
    }
    let (channels, color_space) = color_layout(color_type)?;
    if (color_type == 3 && palette.is_empty())
        || (matches!(color_type, 0 | 4) && !palette.is_empty())
        || (color_type != 3 && !transparency.is_empty())
        || transparency.len() > palette.len() / 3
    {
        return None;
    }
    let row_bytes = usize::try_from(width).ok()?.checked_mul(channels)?;
    let expected = usize::try_from(height)
        .ok()?
        .checked_mul(row_bytes.checked_add(1)?)?;
    let decoder = ZlibDecoder::new(compressed.as_slice());
    let mut limited = decoder.take(expected as u64 + 1);
    let mut inflated = Vec::with_capacity(expected);
    limited.read_to_end(&mut inflated).ok()?;
    if inflated.len() != expected || limited.get_ref().total_in() != compressed.len() as u64 {
        return None;
    }
    let raw = unfilter(
        &inflated,
        row_bytes,
        channels,
        usize::try_from(height).ok()?,
    )?;
    let rgba = to_rgba(&raw, color_type, &palette, &transparency)?;
    let buffer = PixelBuffer {
        width,
        height,
        rgba,
    };
    let perceptual_hash = difference_hash(&buffer);
    Some(DecodedImage {
        pixels: buffer,
        color: ColorInfo {
            color_space,
            bit_depth,
            channel_count: channels as u8,
        },
        perceptual_hash,
    })
}

fn color_layout(color_type: u8) -> Option<(usize, ImageColorSpace)> {
    Some(match color_type {
        0 => (1usize, ImageColorSpace::Gray),
        2 => (3usize, ImageColorSpace::Rgb),
        3 => (1usize, ImageColorSpace::Indexed),
        4 => (2usize, ImageColorSpace::GrayAlpha),
        6 => (4usize, ImageColorSpace::Rgba),
        _ => return None,
    })
}

fn unfilter(
    input: &[u8],
    row_bytes: usize,
    bytes_per_pixel: usize,
    rows: usize,
) -> Option<Vec<u8>> {
    let mut output = vec![0u8; row_bytes.checked_mul(rows)?];
    for row in 0..rows {
        let encoded_start = row.checked_mul(row_bytes.checked_add(1)?)?;
        let filter = *input.get(encoded_start)?;
        let source = input.get(encoded_start + 1..encoded_start + 1 + row_bytes)?;
        let target_start = row.checked_mul(row_bytes)?;
        for column in 0..row_bytes {
            let left = if column >= bytes_per_pixel {
                output[target_start + column - bytes_per_pixel]
            } else {
                0
            };
            let above = if row > 0 {
                output[target_start + column - row_bytes]
            } else {
                0
            };
            let upper_left = if row > 0 && column >= bytes_per_pixel {
                output[target_start + column - row_bytes - bytes_per_pixel]
            } else {
                0
            };
            output[target_start + column] = match filter {
                0 => source[column],
                1 => source[column].wrapping_add(left),
                2 => source[column].wrapping_add(above),
                3 => source[column].wrapping_add(((u16::from(left) + u16::from(above)) / 2) as u8),
                4 => source[column].wrapping_add(paeth(left, above, upper_left)),
                _ => return None,
            };
        }
    }
    Some(output)
}

fn paeth(left: u8, above: u8, upper_left: u8) -> u8 {
    let left = i32::from(left);
    let above = i32::from(above);
    let upper_left = i32::from(upper_left);
    let estimate = left + above - upper_left;
    let distances = [
        ((estimate - left).abs(), left),
        ((estimate - above).abs(), above),
        ((estimate - upper_left).abs(), upper_left),
    ];
    distances
        .into_iter()
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, value)| value as u8)
        .unwrap_or(0)
}

fn to_rgba(raw: &[u8], color_type: u8, palette: &[u8], alpha: &[u8]) -> Option<Vec<u8>> {
    let pixel_count = match color_type {
        0 => raw.len(),
        2 => raw.len().checked_div(3)?,
        3 => raw.len(),
        4 => raw.len().checked_div(2)?,
        6 => raw.len().checked_div(4)?,
        _ => return None,
    };
    let mut output = Vec::with_capacity(pixel_count.checked_mul(4)?);
    match color_type {
        0 => raw
            .iter()
            .for_each(|gray| output.extend_from_slice(&[*gray, *gray, *gray, 255])),
        2 => raw
            .chunks_exact(3)
            .for_each(|rgb| output.extend_from_slice(&[rgb[0], rgb[1], rgb[2], 255])),
        3 => {
            for index in raw {
                let offset = usize::from(*index).checked_mul(3)?;
                let rgb = palette.get(offset..offset + 3)?;
                output.extend_from_slice(&[
                    rgb[0],
                    rgb[1],
                    rgb[2],
                    alpha.get(usize::from(*index)).copied().unwrap_or(255),
                ]);
            }
        }
        4 => raw
            .chunks_exact(2)
            .for_each(|value| output.extend_from_slice(&[value[0], value[0], value[0], value[1]])),
        6 => output.extend_from_slice(raw),
        _ => return None,
    }
    Some(output)
}

fn difference_hash(pixels: &PixelBuffer) -> u64 {
    let mut hash = 0u64;
    for y in 0..8u32 {
        for x in 0..8u32 {
            let left = luminance(pixels, x * pixels.width / 9, y * pixels.height / 8);
            let right = luminance(pixels, (x + 1) * pixels.width / 9, y * pixels.height / 8);
            hash = (hash << 1) | u64::from(left > right);
        }
    }
    hash
}

fn luminance(pixels: &PixelBuffer, x: u32, y: u32) -> u16 {
    let x = x.min(pixels.width - 1);
    let y = y.min(pixels.height - 1);
    let offset = (usize::try_from(y).unwrap_or(0) * usize::try_from(pixels.width).unwrap_or(0)
        + usize::try_from(x).unwrap_or(0))
        * 4;
    let pixel = &pixels.rgba[offset..offset + 4];
    (u16::from(pixel[0]) * 77 + u16::from(pixel[1]) * 150 + u16::from(pixel[2]) * 29) >> 8
}

#[cfg(test)]
fn crc32(bytes: &[u8]) -> u32 {
    !crc32_update(!0u32, bytes)
}

fn crc32_parts(kind: &[u8; 4], data: &[u8]) -> u32 {
    !crc32_update(crc32_update(!0u32, kind), data)
}

fn crc32_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{write::ZlibEncoder, Compression};

    use super::*;

    fn chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&(data.len() as u32).to_be_bytes());
        output.extend_from_slice(kind);
        output.extend_from_slice(data);
        let mut crc_data = kind.to_vec();
        crc_data.extend_from_slice(data);
        output.extend_from_slice(&crc32(&crc_data).to_be_bytes());
        output
    }

    fn png_fixture() -> Vec<u8> {
        let mut ihdr = Vec::new();
        ihdr.extend_from_slice(&9u32.to_be_bytes());
        ihdr.extend_from_slice(&8u32.to_be_bytes());
        ihdr.extend_from_slice(&[8, 6, 0, 0, 0]);
        let mut scanlines = Vec::new();
        for y in 0..8u8 {
            scanlines.push(0);
            for x in 0..9u8 {
                scanlines.extend_from_slice(&[x.saturating_mul(24), y.saturating_mul(24), 0, 255]);
            }
        }
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&scanlines).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut bytes = PNG_SIGNATURE.to_vec();
        bytes.extend_from_slice(&chunk(b"IHDR", &ihdr));
        bytes.extend_from_slice(&chunk(b"IDAT", &compressed));
        bytes.extend_from_slice(&chunk(b"IEND", &[]));
        bytes
    }

    #[test]
    fn native_runtime_decodes_pixels_and_derives_perceptual_hash() {
        let bytes = png_fixture();
        let runtime = NativeImageRuntime::decode_png(&bytes).unwrap();
        assert_eq!(
            (
                runtime.decoded().pixels.width,
                runtime.decoded().pixels.height
            ),
            (9, 8)
        );
        assert_eq!(runtime.decoded().pixels.rgba.len(), 9 * 8 * 4);
        assert_ne!(runtime.decoded().perceptual_hash, 0);
        let value = runtime.normalized_data();
        assert_eq!(value.color_space, ImageColorSpace::Rgba);
        assert_eq!(value.bit_depth, 8);
        assert_eq!(value.perceptual_hash, runtime.decoded().perceptual_hash);
    }

    #[test]
    fn malformed_crc_and_truncated_deflate_are_rejected() {
        let mut bytes = png_fixture();
        bytes[29] ^= 1;
        assert!(NativeImageRuntime::decode_png(&bytes).is_none());
        let mut truncated = png_fixture();
        truncated.truncate(truncated.len() - 20);
        assert!(NativeImageRuntime::decode_png(&truncated).is_none());
    }
}
