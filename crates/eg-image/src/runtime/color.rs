//! PNG color-type → RGBA conversion (CONCEPT:EG-KG.query.png-decode).
//!
//! Split out of `runtime.rs` (a "shared module instead of growing a file past
//! the file-size caps" split, not a rename: `runtime.rs` keeps its own path
//! and every other decode concern; this module holds only the per-color-type
//! `to_rgba` conversion `finish_decode` calls into).

/// The PNG color-type dispatch: expand `raw` scanline bytes to RGBA.
pub(super) fn to_rgba(raw: &[u8], color_type: u8, palette: &[u8], alpha: &[u8]) -> Option<Vec<u8>> {
    match color_type {
        0 => gray_to_rgba(raw),
        2 => rgb_to_rgba(raw),
        3 => indexed_to_rgba(raw, palette, alpha),
        4 => gray_alpha_to_rgba(raw),
        6 => Some(raw.to_vec()),
        _ => None,
    }
}

/// PNG color type 0 (grayscale) → RGBA. Split out of [`to_rgba`]
/// (extract-method) so each color type's conversion stays within the
/// per-function complexity cap.
fn gray_to_rgba(raw: &[u8]) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(raw.len().checked_mul(4)?);
    for gray in raw {
        output.extend_from_slice(&[*gray, *gray, *gray, 255]);
    }
    Some(output)
}

/// PNG color type 2 (truecolor RGB) → RGBA.
fn rgb_to_rgba(raw: &[u8]) -> Option<Vec<u8>> {
    let pixel_count = raw.len().checked_div(3)?;
    let mut output = Vec::with_capacity(pixel_count.checked_mul(4)?);
    for rgb in raw.chunks_exact(3) {
        output.extend_from_slice(&[rgb[0], rgb[1], rgb[2], 255]);
    }
    Some(output)
}

/// PNG color type 3 (indexed) → RGBA via the palette + optional `tRNS` alpha.
fn indexed_to_rgba(raw: &[u8], palette: &[u8], alpha: &[u8]) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(raw.len().checked_mul(4)?);
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
    Some(output)
}

/// PNG color type 4 (grayscale + alpha) → RGBA.
fn gray_alpha_to_rgba(raw: &[u8]) -> Option<Vec<u8>> {
    let pixel_count = raw.len().checked_div(2)?;
    let mut output = Vec::with_capacity(pixel_count.checked_mul(4)?);
    for value in raw.chunks_exact(2) {
        output.extend_from_slice(&[value[0], value[0], value[0], value[1]]);
    }
    Some(output)
}
