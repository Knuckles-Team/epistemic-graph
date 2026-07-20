//! Dependency-free MP4/ISOBMFF box walking: real `duration_ms` read straight out of
//! the `moov/mvhd` box's `timescale`/`duration` fields — NOT a frame decode. Mirrors
//! `eg-geo::raster`'s hand-rolled PNG chunk walk / `eg-image`'s header parse (same
//! idiom: walk a length-prefixed container format far enough to read a few header
//! fields, stop there).
//!
//! ISOBMFF (the box format MP4/MOV/M4A all share) is a tree of `[size(4 BE)][type(4)]
//! [body]` boxes (with a 64-bit size extension when `size == 1`). `moov` (movie
//! metadata) contains `mvhd` (movie header), whose `version` byte selects 32-bit or
//! 64-bit `timescale`/`duration` fields.

use sha2::{Digest, Sha256};

/// Read one box header at the start of `bytes`: `(fourcc, header_len, body_len)`.
/// `header_len` is 8 normally, or 16 when a 64-bit extended size is present.
fn read_box_header(bytes: &[u8]) -> Option<([u8; 4], usize, usize)> {
    if bytes.len() < 8 {
        return None;
    }
    let size32 = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
    let kind: [u8; 4] = bytes[4..8].try_into().ok()?;
    if size32 == 1 {
        if bytes.len() < 16 {
            return None;
        }
        let size64 = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
        let body_len = usize::try_from(size64).ok()?.checked_sub(16)?;
        Some((kind, 16, body_len))
    } else if size32 == 0 {
        // A size of 0 means "extends to the end of the containing box/file".
        Some((kind, 8, bytes.len().checked_sub(8)?))
    } else {
        let body_len = (size32 as usize).checked_sub(8)?;
        Some((kind, 8, body_len))
    }
}

/// Find the FIRST box named `target` among the boxes packed in `bytes` and return its
/// body slice (not including the target's own header).
fn find_box<'a>(bytes: &'a [u8], target: &[u8; 4]) -> Option<&'a [u8]> {
    let mut pos = 0usize;
    while pos + 8 <= bytes.len() {
        let (kind, header_len, body_len) = read_box_header(&bytes[pos..])?;
        let body_start = pos + header_len;
        let body_end = body_start.checked_add(body_len)?;
        if body_end > bytes.len() {
            return None;
        }
        if &kind == target {
            return Some(&bytes[body_start..body_end]);
        }
        pos = body_end;
    }
    None
}

/// Parse an MP4/ISOBMFF file's `moov/mvhd` box and return its true duration in
/// milliseconds (`duration / timescale * 1000`, both fields read verbatim from the
/// header — no frame data is touched). Returns `None` if no `moov` box, no `mvhd`
/// box inside it, an unrecognized `mvhd` version, or a zero timescale is found.
pub fn read_mp4_duration_ms(bytes: &[u8]) -> Option<u64> {
    let moov = find_box(bytes, b"moov")?;
    let mvhd = find_box(moov, b"mvhd")?;
    if mvhd.is_empty() {
        return None;
    }
    let version = mvhd[0];
    let (timescale, duration) = match version {
        0 => {
            // version(1) + flags(3) + creation_time(4) + modification_time(4)
            // + timescale(4) + duration(4)
            if mvhd.len() < 20 {
                return None;
            }
            let timescale = u32::from_be_bytes(mvhd[12..16].try_into().ok()?);
            let duration = u32::from_be_bytes(mvhd[16..20].try_into().ok()?) as u64;
            (timescale, duration)
        }
        1 => {
            // version(1) + flags(3) + creation_time(8) + modification_time(8)
            // + timescale(4) + duration(8)
            if mvhd.len() < 32 {
                return None;
            }
            let timescale = u32::from_be_bytes(mvhd[20..24].try_into().ok()?);
            let duration = u64::from_be_bytes(mvhd[24..32].try_into().ok()?);
            (timescale, duration)
        }
        _ => return None,
    };
    if timescale == 0 {
        return None;
    }
    u64::try_from(u128::from(duration) * 1_000 / u128::from(timescale)).ok()
}

/// SHA-256 content address rendered as 64 lowercase hexadecimal characters.
pub fn content_hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mvhd_box_v0(timescale: u32, duration: u32) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(0); // version 0
        body.extend_from_slice(&[0, 0, 0]); // flags
        body.extend_from_slice(&0u32.to_be_bytes()); // creation_time
        body.extend_from_slice(&0u32.to_be_bytes()); // modification_time
        body.extend_from_slice(&timescale.to_be_bytes());
        body.extend_from_slice(&duration.to_be_bytes());
        let mut b = Vec::new();
        b.extend_from_slice(&(8u32 + body.len() as u32).to_be_bytes());
        b.extend_from_slice(b"mvhd");
        b.extend_from_slice(&body);
        b
    }

    fn moov_box(mvhd: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&(8u32 + mvhd.len() as u32).to_be_bytes());
        b.extend_from_slice(b"moov");
        b.extend_from_slice(mvhd);
        b
    }

    fn ftyp_box() -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&16u32.to_be_bytes());
        b.extend_from_slice(b"ftyp");
        b.extend_from_slice(b"isom");
        b.extend_from_slice(&0u32.to_be_bytes());
        b
    }

    #[test]
    fn mp4_duration_is_read_from_a_real_mvhd_box() {
        let mvhd = mvhd_box_v0(1000, 5000); // timescale=1000, duration=5000 units -> 5000ms
        let moov = moov_box(&mvhd);
        let mut file = ftyp_box();
        file.extend_from_slice(&moov);

        assert_eq!(read_mp4_duration_ms(&file), Some(5000));
    }

    #[test]
    fn mp4_duration_handles_non_1000_timescale() {
        // A common video timescale: 90000, duration = 90000*3 = 3s.
        let mvhd = mvhd_box_v0(90_000, 270_000);
        let moov = moov_box(&mvhd);
        assert_eq!(read_mp4_duration_ms(&moov), Some(3000));
    }

    #[test]
    fn missing_moov_yields_none() {
        let file = ftyp_box();
        assert_eq!(read_mp4_duration_ms(&file), None);
    }

    #[test]
    fn zero_timescale_yields_none() {
        let mvhd = mvhd_box_v0(0, 100);
        let moov = moov_box(&mvhd);
        assert_eq!(read_mp4_duration_ms(&moov), None);
    }

    #[test]
    fn content_hash_is_deterministic() {
        assert_eq!(content_hash(b"clip"), content_hash(b"clip"));
        assert_ne!(content_hash(b"clip"), content_hash(b"clop"));
    }
}
