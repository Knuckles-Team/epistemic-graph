//! Dependency-free content addressing for raw document bytes. Mirrors
//! `eg_image::header::content_hash` / `eg_audio::header::content_hash` /
//! `eg_video::header::content_hash` byte-for-byte (the SAME FNV-1a-128
//! construction, duplicated rather than shared since `eg-document` must not depend
//! on any of its sibling modality crates — all are DAG-parallel leaves).

/// Deterministic 128-bit FNV-1a content hash, rendered as 32-char lowercase hex.
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

    #[test]
    fn content_hash_is_deterministic_and_distinguishes_bytes() {
        let a = content_hash(b"hello world");
        let b = content_hash(b"hello world");
        let c = content_hash(b"goodbye world");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 32);
    }
}
