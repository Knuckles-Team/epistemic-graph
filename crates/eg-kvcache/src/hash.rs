//! Content addressing for KV blocks (CONCEPT:EG-KG.enrichment.content-address-separation).
//!
//! A deterministic 128-bit FNV-1a content hash rendered as 32-char lowercase hex —
//! the SAME construction eg-tensor uses for its content-addressed blob store
//! (`eg_tensor::content_hash`). It is portable + stable across processes / platforms
//! (unlike `std`'s `DefaultHasher`), which a content-addressed / shared cache REQUIRES:
//! two independent vLLM instances must derive the SAME address for the SAME token
//! block so they DEDUP against one shared backend (CONCEPT:EG-KG.enrichment.content-address-separation). NOT cryptographic —
//! it is a dedup / integrity address within the engine trust boundary.

/// Deterministic 128-bit FNV-1a content hash of `bytes`, rendered as 32-char lowercase
/// hex (CONCEPT:EG-KG.enrichment.content-address-separation). Pure-Rust `u128` arithmetic, no dependency.
pub fn content_hash(bytes: &[u8]) -> String {
    // 128-bit FNV-1a constants (the standard offset basis + prime).
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

    /// CONCEPT:EG-KG.enrichment.content-address-separation — the hash is deterministic (same bytes ⇒ same address across
    /// calls), the property that lets independent instances dedup against one backend.
    #[test]
    fn eg186_content_hash_is_deterministic_and_content_addressed() {
        let a = content_hash(b"the quick brown fox");
        let b = content_hash(b"the quick brown fox");
        let c = content_hash(b"the quick brown box");
        assert_eq!(a, b, "same content must hash identically (dedup key)");
        assert_ne!(a, c, "different content must not collide here");
        assert_eq!(a.len(), 32, "32-char lowercase hex");
        assert!(a.chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    /// CONCEPT:EG-KG.enrichment.content-address-separation — the empty block still yields the stable FNV-1a offset basis.
    #[test]
    fn eg186_content_hash_empty_is_offset_basis() {
        assert_eq!(
            content_hash(b""),
            format!("{:032x}", 0x6c62_272e_07bb_0142_62b8_2175_6295_c58du128)
        );
    }
}
