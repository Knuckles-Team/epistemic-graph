//! Collision-resistant content addressing for raw document bytes.

use sha2::{Digest, Sha256};

/// SHA-256 content address rendered as 64 lowercase hexadecimal characters.
pub fn content_hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
        assert_eq!(a.len(), 64);
    }
}
