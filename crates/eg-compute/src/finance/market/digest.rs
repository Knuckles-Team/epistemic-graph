//! Domain-separated sha256 digests for signal keys, flip events and records.

use sha2::{Digest, Sha256};

/// `sha256:<hex>` over `domain` and length-framed `fields`, so no two field
/// lists that differ can collide by concatenation.
pub fn framed(domain: &str, fields: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    frame(&mut hasher, domain.as_bytes());
    for field in fields {
        frame(&mut hasher, field);
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn frame(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

/// `sha256:<hex>` over a value's canonical JSON (struct field order, shortest
/// round-trip floats), under `domain`.
pub fn of_json<T: serde::Serialize>(domain: &str, value: &T) -> String {
    let body = serde_json::to_vec(value).unwrap_or_default();
    framed(domain, &[&body])
}

#[cfg(test)]
mod tests {
    #[test]
    fn framing_separates_field_boundaries() {
        let joined = super::framed("d", &[b"ab", b"c"]);
        let split = super::framed("d", &[b"a", b"bc"]);
        assert_ne!(joined, split);
        assert!(joined.starts_with("sha256:") && joined.len() == 71);
    }
}
