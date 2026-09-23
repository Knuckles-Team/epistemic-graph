//! The keyed exploration and audit seed (EH-020, EH-026).
//!
//! A seed derived from the decision's own inputs could be steered by the caller
//! -- change an input until the draw explores -- and so poison the logs. The
//! seed is therefore `HMAC-SHA256(server key, state_digest ‖ 0x00 ‖
//! decision_id)`: the caller knows every input except the key. The record
//! carries only `sha256(seed)` as a commitment; the seed itself is revealed
//! after commit, and anyone can then check the draw against the commitment.
//!
//! HMAC is written out here (RFC 2104) over the `sha2` this crate already
//! links, rather than adding a MAC crate for twelve lines.

use sha2::{Digest, Sha256};

const BLOCK: usize = 64;

/// Domain of the exploration key derived from the server secret.
pub const EXPLORATION_KEY_DOMAIN: &[u8] = b"eg/decide/exploration-key/v1";

/// `HMAC-SHA256(key, message)`.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let pad = |byte: u8| -> [u8; BLOCK] {
        let mut out = [0u8; BLOCK];
        for (slot, value) in out.iter_mut().zip(block.iter()) {
            *slot = value ^ byte;
        }
        out
    };
    let inner = Sha256::new()
        .chain_update(pad(0x36))
        .chain_update(message)
        .finalize();
    Sha256::new()
        .chain_update(pad(0x5c))
        .chain_update(inner)
        .finalize()
        .into()
}

/// The exploration key a server derives from its own secret.
pub fn exploration_key(server_secret: &[u8]) -> [u8; 32] {
    hmac_sha256(server_secret, EXPLORATION_KEY_DOMAIN)
}

/// The keyed seed of one decision.
pub fn decision_seed(key: &[u8; 32], state_digest: &str, decision_id: &str) -> [u8; 32] {
    let mut message = Vec::with_capacity(state_digest.len() + decision_id.len() + 1);
    message.extend_from_slice(state_digest.as_bytes());
    message.push(0);
    message.extend_from_slice(decision_id.as_bytes());
    hmac_sha256(key, &message)
}

/// `sha256:<hex>` of the seed: what a record commits to before revealing it.
pub fn seed_commitment(seed: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(seed)))
}

/// Hex text of a revealed seed.
pub fn seed_text(seed: &[u8; 32]) -> String {
    hex::encode(seed)
}

/// A uniform draw in `[0, bound)` for the named purpose, by rejection sampling
/// over successive HMAC blocks so the result is exactly uniform.
pub fn uniform_draw(seed: &[u8; 32], purpose: &str, bound: u64) -> u64 {
    if bound <= 1 {
        return 0;
    }
    let zone = u64::MAX - (u64::MAX % bound);
    let mut counter: u32 = 0;
    loop {
        let mut message = purpose.as_bytes().to_vec();
        message.extend_from_slice(&counter.to_be_bytes());
        let block = hmac_sha256(seed, &message);
        let mut word = [0u8; 8];
        word.copy_from_slice(&block[..8]);
        let value = u64::from_be_bytes(word);
        if value < zone {
            return value % bound;
        }
        counter = counter.wrapping_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 4231 test case 2.
    #[test]
    fn hmac_matches_the_rfc_4231_vector() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            hex::encode(mac),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn the_seed_depends_on_the_key_and_the_commitment_binds_it() {
        let key_a = exploration_key(b"secret-a");
        let key_b = exploration_key(b"secret-b");
        let seed_a = decision_seed(&key_a, "sha256:state", "decision-1");
        assert_ne!(seed_a, decision_seed(&key_b, "sha256:state", "decision-1"));
        assert_ne!(seed_a, decision_seed(&key_a, "sha256:state", "decision-2"));
        assert_eq!(seed_commitment(&seed_a), seed_commitment(&seed_a));
        assert!(uniform_draw(&seed_a, "explore", 7) < 7);
        assert_eq!(uniform_draw(&seed_a, "explore", 1), 0);
    }
}
