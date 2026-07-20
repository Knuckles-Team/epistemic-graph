//! Encryption-at-rest for the redb durable value blobs (CONCEPT:EG-KG.sharding.row-level-security, Lane O).
//!
//! A PURE-RUST RustCrypto AEAD (`chacha20poly1305::ChaCha20Poly1305`) — NO ring,
//! NO openssl, NO C — so the Pi crypto contract holds. The data key is derived from
//! `EPISTEMIC_GRAPH_ENCRYPTION_KEY` (a KMS hook seam is provided via
//! [`resolve_key`]). Encryption is **opt-in / default OFF**: it changes the on-disk
//! value format, so a cipher is installed only when the env key is present.
//!
//! On-disk value wrapping (when encryption is active):
//! ```text
//!   [ MAGIC (1 byte) | nonce (12 bytes) | ciphertext+tag ]
//! ```
//! The `MAGIC` byte (`0xE6`, "eg-encrypted") makes the sealed format
//! self-describing. When encryption is configured, every value must carry this
//! format; unsealed bytes fail closed and require an explicit offline conversion.
//!
//! Only VALUE bytes are sealed (node/edge property blobs, the semantic store blob).
//! redb KEYS (graph name, node id, seq) stay plaintext so range scans / point reads
//! work unchanged — the threat model is "raw .redb file bytes must not reveal node
//! PROPERTIES", which the gate proves (`grep -c <plaintext> graph-0.redb == 0`).

#![cfg(any(feature = "security", feature = "raft"))]

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Env var holding the raw encryption key material (any length; hashed to 32 bytes).
pub const ENCRYPTION_KEY_ENV: &str = "EPISTEMIC_GRAPH_ENCRYPTION_KEY";

/// First byte of an encrypted value blob.
const MAGIC: u8 = 0xE6;
const NONCE_LEN: usize = 12;

/// An installed value-blob cipher. `Some` only when encryption is enabled; cloned
/// cheaply (it wraps the AEAD in an `Arc`) into the durable backend's write/read path.
#[derive(Clone)]
pub struct ValueCipher {
    aead: Arc<ChaCha20Poly1305>,
}

impl std::fmt::Debug for ValueCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ValueCipher(<aead>)")
    }
}

impl ValueCipher {
    /// Build a cipher from raw key material (hashed to a 32-byte ChaCha20 key, so any
    /// length env value works). A KMS hook can call this with key bytes it fetched.
    pub fn from_key_material(material: &[u8]) -> Self {
        let key_bytes = Sha256::digest(material);
        let key = Key::from_slice(&key_bytes);
        ValueCipher {
            aead: Arc::new(ChaCha20Poly1305::new(key)),
        }
    }

    /// Resolve the cipher from the environment (the KMS seam — today it reads the env
    /// key; a future KMS provider would fetch the data key here). Returns `None` when
    /// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` is unset/empty ⇒ encryption stays OFF.
    pub fn from_env() -> Option<Self> {
        resolve_key().map(|m| ValueCipher::from_key_material(&m))
    }

    /// Seal a plaintext value blob → `[MAGIC | nonce | ciphertext+tag]`. A random
    /// nonce per call (so identical plaintexts encrypt to different bytes).
    pub fn seal(&self, plaintext: &[u8]) -> Vec<u8> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        // AEAD encryption is infallible for ChaCha20Poly1305 with a valid key; on the
        // theoretical error we fall back to NOT corrupting data — propagate by panic
        // would be wrong, so return the plaintext is also wrong; instead unwrap, which
        // only fails on an impossible buffer-size condition.
        let ct = self
            .aead
            .encrypt(nonce, plaintext)
            .expect("AEAD seal cannot fail for ChaCha20Poly1305");
        let mut out = Vec::with_capacity(1 + NONCE_LEN + ct.len());
        out.push(MAGIC);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ct);
        out
    }

    /// Unseal a stored value blob. Missing framing, a wrong key, and ciphertext
    /// tampering all fail closed; plaintext is never accepted while a cipher is active.
    pub fn unseal(&self, stored: &[u8]) -> Result<Vec<u8>, String> {
        if !is_sealed(stored) {
            return Err("encrypted durable value is missing sealed framing".to_string());
        }
        let nonce = Nonce::from_slice(&stored[1..1 + NONCE_LEN]);
        let ct = &stored[1 + NONCE_LEN..];
        self.aead
            .decrypt(nonce, ct)
            .map_err(|_| "decryption failed (wrong key or tampered ciphertext)".to_string())
    }
}

/// Is a stored value blob an encrypted (sealed) blob? (Begins with the magic byte and
/// is large enough to carry the nonce + AEAD tag.)
pub fn is_sealed(stored: &[u8]) -> bool {
    stored.len() > 1 + NONCE_LEN && stored[0] == MAGIC
}

/// The KMS hook seam: resolve raw key material. Today it reads
/// `EPISTEMIC_GRAPH_ENCRYPTION_KEY`; a KMS-backed deployment swaps this for a fetch
/// of the data key (envelope encryption). Returns `None` ⇒ encryption disabled.
pub fn resolve_key() -> Option<Vec<u8>> {
    match std::env::var(ENCRYPTION_KEY_ENV) {
        Ok(v) if !v.is_empty() => Some(v.into_bytes()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let c = ValueCipher::from_key_material(b"super-secret");
        let pt = b"{\"name\":\"Alice\",\"ssn\":\"123-45-6789\"}";
        let sealed = c.seal(pt);
        assert!(is_sealed(&sealed));
        // The sealed bytes must NOT contain the plaintext.
        assert!(
            !sealed.windows(pt.len()).any(|w| w == pt),
            "plaintext leaked into ciphertext"
        );
        assert_eq!(c.unseal(&sealed).unwrap(), pt);
    }

    #[test]
    fn wrong_key_fails() {
        let a = ValueCipher::from_key_material(b"key-a");
        let b = ValueCipher::from_key_material(b"key-b");
        let sealed = a.seal(b"secret payload");
        assert!(b.unseal(&sealed).is_err(), "wrong key must NOT decrypt");
    }

    #[test]
    fn plaintext_is_rejected_when_cipher_is_active() {
        let c = ValueCipher::from_key_material(b"k");
        let plain = b"not-encrypted-bytes";
        assert!(!is_sealed(plain));
        assert!(c.unseal(plain).is_err());
    }

    #[test]
    fn tamper_detected() {
        let c = ValueCipher::from_key_material(b"k");
        let mut sealed = c.seal(b"payload");
        let last = sealed.len() - 1;
        sealed[last] ^= 0xFF; // flip a ciphertext/tag bit
        assert!(c.unseal(&sealed).is_err());
    }

    #[test]
    fn distinct_nonces() {
        let c = ValueCipher::from_key_material(b"k");
        let a = c.seal(b"same");
        let b = c.seal(b"same");
        assert_ne!(a, b, "nonce reuse: identical ciphertexts");
    }
}
