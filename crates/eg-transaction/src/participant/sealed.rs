//! Sealed-payload framing and the authentication seam.
//!
//! The record model must recognise an authenticated-ciphertext blob without
//! owning a cipher, and must be able to open one when a caller supplies the
//! authority to do so. Framing is a pure byte-shape predicate; opening is an
//! injected capability, so this crate links no AEAD implementation and no
//! vendor-specific key material.

/// First byte of an authenticated-ciphertext value blob.
pub const SEALED_PAYLOAD_MAGIC: u8 = 0xE6;
/// Nonce length carried between the magic byte and the ciphertext+tag.
pub const SEALED_PAYLOAD_NONCE_BYTES: usize = 12;

/// Is this blob shaped as `[MAGIC | nonce | ciphertext+tag]`?
///
/// This is a *framing* predicate, not a cryptographic check: it proves the
/// value was produced by the sealing format, never that it authenticates.
/// [`SealedPayloadOpener`] is the only thing that can prove the latter.
pub fn is_sealed_payload(value: &[u8]) -> bool {
    value.len() > 1 + SEALED_PAYLOAD_NONCE_BYTES && value[0] == SEALED_PAYLOAD_MAGIC
}

/// Authority to open a sealed payload. The composition root supplies the one
/// implementation; this kernel never constructs key material.
pub trait SealedPayloadOpener {
    fn unseal(&self, sealed: &[u8]) -> Result<Vec<u8>, String>;
}
