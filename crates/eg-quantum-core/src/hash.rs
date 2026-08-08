//! Content-addressed circuit hashing.
//!
//! `QuantumResult::circuit_hash` (see `result.rs`) exists so that "same circuit +
//! same seed + same backend reproduces bit-exactly" (the PROGRAM.md acceptance
//! check) is checkable mechanically: hash the IR that was actually submitted, not a
//! caller-supplied label that can drift from the real program.

use crate::ir::QuantumProgram;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// A SHA-256 digest of a `QuantumProgram`'s canonical JSON encoding. Wrapped (rather
/// than a bare `[u8; 32]` or `String`) so the type signals intent at every call site
/// and so the hex `Display`/`Debug`/serde shape is defined in exactly one place.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CircuitHash([u8; 32]);

impl CircuitHash {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl fmt::Debug for CircuitHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CircuitHash({})", self.to_hex())
    }
}

impl fmt::Display for CircuitHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Errors hashing a program can hit. `serde_json::to_vec` on `QuantumProgram` cannot
/// actually fail for any value this crate constructs (no maps with non-string keys,
/// no NaN/inf floats expected in a well-formed circuit — though a caller-supplied
/// `f64::NAN` parameter would serialize fine as JSON `null`... `serde_json` in fact
/// errors on non-finite floats), so this is surfaced rather than swallowed.
#[derive(Debug, thiserror::Error)]
#[error("failed to canonicalize QuantumProgram for hashing: {0}")]
pub struct HashError(#[from] serde_json::Error);

pub fn circuit_hash(program: &QuantumProgram) -> Result<CircuitHash, HashError> {
    // `serde_json` serializes struct fields in DECLARATION order (not sorted), which
    // is fine here: `QuantumProgram`'s field order is part of its own stable shape
    // (see `ir.rs`), so two structurally-equal values always produce byte-identical
    // JSON — that is all "canonical" needs to mean for a content hash over a single
    // known Rust type (unlike hashing arbitrary externally-sourced JSON, which would
    // need genuine key-sorting to canonicalize).
    let bytes = serde_json::to_vec(program)?;
    let out: [u8; 32] = Sha256::digest(&bytes).into();
    Ok(CircuitHash(out))
}

impl QuantumProgram {
    /// Convenience wrapper around [`circuit_hash`].
    pub fn circuit_hash(&self) -> Result<CircuitHash, HashError> {
        circuit_hash(self)
    }
}
