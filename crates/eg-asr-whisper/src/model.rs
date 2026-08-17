//! Model acquisition is explicitly NOT this crate's job (GOC-36, "governed
//! model acquisition, digest verification, licensing, manifests"). This module
//! only ever CONSUMES a model path/handle a caller already resolved — it never
//! constructs a URL, never spawns a download, and never falls back to any
//! network fetch. It fails closed whenever the file is absent, unreadable, or
//! its content digest does not match the digest the caller declared.
//!
//! The digest check here is a **local content-integrity check**, not a
//! substitute for GOC-36's future cryptographic signer-chain verification: a
//! caller who declares the WRONG digest for a file they control can still get
//! it loaded. What this module guarantees is narrower and still real: a model
//! file that was truncated, substituted, or simply doesn't match what the
//! caller believes they are pointing at is refused before a single model byte
//! reaches whisper.cpp.

use std::path::Path;

use eg_audio::asr::{AsrBoundedId, AsrError, ModelManifestRef};
use sha2::{Digest, Sha256};

/// A model file whose bytes have been read and hash-verified against a
/// caller-declared digest. The ONLY way to obtain one — mirroring
/// `eg_audio::asr::AuthorizedCarrier`'s "the type does not exist without the
/// call succeeding" discipline.
#[derive(Debug)]
pub struct VerifiedModel {
    path: std::path::PathBuf,
    digest_hex: String,
}

impl VerifiedModel {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn digest_hex(&self) -> &str {
        &self.digest_hex
    }

    /// A [`ModelManifestRef`] a real worker can attach to segments/results.
    /// `signature_ref` is honestly labeled `local-digest-only`: this module
    /// verifies a SHA-256 content match, not a GOC-36 signer-chain proof — see
    /// the module doc.
    pub fn manifest_ref(&self, manifest_id: &str) -> Result<ModelManifestRef, AsrError> {
        // `AsrBoundedId::new` returns the `ingress` module's own `IngressError`
        // (see `eg_audio::asr`'s `pub use crate::ingress::BoundedId as
        // AsrBoundedId`), not `AsrError` — map explicitly rather than `?`.
        let bounded = |value: &str| {
            AsrBoundedId::new(value).map_err(|_| AsrError::MalformedRequest {
                reason: "manifest id is not a valid bounded identifier",
            })
        };
        Ok(ModelManifestRef {
            manifest_id: bounded(manifest_id)?,
            digest: self.digest_hex.clone(),
            signature_ref: bounded("local-digest-only")?,
        })
    }
}

/// Read `path`, hash it, and require the hash to equal `expected_sha256_hex`
/// (case-insensitive, 64 lowercase-normalized hex chars). Fails closed —
/// distinctly — on: missing file, unreadable file, empty file, malformed
/// expected-digest string, and digest mismatch. Never downloads anything.
pub fn verify_model(
    path: impl AsRef<Path>,
    expected_sha256_hex: &str,
) -> Result<VerifiedModel, AsrError> {
    let path = path.as_ref();
    let expected = expected_sha256_hex.trim().to_ascii_lowercase();
    if expected.len() != 64 || !expected.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(AsrError::ModelUnavailable {
            reason: "expected model digest is not a 64-char lowercase hex sha256",
        });
    }
    let bytes = std::fs::read(path).map_err(|_| AsrError::ModelUnavailable {
        reason: "model file is absent or unreadable",
    })?;
    if bytes.is_empty() {
        return Err(AsrError::ModelUnavailable {
            reason: "model file is empty",
        });
    }
    let actual = format!("{:x}", Sha256::digest(&bytes));
    if actual != expected {
        return Err(AsrError::ModelUnavailable {
            reason: "model file content digest does not match the declared digest",
        });
    }
    Ok(VerifiedModel {
        path: path.to_path_buf(),
        digest_hex: actual,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn rejects_missing_file() {
        let err = verify_model("/nonexistent/path/does-not-exist.bin", &"a".repeat(64))
            .expect_err("missing file must fail closed");
        assert_eq!(
            err,
            AsrError::ModelUnavailable {
                reason: "model file is absent or unreadable"
            }
        );
    }

    #[test]
    fn rejects_malformed_expected_digest() {
        let path = tempfile_with(b"not a real model").expect("temp file");
        let err = verify_model(&path, "short").expect_err("malformed digest must fail closed");
        assert_eq!(
            err,
            AsrError::ModelUnavailable {
                reason: "expected model digest is not a 64-char lowercase hex sha256"
            }
        );
    }

    #[test]
    fn rejects_digest_mismatch() {
        let path = tempfile_with(b"some model bytes for mismatch test").expect("temp file");
        let wrong_digest = "0".repeat(64);
        let err = verify_model(&path, &wrong_digest).expect_err("mismatch must fail closed");
        assert_eq!(
            err,
            AsrError::ModelUnavailable {
                reason: "model file content digest does not match the declared digest"
            }
        );
    }

    #[test]
    fn accepts_matching_digest() {
        let content = b"some model bytes for accepts test";
        let path = tempfile_with(content).expect("temp file");
        let digest = format!("{:x}", Sha256::digest(content));
        let verified = verify_model(&path, &digest).expect("matching digest verifies");
        assert_eq!(verified.digest_hex(), digest);
    }

    /// Minimal `std`-only temp file helper (no `tempfile` crate dependency).
    /// Writes+closes the file before returning, so the path is immediately
    /// safe for another reader (`verify_model`) with no race.
    fn tempfile_with(content: &[u8]) -> std::io::Result<std::path::PathBuf> {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "eg-asr-whisper-test-{}-{:x}",
            std::process::id(),
            Sha256::digest(content)
        ));
        let mut f = std::fs::File::create(&path)?;
        f.write_all(content)?;
        f.flush()?;
        Ok(path)
    }
}
