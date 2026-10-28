//! Code enrichment for indexed repositories (EG-REPO-INGEST-R600..R603).
//!
//! Pure, typed transformations over the parser's structural facts:
//!
//! * [`code`] — code, test and feature entities, rung-ranked edges, design
//!   pattern hits and test-quality verdicts (R600);
//! * [`history`] — commits, authors, per-file changes, churn and change
//!   coupling (R601);
//! * [`links`] — infrastructure resources and HTTP routes linked to their
//!   owning service, features resolved to business capabilities (R602);
//! * [`fingerprint`] — content and structure hashes that keep incremental
//!   re-indexing to real structural change (R603).
//!
//! Every identifier is validated at construction, and every refusal carries a
//! stable code.

use serde::{Deserialize, Serialize};

pub mod code;
pub mod fingerprint;
pub mod history;
pub mod links;

/// The longest identifier any enrichment type accepts.
pub const MAX_ID_BYTES: usize = 512;

/// A refused enrichment input, with its stable code.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrichError {
    pub code: String,
    pub detail: String,
}

impl EnrichError {
    pub fn new(code: &str, detail: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for EnrichError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for EnrichError {}

/// The stable code of an identifier that fails validation.
pub const INVALID_IDENTIFIER: &str = "INVALID_IDENTIFIER";

/// A validated identifier: non-empty, at most [`MAX_ID_BYTES`], no whitespace
/// or control characters.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Id(String);

impl Id {
    pub fn new(value: impl Into<String>) -> Result<Self, EnrichError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_ID_BYTES
            || value.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(EnrichError::new(
                INVALID_IDENTIFIER,
                format!("identifier {value:?} must be 1..={MAX_ID_BYTES} bytes with no whitespace"),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Id {
    type Error = EnrichError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Id> for String {
    fn from(id: Id) -> Self {
        id.0
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Hex of the SHA-256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-REPO-INGEST-R600.1
    #[test]
    fn identifiers_are_validated_at_construction_and_on_the_wire() {
        assert!(Id::new("src/lib.rs::main").is_ok());
        for bad in ["", "has space", "tab\there", &"x".repeat(MAX_ID_BYTES + 1)] {
            assert_eq!(Id::new(bad).unwrap_err().code, INVALID_IDENTIFIER);
        }
        assert!(serde_json::from_str::<Id>("\"a b\"").is_err());
    }
}
