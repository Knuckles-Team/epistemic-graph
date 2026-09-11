use serde::{Deserialize, Serialize};

use super::{SemanticDigest, SemanticIndexError};

/// Digest-only notification that a committed SQL source may need semantic work.
///
/// The durable mutation outbox record supplies the exact committed source
/// version. Keeping OCC and attempt metadata out of this payload makes a
/// fresh-nonce retry byte-identical to the original operation while still
/// letting the consumer prove which committed revision triggered the scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticSourceDirtyIntent {
    /// Tenant and logical SQL mutation scope, excluding lifecycle generation.
    pub source_scope_digest: SemanticDigest,
    /// Digest of the canonical SQL method input; query text and row values are
    /// never copied into the notification.
    pub input_digest: SemanticDigest,
}

impl SemanticSourceDirtyIntent {
    pub fn new(source_scope_digest: SemanticDigest, input_digest: SemanticDigest) -> Self {
        Self {
            source_scope_digest,
            input_digest,
        }
    }

    /// Both fields are fixed-width SHA-256 values validated by their newtype.
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        Ok(())
    }
}
