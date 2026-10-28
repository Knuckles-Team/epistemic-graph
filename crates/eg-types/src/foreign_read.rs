//! Typed, request/response foreign-read lifecycle (EG-FEDERATED-QUERY-R722.3).
//!
//! The engine transport remains one authenticated request and one response.  A
//! host therefore opens a bounded execution, appends pages with monotone
//! sequence numbers, and closes it before sending the correlated UQL or
//! `UnifiedQuery`.  These DTOs carry no credentials and no source URL; the
//! server re-reads the approval and catalog before it binds its page source.

use crate::connector_pack::ConnectorPackSourceApprovalResult;
use crate::contract::Digest256;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const FOREIGN_READ_SCHEMA_VERSION: u16 = 1;

/// The finite limits accepted for one host-driven foreign read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadLimits {
    pub max_rows: u32,
    pub max_bytes: u64,
    pub chunk_rows: u32,
    pub chunk_bytes: u32,
    pub deadline_ms: u64,
}

impl ForeignReadLimits {
    pub const MAX_ROWS: u32 = 250_000;
    pub const MAX_BYTES: u64 = 64 * 1024 * 1024;
    pub const MAX_CHUNK_ROWS: u32 = 16_384;
    pub const MAX_CHUNK_BYTES: u32 = 4 * 1024 * 1024;
    pub const MAX_DEADLINE_MS: u64 = 60_000;

    pub fn validate(self) -> Result<(), String> {
        if self.max_rows == 0 || self.max_rows > Self::MAX_ROWS {
            return Err(format!(
                "INVALID_ARGUMENT: foreign read max_rows must be 1..={}",
                Self::MAX_ROWS
            ));
        }
        if self.max_bytes == 0 || self.max_bytes > Self::MAX_BYTES {
            return Err(format!(
                "INVALID_ARGUMENT: foreign read max_bytes must be 1..={}",
                Self::MAX_BYTES
            ));
        }
        if self.chunk_rows == 0 || self.chunk_rows > Self::MAX_CHUNK_ROWS {
            return Err(format!(
                "INVALID_ARGUMENT: foreign read chunk_rows must be 1..={}",
                Self::MAX_CHUNK_ROWS
            ));
        }
        if self.chunk_bytes == 0 || u64::from(self.chunk_bytes) > u64::from(Self::MAX_CHUNK_BYTES) {
            return Err(format!(
                "INVALID_ARGUMENT: foreign read chunk_bytes must be 1..={}",
                Self::MAX_CHUNK_BYTES
            ));
        }
        if self.deadline_ms == 0 || self.deadline_ms > Self::MAX_DEADLINE_MS {
            return Err(format!(
                "INVALID_ARGUMENT: foreign read deadline_ms must be 1..={}",
                Self::MAX_DEADLINE_MS
            ));
        }
        Ok(())
    }
}

/// Server-issued opaque execution handle plus the query digest it is allowed to
/// answer.  The digest is recomputed from the correlated query by the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadExecutionRef {
    pub execution_id: u64,
    pub query_digest: Digest256,
}

/// The source identity pinned by the admitted pack head for one execution.
///
/// `source_ref` is the current registration's resource URI.  The remaining
/// fields are derived from the authoritative approval receipt: the pack head
/// revision is the source revision, the approved contract digest is the schema
/// digest, and the pack digest is the snapshot reference.  A host may echo this
/// value on each chunk, but it cannot choose or alter it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadSourceIdentity {
    pub source_ref: String,
    pub source_revision: u64,
    pub schema_digest: Digest256,
    pub snapshot_ref: Digest256,
}

impl ForeignReadSourceIdentity {
    pub fn from_approval(approval: &ConnectorPackSourceApprovalResult) -> Result<Self, String> {
        let source_ref = approval.registration.resource_uri.clone();
        if source_ref.is_empty() {
            return Err("SOURCE_BINDING_INVALID: source resource URI is empty".into());
        }
        Ok(Self {
            source_ref,
            source_revision: approval.head.binding_revision,
            schema_digest: approval.contract_digest,
            snapshot_ref: approval.head.pack_digest,
        })
    }
}

/// A bounded row supplied by the already-authorized host adapter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadRow {
    pub id: String,
    #[serde(default)]
    pub score: Option<f32>,
    /// Mapping-approved connector cells in the existing `ForeignRows` value
    /// currency. The SDK receiver owns response-to-mapping validation; EG
    /// bounds and preserves the cells for the existing residual/projection
    /// path instead of introducing another property representation.
    #[serde(default)]
    pub columns: BTreeMap<String, serde_json::Value>,
    /// Source references are retained for the lifecycle boundary and are exposed
    /// through the existing column-capable projection when that path is used.
    /// The ordinary RowSet executor still consumes `(id, score)`; this DTO does
    /// not claim the complete R722.5 served-answer proof by itself.
    #[serde(default)]
    pub provenance: Vec<String>,
}

/// Open one approved source execution.  `approval` is only a candidate receipt;
/// the server re-reads its durable current head/member before accepting it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadOpenRequest {
    pub schema_version: u16,
    pub source_name: String,
    pub approval: ConnectorPackSourceApprovalResult,
    pub plan_digest: Digest256,
    pub query_digest: Digest256,
    pub limits: ForeignReadLimits,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadOpenResult {
    pub schema_version: u16,
    pub execution: ForeignReadExecutionRef,
    pub expires_at_ms: u64,
    pub next_sequence: u64,
    pub source_identity: ForeignReadSourceIdentity,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadChunkRequest {
    pub execution: ForeignReadExecutionRef,
    pub sequence: u64,
    pub source_identity: ForeignReadSourceIdentity,
    pub rows: Vec<ForeignReadRow>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadChunkResult {
    pub execution: ForeignReadExecutionRef,
    pub sequence: u64,
    pub accepted_rows: u32,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub next_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadCompleteRequest {
    pub execution: ForeignReadExecutionRef,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadCompleteResult {
    pub execution: ForeignReadExecutionRef,
    pub sequence: u64,
    pub total_rows: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadAbortRequest {
    pub execution: ForeignReadExecutionRef,
    pub sequence: u64,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignReadAbortResult {
    pub execution: ForeignReadExecutionRef,
    pub sequence: u64,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ForeignReadLimits {
        ForeignReadLimits {
            max_rows: 10,
            max_bytes: 1024,
            chunk_rows: 2,
            chunk_bytes: 512,
            deadline_ms: 1_000,
        }
    }

    #[test]
    fn bounded_limits_reject_zero_and_over_cap() {
        assert!(limits().validate().is_ok());
        let mut zero = limits();
        zero.max_rows = 0;
        assert!(zero.validate().is_err());
        let mut wide = limits();
        wide.max_bytes = ForeignReadLimits::MAX_BYTES + 1;
        assert!(wide.validate().is_err());
    }

    #[test]
    fn lifecycle_dtos_round_trip_with_digest_and_optional_provenance() {
        let digest = Digest256::sha256(b"foreign-read-query");
        let execution = ForeignReadExecutionRef {
            execution_id: 7,
            query_digest: digest,
        };
        let request = ForeignReadChunkRequest {
            execution,
            sequence: 0,
            source_identity: ForeignReadSourceIdentity {
                source_ref: "mcp://connector/read".into(),
                source_revision: 1,
                schema_digest: Digest256::sha256(b"contract"),
                snapshot_ref: Digest256::sha256(b"pack"),
            },
            rows: vec![ForeignReadRow {
                id: "row-1".into(),
                score: Some(0.5),
                columns: BTreeMap::from([(
                    "title".into(),
                    serde_json::Value::String("Example".into()),
                )]),
                provenance: vec!["gitlab:issue:1".into()],
            }],
        };
        let bytes = rmp_serde::to_vec_named(&request).expect("encode");
        let decoded: ForeignReadChunkRequest = rmp_serde::from_slice(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }
}
