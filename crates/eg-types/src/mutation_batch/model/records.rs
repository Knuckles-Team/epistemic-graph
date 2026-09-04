use serde::{Deserialize, Serialize};

use super::{CommittedVersion, MutationBatch, MutationOutboxIntent, MutationScopeIdentity};

/// Durable terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationBatchStatus {
    Prepared,
    Committed,
    Aborted,
}

/// Durable status/result record used for retry reconciliation after restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBatchRecord {
    pub batch: MutationBatch,
    /// Self-checking envelope copy; it must exactly equal `batch.identity`.
    pub identity: MutationScopeIdentity,
    pub status: MutationBatchStatus,
    pub committed_version: CommittedVersion,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_msgpack: Option<Vec<u8>>,
    pub committed_at_ms: u64,
}

/// One durable outbox row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationOutboxRecord {
    pub schema_version: u16,
    pub batch_id: String,
    pub ordinal: u32,
    pub identity: MutationScopeIdentity,
    pub committed_version: CommittedVersion,
    pub intent: MutationOutboxIntent,
    pub created_at_ms: u64,
}

/// Durable lease over an outbox row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationOutboxLease {
    pub record: MutationOutboxRecord,
    pub consumer: String,
    pub lease_epoch: u64,
    pub lease_until_ms: u64,
    pub attempt: u32,
}

/// Monotonic projection watermark updated in the same transaction as outbox ack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationProjectionCursor {
    pub schema_version: u16,
    pub projection: String,
    pub identity: MutationScopeIdentity,
    pub batch_id: String,
    pub outbox_ordinal: u32,
    pub committed_version: CommittedVersion,
    pub advanced_at_ms: u64,
}

/// Result returned by a persistence commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBatchCommit {
    pub record: MutationBatchRecord,
    /// Self-checking envelope copy; it must equal both record identities.
    pub identity: MutationScopeIdentity,
    pub replayed: bool,
}

impl MutationBatchRecord {
    pub fn validate_identity(&self) -> Result<(), String> {
        self.batch.validate_identity()?;
        require_same_identity(
            &self.identity,
            &self.batch.identity,
            "mutation batch record identity does not match its batch",
        )
    }
}

impl MutationOutboxRecord {
    pub fn validate_identity(&self) -> Result<(), String> {
        self.identity.validate_digest()
    }
}

impl MutationProjectionCursor {
    pub fn validate_identity(&self) -> Result<(), String> {
        self.identity.validate_digest()
    }
}

impl MutationBatchCommit {
    pub fn validate_identity(&self) -> Result<(), String> {
        self.record.validate_identity()?;
        require_same_identity(
            &self.identity,
            &self.record.identity,
            "mutation batch commit identity does not match its record",
        )
    }
}

fn require_same_identity(
    envelope: &MutationScopeIdentity,
    nested: &MutationScopeIdentity,
    mismatch: &str,
) -> Result<(), String> {
    envelope.validate_digest()?;
    if envelope == nested {
        Ok(())
    } else {
        Err(mismatch.to_string())
    }
}
