//! Wire result DTOs for semantic-index operations.

use serde::{Deserialize, Serialize};

use crate::mutation_outbox::OutboxConsumerStatus;
use crate::semantic_index::SemanticDigest;

/// Durable receipt returned by the semantic-index owner for a committed mutation.
/// The runtime type lives above `eg-types`; this wire mirror keeps the contract at
/// the bottom of the crate DAG while retaining the exact serialized fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticMutationReceipt {
    pub batch_id: String,
    pub mutation_digest: SemanticDigest,
    pub source_version: u64,
    pub target_version: u64,
    pub replayed: bool,
}

/// Bounded continuation returned by one semantic source-page admission turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticSqlSourcePageAdmission {
    pub receipts: Vec<SemanticMutationReceipt>,
    pub next_cursor: Option<Vec<u8>>,
    pub complete: bool,
}

/// Bounded continuation returned by one semantic source-reconciliation turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticSqlSourceReconciliationAdmission {
    pub receipts: Vec<SemanticMutationReceipt>,
    pub source_revision: String,
    pub page_count: usize,
    pub complete: bool,
    pub wakeup_consumed: bool,
    pub rows_seen: u64,
    pub source_bytes_seen: u64,
}

/// Wire view of the outbox status returned by the semantic stage queue. The
/// fields every outbox-status view shares live in
/// [`OutboxConsumerStatus`]; `consecutive_claims`/`total_claims` are this
/// queue's own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticOutboxStatus {
    pub status: OutboxConsumerStatus,
    pub consecutive_claims: u32,
    pub total_claims: u64,
}
