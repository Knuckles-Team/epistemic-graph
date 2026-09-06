//! Sole mutation authority for the epistemic-graph workspace.
//!
//! Every durable open, physical identity, owner-table declaration, scope
//! binding, snapshot and recovery path belongs to `eg_storage`. This crate owns
//! only the mutation ledger on top of it: admission, ordering, fencing, commit,
//! replay identity, saga preparation, outbox rows, and the durable
//! consensus-transaction intent records. It cannot open a database, and it can
//! write only through an [`eg_storage::PhysicalWriteCapability`] minted by the
//! single [`eg_storage::MutationOwnerAuthority`].

use eg_types::MutationBatchRecord;

/// Result of validating a proposed batch within its owner transaction.
#[derive(Debug, Clone)]
pub enum Begin {
    Apply { source_version: Option<u64> },
    Replay(Box<MutationBatchRecord>),
}

#[derive(Debug, Clone)]
pub enum SagaBegin {
    Execute,
    Resume(MutationBatchRecord),
    Committed(MutationBatchRecord),
}

mod admission;
mod admitted;
mod commit;
mod group;
mod kernel;
mod ledger;
mod maintenance;
mod participant;
mod read;
mod replay;
mod saga;
mod tables;

pub use admitted::{AdmittedMutation, AdmittedOwnerWrite};
pub use group::{AdmittedGroup, ScopedIntent};
pub use kernel::MutationKernelV1;
pub use maintenance::MaintenanceBatch;
pub use participant::{
    decode_record, decode_record_key, encode_record, encode_record_key, is_sealed_payload,
    sealed_blob_digest, validate_key_matches_record, validate_transition,
    ConsensusParentOperationKind, ConsensusTransactionBinding, ConsensusTransactionCasOutcome,
    ConsensusTransactionDecision, ConsensusTransactionGcOutcome, ConsensusTransactionGcRequest,
    ConsensusTransactionGroupMeta, ConsensusTransactionParentState,
    ConsensusTransactionParticipantState, ConsensusTransactionPendingFinalization,
    ConsensusTransactionRawSnapshot, ConsensusTransactionRecord, ConsensusTransactionRecordKey,
    ConsensusTransactionRetentionFence, ConsensusTransactionScanCursor,
    ConsensusTransactionScanPage, SealedPayloadOpener, CONSENSUS_TRANSACTION_SCHEMA_VERSION,
    MAX_CONSENSUS_GROUP_BYTES, MAX_CONSENSUS_RECORDS_PER_GROUP, MAX_CONSENSUS_RECORD_BYTES,
    MAX_COORDINATOR_ID_BYTES, MAX_ENCODED_RECORD_KEY_BYTES,
};
pub use read::{
    read_batches, read_class, read_fences, read_ledger, read_outbox, read_private_payload, version,
    OutboxCursor,
};
pub use replay::ReplayResolution;

#[cfg(test)]
mod tests;
