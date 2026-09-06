//! Current-only durable records for consensus transactions.
//!
//! Transplanted verbatim from the group-routed consensus-transaction leaf. The
//! only adaptations are the crate move itself: the record model's sealed-blob
//! framing predicate and its authentication seam are now the crate-local
//! [`sealed`] abstraction instead of the root binary's concrete `ValueCipher`,
//! so this kernel stays vendor-neutral and the composition root injects the
//! cipher it already owns.

mod key_codec;
mod model;
mod record_codec;
mod sealed;
mod transition;
mod validate;

pub use key_codec::{decode_record_key, encode_record_key};
pub use model::{
    ConsensusParentOperationKind, ConsensusTransactionBinding, ConsensusTransactionCasOutcome,
    ConsensusTransactionDecision, ConsensusTransactionGcOutcome, ConsensusTransactionGcRequest,
    ConsensusTransactionGroupMeta, ConsensusTransactionParentState,
    ConsensusTransactionParticipantState, ConsensusTransactionPendingFinalization,
    ConsensusTransactionRawSnapshot, ConsensusTransactionRecord, ConsensusTransactionRecordKey,
    ConsensusTransactionRetentionFence, ConsensusTransactionScanCursor,
    ConsensusTransactionScanPage,
};
pub use record_codec::{decode_record, encode_record, sealed_blob_digest};
pub use sealed::{is_sealed_payload, SealedPayloadOpener};
pub use transition::{validate_key_matches_record, validate_transition};

pub const CONSENSUS_TRANSACTION_SCHEMA_VERSION: u16 = 1;
pub const MAX_COORDINATOR_ID_BYTES: usize = 512;
pub const MAX_ENCODED_RECORD_KEY_BYTES: usize = MAX_COORDINATOR_ID_BYTES * 2 + 19;
pub const MAX_CONSENSUS_RECORD_BYTES: usize = 96 * 1024 * 1024;
pub const MAX_CONSENSUS_GROUP_BYTES: usize = 192 * 1024 * 1024;
pub const MAX_CONSENSUS_RECORDS_PER_GROUP: usize = 65_536;
pub(super) const MAX_CONSENSUS_RECORD_ITEMS: usize = 4_000_000;

#[cfg(test)]
mod tests;
