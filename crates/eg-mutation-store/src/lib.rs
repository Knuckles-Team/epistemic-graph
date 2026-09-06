//! Atomic mutation-ledger persistence over storage-kernel capabilities.
//!
//! Every durable open, physical identity, owner-table declaration, scope
//! binding, snapshot and recovery path belongs to `eg_storage`. This crate owns
//! only the mutation ledger on top of it: admission, ordering, fencing, commit,
//! replay identity, saga preparation, and outbox rows. It cannot open a
//! database, and it can write only through a
//! [`eg_storage::PhysicalWriteCapability`] minted by the single
//! [`eg_storage::MutationOwnerAuthority`].

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

#[path = "store/ledger_tables.rs"]
mod ledger_tables;

#[path = "store/write.rs"]
mod write;
pub use write::{AdmittedOwnerWrite, MutationWrite};

#[path = "store/admission.rs"]
mod admission;

#[path = "store/ledger.rs"]
mod ledger;

#[path = "store/apply.rs"]
mod apply;
pub use apply::{begin, commit, finish, purge_scope};

#[path = "store/saga.rs"]
mod saga;
pub use saga::{commit_saga, prepare_saga, prepare_saga_with_private_payload};

#[path = "store/read.rs"]
mod read;
pub use read::{read_outbox, read_private_payload, read_record, version};

#[cfg(test)]
#[path = "store/tests.rs"]
mod tests;
