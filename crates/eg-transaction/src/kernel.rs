//! `MutationKernelV1` -- the sole mutation authority.
//!
//! It depends inward on [`eg_storage::StorageKernelV1`] and holds the single
//! move-once [`MutationOwnerAuthority`] that kernel issued. It alone admits,
//! orders, fences, commits, replays and emits durable mutation and outbox rows,
//! and it does so only through storage-issued capabilities. `eg-storage` never
//! imports this crate, and neither kernel imports a domain consumer.

use crate::admitted::AdmittedMutation;
use crate::replay::{record_replay_in, resolve_replay_in, ReplayResolution};
use crate::tables::ledger_table_names;
use crate::{commit, saga, Begin, SagaBegin};
use eg_storage::{
    declared_table_names, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain, OwnerLayout,
};
use eg_types::authority::{NonceReplayKeyV1, OperationReplayIdentityV1};
use eg_types::mutation::MutationReceiptV1;
use eg_types::{MutationBatch, MutationBatchRecord, MutationScopeIdentity};
use std::collections::BTreeSet;

/// The sole mutation owner over one physical owner file.
pub struct MutationKernelV1 {
    authority: MutationOwnerAuthority,
}

impl MutationKernelV1 {
    /// Take ownership of the one physical write authority. The token is not
    /// `Clone` and has no public constructor, so exactly one mutation kernel
    /// can exist per storage kernel.
    pub fn new(authority: MutationOwnerAuthority) -> Self {
        Self { authority }
    }

    #[cfg(test)]
    pub(crate) fn authority(&self) -> &MutationOwnerAuthority {
        &self.authority
    }

    /// Prove this owner file is ledger-ready before any batch is admitted.
    ///
    /// The storage kernel creates the whole declared table census atomically
    /// when the file is created, so this cannot partially create tables. What
    /// it does is fail closed when the file's declared ledger census differs
    /// from the tables this crate reads and writes -- the one seam where the
    /// storage/ledger table-ownership split could silently diverge -- and open
    /// every one of them once under a real write capability.
    pub fn bootstrap_ledger<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<(), String> {
        let declared = declared_table_names(OwnerLayout::LedgerOnly)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mine = ledger_table_names().into_iter().collect::<BTreeSet<_>>();
        if !mine.is_subset(&declared) {
            return Err("mutation ledger census is not declared by the storage kernel".to_string());
        }
        let write = AdmittedMutation::open(&self.authority, owner)?;
        commit::open_ledger_tables(&write)?;
        write.commit()
    }

    /// Open the one write transaction for `owner` and admit `batch` into it.
    ///
    /// Returns [`Begin::Replay`] when the batch's idempotency key already names
    /// a terminally committed receipt; the caller must then abort the returned
    /// write rather than reapplying.
    pub fn admit<'a, D: OwnerDomain>(
        &'a self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
    ) -> Result<(AdmittedMutation<'a, D>, Begin), String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        let begun = commit::begin(&write, batch)?;
        Ok((write, begun))
    }

    /// Persist terminal metadata for one admitted batch, without committing.
    pub fn finish<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        batch: &MutationBatch,
        result_msgpack: Option<Vec<u8>>,
        committed_at_ms: u64,
        source_version: Option<u64>,
    ) -> Result<MutationBatchRecord, String> {
        commit::finish(write, batch, result_msgpack, committed_at_ms, source_version)
    }

    /// Commit one admitted write, consuming it.
    pub fn commit<D: OwnerDomain>(
        &self,
        write: AdmittedMutation<'_, D>,
        batch: &MutationBatch,
    ) -> Result<(), String> {
        commit::commit(write, batch)
    }

    /// Atomically remove authority for one exact logical generation.
    pub fn purge_scope<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        identity: &MutationScopeIdentity,
    ) -> Result<(), String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        commit::purge_scope(&write, identity)?;
        write.commit()
    }

    /// Prepare one saga: durable `Prepared` receipt, no owner rows yet.
    pub fn saga_begin<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
        prepared_at_ms: u64,
    ) -> Result<SagaBegin, String> {
        saga::prepare_saga(&self.authority, owner, batch, prepared_at_ms)
    }

    /// Prepare one saga together with its sealed private recovery payload.
    pub fn saga_step<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
        prepared_at_ms: u64,
        private_payload: Option<&[u8]>,
    ) -> Result<SagaBegin, String> {
        saga::prepare_saga_with_private_payload(
            &self.authority,
            owner,
            batch,
            prepared_at_ms,
            private_payload,
        )
    }

    /// Terminalize one prepared saga. The `bool` is `true` when the saga was
    /// already committed and this call only replayed its receipt.
    pub fn saga_end<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
        result_msgpack: Vec<u8>,
        committed_at_ms: u64,
    ) -> Result<(MutationBatchRecord, bool), String> {
        saga::commit_saga(
            &self.authority,
            owner,
            batch,
            result_msgpack,
            committed_at_ms,
        )
    }

    /// Decide one proposed attempt against the durable replay ledger. Never
    /// mutates: the resolving transaction is aborted before returning.
    pub fn resolve_replay<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        operation: &OperationReplayIdentityV1,
        nonce: &NonceReplayKeyV1,
    ) -> Result<ReplayResolution, String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        let resolution = resolve_replay_in(&write, operation, nonce);
        write.abort()?;
        resolution
    }

    /// Consume one attempt nonce and record its receipt inside `write`, so the
    /// effect and its replay evidence commit atomically.
    pub fn record_replay<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        operation: &OperationReplayIdentityV1,
        nonce: &NonceReplayKeyV1,
        receipt: &MutationReceiptV1,
    ) -> Result<(), String> {
        record_replay_in(write, operation, nonce, receipt)
    }
}
