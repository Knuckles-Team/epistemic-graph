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
    declared_table_names, MutationClass, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain,
    OwnerLayout, OwnerPayloadRetirement,
};
use eg_types::authority::{NonceReplayKeyV1, OperationReplayIdentityV1};
use eg_types::mutation::MutationReceiptV1;
use eg_types::mutation_batch::VersionExpectation;
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
        self.admit_class(owner, batch, MutationClass::Operation)
    }

    /// Admit one owner-maintenance write: compaction, retention, index
    /// initialization, a content-addressed definition insert.
    ///
    /// It is a full mutation -- ledgered, fenced, version-bumping, replayable
    /// by its idempotency key -- but it carries no caller identity, so it is
    /// outside operation-replay conflict semantics: [`Self::record_replay`]
    /// refuses to run inside one, and it can never consume an attempt nonce.
    /// Under RF-RULING-004 this is what an owner write with no caller identity
    /// must be, because an un-ledgered write path would be a second authority.
    pub fn admit_maintenance<'a, D: OwnerDomain>(
        &'a self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
    ) -> Result<(AdmittedMutation<'a, D>, Begin), String> {
        self.admit_class(owner, batch, MutationClass::Maintenance)
    }

    /// Open the one write for `owner`, resolve the scope's authoritative version
    /// INSIDE that exclusive transaction, and admit the batch `build` returns for
    /// it.
    ///
    /// The two-step alternative -- read the version from a fresh snapshot, build
    /// a batch carrying it as `VersionExpectation::Native`, then admit -- has a
    /// window between the read and the write lock in which any other writer may
    /// commit, and [`commit::begin`] then fails the batch closed with
    /// `STALE_VERSION`. That is correct for a caller-supplied expectation, which
    /// is a real OCC claim about state the caller observed. It is wrong for a
    /// write whose expectation is not a claim at all but merely "whatever the
    /// scope is at". redb serializes writers, so a version read while the write
    /// transaction is held cannot move underneath the batch built from it: this
    /// entry point closes the window rather than retrying around it.
    ///
    /// `build` receives that authoritative version and must return a batch whose
    /// `version_expectation` is `Native(version)`; anything else is refused,
    /// because it would reintroduce the claim this method exists to remove.
    pub fn admit_current<'a, D: OwnerDomain, F>(
        &'a self,
        owner: &OwnedStoreHandle<D>,
        class: MutationClass,
        build: F,
    ) -> Result<(AdmittedMutation<'a, D>, MutationBatch, Begin), String>
    where
        F: FnOnce(u64) -> Result<MutationBatch, String>,
    {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        let version = match crate::ledger::bound_scope_version(&write, owner.identity()) {
            Ok(version) => version,
            Err(error) => {
                write.abort()?;
                return Err(error);
            }
        };
        let batch = match build(version) {
            Ok(batch) => batch,
            Err(error) => {
                write.abort()?;
                return Err(error);
            }
        };
        if batch.version_expectation != VersionExpectation::Native(version) {
            write.abort()?;
            return Err(format!(
                "current-version admission requires Native({version}) but the batch expects {:?}",
                batch.version_expectation
            ));
        }
        match commit::begin(&write, &batch, class) {
            Ok(begun) => Ok((write, batch, begun)),
            Err(error) => {
                write.abort()?;
                Err(error)
            }
        }
    }

    fn admit_class<'a, D: OwnerDomain>(
        &'a self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
        class: MutationClass,
    ) -> Result<(AdmittedMutation<'a, D>, Begin), String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        let begun = commit::begin(&write, batch, class)?;
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
    ///
    /// Refused for a layout that owns tables: the kernel cannot sweep owner
    /// rows (their keys carry no scope component), and retiring the authority
    /// alone would leave the payload for the next binding of the same logical
    /// name. Such a layout uses [`Self::purge_scope_with`].
    pub fn purge_scope<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        identity: &MutationScopeIdentity,
    ) -> Result<(), String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        commit::purge_scope(&write, identity, None)?;
        write.commit()
    }

    /// Retire one generation's ledger authority **and** its domain payload in
    /// one transaction, the domain supplying the sweep of its own tables.
    pub fn purge_scope_with<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        identity: &MutationScopeIdentity,
        owner_payload: &dyn OwnerPayloadRetirement<D>,
    ) -> Result<(), String> {
        let write = AdmittedMutation::open(&self.authority, owner)?;
        commit::purge_scope(&write, identity, Some(owner_payload))?;
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

    /// Decide one proposed attempt against the durable replay ledger, **inside**
    /// the caller's admitted write.
    ///
    /// Resolution, the effect and [`Self::record_replay`] therefore share one
    /// physical transaction: no window exists in which two attempts can both
    /// resolve `Fresh` and both commit. A resolution taken in a transaction
    /// that is then aborted decides nothing, which is why this does not open
    /// its own.
    pub fn resolve_replay<D: OwnerDomain>(
        &self,
        write: &AdmittedMutation<'_, D>,
        operation: &OperationReplayIdentityV1,
        nonce: &NonceReplayKeyV1,
    ) -> Result<ReplayResolution, String> {
        resolve_replay_in(write, operation, nonce)
    }

    /// Open the one write for `owner` without admitting a batch, for a caller
    /// that must resolve replay before it knows whether a batch exists.
    pub fn open_write<'a, D: OwnerDomain>(
        &'a self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<AdmittedMutation<'a, D>, String> {
        AdmittedMutation::open(&self.authority, owner)
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
