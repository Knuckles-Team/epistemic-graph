//! The admitted mutation: a storage-kernel write capability plus this crate's
//! own admission state machine.
//!
//! One value of this type is one physical write transaction. The kernel mints
//! it, admits batches into it, and consumes it on commit; no other crate can
//! construct one, because only [`eg_storage::MutationOwnerAuthority`] can mint
//! the underlying capability.

use crate::admission::AdmissionState;
use eg_storage::{
    decode_ledger_record, BlobOwner, BlobSharedServiceHandle, BlobSharedWrite, LedgerRowScope,
    MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain, OwnerLayout, OwnerReadTable,
    OwnerRowScope, PhysicalWriteCapability, ScopedOwnerTableMut, ScopedTableMut,
};
use eg_types::{MutationBatch, MutationScopeIdentity};
use redb::{Table, TableDefinition};
use std::cell::RefCell;
use std::marker::PhantomData;

/// One physical write transaction under one or more admitted mutation batches.
pub struct AdmittedMutation<'a, D: OwnerDomain> {
    capability: PhysicalWriteCapability<'a, D>,
    pub(crate) admission: RefCell<AdmissionState>,
}

impl<'a, D: OwnerDomain> AdmittedMutation<'a, D> {
    /// Mint the one write for this owner scope. The capability is the only way
    /// to reach a write transaction, and only the mutation authority can issue it.
    pub(crate) fn open(
        authority: &'a MutationOwnerAuthority,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        Ok(Self {
            capability: authority.write_capability(owner)?,
            admission: RefCell::new(AdmissionState::Idle),
        })
    }

    /// Wrap one already-minted group-member capability as an admitted
    /// mutation.
    ///
    /// The member is an ordinary `AdmittedMutation` in every respect: it has
    /// its own admission state machine, its own scope, and its own ledger row
    /// ACL. Only the capability underneath differs — it shares the group's one
    /// transaction and cannot end it.
    pub(crate) fn from_group_member(capability: PhysicalWriteCapability<'a, D>) -> Self {
        Self {
            capability,
            admission: RefCell::new(AdmissionState::Idle),
        }
    }

    /// End the group's shared transaction from this, its last live member.
    pub(crate) fn end_group_transaction(self, commit: bool) -> Result<(), String> {
        self.capability.end_group_transaction(commit)
    }

    /// Open one ledger table bounded to this write's own serving scope.
    ///
    /// Every ledger read and write this crate performs goes through here, so a
    /// mutation admitted for one tenant cannot address — or delete — another's
    /// rows even though both live in the same physical table.
    pub(crate) fn scoped_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedTableMut<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: LedgerRowScope,
        V: redb::Value + 'static,
    {
        self.capability.scoped_table_mut(definition)
    }

    pub(crate) fn capability(&self) -> &PhysicalWriteCapability<'a, D> {
        &self.capability
    }

    /// Open one owner table of this domain's layout for **reading only**,
    /// inside this admitted write.
    ///
    /// Available before a batch exists, because a domain that content-addresses
    /// its batch from the rows it is about to change must read them in the same
    /// serialized transaction. Strictly weaker than an owner write: no mutation
    /// and no transaction is reachable through the returned view, and the
    /// ledger, the identity tables and every other layout's tables fail closed.
    pub fn open_read_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<OwnerReadTable<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.capability.open_owner_read(definition)
    }

    /// Remove every row of one ledger table belonging to this write's own
    /// serving scope. The scope is the capability's, never an argument.
    pub(crate) fn purge_scoped_rows<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<(), String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: LedgerRowScope,
        V: redb::Value + 'static,
    {
        self.capability.purge_scoped_rows(definition)
    }

    pub(crate) fn retire_scope_binding(&self) -> Result<(), String> {
        self.capability.retire_scope_binding()
    }

    pub(crate) fn verify_scope(&self, identity: &MutationScopeIdentity) -> Result<(), String> {
        self.capability.verify_scope(identity)
    }

    pub(crate) fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        self.capability.authenticate_private(sealed, digest)
    }

    /// The exact serving scope this admitted write was issued for.
    pub fn scope(&self) -> &MutationScopeIdentity {
        self.capability.scope()
    }

    /// Return the class of the batch this write actually admitted, as declared
    /// by its envelope.  The class is recovered from the encoded batch already
    /// held by the admission state; it is never supplied as a free admission
    /// argument.  Replay recording uses this structural check to keep owner
    /// maintenance outside operation-replay semantics.
    pub(crate) fn admitted_batch_is_maintenance(&self) -> Result<bool, String> {
        let state = self.admission.borrow();
        let encoded = match &*state {
            AdmissionState::Applying { batch, .. }
            | AdmissionState::Finished { batch }
            | AdmissionState::Replayed { batch, .. } => batch,
            AdmissionState::Poisoned => return Err("mutation write is poisoned".to_string()),
            AdmissionState::Idle => {
                return Err("mutation write has no admitted batch".to_string());
            }
        };
        let batch: MutationBatch = decode_ledger_record(encoded)?;
        Ok(batch.is_maintenance())
    }

    /// Return the exact batch id currently admitted in this physical write.
    ///
    /// Replay metadata is written by the same capability as the owner rows and
    /// terminal batch record. Reading the id from admission state prevents a
    /// caller from attaching a typed receipt to a fabricated or unrelated
    /// batch id.
    pub(crate) fn admitted_batch_id(&self) -> Result<String, String> {
        let state = self.admission.borrow();
        let encoded = match &*state {
            AdmissionState::Applying { batch, .. }
            | AdmissionState::Finished { batch }
            | AdmissionState::Replayed { batch, .. } => batch,
            AdmissionState::Poisoned => return Err("mutation write is poisoned".to_string()),
            AdmissionState::Idle => {
                return Err("mutation write has no admitted batch".to_string());
            }
        };
        let batch: MutationBatch = decode_ledger_record(encoded)?;
        if batch.batch_id.is_empty() {
            return Err("admitted mutation batch has no batch id".to_string());
        }
        Ok(batch.batch_id)
    }

    pub(crate) fn commit(self) -> Result<(), String> {
        self.capability.commit()
    }

    /// Discard every row written under this admitted write.
    pub fn abort(self) -> Result<(), String> {
        self.capability.abort()
    }

    /// Admit a further batch inside this same write transaction, so a caller
    /// can order several operation or maintenance envelopes under one commit.
    pub fn begin(&self, batch: &MutationBatch) -> Result<crate::Begin, String> {
        crate::commit::begin(self, batch)
    }

    /// Admit an operation after its stable replay identity was reconstructed
    /// from retained domain rows.
    pub fn begin_with_replay_identity(
        &self,
        batch: &MutationBatch,
        operation: &eg_types::authority::OperationReplayIdentity,
        nonce: &eg_types::authority::NonceReplayKey,
    ) -> Result<crate::Begin, String> {
        crate::commit::begin_with_replay_identity(self, batch, operation, nonce)
    }

    /// Admit a kernel-owned graft marker or destination reservation.  These
    /// batches use the ordinary maintenance ledger format but their namespace
    /// is reserved against public callers forging graft provenance.
    pub(crate) fn begin_graft(&self, batch: &MutationBatch) -> Result<crate::Begin, String> {
        crate::commit::begin_graft(self, batch)
    }

    /// Admit one owner-row operation inside an already admitted batch.
    pub fn owner_rows(
        &self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
    ) -> Result<AdmittedOwnerWrite<'_, D>, String> {
        if owner.identity() != &batch.identity
            || owner.principal() != batch.serving_principal()
            || self.capability.scope() != &batch.identity
        {
            return Err("owner write capability does not match admitted batch".to_string());
        }
        self.verify_scope(&batch.identity)?;
        self.open_owner_admission(batch, D::LAYOUT)?;
        Ok(AdmittedOwnerWrite {
            write: self,
            identity: batch.identity.clone(),
            finished: false,
            _domain: PhantomData,
        })
    }
}

impl<'a> AdmittedMutation<'a, BlobOwner> {
    /// Mint the blob layout's independent shared-service write over **this**
    /// admitted mutation's transaction.
    ///
    /// `cas_chunks`/`cas_refcount` are `TableScope::SharedService`, not owner
    /// rows, so they are authorised by the blob service's own handle rather
    /// than by this write's serving scope. They still have to land in this
    /// transaction: `redb` permits one writer, this mutation is holding it, and
    /// a chunk row whose refcount lands in a different transaction is a row
    /// that a failed batch orphans. The returned capability therefore borrows
    /// this write and commits or aborts with it.
    pub fn blob_shared_write(
        &'a self,
        owner: &BlobSharedServiceHandle,
        principal: &str,
    ) -> Result<BlobSharedWrite<'a>, String> {
        self.capability.blob_shared_write(owner, principal)
    }
}

/// Consuming owner-write gate. Dropping it unfinished poisons the outer write.
///
/// ```compile_fail
/// # use eg_transaction::AdmittedOwnerWrite;
/// # use eg_storage::KvOwner;
/// fn leaks_rows(token: &AdmittedOwnerWrite<'_, KvOwner>) {
///     let _ = token.write.scope();
/// }
/// ```
pub struct AdmittedOwnerWrite<'a, D: OwnerDomain> {
    pub(crate) write: &'a AdmittedMutation<'a, D>,
    pub(crate) identity: MutationScopeIdentity,
    finished: bool,
    _domain: PhantomData<D>,
}

impl<D: OwnerDomain> AdmittedOwnerWrite<'_, D> {
    /// Retire the ledger of the generation this scope is REPLACING, inside this
    /// same admitted write.
    ///
    /// The domain-facing name for
    /// [`crate::commit::purge_scope_ledger_generation`] -- see it for why a
    /// same-name recreate would otherwise inherit the deleted generation's
    /// replay keys, nonces, receipts and outbox rows, and why the scope VERSION
    /// is deliberately left monotonic. The sweep is performed BY THE KERNEL; a
    /// domain never opens a ledger table.
    pub fn purge_replaced_generation_ledger(&self) -> Result<(), String> {
        crate::commit::purge_scope_ledger_generation(self.write, &self.identity)
    }

    /// Open one owner table of **this** domain's layout for writing.
    ///
    /// This is the only owner-row write path a domain crate has. The name must
    /// be one of `owner_table_names(D::LAYOUT)`, so the ledger, the three
    /// physical-identity tables and every other layout's tables stay
    /// unreachable, and the write is only possible between `owner_rows` and
    /// `finish_owner` -- i.e. inside an admitted mutation.
    pub fn open_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Table<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.write.capability().open_owner_write(definition)
    }

    /// Open one **scope-prefixed** owner table of this layout, bounded to this
    /// write's own serving scope.
    ///
    /// A layout whose owner tables lead their key with the scope's name — a
    /// graph shard, where 42 of 53 tables do — makes those tables unreachable
    /// through [`Self::open_table`], because a raw `redb::Table` cannot carry a
    /// row bound. This is the accessor for them, and its scope name comes from
    /// the capability rather than from an argument, so one graph cannot write
    /// another's rows in the file they share.
    pub fn open_scoped_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedOwnerTableMut<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: OwnerRowScope,
        V: redb::Value + 'static,
    {
        self.write.capability().scoped_owner_table_mut(definition)
    }

    pub fn finish_owner(mut self) -> Result<(), String> {
        self.write.finish_owner_admission(D::LAYOUT)?;
        self.finished = true;
        Ok(())
    }

    pub fn identity(&self) -> &MutationScopeIdentity {
        &self.identity
    }
}

impl<'a> AdmittedOwnerWrite<'a, BlobOwner> {
    /// The shared-service write, reached from inside an open owner-row
    /// admission so a batch closure that writes `cas_blobs`/`cas_uploads` can
    /// write the chunk and refcount rows of the same batch without leaving the
    /// transaction. Delegates to
    /// [`AdmittedMutation::blob_shared_write`]; it adds no authority of its own.
    pub fn blob_shared_write(
        &self,
        owner: &BlobSharedServiceHandle,
        principal: &str,
    ) -> Result<BlobSharedWrite<'a>, String> {
        self.write.blob_shared_write(owner, principal)
    }
}

impl<D: OwnerDomain> Drop for AdmittedOwnerWrite<'_, D> {
    fn drop(&mut self) {
        if !self.finished {
            self.write.poison_owner_admission();
        }
    }
}

pub(crate) fn is_ledger_only(layout: OwnerLayout) -> bool {
    layout == OwnerLayout::LedgerOnly
}
