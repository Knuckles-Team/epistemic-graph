//! The admitted mutation: a storage-kernel write capability plus this crate's
//! own admission state machine.
//!
//! One value of this type is one physical write transaction. The kernel mints
//! it, admits batches into it, and consumes it on commit; no other crate can
//! construct one, because only [`eg_storage::MutationOwnerAuthority`] can mint
//! the underlying capability.

use crate::admission::AdmissionState;
use eg_storage::{
    owner_table_names, LedgerRowScope, MutationClass, MutationOwnerAuthority, OwnedStoreHandle,
    OwnerDomain, OwnerLayout, OwnerReadTable, PhysicalWriteCapability,
};
use eg_types::{MutationBatch, MutationScopeIdentity};
use redb::{Table, TableDefinition, TableHandle};
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

    /// Open one declared, non-identity table of this owner file for writing.
    /// The storage kernel never lends out the raw transaction, so this is the
    /// only write path and the three physical-identity tables are unreachable.
    pub(crate) fn open_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Table<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        self.capability.open_table(definition)
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

    pub(crate) fn commit(self) -> Result<(), String> {
        self.capability.commit()
    }

    /// Discard every row written under this admitted write.
    pub fn abort(self) -> Result<(), String> {
        self.capability.abort()
    }

    /// Admit a further caller-originated batch inside this same write
    /// transaction, so a caller can order several batches under one commit.
    pub fn begin(&self, batch: &MutationBatch) -> Result<crate::Begin, String> {
        crate::commit::begin(self, batch, MutationClass::Operation)
    }

    /// Admit a further owner-maintenance batch in this same write transaction.
    pub fn begin_maintenance(&self, batch: &MutationBatch) -> Result<crate::Begin, String> {
        crate::commit::begin(self, batch, MutationClass::Maintenance)
    }

    /// Admit one owner-row operation inside an already admitted batch.
    pub fn owner_rows(
        &self,
        owner: &OwnedStoreHandle<D>,
        batch: &MutationBatch,
    ) -> Result<AdmittedOwnerWrite<'_, D>, String> {
        if owner.identity() != &batch.identity
            || owner.principal() != batch.context.principal
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
        let name = definition.name();
        if !owner_table_names(D::LAYOUT).contains(&name) {
            return Err("owner write may not open a table outside its layout".to_string());
        }
        self.write.open_table(definition)
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
