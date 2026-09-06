//! The admitted mutation write: a storage-kernel write capability plus this
//! crate's own admission state machine.

use crate::admission::AdmissionState;
use eg_storage::{
    MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain, OwnerLayout, PhysicalWriteCapability,
};
use eg_types::{MutationBatch, MutationScopeIdentity};
use redb::WriteTransaction;
use std::cell::RefCell;
use std::marker::PhantomData;

/// One physical write transaction under one admitted mutation batch.
pub struct MutationWrite<'a, D: OwnerDomain> {
    capability: PhysicalWriteCapability<'a, D>,
    pub(crate) admission: RefCell<AdmissionState>,
}

impl<'a, D: OwnerDomain> MutationWrite<'a, D> {
    /// Mint the one write for this owner scope. The capability is the only way
    /// to reach a write transaction, and only the mutation authority can issue it.
    pub fn open(
        authority: &'a MutationOwnerAuthority,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        Ok(Self {
            capability: authority.write_capability(owner)?,
            admission: RefCell::new(AdmissionState::Idle),
        })
    }

    pub(crate) fn transaction(&self) -> &WriteTransaction {
        self.capability.transaction()
    }

    pub(crate) fn verify_scope(&self, identity: &MutationScopeIdentity) -> Result<(), String> {
        self.capability.verify_scope(identity)
    }

    pub(crate) fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        self.capability.authenticate_private(sealed, digest)
    }

    pub fn scope(&self) -> &MutationScopeIdentity {
        self.capability.scope()
    }

    pub fn commit(self) -> Result<(), String> {
        self.capability.commit()
    }

    pub fn abort(self) -> Result<(), String> {
        self.capability.abort()
    }

    /// Admit one owner-row operation inside an already admitted batch.
    pub fn begin_owner(
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
/// # use eg_mutation_store::AdmittedOwnerWrite;
/// # use eg_storage::KvOwner;
/// fn leaks_transaction(token: &AdmittedOwnerWrite<'_, KvOwner>) {
///     let _ = token.transaction();
/// }
/// ```
pub struct AdmittedOwnerWrite<'a, D: OwnerDomain> {
    pub(crate) write: &'a MutationWrite<'a, D>,
    pub(crate) identity: MutationScopeIdentity,
    finished: bool,
    _domain: PhantomData<D>,
}

impl<D: OwnerDomain> AdmittedOwnerWrite<'_, D> {
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
