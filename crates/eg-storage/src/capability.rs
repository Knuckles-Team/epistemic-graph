//! Scoped capabilities the kernel issues over one physical owner file.
//!
//! A capability is the only way any code outside this crate reaches a redb
//! transaction. Read and snapshot capabilities need an [`OwnedStoreHandle`];
//! the write capability additionally needs the single, move-once
//! [`crate::MutationOwnerAuthority`] token.

use crate::owner::domain::OwnerDomain;
use crate::owner::handle::OwnedStoreHandle;
use crate::physical::binding::{binding_for_read, binding_for_write};
use crate::physical::root::PhysicalStore;
use crate::recovery::evidence::{strict_snapshot_read, StrictRecoveryEvidence};
use eg_types::MutationScopeIdentity;
use redb::{ReadTransaction, WriteTransaction};
use std::marker::PhantomData;

/// The one mintable physical write authority for one owner file.
pub struct PhysicalWriteCapability<'a, D: OwnerDomain> {
    transaction: WriteTransaction,
    store: &'a PhysicalStore,
    identity: MutationScopeIdentity,
    principal: String,
    _domain: PhantomData<D>,
}

impl<'a, D: OwnerDomain> PhysicalWriteCapability<'a, D> {
    pub(crate) fn open(
        store: &'a PhysicalStore,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        let manifest = store.manifest();
        if manifest.layout != D::LAYOUT
            || manifest.authority_digest(store.incarnation()) != *owner.authority_digest()
        {
            return Err("owner write capability does not match this store".to_string());
        }
        let transaction = store.begin_write()?;
        binding_for_write(store, &transaction, owner.identity())?;
        Ok(Self {
            transaction,
            store,
            identity: owner.identity().clone(),
            principal: owner.principal().to_string(),
            _domain: PhantomData,
        })
    }

    /// The one physical write transaction of this capability.
    pub fn transaction(&self) -> &WriteTransaction {
        &self.transaction
    }

    /// The exact serving scope this capability was issued for.
    pub fn scope(&self) -> &MutationScopeIdentity {
        &self.identity
    }

    /// The authenticated principal this capability was issued to.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Reprove that one logical scope is bound to this store under the current
    /// manifest and declared table census, inside this write transaction.
    pub fn verify_scope(&self, identity: &MutationScopeIdentity) -> Result<(), String> {
        binding_for_write(self.store, &self.transaction, identity).map(|_| ())
    }

    pub fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        self.store.authenticate_private(sealed, digest)
    }

    pub fn commit(self) -> Result<(), String> {
        self.transaction.commit().map_err(|error| error.to_string())
    }

    pub fn abort(self) -> Result<(), String> {
        self.transaction.abort().map_err(|error| error.to_string())
    }
}

/// A scoped read over one owner file, bound to one authenticated serving scope.
pub struct ScopedRead<'a, D: OwnerDomain> {
    transaction: ReadTransaction,
    store: &'a PhysicalStore,
    identity: MutationScopeIdentity,
    _domain: PhantomData<D>,
}

impl<'a, D: OwnerDomain> ScopedRead<'a, D> {
    pub(crate) fn open(
        store: &'a PhysicalStore,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        let manifest = store.manifest();
        if manifest.layout != D::LAYOUT
            || manifest.authority_digest(store.incarnation()) != *owner.authority_digest()
        {
            return Err("owner read capability does not match this store".to_string());
        }
        let transaction = store.begin_read()?;
        binding_for_read(store, &transaction, owner.identity())?;
        Ok(Self {
            transaction,
            store,
            identity: owner.identity().clone(),
            _domain: PhantomData,
        })
    }

    pub fn transaction(&self) -> &ReadTransaction {
        &self.transaction
    }

    pub fn scope(&self) -> &MutationScopeIdentity {
        &self.identity
    }

    pub fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        self.store.authenticate_private(sealed, digest)
    }
}

/// A complete owner-scoped physical snapshot: per-table row counts and
/// fingerprints over the whole declared census.
pub struct ScopedSnapshot<D: OwnerDomain> {
    evidence: StrictRecoveryEvidence,
    identity: MutationScopeIdentity,
    _domain: PhantomData<D>,
}

impl<D: OwnerDomain> ScopedSnapshot<D> {
    pub(crate) fn capture(
        store: &PhysicalStore,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        let read = ScopedRead::open(store, owner)?;
        let evidence = strict_snapshot_read(read.transaction(), D::LAYOUT)?;
        Ok(Self {
            evidence,
            identity: owner.identity().clone(),
            _domain: PhantomData,
        })
    }

    pub fn evidence(&self) -> &StrictRecoveryEvidence {
        &self.evidence
    }

    pub fn scope(&self) -> &MutationScopeIdentity {
        &self.identity
    }
}
