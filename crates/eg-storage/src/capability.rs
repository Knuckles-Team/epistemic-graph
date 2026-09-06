//! Scoped capabilities the kernel issues over one physical owner file.
//!
//! A capability is the only way any code outside this crate reaches a redb
//! transaction. Read and snapshot capabilities need an [`OwnedStoreHandle`];
//! the write capability additionally needs the single, move-once
//! [`crate::MutationOwnerAuthority`] token.

use crate::owner::domain::OwnerDomain;
use crate::owner::handle::OwnedStoreHandle;
use crate::owner::registry::declared_table_names;
use crate::physical::binding::{binding_for_read, binding_for_write, retire_scope_in};
use crate::physical::root::PhysicalStore;
use crate::recovery::evidence::{strict_snapshot_read, StrictRecoveryEvidence};
use crate::tables::LedgerRowScope;
use eg_types::MutationScopeIdentity;
use redb::{ReadOnlyTable, ReadTransaction, Table, TableDefinition, TableHandle, WriteTransaction};
use std::marker::PhantomData;

/// The three tables that ARE the file's physical identity. A capability never
/// opens one: they are written only by the storage kernel's own create, bind,
/// adopt and backup paths.
const IDENTITY_TABLES: [&str; 3] = [
    "mutation_store_root_v1",
    "mutation_scope_bindings_v1",
    "mutation_owner_manifest_v1",
];

/// A capability may open exactly the tables this owner file declares, minus the
/// three that carry its physical identity. Anything else -- an undeclared name,
/// another layout's table, or an identity table -- fails closed.
fn permit_table(store: &PhysicalStore, name: &str) -> Result<(), String> {
    if IDENTITY_TABLES.contains(&name) {
        return Err("capability may not open a physical-identity table".to_string());
    }
    if !declared_table_names(store.manifest().layout).contains(&name) {
        return Err("capability may not open an undeclared table".to_string());
    }
    Ok(())
}

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

    /// Open one declared, non-identity table of this owner file for writing.
    ///
    /// The underlying `redb::WriteTransaction` is never lent out: in redb 4.1 a
    /// `&WriteTransaction` opens and deletes any table through `&self`, so
    /// returning one would be unrestricted authority over the
    /// physical-identity tables. This is the only write path, and
    /// [`permit_table`] bounds it to this owner file's declared census.
    pub fn open_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<Table<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        permit_table(self.store, definition.name())?;
        self.transaction
            .open_table(definition)
            .map_err(|error| error.to_string())
    }

    /// Remove every row of one declared scoped table belonging to `scope_key`.
    pub fn purge_scoped_rows<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
        scope_key: &str,
    ) -> Result<(), String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: LedgerRowScope,
        V: redb::Value + 'static,
    {
        permit_table(self.store, definition.name())?;
        crate::tables::purge_scoped_rows(&self.transaction, definition, scope_key)
    }

    /// Retire one logical scope's binding and its authoritative version row.
    ///
    /// `mutation_scope_bindings_v1` is physical identity, so the storage kernel
    /// owns this write; the mutation owner clears its own ledger rows and then
    /// asks for retirement inside the same transaction.
    pub fn retire_scope_binding(&self, identity: &MutationScopeIdentity) -> Result<(), String> {
        retire_scope_in(self.store, &self.transaction, identity)
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

    /// Crate-private for the same reason as the write side: a
    /// `&ReadTransaction` opens and enumerates every table in the file.
    ///
    /// ```compile_fail
    /// # use eg_storage::{KvOwner, ScopedRead};
    /// fn leaks_transaction(read: &ScopedRead<'_, KvOwner>) {
    ///     let _ = read.transaction();
    /// }
    /// ```
    pub(crate) fn transaction(&self) -> &ReadTransaction {
        &self.transaction
    }

    /// Open one declared, non-identity table of this owner file for reading.
    pub fn open_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ReadOnlyTable<K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        permit_table(self.store, definition.name())?;
        self.transaction
            .open_table(definition)
            .map_err(|error| error.to_string())
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
