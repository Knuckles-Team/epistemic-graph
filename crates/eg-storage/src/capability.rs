//! Scoped capabilities the kernel issues over one physical owner file.
//!
//! A capability is the only way any code outside this crate reaches a redb
//! transaction. Read and snapshot capabilities need an [`OwnedStoreHandle`];
//! the write capability additionally needs the single, move-once
//! [`crate::MutationOwnerAuthority`] token.

use crate::owner::domain::OwnerDomain;
use crate::owner::handle::OwnedStoreHandle;
use crate::owner::registry::{declared_table_names, owner_table_names};
use crate::physical::binding::{
    binding_for_read, binding_for_write, ledger_scope_key, retire_scope_in,
};
use crate::physical::root::PhysicalStore;
use crate::recovery::evidence::{strict_snapshot_read, StrictRecoveryEvidence};
use crate::tables::LedgerRowScope;
use eg_types::MutationScopeIdentity;
use redb::{
    AccessGuard, Range, ReadOnlyTable, ReadTransaction, ReadableTable, ReadableTableMetadata, Table,
    TableDefinition, TableHandle, WriteTransaction,
};
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
    /// Crate-private, and for the same reason the read side's `open_table` is:
    /// a whole ledger table spans every scope the file serves, so a raw
    /// `redb::Table` over one is row-level write authority over every tenant on
    /// it. External writers use [`Self::scoped_table_mut`] for ledger rows and
    /// [`Self::open_owner_write`] for their own layout's rows.
    pub(crate) fn open_table<K, V>(
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

    /// Open one owner table of this capability's layout for writing.
    ///
    /// Owner rows are the domain's own, and several owner tables carry no scope
    /// component in their key at all, so the bound here is the layout — the
    /// same bound [`ScopedRead::open_owner_table`] applies on the read side.
    pub fn open_owner_write<K, V>(
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
        self.open_table(definition)
    }

    /// Open one declared table for writing, bounded to this capability's own
    /// serving scope.
    ///
    /// The write mirror of [`ScopedRead::scoped_table`]: every key it accepts,
    /// on read, insert, remove or range, must name that scope, so a capability
    /// for one tenant cannot reach — or delete — another's ledger rows even
    /// though both live in the same physical table.
    pub fn scoped_table_mut<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedTableMut<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: LedgerRowScope,
        V: redb::Value + 'static,
    {
        Ok(ScopedTableMut {
            table: self.open_table(definition)?,
            scope_key: ledger_scope_key(&self.identity),
        })
    }

    /// Open one owner table of this capability's layout for **reading only**,
    /// inside the admitted write transaction.
    ///
    /// A domain that decides what to write by first reading its own rows --
    /// picking the next claimable job out of six index tables, say -- must do
    /// that read in the same transaction as the write, or the decision is not
    /// serialized against a concurrent writer. A separate read snapshot would
    /// lose exactly that. This is strictly weaker than [`Self::open_table`]:
    /// the returned view exposes lookups and ranges and no mutation, and no
    /// raw transaction is reachable through it.
    ///
    /// The bound is the layout, as on the owner write path. Several owner
    /// tables (the jobs scheduler indexes, for instance) carry no scope
    /// component in their key at all, so the ledger's scope bound
    /// ([`ScopedTable`]) cannot be applied to them.
    pub fn open_owner_read<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<OwnerReadTable<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let name = definition.name();
        if !owner_table_names(D::LAYOUT).contains(&name) {
            return Err("owner read may not open a table outside its layout".to_string());
        }
        Ok(OwnerReadTable {
            table: self
                .transaction
                .open_table(definition)
                .map_err(|error| error.to_string())?,
        })
    }

    /// Remove every row of one declared scoped table belonging to **this
    /// capability's own** serving scope.
    ///
    /// The scope is read from the capability, never taken as an argument: a
    /// capability minted for one scope must not be able to name another, or
    /// holding tenant A's handle would be enough to wipe tenant B's ledger on
    /// the same physical file.
    pub fn purge_scoped_rows<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<(), String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: LedgerRowScope,
        V: redb::Value + 'static,
    {
        permit_table(self.store, definition.name())?;
        crate::tables::purge_scoped_rows(
            &self.transaction,
            definition,
            &ledger_scope_key(&self.identity),
        )
    }

    /// Retire **this capability's own** scope: its binding and its
    /// authoritative version row.
    ///
    /// `mutation_scope_bindings_v1` is physical identity, so the storage kernel
    /// owns this write; the mutation owner clears its own ledger rows and then
    /// asks for retirement inside the same transaction. Like
    /// [`Self::purge_scoped_rows`], the scope comes from the capability and is
    /// not an argument.
    pub fn retire_scope_binding(&self) -> Result<(), String> {
        retire_scope_in(self.store, &self.transaction, &self.identity)
    }

    /// The physical store this capability was minted over.
    ///
    /// Crate-private: a `&PhysicalStore` is the whole physical authority — the
    /// database, the incarnation and the manifest — and handing one out would
    /// make every capability bound on this type vacuous. It exists so
    /// [`crate::owner::blob_shared`] can re-prove the blob layout's independent
    /// shared-service authority against the same store, inside this same
    /// already-open write transaction.
    pub(crate) fn store(&self) -> &PhysicalStore {
        self.store
    }

    /// The exact serving scope this capability was issued for.
    pub fn scope(&self) -> &MutationScopeIdentity {
        &self.identity
    }

    /// The authenticated principal this capability was issued to.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Reprove that `identity` is **this capability's own** scope and is still
    /// bound to this store under the current manifest and declared table
    /// census, inside this write transaction.
    ///
    /// Boundness alone is not enough: every scope served by one physical file
    /// is bound to it, so a boundness-only check would let a capability for one
    /// tenant act on another.
    pub fn verify_scope(&self, identity: &MutationScopeIdentity) -> Result<(), String> {
        if identity != &self.identity {
            return Err("mutation capability does not serve this scope".to_string());
        }
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

impl<'w> PhysicalWriteCapability<'w, crate::owner::domain::BlobOwner> {
    /// Mint the blob layout's independent shared-service write over **this**
    /// already-open, already-admitted write transaction.
    ///
    /// `cas_chunks` and `cas_refcount` are declared `TableScope::SharedService`
    /// and are not owner rows: they are reached through the blob service's own
    /// authority, not through a serving scope's owner-row adapter. But `redb`
    /// permits one writer, and on every batch path that writer is this
    /// capability, so the shared-service write borrows it instead of opening a
    /// second transaction — a chunk row and the refcount that accounts for it
    /// then commit with the batch's ledger rows, or neither lands.
    pub fn blob_shared_write(
        &'w self,
        owner: &crate::owner::blob_shared::BlobSharedServiceHandle,
        principal: &str,
    ) -> Result<crate::owner::blob_shared::BlobSharedWrite<'w>, String> {
        crate::owner::blob_shared::write_blob_shared(self, owner, principal)
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
    ///
    /// Crate-private: a whole ledger table spans every scope the file serves,
    /// so handing one to a domain crate would let a reader bound to one tenant
    /// iterate another's receipts. External readers use [`Self::scoped_table`]
    /// for ledger rows and [`Self::open_owner_table`] for their own layout.
    pub(crate) fn open_table<K, V>(
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

    /// Open one declared table bounded to this read's own serving scope.
    ///
    /// Every key [`ScopedTable`] accepts must name that scope, so a reader for
    /// one tenant cannot address another's rows even though both live in the
    /// same physical table.
    pub fn scoped_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedTable<K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: LedgerRowScope,
        V: redb::Value + 'static,
    {
        Ok(ScopedTable {
            table: self.open_table(definition)?,
            scope_key: ledger_scope_key(&self.identity),
        })
    }

    /// Open one owner table of this read's own layout.
    ///
    /// Owner tables are the domain's own rows and several of them (the jobs
    /// scheduler indexes, for instance) carry no scope component in their key
    /// at all, so they cannot be scope-bound the way a ledger table can. The
    /// bound here is the layout, exactly as on the write side.
    pub fn open_owner_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ReadOnlyTable<K, V>, String>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let name = definition.name();
        if !owner_table_names(D::LAYOUT).contains(&name) {
            return Err("scoped read may not open a table outside its layout".to_string());
        }
        self.open_table(definition)
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

/// A declared table restricted to one serving scope.
///
/// It is the read counterpart of the write capability's confinement: the scope
/// key comes from the [`ScopedRead`] that issued it, and every key presented to
/// it must carry that same key in its first position.
pub struct ScopedTable<K: redb::Key + 'static, V: redb::Value + 'static> {
    table: ReadOnlyTable<K, V>,
    scope_key: String,
}

impl<K, V> ScopedTable<K, V>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: LedgerRowScope,
    V: redb::Value + 'static,
{
    /// The one scope every key of this table must name.
    pub fn scope_key(&self) -> &str {
        &self.scope_key
    }

    /// One row of this read's own scope. A key naming another scope is refused.
    pub fn get<'k>(&self, key: K::SelfType<'k>) -> Result<Option<AccessGuard<'static, V>>, String> {
        self.permit(&key)?;
        self.table.get(&key).map_err(|error| error.to_string())
    }

    /// Every row between two inclusive bounds, both of which must name this
    /// read's own scope.
    pub fn range_inclusive<'k>(
        &self,
        start: K::SelfType<'k>,
        end: K::SelfType<'k>,
    ) -> Result<Range<'static, K, V>, String> {
        self.permit(&start)?;
        self.permit(&end)?;
        self.table
            .range(start..=end)
            .map_err(|error| error.to_string())
    }

    fn permit(&self, key: &K::SelfType<'_>) -> Result<(), String> {
        if key.ledger_scope() != self.scope_key {
            return Err("scoped read may not address another scope's rows".to_string());
        }
        Ok(())
    }
}

/// A read-only view of one owner table inside an admitted write transaction.
///
/// It exists so a domain can read its own rows and write in one serialized
/// transaction without ever holding something that can mutate or that exposes
/// the transaction. Every method here is a read.
pub struct OwnerReadTable<'a, K: redb::Key + 'static, V: redb::Value + 'static> {
    table: Table<'a, K, V>,
}

impl<K, V> OwnerReadTable<'_, K, V>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    pub fn get<'k>(&self, key: K::SelfType<'k>) -> Result<Option<AccessGuard<'_, V>>, String> {
        self.table.get(&key).map_err(|error| error.to_string())
    }

    pub fn range_inclusive<'k>(
        &self,
        start: K::SelfType<'k>,
        end: K::SelfType<'k>,
    ) -> Result<Range<'_, K, V>, String> {
        self.table
            .range(start..=end)
            .map_err(|error| error.to_string())
    }

    /// Rows from `start` to the end of the table.
    ///
    /// Strictly weaker than [`Self::iter`], which already returns every row of
    /// this layout-bounded table: it adds no reach, only a starting position,
    /// so a prefix scan over a composite key does not have to read from the
    /// first row. Needed because several owner tables key on
    /// `(partition, key)` and a prefix scan of one partition has no natural
    /// inclusive upper bound.
    pub fn range_from<'k>(&self, start: K::SelfType<'k>) -> Result<Range<'_, K, V>, String> {
        self.table.range(start..).map_err(|error| error.to_string())
    }

    pub fn iter(&self) -> Result<Range<'_, K, V>, String> {
        self.table.iter().map_err(|error| error.to_string())
    }

    pub fn len(&self) -> Result<u64, String> {
        self.table.len().map_err(|error| error.to_string())
    }

    pub fn is_empty(&self) -> Result<bool, String> {
        self.len().map(|len| len == 0)
    }
}

/// A declared table opened for writing and restricted to one serving scope.
///
/// The write counterpart of [`ScopedTable`]. Its scope key comes from the
/// capability that issued it, and every key presented to it — read, insert,
/// remove, or either bound of a range — must carry that same key in its first
/// position. No accessor returns the underlying `redb::Table`.
pub struct ScopedTableMut<'a, K: redb::Key + 'static, V: redb::Value + 'static> {
    table: Table<'a, K, V>,
    scope_key: String,
}

impl<K, V> ScopedTableMut<'_, K, V>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: LedgerRowScope,
    V: redb::Value + 'static,
{
    /// The one scope every key of this table must name.
    pub fn scope_key(&self) -> &str {
        &self.scope_key
    }

    pub fn get<'k>(&self, key: K::SelfType<'k>) -> Result<Option<AccessGuard<'_, V>>, String> {
        self.permit(&key)?;
        self.table.get(&key).map_err(|error| error.to_string())
    }

    pub fn insert<'k, 'v>(
        &mut self,
        key: K::SelfType<'k>,
        value: V::SelfType<'v>,
    ) -> Result<(), String> {
        self.permit(&key)?;
        self.table
            .insert(&key, &value)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub fn remove<'k>(&mut self, key: K::SelfType<'k>) -> Result<(), String> {
        self.permit(&key)?;
        self.table
            .remove(&key)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub fn range_inclusive<'k>(
        &self,
        start: K::SelfType<'k>,
        end: K::SelfType<'k>,
    ) -> Result<Range<'_, K, V>, String> {
        self.permit(&start)?;
        self.permit(&end)?;
        self.table
            .range(start..=end)
            .map_err(|error| error.to_string())
    }

    fn permit(&self, key: &K::SelfType<'_>) -> Result<(), String> {
        if key.ledger_scope() != self.scope_key {
            return Err("scoped write may not address another scope's rows".to_string());
        }
        Ok(())
    }
}

/// Retirement of one layout's owner payload.
///
/// `purge_scope` retires a generation's ledger, binding and version row, but
/// owner tables are the domain's own and their keys carry no scope component in
/// general, so the kernel cannot sweep them. A domain implements this for its
/// own tables and the mutation kernel invokes it inside the same write
/// transaction, so authority and payload retire together or not at all. Without
/// it, the next binding of the same logical name reads the retired
/// generation's rows as its own.
pub trait OwnerPayloadRetirement<D: OwnerDomain> {
    fn retire_owner_payload(
        &self,
        write: &PhysicalWriteCapability<'_, D>,
        scope: &MutationScopeIdentity,
    ) -> Result<(), String>;
}
