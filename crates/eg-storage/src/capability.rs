//! Scoped capabilities the kernel issues over one physical owner file.
//!
//! A capability is the only way any code outside this crate reaches a redb
//! transaction. Read and snapshot capabilities need an [`OwnedStoreHandle`];
//! the write capability additionally needs the single, move-once
//! [`crate::MutationOwnerAuthority`] token.

use crate::owner::contract::expected_owner_table_contract;
use crate::owner::domain::OwnerDomain;
use crate::owner::handle::OwnedStoreHandle;
use crate::owner::registry::{declared_table_names, owner_table_names};
use crate::physical::binding::{
    binding_for_read, binding_for_write, ledger_scope_key, retire_scope_in,
};
use crate::physical::manifest::TableScope;
use crate::physical::root::PhysicalStore;
use crate::recovery::evidence::{strict_snapshot_read, StrictRecoveryEvidence};
use crate::tables::LedgerRowScope;
use eg_types::MutationScopeIdentity;
use redb::{
    AccessGuard, Range, ReadOnlyTable, ReadTransaction, ReadableTable, ReadableTableMetadata, Table,
    TableDefinition, TableHandle, WriteTransaction,
};
use std::marker::PhantomData;
use std::sync::Arc;

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

/// How one write capability holds its physical transaction.
///
/// `redb` permits one writer, so a transaction is either served by exactly one
/// capability, which commits or aborts it, or shared by the members of one
/// admitted scope group, in which case only the group may end it. The variant
/// is not a flag a caller can set: [`PhysicalWriteCapability::open`] always
/// produces `Sole` and only [`crate::MutationOwnerAuthority::group_write_capabilities`]
/// produces `Member`.
enum WriteTxn {
    /// Boxed so the two variants are the same size: a `redb::WriteTransaction`
    /// is ~600 bytes and a shared handle is two words, and one heap word per
    /// physical write transaction is nothing beside the transaction itself.
    Sole(Box<WriteTransaction>),
    Member {
        transaction: Arc<WriteTransaction>,
        rows: GroupRowClass,
    },
}

impl WriteTxn {
    fn get(&self) -> &WriteTransaction {
        match self {
            Self::Sole(transaction) => transaction.as_ref(),
            Self::Member { transaction, .. } => transaction,
        }
    }
}

/// Which owner tables one group member may reach.
///
/// A shard file's tables split two ways: most lead their key with the graph
/// name and belong to one serving scope (`Serving`), while the Raft log and
/// meta, the cross-shard 2PC records, the matview and canary rows and the
/// series key spaces are keyed by Raft group, transaction id, view name or
/// series id and belong to the FILE. Both kinds of scope are graph scopes
/// under this layout, so the class cannot be read off the identity: it is the
/// member's position in the group, decided by the mutation owner authority
/// when the group is minted and not settable by a consumer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupRowClass {
    /// The store's own file-wide member: control rows only.
    Control,
    /// A scoped member: its own layout's serving rows only.
    Scoped,
}

/// The one mintable physical write authority for one owner file.
pub struct PhysicalWriteCapability<'a, D: OwnerDomain> {
    transaction: WriteTxn,
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
        let transaction = store.begin_write()?;
        Self::bind(store, WriteTxn::Sole(Box::new(transaction)), owner)
    }

    /// Mint one member of an admitted scope group over an already-open shared
    /// transaction.
    ///
    /// Crate-private, and reached only through the mutation owner authority, so
    /// a domain crate cannot manufacture a member and cannot obtain a second
    /// capability on a transaction it does not already hold.
    pub(crate) fn open_member(
        store: &'a PhysicalStore,
        transaction: Arc<WriteTransaction>,
        rows: GroupRowClass,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        Self::bind(store, WriteTxn::Member { transaction, rows }, owner)
    }

    /// Prove the store's layout and incarnation-anchored authority, then bind
    /// this capability's one serving scope inside the transaction. Shared by
    /// the sole and group-member paths so a member is bound exactly as
    /// strictly as a sole writer is.
    fn bind(
        store: &'a PhysicalStore,
        transaction: WriteTxn,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        let manifest = store.manifest();
        if manifest.layout != D::LAYOUT
            || manifest.authority_digest(store.incarnation()) != *owner.authority_digest()
        {
            return Err("owner write capability does not match this store".to_string());
        }
        binding_for_write(store, transaction.get(), owner.identity())?;
        Ok(Self {
            transaction,
            store,
            identity: owner.identity().clone(),
            principal: owner.principal().to_string(),
            _domain: PhantomData,
        })
    }

    /// Whether this capability is one member of an admitted scope group.
    pub fn is_group_member(&self) -> bool {
        matches!(self.transaction, WriteTxn::Member { .. })
    }

    /// Whether this capability is the group's file-wide control member.
    pub fn is_group_control(&self) -> bool {
        matches!(
            self.transaction,
            WriteTxn::Member {
                rows: GroupRowClass::Control,
                ..
            }
        )
    }

    /// A group member's owner-row class bound.
    ///
    /// Ledger rows are already confined per member by [`ScopedTableMut`], whose
    /// scope key comes from this capability. Owner rows are layout-bounded
    /// everywhere else in this crate, which is right for a single-scope write
    /// but not for a group: the shard's control rows (Raft log and meta, the
    /// cross-shard 2PC records, the matview, canary and series key spaces)
    /// belong to the FILE and carry no graph component, while its graph rows
    /// lead their key with the graph name. So a member admitted for a graph
    /// scope reaches only `Serving` owner tables and the control member only
    /// the file-wide ones. Without this, one member of a group could write
    /// another member's owner rows even though it cannot touch its ledger.
    ///
    /// This applies to group members only. A sole capability is unchanged, so
    /// no existing layout's behaviour moves.
    fn permit_group_owner_table(&self, name: &str) -> Result<(), String> {
        let WriteTxn::Member { rows, .. } = &self.transaction else {
            return Ok(());
        };
        let serving = expected_owner_table_contract(name, D::LAYOUT).scope == TableScope::Serving;
        let permitted = match rows {
            GroupRowClass::Scoped => serving,
            GroupRowClass::Control => !serving,
        };
        if !permitted {
            return Err(
                "group member may not open an owner table outside its row class".to_string(),
            );
        }
        Ok(())
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
            .get()
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
        self.permit_group_owner_table(name)?;
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
        self.permit_group_owner_table(name)?;
        Ok(OwnerReadTable {
            table: self
                .transaction
                .get()
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
            self.transaction.get(),
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
        retire_scope_in(self.store, self.transaction.get(), &self.identity)
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
        binding_for_write(self.store, self.transaction.get(), identity).map(|_| ())
    }

    pub fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        self.store.authenticate_private(sealed, digest)
    }

    /// Commit this capability's transaction. Refused for a group member: the
    /// group's members share one transaction and committing from inside one of
    /// them would commit the others' half-written work.
    pub fn commit(self) -> Result<(), String> {
        match self.transaction {
            WriteTxn::Sole(transaction) => {
                (*transaction).commit().map_err(|error| error.to_string())
            }
            WriteTxn::Member { .. } => Err(GROUP_MEMBER_CANNOT_END.to_string()),
        }
    }

    /// Discard this capability's transaction. Refused for a group member, for
    /// the same reason [`Self::commit`] is.
    pub fn abort(self) -> Result<(), String> {
        match self.transaction {
            WriteTxn::Sole(transaction) => {
                (*transaction).abort().map_err(|error| error.to_string())
            }
            WriteTxn::Member { .. } => Err(GROUP_MEMBER_CANNOT_END.to_string()),
        }
    }

    /// End the whole group's shared transaction from its **last** member.
    ///
    /// The `Arc` is the proof: it unwraps only when every other member has been
    /// dropped, so a group cannot be committed while any member is still live
    /// and able to write. Refused for a sole capability, which uses
    /// [`Self::commit`] / [`Self::abort`].
    pub fn end_group_transaction(self, commit: bool) -> Result<(), String> {
        let WriteTxn::Member { transaction, .. } = self.transaction else {
            return Err("a sole write capability is not a group member".to_string());
        };
        let transaction = Arc::try_unwrap(transaction)
            .map_err(|_| "an admitted scope group member is still live".to_string())?;
        if commit {
            transaction.commit().map_err(|error| error.to_string())
        } else {
            transaction.abort().map_err(|error| error.to_string())
        }
    }
}

const GROUP_MEMBER_CANNOT_END: &str =
    "an admitted scope group member may not commit or abort its own transaction";

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
