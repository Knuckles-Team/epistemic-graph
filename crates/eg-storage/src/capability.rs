//! Scoped capabilities the kernel issues over one physical owner file.
//!
//! A capability is the only way any code outside this crate reaches a redb
//! transaction. Read and snapshot capabilities need an [`OwnedStoreHandle`];
//! the write capability additionally needs the single, move-once
//! [`crate::MutationOwnerAuthority`] token.

use crate::owner::domain::OwnerDomain;
use crate::owner::handle::OwnedStoreHandle;
use crate::owner::registry::{declared_table_names, owner_table_names};
use crate::owner::row_key::{is_control_scope, owner_row_key, OwnerRowScope, RowKey};
use crate::physical::binding::{
    binding_for_read, binding_for_write, ledger_scope_key, retire_scope_in,
};
use crate::physical::root::PhysicalStore;
use crate::recovery::evidence::{strict_snapshot_read, StrictRecoveryEvidence};
use crate::scoped::{
    OwnerReadTable, ScopedOwnerTable, ScopedOwnerTableMut, ScopedTable, ScopedTableMut,
};
use crate::tables::LedgerRowScope;
use eg_types::MutationScopeIdentity;
use redb::{ReadOnlyTable, ReadTransaction, Table, TableDefinition, TableHandle, WriteTransaction};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// The three tables that ARE the file's physical identity. A capability never
/// opens one: they are written only by the storage kernel's own create, bind,
/// adopt and backup paths.
const IDENTITY_TABLES: [&str; 3] = [
    "mutation_store_root",
    "mutation_scope_bindings",
    "mutation_owner_manifest",
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
    /// is ~600 bytes and a shared handle is one word, and one heap word per
    /// physical write transaction is nothing beside the transaction itself.
    Sole(Box<WriteTransaction>),
    Member(Arc<WriteTransaction>),
}

impl WriteTxn {
    fn get(&self) -> &WriteTransaction {
        match self {
            Self::Sole(transaction) => transaction.as_ref(),
            Self::Member(transaction) => transaction,
        }
    }
}

/// The one mintable physical write authority for one owner file.
pub struct PhysicalWriteCapability<'a, D: OwnerDomain> {
    transaction: WriteTxn,
    store: &'a PhysicalStore,
    identity: MutationScopeIdentity,
    principal: String,
    /// Set by any capability on this transaction whose operation failed.
    ///
    /// The owner-row path already refuses to commit unfinished work through
    /// the admission state machine. Nothing did that for the shared-service
    /// CAS surface, so a caller that swallowed, say, a refcount-underflow
    /// refusal could still commit the rows it had already written. Poison is
    /// per TRANSACTION, not per capability, so one member's failure stops the
    /// whole group's commit: it is shared by every member and the group cannot
    /// commit while it is set.
    poison: Arc<AtomicBool>,
    _domain: PhantomData<D>,
}

impl<'a, D: OwnerDomain> PhysicalWriteCapability<'a, D> {
    pub(crate) fn open(
        store: &'a PhysicalStore,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        let transaction = store.begin_write()?;
        Self::bind(
            store,
            WriteTxn::Sole(Box::new(transaction)),
            Arc::new(AtomicBool::new(false)),
            owner,
        )
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
        poison: Arc<AtomicBool>,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<Self, String> {
        Self::bind(store, WriteTxn::Member(transaction), poison, owner)
    }

    /// Prove the store's layout and incarnation-anchored authority, then bind
    /// this capability's one serving scope inside the transaction. Shared by
    /// the sole and group-member paths so a member is bound exactly as
    /// strictly as a sole writer is.
    fn bind(
        store: &'a PhysicalStore,
        transaction: WriteTxn,
        poison: Arc<AtomicBool>,
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
            poison,
            _domain: PhantomData,
        })
    }

    /// Whether this capability is one member of an admitted scope group.
    pub fn is_group_member(&self) -> bool {
        matches!(self.transaction, WriteTxn::Member(_))
    }

    /// Whether this capability serves the file's reserved control scope.
    ///
    /// Read off the bound identity, never off a caller's argument order, so
    /// the control/serving split is a property of the layout and holds for a
    /// sole admit exactly as it does for a group member.
    pub fn is_control(&self) -> bool {
        is_control_scope(D::LAYOUT, &self.identity)
    }

    /// Mark this transaction unusable for commit.
    ///
    /// Called on every failed operation of a surface that has no admission
    /// window of its own, so a swallowed error cannot be followed by a commit.
    pub(crate) fn poison(&self) {
        self.poison.store(true, Ordering::SeqCst);
    }

    /// Poison this transaction when a shared-service operation fails.
    ///
    /// Crate-visible rather than private because
    /// [`crate::owner::blob_shared`] is the surface that needs it: it has no
    /// admission window of its own to drop unfinished.
    pub(crate) fn poison_shared_on_error<T>(&self, result: Result<T, String>) -> Result<T, String> {
        if result.is_err() {
            self.poison();
        }
        result
    }

    fn refuse_if_poisoned(&self) -> Result<(), String> {
        if self.poison.load(Ordering::SeqCst) {
            return Err("write transaction is poisoned by a failed operation".to_string());
        }
        Ok(())
    }

    /// The owner-row class bound.
    ///
    /// Owner tables were layout-bounded and nothing more, which is right for a
    /// file that serves one scope. A graph shard serves many, and 42 of its 53
    /// tables lead their key with the graph name while 11 belong to the file.
    /// So a scope-prefixed table is unreachable through the raw accessors —
    /// they cannot express a row bound — and a file-wide table is reachable
    /// only from the reserved control scope. Layouts that declare neither
    /// ([`RowKey::Unscoped`], every layout but the shard) are unchanged.
    fn permit_owner_row_class(&self, name: &str) -> Result<(), String> {
        permit_owner_row_class::<D>(name, self.is_control())
    }

    /// The scope name every key of a scope-prefixed owner table must carry.
    fn owner_scope_key(&self) -> Result<&str, String> {
        owner_scope_key(&self.identity)
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
        self.permit_owner_row_class(name)?;
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
        self.permit_owner_row_class(name)?;
        Ok(OwnerReadTable {
            table: self
                .transaction
                .get()
                .open_table(definition)
                .map_err(|error| error.to_string())?,
        })
    }

    /// Open one scope-prefixed owner table of this layout, bounded to **this
    /// capability's own** serving scope.
    ///
    /// The owner-row counterpart of [`Self::scoped_table_mut`]: the scope name
    /// comes from the capability, never from an argument, and every key it
    /// accepts — read, insert, remove, or either bound of a range — must carry
    /// that name in its first position. This is the only way a scoped member
    /// reaches a table its neighbours also live in.
    pub fn scoped_owner_table_mut<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedOwnerTableMut<'_, K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: OwnerRowScope,
        V: redb::Value + 'static,
    {
        let name = definition.name();
        permit_scope_prefixed::<D>(name, self.is_control())?;
        Ok(ScopedOwnerTableMut {
            table: self.open_table(definition)?,
            scope_key: self.owner_scope_key()?.to_string(),
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
    /// `mutation_scope_bindings` is physical identity, so the storage kernel
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
        self.refuse_if_poisoned()?;
        match self.transaction {
            WriteTxn::Sole(transaction) => {
                (*transaction).commit().map_err(|error| error.to_string())
            }
            WriteTxn::Member(_) => Err(GROUP_MEMBER_CANNOT_END.to_string()),
        }
    }

    /// Discard this capability's transaction. Refused for a group member, for
    /// the same reason [`Self::commit`] is.
    pub fn abort(self) -> Result<(), String> {
        match self.transaction {
            WriteTxn::Sole(transaction) => {
                (*transaction).abort().map_err(|error| error.to_string())
            }
            WriteTxn::Member(_) => Err(GROUP_MEMBER_CANNOT_END.to_string()),
        }
    }

    /// End the whole group's shared transaction from its **last** member.
    ///
    /// The `Arc` is the proof: it unwraps only when every other member has been
    /// dropped, so a group cannot be committed while any member is still live
    /// and able to write. Refused for a sole capability, which uses
    /// [`Self::commit`] / [`Self::abort`].
    pub fn end_group_transaction(self, commit: bool) -> Result<(), String> {
        if commit {
            self.refuse_if_poisoned()?;
        }
        let WriteTxn::Member(transaction) = self.transaction else {
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

/// The owner-row class bound, shared by the write and read sides so a reader
/// cannot reach what a writer of the same scope cannot.
fn permit_owner_row_class<D: OwnerDomain>(name: &str, control: bool) -> Result<(), String> {
    match owner_row_key(name, D::LAYOUT) {
        RowKey::Unscoped => Ok(()),
        RowKey::FileWide if control => Ok(()),
        RowKey::FileWide => {
            Err("only the file's control scope may open a file-wide owner table".to_string())
        }
        RowKey::ScopePrefixed => Err(
            "a scope-prefixed owner table is reachable only through its scoped accessor"
                .to_string(),
        ),
    }
}

/// The scope-prefixed accessor's own bound: the table must be scope-prefixed,
/// and the control scope owns no rows in one.
fn permit_scope_prefixed<D: OwnerDomain>(name: &str, control: bool) -> Result<(), String> {
    if !owner_table_names(D::LAYOUT).contains(&name) {
        return Err("scoped owner access may not open a table outside its layout".to_string());
    }
    if owner_row_key(name, D::LAYOUT) != RowKey::ScopePrefixed {
        return Err("this owner table is not scope-prefixed".to_string());
    }
    if control {
        return Err("the file's control scope owns no scope-prefixed rows".to_string());
    }
    Ok(())
}

/// The scope name a scope-prefixed owner row must carry.
fn owner_scope_key(identity: &MutationScopeIdentity) -> Result<&str, String> {
    identity
        .scope()
        .graph_name()
        .map(|name| name.as_str())
        .ok_or_else(|| "this serving scope has no name to prefix owner rows with".to_string())
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
        permit_owner_row_class::<D>(name, is_control_scope(D::LAYOUT, &self.identity))?;
        self.open_table(definition)
    }

    /// Open one scope-prefixed owner table bounded to this read's own serving
    /// scope.
    ///
    /// The read twin of [`PhysicalWriteCapability::scoped_owner_table_mut`].
    /// Without it a reader bound to one graph could enumerate every other
    /// graph's rows in the same shard file, which is the read half of the
    /// confinement the write side enforces.
    pub fn scoped_owner_table<K, V>(
        &self,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<ScopedOwnerTable<K, V>, String>
    where
        K: redb::Key + 'static,
        for<'k> K::SelfType<'k>: OwnerRowScope,
        V: redb::Value + 'static,
    {
        let name = definition.name();
        permit_scope_prefixed::<D>(name, is_control_scope(D::LAYOUT, &self.identity))?;
        Ok(ScopedOwnerTable {
            table: self.open_table(definition)?,
            scope_key: owner_scope_key(&self.identity)?.to_string(),
        })
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
