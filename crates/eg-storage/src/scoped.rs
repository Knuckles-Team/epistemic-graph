//! Scope-bounded views over one declared table.
//!
//! A capability is bounded to one owner file and one serving scope; these are
//! what make that bound reach individual ROWS. Each holds a real `redb` table
//! and a scope key taken from the capability that issued it — never from an
//! argument — and refuses every key that does not carry it, so a reader or
//! writer for one tenant cannot address another's rows in a table they share.
//!
//! Two key shapes, two traits. A ledger key leads with the scope's 64-hex
//! binding digest ([`LedgerRowScope`]); an owner key leads with the serving
//! scope's logical name ([`OwnerRowScope`]), because that is what the domain's
//! own reads and writes carry. [`OwnerReadTable`] is the odd one out: it is a
//! read-only view of one LAYOUT-bounded owner table inside an admitted write,
//! for the tables whose keys carry no scope component at all.

use crate::owner::row_key::{OwnerRowScope, OwnerRowScopeStart};
use crate::tables::LedgerRowScope;
use redb::{AccessGuard, Range, ReadOnlyTable, ReadableTable, ReadableTableMetadata, Table};

/// A declared table restricted to one serving scope.
///
/// It is the read counterpart of the write capability's confinement: the scope
/// key comes from the [`ScopedRead`] that issued it, and every key presented to
/// it must carry that same key in its first position.
pub struct ScopedTable<K: redb::Key + 'static, V: redb::Value + 'static> {
    pub(crate) table: ReadOnlyTable<K, V>,
    pub(crate) scope_key: String,
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
    pub(crate) table: Table<'a, K, V>,
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
    pub(crate) table: Table<'a, K, V>,
    pub(crate) scope_key: String,
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

/// One scope-prefixed owner table, restricted to one serving scope.
///
/// The owner-row counterpart of [`ScopedTable`]. Its scope key is the serving
/// scope's logical name — for a graph shard, the graph name that leads every
/// one of those tables' keys — taken from the read that issued it and never
/// from an argument.
pub struct ScopedOwnerTable<K: redb::Key + 'static, V: redb::Value + 'static> {
    pub(crate) table: ReadOnlyTable<K, V>,
    pub(crate) scope_key: String,
}

impl<K, V> ScopedOwnerTable<K, V>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope,
    V: redb::Value + 'static,
{
    pub fn scope_key(&self) -> &str {
        &self.scope_key
    }

    pub fn get<'k>(&self, key: K::SelfType<'k>) -> Result<Option<AccessGuard<'static, V>>, String> {
        permit_owner_row(&key, &self.scope_key)?;
        self.table.get(&key).map_err(|error| error.to_string())
    }

    pub fn range_inclusive<'k>(
        &self,
        start: K::SelfType<'k>,
        end: K::SelfType<'k>,
    ) -> Result<Range<'static, K, V>, String> {
        permit_owner_row(&start, &self.scope_key)?;
        permit_owner_row(&end, &self.scope_key)?;
        self.table
            .range(start..=end)
            .map_err(|error| error.to_string())
    }

    /// Every row of THIS scope, in key order, and no other scope's.
    ///
    /// The scan starts at the least key the scope can own
    /// ([`OwnerRowScopeStart`]) and stops on the first key whose leading
    /// component is a different scope, so it never reads past the scope's own
    /// range and takes no bound from the caller. It is strictly weaker than a
    /// whole-table `iter()`, which is why a scope-prefixed owner table can
    /// offer it at all: `iter()` would expose every other scope's rows in the
    /// file they share.
    ///
    /// This is the accessor a per-graph prefix scan needs. `range_inclusive`
    /// cannot express one for a key whose non-leading components include a
    /// `&str`, because `&str` has no maximum -- see [`OwnerRowScopeStart`].
    pub fn scope_rows(
        &self,
    ) -> Result<impl Iterator<Item = ScopeRow<'static, K, V>>, String>
    where
        for<'k> K::SelfType<'k>: OwnerRowScopeStart<'k>,
    {
        let rows = self
            .table
            .range(K::SelfType::scope_start(self.scope_key.as_str())..)
            .map_err(|error| error.to_string())?;
        Ok(bounded_to_scope(rows, self.scope_key.clone()))
    }
}

/// One scope-prefixed owner table opened for writing and restricted to one
/// serving scope. The write counterpart of [`ScopedOwnerTable`]; no accessor
/// returns the underlying `redb::Table`.
pub struct ScopedOwnerTableMut<'a, K: redb::Key + 'static, V: redb::Value + 'static> {
    pub(crate) table: Table<'a, K, V>,
    pub(crate) scope_key: String,
}

impl<K, V> ScopedOwnerTableMut<'_, K, V>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope,
    V: redb::Value + 'static,
{
    pub fn scope_key(&self) -> &str {
        &self.scope_key
    }

    pub fn get<'k>(&self, key: K::SelfType<'k>) -> Result<Option<AccessGuard<'_, V>>, String> {
        permit_owner_row(&key, &self.scope_key)?;
        self.table.get(&key).map_err(|error| error.to_string())
    }

    pub fn insert<'k, 'v>(
        &mut self,
        key: K::SelfType<'k>,
        value: V::SelfType<'v>,
    ) -> Result<(), String> {
        permit_owner_row(&key, &self.scope_key)?;
        self.table
            .insert(&key, &value)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub fn remove<'k>(&mut self, key: K::SelfType<'k>) -> Result<(), String> {
        permit_owner_row(&key, &self.scope_key)?;
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
        permit_owner_row(&start, &self.scope_key)?;
        permit_owner_row(&end, &self.scope_key)?;
        self.table
            .range(start..=end)
            .map_err(|error| error.to_string())
    }

    /// Every row of THIS scope, in key order, and no other scope's.
    ///
    /// The scan starts at the least key the scope can own
    /// ([`OwnerRowScopeStart`]) and stops on the first key whose leading
    /// component is a different scope, so it never reads past the scope's own
    /// range and takes no bound from the caller. It is strictly weaker than a
    /// whole-table `iter()`, which is why a scope-prefixed owner table can
    /// offer it at all: `iter()` would expose every other scope's rows in the
    /// file they share.
    ///
    /// This is the accessor a per-graph prefix scan needs. `range_inclusive`
    /// cannot express one for a key whose non-leading components include a
    /// `&str`, because `&str` has no maximum -- see [`OwnerRowScopeStart`].
    pub fn scope_rows<'s>(
        &'s self,
    ) -> Result<impl Iterator<Item = ScopeRow<'s, K, V>> + 's, String>
    where
        for<'k> K::SelfType<'k>: OwnerRowScopeStart<'k>,
    {
        let rows = self
            .table
            .range(K::SelfType::scope_start(self.scope_key.as_str())..)
            .map_err(|error| error.to_string())?;
        Ok(bounded_to_scope(rows, self.scope_key.clone()))
    }

    /// Remove every row of THIS scope, and no other scope's.
    ///
    /// The write twin of [`Self::scope_rows`], and the primitive an owner
    /// layout needs to implement [`crate::OwnerPayloadRetirement`] at all when
    /// its tables are scope-prefixed. `PhysicalWriteCapability::purge_scoped_rows`
    /// cannot serve that: it is the LEDGER sweep, keyed by
    /// [`crate::ledger_scope_key`] -- the 64-hex binding digest -- while an owner
    /// key leads with the scope's logical NAME, so on an owner table it matches
    /// nothing and returns `Ok(())` having removed no row. A retirement that
    /// silently retains the payload is the exact failure
    /// `OwnerPayloadRetirement` exists to prevent, so the sweep has to be keyed
    /// the way the rows are.
    ///
    /// Implemented with `redb`'s own `retain`, bounded to the scope this table
    /// is already bound to: the scope comes from the capability that opened it,
    /// never from an argument, so holding one graph's handle can never wipe
    /// another's rows in the file they share.
    pub fn purge_scope_rows(&mut self) -> Result<(), String>
    where
        for<'k> K::SelfType<'k>: OwnerRowScope,
    {
        let scope_key = self.scope_key.clone();
        self.table
            .retain(|key, _| key.owner_scope() != scope_key.as_str())
            .map_err(|error| error.to_string())
    }
}

/// Whole-component equality against the holder's own scope name, exactly as
/// [`ScopedTable::permit`] does for a ledger key.
/// One row yielded by a scope-bounded scan.
pub type ScopeRow<'t, K, V> = Result<(AccessGuard<'t, K>, AccessGuard<'t, V>), String>;

/// Stop a scope-started range at the first row that leaves the scope.
///
/// The `take_while` lives here rather than at any call site: a caller that
/// forgot it would read every following scope's rows, which is exactly the
/// confinement this module exists to enforce. A storage error is yielded rather
/// than treated as the end of the scope, so a failing scan cannot look like an
/// empty one.
fn bounded_to_scope<'t, K, V>(
    rows: Range<'t, K, V>,
    scope_key: String,
) -> impl Iterator<Item = ScopeRow<'t, K, V>>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: OwnerRowScope,
    V: redb::Value + 'static,
{
    rows.map(|row| row.map_err(|error| error.to_string()))
        .take_while(move |row| match row {
            Ok((key, _)) => key.value().owner_scope() == scope_key.as_str(),
            Err(_) => true,
        })
}

pub(crate) fn permit_owner_row<K: OwnerRowScope>(key: &K, scope_key: &str) -> Result<(), String> {
    if key.owner_scope() != scope_key {
        return Err("scoped owner access may not address another scope's rows".to_string());
    }
    Ok(())
}
