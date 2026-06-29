//! The redb-backed user-table store (CONCEPT:EG-018; relational completeness
//! CONCEPT:EG-020) — the durable, ACID home for ARBITRARY user-defined relational
//! tables (Prometheus metrics, Langfuse time-series, stock bars, connector mirrors,
//! ETL outputs).
//!
//! ## redb layout (one `Database` file, three system tables)
//!   * `__sql_catalog__`  `table_name              -> MessagePack(TableSchema)`
//!     The catalog. One row per user table, recording its full typed schema.
//!   * `__sql_rows__`     `(table_name, rowid: u64) -> MessagePack(Vec<Cell>)`
//!     Every user table's rows live in ONE physical redb table, namespaced by the
//!     logical table name in the composite key (redb's `&'static str` table-name
//!     constraint rules out a per-table `TableDefinition`), keeping each logical
//!     table's rows contiguous for an efficient prefix range-scan.
//!   * `__sql_seq__`      `table_name              -> next rowid (u64)`
//!     A per-table monotonic rowid allocator — the internal row identity AND the
//!     surface exposed as `SERIAL`/`DEFAULT nextval` (CONCEPT:EG-020).
//!
//! ## Durability + transactions (CONCEPT:EG-020)
//! Every one-shot mutation runs in ONE redb `WriteTransaction` committed at
//! `Durability::Immediate` BEFORE the method returns (commit-before-ack). A
//! mid-transaction error drops the txn without `commit()`, so redb discards every
//! staged write (true rollback, no partial). A MULTI-statement transaction
//! (`BEGIN … COMMIT`) is modeled as a [`TableTxn`] op buffer applied via
//! [`TableStore::commit_txn`] in ONE redb `WriteTransaction`: the whole batch is
//! atomic and a constraint violation on ANY op aborts (and rolls back) the lot.
//!
//! ## Constraints (CONCEPT:EG-020)
//! `NOT NULL` (rejected at coerce time), column `DEFAULT` (filled when a column is
//! omitted), `SERIAL`/`DEFAULT nextval` (auto-assigned from the per-table sequence),
//! `PRIMARY KEY`/`UNIQUE` uniqueness, and a simple `CHECK (col OP literal)` are all
//! enforced on the write path; a violation aborts the (one-shot or multi-statement)
//! transaction.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};
use serde_json::Value;

use super::schema::{Cell, Column, TableSchema};

/// Catalog system table: `table_name -> MessagePack(TableSchema)`.
const CATALOG: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_catalog__");
/// Row store: `(table_name, rowid) -> MessagePack(Vec<Cell>)`.
const ROWS: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("__sql_rows__");
/// Per-table rowid allocator (also the `SERIAL` sequence): `table_name -> next rowid`.
const SEQ: TableDefinition<&str, u64> = TableDefinition::new("__sql_seq__");

/// A single-column equality predicate — the WHERE shape user-table UPDATE/DELETE
/// resolves (`WHERE <col> = <literal>`), mirroring the `nodes` DML policy.
#[derive(Debug, Clone, PartialEq)]
pub struct ColEq {
    pub column: String,
    pub value: Value,
}

/// One staged operation in a multi-statement transaction (CONCEPT:EG-020). Buffered
/// by [`TableTxn`] and applied in order, in ONE redb `WriteTransaction`, by
/// [`TableStore::commit_txn`].
#[derive(Debug, Clone, PartialEq)]
pub enum TxnOp {
    CreateTable {
        schema: TableSchema,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    AddColumn {
        table: String,
        column: Column,
    },
    Insert {
        table: String,
        col_order: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Update {
        table: String,
        set: serde_json::Map<String, Value>,
        selector: ColEq,
    },
    Delete {
        table: String,
        selector: ColEq,
    },
}

/// A buffered multi-statement transaction (CONCEPT:EG-020). `BEGIN` creates one;
/// each DDL/DML statement pushes a [`TxnOp`]; `COMMIT` applies them via
/// [`TableStore::commit_txn`] (one redb txn — all-or-nothing); `ROLLBACK` drops it.
#[derive(Debug, Clone, Default)]
pub struct TableTxn {
    pub ops: Vec<TxnOp>,
}

impl TableTxn {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push(&mut self, op: TxnOp) {
        self.ops.push(op);
    }
}

/// The durable user-table store. Cheap to clone (`Arc<Database>`), so the pgwire
/// shim can hold one process-wide and hand a clone to each connection.
#[derive(Clone)]
pub struct TableStore {
    db: Arc<Database>,
}

impl TableStore {
    /// Open (creating if absent) the user-table store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let db = Database::create(path).map_err(|e| format!("open sql table store: {e}"))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Open a fresh store at a unique temp path — for tests and ephemeral use.
    pub fn open_temp() -> Result<(Self, std::path::PathBuf), String> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "eg_sql_tables_{}_{}_{n}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        ));
        Ok((Self::open(&path)?, path))
    }

    // ── one-shot DDL (each opens + commits its own txn) ───────────────────────

    /// `CREATE TABLE`: record `schema` in the catalog. `Ok(true)` when created,
    /// `Ok(false)` when it already existed and `if_not_exists` was set.
    pub fn create_table(&self, schema: &TableSchema, if_not_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let created = create_in(&wtx, schema, if_not_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(created)
    }

    /// `DROP TABLE`: remove the catalog entry, the sequence, and EVERY row.
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let dropped = drop_in(&wtx, name, if_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(dropped)
    }

    /// `ALTER TABLE ADD COLUMN`: append `column` to the table's schema.
    pub fn add_column(&self, table: &str, column: Column) -> Result<(), String> {
        let wtx = self.begin()?;
        add_column_in(&wtx, table, &column)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    // ── catalog reads (own read txn) ──────────────────────────────────────────

    /// The schema of `name`, or `None` if no such user table exists.
    pub fn get_schema(&self, name: &str) -> Result<Option<TableSchema>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let cat = match rtx.open_table(CATALOG) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        match cat.get(name).map_err(map_err)? {
            Some(v) => {
                let schema: TableSchema =
                    rmp_serde::from_slice(v.value()).map_err(|e| format!("decode schema: {e}"))?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }

    /// The names of every user table (sorted for determinism).
    pub fn list_tables(&self) -> Result<Vec<String>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let cat = match rtx.open_table(CATALOG) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut names: Vec<String> = cat
            .iter()
            .map_err(map_err)?
            .map(|r| r.map(|(k, _)| k.value().to_string()))
            .collect::<Result<_, _>>()
            .map_err(map_err)?;
        names.sort();
        Ok(names)
    }

    /// Every row of `table`, each a schema-aligned `Vec<Cell>` (NULL-padded to the
    /// current schema width). Errors if the table does not exist.
    pub fn scan(&self, table: &str) -> Result<Vec<Vec<Cell>>, String> {
        let schema = self
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        let width = schema.columns.len();
        let rtx = self.db.begin_read().map_err(map_err)?;
        // The physical row table is created lazily on the first committed INSERT; a
        // table that only ever had its schema created (or whose inserts rolled back)
        // has no `__sql_rows__` table yet → an empty scan, not an error.
        let rows = match rtx.open_table(ROWS) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        for r in rows
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (_, v) = r.map_err(map_err)?;
            let mut cells: Vec<Cell> =
                rmp_serde::from_slice(v.value()).map_err(|e| format!("decode row: {e}"))?;
            if cells.len() < width {
                cells.resize(width, Cell::Null);
            }
            out.push(cells);
        }
        Ok(out)
    }

    // ── one-shot DML (each opens + commits its own txn) ───────────────────────

    /// `INSERT INTO table (cols...) VALUES ...` with constraints (CONCEPT:EG-020):
    /// NOT NULL, DEFAULT, SERIAL, PK/UNIQUE uniqueness, CHECK. Returns the row count.
    pub fn insert_rows(
        &self,
        table: &str,
        col_order: &[String],
        rows: &[Vec<Value>],
    ) -> Result<usize, String> {
        let wtx = self.begin()?;
        let n = insert_in(&wtx, table, col_order, rows)?;
        wtx.commit().map_err(map_err)?;
        Ok(n)
    }

    /// `UPDATE table SET <set> WHERE <col> = <value>` with constraint re-validation.
    pub fn update_where(
        &self,
        table: &str,
        set: &serde_json::Map<String, Value>,
        selector: &ColEq,
    ) -> Result<usize, String> {
        let wtx = self.begin()?;
        let n = update_in(&wtx, table, set, selector)?;
        wtx.commit().map_err(map_err)?;
        Ok(n)
    }

    /// `DELETE FROM table WHERE <col> = <value>`. Returns the number of rows removed.
    pub fn delete_where(&self, table: &str, selector: &ColEq) -> Result<usize, String> {
        let wtx = self.begin()?;
        let n = delete_in(&wtx, table, selector)?;
        wtx.commit().map_err(map_err)?;
        Ok(n)
    }

    // ── multi-statement transaction (CONCEPT:EG-020) ──────────────────────────

    /// Apply a [`TableTxn`]'s buffered ops in ONE redb `WriteTransaction`
    /// (`BEGIN … COMMIT`). Atomic: a constraint violation (or any error) on ANY op
    /// returns `Err` and the whole transaction is rolled back (the txn drops without
    /// `commit()`, so redb discards every staged write). Returns the total affected
    /// row count of the DML ops. The read-your-writes semantics of a single redb
    /// write txn mean later ops in the batch SEE earlier ops' staged writes (e.g. a
    /// later UNIQUE check sees an earlier insert in the same transaction).
    pub fn commit_txn(&self, txn: &TableTxn) -> Result<usize, String> {
        let wtx = self.begin()?;
        let mut affected = 0usize;
        for op in &txn.ops {
            match op {
                TxnOp::CreateTable {
                    schema,
                    if_not_exists,
                } => {
                    create_in(&wtx, schema, *if_not_exists)?;
                }
                TxnOp::DropTable { name, if_exists } => {
                    drop_in(&wtx, name, *if_exists)?;
                }
                TxnOp::AddColumn { table, column } => {
                    add_column_in(&wtx, table, column)?;
                }
                TxnOp::Insert {
                    table,
                    col_order,
                    rows,
                } => {
                    affected += insert_in(&wtx, table, col_order, rows)?;
                }
                TxnOp::Update {
                    table,
                    set,
                    selector,
                } => {
                    affected += update_in(&wtx, table, set, selector)?;
                }
                TxnOp::Delete { table, selector } => {
                    affected += delete_in(&wtx, table, selector)?;
                }
            }
        }
        wtx.commit().map_err(map_err)?;
        Ok(affected)
    }

    /// Begin an immediate-durability write transaction (commit-before-ack).
    fn begin(&self) -> Result<WriteTransaction, String> {
        let mut wtx = self.db.begin_write().map_err(map_err)?;
        wtx.set_durability(Durability::Immediate).map_err(map_err)?;
        Ok(wtx)
    }
}

// ── txn-scoped helpers (operate on an OPEN WriteTransaction) ──────────────────
//
// These are the single source of truth for every mutation. A one-shot public method
// wraps one in its own begin/commit; `commit_txn` chains several in ONE txn. Reading
// the catalog/rows THROUGH the same `wtx` (read-your-writes) is what lets a later op
// in a multi-statement transaction see an earlier op's staged writes.

/// Read a table schema through an open write txn (sees staged CREATE/ALTER).
fn get_schema_in(wtx: &WriteTransaction, name: &str) -> Result<Option<TableSchema>, String> {
    let cat = match wtx.open_table(CATALOG) {
        Ok(t) => t,
        Err(_) => return Ok(None),
    };
    let blob = match cat.get(name).map_err(map_err)? {
        Some(v) => v.value().to_vec(),
        None => return Ok(None),
    };
    Ok(Some(
        rmp_serde::from_slice(&blob).map_err(|e| format!("decode schema: {e}"))?,
    ))
}

fn create_in(
    wtx: &WriteTransaction,
    schema: &TableSchema,
    if_not_exists: bool,
) -> Result<bool, String> {
    if get_schema_in(wtx, &schema.name)?.is_some() {
        if if_not_exists {
            return Ok(false);
        }
        return Err(format!("table `{}` already exists", schema.name));
    }
    let blob = rmp_serde::to_vec_named(schema).map_err(|e| format!("encode schema: {e}"))?;
    {
        let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
        cat.insert(schema.name.as_str(), blob.as_slice())
            .map_err(map_err)?;
    }
    {
        let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
        seq.insert(schema.name.as_str(), 0u64).map_err(map_err)?;
    }
    Ok(true)
}

fn drop_in(wtx: &WriteTransaction, name: &str, if_exists: bool) -> Result<bool, String> {
    if get_schema_in(wtx, name)?.is_none() {
        if if_exists {
            return Ok(false);
        }
        return Err(format!("table `{name}` does not exist"));
    }
    {
        let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
        cat.remove(name).map_err(map_err)?;
    }
    {
        let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
        seq.remove(name).map_err(map_err)?;
    }
    {
        let mut rows = wtx.open_table(ROWS).map_err(map_err)?;
        let keys: Vec<u64> = rows
            .range((name, 0u64)..=(name, u64::MAX))
            .map_err(map_err)?
            .map(|r| r.map(|(k, _)| k.value().1))
            .collect::<Result<_, _>>()
            .map_err(map_err)?;
        for rowid in keys {
            rows.remove((name, rowid)).map_err(map_err)?;
        }
    }
    Ok(true)
}

fn add_column_in(wtx: &WriteTransaction, table: &str, column: &Column) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    if schema.column(&column.name).is_some() {
        return Err(format!(
            "column `{}` already exists in table `{table}`",
            column.name
        ));
    }
    schema.columns.push(column.clone());
    let blob = rmp_serde::to_vec_named(&schema).map_err(|e| format!("encode schema: {e}"))?;
    let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
    cat.insert(table, blob.as_slice()).map_err(map_err)?;
    Ok(())
}

/// Read+advance the per-table sequence (the `SERIAL`/rowid allocator) by `count`,
/// returning the FIRST allocated value. The rowids `[first, first+count)` are the new
/// rows' physical keys; a SERIAL column reads back `rowid + 1` (1-based, never reused).
fn alloc_rowids(wtx: &WriteTransaction, table: &str, count: u64) -> Result<u64, String> {
    let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
    let first = seq
        .get(table)
        .map_err(map_err)?
        .map(|g| g.value())
        .unwrap_or(0);
    seq.insert(table, first + count).map_err(map_err)?;
    Ok(first)
}

fn insert_in(
    wtx: &WriteTransaction,
    table: &str,
    col_order: &[String],
    rows: &[Vec<Value>],
) -> Result<usize, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let width = schema.columns.len();

    // Resolve each named insert column to a schema index.
    let mut targets = Vec::with_capacity(col_order.len());
    for name in col_order {
        let idx = schema
            .column_index(name)
            .ok_or_else(|| format!("column `{name}` does not exist in table `{table}`"))?;
        targets.push(idx);
    }

    // Allocate the rowid block up front so SERIAL columns get stable, contiguous ids.
    let first_rowid = alloc_rowids(wtx, table, rows.len() as u64)?;

    // Build each row's typed cells BEFORE writing so a coercion/constraint error never
    // leaves a half-applied INSERT (the txn is dropped on Err).
    let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(rows.len());
    for (ri, row) in rows.iter().enumerate() {
        if row.len() != col_order.len() {
            return Err(format!(
                "INSERT column/value count mismatch: {} columns, {} values",
                col_order.len(),
                row.len()
            ));
        }
        let rowid = first_rowid + ri as u64;
        let mut cells: Vec<Cell> = vec![Cell::Null; width];
        // Place supplied values.
        for (val, &idx) in row.iter().zip(targets.iter()) {
            let col = &schema.columns[idx];
            cells[idx] = Cell::coerce(val, col.ty, col.nullable)?;
        }
        // Fill omitted columns: SERIAL → sequence; else DEFAULT; else NULL/NOT NULL.
        for (ci, col) in schema.columns.iter().enumerate() {
            if targets.contains(&ci) {
                continue;
            }
            if col.serial {
                cells[ci] = Cell::coerce(&Value::Number((rowid as i64 + 1).into()), col.ty, false)?;
            } else if let Some(def) = &col.default {
                cells[ci] = Cell::coerce(def, col.ty, col.nullable)?;
            } else if !col.nullable {
                return Err(format!(
                    "column `{}` is NOT NULL and was not supplied",
                    col.name
                ));
            }
        }
        // CHECK constraints (per row, post-fill).
        for (ci, col) in schema.columns.iter().enumerate() {
            if let Some(check) = &col.check {
                if !check.holds(&cells[ci].to_json()) {
                    return Err(format!(
                        "new row violates CHECK constraint on column `{}`",
                        col.name
                    ));
                }
            }
        }
        encoded.push((
            rowid,
            rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?,
        ));
    }

    {
        let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
        for (rowid, blob) in &encoded {
            rows_t
                .insert((table, *rowid), blob.as_slice())
                .map_err(map_err)?;
        }
    }
    // Uniqueness over the post-insert state (reads staged writes through `wtx`).
    validate_uniqueness_in(wtx, table, &schema)?;
    Ok(encoded.len())
}

fn update_in(
    wtx: &WriteTransaction,
    table: &str,
    set: &serde_json::Map<String, Value>,
    selector: &ColEq,
) -> Result<usize, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let sel_idx = schema
        .column_index(&selector.column)
        .ok_or_else(|| format!("column `{}` does not exist", selector.column))?;
    let mut assigns: Vec<(usize, Cell)> = Vec::with_capacity(set.len());
    for (col, val) in set {
        let idx = schema
            .column_index(col)
            .ok_or_else(|| format!("column `{col}` does not exist in table `{table}`"))?;
        let c = &schema.columns[idx];
        assigns.push((idx, Cell::coerce(val, c.ty, c.nullable)?));
    }
    let width = schema.columns.len();
    let mut updated = 0usize;
    {
        let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
        let mut hits: Vec<(u64, Vec<Cell>)> = Vec::new();
        for r in rows_t
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (k, v) = r.map_err(map_err)?;
            let mut cells: Vec<Cell> =
                rmp_serde::from_slice(v.value()).map_err(|e| format!("decode row: {e}"))?;
            if cells.len() < width {
                cells.resize(width, Cell::Null);
            }
            if cells[sel_idx].to_json() == selector.value {
                hits.push((k.value().1, cells));
            }
        }
        for (rowid, mut cells) in hits {
            for (idx, cell) in &assigns {
                cells[*idx] = cell.clone();
            }
            // CHECK constraints on the updated row.
            for (ci, col) in schema.columns.iter().enumerate() {
                if let Some(check) = &col.check {
                    if !check.holds(&cells[ci].to_json()) {
                        return Err(format!(
                            "updated row violates CHECK constraint on column `{}`",
                            col.name
                        ));
                    }
                }
            }
            let blob = rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?;
            rows_t
                .insert((table, rowid), blob.as_slice())
                .map_err(map_err)?;
            updated += 1;
        }
    }
    validate_uniqueness_in(wtx, table, &schema)?;
    Ok(updated)
}

fn delete_in(wtx: &WriteTransaction, table: &str, selector: &ColEq) -> Result<usize, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let sel_idx = schema
        .column_index(&selector.column)
        .ok_or_else(|| format!("column `{}` does not exist", selector.column))?;
    let width = schema.columns.len();
    let mut deleted = 0usize;
    let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    let mut victims: Vec<u64> = Vec::new();
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (k, v) = r.map_err(map_err)?;
        let mut cells: Vec<Cell> =
            rmp_serde::from_slice(v.value()).map_err(|e| format!("decode row: {e}"))?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        if cells[sel_idx].to_json() == selector.value {
            victims.push(k.value().1);
        }
    }
    for rowid in victims {
        rows_t.remove((table, rowid)).map_err(map_err)?;
        deleted += 1;
    }
    Ok(deleted)
}

/// Enforce PK/UNIQUE uniqueness over the table's CURRENT state (reads staged writes
/// through `wtx`). Called AFTER an insert/update stages its writes, so a duplicate
/// returns `Err` and the whole transaction rolls back. NULLs are exempt (SQL allows
/// multiple NULLs in a UNIQUE column; a PK column is NOT NULL and so never NULL here).
fn validate_uniqueness_in(
    wtx: &WriteTransaction,
    table: &str,
    schema: &TableSchema,
) -> Result<(), String> {
    let unique_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_unique())
        .map(|(i, _)| i)
        .collect();
    if unique_cols.is_empty() {
        return Ok(());
    }
    let width = schema.columns.len();
    let rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    let mut seen: Vec<HashSet<String>> = vec![HashSet::new(); unique_cols.len()];
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        let mut cells: Vec<Cell> =
            rmp_serde::from_slice(v.value()).map_err(|e| format!("decode row: {e}"))?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        for (slot, &ci) in unique_cols.iter().enumerate() {
            let v = cells[ci].to_json();
            if v.is_null() {
                continue;
            }
            let key = v.to_string();
            if !seen[slot].insert(key) {
                return Err(format!(
                    "duplicate key value violates unique constraint on column `{}`",
                    schema.columns[ci].name
                ));
            }
        }
    }
    Ok(())
}

/// redb error → flat string (the crate-wide error convention here).
fn map_err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables::schema::{CmpOp, ColCheck, ColumnType};

    fn col(name: &str, ty: ColumnType, nullable: bool) -> Column {
        Column::new(name, ty, nullable, false)
    }

    fn metrics_schema() -> TableSchema {
        TableSchema {
            name: "metrics".to_string(),
            columns: vec![
                col("ts", ColumnType::Timestamp, false),
                col("name", ColumnType::Text, false),
                col("value", ColumnType::Double, true),
            ],
        }
    }

    #[test]
    fn create_insert_scan_roundtrip() {
        let (store, _p) = TableStore::open_temp().unwrap();
        assert!(store.create_table(&metrics_schema(), false).unwrap());
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        let n = store
            .insert_rows(
                "metrics",
                &cols,
                &[
                    vec![1700000000000000i64.into(), "cpu".into(), 0.5.into()],
                    vec![1700000000000001i64.into(), "mem".into(), 0.9.into()],
                ],
            )
            .unwrap();
        assert_eq!(n, 2);
        let rows = store.scan("metrics").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Cell::Timestamp(1700000000000000));
        assert_eq!(rows[0][1], Cell::Text("cpu".into()));
        assert_eq!(rows[0][2], Cell::Float(0.5));
        assert_eq!(rows[1][1], Cell::Text("mem".into()));
    }

    #[test]
    fn update_and_delete_where() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        store
            .insert_rows(
                "metrics",
                &cols,
                &[
                    vec![1i64.into(), "cpu".into(), 0.5.into()],
                    vec![2i64.into(), "cpu".into(), 0.6.into()],
                    vec![3i64.into(), "mem".into(), 0.9.into()],
                ],
            )
            .unwrap();
        let mut set = serde_json::Map::new();
        set.insert("value".into(), 0.0.into());
        let upd = store
            .update_where(
                "metrics",
                &set,
                &ColEq {
                    column: "name".into(),
                    value: "cpu".into(),
                },
            )
            .unwrap();
        assert_eq!(upd, 2);
        let del = store
            .delete_where(
                "metrics",
                &ColEq {
                    column: "name".into(),
                    value: "mem".into(),
                },
            )
            .unwrap();
        assert_eq!(del, 1);
        let rows = store.scan("metrics").unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r[2] == Cell::Float(0.0)));
    }

    #[test]
    fn drop_removes_table_and_rows() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        store
            .insert_rows(
                "metrics",
                &cols,
                &[vec![1i64.into(), "cpu".into(), 0.5.into()]],
            )
            .unwrap();
        assert!(store.drop_table("metrics", false).unwrap());
        assert!(store.get_schema("metrics").unwrap().is_none());
        assert!(store.drop_table("metrics", false).is_err());
        assert!(!store.drop_table("metrics", true).unwrap());
    }

    #[test]
    fn alter_add_column_backfills_null() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        store
            .insert_rows(
                "metrics",
                &cols,
                &[vec![1i64.into(), "cpu".into(), 0.5.into()]],
            )
            .unwrap();
        store
            .add_column("metrics", col("labels", ColumnType::Json, true))
            .unwrap();
        let rows = store.scan("metrics").unwrap();
        assert_eq!(rows[0].len(), 4);
        assert_eq!(
            rows[0][3],
            Cell::Null,
            "pre-existing row reads new column NULL"
        );
        let cols2 = vec![
            "ts".to_string(),
            "name".to_string(),
            "value".to_string(),
            "labels".to_string(),
        ];
        store
            .insert_rows(
                "metrics",
                &cols2,
                &[vec![
                    2i64.into(),
                    "mem".into(),
                    0.9.into(),
                    serde_json::json!({"host": "a"}),
                ]],
            )
            .unwrap();
        let rows = store.scan("metrics").unwrap();
        assert_eq!(rows[1][3], Cell::Json(serde_json::json!({"host": "a"})));
    }

    #[test]
    fn schema_persists_across_reopen() {
        let (store, path) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        store
            .insert_rows(
                "metrics",
                &cols,
                &[vec![42i64.into(), "cpu".into(), 1.0.into()]],
            )
            .unwrap();
        drop(store);
        let store2 = TableStore::open(&path).unwrap();
        let schema = store2.get_schema("metrics").unwrap().unwrap();
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].ty, ColumnType::Timestamp);
        let rows = store2.scan("metrics").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Cell::Timestamp(42));
    }

    #[test]
    fn create_if_not_exists_is_noop() {
        let (store, _p) = TableStore::open_temp().unwrap();
        assert!(store.create_table(&metrics_schema(), false).unwrap());
        assert!(store.create_table(&metrics_schema(), false).is_err());
        assert!(!store.create_table(&metrics_schema(), true).unwrap());
    }

    #[test]
    fn not_null_column_rejected_when_missing() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let err = store
            .insert_rows("metrics", &["value".to_string()], &[vec![0.1.into()]])
            .unwrap_err();
        assert!(err.contains("NOT NULL"), "{err}");
    }

    // ── constraints + transactions + sequences (CONCEPT:EG-020) ───────────────

    /// A table with a SERIAL PK, a UNIQUE text column, a DEFAULT, and a CHECK.
    fn constrained_schema() -> TableSchema {
        let mut id = Column::new("id", ColumnType::BigInt, false, true);
        id.serial = true;
        let mut sku = Column::new("sku", ColumnType::Text, false, false);
        sku.unique = true;
        let mut qty = Column::new("qty", ColumnType::Int, true, false);
        qty.default = Some(Value::Number(0.into()));
        qty.check = Some(ColCheck {
            op: CmpOp::Ge,
            value: Value::Number(0.into()),
        });
        TableSchema {
            name: "items".to_string(),
            columns: vec![id, sku, qty],
        }
    }

    #[test]
    fn serial_and_default_filled_on_insert() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&constrained_schema(), false).unwrap();
        // Supply only `sku`; `id` SERIAL and `qty` DEFAULT auto-fill.
        store
            .insert_rows(
                "items",
                &["sku".into()],
                &[vec!["A".into()], vec!["B".into()]],
            )
            .unwrap();
        let rows = store.scan("items").unwrap();
        assert_eq!(rows[0][0], Cell::Int(1), "SERIAL starts at 1");
        assert_eq!(rows[1][0], Cell::Int(2), "SERIAL increments");
        assert_eq!(rows[0][2], Cell::Int(0), "DEFAULT 0 filled");
    }

    #[test]
    fn unique_violation_rejected() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&constrained_schema(), false).unwrap();
        store
            .insert_rows("items", &["sku".into()], &[vec!["A".into()]])
            .unwrap();
        let err = store
            .insert_rows("items", &["sku".into()], &[vec!["A".into()]])
            .unwrap_err();
        assert!(err.contains("unique"), "{err}");
        // The rejected insert left no row behind (rolled back).
        assert_eq!(store.scan("items").unwrap().len(), 1);
    }

    #[test]
    fn check_violation_rejected() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&constrained_schema(), false).unwrap();
        let err = store
            .insert_rows(
                "items",
                &["sku".into(), "qty".into()],
                &[vec!["A".into(), Value::Number((-5).into())]],
            )
            .unwrap_err();
        assert!(err.contains("CHECK"), "{err}");
    }

    #[test]
    fn txn_commits_all_or_rolls_back_on_error() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&constrained_schema(), false).unwrap();
        // A transaction whose 2nd insert duplicates the UNIQUE sku must roll BOTH back.
        let mut txn = TableTxn::new();
        txn.push(TxnOp::Insert {
            table: "items".into(),
            col_order: vec!["sku".into()],
            rows: vec![vec!["X".into()]],
        });
        txn.push(TxnOp::Insert {
            table: "items".into(),
            col_order: vec!["sku".into()],
            rows: vec![vec!["X".into()]],
        });
        let err = store.commit_txn(&txn).unwrap_err();
        assert!(err.contains("unique"), "{err}");
        assert_eq!(
            store.scan("items").unwrap().len(),
            0,
            "whole txn rolled back"
        );

        // A clean transaction commits all ops atomically.
        let mut ok = TableTxn::new();
        ok.push(TxnOp::Insert {
            table: "items".into(),
            col_order: vec!["sku".into()],
            rows: vec![vec!["Y".into()]],
        });
        ok.push(TxnOp::Insert {
            table: "items".into(),
            col_order: vec!["sku".into()],
            rows: vec![vec!["Z".into()]],
        });
        assert_eq!(store.commit_txn(&ok).unwrap(), 2);
        assert_eq!(store.scan("items").unwrap().len(), 2);
    }
}
