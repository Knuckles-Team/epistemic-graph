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

use super::schema::{Cell, Column, ColumnType, StoredFunction, TableSchema};
// CONCEPT:EG-116/EG-313 — the durable pgvector ANN index registration the exec
// pushdown consults to choose a real eg-ann index over the brute-force scan.
use crate::sql::AnnIndexPlan;

/// Catalog system table: `table_name -> MessagePack(TableSchema)`.
const CATALOG: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_catalog__");
/// Row store: `(table_name, rowid) -> MessagePack(Vec<Cell>)`.
const ROWS: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("__sql_rows__");
/// Per-table rowid allocator (also the `SERIAL` sequence): `table_name -> next rowid`.
const SEQ: TableDefinition<&str, u64> = TableDefinition::new("__sql_seq__");
/// View catalog (CONCEPT:EG-072): `view_name -> SELECT text`. A read-only named query
/// mirrored beside the user-table catalog; expanded during SQL context build.
const VIEWS: TableDefinition<&str, &str> = TableDefinition::new("__sql_views__");
/// Extension catalog (CONCEPT:EG-102): `extension_name -> ""`. Records the extensions a
/// client has `CREATE EXTENSION`-enabled (pgvector, AGE, TimescaleDB, pg_search), so a
/// setup script's enablement is durable across a restart. The value is unused today (a
/// per-extension version/schema is a follow-up); the KEY presence is the enablement.
const EXTENSIONS: TableDefinition<&str, &str> = TableDefinition::new("__sql_extensions__");
/// Function catalog (CONCEPT:EG-118): `function_name -> MessagePack(StoredFunction)`. A
/// SQL-language stored function (`CREATE FUNCTION … LANGUAGE sql`) mirrored beside the
/// view/extension catalogs; expanded into a query at plan time (no separate evaluator).
const FUNCTIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_functions__");
/// pgvector ANN index catalog (CONCEPT:EG-116/EG-313): `index_key -> MessagePack(AnnIndexPlan)`.
/// A `CREATE INDEX … USING hnsw|ivfflat (col opclass)` registers the index here so a
/// `ORDER BY col <-> $1 LIMIT k` query pushes down to a real eg-ann index (EG-313)
/// instead of the EG-115 brute-force scan. Keyed by `"<table>.<column>.<metric>"`
/// (lower-cased) so one column can carry an index per metric; the value is the durable
/// [`AnnIndexPlan`] the exec pushdown consults.
const ANN_INDEXES: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_ann_indexes__");

/// The action an `ON CONFLICT` clause takes for a user-table insert (CONCEPT:EG-048).
/// The store-level mirror of `classify::OnConflictAction` (kept here so the store has
/// no dependency on the SQL classifier layer).
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictAction {
    /// `DO NOTHING` — skip a row that would violate a UNIQUE/PK constraint.
    DoNothing,
    /// `DO UPDATE SET …` — merge these assignments into the conflicting row.
    DoUpdate(serde_json::Map<String, Value>),
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
    /// CONCEPT:EG-310 — `ALTER TABLE … DROP COLUMN`: drop a column from the schema and
    /// its cell from every stored row.
    DropColumn {
        table: String,
        column: String,
        if_exists: bool,
    },
    /// CONCEPT:EG-310 — `ALTER TABLE … RENAME COLUMN a TO b` (rows are positional; no
    /// per-row migration needed).
    RenameColumn {
        table: String,
        from: String,
        to: String,
    },
    /// CONCEPT:EG-310 — `ALTER TABLE … RENAME TO newtable`: rename the catalog entry,
    /// sequence, and every stored row's key.
    RenameTable {
        table: String,
        new_name: String,
    },
    /// CONCEPT:EG-310 — `ALTER TABLE … ALTER COLUMN col TYPE newtype`: change the column
    /// type, best-effort coercing every stored cell (reject on an incompatible value).
    AlterColumnType {
        table: String,
        column: String,
        new_type: ColumnType,
    },
    /// CONCEPT:EG-310 — `ALTER TABLE … DROP CONSTRAINT name`: drop a named constraint.
    DropConstraint {
        table: String,
        constraint: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        col_order: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Update {
        table: String,
        set: serde_json::Map<String, Value>,
        selector: eg_types::RowPredicate,
    },
    Delete {
        table: String,
        selector: eg_types::RowPredicate,
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

    /// CONCEPT:EG-310 — `ALTER TABLE DROP COLUMN`: remove `column` from the schema and
    /// drop its cell from every stored row, atomically in one write txn. Errors if the
    /// column (or table) does not exist unless `if_exists`.
    pub fn drop_column(&self, table: &str, column: &str, if_exists: bool) -> Result<(), String> {
        let wtx = self.begin()?;
        drop_column_in(&wtx, table, column, if_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-310 — `ALTER TABLE RENAME COLUMN a TO b`: rename a column in place.
    /// Stored rows are positional so they need no migration. Errors if `from` is absent
    /// or `to` already exists.
    pub fn rename_column(&self, table: &str, from: &str, to: &str) -> Result<(), String> {
        let wtx = self.begin()?;
        rename_column_in(&wtx, table, from, to)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-310 — `ALTER TABLE RENAME TO newtable`: move the table's catalog entry,
    /// sequence, and every stored row's key to `new_name`, atomically. Errors if the
    /// table is absent or `new_name` already exists.
    pub fn rename_table(&self, table: &str, new_name: &str) -> Result<(), String> {
        let wtx = self.begin()?;
        rename_table_in(&wtx, table, new_name)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-310 — `ALTER TABLE ALTER COLUMN col TYPE newtype`: change a column's
    /// declared type and best-effort coerce every stored cell to it, atomically. A cell
    /// that cannot be coerced aborts (and rolls back) the whole change.
    pub fn alter_column_type(
        &self,
        table: &str,
        column: &str,
        new_type: ColumnType,
    ) -> Result<(), String> {
        let wtx = self.begin()?;
        alter_column_type_in(&wtx, table, column, new_type)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-310 — `ALTER TABLE DROP CONSTRAINT name`: drop the named constraint
    /// (matched against Postgres's synthesized names — `<table>_pkey`, `<table>_<col>_key`,
    /// `<table>_<col>_check`). Errors if no such constraint exists unless `if_exists`.
    pub fn drop_constraint(
        &self,
        table: &str,
        constraint: &str,
        if_exists: bool,
    ) -> Result<(), String> {
        let wtx = self.begin()?;
        drop_constraint_in(&wtx, table, constraint, if_exists)?;
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
        Ok(self.insert_rows_returning(table, col_order, rows)?.len())
    }

    /// `INSERT …` returning the inserted rows' typed cells (CONCEPT:EG-048 RETURNING).
    pub fn insert_rows_returning(
        &self,
        table: &str,
        col_order: &[String],
        rows: &[Vec<Value>],
    ) -> Result<Vec<Vec<Cell>>, String> {
        let wtx = self.begin()?;
        let out = insert_in(&wtx, table, col_order, rows)?;
        wtx.commit().map_err(map_err)?;
        Ok(out)
    }

    /// `INSERT … ON CONFLICT (…) DO NOTHING|DO UPDATE` (CONCEPT:EG-048). Returns the
    /// rows inserted-or-updated (for `RETURNING`).
    pub fn insert_rows_on_conflict(
        &self,
        table: &str,
        col_order: &[String],
        rows: &[Vec<Value>],
        action: &ConflictAction,
    ) -> Result<Vec<Vec<Cell>>, String> {
        let wtx = self.begin()?;
        let out = insert_on_conflict_in(&wtx, table, col_order, rows, action)?;
        wtx.commit().map_err(map_err)?;
        Ok(out)
    }

    /// `UPDATE table SET <set> WHERE <predicate>` with constraint re-validation
    /// (CONCEPT:EG-045 — a compound predicate evaluated per row inside the txn).
    pub fn update_where(
        &self,
        table: &str,
        set: &serde_json::Map<String, Value>,
        selector: &eg_types::RowPredicate,
    ) -> Result<usize, String> {
        Ok(self.update_where_returning(table, set, selector)?.len())
    }

    /// `UPDATE … WHERE …` returning the post-update rows (CONCEPT:EG-048 RETURNING).
    pub fn update_where_returning(
        &self,
        table: &str,
        set: &serde_json::Map<String, Value>,
        selector: &eg_types::RowPredicate,
    ) -> Result<Vec<Vec<Cell>>, String> {
        let wtx = self.begin()?;
        let out = update_in(&wtx, table, set, selector)?;
        wtx.commit().map_err(map_err)?;
        Ok(out)
    }

    /// `DELETE FROM table WHERE <predicate>` (CONCEPT:EG-045). Returns rows removed.
    pub fn delete_where(
        &self,
        table: &str,
        selector: &eg_types::RowPredicate,
    ) -> Result<usize, String> {
        Ok(self.delete_where_returning(table, selector)?.len())
    }

    /// `DELETE … WHERE …` returning the removed rows as they were BEFORE removal
    /// (CONCEPT:EG-048 RETURNING).
    pub fn delete_where_returning(
        &self,
        table: &str,
        selector: &eg_types::RowPredicate,
    ) -> Result<Vec<Vec<Cell>>, String> {
        let wtx = self.begin()?;
        let out = delete_in(&wtx, table, selector)?;
        wtx.commit().map_err(map_err)?;
        Ok(out)
    }

    // ── view catalog (CONCEPT:EG-072) ─────────────────────────────────────────

    /// `CREATE [OR REPLACE] VIEW name AS <select>`: record `select_sql` in the view
    /// catalog. Errors if the name already exists and `or_replace` is false, or if a
    /// user table already claims the name (a view and a table cannot share a name).
    pub fn create_view(
        &self,
        name: &str,
        select_sql: &str,
        or_replace: bool,
    ) -> Result<(), String> {
        if self.get_schema(name)?.is_some() {
            return Err(format!(
                "`{name}` is a table; cannot create a view with that name"
            ));
        }
        let wtx = self.begin()?;
        {
            let mut views = wtx.open_table(VIEWS).map_err(map_err)?;
            if !or_replace && views.get(name).map_err(map_err)?.is_some() {
                return Err(format!("view `{name}` already exists"));
            }
            views.insert(name, select_sql).map_err(map_err)?;
        }
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// `DROP VIEW [IF EXISTS] name`: remove the view catalog entry. `Ok(true)` when a
    /// view was removed, `Ok(false)` when absent and `if_exists` was set.
    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let existed = {
            let mut views = wtx.open_table(VIEWS).map_err(map_err)?;
            let existed = views.get(name).map_err(map_err)?.is_some();
            if !existed {
                if if_exists {
                    drop(views);
                    wtx.commit().map_err(map_err)?;
                    return Ok(false);
                }
                return Err(format!("view `{name}` does not exist"));
            }
            views.remove(name).map_err(map_err)?;
            existed
        };
        wtx.commit().map_err(map_err)?;
        Ok(existed)
    }

    /// The stored SELECT text of view `name`, or `None` if no such view exists.
    pub fn get_view(&self, name: &str) -> Result<Option<String>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let views = match rtx.open_table(VIEWS) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        Ok(views
            .get(name)
            .map_err(map_err)?
            .map(|v| v.value().to_string()))
    }

    /// Every view as `(name, select_sql)` (sorted by name for determinism).
    pub fn list_views(&self) -> Result<Vec<(String, String)>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let views = match rtx.open_table(VIEWS) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut out: Vec<(String, String)> = views
            .iter()
            .map_err(map_err)?
            .map(|r| r.map(|(k, v)| (k.value().to_string(), v.value().to_string())))
            .collect::<Result<_, _>>()
            .map_err(map_err)?;
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    // ── extension catalog (CONCEPT:EG-102) ────────────────────────────────────

    /// `CREATE EXTENSION [IF NOT EXISTS] name`: record `name` as enabled. `Ok(true)`
    /// when newly enabled, `Ok(false)` when it was already enabled (idempotent — no
    /// error even without `IF NOT EXISTS`, matching Postgres' `CREATE EXTENSION`
    /// which errors only on a genuine re-create; the wire shim treats an existing
    /// extension as a benign success so a re-run setup script proceeds).
    pub fn create_extension(&self, name: &str, _if_not_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let created = {
            let mut exts = wtx.open_table(EXTENSIONS).map_err(map_err)?;
            let existed = exts.get(name).map_err(map_err)?.is_some();
            if !existed {
                exts.insert(name, "").map_err(map_err)?;
            }
            !existed
        };
        wtx.commit().map_err(map_err)?;
        Ok(created)
    }

    /// `DROP EXTENSION [IF EXISTS] name`: remove the catalog entry. `Ok(true)` when an
    /// extension was removed, `Ok(false)` when absent and `if_exists` was set (else Err).
    pub fn drop_extension(&self, name: &str, if_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let existed = {
            let mut exts = wtx.open_table(EXTENSIONS).map_err(map_err)?;
            let existed = exts.get(name).map_err(map_err)?.is_some();
            if !existed {
                if if_exists {
                    drop(exts);
                    wtx.commit().map_err(map_err)?;
                    return Ok(false);
                }
                return Err(format!("extension `{name}` does not exist"));
            }
            exts.remove(name).map_err(map_err)?;
            existed
        };
        wtx.commit().map_err(map_err)?;
        Ok(existed)
    }

    /// Whether extension `name` is currently enabled.
    pub fn has_extension(&self, name: &str) -> Result<bool, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let exts = match rtx.open_table(EXTENSIONS) {
            Ok(t) => t,
            Err(_) => return Ok(false),
        };
        Ok(exts.get(name).map_err(map_err)?.is_some())
    }

    /// Every enabled extension name (sorted for determinism).
    pub fn list_extensions(&self) -> Result<Vec<String>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let exts = match rtx.open_table(EXTENSIONS) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut out: Vec<String> = exts
            .iter()
            .map_err(map_err)?
            .map(|r| r.map(|(k, _)| k.value().to_string()))
            .collect::<Result<_, _>>()
            .map_err(map_err)?;
        out.sort();
        Ok(out)
    }

    // ── function catalog (CONCEPT:EG-118) ──────────────────────────────────────

    /// `CREATE [OR REPLACE] FUNCTION name(...) RETURNS … AS $$ … $$ LANGUAGE sql`:
    /// record `func` in the durable function catalog. Errors if the name already exists
    /// and `or_replace` is false, or if a user table already claims the name (a function
    /// and a table cannot share a name — a call `name(args)` would be ambiguous).
    pub fn create_function(&self, func: &StoredFunction, or_replace: bool) -> Result<(), String> {
        if self.get_schema(&func.name)?.is_some() {
            return Err(format!(
                "`{}` is a table; cannot create a function with that name",
                func.name
            ));
        }
        let bytes = rmp_serde::to_vec_named(func).map_err(|e| format!("encode function: {e}"))?;
        let wtx = self.begin()?;
        {
            let mut funcs = wtx.open_table(FUNCTIONS).map_err(map_err)?;
            if !or_replace && funcs.get(func.name.as_str()).map_err(map_err)?.is_some() {
                return Err(format!("function `{}` already exists", func.name));
            }
            funcs
                .insert(func.name.as_str(), bytes.as_slice())
                .map_err(map_err)?;
        }
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// `DROP FUNCTION [IF EXISTS] name`: remove the function catalog entry. `Ok(true)`
    /// when a function was removed, `Ok(false)` when absent and `if_exists` was set.
    pub fn drop_function(&self, name: &str, if_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let existed = {
            let mut funcs = wtx.open_table(FUNCTIONS).map_err(map_err)?;
            let existed = funcs.get(name).map_err(map_err)?.is_some();
            if !existed {
                if if_exists {
                    drop(funcs);
                    wtx.commit().map_err(map_err)?;
                    return Ok(false);
                }
                return Err(format!("function `{name}` does not exist"));
            }
            funcs.remove(name).map_err(map_err)?;
            existed
        };
        wtx.commit().map_err(map_err)?;
        Ok(existed)
    }

    /// The stored definition of function `name`, or `None` if no such function exists.
    pub fn get_function(&self, name: &str) -> Result<Option<StoredFunction>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let funcs = match rtx.open_table(FUNCTIONS) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        match funcs.get(name).map_err(map_err)? {
            Some(v) => {
                let f: StoredFunction = rmp_serde::from_slice(v.value())
                    .map_err(|e| format!("decode function: {e}"))?;
                Ok(Some(f))
            }
            None => Ok(None),
        }
    }

    /// Every stored function (sorted by name for determinism) — the set the SQL exec
    /// path expands into a query at plan time (CONCEPT:EG-118).
    pub fn list_functions(&self) -> Result<Vec<StoredFunction>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let funcs = match rtx.open_table(FUNCTIONS) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut out: Vec<StoredFunction> = funcs
            .iter()
            .map_err(map_err)?
            .map(|r| {
                r.map_err(map_err).and_then(|(_, v)| {
                    rmp_serde::from_slice::<StoredFunction>(v.value())
                        .map_err(|e| format!("decode function: {e}"))
                })
            })
            .collect::<Result<_, _>>()?;
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    // ── pgvector ANN index catalog (CONCEPT:EG-116/EG-313) ─────────────────────

    /// The catalog key for an ANN index: `"<table>.<column>.<metric>"` (lower-cased),
    /// so one column may register a separate index per distance metric.
    fn ann_index_key(plan: &AnnIndexPlan) -> String {
        format!(
            "{}.{}.{:?}",
            plan.table.to_ascii_lowercase(),
            plan.column.to_ascii_lowercase(),
            plan.metric
        )
    }

    /// `CREATE INDEX … USING hnsw|ivfflat (col opclass)` (CONCEPT:EG-116): register the
    /// [`AnnIndexPlan`] so a matching `ORDER BY col <-> $1 LIMIT k` pushes down to a real
    /// eg-ann index (CONCEPT:EG-313). Idempotent on `if_not_exists` (a re-register of the
    /// same key is a benign success); an existing key without `if_not_exists` is replaced
    /// (the newest DDL wins — pgvector's `CREATE INDEX` build is idempotent in practice).
    pub fn put_ann_index(&self, plan: &AnnIndexPlan) -> Result<(), String> {
        let key = Self::ann_index_key(plan);
        let bytes = rmp_serde::to_vec_named(plan).map_err(|e| format!("encode ann index: {e}"))?;
        let wtx = self.begin()?;
        {
            let mut idxs = wtx.open_table(ANN_INDEXES).map_err(map_err)?;
            idxs.insert(key.as_str(), bytes.as_slice())
                .map_err(map_err)?;
        }
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// `DROP INDEX name` (CONCEPT:EG-116): remove every ANN index registered for
    /// `table`.`column` (all metrics). `Ok(n)` = number of entries removed.
    pub fn drop_ann_indexes_for_column(&self, table: &str, column: &str) -> Result<usize, String> {
        let prefix = format!(
            "{}.{}.",
            table.to_ascii_lowercase(),
            column.to_ascii_lowercase()
        );
        let wtx = self.begin()?;
        let removed = {
            let mut idxs = wtx.open_table(ANN_INDEXES).map_err(map_err)?;
            let keys: Vec<String> = idxs
                .iter()
                .map_err(map_err)?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .filter(|k| k.starts_with(&prefix))
                .collect();
            for k in &keys {
                idxs.remove(k.as_str()).map_err(map_err)?;
            }
            keys.len()
        };
        wtx.commit().map_err(map_err)?;
        Ok(removed)
    }

    /// Every registered ANN index (sorted by key for determinism) — the set the SQL
    /// exec path consults to decide the pgvector pushdown (CONCEPT:EG-313).
    pub fn list_ann_indexes(&self) -> Result<Vec<AnnIndexPlan>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let idxs = match rtx.open_table(ANN_INDEXES) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut pairs: Vec<(String, AnnIndexPlan)> = idxs
            .iter()
            .map_err(map_err)?
            .map(|r| {
                r.map_err(map_err).and_then(|(k, v)| {
                    rmp_serde::from_slice::<AnnIndexPlan>(v.value())
                        .map(|p| (k.value().to_string(), p))
                        .map_err(|e| format!("decode ann index: {e}"))
                })
            })
            .collect::<Result<_, _>>()?;
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pairs.into_iter().map(|(_, p)| p).collect())
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
                // CONCEPT:EG-310 — ALTER TABLE ops staged into a multi-statement txn.
                TxnOp::DropColumn {
                    table,
                    column,
                    if_exists,
                } => {
                    drop_column_in(&wtx, table, column, *if_exists)?;
                }
                TxnOp::RenameColumn { table, from, to } => {
                    rename_column_in(&wtx, table, from, to)?;
                }
                TxnOp::RenameTable { table, new_name } => {
                    rename_table_in(&wtx, table, new_name)?;
                }
                TxnOp::AlterColumnType {
                    table,
                    column,
                    new_type,
                } => {
                    alter_column_type_in(&wtx, table, column, *new_type)?;
                }
                TxnOp::DropConstraint {
                    table,
                    constraint,
                    if_exists,
                } => {
                    drop_constraint_in(&wtx, table, constraint, *if_exists)?;
                }
                TxnOp::Insert {
                    table,
                    col_order,
                    rows,
                } => {
                    affected += insert_in(&wtx, table, col_order, rows)?.len();
                }
                TxnOp::Update {
                    table,
                    set,
                    selector,
                } => {
                    affected += update_in(&wtx, table, set, selector)?.len();
                }
                TxnOp::Delete { table, selector } => {
                    affected += delete_in(&wtx, table, selector)?.len();
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
    put_schema_in(wtx, &schema)
}

/// Persist a (possibly renamed) schema back into the catalog under its `name` key.
/// The single place an ALTER rewrites the catalog entry (CONCEPT:EG-310).
fn put_schema_in(wtx: &WriteTransaction, schema: &TableSchema) -> Result<(), String> {
    let blob = rmp_serde::to_vec_named(schema).map_err(|e| format!("encode schema: {e}"))?;
    let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
    cat.insert(schema.name.as_str(), blob.as_slice())
        .map_err(map_err)?;
    Ok(())
}

/// Rewrite every stored row of `table` through `f` (which mutates the row's `Vec<Cell>`
/// in place), inside the open write txn — the atomic row-migration primitive shared by
/// DROP COLUMN and ALTER COLUMN TYPE (CONCEPT:EG-310). An error from `f` on ANY row
/// propagates so the whole ALTER rolls back (the txn drops without commit).
fn migrate_rows_in(
    wtx: &WriteTransaction,
    table: &str,
    mut f: impl FnMut(&mut Vec<Cell>) -> Result<(), String>,
) -> Result<(), String> {
    let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    // Decode every (rowid, cells) first; the range borrow ends before we mutate.
    let mut items: Vec<(u64, Vec<Cell>)> = Vec::new();
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (k, v) = r.map_err(map_err)?;
        let cells: Vec<Cell> =
            rmp_serde::from_slice(v.value()).map_err(|e| format!("decode row: {e}"))?;
        items.push((k.value().1, cells));
    }
    for (rowid, mut cells) in items {
        f(&mut cells)?;
        let blob = rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?;
        rows_t
            .insert((table, rowid), blob.as_slice())
            .map_err(map_err)?;
    }
    Ok(())
}

/// CONCEPT:EG-310 — `DROP COLUMN`: remove `column` from the schema and drop its cell
/// from every stored row (positional splice at the column's index). Refuses to drop the
/// only column of a table. `if_exists` turns an absent-column error into a no-op.
fn drop_column_in(
    wtx: &WriteTransaction,
    table: &str,
    column: &str,
    if_exists: bool,
) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let idx = match schema.column_index(column) {
        Some(i) => i,
        None => {
            if if_exists {
                return Ok(());
            }
            return Err(format!(
                "column `{column}` does not exist in table `{table}`"
            ));
        }
    };
    if schema.columns.len() == 1 {
        return Err(format!(
            "cannot drop the only column `{column}` of table `{table}`"
        ));
    }
    schema.columns.remove(idx);
    put_schema_in(wtx, &schema)?;
    // Splice the dropped cell out of each stored row (rows may be short if written before
    // a later ADD COLUMN — guard the index).
    migrate_rows_in(wtx, table, |cells| {
        if idx < cells.len() {
            cells.remove(idx);
        }
        Ok(())
    })
}

/// CONCEPT:EG-310 — `RENAME COLUMN a TO b`: rename in the schema only (rows are
/// positional). Errors if `from` is absent or `to` already exists.
fn rename_column_in(
    wtx: &WriteTransaction,
    table: &str,
    from: &str,
    to: &str,
) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    if from != to && schema.column(to).is_some() {
        return Err(format!("column `{to}` already exists in table `{table}`"));
    }
    let idx = schema
        .column_index(from)
        .ok_or_else(|| format!("column `{from}` does not exist in table `{table}`"))?;
    schema.columns[idx].name = to.to_string();
    put_schema_in(wtx, &schema)
}

/// CONCEPT:EG-310 — `RENAME TO newtable`: move the catalog entry, the sequence, and
/// every stored row's key from `table` to `new_name`. Errors if the table is absent or
/// `new_name` already exists.
fn rename_table_in(wtx: &WriteTransaction, table: &str, new_name: &str) -> Result<(), String> {
    if table == new_name {
        return Ok(());
    }
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    if get_schema_in(wtx, new_name)?.is_some() {
        return Err(format!("table `{new_name}` already exists"));
    }
    // Catalog: drop the old key, write the schema under the new name.
    schema.name = new_name.to_string();
    {
        let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
        cat.remove(table).map_err(map_err)?;
    }
    put_schema_in(wtx, &schema)?;
    // Sequence: carry the rowid allocator forward so SERIAL ids never collide/reuse.
    {
        let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
        let val = seq.get(table).map_err(map_err)?.map(|g| g.value());
        seq.remove(table).map_err(map_err)?;
        if let Some(v) = val {
            seq.insert(new_name, v).map_err(map_err)?;
        }
    }
    // Rows: re-key each (table, rowid) → (new_name, rowid).
    {
        let mut rows = wtx.open_table(ROWS).map_err(map_err)?;
        let mut items: Vec<(u64, Vec<u8>)> = Vec::new();
        for r in rows
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (k, v) = r.map_err(map_err)?;
            items.push((k.value().1, v.value().to_vec()));
        }
        for (rowid, blob) in &items {
            rows.remove((table, *rowid)).map_err(map_err)?;
            rows.insert((new_name, *rowid), blob.as_slice())
                .map_err(map_err)?;
        }
    }
    Ok(())
}

/// CONCEPT:EG-310 — `ALTER COLUMN col TYPE newtype`: best-effort coerce every stored
/// cell at the column's index to `new_type`, then record the new type. A cell that
/// cannot be coerced returns `Err` so the whole ALTER rolls back (no partial migration).
fn alter_column_type_in(
    wtx: &WriteTransaction,
    table: &str,
    column: &str,
    new_type: ColumnType,
) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let idx = schema
        .column_index(column)
        .ok_or_else(|| format!("column `{column}` does not exist in table `{table}`"))?;
    let nullable = schema.columns[idx].nullable;
    // Migrate rows FIRST so an incompatible value aborts before the schema is touched.
    migrate_rows_in(wtx, table, |cells| {
        if let Some(cell) = cells.get_mut(idx) {
            *cell = coerce_cell(cell, new_type, nullable)
                .map_err(|e| format!("cannot ALTER COLUMN `{column}` TYPE: {e}"))?;
        }
        Ok(())
    })?;
    schema.columns[idx].ty = new_type;
    put_schema_in(wtx, &schema)
}

/// CONCEPT:EG-310 — `DROP CONSTRAINT name`: this catalog stores constraints per column
/// (PK / UNIQUE / CHECK) without user-visible names, so a dropped constraint is matched
/// against Postgres's synthesized names — `<table>_pkey`, `<table>_<col>_key`,
/// `<table>_<col>_check` — and the matching column flag is cleared. Errors if nothing
/// matches unless `if_exists`.
fn drop_constraint_in(
    wtx: &WriteTransaction,
    table: &str,
    constraint: &str,
    if_exists: bool,
) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let mut matched = false;
    if constraint == format!("{table}_pkey") {
        for c in &mut schema.columns {
            if c.primary_key {
                c.primary_key = false;
                c.unique = false;
                matched = true;
            }
        }
    }
    if !matched {
        for c in &mut schema.columns {
            if constraint == format!("{table}_{}_key", c.name) && c.is_unique() {
                c.unique = false;
                c.primary_key = false;
                matched = true;
            } else if constraint == format!("{table}_{}_check", c.name) && c.check.is_some() {
                c.check = None;
                matched = true;
            }
        }
    }
    if !matched {
        if if_exists {
            return Ok(());
        }
        return Err(format!(
            "constraint `{constraint}` does not exist on table `{table}`"
        ));
    }
    put_schema_in(wtx, &schema)
}

/// Best-effort coerce an already-stored [`Cell`] to `ty` for an `ALTER COLUMN … TYPE`
/// migration (CONCEPT:EG-310). A cell already of the target shape is kept verbatim; a
/// NULL passes through subject to `nullable`; otherwise the value is rendered into a
/// JSON value tuned for the target and run through the SAME [`Cell::coerce`] the write
/// path uses — so an incompatible value (e.g. `'abc'` → int) is rejected identically.
fn coerce_cell(old: &Cell, ty: ColumnType, nullable: bool) -> Result<Cell, String> {
    if matches!(old, Cell::Null) {
        return Cell::coerce(&Value::Null, ty, nullable);
    }
    if cell_matches_type(old, ty) {
        return Ok(old.clone());
    }
    let v = coercion_value(old, ty);
    Cell::coerce(&v, ty, nullable)
}

/// Whether `cell`'s stored shape already matches `ty` (so an ALTER TYPE is a no-op for
/// that cell). `Int`/`BigInt` share `Cell::Int`; `Float`/`Double` share `Cell::Float`.
fn cell_matches_type(cell: &Cell, ty: ColumnType) -> bool {
    matches!(
        (cell, ty),
        (Cell::Int(_), ColumnType::Int | ColumnType::BigInt)
            | (Cell::Timestamp(_), ColumnType::Timestamp)
            | (Cell::Float(_), ColumnType::Float | ColumnType::Double)
            | (Cell::Text(_), ColumnType::Text)
            | (Cell::Bool(_), ColumnType::Bool)
            | (Cell::Bytes(_), ColumnType::Bytes)
            | (Cell::Json(_), ColumnType::Json)
            | (Cell::Vector(_), ColumnType::Vector(_))
    )
}

/// Render `old` into a JSON value best-tuned for coercion into `ty` (CONCEPT:EG-310):
/// numeric text is parsed into a JSON number, integral floats become integers, booleans
/// map to 0/1, etc. Anything that cannot be represented falls back to the cell's plain
/// JSON form, so the downstream [`Cell::coerce`] produces a precise rejection error.
fn coercion_value(old: &Cell, ty: ColumnType) -> Value {
    let json_f64 = |f: f64| {
        serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    };
    match ty {
        ColumnType::Int | ColumnType::BigInt | ColumnType::Timestamp => match old {
            Cell::Int(i) | Cell::Timestamp(i) => Value::Number((*i).into()),
            Cell::Float(f) if f.fract() == 0.0 && f.is_finite() => {
                Value::Number((*f as i64).into())
            }
            Cell::Bool(b) => Value::Number((*b as i64).into()),
            Cell::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(|n| Value::Number(n.into()))
                .unwrap_or_else(|_| Value::String(s.clone())),
            other => other.to_json(),
        },
        ColumnType::Float | ColumnType::Double => match old {
            Cell::Int(i) | Cell::Timestamp(i) => json_f64(*i as f64),
            Cell::Float(f) => json_f64(*f),
            Cell::Text(s) => s
                .trim()
                .parse::<f64>()
                .map(json_f64)
                .unwrap_or_else(|_| Value::String(s.clone())),
            other => other.to_json(),
        },
        ColumnType::Bool => match old {
            Cell::Bool(b) => Value::Bool(*b),
            Cell::Int(i) => Value::Bool(*i != 0),
            Cell::Text(s) => parse_bool_text(s)
                .map(Value::Bool)
                .unwrap_or_else(|| Value::String(s.clone())),
            other => other.to_json(),
        },
        // Text / Json / Bytes / Vector reuse the cell's plain JSON form; `Cell::coerce`
        // renders a scalar into text, parses a string into bytes, etc.
        _ => old.to_json(),
    }
}

/// Parse the common SQL/Postgres textual boolean spellings for an ALTER-TYPE migration
/// (CONCEPT:EG-310). Returns `None` for an unrecognized spelling (→ rejected downstream).
fn parse_bool_text(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
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
) -> Result<Vec<Vec<Cell>>, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;

    // Resolve each named insert column to a schema index.
    let targets = resolve_targets(&schema, table, col_order)?;

    // Allocate the rowid block up front so SERIAL columns get stable, contiguous ids.
    let first_rowid = alloc_rowids(wtx, table, rows.len() as u64)?;

    // Build each row's typed cells BEFORE writing so a coercion/constraint error never
    // leaves a half-applied INSERT (the txn is dropped on Err).
    let mut inserted: Vec<Vec<Cell>> = Vec::with_capacity(rows.len());
    let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(rows.len());
    for (ri, row) in rows.iter().enumerate() {
        let rowid = first_rowid + ri as u64;
        let cells = build_insert_cells(&schema, col_order, &targets, row, rowid)?;
        encoded.push((
            rowid,
            rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?,
        ));
        inserted.push(cells);
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
    Ok(inserted)
}

/// Resolve a named insert column list to schema indices (shared by the plain and
/// ON CONFLICT insert paths).
fn resolve_targets(
    schema: &TableSchema,
    table: &str,
    col_order: &[String],
) -> Result<Vec<usize>, String> {
    let mut targets = Vec::with_capacity(col_order.len());
    for name in col_order {
        let idx = schema
            .column_index(name)
            .ok_or_else(|| format!("column `{name}` does not exist in table `{table}`"))?;
        targets.push(idx);
    }
    Ok(targets)
}

/// Build one row's typed, schema-aligned cells: place supplied values, fill omitted
/// columns (SERIAL → the allocated `rowid+1`; else DEFAULT; else NULL / reject if NOT
/// NULL), and enforce per-column CHECK constraints. Shared by the plain and ON CONFLICT
/// insert paths (CONCEPT:EG-048).
fn build_insert_cells(
    schema: &TableSchema,
    col_order: &[String],
    targets: &[usize],
    row: &[Value],
    rowid: u64,
) -> Result<Vec<Cell>, String> {
    let width = schema.columns.len();
    if row.len() != col_order.len() {
        return Err(format!(
            "INSERT column/value count mismatch: {} columns, {} values",
            col_order.len(),
            row.len()
        ));
    }
    let mut cells: Vec<Cell> = vec![Cell::Null; width];
    for (val, &idx) in row.iter().zip(targets.iter()) {
        let col = &schema.columns[idx];
        cells[idx] = Cell::coerce(val, col.ty, col.nullable)?;
    }
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
    Ok(cells)
}

/// `INSERT … ON CONFLICT (…) DO NOTHING|DO UPDATE` (CONCEPT:EG-048). For each row: if a
/// UNIQUE/PK column value already exists (in the committed OR same-batch state), apply
/// the conflict action — skip (DO NOTHING) or merge the SET assignments into the
/// existing row (DO UPDATE); otherwise insert a fresh row. Returns the rows that were
/// inserted-or-updated (for `RETURNING`). Reuses [`validate_uniqueness_in`] as the final
/// integrity gate so a DO UPDATE that itself introduces a duplicate still aborts.
fn insert_on_conflict_in(
    wtx: &WriteTransaction,
    table: &str,
    col_order: &[String],
    rows: &[Vec<Value>],
    action: &ConflictAction,
) -> Result<Vec<Vec<Cell>>, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let width = schema.columns.len();
    let targets = resolve_targets(&schema, table, col_order)?;
    let unique_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_unique())
        .map(|(i, _)| i)
        .collect();

    // Current unique-value snapshot (committed + staged), rebuilt from the store. When
    // the physical row table does not exist yet there are simply no existing rows.
    let mut existing: Vec<(u64, Vec<Cell>)> = Vec::new();
    if let Ok(rows_t) = wtx.open_table(ROWS) {
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
            existing.push((k.value().1, cells));
        }
    }

    let mut affected: Vec<Vec<Cell>> = Vec::new();
    for row in rows {
        // Coerce the supplied unique-column values to detect a conflict.
        let mut conflict_rowid: Option<u64> = None;
        'find: for &uci in &unique_cols {
            // The value this row supplies for the unique column (if any).
            let Some(pos) = targets.iter().position(|&t| t == uci) else {
                continue;
            };
            let col = &schema.columns[uci];
            let supplied = Cell::coerce(&row[pos], col.ty, col.nullable)?;
            if supplied == Cell::Null {
                continue;
            }
            for (rid, cells) in &existing {
                if cells.get(uci) == Some(&supplied) {
                    conflict_rowid = Some(*rid);
                    break 'find;
                }
            }
        }

        match (conflict_rowid, action) {
            (Some(_), ConflictAction::DoNothing) => { /* skip */ }
            (Some(rid), ConflictAction::DoUpdate(set)) => {
                // Merge the SET assignments into the conflicting row.
                let slot = existing
                    .iter_mut()
                    .find(|(r, _)| *r == rid)
                    .expect("conflict rowid present");
                for (col, val) in set {
                    let idx = schema.column_index(col).ok_or_else(|| {
                        format!("column `{col}` does not exist in table `{table}`")
                    })?;
                    let c = &schema.columns[idx];
                    slot.1[idx] = Cell::coerce(val, c.ty, c.nullable)?;
                }
                // Re-check CHECK constraints on the updated row.
                for (ci, col) in schema.columns.iter().enumerate() {
                    if let Some(check) = &col.check {
                        if !check.holds(&slot.1[ci].to_json()) {
                            return Err(format!(
                                "updated row violates CHECK constraint on column `{}`",
                                col.name
                            ));
                        }
                    }
                }
                let blob =
                    rmp_serde::to_vec_named(&slot.1).map_err(|e| format!("encode row: {e}"))?;
                let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
                rows_t
                    .insert((table, rid), blob.as_slice())
                    .map_err(map_err)?;
                affected.push(slot.1.clone());
            }
            (None, _) => {
                // A fresh insert: allocate one rowid, build + write the row.
                let rowid = alloc_rowids(wtx, table, 1)?;
                let cells = build_insert_cells(&schema, col_order, &targets, row, rowid)?;
                let blob =
                    rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?;
                {
                    let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
                    rows_t
                        .insert((table, rowid), blob.as_slice())
                        .map_err(map_err)?;
                }
                existing.push((rowid, cells.clone()));
                affected.push(cells);
            }
        }
    }
    validate_uniqueness_in(wtx, table, &schema)?;
    Ok(affected)
}

/// Build a `col -> json` row map for predicate evaluation (CONCEPT:EG-045): one
/// entry per schema column, the cell decoded to its JSON value. A column the
/// predicate references that is NOT in the schema is simply absent (reads as NULL).
fn row_map(schema: &TableSchema, cells: &[Cell]) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::with_capacity(schema.columns.len());
    for (ci, col) in schema.columns.iter().enumerate() {
        let cell = cells.get(ci).cloned().unwrap_or(Cell::Null);
        map.insert(col.name.clone(), cell.to_json());
    }
    map
}

fn update_in(
    wtx: &WriteTransaction,
    table: &str,
    set: &serde_json::Map<String, Value>,
    selector: &eg_types::RowPredicate,
) -> Result<Vec<Vec<Cell>>, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let mut assigns: Vec<(usize, Cell)> = Vec::with_capacity(set.len());
    for (col, val) in set {
        let idx = schema
            .column_index(col)
            .ok_or_else(|| format!("column `{col}` does not exist in table `{table}`"))?;
        let c = &schema.columns[idx];
        assigns.push((idx, Cell::coerce(val, c.ty, c.nullable)?));
    }
    let width = schema.columns.len();
    let mut updated: Vec<Vec<Cell>> = Vec::new();
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
            // CONCEPT:EG-045 — serializable per-row predicate eval INSIDE the open
            // redb write txn (the row cannot change before commit).
            if selector.eval(&row_map(&schema, &cells)) {
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
            updated.push(cells);
        }
    }
    validate_uniqueness_in(wtx, table, &schema)?;
    Ok(updated)
}

fn delete_in(
    wtx: &WriteTransaction,
    table: &str,
    selector: &eg_types::RowPredicate,
) -> Result<Vec<Vec<Cell>>, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let width = schema.columns.len();
    // Capture the pre-removal cells (CONCEPT:EG-048 — DELETE … RETURNING sees the row
    // as it was before deletion).
    let mut removed: Vec<Vec<Cell>> = Vec::new();
    let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    let mut victims: Vec<(u64, Vec<Cell>)> = Vec::new();
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
        // CONCEPT:EG-045 — serializable per-row predicate eval inside the write txn.
        if selector.eval(&row_map(&schema, &cells)) {
            victims.push((k.value().1, cells));
        }
    }
    for (rowid, cells) in victims {
        rows_t.remove((table, rowid)).map_err(map_err)?;
        removed.push(cells);
    }
    Ok(removed)
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
                &eg_types::RowPredicate::Cmp {
                    col: "name".into(),
                    op: eg_types::CmpOp::Eq,
                    value: "cpu".into(),
                },
            )
            .unwrap();
        assert_eq!(upd, 2);
        let del = store
            .delete_where(
                "metrics",
                &eg_types::RowPredicate::Cmp {
                    col: "name".into(),
                    op: eg_types::CmpOp::Eq,
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
    fn compound_predicate_update_and_delete() {
        // CONCEPT:EG-045 — AND / range / IN predicates select rows in the store.
        use eg_types::{CmpOp, RowPredicate};
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        store
            .insert_rows(
                "metrics",
                &cols,
                &[
                    vec![1i64.into(), "cpu".into(), 0.2.into()],
                    vec![2i64.into(), "cpu".into(), 0.8.into()],
                    vec![3i64.into(), "mem".into(), 0.9.into()],
                    vec![4i64.into(), "disk".into(), 0.5.into()],
                ],
            )
            .unwrap();
        // UPDATE … WHERE name = 'cpu' AND value > 0.5  → only row 2.
        let mut set = serde_json::Map::new();
        set.insert("value".into(), 0.0.into());
        let n = store
            .update_where(
                "metrics",
                &set,
                &RowPredicate::And(vec![
                    RowPredicate::Cmp {
                        col: "name".into(),
                        op: CmpOp::Eq,
                        value: "cpu".into(),
                    },
                    RowPredicate::Cmp {
                        col: "value".into(),
                        op: CmpOp::Gt,
                        value: 0.5.into(),
                    },
                ]),
            )
            .unwrap();
        assert_eq!(n, 1);
        // DELETE … WHERE name IN ('mem', 'disk') → rows 3 and 4.
        let n = store
            .delete_where(
                "metrics",
                &RowPredicate::In {
                    col: "name".into(),
                    values: vec!["mem".into(), "disk".into()],
                },
            )
            .unwrap();
        assert_eq!(n, 2);
        let rows = store.scan("metrics").unwrap();
        assert_eq!(rows.len(), 2);
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

    // ── ON CONFLICT + RETURNING (CONCEPT:EG-048) ──────────────────────────────

    #[test]
    fn on_conflict_do_nothing_skips_duplicate() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&constrained_schema(), false).unwrap();
        store
            .insert_rows("items", &["sku".into()], &[vec!["A".into()]])
            .unwrap();
        // A duplicate `sku` under DO NOTHING is skipped (not an error, no new row).
        let affected = store
            .insert_rows_on_conflict(
                "items",
                &["sku".into()],
                &[vec!["A".into()], vec!["B".into()]],
                &ConflictAction::DoNothing,
            )
            .unwrap();
        assert_eq!(affected.len(), 1, "only the non-conflicting `B` inserted");
        assert_eq!(store.scan("items").unwrap().len(), 2);
    }

    #[test]
    fn on_conflict_do_update_merges_existing() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&constrained_schema(), false).unwrap();
        store
            .insert_rows(
                "items",
                &["sku".into(), "qty".into()],
                &[vec!["A".into(), 1.into()]],
            )
            .unwrap();
        let mut set = serde_json::Map::new();
        set.insert("qty".into(), 42.into());
        let affected = store
            .insert_rows_on_conflict(
                "items",
                &["sku".into(), "qty".into()],
                &[vec!["A".into(), 9.into()]],
                &ConflictAction::DoUpdate(set),
            )
            .unwrap();
        assert_eq!(affected.len(), 1);
        let rows = store.scan("items").unwrap();
        assert_eq!(rows.len(), 1, "no new row — the existing one was updated");
        assert_eq!(rows[0][2], Cell::Int(42), "DO UPDATE merged qty");
    }

    #[test]
    fn insert_update_delete_returning_rows() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        let ins = store
            .insert_rows_returning(
                "metrics",
                &cols,
                &[vec![1i64.into(), "cpu".into(), 0.5.into()]],
            )
            .unwrap();
        assert_eq!(ins.len(), 1);
        assert_eq!(ins[0][1], Cell::Text("cpu".into()));

        let mut set = serde_json::Map::new();
        set.insert("value".into(), 0.9.into());
        let upd = store
            .update_where_returning(
                "metrics",
                &set,
                &eg_types::RowPredicate::Cmp {
                    col: "name".into(),
                    op: eg_types::CmpOp::Eq,
                    value: "cpu".into(),
                },
            )
            .unwrap();
        assert_eq!(upd.len(), 1);
        assert_eq!(
            upd[0][2],
            Cell::Float(0.9),
            "RETURNING sees post-update value"
        );

        let del = store
            .delete_where_returning(
                "metrics",
                &eg_types::RowPredicate::Cmp {
                    col: "name".into(),
                    op: eg_types::CmpOp::Eq,
                    value: "cpu".into(),
                },
            )
            .unwrap();
        assert_eq!(del.len(), 1);
        assert_eq!(
            del[0][2],
            Cell::Float(0.9),
            "RETURNING sees pre-removal row"
        );
        assert_eq!(store.scan("metrics").unwrap().len(), 0);
    }

    // ── view catalog (CONCEPT:EG-072) ─────────────────────────────────────────

    #[test]
    fn view_catalog_create_get_drop() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store
            .create_view("agents", "SELECT id FROM nodes WHERE type = 'Agent'", false)
            .unwrap();
        assert_eq!(
            store.get_view("agents").unwrap().as_deref(),
            Some("SELECT id FROM nodes WHERE type = 'Agent'")
        );
        // Duplicate without OR REPLACE errors; with OR REPLACE it overwrites.
        assert!(store.create_view("agents", "SELECT 1", false).is_err());
        store
            .create_view("agents", "SELECT id FROM nodes", true)
            .unwrap();
        assert_eq!(store.list_views().unwrap().len(), 1);
        assert!(store.drop_view("agents", false).unwrap());
        assert!(store.get_view("agents").unwrap().is_none());
        assert!(store.drop_view("agents", false).is_err());
        assert!(!store.drop_view("agents", true).unwrap());
    }

    #[test]
    fn view_persists_across_reopen() {
        let (store, path) = TableStore::open_temp().unwrap();
        store
            .create_view("v", "SELECT id FROM nodes", false)
            .unwrap();
        drop(store);
        let store2 = TableStore::open(&path).unwrap();
        assert_eq!(
            store2.get_view("v").unwrap().as_deref(),
            Some("SELECT id FROM nodes")
        );
    }

    // ── function catalog (CONCEPT:EG-118) ──────────────────────────────────────

    fn sample_add_fn() -> StoredFunction {
        use super::super::schema::{FunctionArg, FunctionLanguage, FunctionReturns};
        StoredFunction {
            name: "add".to_string(),
            args: vec![
                FunctionArg {
                    name: "a".to_string(),
                    type_name: "int".to_string(),
                },
                FunctionArg {
                    name: "b".to_string(),
                    type_name: "int".to_string(),
                },
            ],
            returns: FunctionReturns::Scalar("int".to_string()),
            body: "SELECT a + b".to_string(),
            language: FunctionLanguage::Sql,
        }
    }

    #[test]
    fn function_catalog_create_get_drop_eg118() {
        let (store, _p) = TableStore::open_temp().unwrap();
        let f = sample_add_fn();
        store.create_function(&f, false).unwrap();
        assert_eq!(store.get_function("add").unwrap().as_ref(), Some(&f));
        // Duplicate without OR REPLACE errors; with OR REPLACE it overwrites.
        assert!(store.create_function(&f, false).is_err());
        store.create_function(&f, true).unwrap();
        assert_eq!(store.list_functions().unwrap().len(), 1);
        assert!(store.drop_function("add", false).unwrap());
        assert!(store.get_function("add").unwrap().is_none());
        assert!(store.drop_function("add", false).is_err());
        assert!(!store.drop_function("add", true).unwrap());
    }

    #[test]
    fn function_persists_across_reopen_eg118() {
        let (store, path) = TableStore::open_temp().unwrap();
        let f = sample_add_fn();
        store.create_function(&f, false).unwrap();
        drop(store);
        let store2 = TableStore::open(&path).unwrap();
        assert_eq!(store2.get_function("add").unwrap().as_ref(), Some(&f));
    }
}
