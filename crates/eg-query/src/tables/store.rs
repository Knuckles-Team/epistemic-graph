//! The redb-backed user-table store (CONCEPT:EG-KG.query.register-user-tables-alongside; relational completeness
//! CONCEPT:EG-KG.query.register-each-user-table) — the durable, ACID home for ARBITRARY user-defined relational
//! tables (Prometheus metrics, Langfuse time-series, stock bars, connector mirrors,
//! ETL outputs).
//!
//! ## redb layout (one `Database` file)
//!   * `__sql_catalog__`  `table_name              -> MessagePack(TableSchema)`
//!     The catalog. One row per user table, recording its full typed schema.
//!   * `__sql_rows__`     `(table_name, rowid: u64) -> MessagePack(Vec<Cell>)`
//!     Every user table's rows live in ONE physical redb table, namespaced by the
//!     logical table name in the composite key (redb's `&'static str` table-name
//!     constraint rules out a per-table `TableDefinition`), keeping each logical
//!     table's rows contiguous for an efficient prefix range-scan.
//!   * `__sql_seq__`      `table_name              -> next rowid (u64)`
//!     A per-table monotonic rowid allocator — the internal row identity AND the
//!     surface exposed as `SERIAL`/`DEFAULT nextval` (CONCEPT:EG-KG.query.register-each-user-table).
//!   * `__sql_secondary_indexes__` / `__sql_secondary_index_entries__`
//!     owner-scoped scalar index definitions and schema-bound B-tree directory
//!     entries. These are optional row-reduction structures; a digest/version
//!     mismatch always falls back to the authoritative rows.
//!   * `__sql_mutation_*` batch/status, idempotency, SQL-domain version/fence,
//!     and immutable outbox tables. These are committed in the same transaction as
//!     table/catalog mutations, enabling exact retry and restart reconciliation.
//!
//! ## Durability + transactions (CONCEPT:EG-KG.query.register-each-user-table)
//! Every one-shot mutation runs in ONE redb `WriteTransaction` committed at
//! `Durability::Immediate` BEFORE the method returns (commit-before-ack). A
//! mid-transaction error drops the txn without `commit()`, so redb discards every
//! staged write (true rollback, no partial). A MULTI-statement transaction
//! (`BEGIN … COMMIT`) is modeled as a [`TableTxn`] op buffer applied via
//! [`TableStore::commit_txn`] in ONE redb `WriteTransaction`: the whole batch is
//! atomic and a constraint violation on ANY op aborts (and rolls back) the lot.
//!
//! ## Constraints (CONCEPT:EG-KG.query.register-each-user-table)
//! `NOT NULL` (rejected at coerce time), column `DEFAULT` (filled when a column is
//! omitted), `SERIAL`/`DEFAULT nextval` (auto-assigned from the per-table sequence),
//! `PRIMARY KEY`/`UNIQUE` uniqueness, and a simple `CHECK (col OP literal)` are all
//! enforced on the write path; a violation aborts the (one-shot or multi-statement)
//! transaction.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use eg_types::mutation_batch::{
    MutationBatch, MutationBatchCommit, MutationBatchRecord, MutationBatchStatus, MutationDomain,
    MutationOutboxIntent, MutationOutboxRecord, MutationVersionScope, MUTATION_BATCH_VERSION,
    NON_GRAPH_SOURCE_VERSION,
};
use redb::{
    Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition,
    WriteTransaction,
};
use serde_json::Value;

use super::schema::{
    Cell, Column, ColumnType, RefAction, StoredFunction, TableConstraint, TableSchema,
};
use super::index::{
    catalog_key as secondary_catalog_key, entry_key as secondary_entry_key,
    entry_prefix as secondary_entry_prefix, entry_range as secondary_entry_range,
    rowid_from_entry_key, validate_spec as validate_secondary_spec, SecondaryIndexLookup,
    SecondaryIndexOrder, SecondaryIndexSpec, MAX_SECONDARY_INDEX_BUILD_ROWS,
    MAX_SECONDARY_INDEX_CANDIDATES, MAX_SECONDARY_INDEXES_PER_TABLE,
};
// CONCEPT:EG-KG.query.real-ann-top-k/EG-313 — the durable pgvector ANN index registration the exec
// pushdown consults to choose a real eg-ann index over the brute-force scan.
use crate::sql::{AnnIndexPlan, HypertablePlan};

/// Catalog system table: `table_name -> MessagePack(TableSchema)`.
const CATALOG: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_catalog__");
/// Row store: `(table_name, rowid) -> MessagePack(Vec<Cell>)`.
const ROWS: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("__sql_rows__");
/// Per-table rowid allocator (also the `SERIAL` sequence): `table_name -> next rowid`.
const SEQ: TableDefinition<&str, u64> = TableDefinition::new("__sql_seq__");
/// View catalog (CONCEPT:EG-KG.query.create-drop-view): `view_name -> SELECT text`. A read-only named query
/// mirrored beside the user-table catalog; expanded during SQL context build.
const VIEWS: TableDefinition<&str, &str> = TableDefinition::new("__sql_views__");
/// Extension catalog (CONCEPT:EG-KG.query.create-drop-extension-over): `extension_name -> ""`. Records the extensions a
/// client has `CREATE EXTENSION`-enabled (pgvector, AGE, TimescaleDB, pg_search), so a
/// setup script's enablement is durable across a restart. The value is unused today (a
/// per-extension version/schema is a follow-up); the KEY presence is the enablement.
const EXTENSIONS: TableDefinition<&str, &str> = TableDefinition::new("__sql_extensions__");
/// Function catalog (CONCEPT:EG-KG.query.create-drop-function): `function_name -> MessagePack(StoredFunction)`. A
/// SQL-language stored function (`CREATE FUNCTION … LANGUAGE sql`) mirrored beside the
/// view/extension catalogs; expanded into a query at plan time (no separate evaluator).
const FUNCTIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_functions__");
/// pgvector ANN index catalog (CONCEPT:EG-KG.query.real-ann-top-k/EG-313): `index_key -> MessagePack(AnnIndexPlan)`.
/// A `CREATE INDEX … USING hnsw|ivfflat (col opclass)` registers the index here so a
/// `ORDER BY col <-> $1 LIMIT k` query pushes down to a real eg-ann index (EG-313)
/// instead of the EG-115 brute-force scan. Keyed by `"<table>.<column>.<metric>"`
/// (lower-cased) so one column can carry an index per metric; the value is the durable
/// [`AnnIndexPlan`] the exec pushdown consults.
const ANN_INDEXES: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_ann_indexes__");
/// Ordinary scalar secondary-index catalog: `scope\0table\0name -> MessagePack(SecondaryIndexSpec)`.
const SECONDARY_INDEXES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__sql_secondary_indexes__");
/// Ordinary scalar secondary-index directory: `catalog_key\0hex(value)\0rowid -> empty`.
/// Keeping the directory separate from the schema catalog lets a stale definition
/// fail closed without ever changing the authoritative row store.
const SECONDARY_INDEX_ENTRIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__sql_secondary_index_entries__");
/// Timescale-compatible hypertable catalog: `table_name -> MessagePack(HypertablePlan)`.
const HYPERTABLES: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_hypertables__");
/// Universal SQL-domain mutation status/result rows.
const MUTATION_BATCHES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__sql_mutation_batches__");
/// `(tenant, graph, idempotency_key) -> batch_id`.
const MUTATION_IDEMPOTENCY: TableDefinition<(&str, &str, &str), &str> =
    TableDefinition::new("__sql_mutation_idempotency__");
/// SQL catalog/data OCC version, independent from the graph-row version.
const MUTATION_VERSION: TableDefinition<(&str, &str), u64> =
    TableDefinition::new("__sql_mutation_version__");
/// SQL-domain placement/worker fence.
const MUTATION_FENCE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("__sql_mutation_fence__");
/// Immutable transactional outbox rows.
const MUTATION_OUTBOX: TableDefinition<(&str, u32), &[u8]> =
    TableDefinition::new("__sql_mutation_outbox__");
const MAX_SQL_STORED_VALUE_BYTES: usize = 64 * 1024 * 1024;
const MAX_SQL_STORED_VALUE_ITEMS: usize = 1_000_000;
const MAX_SQL_SCAN_ROWS: usize = 100_000;
const MAX_SQL_SCAN_BYTES: usize = 64 * 1024 * 1024;
const INITIAL_SQL_DOMAIN_VERSION: u64 = 0;

fn decode_stored<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    kind: &'static str,
) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_SQL_STORED_VALUE_BYTES,
            MAX_SQL_STORED_VALUE_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| format!("stored SQL {kind} is invalid or exceeds resource limits"))
}

fn decode_mutation_record(bytes: &[u8]) -> Result<MutationBatchRecord, String> {
    let record: MutationBatchRecord = decode_stored(bytes, "mutation record")?;
    record.batch.validate()?;
    Ok(record)
}

fn decode_mutation_outbox(bytes: &[u8]) -> Result<MutationOutboxRecord, String> {
    let record: MutationOutboxRecord = decode_stored(bytes, "mutation outbox record")?;
    record.validate()?;
    if record.version_scope != MutationVersionScope::NonGraph
        || record.source_graph_version != NON_GRAPH_SOURCE_VERSION
    {
        return Err("SQL mutation store contains a graph-scoped outbox record".to_string());
    }
    Ok(record)
}

fn account_collection(count: &mut usize, bytes: &mut usize, added: usize) -> Result<(), String> {
    *count = (*count)
        .checked_add(1)
        .filter(|count| *count <= MAX_SQL_SCAN_ROWS)
        .ok_or_else(|| "SQL collection row limit exceeded".to_string())?;
    *bytes = (*bytes)
        .checked_add(added)
        .filter(|bytes| *bytes <= MAX_SQL_SCAN_BYTES)
        .ok_or_else(|| "SQL collection byte limit exceeded".to_string())?;
    Ok(())
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct SqlMutationFence {
    placement_epoch: u64,
    fencing_token: u64,
}

/// Failure-injection boundaries proving SQL rows/catalog and coordinator metadata
/// recover as one unit. Production always supplies `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlMutationCrashpoint {
    BeforeRows,
    AfterRowsBeforeMetadata,
    BeforeCommit,
    AfterCommitBeforeAck,
}

/// The action an `ON CONFLICT` clause takes for a user-table insert (CONCEPT:EG-KG.query.delete-returning-sees-row).
/// The store-level mirror of `classify::OnConflictAction` (kept here so the store has
/// no dependency on the SQL classifier layer).
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictAction {
    /// `DO NOTHING` — skip a row that would violate a UNIQUE/PK constraint.
    DoNothing,
    /// `DO UPDATE SET …` — merge these assignments into the conflicting row.
    DoUpdate(serde_json::Map<String, Value>),
}

/// One staged operation in a multi-statement transaction (CONCEPT:EG-KG.query.register-each-user-table). Buffered
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
    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE … DROP COLUMN`: drop a column from the schema and
    /// its cell from every stored row.
    DropColumn {
        table: String,
        column: String,
        if_exists: bool,
    },
    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE … RENAME COLUMN a TO b` (rows are positional; no
    /// per-row migration needed).
    RenameColumn {
        table: String,
        from: String,
        to: String,
    },
    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE … RENAME TO newtable`: rename the catalog entry,
    /// sequence, and every stored row's key.
    RenameTable {
        table: String,
        new_name: String,
    },
    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE … ALTER COLUMN col TYPE newtype`: change the column
    /// type, best-effort coercing every stored cell (reject on an incompatible value).
    AlterColumnType {
        table: String,
        column: String,
        new_type: ColumnType,
    },
    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE … DROP CONSTRAINT name`: drop a named constraint.
    DropConstraint {
        table: String,
        constraint: String,
        if_exists: bool,
    },
    /// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — `ALTER TABLE … ADD CONSTRAINT`: append a table-level
    /// constraint to an existing table.
    AddConstraint {
        table: String,
        constraint: TableConstraint,
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
    CreateView {
        name: String,
        select_sql: String,
        or_replace: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    CreateExtension {
        name: String,
        if_not_exists: bool,
    },
    DropExtension {
        name: String,
        if_exists: bool,
    },
    CreateFunction {
        function: StoredFunction,
        or_replace: bool,
    },
    DropFunction {
        name: String,
        if_exists: bool,
    },
    PutAnnIndex {
        plan: AnnIndexPlan,
    },
    PutHypertable {
        plan: HypertablePlan,
    },
    DropAnnIndexesForColumn {
        table: String,
        column: String,
    },
}

/// A buffered multi-statement transaction (CONCEPT:EG-KG.query.register-each-user-table). `BEGIN` creates one;
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

/// One durable user-table catalog. Cheap to clone (`Arc<Database>`), so the served
/// owner-scoped registry can hand the verified tenant+actor's handle to each
/// surface — including a [`crate::sql::providers::EdgesTableProvider`]-style lazy
/// `TableProvider` (`crate::tables::provider::UserTableProvider`) that holds its
/// own owned handle instead of a borrow. `Debug` is derived (via `redb::Database`'s
/// own impl) because `datafusion::catalog::TableProvider` requires it.
#[derive(Debug, Clone)]
pub struct TableStore {
    db: Arc<Database>,
    /// Owner/tenant namespace for secondary-index catalog keys. `open()` keeps
    /// legacy callers isolated to one stable default; multiplexed services use
    /// `open_scoped()` and must provide the authenticated tenant scope.
    index_scope: Arc<String>,
}

impl TableStore {
    /// Open (creating if absent) the user-table store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_scoped(path, "__legacy_store__")
    }

    /// Open a store with an explicit owner scope for its secondary-index
    /// catalog. The scope is persisted in every index identity and is checked
    /// on CREATE/read/drop, preventing a cross-tenant name collision when a
    /// physical redb file is shared by more than one owner.
    pub fn open_scoped(
        path: impl AsRef<Path>,
        tenant_scope: impl Into<String>,
    ) -> Result<Self, String> {
        let tenant_scope = tenant_scope.into();
        if tenant_scope.is_empty() || tenant_scope.contains('\0') {
            return Err("SQL table-store tenant scope must be non-empty and NUL-free".to_string());
        }
        let db = Database::create(path).map_err(|e| format!("open sql table store: {e}"))?;
        Ok(Self {
            db: Arc::new(db),
            index_scope: Arc::new(tenant_scope),
        })
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

    /// The authenticated owner namespace used by secondary-index DDL. This is
    /// intentionally not derived from an untrusted SQL identifier.
    pub fn index_scope(&self) -> &str {
        self.index_scope.as_str()
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
        let dropped = drop_in(&wtx, self.index_scope(), name, if_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(dropped)
    }

    /// `ALTER TABLE ADD COLUMN`: append `column` to the table's schema.
    pub fn add_column(&self, table: &str, column: Column) -> Result<(), String> {
        let wtx = self.begin()?;
        add_column_in(&wtx, self.index_scope(), table, &column)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE DROP COLUMN`: remove `column` from the schema and
    /// drop its cell from every stored row, atomically in one write txn. Errors if the
    /// column (or table) does not exist unless `if_exists`.
    pub fn drop_column(&self, table: &str, column: &str, if_exists: bool) -> Result<(), String> {
        let wtx = self.begin()?;
        drop_column_in(&wtx, self.index_scope(), table, column, if_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE RENAME COLUMN a TO b`: rename a column in place.
    /// Stored rows are positional so they need no migration. Errors if `from` is absent
    /// or `to` already exists.
    pub fn rename_column(&self, table: &str, from: &str, to: &str) -> Result<(), String> {
        let wtx = self.begin()?;
        rename_column_in(&wtx, self.index_scope(), table, from, to)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE RENAME TO newtable`: move the table's catalog entry,
    /// sequence, and every stored row's key to `new_name`, atomically. Errors if the
    /// table is absent or `new_name` already exists.
    pub fn rename_table(&self, table: &str, new_name: &str) -> Result<(), String> {
        let wtx = self.begin()?;
        rename_table_in(&wtx, self.index_scope(), table, new_name)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE ALTER COLUMN col TYPE newtype`: change a column's
    /// declared type and best-effort coerce every stored cell to it, atomically. A cell
    /// that cannot be coerced aborts (and rolls back) the whole change.
    pub fn alter_column_type(
        &self,
        table: &str,
        column: &str,
        new_type: ColumnType,
    ) -> Result<(), String> {
        let wtx = self.begin()?;
        alter_column_type_in(&wtx, self.index_scope(), table, column, new_type)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE DROP CONSTRAINT name`: drop the named constraint
    /// (matched against Postgres's synthesized names — `<table>_pkey`, `<table>_<col>_key`,
    /// `<table>_<col>_check`, `<table>_<col>_fkey`). Errors if no such constraint
    /// exists unless `if_exists`.
    pub fn drop_constraint(
        &self,
        table: &str,
        constraint: &str,
        if_exists: bool,
    ) -> Result<(), String> {
        let wtx = self.begin()?;
        drop_constraint_in(&wtx, self.index_scope(), table, constraint, if_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — `ALTER TABLE ADD CONSTRAINT`: append a table-level
    /// constraint (composite PK/UNIQUE, FOREIGN KEY, or general CHECK) to an
    /// already-created table, atomically. Runs the SAME structural + cross-table
    /// validation `CREATE TABLE` runs (FK target existence/uniqueness, at most one
    /// PK, …) and, for PK/UNIQUE/CHECK, re-validates every EXISTING row against the
    /// new constraint before committing — matching Postgres's `ADD CONSTRAINT`
    /// behavior of refusing to add a constraint the current data already violates.
    pub fn add_constraint(&self, table: &str, constraint: TableConstraint) -> Result<(), String> {
        let wtx = self.begin()?;
        add_constraint_in(&wtx, self.index_scope(), table, constraint)?;
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
                let schema: TableSchema = decode_stored(v.value(), "schema")?;
                schema.validate()?;
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
        let mut names = Vec::new();
        let mut count = 0usize;
        let mut bytes = 0usize;
        for row in cat.iter().map_err(map_err)? {
            let (key, _) = row.map_err(map_err)?;
            account_collection(&mut count, &mut bytes, key.value().len())?;
            names.push(key.value().to_string());
        }
        names.sort();
        Ok(names)
    }

    /// Every row of `table`, each a schema-aligned `Vec<Cell>` (NULL-padded to the
    /// current schema width). Errors if the table does not exist.
    pub fn scan(&self, table: &str) -> Result<Vec<Vec<Cell>>, String> {
        let schema = self
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        let width = schema.columns().len();
        let rtx = self.db.begin_read().map_err(map_err)?;
        // The physical row table is created lazily on the first committed INSERT; a
        // table that only ever had its schema created (or whose inserts rolled back)
        // has no `__sql_rows__` table yet → an empty scan, not an error.
        let rows = match rtx.open_table(ROWS) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        let mut encoded_bytes = 0usize;
        for r in rows
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (_, v) = r.map_err(map_err)?;
            encoded_bytes = encoded_bytes
                .checked_add(v.value().len())
                .filter(|bytes| *bytes <= MAX_SQL_SCAN_BYTES)
                .ok_or_else(|| "SQL scan response exceeds resource limits".to_string())?;
            if out.len() >= MAX_SQL_SCAN_ROWS {
                return Err("SQL scan row limit exceeded".to_string());
            }
            let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
            if cells.len() < width {
                cells.resize(width, Cell::Null);
            }
            out.push(cells);
        }
        Ok(out)
    }

    /// A single row by its PHYSICAL rowid — a true redb POINT GET (CONCEPT:EG-KG.query.register-user-tables-alongside),
    /// O(log n) via the B-tree rather than [`Self::scan`]'s full O(n) table walk.
    /// `Ok(None)` when no row lives at `rowid` (never allocated, or since removed —
    /// rowids are never reused, see the module doc). NULL-padded to the current
    /// schema width, exactly like `scan`.
    ///
    /// The only place a caller computes a TARGET `rowid` from a SQL value is
    /// `crate::tables::provider::UserTableProvider`, which maps a `SERIAL` column's
    /// value back to `rowid` (`value - 1`, CONCEPT:EG-KG.query.register-each-user-table) and then RE-CHECKS the
    /// fetched row's own cell before trusting it — a caller MAY supply (INSERT) or
    /// later (UPDATE) an explicit value into a nominally-`SERIAL` column, same as
    /// Postgres, which would otherwise silently diverge that mapping for one row.
    /// This method itself makes no such assumption; it is a plain point lookup.
    pub fn get_row(&self, table: &str, rowid: u64) -> Result<Option<Vec<Cell>>, String> {
        let schema = self
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        let width = schema.columns().len();
        let rtx = self.db.begin_read().map_err(map_err)?;
        // Mirrors `scan`: no committed INSERT yet ⇒ no physical row table ⇒ no row.
        let rows = match rtx.open_table(ROWS) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        match rows.get((table, rowid)).map_err(map_err)? {
            Some(v) => {
                let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
                if cells.len() < width {
                    cells.resize(width, Cell::Null);
                }
                Ok(Some(cells))
            }
            None => Ok(None),
        }
    }

    // ── one-shot DML (each opens + commits its own txn) ───────────────────────

    /// `INSERT INTO table (cols...) VALUES ...` with constraints (CONCEPT:EG-KG.query.register-each-user-table):
    /// NOT NULL, DEFAULT, SERIAL, PK/UNIQUE uniqueness, CHECK. Returns the row count.
    pub fn insert_rows(
        &self,
        table: &str,
        col_order: &[String],
        rows: &[Vec<Value>],
    ) -> Result<usize, String> {
        Ok(self.insert_rows_returning(table, col_order, rows)?.len())
    }

    /// `INSERT …` returning the inserted rows' typed cells (CONCEPT:EG-KG.query.delete-returning-sees-row RETURNING).
    pub fn insert_rows_returning(
        &self,
        table: &str,
        col_order: &[String],
        rows: &[Vec<Value>],
    ) -> Result<Vec<Vec<Cell>>, String> {
        let wtx = self.begin()?;
        let out = insert_in(&wtx, self.index_scope(), table, col_order, rows)?;
        wtx.commit().map_err(map_err)?;
        Ok(out)
    }

    /// `INSERT … ON CONFLICT (…) DO NOTHING|DO UPDATE` (CONCEPT:EG-KG.query.delete-returning-sees-row). Returns the
    /// rows inserted-or-updated (for `RETURNING`).
    pub fn insert_rows_on_conflict(
        &self,
        table: &str,
        col_order: &[String],
        rows: &[Vec<Value>],
        action: &ConflictAction,
    ) -> Result<Vec<Vec<Cell>>, String> {
        let wtx = self.begin()?;
        let out = insert_on_conflict_in(&wtx, self.index_scope(), table, col_order, rows, action)?;
        wtx.commit().map_err(map_err)?;
        Ok(out)
    }

    /// `UPDATE table SET <set> WHERE <predicate>` with constraint re-validation
    /// (CONCEPT:EG-KG.query.compound-predicate-decode — a compound predicate evaluated per row inside the txn).
    pub fn update_where(
        &self,
        table: &str,
        set: &serde_json::Map<String, Value>,
        selector: &eg_types::RowPredicate,
    ) -> Result<usize, String> {
        Ok(self.update_where_returning(table, set, selector)?.len())
    }

    /// `UPDATE … WHERE …` returning the post-update rows (CONCEPT:EG-KG.query.delete-returning-sees-row RETURNING).
    pub fn update_where_returning(
        &self,
        table: &str,
        set: &serde_json::Map<String, Value>,
        selector: &eg_types::RowPredicate,
    ) -> Result<Vec<Vec<Cell>>, String> {
        let wtx = self.begin()?;
        let out = update_in(&wtx, self.index_scope(), table, set, selector)?;
        wtx.commit().map_err(map_err)?;
        Ok(out)
    }

    /// `DELETE FROM table WHERE <predicate>` (CONCEPT:EG-KG.query.compound-predicate-decode). Returns rows removed.
    pub fn delete_where(
        &self,
        table: &str,
        selector: &eg_types::RowPredicate,
    ) -> Result<usize, String> {
        Ok(self.delete_where_returning(table, selector)?.len())
    }

    /// `DELETE … WHERE …` returning the removed rows as they were BEFORE removal
    /// (CONCEPT:EG-KG.query.delete-returning-sees-row RETURNING).
    pub fn delete_where_returning(
        &self,
        table: &str,
        selector: &eg_types::RowPredicate,
    ) -> Result<Vec<Vec<Cell>>, String> {
        let wtx = self.begin()?;
        let out = delete_in(&wtx, self.index_scope(), table, selector)?;
        wtx.commit().map_err(map_err)?;
        Ok(out)
    }

    // ── view catalog (CONCEPT:EG-KG.query.create-drop-view) ─────────────────────────────────────────

    /// `CREATE [OR REPLACE] VIEW name AS <select>`: record `select_sql` in the view
    /// catalog. Errors if the name already exists and `or_replace` is false, or if a
    /// user table already claims the name (a view and a table cannot share a name).
    pub fn create_view(
        &self,
        name: &str,
        select_sql: &str,
        or_replace: bool,
    ) -> Result<(), String> {
        let wtx = self.begin()?;
        create_view_in(&wtx, name, select_sql, or_replace)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// `DROP VIEW [IF EXISTS] name`: remove the view catalog entry. `Ok(true)` when a
    /// view was removed, `Ok(false)` when absent and `if_exists` was set.
    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let existed = drop_view_in(&wtx, name, if_exists)?;
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
        let mut out = Vec::new();
        let mut count = 0usize;
        let mut bytes = 0usize;
        for row in views.iter().map_err(map_err)? {
            let (key, value) = row.map_err(map_err)?;
            let added = key
                .value()
                .len()
                .checked_add(value.value().len())
                .ok_or_else(|| "SQL collection byte limit exceeded".to_string())?;
            account_collection(&mut count, &mut bytes, added)?;
            out.push((key.value().to_string(), value.value().to_string()));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    // ── extension catalog (CONCEPT:EG-KG.query.create-drop-extension-over) ────────────────────────────────────

    /// `CREATE EXTENSION [IF NOT EXISTS] name`: record `name` as enabled. `Ok(true)`
    /// when newly enabled, `Ok(false)` when it was already enabled (idempotent — no
    /// error even without `IF NOT EXISTS`, matching Postgres' `CREATE EXTENSION`
    /// which errors only on a genuine re-create; the wire shim treats an existing
    /// extension as a benign success so a re-run setup script proceeds).
    pub fn create_extension(&self, name: &str, _if_not_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let created = create_extension_in(&wtx, name, _if_not_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(created)
    }

    /// `DROP EXTENSION [IF EXISTS] name`: remove the catalog entry. `Ok(true)` when an
    /// extension was removed, `Ok(false)` when absent and `if_exists` was set (else Err).
    pub fn drop_extension(&self, name: &str, if_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let existed = drop_extension_in(&wtx, name, if_exists)?;
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
        let mut out = Vec::new();
        let mut count = 0usize;
        let mut bytes = 0usize;
        for row in exts.iter().map_err(map_err)? {
            let (key, _) = row.map_err(map_err)?;
            account_collection(&mut count, &mut bytes, key.value().len())?;
            out.push(key.value().to_string());
        }
        out.sort();
        Ok(out)
    }

    // ── function catalog (CONCEPT:EG-KG.query.create-drop-function) ──────────────────────────────────────

    /// `CREATE [OR REPLACE] FUNCTION name(...) RETURNS … AS $$ … $$ LANGUAGE sql`:
    /// record `func` in the durable function catalog. Errors if the name already exists
    /// and `or_replace` is false, or if a user table already claims the name (a function
    /// and a table cannot share a name — a call `name(args)` would be ambiguous).
    pub fn create_function(&self, func: &StoredFunction, or_replace: bool) -> Result<(), String> {
        let wtx = self.begin()?;
        create_function_in(&wtx, func, or_replace)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// `DROP FUNCTION [IF EXISTS] name`: remove the function catalog entry. `Ok(true)`
    /// when a function was removed, `Ok(false)` when absent and `if_exists` was set.
    pub fn drop_function(&self, name: &str, if_exists: bool) -> Result<bool, String> {
        let wtx = self.begin()?;
        let existed = drop_function_in(&wtx, name, if_exists)?;
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
                let f: StoredFunction = decode_stored(v.value(), "function")?;
                Ok(Some(f))
            }
            None => Ok(None),
        }
    }

    /// Every stored function (sorted by name for determinism) — the set the SQL exec
    /// path expands into a query at plan time (CONCEPT:EG-KG.query.create-drop-function).
    pub fn list_functions(&self) -> Result<Vec<StoredFunction>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let funcs = match rtx.open_table(FUNCTIONS) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        let mut count = 0usize;
        let mut bytes = 0usize;
        for row in funcs.iter().map_err(map_err)? {
            let (_, value) = row.map_err(map_err)?;
            account_collection(&mut count, &mut bytes, value.value().len())?;
            out.push(decode_stored::<StoredFunction>(value.value(), "function")?);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    // ── pgvector ANN index catalog (CONCEPT:EG-KG.query.real-ann-top-k/EG-313) ─────────────────────

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

    /// `CREATE INDEX … USING hnsw|ivfflat (col opclass)` (CONCEPT:EG-KG.query.real-ann-top-k): register the
    /// [`AnnIndexPlan`] so a matching `ORDER BY col <-> $1 LIMIT k` pushes down to a real
    /// eg-ann index (CONCEPT:EG-KG.query.real-pgvector-ann-top). Idempotent on `if_not_exists` (a re-register of the
    /// same key is a benign success); an existing key without `if_not_exists` is replaced
    /// (the newest DDL wins — pgvector's `CREATE INDEX` build is idempotent in practice).
    pub fn put_ann_index(&self, plan: &AnnIndexPlan) -> Result<(), String> {
        let wtx = self.begin()?;
        put_ann_index_in(&wtx, plan)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// `DROP INDEX name` (CONCEPT:EG-KG.query.real-ann-top-k): remove every ANN index registered for
    /// `table`.`column` (all metrics). `Ok(n)` = number of entries removed.
    pub fn drop_ann_indexes_for_column(&self, table: &str, column: &str) -> Result<usize, String> {
        let wtx = self.begin()?;
        let removed = drop_ann_indexes_for_column_in(&wtx, table, column)?;
        wtx.commit().map_err(map_err)?;
        Ok(removed)
    }

    /// Every registered ANN index (sorted by key for determinism) — the set the SQL
    /// exec path consults to decide the pgvector pushdown (CONCEPT:EG-KG.query.real-pgvector-ann-top).
    pub fn list_ann_indexes(&self) -> Result<Vec<AnnIndexPlan>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let idxs = match rtx.open_table(ANN_INDEXES) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut pairs = Vec::new();
        let mut count = 0usize;
        let mut bytes = 0usize;
        for row in idxs.iter().map_err(map_err)? {
            let (key, value) = row.map_err(map_err)?;
            let added = key
                .value()
                .len()
                .checked_add(value.value().len())
                .ok_or_else(|| "SQL collection byte limit exceeded".to_string())?;
            account_collection(&mut count, &mut bytes, added)?;
            pairs.push((
                key.value().to_string(),
                decode_stored::<AnnIndexPlan>(value.value(), "ANN index")?,
            ));
        }
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pairs.into_iter().map(|(_, p)| p).collect())
    }

    // ── ordinary scalar secondary-index catalog ─────────────────────────────

    /// Build a schema-bound definition in the store's authenticated owner
    /// scope. SQL adapters should use this helper rather than accepting a scope
    /// from the request body.
    pub fn secondary_index_spec(
        &self,
        table: &str,
        name: &str,
        columns: Vec<super::index::SecondaryIndexColumn>,
        schema: &TableSchema,
    ) -> Result<SecondaryIndexSpec, String> {
        SecondaryIndexSpec::btree(self.index_scope(), table, name, columns, schema)
    }

    /// `CREATE INDEX` for ordinary scalar equality/range/order support. The
    /// catalog entry and all initial directory rows are committed atomically;
    /// a request exceeding the bounded build budget is rejected rather than
    /// silently creating a partial index. `Ok(false)` is the deterministic
    /// `IF NOT EXISTS` result.
    pub fn create_secondary_index(
        &self,
        spec: &SecondaryIndexSpec,
        if_not_exists: bool,
    ) -> Result<bool, String> {
        let wtx = self.begin()?;
        let created = create_secondary_index_in(
            &wtx,
            self.index_scope(),
            spec,
            if_not_exists,
        )?;
        wtx.commit().map_err(map_err)?;
        Ok(created)
    }

    /// `DROP INDEX` by owner scope, table, and index name. The physical entry
    /// directory is removed in the same transaction as the catalog row.
    pub fn drop_secondary_index(
        &self,
        table: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<bool, String> {
        let wtx = self.begin()?;
        let removed = drop_secondary_index_in(&wtx, self.index_scope(), table, name, if_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(removed)
    }

    /// List ordinary indexes in deterministic catalog order. A table filter is
    /// useful to providers and prevents exposing another table's definitions to
    /// an index-planning request.
    pub fn list_secondary_indexes(
        &self,
        table: Option<&str>,
    ) -> Result<Vec<SecondaryIndexSpec>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        list_secondary_indexes_in(&rtx, self.index_scope(), table)
    }

    /// Resolve a simple first-column equality/range lookup through the durable
    /// directory and fetch rows in ONE redb read transaction. Returning `None`
    /// means no current, schema-valid index can prove a narrowing; callers must
    /// use the ordinary scan. `Some(empty)` is a valid indexed result.
    pub fn secondary_index_rows(
        &self,
        table: &str,
        lookup: &SecondaryIndexLookup,
    ) -> Result<Option<Vec<Vec<Cell>>>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        secondary_index_rows_in(&rtx, self.index_scope(), table, lookup)
    }

    /// Deterministic ordered/paginated read over a named ordinary index. This
    /// is the explicit planner seam for callers that know an ORDER BY and
    /// LIMIT/OFFSET; DataFusion's generic `TableProvider::scan` does not carry
    /// those expressions, so it intentionally does not guess here. A stale or
    /// missing index returns `None` and the caller must plan a scan+sort.
    pub fn secondary_index_ordered_rows(
        &self,
        table: &str,
        index_name: &str,
        order: SecondaryIndexOrder,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Option<Vec<Vec<Cell>>>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        secondary_index_ordered_rows_in(
            &rtx,
            self.index_scope(),
            table,
            index_name,
            order,
            offset,
            limit,
        )
    }

    // ── Timescale-compatible hypertable catalog ──────────────────────────────

    /// Persist a hypertable declaration after validating its table and time
    /// column against the current catalog.
    pub fn put_hypertable(&self, plan: &HypertablePlan) -> Result<(), String> {
        let wtx = self.begin()?;
        put_hypertable_in(&wtx, plan)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// Return every native hypertable declaration in stable table-name order.
    pub fn list_hypertables(&self) -> Result<Vec<HypertablePlan>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let hypertables = match rtx.open_table(HYPERTABLES) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(map_err(error)),
        };
        let mut rows = Vec::new();
        let mut count = 0usize;
        let mut bytes = 0usize;
        for row in hypertables.iter().map_err(map_err)? {
            let (key, value) = row.map_err(map_err)?;
            let added = key
                .value()
                .len()
                .checked_add(value.value().len())
                .ok_or_else(|| "SQL collection byte limit exceeded".to_string())?;
            account_collection(&mut count, &mut bytes, added)?;
            rows.push((
                key.value().to_string(),
                decode_stored::<HypertablePlan>(value.value(), "hypertable")?,
            ));
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(rows.into_iter().map(|(_, plan)| plan).collect())
    }

    // ── multi-statement transaction (CONCEPT:EG-KG.query.register-each-user-table) ──────────────────────────

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
            affected = affected.saturating_add(apply_txn_op(&wtx, self.index_scope(), op)?);
        }
        wtx.commit().map_err(map_err)?;
        Ok(affected)
    }

    /// Commit a SQL table/catalog transaction together with the universal durable
    /// MutationBatch record, OCC/fence, idempotency result and immutable outbox.
    /// One redb `WriteTransaction` is the only commit point; a retry after an
    /// acknowledgement-lost crash returns the stored affected-row count without
    /// executing the SQL mutation again.
    pub fn commit_txn_batch(
        &self,
        txn: &TableTxn,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        self.commit_txn_batch_inner(txn, batch, committed_at_ms, None, None)
    }

    /// Variant for native adapters whose terminal result is richer than the SQL
    /// affected-row count (for example a whole SQLite-file import report).
    pub fn commit_txn_batch_result(
        &self,
        txn: &TableTxn,
        batch: &MutationBatch,
        result_msgpack: Vec<u8>,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        self.commit_txn_batch_inner(txn, batch, committed_at_ms, None, Some(result_msgpack))
    }

    fn commit_txn_batch_inner(
        &self,
        txn: &TableTxn,
        batch: &MutationBatch,
        committed_at_ms: u64,
        crashpoint: Option<SqlMutationCrashpoint>,
        result_override: Option<Vec<u8>>,
    ) -> Result<MutationBatchCommit, String> {
        batch.validate()?;
        if batch
            .operations
            .iter()
            .any(|operation| operation.domain != MutationDomain::SqlCatalog)
        {
            return Err("SQL MutationBatch contains a non-SqlCatalog operation".to_string());
        }
        let wtx = self.begin()?;

        // Idempotency check and insertion share this write transaction, closing the
        // concurrent double-execution race.
        {
            let idem = wtx.open_table(MUTATION_IDEMPOTENCY).map_err(map_err)?;
            let existing = idem
                .get((
                    batch.tenant.as_str(),
                    batch.graph.as_str(),
                    batch.idempotency_key.as_str(),
                ))
                .map_err(map_err)?
                .map(|value| value.value().to_string());
            if let Some(existing) = existing {
                let records = wtx.open_table(MUTATION_BATCHES).map_err(map_err)?;
                let bytes = records
                    .get(existing.as_str())
                    .map_err(map_err)?
                    .ok_or_else(|| {
                        format!(
                            "corrupt SQL mutation idempotency index: '{existing}' has no batch record"
                        )
                    })?
                    .value()
                    .to_vec();
                let record = decode_mutation_record(&bytes)?;
                if !same_batch_identity(&record.batch, batch)? {
                    return Err(format!(
                        "IDEMPOTENCY_CONFLICT: SQL key '{}' is already committed as batch '{}'",
                        batch.idempotency_key, record.batch.batch_id
                    ));
                }
                return Ok(MutationBatchCommit {
                    record,
                    replayed: true,
                });
            }
        }
        {
            let records = wtx.open_table(MUTATION_BATCHES).map_err(map_err)?;
            if records
                .get(batch.batch_id.as_str())
                .map_err(map_err)?
                .is_some()
            {
                return Err(format!(
                    "IDEMPOTENCY_CONFLICT: SQL batch_id '{}' already exists",
                    batch.batch_id
                ));
            }
        }

        let version_key = (batch.tenant.as_str(), batch.graph.as_str());
        let current_version = {
            let versions = wtx.open_table(MUTATION_VERSION).map_err(map_err)?;
            let version = match versions.get(version_key).map_err(map_err)? {
                Some(value) => value.value(),
                None => INITIAL_SQL_DOMAIN_VERSION,
            };
            version
        };
        let expected = batch.expected_graph_version.ok_or_else(|| {
            "authoritative SQL MutationBatch requires expected_graph_version".to_string()
        })?;
        if expected != current_version {
            return Err(format!(
                "STALE_VERSION: SQL scope '{}/{}' expected {} but authoritative version is {}",
                batch.tenant, batch.graph, expected, current_version
            ));
        }

        let current_fence = {
            let fences = wtx.open_table(MUTATION_FENCE).map_err(map_err)?;
            let value = fences
                .get(version_key)
                .map_err(map_err)?
                .map(|value| decode_stored::<SqlMutationFence>(value.value(), "mutation fence"))
                .transpose()?
                .unwrap_or_default();
            value
        };
        let proposed_fence = SqlMutationFence {
            placement_epoch: batch.placement_epoch,
            fencing_token: batch.fencing_token.unwrap_or(0),
        };
        if proposed_fence.placement_epoch < current_fence.placement_epoch
            || (proposed_fence.placement_epoch == current_fence.placement_epoch
                && proposed_fence.fencing_token < current_fence.fencing_token)
        {
            return Err("STALE_FENCE: SQL mutation coordinator is superseded".to_string());
        }
        if crashpoint == Some(SqlMutationCrashpoint::BeforeRows) {
            return Err("injected crash before SQL mutation rows".to_string());
        }
        eg_types::mutation_batch::apply_certification_fault(
            batch,
            eg_types::mutation_batch::MutationCommitPhase::BeforeRows,
        )?;

        let mut affected = 0usize;
        for op in &txn.ops {
            affected = affected.saturating_add(apply_txn_op(&wtx, self.index_scope(), op)?);
        }
        if crashpoint == Some(SqlMutationCrashpoint::AfterRowsBeforeMetadata) {
            return Err("injected crash after SQL mutation rows".to_string());
        }
        eg_types::mutation_batch::apply_certification_fault(
            batch,
            eg_types::mutation_batch::MutationCommitPhase::AfterRowsBeforeMetadata,
        )?;

        let result_msgpack = match result_override {
            Some(result) => result,
            None => rmp_serde::to_vec_named(&affected).map_err(|e| e.to_string())?,
        };
        let record = MutationBatchRecord {
            batch: batch.clone(),
            status: MutationBatchStatus::Committed,
            result_msgpack: Some(result_msgpack),
            committed_at_ms,
        };
        let record_bytes = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
        let next_version = current_version
            .checked_add(1)
            .ok_or_else(|| "SQL mutation domain version overflow".to_string())?;
        {
            let mut records = wtx.open_table(MUTATION_BATCHES).map_err(map_err)?;
            records
                .insert(batch.batch_id.as_str(), record_bytes.as_slice())
                .map_err(map_err)?;
            let mut idem = wtx.open_table(MUTATION_IDEMPOTENCY).map_err(map_err)?;
            idem.insert(
                (
                    batch.tenant.as_str(),
                    batch.graph.as_str(),
                    batch.idempotency_key.as_str(),
                ),
                batch.batch_id.as_str(),
            )
            .map_err(map_err)?;
            let mut versions = wtx.open_table(MUTATION_VERSION).map_err(map_err)?;
            versions
                .insert(version_key, next_version)
                .map_err(map_err)?;
            let fence_bytes =
                rmp_serde::to_vec_named(&proposed_fence).map_err(|e| e.to_string())?;
            let mut fences = wtx.open_table(MUTATION_FENCE).map_err(map_err)?;
            fences
                .insert(version_key, fence_bytes.as_slice())
                .map_err(map_err)?;

            let mut outbox = wtx.open_table(MUTATION_OUTBOX).map_err(map_err)?;
            let mut ordinal = 0u32;
            for operation in &batch.operations {
                let intent = MutationOutboxIntent {
                    topic: "engine.mutation.committed".to_string(),
                    key: batch.batch_id.clone(),
                    payload: rmp_serde::to_vec_named(operation).map_err(|e| e.to_string())?,
                    headers: Default::default(),
                };
                insert_sql_outbox(&mut outbox, batch, ordinal, intent)?;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| "SQL mutation outbox ordinal overflow".to_string())?;
            }
            for intent in &batch.outbox {
                insert_sql_outbox(&mut outbox, batch, ordinal, intent.clone())?;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| "SQL mutation outbox ordinal overflow".to_string())?;
            }
        }
        if crashpoint == Some(SqlMutationCrashpoint::BeforeCommit) {
            return Err("injected crash before SQL mutation commit".to_string());
        }
        eg_types::mutation_batch::apply_certification_fault(
            batch,
            eg_types::mutation_batch::MutationCommitPhase::BeforeCommit,
        )?;
        wtx.commit().map_err(map_err)?;
        if crashpoint == Some(SqlMutationCrashpoint::AfterCommitBeforeAck) {
            return Err("injected crash after SQL mutation commit before ack".to_string());
        }
        eg_types::mutation_batch::apply_certification_fault(
            batch,
            eg_types::mutation_batch::MutationCommitPhase::AfterCommitBeforeAck,
        )?;
        Ok(MutationBatchCommit {
            record,
            replayed: false,
        })
    }

    /// Current authoritative SQL-domain OCC version for batch planning.
    pub fn mutation_version(&self, tenant: &str, graph: &str) -> Result<u64, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let table = match rtx.open_table(MUTATION_VERSION) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(error) => return Err(map_err(error)),
        };
        let version = match table.get((tenant, graph)).map_err(map_err)? {
            Some(value) => value.value(),
            None => INITIAL_SQL_DOMAIN_VERSION,
        };
        Ok(version)
    }

    /// A single fingerprint over EVERY durable SQL-domain OCC counter this store
    /// currently holds, across every `(tenant, scope)` entry in [`MUTATION_VERSION`]
    /// -- not just one caller-named scope. [`Self::mutation_version`] reads ONE
    /// `(tenant, graph)` counter; a served SQL write always bumps SOME entry in this
    /// table on commit (`commit_txn`/`commit_txn_batch`/`commit_txn_batch_result`,
    /// the SQL-catalog gateway every `CREATE|DROP|ALTER TABLE|VIEW|FUNCTION|
    /// EXTENSION`, `CREATE INDEX`, `CREATE HYPERTABLE`, and user-table
    /// `INSERT|UPDATE|DELETE` route through) -- but NOT always under the literal
    /// `graph` name a caller happens to be asking about: the sqlite-import gateway
    /// (`src/server/handlers/sqlite_file.rs::compile_import_batch`) commits under a
    /// FIXED cross-graph scope (`authority.namespace("sqlite-import",
    /// "global-user-tables")`), independent of whichever graph issued the import.
    /// A cache keyed on `mutation_version(tenant, graph)` alone would miss that
    /// commit and serve a stale (pre-import) user-table batch. Scanning every scope
    /// closes that gap AND is future-proof: a new write path introduced later that
    /// picks yet another scope string is still covered automatically, with no
    /// caller-side list of "every scope name in use" to keep in sync.
    ///
    /// Cheap: distinct scopes per store are small (one per graph the tenant has run
    /// `Method::Sql`/pgwire DDL/DML against, plus a handful of fixed cross-graph
    /// scopes like the sqlite-import one) -- a full scan of this table is a tiny
    /// fraction of the cost `register_views`/`register_system_catalogs` amortize
    /// away. Deterministic (redb's `iter()` yields keys in sorted order): the same
    /// stored state always hashes to the same fingerprint, and ANY row added or
    /// changed changes it (a version only ever increases monotonically per scope,
    /// and scopes are only ever added, never removed, so the fingerprint can never
    /// coincidentally repeat a prior value after a real commit).
    pub fn catalog_fingerprint(&self) -> Result<u64, String> {
        use std::hash::{Hash, Hasher};
        let rtx = self.db.begin_read().map_err(map_err)?;
        let table = match rtx.open_table(MUTATION_VERSION) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(error) => return Err(map_err(error)),
        };
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for row in table.iter().map_err(map_err)? {
            let (key, value) = row.map_err(map_err)?;
            let (tenant, scope) = key.value();
            tenant.hash(&mut hasher);
            scope.hash(&mut hasher);
            value.value().hash(&mut hasher);
        }
        Ok(hasher.finish())
    }

    /// Read durable SQL-domain batch status/result for retry and restart recovery.
    pub fn mutation_batch(&self, batch_id: &str) -> Result<Option<MutationBatchRecord>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let table = match rtx.open_table(MUTATION_BATCHES) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(map_err(error)),
        };
        let record = table
            .get(batch_id)
            .map_err(map_err)?
            .map(|value| decode_mutation_record(value.value()))
            .transpose()?;
        Ok(record)
    }

    /// Read the SQL-domain transactional outbox for one committed batch.
    pub fn mutation_outbox(&self, batch_id: &str) -> Result<Vec<MutationOutboxRecord>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let table = match rtx.open_table(MUTATION_OUTBOX) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(map_err(error)),
        };
        let mut rows = Vec::new();
        let mut count = 0usize;
        let mut bytes = 0usize;
        for row in table
            .range((batch_id, 0u32)..=(batch_id, u32::MAX))
            .map_err(map_err)?
        {
            let (_, value) = row.map_err(map_err)?;
            account_collection(&mut count, &mut bytes, value.value().len())?;
            rows.push(decode_mutation_outbox(value.value())?);
        }
        Ok(rows)
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

fn apply_txn_op(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    op: &TxnOp,
) -> Result<usize, String> {
    match op {
        TxnOp::CreateTable {
            schema,
            if_not_exists,
        } => {
            create_in(wtx, schema, *if_not_exists)?;
            Ok(0)
        }
        TxnOp::DropTable { name, if_exists } => {
            drop_in(wtx, tenant_scope, name, *if_exists)?;
            Ok(0)
        }
        TxnOp::AddColumn { table, column } => {
            add_column_in(wtx, tenant_scope, table, column)?;
            Ok(0)
        }
        TxnOp::DropColumn {
            table,
            column,
            if_exists,
        } => {
            drop_column_in(wtx, tenant_scope, table, column, *if_exists)?;
            Ok(0)
        }
        TxnOp::RenameColumn { table, from, to } => {
            rename_column_in(wtx, tenant_scope, table, from, to)?;
            Ok(0)
        }
        TxnOp::RenameTable { table, new_name } => {
            rename_table_in(wtx, tenant_scope, table, new_name)?;
            Ok(0)
        }
        TxnOp::AlterColumnType {
            table,
            column,
            new_type,
        } => {
            alter_column_type_in(wtx, tenant_scope, table, column, *new_type)?;
            Ok(0)
        }
        TxnOp::DropConstraint {
            table,
            constraint,
            if_exists,
        } => {
            drop_constraint_in(wtx, tenant_scope, table, constraint, *if_exists)?;
            Ok(0)
        }
        TxnOp::AddConstraint { table, constraint } => {
            add_constraint_in(wtx, tenant_scope, table, constraint.clone())?;
            Ok(0)
        }
        TxnOp::Insert {
            table,
            col_order,
            rows,
        } => Ok(insert_in(wtx, tenant_scope, table, col_order, rows)?.len()),
        TxnOp::Update {
            table,
            set,
            selector,
        } => Ok(update_in(wtx, tenant_scope, table, set, selector)?.len()),
        TxnOp::Delete { table, selector } => Ok(delete_in(wtx, tenant_scope, table, selector)?.len()),
        TxnOp::CreateView {
            name,
            select_sql,
            or_replace,
        } => {
            create_view_in(wtx, name, select_sql, *or_replace)?;
            Ok(0)
        }
        TxnOp::DropView { name, if_exists } => {
            drop_view_in(wtx, name, *if_exists)?;
            Ok(0)
        }
        TxnOp::CreateExtension {
            name,
            if_not_exists,
        } => {
            create_extension_in(wtx, name, *if_not_exists)?;
            Ok(0)
        }
        TxnOp::DropExtension { name, if_exists } => {
            drop_extension_in(wtx, name, *if_exists)?;
            Ok(0)
        }
        TxnOp::CreateFunction {
            function,
            or_replace,
        } => {
            create_function_in(wtx, function, *or_replace)?;
            Ok(0)
        }
        TxnOp::DropFunction { name, if_exists } => {
            drop_function_in(wtx, name, *if_exists)?;
            Ok(0)
        }
        TxnOp::PutAnnIndex { plan } => {
            put_ann_index_in(wtx, plan)?;
            Ok(0)
        }
        TxnOp::PutHypertable { plan } => {
            put_hypertable_in(wtx, plan)?;
            Ok(0)
        }
        TxnOp::DropAnnIndexesForColumn { table, column } => {
            drop_ann_indexes_for_column_in(wtx, table, column)
        }
    }
}

fn same_batch_identity(stored: &MutationBatch, proposed: &MutationBatch) -> Result<bool, String> {
    let stored_ops = rmp_serde::to_vec_named(&stored.operations).map_err(|e| e.to_string())?;
    let proposed_ops = rmp_serde::to_vec_named(&proposed.operations).map_err(|e| e.to_string())?;
    Ok(stored.batch_id == proposed.batch_id
        && stored.context == proposed.context
        && stored.tenant == proposed.tenant
        && stored.graph == proposed.graph
        && stored.placement_epoch == proposed.placement_epoch
        && stored.idempotency_key == proposed.idempotency_key
        && stored.expected_graph_version == proposed.expected_graph_version
        && stored.fencing_token == proposed.fencing_token
        && stored.authoritative_state == proposed.authoritative_state
        && stored.outbox == proposed.outbox
        && stored_ops == proposed_ops)
}

fn insert_sql_outbox(
    outbox: &mut redb::Table<(&str, u32), &[u8]>,
    batch: &MutationBatch,
    ordinal: u32,
    intent: MutationOutboxIntent,
) -> Result<(), String> {
    let record = MutationOutboxRecord {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch.batch_id.clone(),
        ordinal,
        tenant: batch.tenant.clone(),
        graph: batch.graph.clone(),
        version_scope: MutationVersionScope::NonGraph,
        source_graph_version: NON_GRAPH_SOURCE_VERSION,
        intent,
        created_at_ms: batch.created_at_ms,
    };
    record.validate()?;
    let bytes = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
    outbox
        .insert((batch.batch_id.as_str(), ordinal), bytes.as_slice())
        .map_err(map_err)?;
    Ok(())
}

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
    let schema: TableSchema = decode_stored(&blob, "schema")?;
    schema.validate()?;
    Ok(Some(schema))
}

/// The names of every user table, read THROUGH the open write txn (staged-write-aware
/// — CONCEPT:EG-KG.query.table-schema-constraints/NE-001, the FK reverse-lookup this feeds needs to see a table
/// created earlier in the SAME transaction).
fn list_tables_in(wtx: &WriteTransaction) -> Result<Vec<String>, String> {
    let cat = match wtx.open_table(CATALOG) {
        Ok(t) => t,
        Err(_) => return Ok(Vec::new()),
    };
    let mut names = Vec::new();
    for row in cat.iter().map_err(map_err)? {
        let (key, _) = row.map_err(map_err)?;
        names.push(key.value().to_string());
    }
    Ok(names)
}

fn create_view_in(
    wtx: &WriteTransaction,
    name: &str,
    select_sql: &str,
    or_replace: bool,
) -> Result<(), String> {
    if get_schema_in(wtx, name)?.is_some() {
        return Err(format!(
            "`{name}` is a table; cannot create a view with that name"
        ));
    }
    let mut views = wtx.open_table(VIEWS).map_err(map_err)?;
    if !or_replace && views.get(name).map_err(map_err)?.is_some() {
        return Err(format!("view `{name}` already exists"));
    }
    views.insert(name, select_sql).map_err(map_err)?;
    Ok(())
}

fn drop_view_in(wtx: &WriteTransaction, name: &str, if_exists: bool) -> Result<bool, String> {
    let mut views = wtx.open_table(VIEWS).map_err(map_err)?;
    let existed = views.get(name).map_err(map_err)?.is_some();
    if !existed {
        if if_exists {
            return Ok(false);
        }
        return Err(format!("view `{name}` does not exist"));
    }
    views.remove(name).map_err(map_err)?;
    Ok(true)
}

fn create_extension_in(
    wtx: &WriteTransaction,
    name: &str,
    _if_not_exists: bool,
) -> Result<bool, String> {
    let mut extensions = wtx.open_table(EXTENSIONS).map_err(map_err)?;
    let existed = extensions.get(name).map_err(map_err)?.is_some();
    if !existed {
        extensions.insert(name, "").map_err(map_err)?;
    }
    Ok(!existed)
}

fn drop_extension_in(wtx: &WriteTransaction, name: &str, if_exists: bool) -> Result<bool, String> {
    let mut extensions = wtx.open_table(EXTENSIONS).map_err(map_err)?;
    let existed = extensions.get(name).map_err(map_err)?.is_some();
    if !existed {
        if if_exists {
            return Ok(false);
        }
        return Err(format!("extension `{name}` does not exist"));
    }
    extensions.remove(name).map_err(map_err)?;
    Ok(true)
}

fn create_function_in(
    wtx: &WriteTransaction,
    function: &StoredFunction,
    or_replace: bool,
) -> Result<(), String> {
    if get_schema_in(wtx, &function.name)?.is_some() {
        return Err(format!(
            "`{}` is a table; cannot create a function with that name",
            function.name
        ));
    }
    let bytes = rmp_serde::to_vec_named(function).map_err(|e| format!("encode function: {e}"))?;
    let mut functions = wtx.open_table(FUNCTIONS).map_err(map_err)?;
    if !or_replace
        && functions
            .get(function.name.as_str())
            .map_err(map_err)?
            .is_some()
    {
        return Err(format!("function `{}` already exists", function.name));
    }
    functions
        .insert(function.name.as_str(), bytes.as_slice())
        .map_err(map_err)?;
    Ok(())
}

fn drop_function_in(wtx: &WriteTransaction, name: &str, if_exists: bool) -> Result<bool, String> {
    let mut functions = wtx.open_table(FUNCTIONS).map_err(map_err)?;
    let existed = functions.get(name).map_err(map_err)?.is_some();
    if !existed {
        if if_exists {
            return Ok(false);
        }
        return Err(format!("function `{name}` does not exist"));
    }
    functions.remove(name).map_err(map_err)?;
    Ok(true)
}

fn put_ann_index_in(wtx: &WriteTransaction, plan: &AnnIndexPlan) -> Result<(), String> {
    let key = TableStore::ann_index_key(plan);
    let bytes = rmp_serde::to_vec_named(plan).map_err(|e| format!("encode ann index: {e}"))?;
    let mut indexes = wtx.open_table(ANN_INDEXES).map_err(map_err)?;
    indexes
        .insert(key.as_str(), bytes.as_slice())
        .map_err(map_err)?;
    Ok(())
}

fn put_hypertable_in(wtx: &WriteTransaction, plan: &HypertablePlan) -> Result<(), String> {
    let schema = get_schema_in(wtx, &plan.table)?
        .ok_or_else(|| format!("table `{}` does not exist", plan.table))?;
    let time_column = schema.column(&plan.time_column).ok_or_else(|| {
        format!(
            "time column `{}` does not exist in table `{}`",
            plan.time_column, plan.table
        )
    })?;
    if !matches!(time_column.ty, ColumnType::Timestamp) {
        return Err(format!(
            "hypertable time column `{}.{}` must be a timestamp",
            plan.table, plan.time_column
        ));
    }
    let bytes = rmp_serde::to_vec_named(plan).map_err(|e| format!("encode hypertable: {e}"))?;
    let mut hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
    if let Some(existing) = hypertables.get(plan.table.as_str()).map_err(map_err)? {
        let existing = decode_stored::<HypertablePlan>(existing.value(), "hypertable")?;
        if existing != *plan {
            return Err(format!(
                "table `{}` is already a hypertable on `{}`",
                plan.table, existing.time_column
            ));
        }
        return Ok(());
    }
    hypertables
        .insert(plan.table.as_str(), bytes.as_slice())
        .map_err(map_err)?;
    Ok(())
}

fn drop_ann_indexes_for_column_in(
    wtx: &WriteTransaction,
    table: &str,
    column: &str,
) -> Result<usize, String> {
    let prefix = format!(
        "{}.{}.",
        table.to_ascii_lowercase(),
        column.to_ascii_lowercase()
    );
    let mut indexes = wtx.open_table(ANN_INDEXES).map_err(map_err)?;
    let keys = indexes
        .iter()
        .map_err(map_err)?
        .filter_map(|row| row.ok().map(|(key, _)| key.value().to_string()))
        .filter(|key| key.starts_with(&prefix))
        .collect::<Vec<_>>();
    for key in &keys {
        indexes.remove(key.as_str()).map_err(map_err)?;
    }
    Ok(keys.len())
}

// ── ordinary scalar secondary-index catalog and directory ───────────────────

fn get_schema_read(
    rtx: &ReadTransaction,
    name: &str,
) -> Result<Option<TableSchema>, String> {
    let cat = match rtx.open_table(CATALOG) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let Some(value) = cat.get(name).map_err(map_err)? else {
        return Ok(None);
    };
    let schema = decode_stored::<TableSchema>(value.value(), "schema")?;
    schema.validate()?;
    Ok(Some(schema))
}

fn list_secondary_indexes_in(
    rtx: &ReadTransaction,
    tenant_scope: &str,
    table: Option<&str>,
) -> Result<Vec<SecondaryIndexSpec>, String> {
    let indexes = match rtx.open_table(SECONDARY_INDEXES) {
        Ok(table) => table,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    let mut count = 0usize;
    let mut bytes = 0usize;
    for row in indexes.iter().map_err(map_err)? {
        let (key, value) = row.map_err(map_err)?;
        // A malformed optional index definition is never allowed to make the
        // authoritative table unreadable. Skip it here so callers deterministically
        // fall back to a scan; the catalog remains inspectable through redb repair
        // tooling rather than being trusted by the planner.
        let Ok(spec) = decode_stored::<SecondaryIndexSpec>(value.value(), "secondary index")
        else {
            continue;
        };
        if spec.tenant_scope != tenant_scope
            || table.is_some_and(|requested| requested != spec.table.as_str())
        {
            continue;
        }
        account_collection(
            &mut count,
            &mut bytes,
            key.value().len().saturating_add(value.value().len()),
        )?;
        out.push(spec);
    }
    out.sort_by(|a, b| {
        (&a.table, &a.name, &a.schema_digest).cmp(&(&b.table, &b.name, &b.schema_digest))
    });
    Ok(out)
}

fn list_secondary_indexes_write(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
) -> Result<Vec<SecondaryIndexSpec>, String> {
    let indexes = match wtx.open_table(SECONDARY_INDEXES) {
        Ok(table) => table,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for row in indexes.iter().map_err(map_err)? {
        let (_, value) = row.map_err(map_err)?;
        let Ok(spec) = decode_stored::<SecondaryIndexSpec>(value.value(), "secondary index")
        else {
            continue;
        };
        if spec.tenant_scope == tenant_scope && spec.table == table {
            out.push(spec);
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn create_secondary_index_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    spec: &SecondaryIndexSpec,
    if_not_exists: bool,
) -> Result<bool, String> {
    let schema = get_schema_in(wtx, &spec.table)?
        .ok_or_else(|| format!("table `{}` does not exist", spec.table))?;
    if spec.tenant_scope != tenant_scope {
        return Err(format!(
            "secondary index `{}` belongs to a different tenant scope",
            spec.name
        ));
    }
    validate_secondary_spec(spec, &schema)?;
    let key = secondary_catalog_key(spec);
    {
        let indexes = wtx.open_table(SECONDARY_INDEXES).map_err(map_err)?;
        if indexes.get(key.as_str()).map_err(map_err)?.is_some() {
            if if_not_exists {
                return Ok(false);
            }
            return Err(format!("secondary index `{}` already exists", spec.name));
        }
    }
    let existing = list_secondary_indexes_write(wtx, tenant_scope, &spec.table)?;
    if existing.len() >= MAX_SECONDARY_INDEXES_PER_TABLE {
        return Err(format!(
            "table `{}` exceeds the {} secondary-index bound",
            spec.table, MAX_SECONDARY_INDEXES_PER_TABLE
        ));
    }

    let bytes = rmp_serde::to_vec_named(spec)
        .map_err(|error| format!("encode secondary index: {error}"))?;
    let mut indexes = wtx.open_table(SECONDARY_INDEXES).map_err(map_err)?;
    indexes
        .insert(key.as_str(), bytes.as_slice())
        .map_err(map_err)?;
    drop(indexes);

    // Initial directory construction is intentionally bounded and atomic. A
    // large table asks its owner to build/partition it explicitly rather than
    // leaving a silently partial index behind.
    let rows = match wtx.open_table(ROWS) {
        Ok(table) => table,
        Err(_) => return Ok(true),
    };
    let mut row_items = Vec::new();
    let mut row_count = 0usize;
    let mut row_bytes = 0usize;
    for row in rows
        .range((spec.table.as_str(), 0u64)..=(spec.table.as_str(), u64::MAX))
        .map_err(map_err)?
    {
        let (row_key, value) = row.map_err(map_err)?;
        if row_items.len() >= MAX_SECONDARY_INDEX_BUILD_ROWS
            || account_collection(&mut row_count, &mut row_bytes, value.value().len()).is_err()
        {
            return Err(format!(
                "secondary index `{}` build exceeds {} rows",
                spec.name, MAX_SECONDARY_INDEX_BUILD_ROWS
            ));
        }
        row_items.push((
            row_key.value().1,
            decode_stored::<Vec<Cell>>(value.value(), "row")?,
        ));
    }
    drop(rows);
    let mut entries = wtx.open_table(SECONDARY_INDEX_ENTRIES).map_err(map_err)?;
    for (rowid, cells) in row_items {
        let entry = secondary_entry_key(spec, &schema, &cells, rowid)?;
        entries
            .insert(entry.as_str(), &[][..])
            .map_err(map_err)?;
    }
    Ok(true)
}

fn drop_secondary_index_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    name: &str,
    if_exists: bool,
) -> Result<bool, String> {
    let key = format!("{tenant_scope}\0{table}\0{name}");
    let exists = {
        let indexes = wtx.open_table(SECONDARY_INDEXES).map_err(map_err)?;
        indexes.get(key.as_str()).map_err(map_err)?.is_some()
    };
    if !exists {
        if if_exists {
            return Ok(false);
        }
        return Err(format!("secondary index `{name}` does not exist"));
    }
    {
        let mut indexes = wtx.open_table(SECONDARY_INDEXES).map_err(map_err)?;
        indexes.remove(key.as_str()).map_err(map_err)?;
    }
    let prefix = format!("{key}\0");
    let high = format!("{prefix}\u{10ffff}");
    let entry_keys = {
        let entries = wtx.open_table(SECONDARY_INDEX_ENTRIES).map_err(map_err)?;
        let mut keys = Vec::new();
        for row in entries
            .range(prefix.as_str()..high.as_str())
            .map_err(map_err)?
        {
            let (entry, _) = row.map_err(map_err)?;
            if keys.len() >= MAX_SECONDARY_INDEX_CANDIDATES {
                return Err("secondary index drop exceeds the bounded entry limit".to_string());
            }
            keys.push(entry.value().to_string());
        }
        keys
    };
    let mut entries = wtx.open_table(SECONDARY_INDEX_ENTRIES).map_err(map_err)?;
    for entry in entry_keys {
        entries.remove(entry.as_str()).map_err(map_err)?;
    }
    Ok(true)
}

/// Remove definitions and physical rows for a table whose schema or name is
/// changing.  A schema digest mismatch would already force a scan, but removing
/// the stale catalog also makes a later CREATE INDEX deterministic and bounds
/// orphan growth across repeated ALTER/DROP operations.
fn drop_secondary_indexes_for_table_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
) -> Result<usize, String> {
    let specs = list_secondary_indexes_write(wtx, tenant_scope, table)?;
    if specs.is_empty() {
        return Ok(0);
    }
    let keys: Vec<String> = specs.iter().map(secondary_catalog_key).collect();
    {
        let mut indexes = wtx.open_table(SECONDARY_INDEXES).map_err(map_err)?;
        for key in &keys {
            indexes.remove(key.as_str()).map_err(map_err)?;
        }
    }
    let prefixes: Vec<String> = specs.iter().map(secondary_entry_prefix).collect();
    let entry_keys = {
        let entries = wtx.open_table(SECONDARY_INDEX_ENTRIES).map_err(map_err)?;
        let mut keys = Vec::new();
        for prefix in &prefixes {
            let high = format!("{prefix}\u{10ffff}");
            for row in entries
                .range(prefix.as_str()..high.as_str())
                .map_err(map_err)?
            {
                let (entry, _) = row.map_err(map_err)?;
                if keys.len() >= MAX_SECONDARY_INDEX_CANDIDATES {
                    return Err(
                        "secondary table-index drop exceeds the bounded entry limit".to_string(),
                    );
                }
                keys.push(entry.value().to_string());
            }
        }
        keys
    };
    let mut entries = wtx.open_table(SECONDARY_INDEX_ENTRIES).map_err(map_err)?;
    for entry in entry_keys {
        entries.remove(entry.as_str()).map_err(map_err)?;
    }
    Ok(specs.len())
}

fn maintain_secondary_row_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    schema: &TableSchema,
    rowid: u64,
    old: Option<&[Cell]>,
    new: Option<&[Cell]>,
) -> Result<(), String> {
    let specs = list_secondary_indexes_write(wtx, tenant_scope, table)?;
    if specs.is_empty() {
        return Ok(());
    }
    let mut entries = wtx.open_table(SECONDARY_INDEX_ENTRIES).map_err(map_err)?;
    for spec in specs {
        // A stale definition is deliberately ignored. Its reader returns None
        // and scans; it must never block ordinary DML after a schema migration.
        if validate_secondary_spec(&spec, schema).is_err() {
            continue;
        }
        if let Some(old) = old {
            let key = secondary_entry_key(&spec, schema, old, rowid)?;
            entries.remove(key.as_str()).map_err(map_err)?;
        }
        if let Some(new) = new {
            let key = secondary_entry_key(&spec, schema, new, rowid)?;
            entries
                .insert(key.as_str(), &[][..])
                .map_err(map_err)?;
        }
    }
    Ok(())
}

fn secondary_index_rows_in(
    rtx: &ReadTransaction,
    tenant_scope: &str,
    table: &str,
    lookup: &SecondaryIndexLookup,
) -> Result<Option<Vec<Vec<Cell>>>, String> {
    let Some(schema) = get_schema_read(rtx, table)? else {
        return Err(format!("table `{table}` does not exist"));
    };
    let specs = list_secondary_indexes_in(rtx, tenant_scope, Some(table))?;
    let Some(spec) = specs.into_iter().find(|spec| {
        spec.columns
            .first()
            .map(|column| column.name.as_str())
            == Some(lookup.column())
            && validate_secondary_spec(spec, &schema).is_ok()
    }) else {
        return Ok(None);
    };
    let Some((low, high)) = secondary_entry_range(&spec, &schema, lookup)? else {
        return Ok(None);
    };
    let entries = match rtx.open_table(SECONDARY_INDEX_ENTRIES) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let rows = match rtx.open_table(ROWS) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let mut rowids = Vec::new();
    for item in entries
        .range(low.as_str()..high.as_str())
        .map_err(map_err)?
    {
        let (key, _) = item.map_err(map_err)?;
        let Some(rowid) = rowid_from_entry_key(key.value()) else {
            return Ok(None);
        };
        if rowids.len() >= MAX_SECONDARY_INDEX_CANDIDATES {
            return Ok(None);
        }
        rowids.push(rowid);
    }
    let width = schema.columns().len();
    let mut out = Vec::with_capacity(rowids.len());
    let mut row_count = 0usize;
    let mut row_bytes = 0usize;
    for rowid in rowids {
        let Some(value) = rows.get((table, rowid)).map_err(map_err)? else {
            // An orphaned/missing directory entry is not a reason to return a
            // partial result.  Revert to the authoritative row scan.
            return Ok(None);
        };
        if account_collection(&mut row_count, &mut row_bytes, value.value().len()).is_err() {
            return Ok(None);
        }
        let mut cells = decode_stored::<Vec<Cell>>(value.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        out.push(cells);
    }
    Ok(Some(out))
}

fn secondary_index_ordered_rows_in(
    rtx: &ReadTransaction,
    tenant_scope: &str,
    table: &str,
    index_name: &str,
    order: SecondaryIndexOrder,
    offset: usize,
    limit: Option<usize>,
) -> Result<Option<Vec<Vec<Cell>>>, String> {
    let Some(schema) = get_schema_read(rtx, table)? else {
        return Err(format!("table `{table}` does not exist"));
    };
    let Some(spec) = list_secondary_indexes_in(rtx, tenant_scope, Some(table))?
        .into_iter()
        .find(|spec| spec.name == index_name)
    else {
        return Ok(None);
    };
    if validate_secondary_spec(&spec, &schema).is_err() {
        return Ok(None);
    }
    let entries = match rtx.open_table(SECONDARY_INDEX_ENTRIES) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let rows = match rtx.open_table(ROWS) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let prefix = secondary_entry_prefix(&spec);
    let high = format!("{prefix}\u{10ffff}");
    let mut rowids = Vec::new();
    for item in entries
        .range(prefix.as_str()..high.as_str())
        .map_err(map_err)?
    {
        let (key, _) = item.map_err(map_err)?;
        let Some(rowid) = rowid_from_entry_key(key.value()) else {
            return Ok(None);
        };
        if rowids.len() >= MAX_SECONDARY_INDEX_CANDIDATES {
            return Ok(None);
        }
        rowids.push(rowid);
    }
    if matches!(order, SecondaryIndexOrder::Desc) {
        rowids.reverse();
    }
    let start = offset.min(rowids.len());
    let end = limit
        .and_then(|count| start.checked_add(count))
        .unwrap_or(rowids.len())
        .min(rowids.len());
    let width = schema.columns().len();
    let mut out = Vec::with_capacity(end.saturating_sub(start));
    let mut row_count = 0usize;
    let mut row_bytes = 0usize;
    for rowid in &rowids[start..end] {
        let Some(value) = rows.get((table, *rowid)).map_err(map_err)? else {
            return Ok(None);
        };
        if account_collection(&mut row_count, &mut row_bytes, value.value().len()).is_err() {
            return Ok(None);
        }
        let mut cells = decode_stored::<Vec<Cell>>(value.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        out.push(cells);
    }
    Ok(Some(out))
}

// ── table-level constraints: FK cross-table validation + write-path enforcement ───
// (CONCEPT:EG-KG.query.table-schema-constraints/NE-001)

/// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — a table-level `PRIMARY KEY (a, b, …)` implies NOT NULL on
/// every participating column (mirrors Postgres), regardless of how the column's own
/// `ColumnDef` declared nullability.
fn force_pk_not_null(schema: &mut TableSchema) {
    let pk_cols: Vec<String> = schema
        .constraints()
        .iter()
        .filter_map(|c| match c {
            TableConstraint::PrimaryKey { columns, .. } => Some(columns.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    if pk_cols.is_empty() {
        return;
    }
    for col in schema.columns_mut() {
        if pk_cols.iter().any(|c| c == &col.name) {
            col.nullable = false;
        }
    }
}

/// Is `cols` covered by exactly one PRIMARY KEY or UNIQUE constraint (column-level
/// flag for a single column, or a table-level constraint over the SAME column set) on
/// `schema`? Postgres requires a FK's REFERENCES side to be backed by a unique
/// constraint — otherwise "the referenced row" is ambiguous under a referential
/// action, so this is a fail-closed DDL-time check, not a runtime nicety.
fn schema_has_unique_over(schema: &TableSchema, cols: &[String]) -> bool {
    if let [only] = cols {
        if schema.column(only).is_some_and(Column::is_unique) {
            return true;
        }
    }
    let set: HashSet<&str> = cols.iter().map(|s| s.as_str()).collect();
    schema.constraints().iter().any(|c| {
        let group: Option<&[String]> = match c {
            TableConstraint::PrimaryKey { columns, .. }
            | TableConstraint::Unique { columns, .. } => Some(columns),
            _ => None,
        };
        group.is_some_and(|g| g.len() == cols.len() && g.iter().all(|x| set.contains(x.as_str())))
    })
}

/// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — validate ONE `FOREIGN KEY` constraint's cross-table
/// requirements: the referenced table exists (a self-reference resolves against
/// `schema` itself, since the table being created is not yet in the catalog), its
/// `ref_columns` exist, each local/referenced column pair's TYPE matches, and the
/// referenced column set is backed by a PK/UNIQUE constraint. Fails closed with a
/// precise error — a `REFERENCES` naming a table/column that does not exist is
/// otherwise the "silently ignored, which is worse" bug this track exists to fix.
fn validate_fk_target_in(
    wtx: &WriteTransaction,
    schema: &TableSchema,
    columns: &[String],
    ref_table: &str,
    ref_columns: &[String],
) -> Result<(), String> {
    let ref_schema = if ref_table == schema.name {
        schema.clone()
    } else {
        get_schema_in(wtx, ref_table)?.ok_or_else(|| {
            format!(
                "FOREIGN KEY on table `{}`: referenced table `{ref_table}` does not exist",
                schema.name
            )
        })?
    };
    for (local_col, ref_col) in columns.iter().zip(ref_columns) {
        let local = schema.column(local_col).ok_or_else(|| {
            format!(
                "FOREIGN KEY on table `{}`: column `{local_col}` does not exist",
                schema.name
            )
        })?;
        let referenced = ref_schema.column(ref_col).ok_or_else(|| {
            format!(
                "FOREIGN KEY on table `{}`: referenced column `{ref_table}.{ref_col}` does not exist",
                schema.name
            )
        })?;
        if local.ty != referenced.ty {
            return Err(format!(
                "FOREIGN KEY on table `{}`: column `{local_col}` type does not match referenced column `{ref_table}.{ref_col}`",
                schema.name
            ));
        }
    }
    if !schema_has_unique_over(&ref_schema, ref_columns) {
        return Err(format!(
            "FOREIGN KEY on table `{}`: referenced column(s) ({}) on `{ref_table}` are not covered by a PRIMARY KEY or UNIQUE constraint",
            schema.name,
            ref_columns.join(", ")
        ));
    }
    Ok(())
}

/// Evaluate every `Check` constraint in `checks` against one row map (CONCEPT:EG-KG.query.table-schema-constraints/NE-001),
/// returning a precise error naming the first violated constraint.
fn eval_table_checks(
    table: &str,
    checks: &[&TableConstraint],
    row: &serde_json::Map<String, Value>,
) -> Result<(), String> {
    for c in checks {
        if let TableConstraint::Check { expr, .. } = c {
            if !expr.holds(row) {
                let name = TableSchema::constraint_display_name(table, c);
                return Err(format!(
                    "new row for table `{table}` violates check constraint `{name}`"
                ));
            }
        }
    }
    Ok(())
}

/// Does a row with the given `columns`' values equal to `key` already exist in
/// `table` (CONCEPT:EG-KG.query.table-schema-constraints/NE-001)? Reads through `wtx` (staged-write-aware).
fn row_exists_with_key_in(
    wtx: &WriteTransaction,
    table: &str,
    schema: &TableSchema,
    columns: &[String],
    key: &[Cell],
) -> Result<bool, String> {
    let idxs: Vec<usize> = columns
        .iter()
        .map(|c| {
            schema
                .column_index(c)
                .expect("FK column existence validated at DDL time")
        })
        .collect();
    let width = schema.columns().len();
    let rows_t = match wtx.open_table(ROWS) {
        Ok(t) => t,
        Err(_) => return Ok(false),
    };
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        if idxs.iter().zip(key).all(|(&i, k)| cells.get(i) == Some(k)) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — verify every OUTGOING `FOREIGN KEY` on `schema` for ONE
/// row's cells. MATCH SIMPLE semantics (Postgres's default): a FK with ANY NULL
/// participating column is exempt. Reads the referenced table through `wtx`, so a
/// parent inserted earlier in the SAME transaction already satisfies a child
/// inserted later in it.
fn validate_fk_out_for_row_in(
    wtx: &WriteTransaction,
    schema: &TableSchema,
    cells: &[Cell],
) -> Result<(), String> {
    for c in schema.constraints() {
        let TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            ..
        } = c
        else {
            continue;
        };
        let mut key: Vec<Cell> = Vec::with_capacity(columns.len());
        let mut any_null = false;
        for col in columns {
            let idx = schema
                .column_index(col)
                .expect("FK column existence validated at DDL time");
            let cell = cells.get(idx).cloned().unwrap_or(Cell::Null);
            if matches!(cell, Cell::Null) {
                any_null = true;
            }
            key.push(cell);
        }
        if any_null {
            continue;
        }
        let ref_schema = if ref_table == &schema.name {
            schema.clone()
        } else {
            get_schema_in(wtx, ref_table)?.ok_or_else(|| {
                format!(
                    "FOREIGN KEY on table `{}`: referenced table `{ref_table}` does not exist",
                    schema.name
                )
            })?
        };
        if !row_exists_with_key_in(wtx, ref_table, &ref_schema, ref_columns, &key)? {
            let name = TableSchema::constraint_display_name(&schema.name, c);
            return Err(format!(
                "insert or update on table `{}` violates foreign key constraint `{name}`",
                schema.name
            ));
        }
    }
    Ok(())
}

/// The combined per-row write-path check (CONCEPT:EG-KG.query.table-schema-constraints/NE-001): table-level CHECK
/// constraints, then outgoing FOREIGN KEY constraints. Called for every row an
/// INSERT/ON CONFLICT DO UPDATE/UPDATE stages, right after its cells are built.
fn validate_row_constraints_in(
    wtx: &WriteTransaction,
    schema: &TableSchema,
    cells: &[Cell],
) -> Result<(), String> {
    let checks: Vec<&TableConstraint> = schema
        .constraints()
        .iter()
        .filter(|c| matches!(c, TableConstraint::Check { .. }))
        .collect();
    if !checks.is_empty() {
        eval_table_checks(&schema.name, &checks, &row_map(schema, cells))?;
    }
    validate_fk_out_for_row_in(wtx, schema, cells)
}

/// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — enforce every OTHER table's `FOREIGN KEY` that references
/// `table` when ONE of `table`'s rows changes: `new_row = None` for a DELETE,
/// `Some(..)` for an UPDATE (a no-op for a FK whose referenced columns did not
/// change). `visited` guards a cascade across a cyclic FK (self-referencing OR a
/// cycle spanning two-or-more tables) from ever reprocessing the SAME `(table,
/// rowid)` twice, which is what makes an unbounded recursive cascade impossible.
fn enforce_fk_on_parent_change_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    rowid: u64,
    old_row: &[Cell],
    new_row: Option<&[Cell]>,
    visited: &mut HashSet<(String, u64)>,
) -> Result<(), String> {
    if !visited.insert((table.to_string(), rowid)) {
        return Ok(());
    }
    let parent_schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    for child_table in list_tables_in(wtx)? {
        let child_schema = match get_schema_in(wtx, &child_table)? {
            Some(s) => s,
            None => continue,
        };
        for c in child_schema.constraints().to_vec() {
            let TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                name: _,
            } = &c
            else {
                continue;
            };
            if ref_table != table {
                continue;
            }
            let ref_idxs: Vec<usize> = ref_columns
                .iter()
                .map(|c| {
                    parent_schema
                        .column_index(c)
                        .expect("FK ref column existence validated at DDL time")
                })
                .collect();
            let old_key: Vec<Cell> = ref_idxs
                .iter()
                .map(|&i| old_row.get(i).cloned().unwrap_or(Cell::Null))
                .collect();
            let new_key: Option<Vec<Cell>> = new_row.map(|nr| {
                ref_idxs
                    .iter()
                    .map(|&i| nr.get(i).cloned().unwrap_or(Cell::Null))
                    .collect()
            });
            if let Some(nk) = &new_key {
                if nk == &old_key {
                    continue;
                }
            }
            let action = if new_row.is_some() {
                *on_update
            } else {
                *on_delete
            };
            let child_idxs: Vec<usize> = columns
                .iter()
                .map(|c| {
                    child_schema
                        .column_index(c)
                        .expect("FK column existence validated at DDL time")
                })
                .collect();
            let width = child_schema.columns().len();
            let matches: Vec<(u64, Vec<Cell>)> = {
                let rows_t = wtx.open_table(ROWS).map_err(map_err)?;
                let mut out = Vec::new();
                for r in rows_t
                    .range((child_table.as_str(), 0u64)..=(child_table.as_str(), u64::MAX))
                    .map_err(map_err)?
                {
                    let (k, v) = r.map_err(map_err)?;
                    let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
                    if cells.len() < width {
                        cells.resize(width, Cell::Null);
                    }
                    let any_null = child_idxs
                        .iter()
                        .any(|&i| !matches!(cells.get(i), Some(c) if !matches!(c, Cell::Null)));
                    if any_null {
                        continue;
                    }
                    if child_idxs
                        .iter()
                        .zip(&old_key)
                        .all(|(&i, k)| cells.get(i) == Some(k))
                    {
                        out.push((k.value().1, cells));
                    }
                }
                out
            };
            if matches.is_empty() {
                continue;
            }
            match action {
                RefAction::NoAction | RefAction::Restrict => {
                    let cname = TableSchema::constraint_display_name(&child_table, &c);
                    return Err(format!(
                        "update or delete on table `{table}` violates foreign key constraint `{cname}` on table `{child_table}`"
                    ));
                }
                RefAction::Cascade => {
                    for (child_rowid, child_cells) in matches {
                        if let Some(nk) = &new_key {
                            let mut updated = child_cells.clone();
                            for (&i, v) in child_idxs.iter().zip(nk) {
                                updated[i] = v.clone();
                            }
                            validate_fk_out_for_row_in(wtx, &child_schema, &updated)?;
                            let blob = rmp_serde::to_vec_named(&updated)
                                .map_err(|e| format!("encode row: {e}"))?;
                            {
                                let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
                                rows_t
                                    .insert((child_table.as_str(), child_rowid), blob.as_slice())
                                    .map_err(map_err)?;
                            }
                            maintain_secondary_row_in(
                                wtx,
                                tenant_scope,
                                &child_table,
                                &child_schema,
                                child_rowid,
                                Some(&child_cells),
                                Some(&updated),
                            )?;
                            enforce_fk_on_parent_change_in(
                                wtx,
                                tenant_scope,
                                &child_table,
                                child_rowid,
                                &child_cells,
                                Some(&updated),
                                visited,
                            )?;
                        } else {
                            {
                                let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
                                rows_t
                                    .remove((child_table.as_str(), child_rowid))
                                    .map_err(map_err)?;
                            }
                            maintain_secondary_row_in(
                                wtx,
                                tenant_scope,
                                &child_table,
                                &child_schema,
                                child_rowid,
                                Some(&child_cells),
                                None,
                            )?;
                            enforce_fk_on_parent_change_in(
                                wtx,
                                tenant_scope,
                                &child_table,
                                child_rowid,
                                &child_cells,
                                None,
                                visited,
                            )?;
                        }
                    }
                }
                RefAction::SetNull => {
                    for (child_rowid, child_cells) in matches {
                        for &i in &child_idxs {
                            if !child_schema.columns()[i].nullable {
                                return Err(format!(
                                    "cannot SET NULL on non-nullable column `{}` of table `{child_table}` for foreign key `{}`",
                                    child_schema.columns()[i].name,
                                    TableSchema::constraint_display_name(&child_table, &c)
                                ));
                            }
                        }
                        let mut updated = child_cells.clone();
                        for &i in &child_idxs {
                            updated[i] = Cell::Null;
                        }
                        let blob = rmp_serde::to_vec_named(&updated)
                            .map_err(|e| format!("encode row: {e}"))?;
                        {
                            let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
                            rows_t
                                .insert((child_table.as_str(), child_rowid), blob.as_slice())
                                .map_err(map_err)?;
                        }
                        maintain_secondary_row_in(
                            wtx,
                            tenant_scope,
                            &child_table,
                            &child_schema,
                            child_rowid,
                            Some(&child_cells),
                            Some(&updated),
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Existing rows must not already violate a NEWLY added NOT NULL requirement (CONCEPT:EG-KG.query.table-schema-constraints/NE-001,
/// `ADD CONSTRAINT` adding a PK that forces NOT NULL on a previously-nullable
/// column) — mirrors Postgres's refusal to add such a constraint over violating data.
fn validate_not_null_in(
    wtx: &WriteTransaction,
    table: &str,
    schema: &TableSchema,
) -> Result<(), String> {
    let not_null_cols: Vec<usize> = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.nullable)
        .map(|(i, _)| i)
        .collect();
    if not_null_cols.is_empty() {
        return Ok(());
    }
    let width = schema.columns().len();
    let rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        for &ci in &not_null_cols {
            if matches!(cells[ci], Cell::Null) {
                return Err(format!(
                    "column `{}` of table `{table}` has existing NULL values; cannot add a NOT NULL/PRIMARY KEY constraint",
                    schema.columns()[ci].name
                ));
            }
        }
    }
    Ok(())
}

/// Every EXISTING row of `table` must already satisfy every `Check` constraint in
/// `checks` (CONCEPT:EG-KG.query.table-schema-constraints/NE-001, `ADD CONSTRAINT`) — mirrors Postgres's refusal
/// to add a CHECK the current data already violates.
fn validate_table_checks_over_existing_in(
    wtx: &WriteTransaction,
    table: &str,
    schema: &TableSchema,
    checks: &[&TableConstraint],
) -> Result<(), String> {
    if checks.is_empty() {
        return Ok(());
    }
    let width = schema.columns().len();
    let rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        eval_table_checks(table, checks, &row_map(schema, &cells))?;
    }
    Ok(())
}

/// Every EXISTING row of `table` must already satisfy the outgoing FOREIGN KEY
/// `constraint` (CONCEPT:EG-KG.query.table-schema-constraints/NE-001, `ADD CONSTRAINT`) — mirrors Postgres's
/// refusal to add a FK the current data already violates.
fn validate_existing_fk_children_in(
    wtx: &WriteTransaction,
    table: &str,
    schema: &TableSchema,
    constraint: &TableConstraint,
) -> Result<(), String> {
    let width = schema.columns().len();
    let rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    let rows: Vec<Vec<Cell>> = rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
        .map(|r| {
            let (_, v) = r.map_err(map_err)?;
            let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
            if cells.len() < width {
                cells.resize(width, Cell::Null);
            }
            Ok::<_, String>(cells)
        })
        .collect::<Result<_, _>>()?;
    drop(rows_t);
    let scoped = schema.clone().with_constraints(vec![constraint.clone()]);
    for cells in &rows {
        validate_fk_out_for_row_in(wtx, &scoped, cells)?;
    }
    Ok(())
}

fn create_in(
    wtx: &WriteTransaction,
    schema: &TableSchema,
    if_not_exists: bool,
) -> Result<bool, String> {
    schema.validate()?;
    if get_schema_in(wtx, &schema.name)?.is_some() {
        if if_not_exists {
            return Ok(false);
        }
        return Err(format!("table `{}` already exists", schema.name));
    }
    let mut schema = schema.clone();
    force_pk_not_null(&mut schema);
    for c in schema.constraints().to_vec() {
        if let TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            ..
        } = &c
        {
            validate_fk_target_in(wtx, &schema, columns, ref_table, ref_columns)?;
        }
    }
    let schema = &schema;
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

fn drop_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    name: &str,
    if_exists: bool,
) -> Result<bool, String> {
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
    drop_secondary_indexes_for_table_in(wtx, tenant_scope, name)?;
    {
        let mut hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
        hypertables.remove(name).map_err(map_err)?;
    }
    Ok(true)
}

fn add_column_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    column: &Column,
) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    if schema.column(&column.name).is_some() {
        return Err(format!(
            "column `{}` already exists in table `{table}`",
            column.name
        ));
    }
    schema.columns_mut().push(column.clone());
    put_schema_in(wtx, tenant_scope, &schema)
}

/// Persist a (possibly renamed) schema back into the catalog under its `name` key.
/// The single place an ALTER rewrites the catalog entry (CONCEPT:EG-KG.query.rename-table-moves-catalog).
fn put_schema_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    schema: &TableSchema,
) -> Result<(), String> {
    schema.validate()?;
    drop_secondary_indexes_for_table_in(wtx, tenant_scope, &schema.name)?;
    let blob = rmp_serde::to_vec_named(schema).map_err(|e| format!("encode schema: {e}"))?;
    let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
    cat.insert(schema.name.as_str(), blob.as_slice())
        .map_err(map_err)?;
    Ok(())
}

/// Rewrite every stored row of `table` through `f` (which mutates the row's `Vec<Cell>`
/// in place), inside the open write txn — the atomic row-migration primitive shared by
/// DROP COLUMN and ALTER COLUMN TYPE (CONCEPT:EG-KG.query.rename-table-moves-catalog). An error from `f` on ANY row
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
        let cells: Vec<Cell> = decode_stored(v.value(), "row")?;
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

/// CONCEPT:EG-KG.query.rename-table-moves-catalog — `DROP COLUMN`: remove `column` from the schema and drop its cell
/// from every stored row (positional splice at the column's index). Refuses to drop the
/// only column of a table. `if_exists` turns an absent-column error into a no-op.
fn drop_column_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    column: &str,
    if_exists: bool,
) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    {
        let hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
        if let Some(value) = hypertables.get(table).map_err(map_err)? {
            let plan = decode_stored::<HypertablePlan>(value.value(), "hypertable")?;
            if plan.time_column == column {
                return Err(format!(
                    "cannot drop hypertable time column `{table}.{column}`"
                ));
            }
        };
    }
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
    if schema.columns().len() == 1 {
        return Err(format!(
            "cannot drop the only column `{column}` of table `{table}`"
        ));
    }
    schema.columns_mut().remove(idx);
    put_schema_in(wtx, tenant_scope, &schema)?;
    // Splice the dropped cell out of each stored row (rows may be short if written before
    // a later ADD COLUMN — guard the index).
    migrate_rows_in(wtx, table, |cells| {
        if idx < cells.len() {
            cells.remove(idx);
        }
        Ok(())
    })
}

/// CONCEPT:EG-KG.query.rename-table-moves-catalog — `RENAME COLUMN a TO b`: rename in the schema only (rows are
/// positional). Errors if `from` is absent or `to` already exists.
fn rename_column_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
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
    schema.columns_mut()[idx].name = to.to_string();
    put_schema_in(wtx, tenant_scope, &schema)?;
    let replacement = {
        let hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
        let plan = hypertables
            .get(table)
            .map_err(map_err)?
            .map(|value| decode_stored::<HypertablePlan>(value.value(), "hypertable"))
            .transpose()?
            .filter(|plan| plan.time_column == from);
        plan
    };
    if let Some(mut plan) = replacement {
        plan.time_column = to.to_string();
        let bytes =
            rmp_serde::to_vec_named(&plan).map_err(|e| format!("encode hypertable: {e}"))?;
        let mut hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
        hypertables
            .insert(table, bytes.as_slice())
            .map_err(map_err)?;
    }
    Ok(())
}

/// CONCEPT:EG-KG.query.rename-table-moves-catalog — `RENAME TO newtable`: move the catalog entry, the sequence, and
/// every stored row's key from `table` to `new_name`. Errors if the table is absent or
/// `new_name` already exists.
fn rename_table_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    new_name: &str,
) -> Result<(), String> {
    if table == new_name {
        return Ok(());
    }
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    if get_schema_in(wtx, new_name)?.is_some() {
        return Err(format!("table `{new_name}` already exists"));
    }
    drop_secondary_indexes_for_table_in(wtx, tenant_scope, table)?;
    // Catalog: drop the old key, write the schema under the new name.
    schema.name = new_name.to_string();
    {
        let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
        cat.remove(table).map_err(map_err)?;
    }
    put_schema_in(wtx, tenant_scope, &schema)?;
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
    let renamed_hypertable = {
        let hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
        let plan = hypertables
            .get(table)
            .map_err(map_err)?
            .map(|value| decode_stored::<HypertablePlan>(value.value(), "hypertable"))
            .transpose()?;
        plan
    };
    if let Some(mut plan) = renamed_hypertable {
        plan.table = new_name.to_string();
        let bytes =
            rmp_serde::to_vec_named(&plan).map_err(|e| format!("encode hypertable: {e}"))?;
        let mut hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
        hypertables.remove(table).map_err(map_err)?;
        hypertables
            .insert(new_name, bytes.as_slice())
            .map_err(map_err)?;
    }
    Ok(())
}

/// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER COLUMN col TYPE newtype`: best-effort coerce every stored
/// cell at the column's index to `new_type`, then record the new type. A cell that
/// cannot be coerced returns `Err` so the whole ALTER rolls back (no partial migration).
fn alter_column_type_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    column: &str,
    new_type: ColumnType,
) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let idx = schema
        .column_index(column)
        .ok_or_else(|| format!("column `{column}` does not exist in table `{table}`"))?;
    let nullable = schema.columns()[idx].nullable;
    // Migrate rows FIRST so an incompatible value aborts before the schema is touched.
    migrate_rows_in(wtx, table, |cells| {
        if let Some(cell) = cells.get_mut(idx) {
            *cell = coerce_cell(cell, new_type, nullable)
                .map_err(|e| format!("cannot ALTER COLUMN `{column}` TYPE: {e}"))?;
        }
        Ok(())
    })?;
    schema.columns_mut()[idx].ty = new_type;
    put_schema_in(wtx, tenant_scope, &schema)
}

/// CONCEPT:EG-KG.query.rename-table-moves-catalog — `DROP CONSTRAINT name`: this catalog stores constraints per column
/// (PK / UNIQUE / CHECK) without user-visible names, so a dropped constraint is matched
/// against Postgres's synthesized names — `<table>_pkey`, `<table>_<col>_key`,
/// `<table>_<col>_check` — and the matching column flag is cleared. Errors if nothing
/// matches unless `if_exists`.
fn drop_constraint_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    constraint: &str,
    if_exists: bool,
) -> Result<(), String> {
    let mut schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let mut matched = false;
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — a table-level constraint (composite PK/UNIQUE/FK/CHECK)
    // is tried FIRST so an explicit `CONSTRAINT <name>` always takes precedence over
    // a same-named synthesized column-flag match.
    if schema.remove_constraint_named(constraint) {
        matched = true;
    }
    if !matched && constraint == format!("{table}_pkey") {
        for c in schema.columns_mut() {
            if c.primary_key {
                c.primary_key = false;
                c.unique = false;
                matched = true;
            }
        }
    }
    if !matched {
        for c in schema.columns_mut() {
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
    put_schema_in(wtx, tenant_scope, &schema)
}

/// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — `ALTER TABLE … ADD CONSTRAINT`: validate `constraint`
/// against the table's schema AS IT WOULD BE with it added (structural + cross-table
/// FK checks, reusing exactly what `CREATE TABLE` runs), THEN re-validate every
/// EXISTING row against it (PK/UNIQUE uniqueness + implied NOT NULL, CHECK, or FK)
/// before persisting — a constraint the current data already violates is refused,
/// matching Postgres.
fn add_constraint_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    constraint: TableConstraint,
) -> Result<(), String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let mut trial = schema.clone();
    trial.push_constraint(constraint.clone());
    trial.validate()?;
    if let TableConstraint::ForeignKey {
        columns,
        ref_table,
        ref_columns,
        ..
    } = &constraint
    {
        validate_fk_target_in(wtx, &trial, columns, ref_table, ref_columns)?;
    }
    force_pk_not_null(&mut trial);
    match &constraint {
        TableConstraint::PrimaryKey { .. } => {
            validate_not_null_in(wtx, table, &trial)?;
            validate_uniqueness_in(wtx, table, &trial)?;
        }
        TableConstraint::Unique { .. } => {
            validate_uniqueness_in(wtx, table, &trial)?;
        }
        TableConstraint::Check { .. } => {
            validate_table_checks_over_existing_in(wtx, table, &trial, &[&constraint])?;
        }
        TableConstraint::ForeignKey { .. } => {
            validate_existing_fk_children_in(wtx, table, &trial, &constraint)?;
        }
    }
    put_schema_in(wtx, tenant_scope, &trial)
}

/// Best-effort coerce an already-stored [`Cell`] to `ty` for an `ALTER COLUMN … TYPE`
/// migration (CONCEPT:EG-KG.query.rename-table-moves-catalog). A cell already of the target shape is kept verbatim; a
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
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-002 — deliberately NOT extended for `Uuid`/`Numeric`/
    // `TimestampTz`/`Array`, even though they share a storage `Cell` variant with an
    // existing type (`Text`/`Float`/`Timestamp`/`Json`): those four have EXTRA
    // validation beyond their storage shape (UUID format, NUMERIC precision/scale,
    // an explicit TIMESTAMPTZ offset, a well-typed array), so an `ALTER COLUMN …
    // TYPE` onto one of them must always run the full `Cell::coerce` re-validation
    // below — treating "same storage shape" as "already valid" would silently skip
    // that check.
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

/// Render `old` into a JSON value best-tuned for coercion into `ty` (CONCEPT:EG-KG.query.rename-table-moves-catalog):
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
/// (CONCEPT:EG-KG.query.rename-table-moves-catalog). Returns `None` for an unrecognized spelling (→ rejected downstream).
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
    tenant_scope: &str,
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
    for (offset, cells) in inserted.iter().enumerate() {
        maintain_secondary_row_in(
            wtx,
            tenant_scope,
            table,
            &schema,
            first_rowid + offset as u64,
            None,
            Some(cells),
        )?;
    }
    // Uniqueness over the post-insert state (reads staged writes through `wtx`).
    validate_uniqueness_in(wtx, table, &schema)?;
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — table-level CHECK + outgoing FOREIGN KEY per inserted row.
    for cells in &inserted {
        validate_row_constraints_in(wtx, &schema, cells)?;
    }
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
/// insert paths (CONCEPT:EG-KG.query.delete-returning-sees-row).
fn build_insert_cells(
    schema: &TableSchema,
    col_order: &[String],
    targets: &[usize],
    row: &[Value],
    rowid: u64,
) -> Result<Vec<Cell>, String> {
    let width = schema.columns().len();
    if row.len() != col_order.len() {
        return Err(format!(
            "INSERT column/value count mismatch: {} columns, {} values",
            col_order.len(),
            row.len()
        ));
    }
    let mut cells: Vec<Cell> = vec![Cell::Null; width];
    let mut supplied = vec![false; width];
    for (val, &idx) in row.iter().zip(targets.iter()) {
        let col = &schema.columns()[idx];
        cells[idx] = Cell::coerce(val, col.ty, col.nullable)?;
        supplied[idx] = true;
    }
    for (ci, col) in schema.columns().iter().enumerate() {
        if supplied[ci] {
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
    for (ci, col) in schema.columns().iter().enumerate() {
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

/// `INSERT … ON CONFLICT (…) DO NOTHING|DO UPDATE` (CONCEPT:EG-KG.query.delete-returning-sees-row). For each row: if a
/// UNIQUE/PK column value already exists (in the committed OR same-batch state), apply
/// the conflict action — skip (DO NOTHING) or merge the SET assignments into the
/// existing row (DO UPDATE); otherwise insert a fresh row. Returns the rows that were
/// inserted-or-updated (for `RETURNING`). Reuses [`validate_uniqueness_in`] as the final
/// integrity gate so a DO UPDATE that itself introduces a duplicate still aborts.
fn insert_on_conflict_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    col_order: &[String],
    rows: &[Vec<Value>],
    action: &ConflictAction,
) -> Result<Vec<Vec<Cell>>, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let width = schema.columns().len();
    let targets = resolve_targets(&schema, table, col_order)?;
    let unique_cols: Vec<usize> = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_unique())
        .map(|(i, _)| i)
        .collect();
    let mut target_position_by_col = vec![None; width];
    for (position, &column) in targets.iter().enumerate() {
        // Preserve the historical `position` behavior if a malformed INSERT names
        // the same target column more than once: the first supplied value wins for
        // conflict detection (row construction still retains its existing rules).
        target_position_by_col[column].get_or_insert(position);
    }

    // Current unique-value snapshot (committed + staged), rebuilt from the store. When
    // the physical row table does not exist yet there are simply no existing rows.
    let mut existing: Vec<(u64, Vec<Cell>)> = Vec::new();
    let mut row_slot: HashMap<u64, usize> = HashMap::new();
    let mut unique_rows: Vec<HashMap<String, u64>> =
        (0..unique_cols.len()).map(|_| HashMap::new()).collect();
    if let Ok(rows_t) = wtx.open_table(ROWS) {
        for r in rows_t
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (k, v) = r.map_err(map_err)?;
            let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
            if cells.len() < width {
                cells.resize(width, Cell::Null);
            }
            let rowid = k.value().1;
            row_slot.insert(rowid, existing.len());
            for (slot, &column) in unique_cols.iter().enumerate() {
                if let Some(key) = unique_cell_key(&cells[column]) {
                    // Existing corruption is still rejected by the authoritative
                    // final validation. Keeping the first row mirrors the old scan.
                    unique_rows[slot].entry(key).or_insert(rowid);
                }
            }
            existing.push((rowid, cells));
        }
    }

    let mut affected: Vec<Vec<Cell>> = Vec::new();
    let mut index_changes: Vec<(u64, Option<Vec<Cell>>, Option<Vec<Cell>>)> = Vec::new();
    let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    for row in rows {
        // Coerce the supplied unique-column values to detect a conflict.
        let mut conflict_rowid: Option<u64> = None;
        for (slot, &uci) in unique_cols.iter().enumerate() {
            // The value this row supplies for the unique column (if any).
            let Some(pos) = target_position_by_col[uci] else {
                continue;
            };
            let col = &schema.columns()[uci];
            let supplied = Cell::coerce(&row[pos], col.ty, col.nullable)?;
            if let Some(key) = unique_cell_key(&supplied) {
                if let Some(&rowid) = unique_rows[slot].get(&key) {
                    conflict_rowid = Some(rowid);
                    break;
                }
            }
        }

        match (conflict_rowid, action) {
            (Some(_), ConflictAction::DoNothing) => { /* skip */ }
            (Some(rid), ConflictAction::DoUpdate(set)) => {
                // Merge the SET assignments into the conflicting row.
                let index = *row_slot.get(&rid).expect("conflict rowid present");
                let slot = &mut existing[index];
                let old_cells = slot.1.clone();
                for (map, &column) in unique_rows.iter_mut().zip(&unique_cols) {
                    if let Some(key) = unique_cell_key(&slot.1[column]) {
                        if map.get(&key) == Some(&rid) {
                            map.remove(&key);
                        }
                    }
                }
                for (col, val) in set {
                    let idx = schema.column_index(col).ok_or_else(|| {
                        format!("column `{col}` does not exist in table `{table}`")
                    })?;
                    let c = &schema.columns()[idx];
                    slot.1[idx] = Cell::coerce(val, c.ty, c.nullable)?;
                }
                // Re-check CHECK constraints on the updated row.
                for (ci, col) in schema.columns().iter().enumerate() {
                    if let Some(check) = &col.check {
                        if !check.holds(&slot.1[ci].to_json()) {
                            return Err(format!(
                                "updated row violates CHECK constraint on column `{}`",
                                col.name
                            ));
                        }
                    }
                }
                for (map, &column) in unique_rows.iter_mut().zip(&unique_cols) {
                    if let Some(key) = unique_cell_key(&slot.1[column]) {
                        map.entry(key).or_insert(rid);
                    }
                }
                let blob =
                    rmp_serde::to_vec_named(&slot.1).map_err(|e| format!("encode row: {e}"))?;
                rows_t
                    .insert((table, rid), blob.as_slice())
                    .map_err(map_err)?;
                let new_cells = slot.1.clone();
                affected.push(new_cells.clone());
                index_changes.push((rid, Some(old_cells), Some(new_cells)));
            }
            (None, _) => {
                // A fresh insert: allocate one rowid, build + write the row.
                let rowid = alloc_rowids(wtx, table, 1)?;
                let cells = build_insert_cells(&schema, col_order, &targets, row, rowid)?;
                let blob =
                    rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?;
                rows_t
                    .insert((table, rowid), blob.as_slice())
                    .map_err(map_err)?;
                row_slot.insert(rowid, existing.len());
                for (map, &column) in unique_rows.iter_mut().zip(&unique_cols) {
                    if let Some(key) = unique_cell_key(&cells[column]) {
                        map.entry(key).or_insert(rowid);
                    }
                }
                existing.push((rowid, cells.clone()));
                affected.push(cells.clone());
                index_changes.push((rowid, None, Some(cells)));
            }
        }
    }
    drop(rows_t);
    for (rowid, old, new) in &index_changes {
        maintain_secondary_row_in(
            wtx,
            tenant_scope,
            table,
            &schema,
            *rowid,
            old.as_deref(),
            new.as_deref(),
        )?;
    }
    validate_uniqueness_in(wtx, table, &schema)?;
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — table-level CHECK + outgoing FOREIGN KEY per
    // inserted-or-updated row (fresh insert AND a DO UPDATE merge both land in `affected`).
    for cells in &affected {
        validate_row_constraints_in(wtx, &schema, cells)?;
    }
    Ok(affected)
}

/// Canonical key used by the existing uniqueness validator, factored so the
/// ON-CONFLICT lookup directory and the final integrity pass cannot drift. SQL
/// NULL is deliberately absent because UNIQUE permits multiple NULL values.
fn unique_cell_key(cell: &Cell) -> Option<String> {
    let value = cell.to_json();
    (!value.is_null()).then(|| value.to_string())
}

/// Build a `col -> json` row map for predicate evaluation (CONCEPT:EG-KG.query.compound-predicate-decode): one
/// entry per schema column, the cell decoded to its JSON value. A column the
/// predicate references that is NOT in the schema is simply absent (reads as NULL).
fn row_map(schema: &TableSchema, cells: &[Cell]) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::with_capacity(schema.columns().len());
    for (ci, col) in schema.columns().iter().enumerate() {
        let cell = cells.get(ci).cloned().unwrap_or(Cell::Null);
        map.insert(col.name.clone(), cell.to_json());
    }
    map
}

fn update_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
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
        let c = &schema.columns()[idx];
        assigns.push((idx, Cell::coerce(val, c.ty, c.nullable)?));
    }
    let width = schema.columns().len();
    let mut updated: Vec<Vec<Cell>> = Vec::new();
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — `(rowid, old_cells, new_cells)` for the parent-side
    // referential-action pass AFTER the write is staged.
    let mut changed: Vec<(u64, Vec<Cell>, Vec<Cell>)> = Vec::new();
    {
        let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
        let mut hits: Vec<(u64, Vec<Cell>)> = Vec::new();
        for r in rows_t
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (k, v) = r.map_err(map_err)?;
            let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
            if cells.len() < width {
                cells.resize(width, Cell::Null);
            }
            // CONCEPT:EG-KG.query.compound-predicate-decode — serializable per-row predicate eval INSIDE the open
            // redb write txn (the row cannot change before commit).
            if selector.eval(&row_map(&schema, &cells)) {
                hits.push((k.value().1, cells));
            }
        }
        for (rowid, old_cells) in hits {
            let mut cells = old_cells.clone();
            for (idx, cell) in &assigns {
                cells[*idx] = cell.clone();
            }
            // CHECK constraints on the updated row.
            for (ci, col) in schema.columns().iter().enumerate() {
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
            changed.push((rowid, old_cells, cells.clone()));
            updated.push(cells);
        }
    }
    for (rowid, old_cells, new_cells) in &changed {
        maintain_secondary_row_in(
            wtx,
            tenant_scope,
            table,
            &schema,
            *rowid,
            Some(old_cells),
            Some(new_cells),
        )?;
    }
    validate_uniqueness_in(wtx, table, &schema)?;
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — table-level CHECK + outgoing FOREIGN KEY, then the
    // parent-side referential action for every OTHER table whose FK references a
    // column this UPDATE changed (NO ACTION/RESTRICT/CASCADE/SET NULL).
    let mut visited = HashSet::new();
    for (rowid, old_cells, new_cells) in &changed {
        validate_row_constraints_in(wtx, &schema, new_cells)?;
        enforce_fk_on_parent_change_in(
            wtx,
            tenant_scope,
            table,
            *rowid,
            old_cells,
            Some(new_cells),
            &mut visited,
        )?;
    }
    Ok(updated)
}

fn delete_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    selector: &eg_types::RowPredicate,
) -> Result<Vec<Vec<Cell>>, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    let width = schema.columns().len();
    // Capture the pre-removal cells (CONCEPT:EG-KG.query.delete-returning-sees-row — DELETE … RETURNING sees the row
    // as it was before deletion).
    let mut removed: Vec<Vec<Cell>> = Vec::new();
    let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    let mut victims: Vec<(u64, Vec<Cell>)> = Vec::new();
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (k, v) = r.map_err(map_err)?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        // CONCEPT:EG-KG.query.compound-predicate-decode — serializable per-row predicate eval inside the write txn.
        if selector.eval(&row_map(&schema, &cells)) {
            victims.push((k.value().1, cells));
        }
    }
    for (rowid, cells) in &victims {
        rows_t.remove((table, *rowid)).map_err(map_err)?;
        removed.push(cells.clone());
    }
    drop(rows_t);
    for (rowid, cells) in &victims {
        maintain_secondary_row_in(
            wtx,
            tenant_scope,
            table,
            &schema,
            *rowid,
            Some(cells),
            None,
        )?;
    }
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — the parent-side referential action for every OTHER
    // table whose FK references a column of a row this DELETE removed.
    let mut visited = HashSet::new();
    for (rowid, cells) in &victims {
        enforce_fk_on_parent_change_in(
            wtx,
            tenant_scope,
            table,
            *rowid,
            cells,
            None,
            &mut visited,
        )?;
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
    // Single-column groups from the per-column PK/UNIQUE flags, PLUS every
    // multi-column PK/UNIQUE table-level constraint (CONCEPT:EG-KG.query.table-schema-constraints/NE-001) — each
    // group is checked independently (a composite key is the JOIN of its columns'
    // individual coerced-value keys; a NULL in ANY participating column exempts the
    // row from THAT group, mirroring single-column UNIQUE's NULL exemption and
    // Postgres's multi-column UNIQUE semantics).
    let mut groups: Vec<(Vec<usize>, String)> = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_unique())
        .map(|(i, c)| (vec![i], format!("column `{}`", c.name)))
        .collect();
    for c in schema.constraints() {
        let cols = match c {
            TableConstraint::PrimaryKey { columns, .. }
            | TableConstraint::Unique { columns, .. } => columns,
            _ => continue,
        };
        let idxs: Vec<usize> = cols
            .iter()
            .map(|name| {
                schema
                    .column_index(name)
                    .expect("constraint column existence validated at DDL time")
            })
            .collect();
        let name = TableSchema::constraint_display_name(table, c);
        groups.push((idxs, format!("`{name}`")));
    }
    if groups.is_empty() {
        return Ok(());
    }
    let width = schema.columns().len();
    let rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    let mut seen: Vec<HashSet<String>> = vec![HashSet::new(); groups.len()];
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        for (slot, (idxs, label)) in groups.iter().enumerate() {
            let mut parts: Vec<String> = Vec::with_capacity(idxs.len());
            let mut any_null = false;
            for &ci in idxs {
                match unique_cell_key(&cells[ci]) {
                    Some(k) => parts.push(k),
                    None => {
                        any_null = true;
                        break;
                    }
                }
            }
            if any_null {
                continue;
            }
            let key = parts.join("\u{1}");
            if !seen[slot].insert(key) {
                return Err(format!(
                    "duplicate key value violates unique constraint on {label}"
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

    #[test]
    fn stored_sql_decoder_rejects_declared_allocation_bombs() {
        let bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_stored::<Vec<Cell>>(&bomb, "row").is_err());
    }

    fn col(name: &str, ty: ColumnType, nullable: bool) -> Column {
        Column::new(name, ty, nullable, false)
    }

    fn metrics_schema() -> TableSchema {
        TableSchema::new(
            "metrics",
            vec![
                col("ts", ColumnType::Timestamp, false),
                col("name", ColumnType::Text, false),
                col("value", ColumnType::Double, true),
            ],
        )
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

    /// `get_row` is a true point get: it returns exactly the row at the given
    /// PHYSICAL rowid — the first insert into a fresh table allocates rowids
    /// sequentially from 0 (`alloc_rowids`'s own `unwrap_or(0)` default) — and
    /// `None`, not an error, for a rowid that was never allocated.
    #[test]
    fn get_row_is_a_true_point_get_by_physical_rowid() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        store
            .insert_rows(
                "metrics",
                &cols,
                &[
                    vec![1i64.into(), "cpu".into(), 0.5.into()],
                    vec![2i64.into(), "mem".into(), 0.9.into()],
                ],
            )
            .unwrap();
        let r0 = store.get_row("metrics", 0).unwrap().unwrap();
        assert_eq!(r0[1], Cell::Text("cpu".into()));
        let r1 = store.get_row("metrics", 1).unwrap().unwrap();
        assert_eq!(r1[1], Cell::Text("mem".into()));
        assert_eq!(
            store.get_row("metrics", 99).unwrap(),
            None,
            "no row ever occupied rowid 99"
        );
    }

    /// A table whose schema exists but has never had a committed INSERT has no
    /// physical row table yet — `get_row` returns `None`, mirroring `scan`'s own
    /// "no rows table yet ⇒ empty, not an error" behavior.
    #[test]
    fn get_row_before_any_insert_is_none_not_an_error() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        assert_eq!(store.get_row("metrics", 0).unwrap(), None);
    }

    /// A nonexistent table errors, exactly like `scan`.
    #[test]
    fn get_row_on_a_nonexistent_table_errors_like_scan() {
        let (store, _p) = TableStore::open_temp().unwrap();
        assert!(store.scan("ghost").is_err());
        assert!(store.get_row("ghost", 0).is_err());
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
        // CONCEPT:EG-KG.query.compound-predicate-decode — AND / range / IN predicates select rows in the store.
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
        assert_eq!(schema.columns().len(), 3);
        assert_eq!(schema.columns()[0].ty, ColumnType::Timestamp);
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

    // ── constraints + transactions + sequences (CONCEPT:EG-KG.query.register-each-user-table) ───────────────

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
        TableSchema::new("items", vec![id, sku, qty])
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

    // ── ON CONFLICT + RETURNING (CONCEPT:EG-KG.query.delete-returning-sees-row) ──────────────────────────────

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
    fn on_conflict_directory_tracks_same_batch_inserts_and_unique_updates() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&constrained_schema(), false).unwrap();

        let rows: Vec<Vec<Value>> = (0..128)
            .flat_map(|i| {
                let sku = Value::String(format!("sku-{i:03}"));
                [vec![sku.clone()], vec![sku]]
            })
            .collect();
        let affected = store
            .insert_rows_on_conflict("items", &["sku".into()], &rows, &ConflictAction::DoNothing)
            .unwrap();
        assert_eq!(affected.len(), 128);
        assert_eq!(store.scan("items").unwrap().len(), 128);

        // Move one unique value, then address the moved value again in the same
        // batch. The derived directory must observe the update immediately.
        let mut set = serde_json::Map::new();
        set.insert("sku".into(), "renamed".into());
        let updated = store
            .insert_rows_on_conflict(
                "items",
                &["sku".into()],
                &[vec!["sku-000".into()], vec!["renamed".into()]],
                &ConflictAction::DoUpdate(set),
            )
            .unwrap();
        assert_eq!(updated.len(), 2);
        assert_eq!(store.scan("items").unwrap().len(), 128);
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

    // ── view catalog (CONCEPT:EG-KG.query.create-drop-view) ─────────────────────────────────────────

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

    // ── function catalog (CONCEPT:EG-KG.query.create-drop-function) ──────────────────────────────────────

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

    #[test]
    fn hypertable_catalog_validates_and_tracks_schema_changes() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let plan = HypertablePlan {
            table: "metrics".to_string(),
            time_column: "ts".to_string(),
        };
        store.put_hypertable(&plan).unwrap();
        assert_eq!(store.list_hypertables().unwrap(), vec![plan]);

        let mut rename_column = TableTxn::new();
        rename_column.push(TxnOp::RenameColumn {
            table: "metrics".to_string(),
            from: "ts".to_string(),
            to: "observed_at".to_string(),
        });
        store.commit_txn(&rename_column).unwrap();
        assert_eq!(
            store.list_hypertables().unwrap(),
            vec![HypertablePlan {
                table: "metrics".to_string(),
                time_column: "observed_at".to_string(),
            }]
        );

        let mut drop_time = TableTxn::new();
        drop_time.push(TxnOp::DropColumn {
            table: "metrics".to_string(),
            column: "observed_at".to_string(),
            if_exists: false,
        });
        assert!(drop_time.ops.len() == 1 && store.commit_txn(&drop_time).is_err());

        let mut rename_table = TableTxn::new();
        rename_table.push(TxnOp::RenameTable {
            table: "metrics".to_string(),
            new_name: "measurements".to_string(),
        });
        store.commit_txn(&rename_table).unwrap();
        assert_eq!(
            store.list_hypertables().unwrap(),
            vec![HypertablePlan {
                table: "measurements".to_string(),
                time_column: "observed_at".to_string(),
            }]
        );

        store.drop_table("measurements", false).unwrap();
        assert!(store.list_hypertables().unwrap().is_empty());
    }

    #[test]
    fn hypertable_requires_a_timestamp_column() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        assert!(store
            .put_hypertable(&HypertablePlan {
                table: "metrics".to_string(),
                time_column: "name".to_string(),
            })
            .is_err());
    }

    fn sql_batch(batch_id: &str) -> MutationBatch {
        use eg_types::mutation_batch::{
            MutationOperation, MutationRequestContext, MutationSurface, MUTATION_BATCH_VERSION,
        };
        MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            context: MutationRequestContext {
                request_id: 7,
                principal: format!("principal:sha256:{}", "a".repeat(64)),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
            },
            tenant: "tenant-a".to_string(),
            graph: "graph-a".to_string(),
            placement_epoch: 0,
            idempotency_key: format!("idem-{batch_id}"),
            expected_graph_version: Some(0),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Query,
                domain: MutationDomain::SqlCatalog,
                method: eg_types::protocol::Method::ApplyMutation {
                    event_type: "sql_catalog_operation".to_string(),
                    query:
                        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                            .to_string(),
                },
            }],
            outbox: vec![MutationOutboxIntent {
                topic: "engine.projection.rebuild".to_string(),
                key: batch_id.to_string(),
                payload: vec![1],
                headers: Default::default(),
            }],
            created_at_ms: 100,
        }
    }

    fn create_metrics_txn() -> TableTxn {
        let mut txn = TableTxn::new();
        txn.push(TxnOp::CreateTable {
            schema: metrics_schema(),
            if_not_exists: false,
        });
        txn
    }

    #[test]
    fn mutation_batch_crash_before_commit_recovers_neither_half() {
        for point in [
            SqlMutationCrashpoint::BeforeRows,
            SqlMutationCrashpoint::AfterRowsBeforeMetadata,
            SqlMutationCrashpoint::BeforeCommit,
        ] {
            let (store, path) = TableStore::open_temp().unwrap();
            let batch = sql_batch(&format!("before-{point:?}"));
            assert!(store
                .commit_txn_batch_inner(&create_metrics_txn(), &batch, 101, Some(point), None)
                .is_err());
            drop(store);
            let reopened = TableStore::open(&path).unwrap();
            assert!(reopened.get_schema("metrics").unwrap().is_none());
            assert!(reopened.mutation_batch(&batch.batch_id).unwrap().is_none());
            assert!(reopened
                .mutation_outbox(&batch.batch_id)
                .unwrap()
                .is_empty());
            assert_eq!(reopened.mutation_version("tenant-a", "graph-a").unwrap(), 0);
        }
    }

    #[test]
    fn mutation_batch_after_commit_replays_without_sql_reexecution() {
        let (store, path) = TableStore::open_temp().unwrap();
        let batch = sql_batch("after-commit");
        assert!(store
            .commit_txn_batch_inner(
                &create_metrics_txn(),
                &batch,
                101,
                Some(SqlMutationCrashpoint::AfterCommitBeforeAck),
                None,
            )
            .is_err());
        drop(store);

        let reopened = TableStore::open(&path).unwrap();
        assert!(reopened.get_schema("metrics").unwrap().is_some());
        assert!(reopened.mutation_batch(&batch.batch_id).unwrap().is_some());
        assert_eq!(reopened.mutation_outbox(&batch.batch_id).unwrap().len(), 2);
        assert_eq!(reopened.mutation_version("tenant-a", "graph-a").unwrap(), 1);

        // Reapplying CREATE TABLE would fail. A successful replay therefore proves
        // the durable result was returned without executing the transaction twice.
        let replay = reopened
            .commit_txn_batch(&create_metrics_txn(), &batch, 102)
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(reopened.list_tables().unwrap(), vec!["metrics".to_string()]);
        assert_eq!(reopened.mutation_outbox(&batch.batch_id).unwrap().len(), 2);
    }

    /// L-RLS-1-adjacent (WS-H, the served SQL context cache): [`TableStore::catalog_fingerprint`]
    /// must change on EVERY committed SQL-domain write, regardless of which `(tenant,
    /// scope)` pair the write's `MutationBatch` used -- not just the one a caller
    /// happens to be asking about. `mutation_version("tenant-a", "graph-a")` alone
    /// would miss a commit under a DIFFERENT scope string entirely (the exact shape
    /// `src/server/handlers/sqlite_file.rs`'s sqlite-import gateway uses: a fixed
    /// cross-graph scope independent of the calling graph) -- proving the fingerprint
    /// closes that gap is the whole point of scanning every row instead of one key.
    #[test]
    fn catalog_fingerprint_changes_on_any_scope_commit_and_is_deterministic() {
        let (store, _p) = TableStore::open_temp().unwrap();
        let empty = store.catalog_fingerprint().unwrap();
        // Determinism: re-reading with no intervening write returns the identical value.
        assert_eq!(store.catalog_fingerprint().unwrap(), empty);

        // A commit under "graph-a" (sql_batch's default scope) changes the fingerprint.
        let batch_a = sql_batch("fp-a");
        store
            .commit_txn_batch(&create_metrics_txn(), &batch_a, 101)
            .unwrap();
        let after_a = store.catalog_fingerprint().unwrap();
        assert_ne!(
            after_a, empty,
            "a committed CREATE TABLE must change the fingerprint"
        );
        assert_eq!(
            store.catalog_fingerprint().unwrap(),
            after_a,
            "re-reading with no intervening write is deterministic"
        );

        // A SECOND commit under a DIFFERENT scope string ("graph-b" -- standing in for
        // the sqlite-import gateway's fixed cross-graph scope, which is never the
        // literal calling graph) must ALSO change the fingerprint: this is the
        // property `mutation_version(tenant, ONE_graph)` cannot offer on its own.
        let mut batch_b = sql_batch("fp-b");
        batch_b.graph = "graph-b".to_string();
        batch_b.idempotency_key = "idem-fp-b".to_string();
        let mut txn_b = TableTxn::new();
        txn_b.push(TxnOp::CreateTable {
            schema: TableSchema::new("other_table", vec![col("value", ColumnType::Text, false)]),
            if_not_exists: false,
        });
        store.commit_txn_batch(&txn_b, &batch_b, 102).unwrap();
        let after_b = store.catalog_fingerprint().unwrap();
        assert_ne!(
            after_b, after_a,
            "a committed write under a DIFFERENT scope string must ALSO change the \
             fingerprint -- a cache keyed on only ONE scope's mutation_version would \
             silently miss this commit"
        );
    }
}
