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
//! `PRIMARY KEY`/`UNIQUE` uniqueness, column-level and rich table-level `CHECK`
//! expressions, and explicit FOREIGN KEY referential actions are all enforced on
//! the write path; a violation aborts the (one-shot or multi-statement) transaction.

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

use super::index::{
    catalog_key as secondary_catalog_key, entry_key as secondary_entry_key,
    entry_prefix as secondary_entry_prefix, entry_range as secondary_entry_range,
    rowid_from_entry_key, validate_spec as validate_secondary_spec, SecondaryIndexLookup,
    SecondaryIndexOrder, SecondaryIndexSpec, MAX_SECONDARY_INDEXES_PER_TABLE,
    MAX_SECONDARY_INDEX_BUILD_ROWS, MAX_SECONDARY_INDEX_CANDIDATES,
};
use super::migration::{
    MigrationState, SchemaMigration, SchemaMigrationApply, SchemaMigrationOperation,
    SchemaMigrationRecord, SecondaryIndexPolicy,
};
use super::schema::{
    Cell, CheckExpr, Column, ColumnType, RefAction, StoredFunction, TableConstraint, TableSchema,
};
// CONCEPT:EG-KG.query.real-ann-top-k/EG-313 — the durable pgvector ANN index registration the exec
// pushdown consults to choose a real eg-ann index over the brute-force scan.
use crate::sql::{AnnIndexPlan, HypertablePlan};

/// One row's index-maintenance delta: `(row id, before cells, after cells)`.
/// Named rather than spelled inline so the tuple's meaning is stated once
/// (clippy::type_complexity).
type IndexChange = (u64, Option<Vec<Cell>>, Option<Vec<Cell>>);

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
/// `(tenant_scope, table) -> current schema version`.  Kept separate from the
/// SQL-domain DML OCC counter because a schema reader must not mistake a row
/// write for a schema transition.
const SCHEMA_VERSIONS: TableDefinition<(&str, &str), u64> =
    TableDefinition::new("__sql_schema_versions__");
/// `tenant_scope -> catalog-wide schema version`, advanced once per committed
/// migration regardless of which table changed.
const SCHEMA_CATALOG_VERSIONS: TableDefinition<&str, u64> =
    TableDefinition::new("__sql_schema_catalog_versions__");
/// `(tenant_scope, table, migration_id) -> SchemaMigrationRecord`.
const SCHEMA_MIGRATIONS: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("__sql_schema_migrations__");
/// `(tenant_scope, table, schema_version) -> migration_id`, the durable ordered
/// chain used for gap/restart verification.
const SCHEMA_MIGRATION_ORDER: TableDefinition<(&str, &str, u64), &str> =
    TableDefinition::new("__sql_schema_migration_order__");
/// `(tenant_scope, catalog_version) -> migration_id`, used to reject catalog
/// version gaps or duplicate assignments after restart.
const SCHEMA_CATALOG_ORDER: TableDefinition<(&str, u64), &str> =
    TableDefinition::new("__sql_schema_catalog_order__");
// Owned (not transaction-lifetime-bound, see `redb::ReadOnlyTable`) table handles
// for the schema-migration catalogs, named so `verify_schema_migrations`'s
// decomposed helpers (below) can pass them around without repeating the full
// generic type at every call site.
type SchemaVersionsTable = redb::ReadOnlyTable<(&'static str, &'static str), u64>;
type SchemaOrderTable = redb::ReadOnlyTable<(&'static str, &'static str, u64), &'static str>;
type SchemaRecordsTable =
    redb::ReadOnlyTable<(&'static str, &'static str, &'static str), &'static [u8]>;
type SchemaCatalogVersionsTable = redb::ReadOnlyTable<&'static str, u64>;
type SchemaCatalogOrderTable = redb::ReadOnlyTable<(&'static str, u64), &'static str>;
type SchemaCatalogTable = redb::ReadOnlyTable<&'static str, &'static [u8]>;

/// Every table `verify_schema_migrations` needs, bundled so
/// `open_schema_migration_tables` can hand them back in one piece.
struct SchemaMigrationTables {
    versions: SchemaVersionsTable,
    order: SchemaOrderTable,
    records: SchemaRecordsTable,
    catalog_versions: SchemaCatalogVersionsTable,
    catalog_order: SchemaCatalogOrderTable,
    catalog: SchemaCatalogTable,
}

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
    /// The authenticated owner scope for schema migrations.  `open()` keeps the
    /// historical single-tenant behavior; served callers should use
    /// `open_scoped()` so a migration cannot be replayed against another tenant.
    scope: Arc<str>,
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
        let store = Self {
            db: Arc::new(db),
            index_scope: Arc::new(tenant_scope.clone()),
            scope: Arc::from(tenant_scope),
        };
        store.verify_schema_migrations()?;
        Ok(store)
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
        self.ensure_legacy_schema_ddl_allowed(name)?;
        let wtx = self.begin()?;
        let dropped = drop_in(&wtx, self.index_scope(), name, if_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(dropped)
    }

    /// `ALTER TABLE ADD COLUMN`: append `column` to the table's schema.
    pub fn add_column(&self, table: &str, column: Column) -> Result<(), String> {
        self.ensure_legacy_schema_ddl_allowed(table)?;
        let wtx = self.begin()?;
        add_column_in(&wtx, self.index_scope(), table, &column)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE DROP COLUMN`: remove `column` from the schema and
    /// drop its cell from every stored row, atomically in one write txn. Errors if the
    /// column (or table) does not exist unless `if_exists`.
    pub fn drop_column(&self, table: &str, column: &str, if_exists: bool) -> Result<(), String> {
        self.ensure_legacy_schema_ddl_allowed(table)?;
        let wtx = self.begin()?;
        drop_column_in(&wtx, self.index_scope(), table, column, if_exists)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE RENAME COLUMN a TO b`: rename a column in place.
    /// Stored rows are positional so they need no migration. Errors if `from` is absent
    /// or `to` already exists.
    pub fn rename_column(&self, table: &str, from: &str, to: &str) -> Result<(), String> {
        self.ensure_legacy_schema_ddl_allowed(table)?;
        let wtx = self.begin()?;
        rename_column_in(&wtx, self.index_scope(), table, from, to)?;
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    /// CONCEPT:EG-KG.query.rename-table-moves-catalog — `ALTER TABLE RENAME TO newtable`: move the table's catalog entry,
    /// sequence, and every stored row's key to `new_name`, atomically. Errors if the
    /// table is absent or `new_name` already exists.
    pub fn rename_table(&self, table: &str, new_name: &str) -> Result<(), String> {
        self.ensure_legacy_schema_ddl_allowed(table)?;
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
        self.ensure_legacy_schema_ddl_allowed(table)?;
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
        self.ensure_legacy_schema_ddl_allowed(table)?;
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
        self.ensure_legacy_schema_ddl_allowed(table)?;
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

    /// The current schema version and digest for a table.  Callers should carry
    /// this snapshot into [`Self::apply_schema_migration`]; the migration's
    /// version/digest pair is an in-transaction CAS precondition that rejects a
    /// stale reader rather than guessing how to merge two schema writers.
    pub fn schema_snapshot(
        &self,
        table: &str,
    ) -> Result<Option<super::migration::SchemaSnapshot>, String> {
        let Some(schema) = self.get_schema(table)? else {
            return Ok(None);
        };
        let version = self.schema_version(table)?;
        Ok(Some(super::migration::SchemaSnapshot {
            tenant_scope: self.scope.to_string(),
            table: schema.name.clone(),
            version,
            schema_digest: schema.schema_digest()?,
        }))
    }

    /// Current authoritative schema version for one table.  Tables created by
    /// older engine versions have an implicit version zero until their first
    /// governed migration commits.
    pub fn schema_version(&self, table: &str) -> Result<u64, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let versions = match rtx.open_table(SCHEMA_VERSIONS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(error) => return Err(map_err(error)),
        };
        Ok(versions
            .get((self.scope.as_ref(), table))
            .map_err(map_err)?
            .map(|value| value.value())
            .unwrap_or(0))
    }

    /// Current catalog-wide schema version for this store scope.  It advances
    /// exactly once for every committed migration and is suitable as a cache
    /// invalidation token for readers that materialize more than one table.
    pub fn schema_catalog_version(&self) -> Result<u64, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let versions = match rtx.open_table(SCHEMA_CATALOG_VERSIONS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(error) => return Err(map_err(error)),
        };
        Ok(versions
            .get(self.scope.as_ref())
            .map_err(map_err)?
            .map(|value| value.value())
            .unwrap_or(0))
    }

    fn ensure_legacy_schema_ddl_allowed(&self, table: &str) -> Result<(), String> {
        let version = self.schema_version(table)?;
        if version > 0 {
            return Err(format!(
                "table `{table}` has governed schema version {version}; use apply_schema_migration instead of legacy ALTER/DROP DDL"
            ));
        }
        Ok(())
    }

    /// Apply one forward-only schema migration as one redb transaction.  The
    /// schema/catalog, row coercions, dependency checks, migration record, and
    /// version CAS all commit together or none of them commit.
    pub fn apply_schema_migration(
        &self,
        migration: &SchemaMigration,
    ) -> Result<SchemaMigrationApply, String> {
        let wtx = self.begin()?;
        let result = apply_schema_migration_in(&wtx, self.scope.as_ref(), migration)?;
        wtx.commit().map_err(map_err)?;
        Ok(result)
    }

    /// Return the durable ordered migration chain for `table`, oldest first.
    /// The records are immutable; callers can use the checksums as a compact
    /// audit/provenance proof without retrieving row data.
    pub fn schema_migrations(&self, table: &str) -> Result<Vec<SchemaMigrationRecord>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let order = match rtx.open_table(SCHEMA_MIGRATION_ORDER) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(map_err(error)),
        };
        let records = match rtx.open_table(SCHEMA_MIGRATIONS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(map_err(error)),
        };
        let mut out = Vec::new();
        for row in order
            .range((self.scope.as_ref(), table, 0u64)..=(self.scope.as_ref(), table, u64::MAX))
            .map_err(map_err)?
        {
            let (_, id) = row.map_err(map_err)?;
            let id = id.value();
            let bytes = records
                .get((self.scope.as_ref(), table, id))
                .map_err(map_err)?
                .ok_or_else(|| {
                    format!("schema migration order points to missing record `{table}/{id}`")
                })?;
            out.push(decode_stored::<SchemaMigrationRecord>(
                bytes.value(),
                "schema migration record",
            )?);
        }
        Ok(out)
    }

    /// Verify the complete schema migration chain after opening a database.
    /// Missing tables are valid for old stores, but an existing chain must have
    /// contiguous versions, matching checksums, matching tenant/table bindings,
    /// and a final digest equal to the catalog schema.  This is intentionally
    /// fail-closed: a corrupt or partially copied migration catalog prevents a
    /// store from serving stale schema state.
    pub fn verify_schema_migrations(&self) -> Result<(), String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let Some(tables) = self.open_schema_migration_tables(&rtx)? else {
            return Ok(());
        };
        let catalog_records = self.build_schema_migration_record_map(&tables.records)?;
        self.verify_schema_catalog_order_chain(
            &tables.catalog_versions,
            &tables.catalog_order,
            &catalog_records,
        )?;
        self.verify_schema_version_chains(
            &tables.versions,
            &tables.catalog,
            &tables.order,
            &tables.records,
        )?;
        Ok(())
    }

    /// Opens every table `verify_schema_migrations` needs, or `None` when an
    /// early "nothing to verify" fallback in one of the two grouped opens
    /// (version-chain tables, then catalog-chain tables) already resolved
    /// the whole check. Split from `verify_schema_migrations` itself purely
    /// to keep that caller's own CCN low; all the actual fallback/corruption
    /// logic lives in `open_schema_version_tables`/`open_schema_catalog_tables`
    /// and the six single-table openers below them.
    fn open_schema_migration_tables(
        &self,
        rtx: &ReadTransaction,
    ) -> Result<Option<SchemaMigrationTables>, String> {
        let Some((versions, order, records)) = self.open_schema_version_tables(rtx)? else {
            return Ok(None);
        };
        let Some((catalog_versions, catalog_order, catalog)) =
            self.open_schema_catalog_tables(rtx, &versions)?
        else {
            return Ok(None);
        };
        Ok(Some(SchemaMigrationTables {
            versions,
            order,
            records,
            catalog_versions,
            catalog_order,
            catalog,
        }))
    }

    /// Opens `SCHEMA_VERSIONS`/`SCHEMA_MIGRATION_ORDER`/`SCHEMA_MIGRATIONS`
    /// together, short-circuiting to `None` the moment any one of them
    /// reports "nothing to verify" (each opener's own `None` already means
    /// the whole chain is trivially valid, see their doc comments).
    fn open_schema_version_tables(
        &self,
        rtx: &ReadTransaction,
    ) -> Result<Option<(SchemaVersionsTable, SchemaOrderTable, SchemaRecordsTable)>, String> {
        let Some(versions) = self.open_schema_versions_or_ok(rtx)? else {
            return Ok(None);
        };
        let Some(order) = self.open_schema_migration_order_or_ok(rtx, &versions)? else {
            return Ok(None);
        };
        let Some(records) = self.open_schema_migration_records_or_ok(rtx, &versions)? else {
            return Ok(None);
        };
        Ok(Some((versions, order, records)))
    }

    /// Opens `SCHEMA_CATALOG_VERSIONS`/`SCHEMA_CATALOG_ORDER`/`CATALOG`
    /// together, mirroring `open_schema_version_tables`.
    fn open_schema_catalog_tables(
        &self,
        rtx: &ReadTransaction,
        versions: &SchemaVersionsTable,
    ) -> Result<
        Option<(
            SchemaCatalogVersionsTable,
            SchemaCatalogOrderTable,
            SchemaCatalogTable,
        )>,
        String,
    > {
        let Some(catalog_versions) = self.open_schema_catalog_versions_or_ok(rtx, versions)? else {
            return Ok(None);
        };
        let Some(catalog_order) = self.open_schema_catalog_order_or_ok(rtx, &catalog_versions)?
        else {
            return Ok(None);
        };
        let Some(catalog) = self.open_schema_catalog_or_ok(rtx)? else {
            return Ok(None);
        };
        Ok(Some((catalog_versions, catalog_order, catalog)))
    }

    /// Opens `SCHEMA_VERSIONS`. Missing is valid for an old store UNLESS an
    /// orphaned migration-order or migration-records catalog exists without
    /// it, which is corruption. `Ok(None)` signals the caller to return
    /// `Ok(())` immediately (nothing further to check).
    fn open_schema_versions_or_ok(
        &self,
        rtx: &ReadTransaction,
    ) -> Result<Option<SchemaVersionsTable>, String> {
        match rtx.open_table(SCHEMA_VERSIONS) {
            Ok(table) => Ok(Some(table)),
            Err(redb::TableError::TableDoesNotExist(_)) => {
                let orphan_order = !matches!(
                    rtx.open_table(SCHEMA_MIGRATION_ORDER),
                    Err(redb::TableError::TableDoesNotExist(_))
                );
                let orphan_records = !matches!(
                    rtx.open_table(SCHEMA_MIGRATIONS),
                    Err(redb::TableError::TableDoesNotExist(_))
                );
                if orphan_order || orphan_records {
                    return Err(
                        "schema migration catalogs exist without the authoritative version catalog"
                            .to_string(),
                    );
                }
                Ok(None)
            }
            Err(error) => Err(map_err(error)),
        }
    }

    /// Opens `SCHEMA_MIGRATION_ORDER`. Missing is valid only when every
    /// tracked version in `versions` is still zero (a store that has never
    /// migrated anything).
    fn open_schema_migration_order_or_ok(
        &self,
        rtx: &ReadTransaction,
        versions: &SchemaVersionsTable,
    ) -> Result<Option<SchemaOrderTable>, String> {
        match rtx.open_table(SCHEMA_MIGRATION_ORDER) {
            Ok(table) => Ok(Some(table)),
            Err(redb::TableError::TableDoesNotExist(_)) => {
                for row in versions.iter().map_err(map_err)? {
                    let (key, value) = row.map_err(map_err)?;
                    if value.value() != 0 {
                        return Err(format!(
                            "schema version `{}/{}' has no migration order catalog",
                            key.value().0,
                            key.value().1
                        ));
                    }
                }
                Ok(None)
            }
            Err(error) => Err(map_err(error)),
        }
    }

    /// Opens `SCHEMA_MIGRATIONS`, mirroring `open_schema_migration_order_or_ok`.
    fn open_schema_migration_records_or_ok(
        &self,
        rtx: &ReadTransaction,
        versions: &SchemaVersionsTable,
    ) -> Result<Option<SchemaRecordsTable>, String> {
        match rtx.open_table(SCHEMA_MIGRATIONS) {
            Ok(table) => Ok(Some(table)),
            Err(redb::TableError::TableDoesNotExist(_)) => {
                for row in versions.iter().map_err(map_err)? {
                    let (key, value) = row.map_err(map_err)?;
                    if value.value() != 0 {
                        return Err(format!(
                            "schema version `{}/{}' has no migration record catalog",
                            key.value().0,
                            key.value().1
                        ));
                    }
                }
                Ok(None)
            }
            Err(error) => Err(map_err(error)),
        }
    }

    /// Opens `SCHEMA_CATALOG_VERSIONS`. Missing is valid only when no
    /// migration versions have been recorded at all.
    fn open_schema_catalog_versions_or_ok(
        &self,
        rtx: &ReadTransaction,
        versions: &SchemaVersionsTable,
    ) -> Result<Option<SchemaCatalogVersionsTable>, String> {
        match rtx.open_table(SCHEMA_CATALOG_VERSIONS) {
            Ok(table) => Ok(Some(table)),
            Err(redb::TableError::TableDoesNotExist(_)) => {
                for row in versions.iter().map_err(map_err)? {
                    let (_, value) = row.map_err(map_err)?;
                    if value.value() != 0 {
                        return Err(
                            "schema migration records exist without a catalog version counter"
                                .to_string(),
                        );
                    }
                }
                Ok(None)
            }
            Err(error) => Err(map_err(error)),
        }
    }

    /// Opens `SCHEMA_CATALOG_ORDER`. Missing is valid only when every scope's
    /// catalog version in `catalog_versions` is still zero.
    fn open_schema_catalog_order_or_ok(
        &self,
        rtx: &ReadTransaction,
        catalog_versions: &SchemaCatalogVersionsTable,
    ) -> Result<Option<SchemaCatalogOrderTable>, String> {
        match rtx.open_table(SCHEMA_CATALOG_ORDER) {
            Ok(table) => Ok(Some(table)),
            Err(redb::TableError::TableDoesNotExist(_)) => {
                for row in catalog_versions.iter().map_err(map_err)? {
                    let (scope, value) = row.map_err(map_err)?;
                    if value.value() != 0 {
                        return Err(format!(
                            "schema catalog scope `{}` has no catalog order chain",
                            scope.value()
                        ));
                    }
                }
                Ok(None)
            }
            Err(error) => Err(map_err(error)),
        }
    }

    /// Opens the user-table `CATALOG`. Missing means no user tables exist at
    /// all, so there is trivially nothing to verify.
    fn open_schema_catalog_or_ok(
        &self,
        rtx: &ReadTransaction,
    ) -> Result<Option<SchemaCatalogTable>, String> {
        match rtx.open_table(CATALOG) {
            Ok(table) => Ok(Some(table)),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(error) => Err(map_err(error)),
        }
    }

    /// Validates every migration record's scope binding, identity, and
    /// digest chain (CONCEPT unchanged), returning the `(scope, table,
    /// migration_id) -> catalog_version` map the two verification passes
    /// below need. The transient duplicate-catalog-version detector
    /// (`catalog_identities` in the pre-extraction code) stays local: it is
    /// consulted nowhere after this loop, exactly as before.
    fn build_schema_migration_record_map(
        &self,
        records: &SchemaRecordsTable,
    ) -> Result<HashMap<(String, String, String), u64>, String> {
        let mut catalog_records: HashMap<(String, String, String), u64> = HashMap::new();
        let mut catalog_identities: HashMap<(String, u64), (String, String)> = HashMap::new();
        for row in records.iter().map_err(map_err)? {
            let (key, value) = row.map_err(map_err)?;
            let (scope, table, migration_id) = key.value();
            if scope != self.scope.as_ref() {
                return Err(format!(
                    "schema migration record `{scope}/{table}/{migration_id}` is bound to another scope"
                ));
            }
            let record: SchemaMigrationRecord =
                decode_stored(value.value(), "schema migration record")?;
            verify_migration_record(
                &record,
                scope,
                table,
                record.migration.target_schema_version,
            )?;
            if record.migration.migration_id != migration_id {
                return Err(format!(
                    "schema migration record key `{migration_id}` does not match its immutable identity"
                ));
            }
            let record_key = (
                scope.to_string(),
                table.to_string(),
                migration_id.to_string(),
            );
            if catalog_records
                .insert(record_key.clone(), record.catalog_version)
                .is_some()
            {
                return Err(format!(
                    "duplicate schema migration record identity `{scope}/{table}/{migration_id}`"
                ));
            }
            if catalog_identities
                .insert(
                    (scope.to_string(), record.catalog_version),
                    (table.to_string(), migration_id.to_string()),
                )
                .is_some()
            {
                return Err(format!(
                    "duplicate schema catalog version {} in scope `{scope}`",
                    record.catalog_version
                ));
            }
        }
        Ok(catalog_records)
    }

    /// Validates that every scope's catalog-version chain in
    /// `catalog_order`/`catalog_versions` is contiguous from 1 and each
    /// entry resolves back to a record in `catalog_records`.
    fn verify_schema_catalog_order_chain(
        &self,
        catalog_versions: &SchemaCatalogVersionsTable,
        catalog_order: &SchemaCatalogOrderTable,
        catalog_records: &HashMap<(String, String, String), u64>,
    ) -> Result<(), String> {
        for row in catalog_versions.iter().map_err(map_err)? {
            let (scope, version) = row.map_err(map_err)?;
            if scope.value() != self.scope.as_ref() {
                return Err(format!(
                    "schema catalog version is bound to scope `{}`, not `{}`",
                    scope.value(),
                    self.scope
                ));
            }
            let current_catalog_version = version.value();
            if current_catalog_version == 0
                && catalog_records
                    .keys()
                    .any(|(record_scope, _, _)| record_scope == scope.value())
            {
                return Err(format!(
                    "schema catalog scope `{}` has records but remains at version zero",
                    scope.value()
                ));
            }
            Self::verify_schema_catalog_order_chain_for_scope(
                scope.value(),
                current_catalog_version,
                catalog_order,
                catalog_records,
            )?;
        }
        Ok(())
    }

    /// The inner per-scope walk of `verify_schema_catalog_order_chain`:
    /// every catalog version from 1 to `current_catalog_version` must chain
    /// to a real, matching migration record.
    fn verify_schema_catalog_order_chain_for_scope(
        scope: &str,
        current_catalog_version: u64,
        catalog_order: &SchemaCatalogOrderTable,
        catalog_records: &HashMap<(String, String, String), u64>,
    ) -> Result<(), String> {
        for expected_catalog_version in 1..=current_catalog_version {
            let identity = catalog_order
                .get((scope, expected_catalog_version))
                .map_err(map_err)?
                .ok_or_else(|| {
                    format!("schema catalog version chain has a gap at {expected_catalog_version}")
                })?;
            let (table, migration_id) = identity
                .value()
                .split_once('\0')
                .ok_or_else(|| "schema catalog order contains an invalid identity".to_string())?;
            let record_key = (
                scope.to_string(),
                table.to_string(),
                migration_id.to_string(),
            );
            if catalog_records.get(&record_key) != Some(&expected_catalog_version) {
                return Err(format!(
                    "schema catalog order points to missing or mismatched migration `{table}/{migration_id}`"
                ));
            }
        }
        Ok(())
    }

    /// Validates every table's per-column schema-version chain: contiguous
    /// versions walking backward from `current_version`, each migration's
    /// digest linking to the next, and the chain terminating at the live
    /// catalog schema's digest.
    fn verify_schema_version_chains(
        &self,
        versions: &SchemaVersionsTable,
        catalog: &SchemaCatalogTable,
        order: &SchemaOrderTable,
        records: &SchemaRecordsTable,
    ) -> Result<(), String> {
        for row in versions.iter().map_err(map_err)? {
            let (key, version) = row.map_err(map_err)?;
            let (scope, table) = key.value();
            if scope != self.scope.as_ref() {
                return Err(format!(
                    "schema version catalog is bound to scope `{scope}`, not `{}`",
                    self.scope
                ));
            }
            Self::verify_schema_version_chain_for_table(
                scope,
                table,
                version.value(),
                catalog,
                order,
                records,
            )?;
        }
        Ok(())
    }

    /// The inner per-`(scope, table)` check of `verify_schema_version_chains`:
    /// resolve the live catalog schema's starting digest, walk the migration
    /// chain backward against it, then confirm the chain begins at version 0.
    fn verify_schema_version_chain_for_table(
        scope: &str,
        table: &str,
        current_version: u64,
        catalog: &SchemaCatalogTable,
        order: &SchemaOrderTable,
        records: &SchemaRecordsTable,
    ) -> Result<(), String> {
        let schema_bytes = catalog
            .get(table)
            .map_err(map_err)?
            .ok_or_else(|| format!("schema version references missing table `{table}`"))?;
        let schema = decode_stored::<TableSchema>(schema_bytes.value(), "schema")?;
        schema.validate()?;
        let previous_digest = schema.schema_digest()?;
        Self::verify_schema_migration_digest_chain(
            scope,
            table,
            current_version,
            previous_digest,
            order,
            records,
        )?;
        Self::verify_schema_chain_starts_at_zero(scope, table, current_version, order, records)
    }

    /// Walks the migration chain for `(scope, table)` backward from
    /// `current_version` to 1, confirming versions are contiguous and each
    /// migration's target digest links to the previous step (ending at the
    /// live catalog schema's digest, passed in as the initial
    /// `previous_digest`).
    fn verify_schema_migration_digest_chain(
        scope: &str,
        table: &str,
        current_version: u64,
        mut previous_digest: String,
        order: &SchemaOrderTable,
        records: &SchemaRecordsTable,
    ) -> Result<(), String> {
        for expected_version in (1..=current_version).rev() {
            previous_digest = Self::verify_one_schema_migration_step(
                scope,
                table,
                expected_version,
                previous_digest,
                order,
                records,
            )?;
        }
        Ok(())
    }

    /// One step of `verify_schema_migration_digest_chain`: resolve the
    /// migration recorded at `expected_version`, confirm its identity, and
    /// confirm its target digest links to `previous_digest`. Returns the
    /// next `previous_digest` for the walk to continue with.
    fn verify_one_schema_migration_step(
        scope: &str,
        table: &str,
        expected_version: u64,
        previous_digest: String,
        order: &SchemaOrderTable,
        records: &SchemaRecordsTable,
    ) -> Result<String, String> {
        let record =
            Self::resolve_schema_migration_record(scope, table, expected_version, order, records)?;
        if record.migration.target_schema_digest != previous_digest {
            return Err(format!(
                "schema migration chain for `{table}` does not terminate at the catalog digest"
            ));
        }
        Ok(record.migration.expected_schema_digest.clone())
    }

    /// Resolves and identity-checks the migration recorded for `(scope,
    /// table, expected_version)`: the order chain must point to a record
    /// that exists and whose own migration_id matches.
    fn resolve_schema_migration_record(
        scope: &str,
        table: &str,
        expected_version: u64,
        order: &SchemaOrderTable,
        records: &SchemaRecordsTable,
    ) -> Result<SchemaMigrationRecord, String> {
        let migration_id = order
            .get((scope, table, expected_version))
            .map_err(map_err)?
            .ok_or_else(|| {
                format!(
                    "schema migration chain for `{table}` has a version gap at {expected_version}"
                )
            })?;
        let migration_id = migration_id.value();
        let bytes = records
            .get((scope, table, migration_id))
            .map_err(map_err)?
            .ok_or_else(|| {
                format!("schema migration order for `{table}` points to missing `{migration_id}`")
            })?;
        let record: SchemaMigrationRecord =
            decode_stored(bytes.value(), "schema migration record")?;
        verify_migration_record(&record, scope, table, expected_version)?;
        if record.migration.migration_id != migration_id {
            return Err(format!(
                "schema migration order for `{table}` maps version {expected_version} to a different record identity"
            ));
        }
        Ok(record)
    }

    /// The `current_version == 0` case is trivially valid (no chain to
    /// check); otherwise the first migration in the chain must start at
    /// schema version 0.
    fn verify_schema_chain_starts_at_zero(
        scope: &str,
        table: &str,
        current_version: u64,
        order: &SchemaOrderTable,
        records: &SchemaRecordsTable,
    ) -> Result<(), String> {
        if current_version == 0 {
            return Ok(());
        }
        let first = order
            .get((scope, table, 1u64))
            .map_err(map_err)?
            .ok_or_else(|| format!("schema migration chain for `{table}` starts with a gap"))?;
        let record = records
            .get((scope, table, first.value()))
            .map_err(map_err)?
            .ok_or_else(|| format!("schema migration chain for `{table}` has no first record"))?;
        let first: SchemaMigrationRecord =
            decode_stored(record.value(), "schema migration record")?;
        if first.migration.expected_schema_version != 0 {
            return Err(format!(
                "schema migration chain for `{table}` does not begin at version zero"
            ));
        }
        Ok(())
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
        let created = create_secondary_index_in(&wtx, self.index_scope(), spec, if_not_exists)?;
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
        verify_batch_is_sql_catalog_only(batch)?;
        let wtx = self.begin()?;

        // Capture the authoritative version once for both the OCC gate and the
        // idempotency replay gate.  A retry reconstructed after an ack-loss may
        // carry this current observation rather than the original version stored
        // in its durable batch record; the record itself remains authoritative.
        // Idempotency check and insertion share this write transaction, closing the
        // concurrent double-execution race.
        let version_key = (batch.tenant.as_str(), batch.graph.as_str());
        let (current_version, proposed_fence) =
            match prepare_mutation_commit_in(&wtx, version_key, batch)? {
                MutationCommitPrelude::Replay(replay) => return Ok(*replay),
                MutationCommitPrelude::Fresh {
                    current_version,
                    proposed_fence,
                } => (current_version, proposed_fence),
            };

        let affected = apply_mutation_txn_ops_with_crashpoints(
            &wtx,
            self.index_scope(),
            txn,
            batch,
            crashpoint,
        )?;
        let (record, record_bytes, next_version) = finalize_mutation_commit_metadata(
            batch,
            committed_at_ms,
            result_override,
            affected,
            current_version,
        )?;
        write_mutation_commit_tables_in(
            &wtx,
            batch,
            version_key,
            next_version,
            &proposed_fence,
            &record_bytes,
        )?;

        commit_mutation_txn_with_crashpoints(wtx, batch, crashpoint)?;
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

    /// A single fingerprint over EVERY durable SQL-domain OCC counter and
    /// schema/catalog version this store
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
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match rtx.open_table(MUTATION_VERSION) {
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(map_err(error)),
            Ok(table) => {
                for row in table.iter().map_err(map_err)? {
                    let (key, value) = row.map_err(map_err)?;
                    let (tenant, scope) = key.value();
                    b"mutation".hash(&mut hasher);
                    tenant.hash(&mut hasher);
                    scope.hash(&mut hasher);
                    value.value().hash(&mut hasher);
                }
            }
        }
        match rtx.open_table(SCHEMA_CATALOG_VERSIONS) {
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(map_err(error)),
            Ok(table) => {
                for row in table.iter().map_err(map_err)? {
                    let (scope, value) = row.map_err(map_err)?;
                    b"schema".hash(&mut hasher);
                    scope.value().hash(&mut hasher);
                    value.value().hash(&mut hasher);
                }
            }
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

// ── commit_txn_batch_inner phase helpers ───────────────────────────────────
// Purely mechanical extraction of commit_txn_batch_inner's sequential
// phases -- validate/OCC/idempotency/fence, apply ops (with crash-injection
// points, CONCEPT: chaos/durability certification), build + write the
// durable MutationBatch record, commit (with crash-injection points). No
// behaviour change: every helper's body is the original code verbatim.

/// Result of the read-only "is this a replay, or should we proceed" prelude
/// (`prepare_mutation_commit_in`): either the durably-committed replay
/// result to return as-is, or the current version + verified fence a fresh
/// commit proceeds with.
enum MutationCommitPrelude {
    /// Boxed: a `MutationBatchCommit` carries the whole durable
    /// `MutationBatchRecord`, hundreds of bytes larger than `Fresh`, and this
    /// enum is returned by value on the hot fresh-commit path.
    Replay(Box<MutationBatchCommit>),
    Fresh {
        current_version: u64,
        proposed_fence: SqlMutationFence,
    },
}

/// The read-then-validate phase of `commit_txn_batch_inner`, before any
/// write: resolve the current OCC version, check for an idempotent replay
/// (short-circuiting the caller), and -- for a genuinely new batch -- the
/// batch-id-not-taken / OCC-version / fence checks. Bundled into one enum
/// return purely to keep `commit_txn_batch_inner`'s own CCN low.
fn prepare_mutation_commit_in(
    wtx: &WriteTransaction,
    version_key: (&str, &str),
    batch: &MutationBatch,
) -> Result<MutationCommitPrelude, String> {
    let current_version = read_current_mutation_version_in(wtx, version_key)?;
    if let Some(replay) = check_mutation_idempotency_replay_in(wtx, batch, current_version)? {
        return Ok(MutationCommitPrelude::Replay(Box::new(replay)));
    }
    check_mutation_batch_id_not_exists_in(wtx, batch)?;
    verify_mutation_occ_version(batch, current_version)?;
    let proposed_fence = resolve_and_verify_mutation_fence_in(wtx, version_key, batch)?;
    Ok(MutationCommitPrelude::Fresh {
        current_version,
        proposed_fence,
    })
}

/// Builds the durable `MutationBatchRecord`, its encoded bytes, and the
/// advanced OCC version -- the three pieces of write-side metadata
/// `commit_txn_batch_inner` needs after applying the txn ops, bundled
/// purely to keep its own CCN low.
fn finalize_mutation_commit_metadata(
    batch: &MutationBatch,
    committed_at_ms: u64,
    result_override: Option<Vec<u8>>,
    affected: usize,
    current_version: u64,
) -> Result<(MutationBatchRecord, Vec<u8>, u64), String> {
    let record = build_mutation_batch_record(batch, committed_at_ms, result_override, affected)?;
    let record_bytes = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
    let next_version = current_version
        .checked_add(1)
        .ok_or_else(|| "SQL mutation domain version overflow".to_string())?;
    Ok((record, record_bytes, next_version))
}

fn verify_batch_is_sql_catalog_only(batch: &MutationBatch) -> Result<(), String> {
    if batch
        .operations
        .iter()
        .any(|operation| operation.domain != MutationDomain::SqlCatalog)
    {
        return Err("SQL MutationBatch contains a non-SqlCatalog operation".to_string());
    }
    Ok(())
}

fn read_current_mutation_version_in(
    wtx: &WriteTransaction,
    version_key: (&str, &str),
) -> Result<u64, String> {
    let versions = wtx.open_table(MUTATION_VERSION).map_err(map_err)?;
    // Bind the guard: as a tail expression its temporary would outlive
    // `versions` and borrow a dropped table handle.
    let found = versions.get(version_key).map_err(map_err)?;
    Ok(match found {
        Some(value) => value.value(),
        None => INITIAL_SQL_DOMAIN_VERSION,
    })
}

/// `Some(commit)` when `batch.idempotency_key` was already committed and the
/// resubmitted batch's identity matches (a safe idempotent replay); `None`
/// for a genuinely new batch. Errs on IDEMPOTENCY_CONFLICT (same key,
/// different identity).
fn check_mutation_idempotency_replay_in(
    wtx: &WriteTransaction,
    batch: &MutationBatch,
    current_version: u64,
) -> Result<Option<MutationBatchCommit>, String> {
    let idem = wtx.open_table(MUTATION_IDEMPOTENCY).map_err(map_err)?;
    let existing = idem
        .get((
            batch.tenant.as_str(),
            batch.graph.as_str(),
            batch.idempotency_key.as_str(),
        ))
        .map_err(map_err)?
        .map(|value| value.value().to_string());
    let Some(existing) = existing else {
        return Ok(None);
    };
    let records = wtx.open_table(MUTATION_BATCHES).map_err(map_err)?;
    let bytes = records
        .get(existing.as_str())
        .map_err(map_err)?
        .ok_or_else(|| {
            format!("corrupt SQL mutation idempotency index: '{existing}' has no batch record")
        })?
        .value()
        .to_vec();
    let record = decode_mutation_record(&bytes)?;
    if !same_batch_identity(&record.batch, batch, current_version)? {
        return Err(format!(
            "IDEMPOTENCY_CONFLICT: SQL key '{}' is already committed as batch '{}'",
            batch.idempotency_key, record.batch.batch_id
        ));
    }
    Ok(Some(MutationBatchCommit {
        record,
        replayed: true,
    }))
}

fn check_mutation_batch_id_not_exists_in(
    wtx: &WriteTransaction,
    batch: &MutationBatch,
) -> Result<(), String> {
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
    Ok(())
}

fn verify_mutation_occ_version(batch: &MutationBatch, current_version: u64) -> Result<(), String> {
    let expected = batch.expected_graph_version.ok_or_else(|| {
        "authoritative SQL MutationBatch requires expected_graph_version".to_string()
    })?;
    if expected != current_version {
        return Err(format!(
            "STALE_VERSION: SQL scope '{}/{}' expected {} but authoritative version is {}",
            batch.tenant, batch.graph, expected, current_version
        ));
    }
    Ok(())
}

/// Reads the current fence and confirms `batch`'s proposed fence has not
/// been superseded by a newer placement epoch / fencing token.
fn resolve_and_verify_mutation_fence_in(
    wtx: &WriteTransaction,
    version_key: (&str, &str),
    batch: &MutationBatch,
) -> Result<SqlMutationFence, String> {
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
    Ok(proposed_fence)
}

/// Applies every op in `txn` inside `wtx`, honoring the two crash-injection
/// points either side of the row work (CONCEPT: chaos/durability
/// certification -- production always calls this with `crashpoint: None`).
fn apply_mutation_txn_ops_with_crashpoints(
    wtx: &WriteTransaction,
    index_scope: &str,
    txn: &TableTxn,
    batch: &MutationBatch,
    crashpoint: Option<SqlMutationCrashpoint>,
) -> Result<usize, String> {
    if crashpoint == Some(SqlMutationCrashpoint::BeforeRows) {
        return Err("injected crash before SQL mutation rows".to_string());
    }
    eg_types::mutation_batch::apply_certification_fault(
        batch,
        eg_types::mutation_batch::MutationCommitPhase::BeforeRows,
    )?;

    let mut affected = 0usize;
    for op in &txn.ops {
        affected = affected.saturating_add(apply_txn_op(wtx, index_scope, op)?);
    }

    if crashpoint == Some(SqlMutationCrashpoint::AfterRowsBeforeMetadata) {
        return Err("injected crash after SQL mutation rows".to_string());
    }
    eg_types::mutation_batch::apply_certification_fault(
        batch,
        eg_types::mutation_batch::MutationCommitPhase::AfterRowsBeforeMetadata,
    )?;
    Ok(affected)
}

fn build_mutation_batch_record(
    batch: &MutationBatch,
    committed_at_ms: u64,
    result_override: Option<Vec<u8>>,
    affected: usize,
) -> Result<MutationBatchRecord, String> {
    let result_msgpack = match result_override {
        Some(result) => result,
        None => rmp_serde::to_vec_named(&affected).map_err(|e| e.to_string())?,
    };
    Ok(MutationBatchRecord {
        batch: batch.clone(),
        status: MutationBatchStatus::Committed,
        result_msgpack: Some(result_msgpack),
        committed_at_ms,
    })
}

/// Writes every durable side-effect of a successfully-applied mutation
/// batch: the batch record itself, the idempotency index entry, the
/// advanced OCC version, the fence, and the outbox intents (one per
/// operation, plus any explicit `batch.outbox` entries).
fn write_mutation_commit_tables_in(
    wtx: &WriteTransaction,
    batch: &MutationBatch,
    version_key: (&str, &str),
    next_version: u64,
    proposed_fence: &SqlMutationFence,
    record_bytes: &[u8],
) -> Result<(), String> {
    let mut records = wtx.open_table(MUTATION_BATCHES).map_err(map_err)?;
    records
        .insert(batch.batch_id.as_str(), record_bytes)
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
    let fence_bytes = rmp_serde::to_vec_named(proposed_fence).map_err(|e| e.to_string())?;
    let mut fences = wtx.open_table(MUTATION_FENCE).map_err(map_err)?;
    fences
        .insert(version_key, fence_bytes.as_slice())
        .map_err(map_err)?;
    append_mutation_outbox_intents_in(wtx, batch)
}

fn append_mutation_outbox_intents_in(
    wtx: &WriteTransaction,
    batch: &MutationBatch,
) -> Result<(), String> {
    let mut outbox = wtx.open_table(MUTATION_OUTBOX).map_err(map_err)?;
    let ordinal = append_operation_outbox_intents_in(&mut outbox, batch, 0)?;
    append_explicit_outbox_intents_in(&mut outbox, batch, ordinal)
}

fn append_operation_outbox_intents_in(
    outbox: &mut redb::Table<(&str, u32), &[u8]>,
    batch: &MutationBatch,
    mut ordinal: u32,
) -> Result<u32, String> {
    for operation in &batch.operations {
        let intent = MutationOutboxIntent {
            topic: "engine.mutation.committed".to_string(),
            key: batch.batch_id.clone(),
            payload: rmp_serde::to_vec_named(operation).map_err(|e| e.to_string())?,
            headers: Default::default(),
        };
        insert_sql_outbox(outbox, batch, ordinal, intent)?;
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| "SQL mutation outbox ordinal overflow".to_string())?;
    }
    Ok(ordinal)
}

fn append_explicit_outbox_intents_in(
    outbox: &mut redb::Table<(&str, u32), &[u8]>,
    batch: &MutationBatch,
    mut ordinal: u32,
) -> Result<(), String> {
    for intent in &batch.outbox {
        insert_sql_outbox(outbox, batch, ordinal, intent.clone())?;
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| "SQL mutation outbox ordinal overflow".to_string())?;
    }
    Ok(())
}

/// Commits `wtx`, honoring the two crash-injection points either side of
/// the actual redb commit (CONCEPT: chaos/durability certification).
fn commit_mutation_txn_with_crashpoints(
    wtx: WriteTransaction,
    batch: &MutationBatch,
    crashpoint: Option<SqlMutationCrashpoint>,
) -> Result<(), String> {
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
    )
}

// ── txn-scoped helpers (operate on an OPEN WriteTransaction) ──────────────────
//
// These are the single source of truth for every mutation. A one-shot public method
// wraps one in its own begin/commit; `commit_txn` chains several in ONE txn. Reading
// the catalog/rows THROUGH the same `wtx` (read-your-writes) is what lets a later op
// in a multi-statement transaction see an earlier op's staged writes.

fn apply_txn_op(wtx: &WriteTransaction, tenant_scope: &str, op: &TxnOp) -> Result<usize, String> {
    match op {
        TxnOp::CreateTable {
            schema,
            if_not_exists,
        } => apply_txn_op_create_table(wtx, schema, *if_not_exists),
        TxnOp::DropTable { name, if_exists } => {
            apply_txn_op_drop_table(wtx, tenant_scope, name, *if_exists)
        }
        TxnOp::AddColumn { table, column } => {
            apply_txn_op_add_column(wtx, tenant_scope, table, column)
        }
        TxnOp::DropColumn {
            table,
            column,
            if_exists,
        } => apply_txn_op_drop_column(wtx, tenant_scope, table, column, *if_exists),
        TxnOp::RenameColumn { table, from, to } => {
            apply_txn_op_rename_column(wtx, tenant_scope, table, from, to)
        }
        TxnOp::RenameTable { table, new_name } => {
            apply_txn_op_rename_table(wtx, tenant_scope, table, new_name)
        }
        TxnOp::AlterColumnType {
            table,
            column,
            new_type,
        } => apply_txn_op_alter_column_type(wtx, tenant_scope, table, column, *new_type),
        TxnOp::DropConstraint {
            table,
            constraint,
            if_exists,
        } => apply_txn_op_drop_constraint(wtx, tenant_scope, table, constraint, *if_exists),
        TxnOp::AddConstraint { table, constraint } => {
            apply_txn_op_add_constraint(wtx, tenant_scope, table, constraint.clone())
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
        TxnOp::Delete { table, selector } => {
            Ok(delete_in(wtx, tenant_scope, table, selector)?.len())
        }
        TxnOp::CreateView {
            name,
            select_sql,
            or_replace,
        } => apply_txn_op_create_view(wtx, name, select_sql, *or_replace),
        TxnOp::DropView { name, if_exists } => apply_txn_op_drop_view(wtx, name, *if_exists),
        TxnOp::CreateExtension {
            name,
            if_not_exists,
        } => apply_txn_op_create_extension(wtx, name, *if_not_exists),
        TxnOp::DropExtension { name, if_exists } => {
            apply_txn_op_drop_extension(wtx, name, *if_exists)
        }
        TxnOp::CreateFunction {
            function,
            or_replace,
        } => apply_txn_op_create_function(wtx, function, *or_replace),
        TxnOp::DropFunction { name, if_exists } => {
            apply_txn_op_drop_function(wtx, name, *if_exists)
        }
        TxnOp::PutAnnIndex { plan } => apply_txn_op_put_ann_index(wtx, plan),
        TxnOp::PutHypertable { plan } => apply_txn_op_put_hypertable(wtx, plan),
        TxnOp::DropAnnIndexesForColumn { table, column } => {
            drop_ann_indexes_for_column_in(wtx, table, column)
        }
    }
}

// ── apply_txn_op per-variant bodies ────────────────────────────────────────
// One tiny helper per `TxnOp` variant, purely so `apply_txn_op`'s own match
// arms are single tail-call expressions with no `?` of their own (a `match`
// with N arms costs lizard ~1 regardless of arm count; each `?` operator
// costs ~1 -- the 30 CCN this function had came entirely from ~27 `?` calls
// spread across the 19 arms, not from the match itself). No behaviour
// change: every helper's body is the original arm's body verbatim.

fn apply_txn_op_create_table(
    wtx: &WriteTransaction,
    schema: &TableSchema,
    if_not_exists: bool,
) -> Result<usize, String> {
    create_in(wtx, schema, if_not_exists)?;
    Ok(0)
}

fn apply_txn_op_drop_table(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    name: &str,
    if_exists: bool,
) -> Result<usize, String> {
    ensure_legacy_schema_ddl_allowed_in(wtx, tenant_scope, name)?;
    drop_in(wtx, tenant_scope, name, if_exists)?;
    Ok(0)
}

fn apply_txn_op_add_column(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    column: &Column,
) -> Result<usize, String> {
    ensure_legacy_schema_ddl_allowed_in(wtx, tenant_scope, table)?;
    add_column_in(wtx, tenant_scope, table, column)?;
    Ok(0)
}

fn apply_txn_op_drop_column(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    column: &str,
    if_exists: bool,
) -> Result<usize, String> {
    ensure_legacy_schema_ddl_allowed_in(wtx, tenant_scope, table)?;
    drop_column_in(wtx, tenant_scope, table, column, if_exists)?;
    Ok(0)
}

fn apply_txn_op_rename_column(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    from: &str,
    to: &str,
) -> Result<usize, String> {
    ensure_legacy_schema_ddl_allowed_in(wtx, tenant_scope, table)?;
    rename_column_in(wtx, tenant_scope, table, from, to)?;
    Ok(0)
}

fn apply_txn_op_rename_table(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    new_name: &str,
) -> Result<usize, String> {
    ensure_legacy_schema_ddl_allowed_in(wtx, tenant_scope, table)?;
    rename_table_in(wtx, tenant_scope, table, new_name)?;
    Ok(0)
}

fn apply_txn_op_alter_column_type(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    column: &str,
    new_type: ColumnType,
) -> Result<usize, String> {
    ensure_legacy_schema_ddl_allowed_in(wtx, tenant_scope, table)?;
    alter_column_type_in(wtx, tenant_scope, table, column, new_type)?;
    Ok(0)
}

fn apply_txn_op_drop_constraint(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    constraint: &str,
    if_exists: bool,
) -> Result<usize, String> {
    ensure_legacy_schema_ddl_allowed_in(wtx, tenant_scope, table)?;
    drop_constraint_in(wtx, tenant_scope, table, constraint, if_exists)?;
    Ok(0)
}

fn apply_txn_op_add_constraint(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    constraint: TableConstraint,
) -> Result<usize, String> {
    ensure_legacy_schema_ddl_allowed_in(wtx, tenant_scope, table)?;
    add_constraint_in(wtx, tenant_scope, table, constraint)?;
    Ok(0)
}

fn apply_txn_op_create_view(
    wtx: &WriteTransaction,
    name: &str,
    select_sql: &str,
    or_replace: bool,
) -> Result<usize, String> {
    create_view_in(wtx, name, select_sql, or_replace)?;
    Ok(0)
}

fn apply_txn_op_drop_view(
    wtx: &WriteTransaction,
    name: &str,
    if_exists: bool,
) -> Result<usize, String> {
    drop_view_in(wtx, name, if_exists)?;
    Ok(0)
}

fn apply_txn_op_create_extension(
    wtx: &WriteTransaction,
    name: &str,
    if_not_exists: bool,
) -> Result<usize, String> {
    create_extension_in(wtx, name, if_not_exists)?;
    Ok(0)
}

fn apply_txn_op_drop_extension(
    wtx: &WriteTransaction,
    name: &str,
    if_exists: bool,
) -> Result<usize, String> {
    drop_extension_in(wtx, name, if_exists)?;
    Ok(0)
}

fn apply_txn_op_create_function(
    wtx: &WriteTransaction,
    function: &StoredFunction,
    or_replace: bool,
) -> Result<usize, String> {
    create_function_in(wtx, function, or_replace)?;
    Ok(0)
}

fn apply_txn_op_drop_function(
    wtx: &WriteTransaction,
    name: &str,
    if_exists: bool,
) -> Result<usize, String> {
    drop_function_in(wtx, name, if_exists)?;
    Ok(0)
}

fn apply_txn_op_put_ann_index(
    wtx: &WriteTransaction,
    plan: &AnnIndexPlan,
) -> Result<usize, String> {
    put_ann_index_in(wtx, plan)?;
    Ok(0)
}

fn apply_txn_op_put_hypertable(
    wtx: &WriteTransaction,
    plan: &HypertablePlan,
) -> Result<usize, String> {
    put_hypertable_in(wtx, plan)?;
    Ok(0)
}

fn same_batch_identity(
    stored: &MutationBatch,
    proposed: &MutationBatch,
    current_version: u64,
) -> Result<bool, String> {
    let stored_ops = rmp_serde::to_vec_named(&stored.operations).map_err(|e| e.to_string())?;
    let proposed_ops = rmp_serde::to_vec_named(&proposed.operations).map_err(|e| e.to_string())?;
    // SQL callers may reconstruct an idempotent retry after the original commit
    // and observe the incremented domain version.  Preserve the original value
    // in the durable record, but accept the current observation only when every
    // other request-identity field remains exact.
    let expected_version_matches = stored.expected_graph_version == proposed.expected_graph_version
        || proposed.expected_graph_version == Some(current_version);
    Ok(same_batch_request_identity(stored, proposed)
        && expected_version_matches
        && same_batch_commit_identity(stored, proposed)
        && stored_ops == proposed_ops)
}

/// The addressing half of a mutation batch's request identity — who/where the
/// batch targets. Every field must match exactly for a retry to be the same call.
fn same_batch_request_identity(stored: &MutationBatch, proposed: &MutationBatch) -> bool {
    stored.batch_id == proposed.batch_id
        && stored.context == proposed.context
        && stored.tenant == proposed.tenant
        && stored.graph == proposed.graph
        && stored.placement_epoch == proposed.placement_epoch
        && stored.idempotency_key == proposed.idempotency_key
}

/// The commit-control half of a mutation batch's request identity — fencing,
/// authoritative state, and outbox intent.
fn same_batch_commit_identity(stored: &MutationBatch, proposed: &MutationBatch) -> bool {
    stored.fencing_token == proposed.fencing_token
        && stored.authoritative_state == proposed.authoritative_state
        && stored.outbox == proposed.outbox
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

/// Read a table's migration version through the current write transaction.  A
/// missing row is the compatibility value for a pre-migration store.
fn schema_version_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
) -> Result<u64, String> {
    let versions = wtx.open_table(SCHEMA_VERSIONS).map_err(map_err)?;
    let found = versions.get((tenant_scope, table)).map_err(map_err)?;
    Ok(found.map(|value| value.value()).unwrap_or(0))
}

fn schema_catalog_version_in(wtx: &WriteTransaction, tenant_scope: &str) -> Result<u64, String> {
    let versions = wtx.open_table(SCHEMA_CATALOG_VERSIONS).map_err(map_err)?;
    let found = versions.get(tenant_scope).map_err(map_err)?;
    Ok(found.map(|value| value.value()).unwrap_or(0))
}

fn catalog_order_identity(table: &str, migration_id: &str) -> String {
    // NUL is excluded from both migration table/id components, so this remains
    // an unambiguous compact value in the redb order table.
    format!("{table}\0{migration_id}")
}

fn ensure_legacy_schema_ddl_allowed_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
) -> Result<(), String> {
    let version = schema_version_in(wtx, tenant_scope, table)?;
    if version > 0 {
        return Err(format!(
            "table `{table}` has governed schema version {version}; use apply_schema_migration instead of legacy ALTER/DROP DDL"
        ));
    }
    Ok(())
}

/// Apply one migration under the same write transaction that records its
/// version/order/identity.  This is the only write path for governed schema
/// transitions; no server handler or SQL parser may partially apply a plan.
fn apply_schema_migration_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    migration: &SchemaMigration,
) -> Result<SchemaMigrationApply, String> {
    validate_migration_identity_in(migration, tenant_scope)?;
    let (current, current_digest, current_version, current_catalog_version) =
        resolve_current_schema_state_in(wtx, tenant_scope, migration)?;

    // The identity lookup comes before all operation work.  A retry after a
    // lost acknowledgement is a no-op only when the immutable bytes and the
    // resulting catalog state match exactly.
    if let Some(replay) = check_migration_replay_in(
        wtx,
        tenant_scope,
        migration,
        current_version,
        &current_digest,
        current_catalog_version,
    )? {
        return Ok(replay);
    }

    verify_migration_occ_state_in(migration, &current, current_version, &current_digest)?;
    let next_catalog_version =
        prepare_migration_projection_in(wtx, migration, &current, current_catalog_version)?;

    // Apply the exact ordered operations through the existing row/catalog
    // helpers.  Any coercion, FK check, or schema error returns before commit,
    // and redb drops the whole write transaction (failure atomicity).
    apply_migration_operations_in(wtx, tenant_scope, migration)?;

    let (resulting_digest, record_bytes) =
        finalize_migration_record_in(wtx, migration, next_catalog_version)?;
    write_migration_commit_in(
        wtx,
        tenant_scope,
        migration,
        &record_bytes,
        next_catalog_version,
    )?;

    Ok(SchemaMigrationApply {
        migration_id: migration.migration_id.clone(),
        schema_version: migration.target_schema_version,
        catalog_version: next_catalog_version,
        schema_digest: resulting_digest,
        replayed: false,
    })
}

/// `migration.validate()` plus the tenant-binding check, split out purely
/// to keep `apply_schema_migration_in`'s own CCN low.
fn validate_migration_identity_in(
    migration: &SchemaMigration,
    tenant_scope: &str,
) -> Result<(), String> {
    migration.validate()?;
    if migration.tenant_scope != tenant_scope {
        return Err(format!(
            "schema migration `{}` tenant binding mismatch: expected `{tenant_scope}`, got `{}`",
            migration.migration_id, migration.tenant_scope
        ));
    }
    Ok(())
}

/// The authoritative current schema/digest/version/catalog-version for
/// `migration.table`, read once so the OCC and replay checks below observe
/// a consistent snapshot.
fn resolve_current_schema_state_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    migration: &SchemaMigration,
) -> Result<(TableSchema, String, u64, u64), String> {
    let current = get_schema_in(wtx, &migration.table)?
        .ok_or_else(|| format!("table `{}` does not exist", migration.table))?;
    let current_digest = current.schema_digest()?;
    let current_version = schema_version_in(wtx, tenant_scope, &migration.table)?;
    let current_catalog_version = schema_catalog_version_in(wtx, tenant_scope)?;
    Ok((
        current,
        current_digest,
        current_version,
        current_catalog_version,
    ))
}

/// `Some(apply)` when `migration.migration_id` was already applied and the
/// current catalog state still matches its target (a safe idempotent
/// replay); `None` when this is a genuinely new migration to apply.
fn check_migration_replay_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    migration: &SchemaMigration,
    current_version: u64,
    current_digest: &str,
    current_catalog_version: u64,
) -> Result<Option<SchemaMigrationApply>, String> {
    let Some(record) = load_existing_migration_record_in(wtx, tenant_scope, migration)? else {
        return Ok(None);
    };
    validate_migration_replay_matches_current_state(
        &record,
        tenant_scope,
        migration,
        current_version,
        current_digest,
        current_catalog_version,
    )?;
    Ok(Some(SchemaMigrationApply {
        migration_id: migration.migration_id.clone(),
        schema_version: current_version,
        catalog_version: record.catalog_version,
        schema_digest: current_digest.to_string(),
        replayed: true,
    }))
}

fn load_existing_migration_record_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    migration: &SchemaMigration,
) -> Result<Option<SchemaMigrationRecord>, String> {
    let records = wtx.open_table(SCHEMA_MIGRATIONS).map_err(map_err)?;
    let found = records
        .get((
            tenant_scope,
            migration.table.as_str(),
            migration.migration_id.as_str(),
        ))
        .map_err(map_err)?;
    found
        .map(|value| {
            decode_stored::<SchemaMigrationRecord>(value.value(), "schema migration record")
        })
        .transpose()
}

fn validate_migration_replay_matches_current_state(
    record: &SchemaMigrationRecord,
    tenant_scope: &str,
    migration: &SchemaMigration,
    current_version: u64,
    current_digest: &str,
    current_catalog_version: u64,
) -> Result<(), String> {
    verify_migration_record(
        record,
        tenant_scope,
        &migration.table,
        migration.target_schema_version,
    )?;
    if record.migration != *migration {
        return Err(format!(
            "schema migration identity `{}` already exists with different immutable bytes",
            migration.migration_id
        ));
    }
    if current_version != migration.target_schema_version
        || current_digest != migration.target_schema_digest
    {
        return Err(format!(
            "schema migration replay `{}` found catalog state that does not match its target",
            migration.migration_id
        ));
    }
    if record.catalog_version > current_catalog_version {
        return Err(format!(
            "schema migration replay `{}` has a catalog version newer than the authoritative catalog",
            migration.migration_id
        ));
    }
    Ok(())
}

/// The OCC (STALE_SCHEMA_VERSION/STALE_SCHEMA_DIGEST) and ordering checks a
/// genuinely-new (non-replay) migration must pass before its operations are
/// applied.
fn verify_migration_occ_state_in(
    migration: &SchemaMigration,
    current: &TableSchema,
    current_version: u64,
    current_digest: &str,
) -> Result<(), String> {
    if migration.expected_schema_version != current_version {
        return Err(format!(
            "STALE_SCHEMA_VERSION: table `{}` expected {} but authoritative version is {}",
            migration.table, migration.expected_schema_version, current_version
        ));
    }
    if migration.expected_schema_digest != current_digest {
        return Err(format!(
            "STALE_SCHEMA_DIGEST: table `{}` expected {} but authoritative digest is {}",
            migration.table, migration.expected_schema_digest, current_digest
        ));
    }
    migration.validate_type_policies(current)?;
    if migration.target_schema_version != current_version.saturating_add(1) {
        return Err(format!(
            "schema migration `{}` is out of order: expected next version {}, got {}",
            migration.migration_id,
            current_version.saturating_add(1),
            migration.target_schema_version
        ));
    }
    Ok(())
}

/// Computes the next catalog version and confirms `migration`'s declared
/// target digest matches what its own operations would actually project,
/// plus the dependency/added-column validation that only makes sense
/// against that projection.
fn prepare_migration_projection_in(
    wtx: &WriteTransaction,
    migration: &SchemaMigration,
    current: &TableSchema,
    current_catalog_version: u64,
) -> Result<u64, String> {
    let next_catalog_version = current_catalog_version
        .checked_add(1)
        .ok_or_else(|| "schema catalog version overflow".to_string())?;
    let projected = migration.projected_schema(current)?;
    if projected.schema_digest()? != migration.target_schema_digest {
        return Err(format!(
            "schema migration `{}` target digest does not match its operation projection",
            migration.migration_id
        ));
    }
    validate_migration_dependencies_in(wtx, current, migration)?;
    validate_added_columns_in(wtx, &migration.table, current, &projected, migration)?;
    Ok(next_catalog_version)
}

fn apply_migration_operations_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    migration: &SchemaMigration,
) -> Result<(), String> {
    for operation in &migration.operations {
        match operation {
            SchemaMigrationOperation::AddColumn { column } => {
                apply_migration_add_column_in(wtx, tenant_scope, &migration.table, column)?;
            }
            SchemaMigrationOperation::DropColumn { column } => {
                drop_column_in(wtx, tenant_scope, &migration.table, column, false)?;
            }
            SchemaMigrationOperation::RenameColumn { from, to } => {
                rename_column_in(wtx, tenant_scope, &migration.table, from, to)?;
            }
            SchemaMigrationOperation::AlterColumnType {
                column, new_type, ..
            } => {
                alter_column_type_in(wtx, tenant_scope, &migration.table, column, *new_type)?;
            }
            SchemaMigrationOperation::AddConstraint { constraint } => {
                add_constraint_in(wtx, tenant_scope, &migration.table, constraint.clone())?;
            }
            SchemaMigrationOperation::DropConstraint { constraint } => {
                drop_constraint_in(wtx, tenant_scope, &migration.table, constraint, false)?;
            }
        }
    }
    Ok(())
}

fn apply_migration_add_column_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    column: &Column,
) -> Result<(), String> {
    let mut column = column.clone();
    if column.primary_key {
        column.nullable = false;
    }
    add_column_in(wtx, tenant_scope, table, &column)
}

/// Confirms the migration's operations produced exactly its declared target
/// digest, and encodes the durable record -- but does not write it (see
/// `write_migration_commit_in`).
fn finalize_migration_record_in(
    wtx: &WriteTransaction,
    migration: &SchemaMigration,
    next_catalog_version: u64,
) -> Result<(String, Vec<u8>), String> {
    let resulting = get_schema_in(wtx, &migration.table)?
        .ok_or_else(|| format!("table `{}` disappeared during migration", migration.table))?;
    let resulting_digest = resulting.schema_digest()?;
    if resulting_digest != migration.target_schema_digest {
        return Err(format!(
            "schema migration `{}` produced digest {}, expected {}",
            migration.migration_id, resulting_digest, migration.target_schema_digest
        ));
    }
    let record = SchemaMigrationRecord {
        migration: migration.clone(),
        state: MigrationState::Applied,
        applied_schema_version: migration.target_schema_version,
        catalog_version: next_catalog_version,
    };
    let bytes = rmp_serde::to_vec_named(&record)
        .map_err(|error| format!("encode schema migration record: {error}"))?;
    Ok((resulting_digest, bytes))
}

/// Writes every durable side-effect of a successfully-applied migration:
/// the new schema version, the new catalog version, this migration's slot
/// in both ordered chains (each guarded against a concurrent claim), and
/// the migration record itself.
fn write_migration_commit_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    migration: &SchemaMigration,
    record_bytes: &[u8],
    next_catalog_version: u64,
) -> Result<(), String> {
    write_schema_and_catalog_version_in(
        wtx,
        tenant_scope,
        &migration.table,
        migration.target_schema_version,
        next_catalog_version,
    )?;
    claim_migration_order_slot_in(
        wtx,
        tenant_scope,
        &migration.table,
        migration.target_schema_version,
        &migration.migration_id,
    )?;
    claim_catalog_order_slot_in(
        wtx,
        tenant_scope,
        &migration.table,
        &migration.migration_id,
        next_catalog_version,
    )?;
    let mut records = wtx.open_table(SCHEMA_MIGRATIONS).map_err(map_err)?;
    records
        .insert(
            (
                tenant_scope,
                migration.table.as_str(),
                migration.migration_id.as_str(),
            ),
            record_bytes,
        )
        .map_err(map_err)?;
    Ok(())
}

fn write_schema_and_catalog_version_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    target_schema_version: u64,
    next_catalog_version: u64,
) -> Result<(), String> {
    {
        let mut versions = wtx.open_table(SCHEMA_VERSIONS).map_err(map_err)?;
        versions
            .insert((tenant_scope, table), target_schema_version)
            .map_err(map_err)?;
    }
    let mut versions = wtx.open_table(SCHEMA_CATALOG_VERSIONS).map_err(map_err)?;
    versions
        .insert(tenant_scope, next_catalog_version)
        .map_err(map_err)?;
    Ok(())
}

fn claim_migration_order_slot_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    target_schema_version: u64,
    migration_id: &str,
) -> Result<(), String> {
    let mut order = wtx.open_table(SCHEMA_MIGRATION_ORDER).map_err(map_err)?;
    if let Some(previous) = order
        .get((tenant_scope, table, target_schema_version))
        .map_err(map_err)?
    {
        return Err(format!(
            "concurrent schema migration claimed version {} as `{}`",
            target_schema_version,
            previous.value()
        ));
    }
    order
        .insert((tenant_scope, table, target_schema_version), migration_id)
        .map_err(map_err)?;
    Ok(())
}

fn claim_catalog_order_slot_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    migration_id: &str,
    next_catalog_version: u64,
) -> Result<(), String> {
    let mut order = wtx.open_table(SCHEMA_CATALOG_ORDER).map_err(map_err)?;
    if let Some(previous) = order
        .get((tenant_scope, next_catalog_version))
        .map_err(map_err)?
    {
        return Err(format!(
            "concurrent schema migration claimed catalog version {} as `{}`",
            next_catalog_version,
            previous.value()
        ));
    }
    let identity = catalog_order_identity(table, migration_id);
    order
        .insert((tenant_scope, next_catalog_version), identity.as_str())
        .map_err(map_err)?;
    Ok(())
}

fn verify_migration_record(
    record: &SchemaMigrationRecord,
    tenant_scope: &str,
    table: &str,
    expected_version: u64,
) -> Result<(), String> {
    record.migration.validate()?;
    if record.state != MigrationState::Applied {
        return Err(format!(
            "schema migration `{}` is not in Applied state",
            record.migration.migration_id
        ));
    }
    if record.migration.tenant_scope != tenant_scope
        || record.migration.table != table
        || record.applied_schema_version != record.migration.target_schema_version
        || record.migration.target_schema_version != expected_version
        || record.catalog_version == 0
    {
        return Err(format!(
            "schema migration `{}` has an invalid scope/table/version binding",
            record.migration.migration_id
        ));
    }
    Ok(())
}

/// A migration may not touch a column that THIS table's own `FOREIGN KEY`
/// constraints depend on, on either side of the reference.
fn validate_migration_local_fks(
    current: &TableSchema,
    migration: &SchemaMigration,
    affected: &HashSet<&str>,
) -> Result<(), String> {
    for constraint in current.constraints() {
        let TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            ..
        } = constraint
        else {
            continue;
        };
        if columns
            .iter()
            .any(|column| affected.contains(column.as_str()))
        {
            return Err(format!(
                "schema migration `{}` affects local FOREIGN KEY columns; drop/rebind the FK explicitly first",
                migration.migration_id
            ));
        }
        if ref_table == &current.name
            && ref_columns
                .iter()
                .any(|column| affected.contains(column.as_str()))
        {
            return Err(format!(
                "schema migration `{}` affects referenced FOREIGN KEY columns; child constraints must be rebound explicitly first",
                migration.migration_id
            ));
        }
    }
    Ok(())
}

/// One child table's `FOREIGN KEY` constraints must not reference a column the
/// migration affects.
fn validate_migration_child_table_fks(
    current: &TableSchema,
    migration: &SchemaMigration,
    affected: &HashSet<&str>,
    child_table: &str,
    child_schema: &TableSchema,
) -> Result<(), String> {
    for constraint in child_schema.constraints() {
        let TableConstraint::ForeignKey {
            ref_table,
            ref_columns,
            ..
        } = constraint
        else {
            continue;
        };
        if ref_table == &current.name
            && ref_columns
                .iter()
                .any(|column| affected.contains(column.as_str()))
        {
            return Err(format!(
                "schema migration `{}` affects `{}` columns referenced by child table `{child_table}`",
                migration.migration_id, current.name
            ));
        }
    }
    Ok(())
}

/// The same conservative rule as [`validate_migration_local_fks`], applied to
/// every OTHER table that references `current`.
fn validate_migration_child_fks_in(
    wtx: &WriteTransaction,
    current: &TableSchema,
    migration: &SchemaMigration,
    affected: &HashSet<&str>,
) -> Result<(), String> {
    for child_table in list_tables_in(wtx)? {
        let Some(child_schema) = get_schema_in(wtx, &child_table)? else {
            continue;
        };
        validate_migration_child_table_fks(
            current,
            migration,
            affected,
            &child_table,
            &child_schema,
        )?;
    }
    Ok(())
}

/// The current branch's built-in dependent catalog is pgvector ANN.  The same
/// conservative rule is used for scalar secondary indexes when that catalog is
/// present: a caller may acknowledge a coordinated rebuild, but this transaction
/// never silently drops or leaves a stale index behind.
fn validate_migration_ann_indexes_in(
    wtx: &WriteTransaction,
    current: &TableSchema,
    migration: &SchemaMigration,
    affected: &HashSet<&str>,
) -> Result<(), String> {
    let indexes = wtx.open_table(ANN_INDEXES).map_err(map_err)?;
    for row in indexes.iter().map_err(map_err)? {
        let (_, value) = row.map_err(map_err)?;
        let index: AnnIndexPlan = decode_stored(value.value(), "ANN index")?;
        if index.table != current.name || !affected.contains(index.column.as_str()) {
            continue;
        }
        return Err(match migration.policy.secondary_indexes {
            SecondaryIndexPolicy::RejectAffected => format!(
                "schema migration `{}` affects indexed column `{}`; secondary index must be explicitly rebuilt",
                migration.migration_id, index.column
            ),
            SecondaryIndexPolicy::RebuildByCaller => format!(
                "schema migration `{}` acknowledged index rebuild, but the rebuild is not part of this atomic plan",
                migration.migration_id
            ),
        });
    }
    Ok(())
}

/// Check relationships and known local indexes before any row/catalog helper
/// mutates the write transaction.  The check is deliberately conservative:
/// changing a column participating in an FK is rejected because this bounded
/// migration API does not rewrite both sides atomically.  RLS is an external
/// authority; an affected plan must carry an explicit binding digest.
fn validate_migration_dependencies_in(
    wtx: &WriteTransaction,
    current: &TableSchema,
    migration: &SchemaMigration,
) -> Result<(), String> {
    let affected: HashSet<&str> = migration
        .operations
        .iter()
        .flat_map(SchemaMigrationOperation::affected_columns)
        .collect();
    if !affected.is_empty() {
        validate_migration_local_fks(current, migration, &affected)?;
        validate_migration_child_fks_in(wtx, current, migration, &affected)?;
    }
    validate_migration_ann_indexes_in(wtx, current, migration, &affected)?;
    if migration.policy.require_rls_revalidation && migration.policy.rls_binding_digest.is_none() {
        return Err(format!(
            "schema migration `{}` requires an RLS binding digest",
            migration.migration_id
        ));
    }
    Ok(())
}

/// Adding a NOT NULL column without a default would make existing short rows
/// invalid.  Validate that condition before the first catalog write; the normal
/// `add_column_in` helper intentionally remains permissive for legacy SQL DDL.
fn validate_added_columns_in(
    wtx: &WriteTransaction,
    table: &str,
    current: &TableSchema,
    projected: &TableSchema,
    migration: &SchemaMigration,
) -> Result<(), String> {
    if projected.columns().len() <= current.columns().len() {
        return Ok(());
    }
    let Some(rows) = wtx.open_table(ROWS).ok() else {
        return Ok(());
    };
    let added = &projected.columns()[current.columns().len()..];
    if added.iter().all(|column| column.nullable) {
        return Ok(());
    }
    for row in rows
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, value) = row.map_err(map_err)?;
        let cells: Vec<Cell> = decode_stored(value.value(), "row")?;
        if cells.len() < projected.columns().len() {
            return Err(format!(
                "schema migration `{}` adds a NOT NULL column to a table with existing rows; add it nullable, backfill, then tighten it in a separate migration",
                migration.migration_id
            ));
        }
    }
    Ok(())
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
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for row in cat.iter().map_err(map_err)? {
        let (key, value) = row.map_err(map_err)?;
        account_collection(
            &mut scanned_rows,
            &mut scanned_bytes,
            key.value().len().saturating_add(value.value().len()),
        )?;
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

fn get_schema_read(rtx: &ReadTransaction, name: &str) -> Result<Option<TableSchema>, String> {
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
        let Ok(spec) = decode_stored::<SecondaryIndexSpec>(value.value(), "secondary index") else {
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
        let Ok(spec) = decode_stored::<SecondaryIndexSpec>(value.value(), "secondary index") else {
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

    let Some(row_items) = secondary_index_build_rows_in(wtx, spec)? else {
        return Ok(true);
    };
    let mut entries = wtx.open_table(SECONDARY_INDEX_ENTRIES).map_err(map_err)?;
    for (rowid, cells) in row_items {
        let entry = secondary_entry_key(spec, &schema, &cells, rowid)?;
        entries.insert(entry.as_str(), &[][..]).map_err(map_err)?;
    }
    Ok(true)
}
/// One `(row_id, cells)` pair per row a secondary-index build reads.
///
/// A named alias rather than the bare tuple-in-`Option`-in-`Result`: clippy's
/// `type_complexity` fires on the spelled-out form, and the name says what the
/// pair means, which the tuple did not.
type IndexBuildRows = Vec<(u64, Vec<Cell>)>;


/// Every stored row of `spec.table`, for the initial directory construction.
/// Intentionally bounded and atomic: a large table asks its owner to
/// build/partition it explicitly rather than leaving a silently partial index
/// behind. `Ok(None)` means the rows table is absent, so there is nothing to build.
fn secondary_index_build_rows_in(
    wtx: &WriteTransaction,
    spec: &SecondaryIndexSpec,
) -> Result<Option<IndexBuildRows>, String> {
    let rows = match wtx.open_table(ROWS) {
        Ok(table) => table,
        Err(_) => return Ok(None),
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
    Ok(Some(row_items))
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
        let found = indexes.get(key.as_str()).map_err(map_err)?;
        found.is_some()
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
                        "secondary table-index drop exceeds the bounded entry limit".to_string()
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
            entries.insert(key.as_str(), &[][..]).map_err(map_err)?;
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
        spec.columns.first().map(|column| column.name.as_str()) == Some(lookup.column())
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
    // Probed up front so a missing ROWS table falls back BEFORE the entry scan,
    // exactly as when both handles were opened together.
    if rtx.open_table(ROWS).is_err() {
        return Ok(None);
    }
    let Some(mut rowids) = secondary_index_candidate_rowids_in(rtx, &spec)? else {
        return Ok(None);
    };
    if matches!(order, SecondaryIndexOrder::Desc) {
        rowids.reverse();
    }
    let start = offset.min(rowids.len());
    let end = limit
        .and_then(|count| start.checked_add(count))
        .unwrap_or(rowids.len())
        .min(rowids.len());
    secondary_index_materialize_rows_in(rtx, table, schema.columns().len(), &rowids[start..end])
}

/// The candidate row ids `spec`'s entry range points at, in index order.
/// `Ok(None)` means the caller must fall back to the scan path: the entry table is
/// absent, an entry key does not parse, or the candidate bound was exceeded.
fn secondary_index_candidate_rowids_in(
    rtx: &ReadTransaction,
    spec: &SecondaryIndexSpec,
) -> Result<Option<Vec<u64>>, String> {
    let entries = match rtx.open_table(SECONDARY_INDEX_ENTRIES) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let prefix = secondary_entry_prefix(spec);
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
    Ok(Some(rowids))
}

/// Materialize `rowids` into full, width-padded rows. `Ok(None)` means the caller
/// must fall back to the scan path: the rows table is absent, a row id dangles, or
/// the bounded-collection budget tripped.
fn secondary_index_materialize_rows_in(
    rtx: &ReadTransaction,
    table: &str,
    width: usize,
    rowids: &[u64],
) -> Result<Option<Vec<Vec<Cell>>>, String> {
    let rows = match rtx.open_table(ROWS) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let mut out = Vec::with_capacity(rowids.len());
    let mut row_count = 0usize;
    let mut row_bytes = 0usize;
    for rowid in rowids {
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

/// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — a table-level `PRIMARY KEY (a, b, …)`
/// (and every column-level primary-key flag) implies NOT NULL on every participating
/// column (mirrors Postgres), regardless of how the input `ColumnDef` declared nullability.
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
        // A caller may construct a schema directly with a column-level PRIMARY
        // KEY flag. Keep that path equivalent to SQL DDL: PK members are always
        // NOT NULL even when the input Column accidentally marked them nullable.
        for col in schema.columns_mut() {
            if col.primary_key {
                col.nullable = false;
            }
        }
        return;
    }
    for col in schema.columns_mut() {
        if col.primary_key || pk_cols.iter().any(|c| c == &col.name) {
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
    on_delete: RefAction,
    on_update: RefAction,
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
        if (matches!(on_delete, RefAction::SetNull) || matches!(on_update, RefAction::SetNull))
            && !local.nullable
        {
            return Err(format!(
                "FOREIGN KEY on table `{}`: SET NULL requires nullable column `{local_col}`",
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
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        if idxs.iter().zip(key).all(|(&i, k)| {
            cells
                .get(i)
                .is_some_and(|cell| typed_cells_equal(cell, k, schema.columns()[i].ty))
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Compare two persisted cells through their declared schema type. UUID and
/// NUMERIC deliberately have legacy Cell representations from before NE-002;
/// comparing the raw enum would make an old row fail a new FK/UNIQUE lookup even
/// though the logical value is identical.
fn typed_cells_equal(left: &Cell, right: &Cell, ty: ColumnType) -> bool {
    left.to_typed_json(ty) == right.to_typed_json(ty)
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
    validate_column_checks_in(schema, cells)?;
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

fn validate_column_checks_in(schema: &TableSchema, cells: &[Cell]) -> Result<(), String> {
    for (ci, column) in schema.columns().iter().enumerate() {
        if let Some(check) = &column.check {
            let value = cells
                .get(ci)
                .cloned()
                .unwrap_or(Cell::Null)
                .to_typed_json(column.ty);
            if !check.holds(&value) {
                return Err(format!(
                    "new row violates CHECK constraint on column `{}`",
                    column.name
                ));
            }
        }
    }
    Ok(())
}

/// The parent side of ONE foreign-key cascade step: the row of `table` that just
/// changed, its schema, and the tenant scope the cascade stays inside. Bundled so
/// the recursive FK helpers below stay under the argument cap.
struct ParentChange<'a> {
    tenant_scope: &'a str,
    table: &'a str,
    schema: &'a TableSchema,
    old_row: &'a [Cell],
    /// `None` for a parent DELETE, `Some(..)` for a parent UPDATE.
    new_row: Option<&'a [Cell]>,
}

/// The child side of ONE foreign-key cascade step: the referencing table, its
/// schema, the `FOREIGN KEY` constraint being enforced, and that constraint's
/// column positions already resolved against `schema`.
struct FkChild<'a> {
    table: &'a str,
    schema: &'a TableSchema,
    constraint: &'a TableConstraint,
    idxs: &'a [usize],
}

/// ONE matched child row of a foreign-key cascade: its row id and its cells as
/// they stood BEFORE the cascade touched them.
#[derive(Clone, Copy)]
struct FkChildRow<'a> {
    rowid: u64,
    cells: &'a [Cell],
}

/// What a single `FOREIGN KEY` on a child table implies for the parent change:
/// the referential action to take, the child columns that participate, and the
/// old/new referenced key values. [`plan_fk_cascade`] returns `None` when the
/// constraint is unaffected (not a FK, references another table, or the
/// referenced columns did not change).
struct FkCascadePlan {
    action: RefAction,
    child_idxs: Vec<usize>,
    old_key: Vec<Cell>,
    new_key: Option<Vec<Cell>>,
}

/// `true` when a parent UPDATE left every FK-referenced column untouched, so this
/// constraint's children need no cascade at all. A parent DELETE (`new_key ==
/// None`) always counts as a change.
fn fk_ref_key_unchanged(
    parent_schema: &TableSchema,
    ref_idxs: &[usize],
    old_key: &[Cell],
    new_key: Option<&[Cell]>,
) -> bool {
    let Some(nk) = new_key else {
        return false;
    };
    nk.iter()
        .zip(old_key)
        .enumerate()
        .all(|(offset, (new, old))| {
            typed_cells_equal(new, old, parent_schema.columns()[ref_idxs[offset]].ty)
        })
}

/// Resolve ONE of a child table's constraints against the parent change, or
/// `None` when it implies no work. Pure — it reads no storage.
fn plan_fk_cascade(
    parent: &ParentChange<'_>,
    child_schema: &TableSchema,
    constraint: &TableConstraint,
) -> Option<FkCascadePlan> {
    let TableConstraint::ForeignKey {
        columns,
        ref_table,
        ref_columns,
        on_delete,
        on_update,
        name: _,
    } = constraint
    else {
        return None;
    };
    if ref_table != parent.table {
        return None;
    }
    let ref_idxs: Vec<usize> = ref_columns
        .iter()
        .map(|c| {
            parent
                .schema
                .column_index(c)
                .expect("FK ref column existence validated at DDL time")
        })
        .collect();
    let old_key: Vec<Cell> = ref_idxs
        .iter()
        .map(|&i| parent.old_row.get(i).cloned().unwrap_or(Cell::Null))
        .collect();
    let new_key: Option<Vec<Cell>> = parent.new_row.map(|nr| {
        ref_idxs
            .iter()
            .map(|&i| nr.get(i).cloned().unwrap_or(Cell::Null))
            .collect()
    });
    if fk_ref_key_unchanged(parent.schema, &ref_idxs, &old_key, new_key.as_deref()) {
        return None;
    }
    let action = if parent.new_row.is_some() {
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
    Some(FkCascadePlan {
        action,
        child_idxs,
        old_key,
        new_key,
    })
}

/// MATCH SIMPLE: a child row whose FK columns contain ANY NULL never references a
/// parent, so it is exempt. Otherwise it matches when every FK column equals the
/// parent's old referenced key under the column's type coercion.
fn fk_child_row_matches(child: &FkChild<'_>, old_key: &[Cell], cells: &[Cell]) -> bool {
    let any_null = child
        .idxs
        .iter()
        .any(|&i| !matches!(cells.get(i), Some(c) if !matches!(c, Cell::Null)));
    if any_null {
        return false;
    }
    child.idxs.iter().zip(old_key).all(|(&i, k)| {
        cells
            .get(i)
            .is_some_and(|cell| typed_cells_equal(cell, k, child.schema.columns()[i].ty))
    })
}

/// Snapshot every child row that references the parent's OLD key. The read table
/// is dropped before the caller mutates anything.
fn collect_fk_child_matches_in(
    wtx: &WriteTransaction,
    child: &FkChild<'_>,
    old_key: &[Cell],
) -> Result<Vec<(u64, Vec<Cell>)>, String> {
    let width = child.schema.columns().len();
    let rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    let mut out = Vec::new();
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows_t
        .range((child.table, 0u64)..=(child.table, u64::MAX))
        .map_err(map_err)?
    {
        let (k, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        if fk_child_row_matches(child, old_key, &cells) {
            out.push((k.value().1, cells));
        }
    }
    Ok(out)
}

/// Persist ONE cascaded child row: revalidate its constraints, encode it, write it
/// back, recheck uniqueness, and maintain its secondary-index entries. Shared by
/// the `CASCADE`-update and `SET NULL` paths, which differ only in how `updated`
/// was derived.
fn write_cascaded_child_row_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    child: &FkChild<'_>,
    row: FkChildRow<'_>,
    updated: &[Cell],
) -> Result<(), String> {
    validate_row_constraints_in(wtx, child.schema, updated)?;
    let blob = rmp_serde::to_vec_named(updated).map_err(|e| format!("encode row: {e}"))?;
    if blob.len() > MAX_SQL_STORED_VALUE_BYTES {
        return Err("encoded SQL row exceeds storage value limit".to_string());
    }
    {
        let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
        rows_t
            .insert((child.table, row.rowid), blob.as_slice())
            .map_err(map_err)?;
    }
    validate_uniqueness_in(wtx, child.table, child.schema)?;
    maintain_secondary_row_in(
        wtx,
        tenant_scope,
        child.table,
        child.schema,
        row.rowid,
        Some(row.cells),
        Some(updated),
    )
}

/// `ON UPDATE CASCADE` for ONE child row: rewrite its FK columns to the parent's
/// new key, then recurse so the child's OWN children see the change.
fn cascade_update_child_row_in(
    wtx: &WriteTransaction,
    parent: &ParentChange<'_>,
    child: &FkChild<'_>,
    row: FkChildRow<'_>,
    new_key: &[Cell],
    visited: &mut HashSet<(String, u64)>,
) -> Result<(), String> {
    let mut updated = row.cells.to_vec();
    for (&i, v) in child.idxs.iter().zip(new_key) {
        updated[i] = v.clone();
    }
    write_cascaded_child_row_in(wtx, parent.tenant_scope, child, row, &updated)?;
    enforce_fk_on_parent_change_in(
        wtx,
        parent.tenant_scope,
        child.table,
        row.rowid,
        row.cells,
        Some(&updated),
        visited,
    )
}

/// `ON DELETE CASCADE` for ONE child row: remove it, drop its index entries, then
/// recurse so the child's OWN children are cascaded too.
fn cascade_delete_child_row_in(
    wtx: &WriteTransaction,
    parent: &ParentChange<'_>,
    child: &FkChild<'_>,
    row: FkChildRow<'_>,
    visited: &mut HashSet<(String, u64)>,
) -> Result<(), String> {
    {
        let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
        rows_t.remove((child.table, row.rowid)).map_err(map_err)?;
    }
    maintain_secondary_row_in(
        wtx,
        parent.tenant_scope,
        child.table,
        child.schema,
        row.rowid,
        Some(row.cells),
        None,
    )?;
    enforce_fk_on_parent_change_in(
        wtx,
        parent.tenant_scope,
        child.table,
        row.rowid,
        row.cells,
        None,
        visited,
    )
}

/// `CASCADE` over every matched child row — an UPDATE when the parent supplied a
/// new key, a DELETE otherwise.
fn cascade_fk_matches_in(
    wtx: &WriteTransaction,
    parent: &ParentChange<'_>,
    child: &FkChild<'_>,
    new_key: Option<&[Cell]>,
    matches: Vec<(u64, Vec<Cell>)>,
    visited: &mut HashSet<(String, u64)>,
) -> Result<(), String> {
    for (child_rowid, child_cells) in matches {
        let row = FkChildRow {
            rowid: child_rowid,
            cells: &child_cells,
        };
        match new_key {
            Some(nk) => cascade_update_child_row_in(wtx, parent, child, row, nk, visited)?,
            None => cascade_delete_child_row_in(wtx, parent, child, row, visited)?,
        }
    }
    Ok(())
}

/// `SET NULL` requires every participating child column to be nullable — checked
/// BEFORE any row is rewritten, exactly as the pre-refactor code did.
fn ensure_fk_columns_nullable(child: &FkChild<'_>) -> Result<(), String> {
    for &i in child.idxs {
        if !child.schema.columns()[i].nullable {
            return Err(format!(
                "cannot SET NULL on non-nullable column `{}` of table `{}` for foreign key `{}`",
                child.schema.columns()[i].name,
                child.table,
                TableSchema::constraint_display_name(child.table, child.constraint)
            ));
        }
    }
    Ok(())
}

/// `SET NULL` over every matched child row.
fn set_null_fk_matches_in(
    wtx: &WriteTransaction,
    parent: &ParentChange<'_>,
    child: &FkChild<'_>,
    matches: Vec<(u64, Vec<Cell>)>,
) -> Result<(), String> {
    for (child_rowid, child_cells) in matches {
        ensure_fk_columns_nullable(child)?;
        let mut updated = child_cells.clone();
        for &i in child.idxs {
            updated[i] = Cell::Null;
        }
        write_cascaded_child_row_in(
            wtx,
            parent.tenant_scope,
            child,
            FkChildRow {
                rowid: child_rowid,
                cells: &child_cells,
            },
            &updated,
        )?;
    }
    Ok(())
}

/// Enforce ONE constraint of ONE child table against the parent change: plan it,
/// snapshot the referencing rows, then take the referential action.
fn enforce_fk_constraint_on_child_in(
    wtx: &WriteTransaction,
    parent: &ParentChange<'_>,
    child_table: &str,
    child_schema: &TableSchema,
    constraint: &TableConstraint,
    visited: &mut HashSet<(String, u64)>,
) -> Result<(), String> {
    let Some(plan) = plan_fk_cascade(parent, child_schema, constraint) else {
        return Ok(());
    };
    let child = FkChild {
        table: child_table,
        schema: child_schema,
        constraint,
        idxs: &plan.child_idxs,
    };
    let matches = collect_fk_child_matches_in(wtx, &child, &plan.old_key)?;
    if matches.is_empty() {
        return Ok(());
    }
    match plan.action {
        RefAction::NoAction | RefAction::Restrict => {
            let cname = TableSchema::constraint_display_name(child_table, constraint);
            let table = parent.table;
            Err(format!(
                "update or delete on table `{table}` violates foreign key constraint `{cname}` on table `{child_table}`"
            ))
        }
        RefAction::Cascade => cascade_fk_matches_in(
            wtx,
            parent,
            &child,
            plan.new_key.as_deref(),
            matches,
            visited,
        ),
        RefAction::SetNull => set_null_fk_matches_in(wtx, parent, &child, matches),
    }
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
    let parent = ParentChange {
        tenant_scope,
        table,
        schema: &parent_schema,
        old_row,
        new_row,
    };
    for child_table in list_tables_in(wtx)? {
        let Some(child_schema) = get_schema_in(wtx, &child_table)? else {
            continue;
        };
        for c in child_schema.constraints().to_vec() {
            enforce_fk_constraint_on_child_in(
                wtx,
                &parent,
                &child_table,
                &child_schema,
                &c,
                visited,
            )?;
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
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
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
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
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
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    let rows: Vec<Vec<Cell>> = rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
        .map(|r| {
            let (_, v) = r.map_err(map_err)?;
            account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
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
            on_delete,
            on_update,
            ..
        } = &c
        {
            validate_fk_target_in(
                wtx,
                &schema,
                columns,
                ref_table,
                ref_columns,
                *on_delete,
                *on_update,
            )?;
        }
    }
    let schema = &schema;
    let blob = rmp_serde::to_vec_named(schema).map_err(|e| format!("encode schema: {e}"))?;
    if blob.len() > MAX_SQL_STORED_VALUE_BYTES {
        return Err("encoded SQL schema exceeds storage value limit".to_string());
    }
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
    ensure_no_child_fk_references_in(wtx, name)?;
    {
        let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
        cat.remove(name).map_err(map_err)?;
    }
    {
        let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
        seq.remove(name).map_err(map_err)?;
    }
    delete_all_rows_of_table_in(wtx, name)?;
    drop_secondary_indexes_for_table_in(wtx, tenant_scope, name)?;
    {
        let mut hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
        hypertables.remove(name).map_err(map_err)?;
    }
    Ok(true)
}

/// One child table's `FOREIGN KEY` constraints must not reference `name`.
fn ensure_child_table_does_not_reference(
    child_table: &str,
    child_schema: &TableSchema,
    name: &str,
) -> Result<(), String> {
    for constraint in child_schema.constraints() {
        let TableConstraint::ForeignKey { ref_table, .. } = constraint else {
            continue;
        };
        if ref_table == name {
            let cname = TableSchema::constraint_display_name(child_table, constraint);
            return Err(format!(
                "cannot drop table `{name}` because foreign key `{cname}` on table `{child_table}` references it"
            ));
        }
    }
    Ok(())
}

/// Keep the catalog graph closed: dropping a referenced parent while a child FK
/// remains would leave a durable constraint that can no longer be checked. The
/// check is schema-only (no tenant row values are surfaced) and runs in the same
/// write transaction, so a failure cannot partially remove metadata.
fn ensure_no_child_fk_references_in(wtx: &WriteTransaction, name: &str) -> Result<(), String> {
    for child_table in list_tables_in(wtx)? {
        if child_table == name {
            continue;
        }
        let Some(child_schema) = get_schema_in(wtx, &child_table)? else {
            continue;
        };
        ensure_child_table_does_not_reference(&child_table, &child_schema, name)?;
    }
    Ok(())
}

/// Remove every stored row of `table` inside the open write txn. Row ids are
/// collected first so the range borrow ends before the removals begin.
fn delete_all_rows_of_table_in(wtx: &WriteTransaction, table: &str) -> Result<(), String> {
    let mut rows = wtx.open_table(ROWS).map_err(map_err)?;
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    let keys: Vec<u64> = rows
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
        .map(|r| {
            let (k, v) = r.map_err(map_err)?;
            account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
            Ok::<u64, String>(k.value().1)
        })
        .collect::<Result<_, _>>()?;
    for rowid in keys {
        rows.remove((table, rowid)).map_err(map_err)?;
    }
    Ok(())
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
    if blob.len() > MAX_SQL_STORED_VALUE_BYTES {
        return Err("encoded SQL schema exceeds storage value limit".to_string());
    }
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
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (k, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
        let cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        items.push((k.value().1, cells));
    }
    for (rowid, mut cells) in items {
        f(&mut cells)?;
        let blob = rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?;
        if blob.len() > MAX_SQL_STORED_VALUE_BYTES {
            return Err("encoded SQL row exceeds storage value limit".to_string());
        }
        rows_t
            .insert((table, rowid), blob.as_slice())
            .map_err(map_err)?;
    }
    Ok(())
}

/// A hypertable's time column is structural — `DROP COLUMN` may never remove it.
fn ensure_not_hypertable_time_column_in(
    wtx: &WriteTransaction,
    table: &str,
    column: &str,
) -> Result<(), String> {
    let hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
    let Some(value) = hypertables.get(table).map_err(map_err)? else {
        return Ok(());
    };
    let plan = decode_stored::<HypertablePlan>(value.value(), "hypertable")?;
    if plan.time_column == column {
        return Err(format!(
            "cannot drop hypertable time column `{table}.{column}`"
        ));
    }
    Ok(())
}

/// `column` must not participate in any of `schema`'s OWN `FOREIGN KEY`
/// constraints, on either side of a self-reference.
fn ensure_column_free_of_local_fks(
    schema: &TableSchema,
    table: &str,
    column: &str,
) -> Result<(), String> {
    for constraint in schema.constraints() {
        let TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            ..
        } = constraint
        else {
            continue;
        };
        if columns.iter().any(|name| name == column)
            || (ref_table == table && ref_columns.iter().any(|name| name == column))
        {
            return Err(format!(
                "cannot drop column `{table}.{column}` while foreign key `{}` uses it",
                TableSchema::constraint_display_name(table, constraint)
            ));
        }
    }
    Ok(())
}

/// One child table's `FOREIGN KEY` constraints must not reference `table.column`.
fn ensure_child_table_fks_free_of_column(
    child_table: &str,
    child_schema: &TableSchema,
    table: &str,
    column: &str,
) -> Result<(), String> {
    for constraint in child_schema.constraints() {
        let TableConstraint::ForeignKey {
            ref_table,
            ref_columns,
            ..
        } = constraint
        else {
            continue;
        };
        if ref_table == table && ref_columns.iter().any(|name| name == column) {
            return Err(format!(
                "cannot drop column `{table}.{column}` because foreign key `{}` on table `{child_table}` references it",
                TableSchema::constraint_display_name(child_table, constraint)
            ));
        }
    }
    Ok(())
}

/// No OTHER table may reference `table.column` through a `FOREIGN KEY`.
fn ensure_column_free_of_child_fks_in(
    wtx: &WriteTransaction,
    table: &str,
    column: &str,
) -> Result<(), String> {
    for child_table in list_tables_in(wtx)? {
        if child_table == table {
            continue;
        }
        let Some(child_schema) = get_schema_in(wtx, &child_table)? else {
            continue;
        };
        ensure_child_table_fks_free_of_column(&child_table, &child_schema, table, column)?;
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
    ensure_not_hypertable_time_column_in(wtx, table, column)?;
    let Some(idx) = schema.column_index(column) else {
        if if_exists {
            return Ok(());
        }
        return Err(format!(
            "column `{column}` does not exist in table `{table}`"
        ));
    };
    if schema.columns().len() == 1 {
        return Err(format!(
            "cannot drop the only column `{column}` of table `{table}`"
        ));
    }
    ensure_column_free_of_local_fks(&schema, table, column)?;
    ensure_column_free_of_child_fks_in(wtx, table, column)?;
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

/// Rewrite every `FOREIGN KEY` reference in `child_schema` that points at
/// `table.from` so it points at `table.to`. Returns `true` when anything changed.
fn rebind_child_fk_ref_columns(
    child_schema: &mut TableSchema,
    table: &str,
    from: &str,
    to: &str,
) -> bool {
    let mut changed = false;
    for constraint in child_schema.constraints_mut() {
        let TableConstraint::ForeignKey {
            ref_table,
            ref_columns,
            ..
        } = constraint
        else {
            continue;
        };
        if ref_table != table {
            continue;
        }
        for column in ref_columns {
            if column == from {
                *column = to.to_string();
                changed = true;
            }
        }
    }
    changed
}

/// Rebind and persist every OTHER table whose `FOREIGN KEY` referenced the column
/// being renamed. Collected first, written second, exactly as before.
fn rename_column_in_child_fks_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    from: &str,
    to: &str,
) -> Result<(), String> {
    let mut dependents = Vec::new();
    for child_table in list_tables_in(wtx)? {
        if child_table == table {
            continue;
        }
        let Some(mut child_schema) = get_schema_in(wtx, &child_table)? else {
            continue;
        };
        if rebind_child_fk_ref_columns(&mut child_schema, table, from, to) {
            dependents.push(child_schema);
        }
    }
    for child_schema in &dependents {
        put_schema_in(wtx, tenant_scope, child_schema)?;
    }
    Ok(())
}

/// Rewrite the renamed column's name inside `schema`'s OWN table-level
/// constraints (PK/UNIQUE column lists, FK local and self-referencing lists, and
/// CHECK expressions).
fn rename_column_in_constraints(schema: &mut TableSchema, table: &str, from: &str, to: &str) {
    for constraint in schema.constraints_mut() {
        match constraint {
            TableConstraint::PrimaryKey { columns, .. }
            | TableConstraint::Unique { columns, .. } => {
                rename_constraint_column_list(columns, from, to);
            }
            TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                ..
            } => {
                rename_constraint_column_list(columns, from, to);
                if ref_table == table {
                    rename_constraint_column_list(ref_columns, from, to);
                }
            }
            TableConstraint::Check { expr, .. } => rename_check_column(expr, from, to),
        }
    }
}

/// Follow the rename into the hypertable catalog when the renamed column WAS the
/// hypertable's time column.
fn rename_hypertable_time_column_in(
    wtx: &WriteTransaction,
    table: &str,
    from: &str,
    to: &str,
) -> Result<(), String> {
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
    rename_column_in_child_fks_in(wtx, tenant_scope, table, from, to)?;
    rename_column_in_constraints(&mut schema, table, from, to);
    schema.columns_mut()[idx].name = to.to_string();
    put_schema_in(wtx, tenant_scope, &schema)?;
    rename_hypertable_time_column_in(wtx, table, from, to)
}

fn rename_constraint_column_list(columns: &mut [String], from: &str, to: &str) {
    for column in columns {
        if column == from {
            *column = to.to_string();
        }
    }
}

fn rename_check_column(expr: &mut CheckExpr, from: &str, to: &str) {
    match expr {
        CheckExpr::Cmp { column, .. }
        | CheckExpr::In { column, .. }
        | CheckExpr::IsNull { column, .. } => {
            if column == from {
                *column = to.to_string();
            }
        }
        CheckExpr::ColCmp { left, right, .. } => {
            if left == from {
                *left = to.to_string();
            }
            if right == from {
                *right = to.to_string();
            }
        }
        CheckExpr::And(left, right) | CheckExpr::Or(left, right) => {
            rename_check_column(left, from, to);
            rename_check_column(right, from, to);
        }
    }
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
    let mut schema = load_schema_for_rename_in(wtx, table, new_name)?;
    // Update every inbound/self FK in the same redb transaction. Leaving a
    // child constraint pointing at the old catalog key would make a successful
    // rename create a permanently unenforceable relationship.
    retarget_inbound_foreign_keys_in(wtx, tenant_scope, table, new_name)?;
    retarget_schema_foreign_keys(&mut schema, table, new_name);
    rekey_table_catalog_entry_in(wtx, tenant_scope, table, new_name, &mut schema)?;
    // Sequence: carry the rowid allocator forward so SERIAL ids never collide/reuse.
    carry_forward_table_sequence_in(wtx, table, new_name)?;
    // Rows: re-key each (table, rowid) -> (new_name, rowid).
    rekey_table_rows_in(wtx, table, new_name)?;
    rename_hypertable_if_present_in(wtx, table, new_name)?;
    Ok(())
}

fn load_schema_for_rename_in(
    wtx: &WriteTransaction,
    table: &str,
    new_name: &str,
) -> Result<TableSchema, String> {
    let schema =
        get_schema_in(wtx, table)?.ok_or_else(|| format!("table `{table}` does not exist"))?;
    if get_schema_in(wtx, new_name)?.is_some() {
        return Err(format!("table `{new_name}` already exists"));
    }
    Ok(schema)
}

fn retarget_inbound_foreign_keys_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    new_name: &str,
) -> Result<(), String> {
    let mut dependents = Vec::new();
    for child_table in list_tables_in(wtx)? {
        if child_table == table {
            continue;
        }
        let Some(mut child_schema) = get_schema_in(wtx, &child_table)? else {
            continue;
        };
        if retarget_schema_foreign_keys(&mut child_schema, table, new_name) {
            dependents.push(child_schema);
        }
    }
    for child_schema in &dependents {
        put_schema_in(wtx, tenant_scope, child_schema)?;
    }
    Ok(())
}

/// Points every `FOREIGN KEY ... REFERENCES table(...)` constraint in
/// `schema` at `new_name` instead. Returns whether anything changed (used
/// by `retarget_inbound_foreign_keys_in` to decide which child schemas need
/// writing back).
fn retarget_schema_foreign_keys(schema: &mut TableSchema, table: &str, new_name: &str) -> bool {
    let mut changed = false;
    for constraint in schema.constraints_mut() {
        if let TableConstraint::ForeignKey { ref_table, .. } = constraint {
            if ref_table == table {
                *ref_table = new_name.to_string();
                changed = true;
            }
        }
    }
    changed
}

fn rekey_table_catalog_entry_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    new_name: &str,
    schema: &mut TableSchema,
) -> Result<(), String> {
    drop_secondary_indexes_for_table_in(wtx, tenant_scope, table)?;
    // Catalog: drop the old key, write the schema under the new name.
    schema.name = new_name.to_string();
    {
        let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
        cat.remove(table).map_err(map_err)?;
    }
    put_schema_in(wtx, tenant_scope, schema)
}

fn carry_forward_table_sequence_in(
    wtx: &WriteTransaction,
    table: &str,
    new_name: &str,
) -> Result<(), String> {
    let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
    let val = seq.get(table).map_err(map_err)?.map(|g| g.value());
    seq.remove(table).map_err(map_err)?;
    if let Some(v) = val {
        seq.insert(new_name, v).map_err(map_err)?;
    }
    Ok(())
}

fn rekey_table_rows_in(wtx: &WriteTransaction, table: &str, new_name: &str) -> Result<(), String> {
    let mut rows = wtx.open_table(ROWS).map_err(map_err)?;
    let items = collect_table_rows_for_rekey_in(&rows, table)?;
    for (rowid, blob) in &items {
        rows.remove((table, *rowid)).map_err(map_err)?;
        rows.insert((new_name, *rowid), blob.as_slice())
            .map_err(map_err)?;
    }
    Ok(())
}

fn collect_table_rows_for_rekey_in(
    rows: &redb::Table<(&str, u64), &[u8]>,
    table: &str,
) -> Result<Vec<(u64, Vec<u8>)>, String> {
    let mut items: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (k, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
        items.push((k.value().1, v.value().to_vec()));
    }
    Ok(items)
}

fn rename_hypertable_if_present_in(
    wtx: &WriteTransaction,
    table: &str,
    new_name: &str,
) -> Result<(), String> {
    let renamed_hypertable = {
        let hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
        let plan = hypertables
            .get(table)
            .map_err(map_err)?
            .map(|value| decode_stored::<HypertablePlan>(value.value(), "hypertable"))
            .transpose()?;
        plan
    };
    let Some(mut plan) = renamed_hypertable else {
        return Ok(());
    };
    plan.table = new_name.to_string();
    let bytes = rmp_serde::to_vec_named(&plan).map_err(|e| format!("encode hypertable: {e}"))?;
    let mut hypertables = wtx.open_table(HYPERTABLES).map_err(map_err)?;
    hypertables.remove(table).map_err(map_err)?;
    hypertables
        .insert(new_name, bytes.as_slice())
        .map_err(map_err)?;
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
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — a table-level constraint (composite PK/UNIQUE/FK/CHECK)
    // is tried FIRST so an explicit `CONSTRAINT <name>` always takes precedence over
    // a same-named synthesized column-flag match. `||` short-circuits, so a later
    // rule only runs when no earlier one matched.
    let matched = schema.remove_constraint_named(constraint)
        || drop_synthesized_pkey_flags(&mut schema, table, constraint)
        || drop_synthesized_column_flags(&mut schema, table, constraint);
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

/// Clear the per-column PRIMARY KEY flags addressed by Postgres's synthesized
/// `<table>_pkey` name. Returns whether anything matched.
fn drop_synthesized_pkey_flags(schema: &mut TableSchema, table: &str, constraint: &str) -> bool {
    if constraint != format!("{table}_pkey") {
        return false;
    }
    let mut matched = false;
    for c in schema.columns_mut() {
        if c.primary_key {
            c.primary_key = false;
            c.unique = false;
            matched = true;
        }
    }
    matched
}

/// Clear the per-column UNIQUE / CHECK flags addressed by Postgres's synthesized
/// `<table>_<col>_key` / `<table>_<col>_check` names. Returns whether anything
/// matched.
fn drop_synthesized_column_flags(schema: &mut TableSchema, table: &str, constraint: &str) -> bool {
    let mut matched = false;
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
    matched
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
    force_pk_not_null(&mut trial);
    trial.validate()?;
    if let TableConstraint::ForeignKey {
        columns,
        ref_table,
        ref_columns,
        on_delete,
        on_update,
        ..
    } = &constraint
    {
        validate_fk_target_in(
            wtx,
            &trial,
            columns,
            ref_table,
            ref_columns,
            *on_delete,
            *on_update,
        )?;
    }
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
    match ty {
        ColumnType::Int | ColumnType::BigInt | ColumnType::Timestamp => {
            coercion_value_integral(old)
        }
        ColumnType::Float | ColumnType::Double => coercion_value_float(old),
        ColumnType::Bool => coercion_value_bool(old),
        ColumnType::Uuid
        | ColumnType::Numeric(_)
        | ColumnType::TimestampTz
        | ColumnType::Array(_) => old.to_typed_json(ty),
        // Text / Json / Bytes / Vector reuse the cell's plain JSON form; `Cell::coerce`
        // renders a scalar into text, parses a string into bytes, etc.
        _ => old.to_json(),
    }
}

/// A JSON number for `f`, or `Null` when the value is one JSON cannot represent
/// (NaN / infinity) — the downstream `Cell::coerce` then rejects it precisely.
fn coercion_json_f64(f: f64) -> Value {
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// [`coercion_value`] for an integral target (`Int` / `BigInt` / `Timestamp`).
fn coercion_value_integral(old: &Cell) -> Value {
    match old {
        Cell::Int(i) | Cell::Timestamp(i) => Value::Number((*i).into()),
        Cell::Float(f) if f.fract() == 0.0 && f.is_finite() => Value::Number((*f as i64).into()),
        Cell::Bool(b) => Value::Number((*b as i64).into()),
        Cell::Text(s) => s
            .trim()
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            .unwrap_or_else(|_| Value::String(s.clone())),
        other => other.to_json(),
    }
}

/// [`coercion_value`] for a floating-point target (`Float` / `Double`).
fn coercion_value_float(old: &Cell) -> Value {
    match old {
        Cell::Int(i) | Cell::Timestamp(i) => coercion_json_f64(*i as f64),
        Cell::Float(f) => coercion_json_f64(*f),
        Cell::Text(s) => s
            .trim()
            .parse::<f64>()
            .map(coercion_json_f64)
            .unwrap_or_else(|_| Value::String(s.clone())),
        other => other.to_json(),
    }
}

/// [`coercion_value`] for a `Bool` target.
fn coercion_value_bool(old: &Cell) -> Value {
    match old {
        Cell::Bool(b) => Value::Bool(*b),
        Cell::Int(i) => Value::Bool(*i != 0),
        Cell::Text(s) => parse_bool_text(s)
            .map(Value::Bool)
            .unwrap_or_else(|| Value::String(s.clone())),
        other => other.to_json(),
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

/// Bound caller-controlled JSON before it reaches a Cell or a redb write. This
/// is intentionally checked at every DML boundary (INSERT defaults/supplied
/// values and UPDATE assignments), not only by the eventual MessagePack decoder;
/// a tenant cannot make an unbounded value transiently occupy the write txn.
fn validate_mutation_value(value: &Value) -> Result<(), String> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| format!("SQL mutation value is not serializable: {error}"))?;
    if encoded.len() > MAX_SQL_STORED_VALUE_BYTES {
        return Err("SQL mutation value exceeds storage value limit".to_string());
    }
    Ok(())
}

fn insert_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    col_order: &[String],
    rows: &[Vec<Value>],
) -> Result<Vec<Vec<Cell>>, String> {
    if rows.len() > MAX_SQL_SCAN_ROWS {
        return Err(format!(
            "INSERT contains {} rows; maximum is {MAX_SQL_SCAN_ROWS}",
            rows.len()
        ));
    }
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
        let blob = rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?;
        if blob.len() > MAX_SQL_STORED_VALUE_BYTES {
            return Err("encoded SQL row exceeds storage value limit".to_string());
        }
        encoded.push((rowid, blob));
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
        validate_mutation_value(val)?;
        let col = &schema.columns()[idx];
        cells[idx] = Cell::coerce(val, col.ty, col.nullable)?;
        supplied[idx] = true;
    }
    fill_omitted_insert_cells(schema, &mut cells, &supplied, rowid)?;
    validate_column_checks_in(schema, &cells)?;
    Ok(cells)
}

/// Fill every column an INSERT omitted: SERIAL takes the allocated `rowid + 1`,
/// else the column DEFAULT, else NULL — and a NOT NULL column with neither is
/// rejected.
fn fill_omitted_insert_cells(
    schema: &TableSchema,
    cells: &mut [Cell],
    supplied: &[bool],
    rowid: u64,
) -> Result<(), String> {
    for (ci, col) in schema.columns().iter().enumerate() {
        if supplied[ci] {
            continue;
        }
        if col.serial {
            cells[ci] = Cell::coerce(&Value::Number((rowid as i64 + 1).into()), col.ty, false)?;
        } else if let Some(def) = &col.default {
            validate_mutation_value(def)?;
            cells[ci] = Cell::coerce(def, col.ty, col.nullable)?;
        } else if !col.nullable {
            return Err(format!(
                "column `{}` is NOT NULL and was not supplied",
                col.name
            ));
        }
    }
    Ok(())
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
    if rows.len() > MAX_SQL_SCAN_ROWS {
        return Err(format!(
            "INSERT contains {} rows; maximum is {MAX_SQL_SCAN_ROWS}",
            rows.len()
        ));
    }
    let ctx = prepare_insert_on_conflict_context(wtx, table, col_order)?;
    let mut state = build_conflict_scan_state_in(
        wtx,
        table,
        ctx.width,
        &ctx.schema,
        &ctx.unique_cols,
        &ctx.composite_cols,
    )?;

    let spec = ConflictRowSpec {
        table,
        col_order,
        targets: &ctx.targets,
        target_position_by_col: &ctx.target_position_by_col,
        schema: &ctx.schema,
        unique_cols: &ctx.unique_cols,
        composite_cols: &ctx.composite_cols,
    };
    let mut out = ConflictInsertOutcome::default();
    let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    for row in rows {
        process_insert_on_conflict_row(wtx, &spec, action, row, &mut rows_t, &mut state, &mut out)?;
    }
    drop(rows_t);
    finalize_insert_on_conflict(
        wtx,
        tenant_scope,
        table,
        &ctx.schema,
        &out.index_changes,
        &out.affected,
    )?;
    Ok(out.affected)
}

/// The immutable, per-`INSERT ... ON CONFLICT` resolution every row-level
/// helper needs: the target table's name and schema plus the resolved column
/// geometry. Borrowed, built once from [`InsertOnConflictContext`] in
/// [`insert_on_conflict_in`] and threaded down unchanged -- the same
/// bundle-the-parameters shape as `server::mutation::MutationCtx`.
struct ConflictRowSpec<'a> {
    table: &'a str,
    col_order: &'a [String],
    targets: &'a [usize],
    /// `column index -> first supplied position in the INSERT`, for conflict detection.
    target_position_by_col: &'a [Option<usize>],
    schema: &'a TableSchema,
    unique_cols: &'a [usize],
    composite_cols: &'a [Vec<usize>],
}

/// The two accumulators an `ON CONFLICT` row loop appends to: the
/// inserted-or-updated rows (for `RETURNING`) and the per-row secondary-index
/// deltas consumed by [`finalize_insert_on_conflict`].
#[derive(Default)]
struct ConflictInsertOutcome {
    affected: Vec<Vec<Cell>>,
    index_changes: Vec<IndexChange>,
}

/// Per-call setup for `insert_on_conflict_in`: the target schema, the
/// resolved target-column positions, the set of unique (single-column)
/// columns, a `col -> first-supplied-position` directory, and the set of
/// multi-column PRIMARY KEY/UNIQUE constraints (NE-001 composite keys).
/// Factored out purely to keep `insert_on_conflict_in`'s own CCN low; none
/// of this logic changed.
struct InsertOnConflictContext {
    schema: TableSchema,
    width: usize,
    targets: Vec<usize>,
    unique_cols: Vec<usize>,
    target_position_by_col: Vec<Option<usize>>,
    composite_cols: Vec<Vec<usize>>,
}

fn prepare_insert_on_conflict_context(
    wtx: &WriteTransaction,
    table: &str,
    col_order: &[String],
) -> Result<InsertOnConflictContext, String> {
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

    // Keep the existing per-column directory and add one bounded directory for each
    // composite table-level key; this lets ON CONFLICT honor NE-001 keys without
    // turning the final authoritative uniqueness scan into a second authority.
    let composite_cols: Vec<Vec<usize>> = schema
        .constraints()
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::PrimaryKey { columns, .. }
            | TableConstraint::Unique { columns, .. }
                if columns.len() > 1 =>
            {
                Some(
                    columns
                        .iter()
                        .map(|name| {
                            schema
                                .column_index(name)
                                .expect("constraint column existence validated at DDL time")
                        })
                        .collect(),
                )
            }
            _ => None,
        })
        .collect();
    Ok(InsertOnConflictContext {
        schema,
        width,
        targets,
        unique_cols,
        target_position_by_col,
        composite_cols,
    })
}

/// Current unique-value snapshot (committed + staged), rebuilt from the
/// store, for ON CONFLICT lookup during `insert_on_conflict_in`.
struct ConflictScanState {
    existing: Vec<(u64, Vec<Cell>)>,
    row_slot: HashMap<u64, usize>,
    unique_rows: Vec<HashMap<String, u64>>,
    composite_rows: Vec<HashMap<String, u64>>,
}

/// Builds the ON CONFLICT lookup snapshot by scanning every existing row of
/// `table`. When the physical row table does not exist yet there are simply
/// no existing rows (an empty, not missing, snapshot).
fn build_conflict_scan_state_in(
    wtx: &WriteTransaction,
    table: &str,
    width: usize,
    schema: &TableSchema,
    unique_cols: &[usize],
    composite_cols: &[Vec<usize>],
) -> Result<ConflictScanState, String> {
    let mut state = ConflictScanState {
        existing: Vec::new(),
        row_slot: HashMap::new(),
        unique_rows: (0..unique_cols.len()).map(|_| HashMap::new()).collect(),
        composite_rows: (0..composite_cols.len()).map(|_| HashMap::new()).collect(),
    };
    let Ok(rows_t) = wtx.open_table(ROWS) else {
        return Ok(state);
    };
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (k, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        let rowid = k.value().1;
        record_existing_row_in_conflict_index(
            rowid,
            cells,
            schema,
            unique_cols,
            composite_cols,
            &mut state,
        );
    }
    Ok(state)
}

/// Records one already-committed row (`rowid`, `cells`) into the ON
/// CONFLICT lookup snapshot: its slot in `existing`, and its unique/
/// composite key entries (first row wins a duplicate key, mirroring the
/// pre-extraction scan -- existing corruption is still rejected by the
/// authoritative final `validate_uniqueness_in` pass).
fn record_existing_row_in_conflict_index(
    rowid: u64,
    cells: Vec<Cell>,
    schema: &TableSchema,
    unique_cols: &[usize],
    composite_cols: &[Vec<usize>],
    state: &mut ConflictScanState,
) {
    state.row_slot.insert(rowid, state.existing.len());
    for (slot, &column) in unique_cols.iter().enumerate() {
        if let Some(key) = unique_cell_key(&cells[column], schema.columns()[column].ty) {
            state.unique_rows[slot].entry(key).or_insert(rowid);
        }
    }
    for (slot, columns) in composite_cols.iter().enumerate() {
        if let Some(key) = composite_cell_key(&cells, columns, schema) {
            state.composite_rows[slot].entry(key).or_insert(rowid);
        }
    }
    state.existing.push((rowid, cells));
}

/// Same insertion shape as `record_existing_row_in_conflict_index`, for a
/// row that was just written by this same `insert_on_conflict_in` call
/// (fresh insert or DO UPDATE) rather than found in the pre-scan.
fn insert_row_into_conflict_index(
    rowid: u64,
    cells: &[Cell],
    schema: &TableSchema,
    unique_cols: &[usize],
    composite_cols: &[Vec<usize>],
    state: &mut ConflictScanState,
) {
    for (map, &column) in state.unique_rows.iter_mut().zip(unique_cols) {
        if let Some(key) = unique_cell_key(&cells[column], schema.columns()[column].ty) {
            map.entry(key).or_insert(rowid);
        }
    }
    for (map, columns) in state.composite_rows.iter_mut().zip(composite_cols) {
        if let Some(key) = composite_cell_key(cells, columns, schema) {
            map.entry(key).or_insert(rowid);
        }
    }
}

/// Inverse of `insert_row_into_conflict_index`: removes `rid`'s prior
/// key entries before a DO UPDATE overwrites them, so a changed unique
/// value cannot leave a stale directory entry pointing at the same row.
fn remove_row_from_conflict_index(
    rid: u64,
    cells: &[Cell],
    schema: &TableSchema,
    unique_cols: &[usize],
    composite_cols: &[Vec<usize>],
    state: &mut ConflictScanState,
) {
    for (map, &column) in state.unique_rows.iter_mut().zip(unique_cols) {
        if let Some(key) = unique_cell_key(&cells[column], schema.columns()[column].ty) {
            if map.get(&key) == Some(&rid) {
                map.remove(&key);
            }
        }
    }
    for (map, columns) in state.composite_rows.iter_mut().zip(composite_cols) {
        if let Some(key) = composite_cell_key(cells, columns, schema) {
            if map.get(&key) == Some(&rid) {
                map.remove(&key);
            }
        }
    }
}

/// Whether `row` conflicts with an existing row on a single-column UNIQUE/PK
/// value, then (if not) on a composite UNIQUE/PK value. `Ok(None)` means no
/// conflict: `row` is a fresh insert.
fn detect_conflict_rowid(
    row: &[Value],
    target_position_by_col: &[Option<usize>],
    schema: &TableSchema,
    unique_cols: &[usize],
    composite_cols: &[Vec<usize>],
    state: &ConflictScanState,
) -> Result<Option<u64>, String> {
    if let Some(rowid) =
        detect_unique_conflict(row, target_position_by_col, schema, unique_cols, state)?
    {
        return Ok(Some(rowid));
    }
    detect_composite_conflict(row, target_position_by_col, schema, composite_cols, state)
}

fn detect_unique_conflict(
    row: &[Value],
    target_position_by_col: &[Option<usize>],
    schema: &TableSchema,
    unique_cols: &[usize],
    state: &ConflictScanState,
) -> Result<Option<u64>, String> {
    // Coerce the supplied unique-column values to detect a conflict.
    for (slot, &uci) in unique_cols.iter().enumerate() {
        // The value this row supplies for the unique column (if any).
        let Some(pos) = target_position_by_col[uci] else {
            continue;
        };
        let col = &schema.columns()[uci];
        let supplied = Cell::coerce(&row[pos], col.ty, col.nullable)?;
        if let Some(key) = unique_cell_key(&supplied, col.ty) {
            if let Some(&rowid) = state.unique_rows[slot].get(&key) {
                return Ok(Some(rowid));
            }
        }
    }
    Ok(None)
}

fn detect_composite_conflict(
    row: &[Value],
    target_position_by_col: &[Option<usize>],
    schema: &TableSchema,
    composite_cols: &[Vec<usize>],
    state: &ConflictScanState,
) -> Result<Option<u64>, String> {
    // Composite conflict detection is possible when all key columns are
    // explicitly supplied. Omitted columns are left to build_insert_cells
    // and the final uniqueness pass, preserving DEFAULT/SERIAL behavior.
    for (slot, columns) in composite_cols.iter().enumerate() {
        let mut supplied = Vec::with_capacity(columns.len());
        let mut complete = true;
        for &column in columns {
            let Some(pos) = target_position_by_col[column] else {
                complete = false;
                break;
            };
            let col = &schema.columns()[column];
            let value = Cell::coerce(&row[pos], col.ty, col.nullable)?;
            let Some(key) = unique_cell_key(&value, col.ty) else {
                complete = false;
                break;
            };
            supplied.push(key);
        }
        if complete {
            let key = serde_json::to_string(&supplied).expect("Vec<String> is serializable");
            if let Some(&rowid) = state.composite_rows[slot].get(&key) {
                return Ok(Some(rowid));
            }
        }
    }
    Ok(None)
}

/// Encodes `cells` and writes them at `(table, rowid)`, enforcing the
/// stored-value size limit shared by every write path in this module.
fn write_conflict_row(
    rows_t: &mut redb::Table<'_, (&'static str, u64), &'static [u8]>,
    table: &str,
    rowid: u64,
    cells: &[Cell],
) -> Result<(), String> {
    let blob = rmp_serde::to_vec_named(cells).map_err(|e| format!("encode row: {e}"))?;
    if blob.len() > MAX_SQL_STORED_VALUE_BYTES {
        return Err("encoded SQL row exceeds storage value limit".to_string());
    }
    rows_t
        .insert((table, rowid), blob.as_slice())
        .map_err(map_err)?;
    Ok(())
}

fn apply_do_update_assignments(
    table: &str,
    schema: &TableSchema,
    set: &serde_json::Map<String, Value>,
    cells: &mut [Cell],
) -> Result<(), String> {
    for (col, val) in set {
        validate_mutation_value(val)?;
        let idx = schema
            .column_index(col)
            .ok_or_else(|| format!("column `{col}` does not exist in table `{table}`"))?;
        let c = &schema.columns()[idx];
        cells[idx] = Cell::coerce(val, c.ty, c.nullable)?;
    }
    Ok(())
}

fn validate_do_update_check_constraints(
    schema: &TableSchema,
    cells: &[Cell],
) -> Result<(), String> {
    for (ci, col) in schema.columns().iter().enumerate() {
        if let Some(check) = &col.check {
            if !check.holds(&cells[ci].to_typed_json(col.ty)) {
                return Err(format!(
                    "updated row violates CHECK constraint on column `{}`",
                    col.name
                ));
            }
        }
    }
    Ok(())
}

/// Merges a `DO UPDATE SET ...` conflict resolution into the existing row
/// `rid`: removes its stale directory entries, applies the SET
/// assignments, re-checks CHECK constraints, re-indexes, and writes the
/// row. Returns `(old_cells, new_cells)` for the caller's `affected`/
/// `index_changes` bookkeeping.
fn apply_conflict_do_update(
    rows_t: &mut redb::Table<'_, (&'static str, u64), &'static [u8]>,
    spec: &ConflictRowSpec<'_>,
    rid: u64,
    set: &serde_json::Map<String, Value>,
    state: &mut ConflictScanState,
) -> Result<(Vec<Cell>, Vec<Cell>), String> {
    let ConflictRowSpec {
        table,
        schema,
        unique_cols,
        composite_cols,
        ..
    } = *spec;
    let index = *state.row_slot.get(&rid).expect("conflict rowid present");
    let old_cells = state.existing[index].1.clone();
    remove_row_from_conflict_index(rid, &old_cells, schema, unique_cols, composite_cols, state);
    let mut new_cells = old_cells.clone();
    apply_do_update_assignments(table, schema, set, &mut new_cells)?;
    validate_do_update_check_constraints(schema, &new_cells)?;
    insert_row_into_conflict_index(rid, &new_cells, schema, unique_cols, composite_cols, state);
    write_conflict_row(rows_t, table, rid, &new_cells)?;
    state.existing[index].1 = new_cells.clone();
    Ok((old_cells, new_cells))
}

/// A fresh insert (no conflict): allocate one rowid, build + write the row,
/// and index it. Returns the new `(rowid, cells)` for the caller's
/// `affected`/`index_changes` bookkeeping.
fn apply_conflict_fresh_insert(
    wtx: &WriteTransaction,
    rows_t: &mut redb::Table<'_, (&'static str, u64), &'static [u8]>,
    spec: &ConflictRowSpec<'_>,
    row: &[Value],
    state: &mut ConflictScanState,
) -> Result<(u64, Vec<Cell>), String> {
    let ConflictRowSpec {
        table,
        col_order,
        targets,
        schema,
        unique_cols,
        composite_cols,
        ..
    } = *spec;
    let rowid = alloc_rowids(wtx, table, 1)?;
    let cells = build_insert_cells(schema, col_order, targets, row, rowid)?;
    write_conflict_row(rows_t, table, rowid, &cells)?;
    state.row_slot.insert(rowid, state.existing.len());
    insert_row_into_conflict_index(rowid, &cells, schema, unique_cols, composite_cols, state);
    state.existing.push((rowid, cells.clone()));
    Ok((rowid, cells))
}

/// Processes one input row of an `INSERT ... ON CONFLICT`: validates its
/// values, detects a conflict, and dispatches to skip (DO NOTHING),
/// `apply_conflict_do_update` (DO UPDATE), or `apply_conflict_fresh_insert`
/// (no conflict) -- pushing the outcome into `out`.
fn process_insert_on_conflict_row(
    wtx: &WriteTransaction,
    spec: &ConflictRowSpec<'_>,
    action: &ConflictAction,
    row: &[Value],
    rows_t: &mut redb::Table<'_, (&'static str, u64), &'static [u8]>,
    state: &mut ConflictScanState,
    out: &mut ConflictInsertOutcome,
) -> Result<(), String> {
    for value in row {
        validate_mutation_value(value)?;
    }
    let conflict_rowid = detect_conflict_rowid(
        row,
        spec.target_position_by_col,
        spec.schema,
        spec.unique_cols,
        spec.composite_cols,
        state,
    )?;
    match (conflict_rowid, action) {
        (Some(_), ConflictAction::DoNothing) => { /* skip */ }
        (Some(rid), ConflictAction::DoUpdate(set)) => {
            let (old_cells, new_cells) = apply_conflict_do_update(rows_t, spec, rid, set, state)?;
            out.affected.push(new_cells.clone());
            out.index_changes
                .push((rid, Some(old_cells), Some(new_cells)));
        }
        (None, _) => {
            let (rowid, cells) = apply_conflict_fresh_insert(wtx, rows_t, spec, row, state)?;
            out.affected.push(cells.clone());
            out.index_changes.push((rowid, None, Some(cells)));
        }
    }
    Ok(())
}

/// Post-loop bookkeeping shared by every `insert_on_conflict_in` call:
/// secondary-index maintenance for every changed row, then the
/// authoritative uniqueness pass, then per-row CHECK/FOREIGN KEY
/// validation (CONCEPT:EG-KG.query.table-schema-constraints/NE-001 --
/// fresh insert AND DO UPDATE merges both land in `affected`).
fn finalize_insert_on_conflict(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    schema: &TableSchema,
    index_changes: &[IndexChange],
    affected: &[Vec<Cell>],
) -> Result<(), String> {
    for (rowid, old, new) in index_changes {
        maintain_secondary_row_in(
            wtx,
            tenant_scope,
            table,
            schema,
            *rowid,
            old.as_deref(),
            new.as_deref(),
        )?;
    }
    validate_uniqueness_in(wtx, table, schema)?;
    for cells in affected {
        validate_row_constraints_in(wtx, schema, cells)?;
    }
    Ok(())
}

/// Canonical key used by the existing uniqueness validator, factored so the
/// ON-CONFLICT lookup directory and the final integrity pass cannot drift. SQL
/// NULL is deliberately absent because UNIQUE permits multiple NULL values.
fn unique_cell_key(cell: &Cell, ty: ColumnType) -> Option<String> {
    let value = cell.to_typed_json(ty);
    (!value.is_null()).then(|| value.to_string())
}

/// Canonical key for a composite PK/UNIQUE tuple used by the bounded ON CONFLICT
/// directory. JSON tuple encoding keeps tenant-provided delimiters from changing
/// key boundaries; NULL in any member preserves SQL's NULL-exempt uniqueness rule.
fn composite_cell_key(cells: &[Cell], columns: &[usize], schema: &TableSchema) -> Option<String> {
    let parts = columns
        .iter()
        .map(|&column| unique_cell_key(&cells[column], schema.columns()[column].ty))
        .collect::<Option<Vec<_>>>()?;
    serde_json::to_string(&parts).ok()
}

/// Build a `col -> json` row map for predicate evaluation (CONCEPT:EG-KG.query.compound-predicate-decode): one
/// entry per schema column, the cell decoded to its JSON value. A column the
/// predicate references that is NOT in the schema is simply absent (reads as NULL).
fn row_map(schema: &TableSchema, cells: &[Cell]) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::with_capacity(schema.columns().len());
    for (ci, col) in schema.columns().iter().enumerate() {
        let cell = cells.get(ci).cloned().unwrap_or(Cell::Null);
        map.insert(col.name.clone(), cell.to_typed_json(col.ty));
    }
    map
}

/// Resolve an UPDATE's `SET` map into `(column index, coerced cell)` pairs,
/// validating each supplied value against the column's declared type/nullability.
fn resolve_update_assignments(
    schema: &TableSchema,
    table: &str,
    set: &serde_json::Map<String, Value>,
) -> Result<Vec<(usize, Cell)>, String> {
    let mut assigns: Vec<(usize, Cell)> = Vec::with_capacity(set.len());
    for (col, val) in set {
        validate_mutation_value(val)?;
        let idx = schema
            .column_index(col)
            .ok_or_else(|| format!("column `{col}` does not exist in table `{table}`"))?;
        let c = &schema.columns()[idx];
        assigns.push((idx, Cell::coerce(val, c.ty, c.nullable)?));
    }
    Ok(assigns)
}

/// Per-column CHECK constraints on a row an UPDATE has just rewritten.
fn validate_updated_row_checks(schema: &TableSchema, cells: &[Cell]) -> Result<(), String> {
    for (ci, col) in schema.columns().iter().enumerate() {
        let Some(check) = &col.check else {
            continue;
        };
        if !check.holds(&cells[ci].to_typed_json(col.ty)) {
            return Err(format!(
                "updated row violates CHECK constraint on column `{}`",
                col.name
            ));
        }
    }
    Ok(())
}

/// CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — everything an UPDATE owes AFTER its row writes are
/// staged: secondary-index maintenance, uniqueness, then table-level CHECK +
/// outgoing FOREIGN KEY and the parent-side referential action for every OTHER
/// table whose FK references a column this UPDATE changed
/// (NO ACTION/RESTRICT/CASCADE/SET NULL).
fn finish_update_pass_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    table: &str,
    schema: &TableSchema,
    changed: &[(u64, Vec<Cell>, Vec<Cell>)],
) -> Result<(), String> {
    for (rowid, old_cells, new_cells) in changed {
        maintain_secondary_row_in(
            wtx,
            tenant_scope,
            table,
            schema,
            *rowid,
            Some(old_cells),
            Some(new_cells),
        )?;
    }
    validate_uniqueness_in(wtx, table, schema)?;
    let mut visited = HashSet::new();
    for (rowid, old_cells, new_cells) in changed {
        validate_row_constraints_in(wtx, schema, new_cells)?;
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
    Ok(())
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
    let assigns = resolve_update_assignments(&schema, table, set)?;
    let width = schema.columns().len();
    let mut updated: Vec<Vec<Cell>> = Vec::new();
    // CONCEPT:EG-KG.query.table-schema-constraints/NE-001 — `(rowid, old_cells, new_cells)` for the parent-side
    // referential-action pass AFTER the write is staged.
    let mut changed: Vec<(u64, Vec<Cell>, Vec<Cell>)> = Vec::new();
    {
        let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
        let mut hits: Vec<(u64, Vec<Cell>)> = Vec::new();
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        for r in rows_t
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (k, v) = r.map_err(map_err)?;
            account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
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
            validate_updated_row_checks(&schema, &cells)?;
            let blob = rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?;
            if blob.len() > MAX_SQL_STORED_VALUE_BYTES {
                return Err("encoded SQL row exceeds storage value limit".to_string());
            }
            rows_t
                .insert((table, rowid), blob.as_slice())
                .map_err(map_err)?;
            changed.push((rowid, old_cells, cells.clone()));
            updated.push(cells);
        }
    }
    finish_update_pass_in(wtx, tenant_scope, table, &schema, &changed)?;
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
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (k, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
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
        maintain_secondary_row_in(wtx, tenant_scope, table, &schema, *rowid, Some(cells), None)?;
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

/// Every uniqueness group to enforce for `table`: one per PK/UNIQUE-flagged
/// column, PLUS one per multi-column PK/UNIQUE table-level constraint
/// (CONCEPT:EG-KG.query.table-schema-constraints/NE-001), each paired with the label its violation message
/// uses. Groups are checked independently — a composite key is the JOIN of its
/// columns' individual coerced-value keys.
fn unique_check_groups(table: &str, schema: &TableSchema) -> Vec<(Vec<usize>, String)> {
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
    groups
}

/// The structural key for ONE uniqueness group on ONE row, or `None` when any
/// participating cell is NULL — which exempts the row from THAT group, mirroring
/// single-column UNIQUE's NULL exemption and Postgres's multi-column semantics.
fn composite_unique_key(schema: &TableSchema, idxs: &[usize], cells: &[Cell]) -> Option<String> {
    let mut parts: Vec<String> = Vec::with_capacity(idxs.len());
    for &ci in idxs {
        parts.push(unique_cell_key(&cells[ci], schema.columns()[ci].ty)?);
    }
    // Encode the tuple boundary structurally; concatenating with a delimiter
    // lets a tenant value containing that delimiter collide with a different
    // composite key.
    Some(serde_json::to_string(&parts).expect("Vec<String> is serializable"))
}

/// Check ONE decoded row against every uniqueness group, recording the keys it
/// occupies in `seen`.
fn check_row_uniqueness(
    schema: &TableSchema,
    groups: &[(Vec<usize>, String)],
    seen: &mut [HashSet<String>],
    cells: &[Cell],
) -> Result<(), String> {
    for (slot, (idxs, label)) in groups.iter().enumerate() {
        let Some(key) = composite_unique_key(schema, idxs, cells) else {
            continue;
        };
        if !seen[slot].insert(key) {
            return Err(format!(
                "duplicate key value violates unique constraint on {label}"
            ));
        }
    }
    Ok(())
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
    let groups = unique_check_groups(table, schema);
    if groups.is_empty() {
        return Ok(());
    }
    let width = schema.columns().len();
    let rows_t = wtx.open_table(ROWS).map_err(map_err)?;
    let mut seen: Vec<HashSet<String>> = vec![HashSet::new(); groups.len()];
    let mut scanned_rows = 0usize;
    let mut scanned_bytes = 0usize;
    for r in rows_t
        .range((table, 0u64)..=(table, u64::MAX))
        .map_err(map_err)?
    {
        let (_, v) = r.map_err(map_err)?;
        account_collection(&mut scanned_rows, &mut scanned_bytes, v.value().len())?;
        let mut cells: Vec<Cell> = decode_stored(v.value(), "row")?;
        if cells.len() < width {
            cells.resize(width, Cell::Null);
        }
        check_row_uniqueness(schema, &groups, &mut seen, &cells)?;
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

        // A rebuilt retry commonly observes the incremented SQL-domain version.
        // The durable record must retain the original observation while returning
        // the stored result instead of executing CREATE TABLE a second time.
        let mut rederived = batch.clone();
        rederived.expected_graph_version = Some(1);
        let replay = reopened
            .commit_txn_batch(&create_metrics_txn(), &rederived, 103)
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.record.batch.expected_graph_version, Some(0));

        // Same key plus a changed operation is not a retry, even though its
        // expected version is the current derived value.
        let mut conflict = rederived.clone();
        conflict.operations[0].method = eg_types::protocol::Method::ApplyMutation {
            event_type: "sql_catalog_operation".to_string(),
            query: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
        };
        let error = reopened
            .commit_txn_batch(&create_metrics_txn(), &conflict, 104)
            .unwrap_err();
        assert!(error.contains("IDEMPOTENCY_CONFLICT"));
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
