//! SQLite `.db` FILE import/export handler (CONCEPT:EG-KG.query.eg-feature / CONCEPT:EG-KG.query.full-protocol) — the
//! documented EG-075 follow-up: reading/writing a real on-disk `sqlite3` `.db` FILE,
//! distinct from the `sqlite-wire` NDJSON dialect surface (which speaks SQLite SQL over
//! a socket but never touches a `.db` file).
//!
//! ## Pure-Rust, no C sqlite
//! Both halves go through `eg-sqlite-format`, a from-scratch SQLite page-format reader +
//! bulk-load writer (no rusqlite, no libsqlite3-sys, no C toolchain). The export half
//! produces a `.db` a stock `sqlite3` CLI can open — a spec-correct b-tree page file whose
//! output passes a real `sqlite3 PRAGMA integrity_check` (see the differential test below).
//! This falsifies the old "a spec-correct pure-Rust SQLite writer is infeasible" blocker.
//!
//! ## What moves
//! Rows flow between the `.db` file and the caller's owner-scoped user-table store
//! (`eg_query::TableStore`, behind `query`) — the SAME durable store the `Method::Sql`
//! DDL/DML path and the pgwire shim write, so a table imported here is immediately
//! visible to `SELECT … FROM <table>` over every SQL surface. Both ops are BATCH — ONE
//! engine round-trip reads/writes the whole file (import: one `insert_rows` per table;
//! export: one `scan` per table), never per-row over the wire.
//!
//! ## Type mapping (dynamic ↔ typed)
//! SQLite is dynamically typed; the engine store is statically typed. On IMPORT each
//! column's declared type is mapped by SQLite affinity to an `eg_query::ColumnType`
//! (INTEGER→BigInt, REAL→Double, TEXT→Text, BLOB/none→Bytes, numeric→Double) and every
//! column is imported NULLABLE + non-PK so exact stored values pass through the store's
//! coercion unchanged (constraints are not mirrored — VALUES are). On EXPORT the inverse
//! map picks a SQLite declared type per column so a re-import round-trips.

use eg_query::{Cell, Column, ColumnType, TableSchema, TableStore, TableTxn, TxnOp};
use eg_sqlite_format::{ColumnDef as SqliteColumnDef, Reader, Value as SqliteValue, Writer};
use serde_json::Value as JsonValue;
use std::path::{Path, PathBuf};

use crate::mutation_batch::{DurabilityDomain, MutationBatch, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use eg_types::contract::Nonce;
use eg_types::result_contract::storage as results;
use eg_types::storage_wire::{
    SqliteExportDestination, SqliteExportReport, SqliteImportReport, SqliteImportSource,
    SqliteTableRows,
};

mod transfer_fs;

const SQLITE_MAX_BYTES_ENV: &str = "EPISTEMIC_GRAPH_SQLITE_MAX_BYTES";
const SQLITE_MAX_ROWS_ENV: &str = "EPISTEMIC_GRAPH_SQLITE_MAX_ROWS";
const DEFAULT_SQLITE_MAX_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CONFIGURED_SQLITE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const DEFAULT_SQLITE_MAX_ROWS: u64 = 1_000_000;
const MAX_CONFIGURED_SQLITE_ROWS: u64 = 100_000_000;
const MAX_SQLITE_TABLES: usize = 4_096;
const MAX_SQLITE_COLUMNS: usize = 2_048;

fn bounded_env_u64(name: &str, default: u64, maximum: u64) -> Result<u64, String> {
    let Some(raw) = nonempty_env_value(name) else {
        return Ok(default);
    };
    let raw = raw.to_string_lossy();
    let value = raw
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an integer"))?;
    if value == 0 || value > maximum {
        return Err(format!("{name} must be between 1 and {maximum}"));
    }
    Ok(value)
}

fn nonempty_env_value(name: &str) -> Option<std::ffi::OsString> {
    let value = std::env::var_os(name)?;
    (!value.to_string_lossy().trim().is_empty()).then_some(value)
}

fn sqlite_limits() -> Result<(u64, u64), String> {
    Ok((
        bounded_env_u64(
            SQLITE_MAX_BYTES_ENV,
            DEFAULT_SQLITE_MAX_BYTES,
            MAX_CONFIGURED_SQLITE_BYTES,
        )?,
        bounded_env_u64(
            SQLITE_MAX_ROWS_ENV,
            DEFAULT_SQLITE_MAX_ROWS,
            MAX_CONFIGURED_SQLITE_ROWS,
        )?,
    ))
}

/// The configured served-engine persistence directory, or the SAME error
/// `sql_tables::user_table_store`/`tenant_table_store` themselves report when it
/// is unset (CONCEPT:NE-046 — EG-WIRE-CATALOG).
async fn tenant_persist_dir(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
) -> Result<PathBuf, String> {
    state
        .read()
        .await
        .persist_dir
        .clone()
        .map(PathBuf::from)
        .ok_or_else(|| {
            "owner-scoped SQL catalog requires the configured persistence directory".to_string()
        })
}

/// Route the two SQLite-file methods. Resolves the caller's TENANT-shared
/// user-table store, then runs the (blocking, file-I/O) import/export on the
/// blocking pool so the reactor is never stalled. `Err(method)` for a method
/// that isn't ours (unreachable — dispatch only routes the two variants here).
///
/// CONCEPT:NE-046 (EG-WIRE-CATALOG) — both methods are gated `require_admin`
/// above, and `sql_catalog_acl::authorize` already treats an admin carrier as an
/// unconditional bypass (ownership/grants AND the row-level predicate — an admin
/// import/export is a bulk backup/restore tool, not a scoped query, so it is
/// deliberately NOT routed through `AuthorizedTable`, which would RLS-filter an
/// admin's own export down to just their own rows). What DOES change here: the
/// physical store resolves directly to the TENANT-shared catalog, so an imported table is
/// immediately visible/joinable to every other tenant member with a grant on it
/// — and import registers ownership of each table it creates, so a subsequent
/// non-admin `GRANT`/`REVOKE` on it works.
pub(crate) async fn try_handle(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: Method,
) -> Result<Response, Method> {
    let admitted = admit_sqlite_file_method(authority, req_id);
    let Ok(()) = admitted else {
        return *admitted.err().unwrap();
    };
    let original_method = method.clone();
    match method {
        Method::ImportSqliteFile { path } => Ok(handle_import_sqlite_file(
            state,
            req_id,
            authority,
            attempt_nonce,
            &original_method,
            path,
        )
        .await),
        Method::ExportSqliteFile { path, tables } => {
            Ok(handle_export_sqlite_file(state, req_id, authority, &path, &tables).await)
        }
        other => Err(other),
    }
}

/// Gate both SQLite-file methods on `require_admin`; `Err` is the final
/// routing/error outcome `try_handle` should return as-is.
fn admit_sqlite_file_method(
    authority: &CarrierAuthority,
    req_id: u64,
) -> Result<(), Box<Result<Response, Method>>> {
    if let Err(error) = authority.require_admin("SQLite user-table import/export") {
        return Err(Box::new(Ok(Response::err(req_id, error))));
    }
    Ok(())
}

/// Resolve the persistence directory plus a caller-owned authority clone shared by both
/// import and export before they build their owned blocking job, or the final
/// `Response::err` outcome when the persist dir isn't configured (same error
/// [`tenant_persist_dir`] itself reports).
async fn resolve_transfer_context(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
) -> Result<(PathBuf, CarrierAuthority), Response> {
    let persist_dir = match tenant_persist_dir(state).await {
        Ok(dir) => dir,
        Err(e) => return Err(Response::err(req_id, e)),
    };
    let owner_authority = authority.clone();
    Ok((persist_dir, owner_authority))
}

async fn handle_import_sqlite_file(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    original_method: &Method,
    path: String,
) -> Response {
    let (persist_dir, owner_authority) =
        match resolve_transfer_context(state, req_id, authority).await {
            Ok(pair) => pair,
            Err(response) => return response,
        };
    let original_method = original_method.clone();
    // Sample replicated authoritative time on the reactor while its task-local
    // apply scope is available, then move the complete filesystem/catalog
    // lifecycle into one owned blocking job.
    let now = crate::server::dispatch::authoritative_now_ms();
    let out = run_transfer_job("import", move || {
        import_sqlite_lifecycle_with_nonce(
            req_id,
            &owner_authority,
            attempt_nonce,
            &original_method,
            &path,
            &persist_dir,
            now,
        )
    })
    .await;
    match out {
        Ok(report) => Response::ok(
            req_id,
            ResultPayload::of::<results::ImportSqliteFile>(report),
        ),
        Err(e) => Response::err(req_id, e),
    }
}

async fn handle_export_sqlite_file(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    path: &str,
    tables: &[String],
) -> Response {
    let (persist_dir, owner_authority) =
        match resolve_transfer_context(state, req_id, authority).await {
            Ok(pair) => pair,
            Err(response) => return response,
        };
    let path = path.to_string();
    let tables = tables.to_vec();
    let out = run_transfer_job("export", move || {
        export_sqlite_lifecycle(&owner_authority, &path, &tables, &persist_dir)
    })
    .await;
    match out {
        Ok(report) => Response::ok(
            req_id,
            ResultPayload::of::<results::ExportSqliteFile>(report),
        ),
        Err(e) => Response::err(req_id, e),
    }
}

/// Run one owned lifecycle to durable commit/atomic install despite waiter cancellation.
/// Panic join details stay private because they can contain host paths or source data.
async fn run_transfer_job<T, F>(operation: &'static str, job: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(job)
        .await
        .map_err(|_| format!("SQLite {operation} task failed"))?
}

fn import_sqlite_lifecycle_with_nonce(
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
    logical_path: &str,
    persist_dir: &Path,
    now: u64,
) -> Result<SqliteImportReport, String> {
    crate::server::sql_catalog_acl::require_source_authority()?;
    crate::server::sql_catalog_acl::with_source_authority_write(persist_dir, authority, |source| {
        let store =
            crate::server::sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
        let batch =
            compile_import_batch_with_nonce(&store, req_id, authority, method, now, attempt_nonce)?;
        if let Some(committed_report) =
            store.probe_txn_batch_replay(&batch, decode_committed_import_report)?
        {
            register_import_owners(source, &batch.batch_id, &committed_report)?;
            return Ok(committed_report);
        }
        let reader = transfer_fs::open_import(logical_path, sqlite_limits()?.0)?;
        let (txn, report) = prepare_sqlite_import(&reader)?;
        let result = rmp_serde::to_vec_named(&report).map_err(|e| e.to_string())?;
        let committed = store.commit_txn_batch_result(&txn, &batch, result, now)?;
        let committed_report = decode_committed_import_report(&committed)?;
        register_import_owners(source, &batch.batch_id, &committed_report)?;
        Ok(committed_report)
    })
}

fn export_sqlite_lifecycle(
    authority: &CarrierAuthority,
    logical_path: &str,
    tables: &[String],
    persist_dir: &Path,
) -> Result<SqliteExportReport, String> {
    crate::server::sql_catalog_acl::require_source_authority()?;
    export_sqlite_file(
        &crate::server::sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?,
        &transfer_fs::export_destination(logical_path)?,
        tables,
    )
}

fn decode_committed_import_report(
    committed: &eg_types::mutation_batch::MutationBatchCommit,
) -> Result<SqliteImportReport, String> {
    let bytes = committed
        .record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed SQLite import batch has no result".to_string())?;
    let report: SqliteImportReport = eg_types::msgpack::decode_property_value(bytes)
        .ok()
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| "committed SQLite import batch has an invalid result".to_string())?;
    validated_import_tables(&report)?;
    Ok(report)
}

fn compile_import_batch(
    store: &TableStore,
    req_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
    now: u64,
) -> Result<MutationBatch, String> {
    compile_import_batch_with_nonce(store, req_id, authority, method, now, None)
}

fn compile_import_batch_with_nonce(
    store: &TableStore,
    req_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
    now: u64,
    attempt_nonce: Option<Nonce>,
) -> Result<MutationBatch, String> {
    let scope = authority.namespace("sqlite-import", "global-user-tables");
    crate::server::sql_tables::SqlOwnerMutation {
        authority,
        kind: "sqlite-import",
        scope: &scope,
        request_id: req_id,
        attempt_nonce,
        created_at_ms: now,
    }
    .compile(store, |context| {
        crate::server::mutation_batch::compile_opaque_method(
            context,
            method,
            MutationSurface::Query,
            DurabilityDomain::SqlCatalog,
            "sqlite_import",
        )
    })
}

/// Register ownership from the durable result on both the fresh-commit and replay
/// paths. Registration is idempotent, so a retry repairs a crash or failure that
/// happened after the table transaction committed but before ACL registration.
fn register_import_owners(
    source: &crate::server::sql_catalog_acl::SqlSourceAuthorityWrite<'_, '_>,
    parent_operation: &str,
    report: &SqliteImportReport,
) -> Result<(), String> {
    for table in validated_import_tables(report)? {
        crate::server::sql_catalog_acl::register_owner_after_create_in(
            source,
            table,
            crate::server::sql_catalog_acl::stable_source_operation_id(parent_operation),
        )?;
    }
    Ok(())
}

fn validated_import_tables(report: &SqliteImportReport) -> Result<Vec<&str>, String> {
    let invalid = || "committed SQLite import batch has an invalid result".to_string();
    if report.imported_tables.len() > MAX_SQLITE_TABLES {
        return Err(invalid());
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut names = Vec::with_capacity(report.imported_tables.len());
    for item in &report.imported_tables {
        let table = item.table.as_str();
        if table.is_empty() || !seen.insert(table) {
            return Err(invalid());
        }
        names.push(table);
    }
    Ok(names)
}

// ── Import (CONCEPT:EG-KG.query.eg-feature) ───────────────────────────────────────────────────

fn prepare_sqlite_import(reader: &Reader) -> Result<(TableTxn, SqliteImportReport), String> {
    let tables = list_user_tables(reader)?;
    let (_, max_rows) = sqlite_limits()?;
    if tables.len() > MAX_SQLITE_TABLES {
        return Err("SQLite import contains too many tables".to_string());
    }
    let mut txn = TableTxn::new();
    let mut report = Vec::with_capacity(tables.len());
    let mut total_rows = 0u64;
    for table in &tables {
        let (schema, col_order) = import_schema(reader, table)?;
        let row_count = sqlite_table_row_count(reader, table)?;
        total_rows = total_rows
            .checked_add(row_count)
            .ok_or_else(|| "SQLite import row count overflow".to_string())?;
        if total_rows > max_rows {
            return Err("SQLite import exceeds the configured row limit".to_string());
        }
        let rows = import_rows(reader, table, &schema)?;
        if rows.len() as u64 != row_count {
            return Err("SQLite import changed while it was being read".to_string());
        }
        txn.push(TxnOp::DropTable {
            name: table.clone(),
            if_exists: true,
        });
        txn.push(TxnOp::CreateTable {
            schema,
            if_not_exists: false,
        });
        if !rows.is_empty() {
            txn.push(TxnOp::Insert {
                table: table.clone(),
                col_order,
                rows,
            });
        }
        report.push(SqliteTableRows {
            table: table.clone(),
            rows: row_count,
        });
    }
    Ok((
        txn,
        SqliteImportReport {
            source: SqliteImportSource::Sqlite,
            imported_tables: report,
        },
    ))
}

fn sqlite_table_row_count(reader: &Reader, table: &str) -> Result<u64, String> {
    reader
        .table_row_count(table)
        .map_err(|_| "count SQLite import rows failed".to_string())
}

/// The user tables in a `.db` (skip `sqlite_*` internal tables), sorted for determinism.
/// The pure-Rust [`Reader`] applies the same filter/ordering the old `sqlite_master`
/// query did (`type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).
fn list_user_tables(reader: &Reader) -> Result<Vec<String>, String> {
    reader
        .list_tables()
        .map_err(|e| format!("read sqlite schema: {e}"))
}

/// Build the engine [`TableSchema`] for a `.db` table from its stored `CREATE TABLE`
/// columns (every column NULLABLE + non-PK — values, not constraints, are mirrored).
/// Returns the schema and the column-name order for the batch insert.
fn import_schema(reader: &Reader, table: &str) -> Result<(TableSchema, Vec<String>), String> {
    let cols = reader
        .table_columns(table)
        .map_err(|e| format!("table columns({table}): {e}"))?;

    let mut columns = Vec::new();
    let mut names = Vec::new();
    for col in cols {
        columns.push(Column::new(
            col.name.clone(),
            affinity_to_type(&col.decl_type),
            true,
            false,
        ));
        names.push(col.name);
    }
    if columns.is_empty() {
        return Err(format!("sqlite table `{table}` has no columns"));
    }
    if columns.len() > MAX_SQLITE_COLUMNS {
        return Err("SQLite table contains too many columns".to_string());
    }
    Ok((TableSchema::new(table, columns), names))
}

/// Map a SQLite declared type to an engine [`ColumnType`] by SQLite affinity rules
/// (the rule ORDER matters — INT before CHAR/TEXT before BLOB/empty before REAL).
fn affinity_to_type(decl: &str) -> ColumnType {
    let d = decl.to_ascii_uppercase();
    if d.contains("INT") {
        ColumnType::BigInt
    } else if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
        ColumnType::Text
    } else if d.contains("BLOB") || d.trim().is_empty() {
        ColumnType::Bytes
    } else if d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB") {
        ColumnType::Double
    } else {
        // NUMERIC affinity (DECIMAL/NUMERIC/BOOLEAN/DATE…) — hold as a float.
        ColumnType::Double
    }
}

/// Read every row of `table`, converting each SQLite value to the JSON shape the
/// target column's [`ColumnType`] coerces from cleanly.
fn import_rows(
    reader: &Reader,
    table: &str,
    schema: &TableSchema,
) -> Result<Vec<Vec<JsonValue>>, String> {
    let ncols = schema.columns().len();
    let raw = reader
        .scan_table(table)
        .map_err(|e| format!("scan `{table}`: {e}"))?;

    let mut out = Vec::new();
    for row in raw {
        // `scan_table` yields exactly the stored columns; a short row (fewer stored cells
        // than schema columns) is NULL-padded to the schema width.
        let mut jrow = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let v = sqlite_value_at(&row, i);
            jrow.push(sqlite_value_to_json(v, schema.columns()[i].ty)?);
        }
        out.push(jrow);
    }
    Ok(out)
}

fn sqlite_value_at(row: &[SqliteValue], index: usize) -> SqliteValue {
    match row.get(index) {
        Some(value) => value.clone(),
        None => SqliteValue::Null,
    }
}

fn num_f64_to_json(f: f64) -> JsonValue {
    serde_json::Number::from_f64(f).map_or(JsonValue::Null, JsonValue::Number)
}

fn sqlite_integer_to_json(i: i64, ty: ColumnType) -> JsonValue {
    match ty {
        ColumnType::Float | ColumnType::Double => num_f64_to_json(i as f64),
        _ => JsonValue::Number(i.into()),
    }
}

fn sqlite_text_to_json(s: String, ty: ColumnType) -> Result<JsonValue, String> {
    match ty {
        ColumnType::Int | ColumnType::BigInt | ColumnType::Timestamp => s
            .trim()
            .parse::<i64>()
            .map(|n| JsonValue::Number(n.into()))
            .map_err(|_| "non-integer text in an integer column".to_string()),
        ColumnType::Float | ColumnType::Double => s
            .trim()
            .parse::<f64>()
            .map(num_f64_to_json)
            .map_err(|_| "non-numeric text in a real column".to_string()),
        _ => Ok(JsonValue::String(s)),
    }
}

fn sqlite_blob_to_json(b: Vec<u8>, ty: ColumnType) -> Result<JsonValue, String> {
    match ty {
        // Bytes coerce accepts a JSON array of byte-sized ints (the props escape form).
        ColumnType::Bytes => Ok(JsonValue::Array(
            b.into_iter().map(|x| JsonValue::Number(x.into())).collect(),
        )),
        ColumnType::Text | ColumnType::Json => {
            Ok(JsonValue::String(String::from_utf8_lossy(&b).into_owned()))
        }
        _ => Err("blob value in a non-bytes column".to_string()),
    }
}

/// Convert a SQLite value into the `serde_json::Value` the store's `Cell::coerce`
/// accepts for `ty`.
fn sqlite_value_to_json(v: SqliteValue, ty: ColumnType) -> Result<JsonValue, String> {
    match v {
        SqliteValue::Null => Ok(JsonValue::Null),
        SqliteValue::Integer(i) => Ok(sqlite_integer_to_json(i, ty)),
        SqliteValue::Real(f) => Ok(num_f64_to_json(f)),
        SqliteValue::Text(s) => sqlite_text_to_json(s, ty),
        SqliteValue::Blob(b) => sqlite_blob_to_json(b, ty),
    }
}

// ── Export (CONCEPT:EG-KG.query.full-protocol) ───────────────────────────────────────────────────

/// Write the selected user tables OUT to a FRESH, valid transfer-root `sqlite3` `.db`
/// (the `sqlite3` CLI can open it). `tables` empty ⇒ every user table; else exactly the
/// named tables (each must exist). The destination must be fresh: descriptor-to-name
/// linking refuses replacement, and active WAL/SHM/journal sidecars fail closed.
/// Returns aggregate table counts without a host path.
fn export_sqlite_file(
    store: &TableStore,
    destination: &transfer_fs::ExportDestination,
    tables: &[String],
) -> Result<SqliteExportReport, String> {
    let before = store.catalog_fingerprint()?;
    let names: Vec<String> = if tables.is_empty() {
        store.list_tables()?
    } else {
        for t in tables {
            if store.get_schema(t)?.is_none() {
                return Err(format!("table `{t}` does not exist"));
            }
        }
        tables.to_vec()
    };

    if names.len() > MAX_SQLITE_TABLES {
        return Err("SQLite export contains too many tables".to_string());
    }
    let (max_bytes, max_rows) = sqlite_limits()?;
    let materialized = materialize_export_tables(store, &names, max_rows)?;
    if store.catalog_fingerprint()? != before {
        return Err("SQLite export source changed while it was being read".to_string());
    }
    let report = transfer_fs::write_export(destination, max_bytes, |path| {
        // The pure-Rust Writer serializes the whole `.db` in one bottom-up bulk load.
        let mut writer =
            Writer::create(path, 4096).map_err(|_| "create SQLite export failed".to_string())?;
        let report = write_export_tables(&materialized, &mut writer)?;
        writer
            .finish()
            .map_err(|_| "finalize SQLite export failed".to_string())?;
        Ok(report)
    })?;
    Ok(SqliteExportReport {
        destination: SqliteExportDestination::TransferRoot,
        exported_tables: report,
    })
}

struct ExportTable {
    schema: TableSchema,
    rows: Vec<Vec<Cell>>,
}

/// Materialize a version-fenced view; concurrent served DDL/DML rejects publication.
fn materialize_export_tables(
    store: &TableStore,
    names: &[String],
    max_rows: u64,
) -> Result<Vec<ExportTable>, String> {
    let mut materialized = Vec::with_capacity(names.len());
    let mut total_rows = 0u64;
    for table in names {
        let schema = store
            .get_schema(table)?
            .ok_or_else(|| "SQLite export table does not exist".to_string())?;
        if schema.columns().len() > MAX_SQLITE_COLUMNS {
            return Err("SQLite export table contains too many columns".to_string());
        }
        let rows = store.scan(table)?;
        total_rows = total_rows
            .checked_add(rows.len() as u64)
            .ok_or_else(|| "SQLite export row count overflow".to_string())?;
        if total_rows > max_rows {
            return Err("SQLite export exceeds the configured row limit".to_string());
        }
        materialized.push(ExportTable { schema, rows });
    }
    Ok(materialized)
}

fn write_export_tables(
    materialized: &[ExportTable],
    writer: &mut Writer,
) -> Result<Vec<SqliteTableRows>, String> {
    let mut report = Vec::with_capacity(materialized.len());
    for table in materialized {
        writer
            .add_table(&table.schema.name, &columns_from_schema(&table.schema))
            .map_err(|_| "create table in SQLite export failed".to_string())?;
        let n = export_rows(writer, &table.schema, &table.rows)?;
        report.push(SqliteTableRows {
            table: table.schema.name.as_str().to_owned(),
            rows: n as u64,
        });
    }
    Ok(report)
}

/// The column list for a table, mapping each [`ColumnType`] back to a SQLite declared type
/// so a re-import round-trips its affinity. The `Writer` builds the `CREATE TABLE` DDL
/// itself from these (`CREATE TABLE "name" ("col" TYPE, …)`), matching the old `export_ddl`.
fn columns_from_schema(schema: &TableSchema) -> Vec<SqliteColumnDef> {
    schema
        .columns()
        .iter()
        .map(|c| SqliteColumnDef {
            name: c.name.clone(),
            decl_type: type_to_sqlite(c.ty).to_string(),
        })
        .collect()
}

/// Map an engine [`ColumnType`] to a SQLite declared type (the inverse of
/// [`affinity_to_type`]). Bool/Timestamp store as INTEGER, Json/Vector as TEXT.
fn type_to_sqlite(ty: ColumnType) -> &'static str {
    match ty {
        // SQLite has no native UUID, NUMERIC-with-scale, timezone-aware
        // timestamp or array type, so the expanded types round-trip through
        // their canonical TEXT form rather than a lossy affinity. TimestampTz
        // stays INTEGER micros like Timestamp.
        ColumnType::Int
        | ColumnType::BigInt
        | ColumnType::Bool
        | ColumnType::Timestamp
        | ColumnType::TimestampTz => "INTEGER",
        ColumnType::Float | ColumnType::Double => "REAL",
        ColumnType::Text
        | ColumnType::Json
        | ColumnType::Uuid
        | ColumnType::Numeric(_)
        | ColumnType::Array(_) => "TEXT",
        ColumnType::Bytes => "BLOB",
        ColumnType::Vector(_) => "TEXT",
    }
}

/// Bulk-insert every scanned row into the writer's in-memory table (one batch, no per-row
/// wire loop — the `Writer` packs leaves/interiors on `finish()`).
fn export_rows(
    writer: &mut Writer,
    schema: &TableSchema,
    rows: &[Vec<Cell>],
) -> Result<usize, String> {
    if rows.is_empty() {
        return Ok(0);
    }
    let ncols = schema.columns().len();
    // `scan` NULL-pads each row to the schema width, so `row.len() == ncols`.
    let converted: Vec<Vec<SqliteValue>> = rows
        .iter()
        .map(|row| row.iter().take(ncols).map(cell_to_sqlite_value).collect())
        .collect();
    writer
        .insert_rows(&schema.name, &converted)
        .map_err(|e| format!("export insert rows: {e}"))
}

/// Convert a stored [`Cell`] to an `eg-sqlite-format` value for export.
fn cell_to_sqlite_value(cell: &Cell) -> SqliteValue {
    match cell {
        Cell::Null => SqliteValue::Null,
        Cell::Int(i) | Cell::Timestamp(i) => SqliteValue::Integer(*i),
        Cell::Float(f) => SqliteValue::Real(*f),
        Cell::Text(s) => SqliteValue::Text(s.clone()),
        Cell::Bool(b) => SqliteValue::Integer(*b as i64),
        Cell::Bytes(b) => SqliteValue::Blob(b.clone()),
        Cell::Json(v) => SqliteValue::Text(v.to_string()),
        Cell::Vector(v) => SqliteValue::Text(render_vector(v)),
    }
}

/// Render a vector as the pgvector-style `[a,b,c]` text SQLite stores.
fn render_vector(v: &[f32]) -> String {
    let inner: Vec<String> = v.iter().map(|f| f.to_string()).collect();
    format!("[{}]", inner.join(","))
}

#[cfg(test)]
mod tests {
    use eg_query::tables::schema::ArrayElemType;

    use super::*;
    use crate::test_rendezvous::meet;

    // The env-var save/restore guard lives once, beside the backup tests that
    // first needed it (`persistence::backup::EnvVarGuard`); this module had an
    // identical private copy.
    #[cfg(any(feature = "raft", target_os = "linux"))]
    use crate::server::persistence::backup::EnvVarGuard as EnvVarRestore;

    fn authority(agent: &str, tenant: &str) -> CarrierAuthority {
        CarrierAuthority::from_verified(
            &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                agent, tenant,
            ),
        )
        .unwrap()
    }

    #[test]
    fn ne002_expanded_types_have_explicit_sqlite_mappings() {
        assert_eq!(type_to_sqlite(ColumnType::Uuid), "TEXT");
        // Preserve NUMERIC's exact canonical text rather than lossy REAL affinity.
        assert_eq!(type_to_sqlite(ColumnType::Numeric(Some((10, 2)))), "TEXT");
        assert_eq!(type_to_sqlite(ColumnType::TimestampTz), "INTEGER");
        assert_eq!(
            type_to_sqlite(ColumnType::Array(ArrayElemType::Text)),
            "TEXT"
        );
    }

    #[cfg(target_os = "linux")]
    fn unique_paths() -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("eg_sqlite_roundtrip_{pid}_{nanos}"));
        rustix::fs::mkdir(&dir, rustix::fs::Mode::from_raw_mode(0o700)).unwrap();
        (
            dir.clone(),
            dir.join("source.db"),
            dir.join("destination.db"),
        )
    }

    /// The real `sqlite3` CLI the round-trip test diffs against: `$EG_SQLITE3`, else
    /// `sqlite3` on `$PATH`. Panics with how to provide it when neither runs.
    fn sqlite3_bin() -> std::ffi::OsString {
        let bin = std::env::var_os("EG_SQLITE3").unwrap_or_else(|| "sqlite3".into());
        match std::process::Command::new(&bin).arg("--version").output() {
            Ok(out) if out.status.success() => bin,
            other => panic!(
                "the sqlite `.db` round-trip test needs the real sqlite3 CLI, but {bin:?} is \
                 not runnable ({other:?}). Install sqlite3 on PATH, or run \
                 `export EG_SQLITE3=\"$(scripts/fetch_sqlite3.sh)\"` from the repository root."
            ),
        }
    }

    /// Run a SQL script through the real `sqlite3` CLI against `db`, returning stdout.
    fn run_sqlite(db: &Path, sql: &str) -> String {
        let out = std::process::Command::new(sqlite3_bin())
            .arg(db)
            .arg(sql)
            .output()
            .expect("spawn sqlite3");
        assert!(
            out.status.success(),
            "sqlite3 failed for {sql:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// CONCEPT:EG-KG.query.eg-feature / CONCEPT:EG-KG.query.full-protocol — the full `.db`
    /// file round-trip, now fully pure-Rust on the engine side and DIFFERENTIALLY verified
    /// against the real `sqlite3` CLI: author a source `.db` with `sqlite3`, IMPORT it
    /// (pure-Rust `Reader`) into an isolated engine store, EXPORT that store back out
    /// (pure-Rust `Writer`), then prove the export with `sqlite3`: `PRAGMA integrity_check`
    /// must be `ok`, and the `.schema`/`SELECT` output must match — including a NULL, a
    /// BLOB, an overflow-forcing large TEXT, and a multi-leaf/interior b-tree. Never skips:
    /// it FAILS when neither `$EG_SQLITE3` nor `sqlite3` on `$PATH` runs (see
    /// `scripts/fetch_sqlite3.sh`).
    #[cfg(target_os = "linux")]
    #[test]
    fn test_sqlite_file_roundtrip_eg331_eg332() {
        let (test_root, src, dst) = unique_paths();

        // 1. Build a source `.db` with the real sqlite3 CLI — every storage class, a NULL,
        //    a BLOB, a >4KB TEXT (overflow), and 4000 rows (multi-leaf + interior b-tree).
        run_sqlite(
            &src,
            "CREATE TABLE people (id INTEGER, name TEXT, score REAL, active INTEGER, blob_col BLOB);
             INSERT INTO people VALUES (1,'alice',9.5,1,x'0102');
             INSERT INTO people VALUES (2,'bob',NULL,0,NULL);
             CREATE TABLE big (id INTEGER, txt TEXT);
             INSERT INTO big VALUES (1, hex(randomblob(9000)));
             CREATE TABLE nums (n INTEGER, label TEXT);
             WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<4000)
               INSERT INTO nums SELECT n, 'row'||n FROM c;",
        );

        // 2. Import into an ISOLATED user-table store (pure-Rust Reader).
        let (store, store_path) = crate::store_authority::open_ephemeral_sql_store().unwrap();
        let reader = Reader::open(&src).unwrap();
        let (txn, report) = prepare_sqlite_import(&reader).unwrap();
        store.commit_txn(&txn).unwrap();
        let imported: Vec<&str> = report
            .imported_tables
            .iter()
            .map(|t| t.table.as_str())
            .collect();
        assert_eq!(imported, ["big", "nums", "people"]);
        assert_eq!(store.scan("people").unwrap().len(), 2);
        assert_eq!(store.scan("nums").unwrap().len(), 4000);

        // 3. Export the store back out to a fresh `.db` (pure-Rust Writer).
        let report2 =
            export_sqlite_file(&store, &transfer_fs::test_destination(&dst), &[]).unwrap();
        assert!(report2.exported_tables.len() == 3);

        // 4a. THE conformance bar: real sqlite3 integrity_check on OUR-written file.
        assert_eq!(
            run_sqlite(&dst, "PRAGMA integrity_check;").trim(),
            "ok",
            "exported `.db` failed sqlite3 integrity_check"
        );

        // 4b. Differential SELECT: values (incl. NULL + BLOB) survived, per the real CLI.
        let people = run_sqlite(
            &dst,
            "SELECT id,name,ifnull(score,'NULL'),active,quote(blob_col) FROM people ORDER BY id;",
        );
        let people: Vec<&str> = people.lines().collect();
        assert_eq!(people[0], "1|alice|9.5|1|X'0102'");
        assert_eq!(people[1], "2|bob|NULL|0|NULL");

        // Overflow TEXT (18000 hex chars) and the large b-tree round-tripped intact.
        assert_eq!(
            run_sqlite(&dst, "SELECT length(txt) FROM big;").trim(),
            "18000"
        );
        assert_eq!(
            run_sqlite(&dst, "SELECT count(*),min(n),max(n) FROM nums;").trim(),
            "4000|1|4000"
        );
        assert_eq!(
            run_sqlite(&dst, "SELECT label FROM nums WHERE n=3333;").trim(),
            "row3333"
        );
        // Exact stored DDL round-trips its affinity.
        assert_eq!(
            run_sqlite(&dst, "SELECT sql FROM sqlite_master WHERE name='people';").trim(),
            "CREATE TABLE \"people\" (\"id\" INTEGER, \"name\" TEXT, \"score\" REAL, \"active\" INTEGER, \"blob_col\" BLOB)"
        );

        // Cleanup.
        rustix::fs::unlink(&src).unwrap();
        rustix::fs::unlink(&dst).unwrap();
        rustix::fs::unlink(&store_path).unwrap();
        rustix::fs::rmdir(&test_root).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transfer_job_runs_once_off_the_current_thread_reactor() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};

        let barrier = Arc::new(Barrier::new(2));
        let executions = Arc::new(AtomicUsize::new(0));
        let completions = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let worker_barrier = Arc::clone(&barrier);
        let worker_executions = Arc::clone(&executions);
        let worker_completions = Arc::clone(&completions);

        let task = tokio::spawn(run_transfer_job("test", move || {
            worker_executions.fetch_add(1, Ordering::SeqCst);
            entered_tx.send(()).expect("reactor still awaits entry");
            meet(
                &worker_barrier,
                "transfer job held inside its blocking body",
            );
            worker_completions.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(17_u64)
        }));

        entered_rx.await.expect("blocking job entered");
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert_eq!(completions.load(Ordering::SeqCst), 0);
        meet(&barrier, "the test releasing the held transfer job");
        assert_eq!(task.await.expect("join transfer future").unwrap(), 17);
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert_eq!(completions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transfer_job_preserves_safe_failures_and_redacts_panics() {
        let expected = "SQLite import source does not exist";
        let ordinary = run_transfer_job("import", move || Err::<(), _>(expected.to_string())).await;
        assert_eq!(ordinary.unwrap_err(), expected);

        let panicked = run_transfer_job("export", || -> Result<(), String> {
            panic!("/private/transfer/operator-secret.db")
        })
        .await
        .unwrap_err();
        assert_eq!(panicked, "SQLite export task failed");
        assert!(!panicked.contains("operator-secret"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_waiter_does_not_cancel_owned_transfer_job() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};

        let barrier = Arc::new(Barrier::new(2));
        let completions = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let worker_barrier = Arc::clone(&barrier);
        let worker_completions = Arc::clone(&completions);
        let waiter = tokio::spawn(run_transfer_job("test", move || {
            entered_tx.send(()).expect("test waits for entry");
            meet(
                &worker_barrier,
                "owned transfer job held after its waiter was cancelled",
            );
            worker_completions.fetch_add(1, Ordering::SeqCst);
            completed_tx.send(()).expect("test waits for completion");
            Ok::<_, String>(())
        }));

        entered_rx.await.expect("blocking job entered");
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        meet(&barrier, "the test releasing the orphaned transfer job");
        completed_rx.await.expect("owned blocking job completed");
        assert_eq!(completions.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn export_version_fence_rejects_deterministic_interleaved_commit() {
        let persist_dir = crate::server::sql_tables::test_persist_dir();
        let authority = authority("sqlite-exporter", "sqlite-export-fence");
        let store =
            crate::server::sql_tables::tenant_table_store(authority.tenant_scope(), &persist_dir)
                .unwrap();
        let schema = TableSchema::new(
            "events",
            vec![Column::new("id", ColumnType::BigInt, false, false)],
        );
        store.create_table(&schema, false).unwrap();
        let before = store.catalog_fingerprint().unwrap();
        let snapshot = materialize_export_tables(&store, &["events".to_string()], 10).unwrap();
        assert!(snapshot[0].rows.is_empty());

        let method = Method::ImportSqliteFile {
            path: "interleaved.db".to_string(),
        };
        let batch = compile_import_batch(&store, 77, &authority, &method, 2).unwrap();
        let mut txn = TableTxn::new();
        txn.push(TxnOp::Insert {
            table: "events".to_string(),
            col_order: vec!["id".to_string()],
            rows: vec![vec![serde_json::json!(1)]],
        });
        store
            .commit_txn_batch_result(&txn, &batch, Vec::new(), 2)
            .unwrap();

        assert_ne!(store.catalog_fingerprint().unwrap(), before);
    }

    #[cfg(feature = "raft")]
    #[test]
    fn export_fails_source_authority_before_opening_store_or_destination() {
        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
        let key = "EPISTEMIC_GRAPH_RAFT_NODE_ID";
        let _env_restore = EnvVarRestore::set(key, "sqlite-export-order-test");
        let error = export_sqlite_lifecycle(
            &authority("sqlite-exporter", "sqlite-export-order"),
            "invalid/path.db",
            &[],
            Path::new("/definitely-not-a-persist-directory"),
        )
        .unwrap_err();
        assert_eq!(error, "SQL source authority has no replicated ordering");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn missing_source_replay_repairs_simulated_post_commit_process_loss() {
        use crate::server::sql_catalog_acl::{open_authorized_table, SqlPrivilege};
        use std::os::unix::fs::PermissionsExt;

        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
        let transfer_root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(transfer_root.path(), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        let _env_restore = EnvVarRestore::set(
            "EPISTEMIC_GRAPH_SQLITE_TRANSFER_ROOT",
            transfer_root.path().to_str().unwrap(),
        );
        let persist_dir = crate::server::sql_tables::test_persist_dir();
        let authority = authority("sqlite-owner", "sqlite-owner-repair");
        let store =
            crate::server::sql_tables::tenant_table_store(authority.tenant_scope(), &persist_dir)
                .unwrap();
        let method = Method::ImportSqliteFile {
            path: "missing-after-commit.db".to_string(),
        };
        let import_scope = authority.namespace("sqlite-import", "global-user-tables");
        let first_nonce = Nonce::from_bytes([1; 32]);
        let batch =
            compile_import_batch_with_nonce(&store, 91, &authority, &method, 3, Some(first_nonce))
                .unwrap();
        let mut txn = TableTxn::new();
        txn.push(TxnOp::CreateTable {
            schema: TableSchema::new(
                "repaired",
                vec![Column::new("id", ColumnType::BigInt, false, false)],
            ),
            if_not_exists: false,
        });
        let report = SqliteImportReport {
            source: SqliteImportSource::Sqlite,
            imported_tables: vec![SqliteTableRows {
                table: "repaired".to_string(),
                rows: 0,
            }],
        };
        store
            .commit_txn_batch_result(&txn, &batch, rmp_serde::to_vec_named(&report).unwrap(), 3)
            .unwrap();
        let committed_version = store
            .mutation_version(authority.tenant_scope(), &import_scope)
            .unwrap();
        let committed_outbox = store
            .mutation_outbox(&batch.identity, &batch.batch_id)
            .unwrap();

        // The physical effect exists without its ACL child, exactly the durable
        // state left by process loss after the table commit. The source never exists.
        assert!(
            open_authorized_table(&authority, &persist_dir, "repaired", SqlPrivilege::Select)
                .is_err()
        );
        assert_eq!(
            import_sqlite_lifecycle_with_nonce(
                92,
                &authority,
                Some(Nonce::from_bytes([2; 32])),
                &method,
                "missing-after-commit.db",
                &persist_dir,
                4,
            )
            .unwrap(),
            report
        );
        assert!(
            open_authorized_table(&authority, &persist_dir, "repaired", SqlPrivilege::Select)
                .is_ok()
        );
        assert_eq!(
            store
                .mutation_version(authority.tenant_scope(), &import_scope)
                .unwrap(),
            committed_version
        );
        assert_eq!(
            store
                .mutation_outbox(&batch.identity, &batch.batch_id)
                .unwrap(),
            committed_outbox
        );

        let consumed = import_sqlite_lifecycle_with_nonce(
            93,
            &authority,
            Some(first_nonce),
            &method,
            "missing-after-commit.db",
            &persist_dir,
            5,
        )
        .unwrap_err();
        assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");

        let changed = Method::ImportSqliteFile {
            path: "different-missing-source.db".to_string(),
        };
        let conflict = import_sqlite_lifecycle_with_nonce(
            94,
            &authority,
            Some(Nonce::from_bytes([3; 32])),
            &changed,
            "different-missing-source.db",
            &persist_dir,
            6,
        )
        .unwrap_err();
        assert!(conflict.contains("IDEMPOTENCY_CONFLICT"), "{conflict}");
    }
}
