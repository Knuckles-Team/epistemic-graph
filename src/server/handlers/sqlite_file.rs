//! SQLite `.db` FILE import/export handler (CONCEPT:EG-KG.query.eg-feature / CONCEPT:EG-KG.query.full-protocol) — the
//! documented EG-075 follow-up: reading/writing a real on-disk `sqlite3` `.db` FILE,
//! distinct from the `sqlite-wire` NDJSON dialect surface (which speaks SQLite SQL over
//! a socket but never touches a `.db` file).
//!
//! ## Why the C `rusqlite`, not pure-Rust
//! The export half must produce a `.db` a stock `sqlite3` CLI can open, i.e. a
//! spec-correct SQLite b-tree page file. There is no mature pure-Rust crate that WRITES
//! that format; hand-rolling it is the exact blocker `src/server/sqlite_wire/mod.rs`
//! documented. So this ONE feature (`sqlite-file`) pulls `rusqlite` with the BUNDLED C
//! sqlite3 — and is kept OUT of `pi`/`default` (folded only into `full`/`node`), so the
//! Pi contract holds: a `--features pi` build links no rusqlite/libsqlite3-sys.
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
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value as JsonValue;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::mutation_batch::{MutationBatch, MutationDomain, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;

const SQLITE_TRANSFER_ROOT_ENV: &str = "EPISTEMIC_GRAPH_SQLITE_TRANSFER_ROOT";
const SQLITE_MAX_BYTES_ENV: &str = "EPISTEMIC_GRAPH_SQLITE_MAX_BYTES";
const SQLITE_MAX_ROWS_ENV: &str = "EPISTEMIC_GRAPH_SQLITE_MAX_ROWS";
const DEFAULT_SQLITE_MAX_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CONFIGURED_SQLITE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const DEFAULT_SQLITE_MAX_ROWS: u64 = 1_000_000;
const MAX_CONFIGURED_SQLITE_ROWS: u64 = 100_000_000;
const MAX_SQLITE_TABLES: usize = 4_096;
const MAX_SQLITE_COLUMNS: usize = 2_048;
static EXPORT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn bounded_env_u64(name: &str, default: u64, maximum: u64) -> Result<u64, String> {
    let Some(raw) = std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(default);
    };
    let value = raw
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an integer"))?;
    if value == 0 || value > maximum {
        return Err(format!("{name} must be between 1 and {maximum}"));
    }
    Ok(value)
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

/// SQLite file transfer is deliberately disabled until an operator provisions a
/// dedicated directory. RPC callers provide a logical filename, never a host path.
fn sqlite_transfer_root() -> Result<PathBuf, String> {
    let configured = std::env::var_os(SQLITE_TRANSFER_ROOT_ENV)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            format!("SQLite file transfer is disabled; configure {SQLITE_TRANSFER_ROOT_ENV}")
        })?;
    let configured = PathBuf::from(configured);
    let metadata = std::fs::symlink_metadata(&configured)
        .map_err(|_| "configured SQLite transfer root is unavailable".to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("configured SQLite transfer root must be a real directory".to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(
                "configured SQLite transfer root must have private permissions".to_string(),
            );
        }
    }
    configured
        .canonicalize()
        .map_err(|_| "configured SQLite transfer root is unavailable".to_string())
}

fn sqlite_logical_filename(value: &str) -> Result<&str, String> {
    let mut chars = value.chars();
    let first = chars.next();
    if value.is_empty()
        || value.len() > 255
        || !first.is_some_and(|character| character.is_ascii_alphanumeric())
        || !chars.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
        || !value.to_ascii_lowercase().ends_with(".db")
    {
        return Err("SQLite transfer name must be a bounded .db filename".to_string());
    }
    let path = Path::new(value);
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err("SQLite transfer name must not contain a path".to_string());
    }
    Ok(value)
}

fn resolve_sqlite_import(value: &str) -> Result<PathBuf, String> {
    let root = sqlite_transfer_root()?;
    let name = sqlite_logical_filename(value)?;
    let candidate = root.join(name);
    let metadata = std::fs::symlink_metadata(&candidate)
        .map_err(|_| "SQLite import source does not exist".to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("SQLite import source must be a regular file".to_string());
    }
    let canonical = candidate
        .canonicalize()
        .map_err(|_| "SQLite import source is unavailable".to_string())?;
    if canonical.parent() != Some(root.as_path()) {
        return Err("SQLite import source escaped the transfer root".to_string());
    }
    let (max_bytes, _) = sqlite_limits()?;
    if metadata.len() > max_bytes {
        return Err("SQLite import source exceeds the configured size limit".to_string());
    }
    Ok(canonical)
}

fn resolve_sqlite_export(value: &str) -> Result<PathBuf, String> {
    let root = sqlite_transfer_root()?;
    let name = sqlite_logical_filename(value)?;
    Ok(root.join(name))
}

/// Route the two SQLite-file methods. Resolves the caller's owner-scoped user-table store, then
/// runs the (blocking, file-I/O) import/export on the blocking pool so the reactor is
/// never stalled. `Err(method)` for a method that isn't ours (unreachable — dispatch only
/// routes the two variants here).
pub(crate) async fn try_handle(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Response, Method> {
    if let Err(error) = authority.require_admin("SQLite user-table import/export") {
        return Ok(Response::err(req_id, error));
    }
    let original_method = method.clone();
    match method {
        Method::ImportSqliteFile { path } => {
            let source = match resolve_sqlite_import(&path) {
                Ok(value) => value,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            let persist_dir = state.read().await.persist_dir.clone();
            let store = match crate::server::sql_tables::user_table_store(
                authority,
                persist_dir.as_deref().map(std::path::Path::new),
            ) {
                Ok(s) => s,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let (batch, now) =
                match compile_import_batch(&store, req_id, authority, &original_method) {
                    Ok(value) => value,
                    Err(error) => return Ok(Response::err(req_id, error)),
                };
            if let Ok(Some(record)) = store.mutation_batch(&batch.batch_id) {
                if same_import_identity(&batch, &record.batch) {
                    let Some(bytes) = record.result_msgpack.as_deref() else {
                        return Ok(Response::err(
                            req_id,
                            "committed SQLite import batch has no result",
                        ));
                    };
                    return Ok(match eg_types::msgpack::decode_property_value(bytes) {
                        Ok(value) => Response::ok(req_id, ResultPayload::Json(value)),
                        Err(_) => Response::err(
                            req_id,
                            "committed SQLite import batch has an invalid result",
                        ),
                    });
                }
                return Ok(Response::err(
                    req_id,
                    "IDEMPOTENCY_CONFLICT: SQLite import request identity changed",
                ));
            }
            let out = tokio::task::spawn_blocking(move || {
                let (txn, report) = prepare_sqlite_import(&source)?;
                let result = rmp_serde::to_vec_named(&report).map_err(|e| e.to_string())?;
                let committed = store.commit_txn_batch_result(&txn, &batch, result, now)?;
                let bytes = committed
                    .record
                    .result_msgpack
                    .as_deref()
                    .ok_or_else(|| "committed SQLite import batch has no result".to_string())?;
                eg_types::msgpack::decode_property_value(bytes)
                    .map_err(|_| "committed SQLite import batch has an invalid result".to_string())
            })
            .await;
            Ok(match out {
                Ok(Ok(v)) => Response::ok(req_id, ResultPayload::Json(v)),
                Ok(Err(e)) => Response::err(req_id, e),
                Err(e) => Response::err(req_id, format!("sqlite import task join error: {e}")),
            })
        }
        Method::ExportSqliteFile { path, tables } => {
            let destination = match resolve_sqlite_export(&path) {
                Ok(value) => value,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            let persist_dir = state.read().await.persist_dir.clone();
            let store = match crate::server::sql_tables::user_table_store(
                authority,
                persist_dir.as_deref().map(std::path::Path::new),
            ) {
                Ok(s) => s,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let out = tokio::task::spawn_blocking(move || {
                export_sqlite_file(&store, &destination, &tables)
            })
            .await;
            Ok(match out {
                Ok(Ok(v)) => Response::ok(req_id, ResultPayload::Json(v)),
                Ok(Err(e)) => Response::err(req_id, e),
                Err(e) => Response::err(req_id, format!("sqlite export task join error: {e}")),
            })
        }
        other => Err(other),
    }
}

fn compile_import_batch(
    store: &TableStore,
    req_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
) -> Result<(MutationBatch, u64), String> {
    let scope = authority.namespace("sqlite-import", "global-user-tables");
    let expected = store.mutation_version(authority.tenant_scope(), &scope)?;
    let batch_id =
        crate::server::mutation_batch::opaque_request_key("sqlite-import", &scope, req_id, method);
    let now = crate::server::dispatch::authoritative_now_ms();
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: req_id,
            principal: Some(authority.actor_scope()),
            tenant: authority.tenant_scope(),
            graph: &scope,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Query,
            authoritative_state: None,
        },
        method,
        MutationSurface::Query,
        MutationDomain::SqlCatalog,
        "sqlite_import",
    )?;
    Ok((batch, now))
}

fn same_import_identity(proposed: &MutationBatch, stored: &MutationBatch) -> bool {
    proposed.batch_id == stored.batch_id
        && proposed.context.principal == stored.context.principal
        && proposed.tenant == stored.tenant
        && proposed.graph == stored.graph
        && rmp_serde::to_vec_named(&proposed.operations).ok()
            == rmp_serde::to_vec_named(&stored.operations).ok()
}

// ── Import (CONCEPT:EG-KG.query.eg-feature) ───────────────────────────────────────────────────

/// Read every user table (+ its rows) from a validated transfer-root `.db` into
/// `store`. A same-name table already in the store is REPLACED (drop-then-recreate) so
/// the import mirrors the file. Returns aggregate table counts without a host path.
#[cfg(test)]
pub(crate) fn import_sqlite_file(store: &TableStore, path: &Path) -> Result<JsonValue, String> {
    if !path.exists() {
        return Err("SQLite import source does not exist".to_string());
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| "open SQLite import source failed".to_string())?;

    let tables = list_user_tables(&conn)?;
    let mut report = Vec::with_capacity(tables.len());
    for table in &tables {
        let (schema, col_order) = import_schema(&conn, table)?;
        // Replace an existing same-name table so the import mirrors the file.
        store.drop_table(table, true)?;
        store.create_table(&schema, false)?;
        let rows = import_rows(&conn, table, &schema)?;
        let n = if rows.is_empty() {
            0
        } else {
            // ONE batch insert per table (never per-row).
            store.insert_rows(table, &col_order, &rows)?
        };
        report.push(serde_json::json!({ "table": table, "rows": n }));
    }
    Ok(serde_json::json!({ "source": "sqlite", "imported_tables": report }))
}

fn prepare_sqlite_import(path: &Path) -> Result<(TableTxn, JsonValue), String> {
    if !path.exists() {
        return Err("SQLite import source does not exist".to_string());
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| "open SQLite import source failed".to_string())?;
    let tables = list_user_tables(&conn)?;
    let (_, max_rows) = sqlite_limits()?;
    if tables.len() > MAX_SQLITE_TABLES {
        return Err("SQLite import contains too many tables".to_string());
    }
    let mut txn = TableTxn::new();
    let mut report = Vec::with_capacity(tables.len());
    let mut total_rows = 0u64;
    for table in &tables {
        let (schema, col_order) = import_schema(&conn, table)?;
        let row_count = sqlite_table_row_count(&conn, table)?;
        total_rows = total_rows
            .checked_add(row_count)
            .ok_or_else(|| "SQLite import row count overflow".to_string())?;
        if total_rows > max_rows {
            return Err("SQLite import exceeds the configured row limit".to_string());
        }
        let rows = import_rows(&conn, table, &schema)?;
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
        report.push(serde_json::json!({ "table": table, "rows": row_count }));
    }
    Ok((
        txn,
        serde_json::json!({ "source": "sqlite", "imported_tables": report }),
    ))
}

fn sqlite_table_row_count(conn: &Connection, table: &str) -> Result<u64, String> {
    let sql = format!("SELECT COUNT(*) FROM {}", quote_ident(table));
    let count = conn
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .map_err(|_| "count SQLite import rows failed".to_string())?;
    u64::try_from(count).map_err(|_| "SQLite import returned a negative row count".to_string())
}

/// The user tables in a `.db` (skip `sqlite_*` internal tables), sorted for determinism.
fn list_user_tables(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='table' \
             AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
        )
        .map_err(|e| format!("read sqlite_master: {e}"))?;
    let names = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| format!("read sqlite_master: {e}"))?
        .collect::<Result<Vec<String>, _>>()
        .map_err(|e| format!("read sqlite_master: {e}"))?;
    Ok(names)
}

/// Build the engine [`TableSchema`] for a `.db` table from `PRAGMA table_info` (every
/// column NULLABLE + non-PK — values, not constraints, are mirrored). Returns the schema
/// and the column-name order for the batch insert.
fn import_schema(conn: &Connection, table: &str) -> Result<(TableSchema, Vec<String>), String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({})", quote_ident(table)))
        .map_err(|e| format!("table_info({table}): {e}"))?;
    // PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk.
    let rows = stmt
        .query_map([], |r| {
            let name: String = r.get(1)?;
            let decl: Option<String> = r.get(2)?;
            Ok((name, decl.unwrap_or_default()))
        })
        .map_err(|e| format!("table_info({table}): {e}"))?;

    let mut columns = Vec::new();
    let mut names = Vec::new();
    for row in rows {
        let (name, decl) = row.map_err(|e| format!("table_info({table}): {e}"))?;
        columns.push(Column::new(
            name.clone(),
            affinity_to_type(&decl),
            true,
            false,
        ));
        names.push(name);
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

/// Read every row of `table`, converting each SQLite runtime value to the JSON shape the
/// target column's [`ColumnType`] coerces from cleanly.
fn import_rows(
    conn: &Connection,
    table: &str,
    schema: &TableSchema,
) -> Result<Vec<Vec<JsonValue>>, String> {
    let ncols = schema.columns().len();
    let mut stmt = conn
        .prepare(&format!("SELECT * FROM {}", quote_ident(table)))
        .map_err(|e| format!("scan `{table}`: {e}"))?;
    let raw = stmt
        .query_map([], |r| {
            let mut cells = Vec::with_capacity(ncols);
            for i in 0..ncols {
                cells.push(r.get::<_, SqlValue>(i)?);
            }
            Ok(cells)
        })
        .map_err(|e| format!("scan `{table}`: {e}"))?;

    let mut out = Vec::new();
    for row in raw {
        let row = row.map_err(|e| format!("scan `{table}`: {e}"))?;
        let mut jrow = Vec::with_capacity(ncols);
        for (i, v) in row.into_iter().enumerate() {
            jrow.push(sqlite_value_to_json(v, schema.columns()[i].ty)?);
        }
        out.push(jrow);
    }
    Ok(out)
}

/// Convert a SQLite runtime value into the `serde_json::Value` the store's `Cell::coerce`
/// accepts for `ty`.
fn sqlite_value_to_json(v: SqlValue, ty: ColumnType) -> Result<JsonValue, String> {
    let num_f64 = |f: f64| {
        serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null)
    };
    match v {
        SqlValue::Null => Ok(JsonValue::Null),
        SqlValue::Integer(i) => match ty {
            ColumnType::Float | ColumnType::Double => Ok(num_f64(i as f64)),
            _ => Ok(JsonValue::Number(i.into())),
        },
        SqlValue::Real(f) => Ok(num_f64(f)),
        SqlValue::Text(s) => match ty {
            ColumnType::Int | ColumnType::BigInt | ColumnType::Timestamp => s
                .trim()
                .parse::<i64>()
                .map(|n| JsonValue::Number(n.into()))
                .map_err(|_| "non-integer text in an integer column".to_string()),
            ColumnType::Float | ColumnType::Double => s
                .trim()
                .parse::<f64>()
                .map(num_f64)
                .map_err(|_| "non-numeric text in a real column".to_string()),
            _ => Ok(JsonValue::String(s)),
        },
        SqlValue::Blob(b) => match ty {
            // Bytes coerce accepts a JSON array of byte-sized ints (the props escape form).
            ColumnType::Bytes => Ok(JsonValue::Array(
                b.into_iter().map(|x| JsonValue::Number(x.into())).collect(),
            )),
            ColumnType::Text | ColumnType::Json => {
                Ok(JsonValue::String(String::from_utf8_lossy(&b).into_owned()))
            }
            _ => Err("blob value in a non-bytes column".to_string()),
        },
    }
}

// ── Export (CONCEPT:EG-KG.query.full-protocol) ───────────────────────────────────────────────────

/// Write the selected user tables OUT to a FRESH, valid transfer-root `sqlite3` `.db`
/// (the `sqlite3` CLI can open it). `tables` empty ⇒ every user table; else exactly the
/// named tables (each must exist). Any pre-existing file at `path` is overwritten.
/// Returns aggregate table counts without a host path.
pub(crate) fn export_sqlite_file(
    store: &TableStore,
    path: &Path,
    tables: &[String],
) -> Result<JsonValue, String> {
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
    let temp_path = create_private_export_temp(path)?;
    let mut conn = match Connection::open(&temp_path) {
        Ok(value) => value,
        Err(_) => {
            let _ = std::fs::remove_file(&temp_path);
            return Err("create SQLite export failed".to_string());
        }
    };

    let result = (|| -> Result<Vec<JsonValue>, String> {
        let mut report = Vec::with_capacity(names.len());
        let mut total_rows = 0u64;
        for table in &names {
            let schema = store
                .get_schema(table)?
                .ok_or_else(|| "SQLite export table does not exist".to_string())?;
            if schema.columns().len() > MAX_SQLITE_COLUMNS {
                return Err("SQLite export table contains too many columns".to_string());
            }
            conn.execute(&export_ddl(&schema), [])
                .map_err(|_| "create table in SQLite export failed".to_string())?;
            // ONE scan per table (batch), then one bulk transaction of inserts.
            let rows = store.scan(table)?;
            total_rows = total_rows
                .checked_add(rows.len() as u64)
                .ok_or_else(|| "SQLite export row count overflow".to_string())?;
            if total_rows > max_rows {
                return Err("SQLite export exceeds the configured row limit".to_string());
            }
            let n = export_rows(&mut conn, &schema, &rows)?;
            report.push(serde_json::json!({ "table": table, "rows": n }));
        }
        Ok(report)
    })();
    // Flush pages to the file before reporting success (the connection drop also flushes).
    let _ = conn.cache_flush();
    drop(conn);
    let report = match result {
        Ok(value) => value,
        Err(error) => {
            let _ = remove_db_files(&temp_path);
            return Err(error);
        }
    };
    let export_bytes = match std::fs::metadata(&temp_path) {
        Ok(metadata) => metadata.len(),
        Err(_) => {
            let _ = remove_db_files(&temp_path);
            return Err("inspect SQLite export failed".to_string());
        }
    };
    if export_bytes > max_bytes {
        let _ = remove_db_files(&temp_path);
        return Err("SQLite export exceeds the configured size limit".to_string());
    }
    if let Err(error) = install_export(&temp_path, path) {
        let _ = remove_db_files(&temp_path);
        return Err(error);
    }
    Ok(serde_json::json!({ "destination": "transfer-root", "exported_tables": report }))
}

fn create_private_export_temp(destination: &Path) -> Result<PathBuf, String> {
    let parent = destination
        .parent()
        .ok_or_else(|| "SQLite export destination is invalid".to_string())?;
    for _ in 0..32 {
        let sequence = EXPORT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".sqlite-export-{}-{sequence}.tmp",
            std::process::id()
        ));
        let opened = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate);
        match opened {
            Ok(file) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    file.set_permissions(std::fs::Permissions::from_mode(0o600))
                        .map_err(|_| "secure SQLite export permissions failed".to_string())?;
                }
                drop(file);
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err("create private SQLite export failed".to_string()),
        }
    }
    Err("create unique SQLite export failed".to_string())
}

fn install_export(temp_path: &Path, destination: &Path) -> Result<(), String> {
    remove_db_files(destination)?;
    std::fs::rename(temp_path, destination)
        .map_err(|_| "install SQLite export failed".to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| "secure SQLite export permissions failed".to_string())?;
    }
    Ok(())
}

/// Remove a `.db` file and any leftover `-wal`/`-shm`/`-journal` siblings so a fresh
/// export never inherits stale pages.
fn remove_db_files(path: &Path) -> Result<(), String> {
    let value = path
        .to_str()
        .ok_or_else(|| "SQLite export destination is invalid".to_string())?;
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let p = format!("{value}{suffix}");
        let pp = Path::new(&p);
        if pp.exists() {
            let metadata = std::fs::symlink_metadata(pp)
                .map_err(|_| "inspect existing SQLite export failed".to_string())?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err("existing SQLite export target is not a regular file".to_string());
            }
            std::fs::remove_file(pp)
                .map_err(|_| "remove existing SQLite export failed".to_string())?;
        }
    }
    Ok(())
}

/// The `CREATE TABLE` DDL for an engine schema, mapping each [`ColumnType`] back to a
/// SQLite declared type so a re-import round-trips its affinity.
fn export_ddl(schema: &TableSchema) -> String {
    let cols: Vec<String> = schema
        .columns()
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), type_to_sqlite(c.ty)))
        .collect();
    format!(
        "CREATE TABLE {} ({})",
        quote_ident(&schema.name),
        cols.join(", ")
    )
}

/// Map an engine [`ColumnType`] to a SQLite declared type (the inverse of
/// [`affinity_to_type`]). Bool/Timestamp store as INTEGER, Json/Vector as TEXT.
fn type_to_sqlite(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Int | ColumnType::BigInt | ColumnType::Bool | ColumnType::Timestamp => {
            "INTEGER"
        }
        ColumnType::Float | ColumnType::Double => "REAL",
        ColumnType::Text | ColumnType::Json => "TEXT",
        ColumnType::Bytes => "BLOB",
        ColumnType::Vector(_) => "TEXT",
    }
}

/// Insert every scanned row into the sqlite table in ONE transaction (all-or-nothing).
fn export_rows(
    conn: &mut Connection,
    schema: &TableSchema,
    rows: &[Vec<Cell>],
) -> Result<usize, String> {
    if rows.is_empty() {
        return Ok(0);
    }
    let ncols = schema.columns().len();
    let placeholders = vec!["?"; ncols].join(", ");
    let insert = format!(
        "INSERT INTO {} VALUES ({})",
        quote_ident(&schema.name),
        placeholders
    );
    let tx = conn
        .transaction()
        .map_err(|e| format!("begin export txn: {e}"))?;
    let mut count = 0usize;
    {
        let mut stmt = tx
            .prepare(&insert)
            .map_err(|e| format!("prepare export insert: {e}"))?;
        for row in rows {
            // `scan` NULL-pads each row to the schema width, so `row.len() == ncols`.
            let params: Vec<SqlValue> = row.iter().take(ncols).map(cell_to_sqlite).collect();
            stmt.execute(rusqlite::params_from_iter(params))
                .map_err(|e| format!("export insert row: {e}"))?;
            count += 1;
        }
    }
    tx.commit().map_err(|e| format!("commit export txn: {e}"))?;
    Ok(count)
}

/// Convert a stored [`Cell`] to a SQLite runtime value for export.
fn cell_to_sqlite(cell: &Cell) -> SqlValue {
    match cell {
        Cell::Null => SqlValue::Null,
        Cell::Int(i) | Cell::Timestamp(i) => SqlValue::Integer(*i),
        Cell::Float(f) => SqlValue::Real(*f),
        Cell::Text(s) => SqlValue::Text(s.clone()),
        Cell::Bool(b) => SqlValue::Integer(*b as i64),
        Cell::Bytes(b) => SqlValue::Blob(b.clone()),
        Cell::Json(v) => SqlValue::Text(v.to_string()),
        Cell::Vector(v) => SqlValue::Text(render_vector(v)),
    }
}

/// Render a vector as the pgvector-style `[a,b,c]` text SQLite stores.
fn render_vector(v: &[f32]) -> String {
    let inner: Vec<String> = v.iter().map(|f| f.to_string()).collect();
    format!("[{}]", inner.join(","))
}

/// Double-quote a SQL identifier (table/column name), escaping embedded quotes, so a name
/// with spaces/keywords is safe in the generated DDL/DML.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn unique_paths() -> (std::path::PathBuf, std::path::PathBuf) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        (
            dir.join(format!("eg_sqlite_src_{pid}_{nanos}.db")),
            dir.join(format!("eg_sqlite_dst_{pid}_{nanos}.db")),
        )
    }

    /// CONCEPT:EG-KG.query.eg-feature / CONCEPT:EG-KG.query.full-protocol — the full `.db` file round-trip: write a source
    /// SQLite file with the C sqlite library, IMPORT it into an isolated engine table
    /// store, EXPORT that store back out to a fresh `.db`, then RE-OPEN the exported file
    /// with the sqlite library (proving it is a valid, `sqlite3`-readable database) and
    /// assert every value — including a NULL and a BLOB — survived the round-trip.
    #[test]
    fn test_sqlite_file_roundtrip_eg331_eg332() {
        let (src, dst) = unique_paths();

        // 1. Build a source `.db` with the bundled sqlite3 library.
        {
            let conn = Connection::open(&src).unwrap();
            conn.execute_batch(
                "CREATE TABLE people (id INTEGER, name TEXT, score REAL, active INTEGER, blob_col BLOB);",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO people VALUES (1, 'alice', 9.5, 1, x'0102')",
                [],
            )
            .unwrap();
            conn.execute("INSERT INTO people VALUES (2, 'bob', NULL, 0, NULL)", [])
                .unwrap();
        }

        // 2. Import into an ISOLATED user-table store (the served path uses the
        //    process-global store; a temp store keeps the test hermetic).
        let (store, store_path) = TableStore::open_temp().unwrap();
        let report = import_sqlite_file(&store, &src).unwrap();
        assert_eq!(report["imported_tables"][0]["table"], "people");
        assert_eq!(report["imported_tables"][0]["rows"], 2);
        assert_eq!(store.scan("people").unwrap().len(), 2);

        // 3. Export the store back out to a fresh `.db`.
        let report2 = export_sqlite_file(&store, &dst, &[]).unwrap();
        assert_eq!(report2["exported_tables"][0]["table"], "people");
        assert_eq!(report2["exported_tables"][0]["rows"], 2);

        // 4. Re-open the EXPORTED file with the sqlite library — this proves it is a
        //    valid, sqlite3-readable `.db` — and assert the rows round-tripped.
        let conn = Connection::open(&dst).unwrap();
        let rows: Vec<(i64, String, Option<f64>, i64)> = {
            let mut stmt = conn
                .prepare("SELECT id, name, score, active FROM people ORDER BY id")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, "alice".to_string(), Some(9.5), 1));
        assert_eq!(rows[1].0, 2);
        assert_eq!(rows[1].1, "bob");
        assert_eq!(rows[1].2, None, "a NULL must survive the round-trip");
        assert_eq!(rows[1].3, 0);
        let blob: Vec<u8> = conn
            .query_row("SELECT blob_col FROM people WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(blob, vec![1u8, 2u8], "a BLOB must survive the round-trip");

        // Cleanup.
        drop(conn);
        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
        let _ = std::fs::remove_file(&store_path);
    }

    #[test]
    fn transfer_name_rejects_paths_and_non_database_files() {
        assert_eq!(
            sqlite_logical_filename("snapshot.db").unwrap(),
            "snapshot.db"
        );
        for invalid in [
            "../snapshot.db",
            "nested/snapshot.db",
            "/tmp/snapshot.db",
            "C:\\temp\\snapshot.db",
            ".hidden.db",
            "snapshot.sqlite",
            "snapshot.db\n",
            "",
        ] {
            assert!(
                sqlite_logical_filename(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }
}
