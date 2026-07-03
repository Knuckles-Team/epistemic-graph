//! SQLite `.db` FILE import/export handler (CONCEPT:EG-331 / CONCEPT:EG-332) — the
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
//! Rows flow between the `.db` file and the engine's process-global user-table store
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

use eg_query::{Cell, Column, ColumnType, TableSchema, TableStore};
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value as JsonValue;

use crate::protocol::{Method, Response, ResultPayload};

/// Route the two SQLite-file methods. Resolves the process-global user-table store, then
/// runs the (blocking, file-I/O) import/export on the blocking pool so the reactor is
/// never stalled. `Err(method)` for a method that isn't ours (unreachable — dispatch only
/// routes the two variants here).
pub(crate) async fn try_handle(req_id: u64, method: Method) -> Result<Response, Method> {
    match method {
        Method::ImportSqliteFile { path } => {
            let store = match crate::server::sql_tables::user_table_store() {
                Ok(s) => s,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let out =
                tokio::task::spawn_blocking(move || import_sqlite_file(&store, &path)).await;
            Ok(match out {
                Ok(Ok(v)) => Response::ok(req_id, ResultPayload::Json(v)),
                Ok(Err(e)) => Response::err(req_id, e),
                Err(e) => Response::err(req_id, format!("sqlite import task join error: {e}")),
            })
        }
        Method::ExportSqliteFile { path, tables } => {
            let store = match crate::server::sql_tables::user_table_store() {
                Ok(s) => s,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let out = tokio::task::spawn_blocking(move || {
                export_sqlite_file(&store, &path, &tables)
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

// ── Import (CONCEPT:EG-331) ───────────────────────────────────────────────────

/// Read every user table (+ its rows) from the `sqlite3` `.db` file at `path` into
/// `store`. A same-name table already in the store is REPLACED (drop-then-recreate) so
/// the import mirrors the file. Returns `{"path", "imported_tables":[{"table","rows"},…]}`.
pub(crate) fn import_sqlite_file(store: &TableStore, path: &str) -> Result<JsonValue, String> {
    if !std::path::Path::new(path).exists() {
        return Err(format!("sqlite file `{path}` does not exist"));
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("open sqlite file `{path}`: {e}"))?;

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
    Ok(serde_json::json!({ "path": path, "imported_tables": report }))
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
        columns.push(Column::new(name.clone(), affinity_to_type(&decl), true, false));
        names.push(name);
    }
    if columns.is_empty() {
        return Err(format!("sqlite table `{table}` has no columns"));
    }
    Ok((
        TableSchema {
            name: table.to_string(),
            columns,
        },
        names,
    ))
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
    let ncols = schema.columns.len();
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
            jrow.push(sqlite_value_to_json(v, schema.columns[i].ty)?);
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
                .map_err(|_| format!("non-integer text `{s}` in an integer column")),
            ColumnType::Float | ColumnType::Double => s
                .trim()
                .parse::<f64>()
                .map(num_f64)
                .map_err(|_| format!("non-numeric text `{s}` in a real column")),
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

// ── Export (CONCEPT:EG-332) ───────────────────────────────────────────────────

/// Write the selected user tables OUT to a FRESH, valid `sqlite3` `.db` file at `path`
/// (the `sqlite3` CLI can open it). `tables` empty ⇒ every user table; else exactly the
/// named tables (each must exist). Any pre-existing file at `path` is overwritten.
/// Returns `{"path", "exported_tables":[{"table","rows"},…]}`.
pub(crate) fn export_sqlite_file(
    store: &TableStore,
    path: &str,
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

    // A fresh file: drop any existing `.db` (and its stray WAL/journal siblings) so the
    // export is a clean database, never appended onto stale content.
    remove_db_files(path)?;
    let mut conn =
        Connection::open(path).map_err(|e| format!("create sqlite file `{path}`: {e}"))?;

    let mut report = Vec::with_capacity(names.len());
    for table in &names {
        let schema = store
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        conn.execute(&export_ddl(&schema), [])
            .map_err(|e| format!("create table `{table}` in sqlite file: {e}"))?;
        // ONE scan per table (batch), then one bulk transaction of inserts.
        let rows = store.scan(table)?;
        let n = export_rows(&mut conn, &schema, &rows)?;
        report.push(serde_json::json!({ "table": table, "rows": n }));
    }
    // Flush pages to the file before reporting success (the connection drop also flushes).
    let _ = conn.cache_flush();
    drop(conn);
    Ok(serde_json::json!({ "path": path, "exported_tables": report }))
}

/// Remove a `.db` file and any leftover `-wal`/`-shm`/`-journal` siblings so a fresh
/// export never inherits stale pages.
fn remove_db_files(path: &str) -> Result<(), String> {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let p = format!("{path}{suffix}");
        let pp = std::path::Path::new(&p);
        if pp.exists() {
            std::fs::remove_file(pp).map_err(|e| format!("remove existing `{p}`: {e}"))?;
        }
    }
    Ok(())
}

/// The `CREATE TABLE` DDL for an engine schema, mapping each [`ColumnType`] back to a
/// SQLite declared type so a re-import round-trips its affinity.
fn export_ddl(schema: &TableSchema) -> String {
    let cols: Vec<String> = schema
        .columns
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
    let ncols = schema.columns.len();
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

    /// CONCEPT:EG-331 / CONCEPT:EG-332 — the full `.db` file round-trip: write a source
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
        let report = import_sqlite_file(&store, src.to_str().unwrap()).unwrap();
        assert_eq!(report["imported_tables"][0]["table"], "people");
        assert_eq!(report["imported_tables"][0]["rows"], 2);
        assert_eq!(store.scan("people").unwrap().len(), 2);

        // 3. Export the store back out to a fresh `.db`.
        let report2 = export_sqlite_file(&store, dst.to_str().unwrap(), &[]).unwrap();
        assert_eq!(report2["exported_tables"][0]["table"], "people");
        assert_eq!(report2["exported_tables"][0]["rows"], 2);

        // 4. Re-open the EXPORTED file with the sqlite library — this proves it is a
        //    valid, sqlite3-readable `.db` — and assert the rows round-tripped.
        let conn = Connection::open(&dst).unwrap();
        let mut stmt = conn
            .prepare("SELECT id, name, score, active FROM people ORDER BY id")
            .unwrap();
        let rows: Vec<(i64, String, Option<f64>, i64)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
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
}
