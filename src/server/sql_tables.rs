//! Process-wide user-defined relational table store (CONCEPT:EG-018/EG-023).
//!
//! redb permits exactly ONE `Database` handle per file per process, so the wire
//! `Method::Sql` DDL/DML path AND the pgwire shim (CONCEPT:KG-2.189) must share the
//! SAME [`eg_query::TableStore`] — otherwise the two surfaces would race to open the
//! same `sql_tables.redb` and the loser would error. This module is that single
//! lazily-opened, process-global store; both surfaces resolve it here so a table
//! created over one wire is visible over the other.

use eg_query::TableStore;

/// Env override for the durable user-table store path (CONCEPT:EG-018). When unset it
/// derives from `GRAPH_SERVICE_PERSIST_DIR` (`<persist_dir>/sql_tables.redb`) so user
/// tables live beside the graph durable tier; absent that, a process-temp file.
pub const SQL_TABLES_PATH_ENV: &str = "EPISTEMIC_GRAPH_SQL_TABLES_PATH";

/// The process-wide user-table store. A `std::sync::Mutex<Option<…>>` serializes the
/// one-time open so two racing callers never double-open the file; the `TableStore`
/// itself is cheap to clone (an `Arc<Database>`).
static USER_TABLE_STORE: std::sync::Mutex<Option<TableStore>> = std::sync::Mutex::new(None);

/// Resolve the durable path for the user-table store (see [`SQL_TABLES_PATH_ENV`]).
pub fn sql_tables_path() -> std::path::PathBuf {
    if let Ok(p) = std::env::var(SQL_TABLES_PATH_ENV) {
        if !p.is_empty() {
            return std::path::PathBuf::from(p);
        }
    }
    if let Ok(dir) = std::env::var("GRAPH_SERVICE_PERSIST_DIR") {
        if !dir.is_empty() {
            return std::path::Path::new(&dir).join("sql_tables.redb");
        }
    }
    std::env::temp_dir().join("epistemic_graph_sql_tables.redb")
}

/// The shared user-table store, opening it on first use (CONCEPT:EG-018). Returns a
/// cheap clone of the single process-wide handle.
pub fn user_table_store() -> Result<TableStore, String> {
    let mut g = USER_TABLE_STORE
        .lock()
        .map_err(|_| "user table store mutex poisoned".to_string())?;
    if let Some(s) = g.as_ref() {
        return Ok(s.clone());
    }
    let path = sql_tables_path();
    let store =
        TableStore::open(&path).map_err(|e| format!("open user table store at {path:?}: {e}"))?;
    *g = Some(store.clone());
    Ok(store)
}
