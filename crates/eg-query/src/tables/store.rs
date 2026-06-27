//! The redb-backed user-table store (CONCEPT:EG-018) — the durable, ACID-ish home
//! for ARBITRARY user-defined relational tables (`CREATE TABLE` for Prometheus
//! metrics, Langfuse time-series, stock bars, connector mirrors, ETL outputs).
//!
//! ## redb layout (one `Database` file, three system tables)
//!   * `__sql_catalog__`  `table_name              -> MessagePack(TableSchema)`
//!     The catalog. One row per user table, recording its full typed schema. This is
//!     the system table the task calls for: name → (columns + Arrow/SQL types +
//!     nullability + PK).
//!   * `__sql_rows__`     `(table_name, rowid: u64) -> MessagePack(Vec<Cell>)`
//!     Every user table's rows live in ONE physical redb table, namespaced by the
//!     logical table name in the composite key. This sidesteps redb's
//!     `&'static str` table-name constraint (a runtime `CREATE TABLE name` cannot
//!     mint a new redb `TableDefinition`) while keeping each logical table's rows
//!     contiguous in key order for an efficient prefix range-scan.
//!   * `__sql_seq__`      `table_name              -> next rowid (u64)`
//!     A per-table monotonic rowid allocator — the internal row identity (also the
//!     seed for a future `SERIAL`/sequence follow-up).
//!
//! ## Durability (ACID-ish, commit-before-ack)
//! Every mutation (`CREATE`/`DROP`/`ALTER`/`INSERT`/`UPDATE`/`DELETE`) runs in ONE
//! redb `WriteTransaction` committed at `Durability::Immediate` BEFORE the method
//! returns — so the caller (the pgwire shim) only acks a write that is already on
//! disk. A mid-transaction error drops the txn without `commit()`, so redb discards
//! every staged write (true rollback, no partial). Reads use a fresh read txn — a
//! consistent snapshot of the committed state. Persists across restart: reopening the
//! same path recovers the catalog and all rows verbatim (the round-trip test proves
//! it via a simulated reopen).

use std::path::Path;
use std::sync::Arc;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::Value;

use super::schema::{Cell, Column, TableSchema};

/// Catalog system table: `table_name -> MessagePack(TableSchema)`.
const CATALOG: TableDefinition<&str, &[u8]> = TableDefinition::new("__sql_catalog__");
/// Row store: `(table_name, rowid) -> MessagePack(Vec<Cell>)`.
const ROWS: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("__sql_rows__");
/// Per-table rowid allocator: `table_name -> next rowid`.
const SEQ: TableDefinition<&str, u64> = TableDefinition::new("__sql_seq__");

/// A single-column equality predicate — the WHERE shape the first increment of
/// user-table UPDATE/DELETE resolves (`WHERE <col> = <literal>`), mirroring the
/// `nodes` DML policy. Composite predicates are a follow-up.
#[derive(Debug, Clone, PartialEq)]
pub struct ColEq {
    pub column: String,
    pub value: Value,
}

/// The durable user-table store. Cheap to clone (`Arc<Database>`), so the pgwire
/// shim can hold one process-wide and hand a clone to each connection.
#[derive(Clone)]
pub struct TableStore {
    db: Arc<Database>,
}

impl TableStore {
    /// Open (creating if absent) the user-table store at `path`. The redb database
    /// file is created on first open; reopening the same path recovers all tables.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let db = Database::create(path).map_err(|e| format!("open sql table store: {e}"))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Open a fresh store at a unique temp path — for tests and ephemeral use. The
    /// file is NOT auto-removed (a unique name per call); a simulated "reopen" is
    /// just [`TableStore::open`] on the returned path.
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

    // ── DDL ────────────────────────────────────────────────────────────────

    /// `CREATE TABLE`: record `schema` in the catalog. Returns `Ok(true)` when the
    /// table was created, `Ok(false)` when it already existed and `if_not_exists`
    /// was set (a no-op). An existing table without `IF NOT EXISTS` is an error.
    pub fn create_table(&self, schema: &TableSchema, if_not_exists: bool) -> Result<bool, String> {
        let exists = self.get_schema(&schema.name)?.is_some();
        if exists {
            if if_not_exists {
                return Ok(false);
            }
            return Err(format!("table `{}` already exists", schema.name));
        }
        let blob = rmp_serde::to_vec_named(schema).map_err(|e| format!("encode schema: {e}"))?;
        let wtx = self.begin()?;
        {
            let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
            cat.insert(schema.name.as_str(), blob.as_slice())
                .map_err(map_err)?;
            let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
            seq.insert(schema.name.as_str(), 0u64).map_err(map_err)?;
        }
        wtx.commit().map_err(map_err)?;
        Ok(true)
    }

    /// `DROP TABLE`: remove the catalog entry, the rowid allocator, and EVERY row of
    /// the table in one transaction. Returns `Ok(true)` when a table was dropped,
    /// `Ok(false)` when it was absent and `if_exists` was set. A missing table
    /// without `IF EXISTS` is an error.
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<bool, String> {
        if self.get_schema(name)?.is_none() {
            if if_exists {
                return Ok(false);
            }
            return Err(format!("table `{name}` does not exist"));
        }
        let wtx = self.begin()?;
        {
            let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
            cat.remove(name).map_err(map_err)?;
            let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
            seq.remove(name).map_err(map_err)?;
            // Delete the table's row range `(name, 0)..=(name, u64::MAX)`.
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
        wtx.commit().map_err(map_err)?;
        Ok(true)
    }

    /// `ALTER TABLE ADD COLUMN`: append `column` to the table's schema. Existing rows
    /// are NOT rewritten — they are shorter than the new schema, and the scan path
    /// back-fills a trailing missing cell as NULL (so a freshly added column reads as
    /// NULL for every pre-existing row, exactly like Postgres adding a nullable
    /// column without a default). A duplicate column name is rejected.
    pub fn add_column(&self, table: &str, column: Column) -> Result<(), String> {
        let mut schema = self
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        if schema.column(&column.name).is_some() {
            return Err(format!(
                "column `{}` already exists in table `{table}`",
                column.name
            ));
        }
        schema.columns.push(column);
        let blob = rmp_serde::to_vec_named(&schema).map_err(|e| format!("encode schema: {e}"))?;
        let wtx = self.begin()?;
        {
            let mut cat = wtx.open_table(CATALOG).map_err(map_err)?;
            cat.insert(table, blob.as_slice()).map_err(map_err)?;
        }
        wtx.commit().map_err(map_err)?;
        Ok(())
    }

    // ── catalog reads ────────────────────────────────────────────────────────

    /// The schema of `name`, or `None` if no such user table exists.
    pub fn get_schema(&self, name: &str) -> Result<Option<TableSchema>, String> {
        let rtx = self.db.begin_read().map_err(map_err)?;
        let cat = match rtx.open_table(CATALOG) {
            Ok(t) => t,
            // Table-not-yet-created (fresh db) → no catalog table → no schema.
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

    /// The names of every user table in the catalog (sorted for determinism).
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

    /// Every row of `table`, each as a schema-aligned `Vec<Cell>` (a row shorter than
    /// the current schema — written before an `ADD COLUMN` — is NULL-padded to the
    /// schema width). Errors if the table does not exist.
    pub fn scan(&self, table: &str) -> Result<Vec<Vec<Cell>>, String> {
        let schema = self
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        let width = schema.columns.len();
        let rtx = self.db.begin_read().map_err(map_err)?;
        let rows = rtx.open_table(ROWS).map_err(map_err)?;
        let mut out = Vec::new();
        for r in rows
            .range((table, 0u64)..=(table, u64::MAX))
            .map_err(map_err)?
        {
            let (_, v) = r.map_err(map_err)?;
            let mut cells: Vec<Cell> =
                rmp_serde::from_slice(v.value()).map_err(|e| format!("decode row: {e}"))?;
            // Back-fill columns added after this row was written.
            if cells.len() < width {
                cells.resize(width, Cell::Null);
            }
            out.push(cells);
        }
        Ok(out)
    }

    // ── DML ───────────────────────────────────────────────────────────────────

    /// `INSERT INTO table (cols...) VALUES ...`. `col_order` names the columns each
    /// row in `rows` supplies (in order); a column of the schema NOT named is filled
    /// NULL (and must be nullable). Each supplied value is coerced to its column type
    /// (a mismatch aborts the whole INSERT — nothing is committed). Returns the row
    /// count. Multi-row INSERT is just `rows.len() > 1`; an INSERT…SELECT caller
    /// passes the projected SELECT rows here.
    pub fn insert_rows(
        &self,
        table: &str,
        col_order: &[String],
        rows: &[Vec<Value>],
    ) -> Result<usize, String> {
        let schema = self
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        // Resolve each named insert column to a schema column index up front.
        let mut targets = Vec::with_capacity(col_order.len());
        for name in col_order {
            let idx = schema
                .column_index(name)
                .ok_or_else(|| format!("column `{name}` does not exist in table `{table}`"))?;
            targets.push(idx);
        }
        // Build every row's cells BEFORE opening the write txn so a coercion error
        // never leaves a half-applied INSERT.
        let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != col_order.len() {
                return Err(format!(
                    "INSERT column/value count mismatch: {} columns, {} values",
                    col_order.len(),
                    row.len()
                ));
            }
            // Default every cell to NULL, then place the supplied values.
            let mut cells: Vec<Cell> = vec![Cell::Null; schema.columns.len()];
            // Validate the NOT-NULL columns that were NOT supplied.
            for (ci, col) in schema.columns.iter().enumerate() {
                if !col.nullable && !targets.contains(&ci) {
                    return Err(format!(
                        "column `{}` is NOT NULL and was not supplied",
                        col.name
                    ));
                }
            }
            for (val, &idx) in row.iter().zip(targets.iter()) {
                let col = &schema.columns[idx];
                cells[idx] = Cell::coerce(val, col.ty, col.nullable)?;
            }
            encoded.push(rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?);
        }
        let wtx = self.begin()?;
        let n = encoded.len();
        {
            let mut next = {
                let seq = wtx.open_table(SEQ).map_err(map_err)?;
                let v = seq.get(table).map_err(map_err)?.map(|g| g.value()).unwrap_or(0);
                v
            };
            let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
            for blob in &encoded {
                rows_t.insert((table, next), blob.as_slice()).map_err(map_err)?;
                next += 1;
            }
            let mut seq = wtx.open_table(SEQ).map_err(map_err)?;
            seq.insert(table, next).map_err(map_err)?;
        }
        wtx.commit().map_err(map_err)?;
        Ok(n)
    }

    /// `UPDATE table SET <set> WHERE <col> = <value>`. Each matched row's named
    /// columns are coerced + replaced; non-matched rows are untouched. Returns the
    /// number of rows updated. A whole-table UPDATE (no predicate) is intentionally
    /// not exposed here — the classifier requires a WHERE, mirroring `nodes`.
    pub fn update_where(
        &self,
        table: &str,
        set: &serde_json::Map<String, Value>,
        selector: &ColEq,
    ) -> Result<usize, String> {
        let schema = self
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        let sel_idx = schema
            .column_index(&selector.column)
            .ok_or_else(|| format!("column `{}` does not exist", selector.column))?;
        // Resolve + coerce the SET assignments to (index, Cell) up front.
        let mut assigns: Vec<(usize, Cell)> = Vec::with_capacity(set.len());
        for (col, val) in set {
            let idx = schema
                .column_index(col)
                .ok_or_else(|| format!("column `{col}` does not exist in table `{table}`"))?;
            let c = &schema.columns[idx];
            assigns.push((idx, Cell::coerce(val, c.ty, c.nullable)?));
        }
        let width = schema.columns.len();
        let wtx = self.begin()?;
        let mut updated = 0usize;
        {
            let mut rows_t = wtx.open_table(ROWS).map_err(map_err)?;
            // Collect matching rowids first (can't mutate while holding the range iter).
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
                let blob =
                    rmp_serde::to_vec_named(&cells).map_err(|e| format!("encode row: {e}"))?;
                rows_t.insert((table, rowid), blob.as_slice()).map_err(map_err)?;
                updated += 1;
            }
        }
        wtx.commit().map_err(map_err)?;
        Ok(updated)
    }

    /// `DELETE FROM table WHERE <col> = <value>`. Returns the number of rows removed.
    pub fn delete_where(&self, table: &str, selector: &ColEq) -> Result<usize, String> {
        let schema = self
            .get_schema(table)?
            .ok_or_else(|| format!("table `{table}` does not exist"))?;
        let sel_idx = schema
            .column_index(&selector.column)
            .ok_or_else(|| format!("column `{}` does not exist", selector.column))?;
        let width = schema.columns.len();
        let wtx = self.begin()?;
        let mut deleted = 0usize;
        {
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
        }
        wtx.commit().map_err(map_err)?;
        Ok(deleted)
    }

    /// Begin an immediate-durability write transaction (commit-before-ack).
    fn begin(&self) -> Result<redb::WriteTransaction, String> {
        let mut wtx = self.db.begin_write().map_err(map_err)?;
        wtx.set_durability(Durability::Immediate).map_err(map_err)?;
        Ok(wtx)
    }
}

/// redb error → flat string (the crate-wide error convention here).
fn map_err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables::schema::ColumnType;

    fn col(name: &str, ty: ColumnType, nullable: bool) -> Column {
        Column {
            name: name.to_string(),
            ty,
            nullable,
            primary_key: false,
        }
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
            .insert_rows("metrics", &cols, &[vec![1i64.into(), "cpu".into(), 0.5.into()]])
            .unwrap();
        assert!(store.drop_table("metrics", false).unwrap());
        assert!(store.get_schema("metrics").unwrap().is_none());
        // A second drop without IF EXISTS errors; with IF EXISTS it is a no-op.
        assert!(store.drop_table("metrics", false).is_err());
        assert!(!store.drop_table("metrics", true).unwrap());
    }

    #[test]
    fn alter_add_column_backfills_null() {
        let (store, _p) = TableStore::open_temp().unwrap();
        store.create_table(&metrics_schema(), false).unwrap();
        let cols = vec!["ts".to_string(), "name".to_string(), "value".to_string()];
        store
            .insert_rows("metrics", &cols, &[vec![1i64.into(), "cpu".into(), 0.5.into()]])
            .unwrap();
        store
            .add_column("metrics", col("labels", ColumnType::Json, true))
            .unwrap();
        let rows = store.scan("metrics").unwrap();
        assert_eq!(rows[0].len(), 4);
        assert_eq!(rows[0][3], Cell::Null, "pre-existing row reads new column NULL");
        // A new insert can populate the added column.
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
            .insert_rows("metrics", &cols, &[vec![42i64.into(), "cpu".into(), 1.0.into()]])
            .unwrap();
        drop(store);
        // Simulated restart: reopen the SAME file.
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
        // `ts` and `name` are NOT NULL; supplying only `value` must error.
        let err = store
            .insert_rows("metrics", &["value".to_string()], &[vec![0.1.into()]])
            .unwrap_err();
        assert!(err.contains("NOT NULL"), "{err}");
    }
}
