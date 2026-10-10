//! Typed catalog model for an attached SQLite file source
//! (EG-UNIFIED-DATA-PLANE-R015): which tables a live SQLite database exposes,
//! whether each is `WITHOUT ROWID`, and which change-capture mode each table
//! can safely use. This is the typed-model slice (`.1`): the catalog model,
//! its capture-mode vocabulary, and the refusal of a `WITHOUT ROWID` table
//! that declares no primary key (nothing — not even a watermark poll — can
//! identify its rows) or that claims WAL-frame/rowid tailing (a `WITHOUT
//! ROWID` table has no implicit rowid to tail). The lock-safe live attach,
//! WAL-frame tailing, and watermark polling are later children.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A SQLite column's storage class, per SQLite's type affinity rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteColumnType {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

/// One column of an attached SQLite table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteColumn {
    pub name: String,
    pub column_type: SqliteColumnType,
    pub is_primary_key: bool,
}

/// How change capture can safely observe one table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteCaptureMode {
    /// Tail WAL frames keyed by the table's implicit rowid.
    WalRowidTailing,
    /// Poll a declared primary-key/updated_at watermark.
    WatermarkPolling,
}

/// One discovered SQLite table, including whether it is `WITHOUT ROWID` and
/// which capture mode it is declared to use.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteTableCatalog {
    pub table: String,
    pub without_rowid: bool,
    pub columns: Vec<SqliteColumn>,
    pub capture_mode: SqliteCaptureMode,
}

/// The catalog for one attached SQLite file source: every discovered table,
/// keyed by name. Pure data — the live read-only attach and the real WAL
/// tailing/watermark polling are later children.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteSourceCatalog {
    pub database_path: String,
    pub tables: BTreeMap<String, SqliteTableCatalog>,
}

/// A SQLite table catalog entry failed validation. Carries the offending
/// table name and the reason so a caller can report it without re-deriving
/// it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidSqliteTable {
    pub table: String,
    pub reason: String,
}

impl std::fmt::Display for InvalidSqliteTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid SQLite table catalog for {:?}: {}",
            self.table, self.reason
        )
    }
}

impl std::error::Error for InvalidSqliteTable {}

/// Confirm a table catalog entry is well-formed: a `WITHOUT ROWID` table must
/// declare at least one primary-key column (it has no implicit rowid to fall
/// back on), and must not claim WAL/rowid tailing (there is no rowid to
/// tail). Refuses rather than silently admitting an unsafe capture plan.
pub fn validate_table(table: &SqliteTableCatalog) -> Result<(), InvalidSqliteTable> {
    if table.without_rowid {
        let has_primary_key = table.columns.iter().any(|c| c.is_primary_key);
        if !has_primary_key {
            return Err(InvalidSqliteTable {
                table: table.table.clone(),
                reason: "WITHOUT ROWID table declares no primary-key column".to_string(),
            });
        }
        if table.capture_mode == SqliteCaptureMode::WalRowidTailing {
            return Err(InvalidSqliteTable {
                table: table.table.clone(),
                reason: "WITHOUT ROWID table has no implicit rowid to tail via WAL frames"
                    .to_string(),
            });
        }
    }
    Ok(())
}

impl SqliteSourceCatalog {
    /// Insert a discovered table, refusing an invalid entry rather than
    /// admitting it into the catalog.
    pub fn insert_table(&mut self, table: SqliteTableCatalog) -> Result<(), InvalidSqliteTable> {
        validate_table(&table)?;
        self.tables.insert(table.table.clone(), table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk_column() -> SqliteColumn {
        SqliteColumn {
            name: "id".to_string(),
            column_type: SqliteColumnType::Integer,
            is_primary_key: true,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.1
    #[test]
    fn rowid_table_with_wal_tailing_round_trips_through_insert() {
        let mut catalog = SqliteSourceCatalog {
            database_path: "/data/app.db".to_string(),
            tables: BTreeMap::new(),
        };
        let table = SqliteTableCatalog {
            table: "events".to_string(),
            without_rowid: false,
            columns: vec![pk_column()],
            capture_mode: SqliteCaptureMode::WalRowidTailing,
        };
        catalog.insert_table(table).unwrap();
        assert!(catalog.tables.contains_key("events"));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.1
    #[test]
    fn without_rowid_table_with_pk_and_watermark_is_valid() {
        let table = SqliteTableCatalog {
            table: "settings".to_string(),
            without_rowid: true,
            columns: vec![pk_column()],
            capture_mode: SqliteCaptureMode::WatermarkPolling,
        };
        assert!(validate_table(&table).is_ok());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.1
    #[test]
    fn without_rowid_table_with_no_primary_key_is_refused() {
        let table = SqliteTableCatalog {
            table: "orphan".to_string(),
            without_rowid: true,
            columns: vec![],
            capture_mode: SqliteCaptureMode::WatermarkPolling,
        };
        assert!(validate_table(&table).is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.1
    #[test]
    fn without_rowid_table_claiming_wal_tailing_is_refused() {
        let table = SqliteTableCatalog {
            table: "settings".to_string(),
            without_rowid: true,
            columns: vec![pk_column()],
            capture_mode: SqliteCaptureMode::WalRowidTailing,
        };
        let Err(error) = validate_table(&table) else {
            panic!("WAL tailing on a WITHOUT ROWID table must be refused");
        };
        assert!(error.reason.contains("no implicit rowid"), "{error}");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.1
    #[test]
    fn invalid_table_is_refused_not_silently_inserted() {
        let mut catalog = SqliteSourceCatalog::default();
        let before = catalog.tables.len();
        let result = catalog.insert_table(SqliteTableCatalog {
            table: "orphan".to_string(),
            without_rowid: true,
            columns: vec![],
            capture_mode: SqliteCaptureMode::WatermarkPolling,
        });
        assert!(result.is_err());
        assert_eq!(catalog.tables.len(), before);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.1
    #[test]
    fn capture_mode_is_a_distinct_variant_not_collapsed() {
        assert_ne!(
            SqliteCaptureMode::WalRowidTailing,
            SqliteCaptureMode::WatermarkPolling
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.1
    #[test]
    fn catalog_serializes_round_trip() {
        let mut catalog = SqliteSourceCatalog {
            database_path: "/data/app.db".to_string(),
            tables: BTreeMap::new(),
        };
        catalog
            .insert_table(SqliteTableCatalog {
                table: "events".to_string(),
                without_rowid: false,
                columns: vec![pk_column()],
                capture_mode: SqliteCaptureMode::WalRowidTailing,
            })
            .unwrap();

        let encoded = serde_json::to_string(&catalog).unwrap();
        let decoded: SqliteSourceCatalog = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, catalog);
    }
}
