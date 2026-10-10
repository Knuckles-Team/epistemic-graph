//! Typed catalog model for an attached ClickHouse source
//! (EG-UNIFIED-DATA-PLANE-R017): the `system.*`-derived table catalog EG
//! federates against, and its acceleration eligibility. This is the
//! typed-model slice (`.1`): the catalog model, and
//! [`ClickHouseTableCatalog::capture_capability`], which always returns the
//! single explicit [`ClickHouseCaptureCapability::Unsupported`] value rather
//! than a silently-missing `Option<_>` — ClickHouse has no change-capture
//! capability at all, and a capture request must get an explicit unsupported
//! result, never a quiet no-op. The HTTP/native client connection and
//! federated query pushdown are later children.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The table engine family, as reported by `system.tables.engine`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClickHouseEngineFamily {
    MergeTree,
    ReplacingMergeTree,
    Memory,
    Distributed,
    Other,
}

/// One column of an attached ClickHouse table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseColumn {
    pub name: String,
    pub column_type: String,
}

/// The one explicit value a change-capture request against ClickHouse ever
/// gets. A closed enum (rather than a boolean or `Option`) so the caller
/// cannot mistake "unsupported" for "not yet checked".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClickHouseCaptureCapability {
    Unsupported,
}

/// One discovered ClickHouse table and its acceleration eligibility.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseTableCatalog {
    pub database: String,
    pub table: String,
    pub engine: ClickHouseEngineFamily,
    pub columns: Vec<ClickHouseColumn>,
    pub supports_acceleration: bool,
}

impl ClickHouseTableCatalog {
    /// ClickHouse's change-capture capability for this table: always
    /// `Unsupported`. Exists so a caller asks the model explicitly instead
    /// of assuming capture is available because nothing said otherwise.
    pub fn capture_capability(&self) -> ClickHouseCaptureCapability {
        ClickHouseCaptureCapability::Unsupported
    }
}

/// The catalog for one attached ClickHouse source: every discovered table,
/// keyed by its database-qualified name. Pure data — the HTTP/native client
/// connection and federated pushdown are later children.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseSourceCatalog {
    pub tables: BTreeMap<String, ClickHouseTableCatalog>,
}

/// A ClickHouse table catalog entry failed validation. Carries the offending
/// database-qualified name and the reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidClickHouseTable {
    pub qualified_name: String,
    pub reason: String,
}

impl std::fmt::Display for InvalidClickHouseTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid ClickHouse table catalog for {:?}: {}",
            self.qualified_name, self.reason
        )
    }
}

impl std::error::Error for InvalidClickHouseTable {}

/// Confirm a table catalog entry is well-formed: non-empty database and
/// table name, and at least one discovered column (an entry with none is
/// not a real catalog read, and must be refused rather than admitted as an
/// empty table).
pub fn validate_table(table: &ClickHouseTableCatalog) -> Result<(), InvalidClickHouseTable> {
    let qualified_name = format!("{}.{}", table.database, table.table);
    if table.database.is_empty() || table.table.is_empty() {
        return Err(InvalidClickHouseTable {
            qualified_name,
            reason: "database and table name must both be non-empty".to_string(),
        });
    }
    if table.columns.is_empty() {
        return Err(InvalidClickHouseTable {
            qualified_name,
            reason: "table has no discovered columns".to_string(),
        });
    }
    Ok(())
}

impl ClickHouseSourceCatalog {
    /// Insert a discovered table, refusing an invalid entry rather than
    /// admitting it into the catalog.
    pub fn insert_table(
        &mut self,
        table: ClickHouseTableCatalog,
    ) -> Result<(), InvalidClickHouseTable> {
        validate_table(&table)?;
        let key = format!("{}.{}", table.database, table.table);
        self.tables.insert(key, table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> ClickHouseTableCatalog {
        ClickHouseTableCatalog {
            database: "analytics".to_string(),
            table: "events".to_string(),
            engine: ClickHouseEngineFamily::MergeTree,
            columns: vec![ClickHouseColumn {
                name: "event_id".to_string(),
                column_type: "UInt64".to_string(),
            }],
            supports_acceleration: true,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.1
    #[test]
    fn valid_table_round_trips_through_insert() {
        let mut catalog = ClickHouseSourceCatalog::default();
        catalog.insert_table(table()).unwrap();
        assert!(catalog.tables.contains_key("analytics.events"));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.1
    #[test]
    fn capture_capability_is_always_explicitly_unsupported() {
        assert_eq!(
            table().capture_capability(),
            ClickHouseCaptureCapability::Unsupported
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.1
    #[test]
    fn empty_database_or_table_name_is_refused() {
        let mut bad = table();
        bad.database = String::new();
        assert!(validate_table(&bad).is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.1
    #[test]
    fn table_with_no_columns_is_refused() {
        let mut bad = table();
        bad.columns.clear();
        let Err(error) = validate_table(&bad) else {
            panic!("a table with no discovered columns must be refused");
        };
        assert!(error.reason.contains("no discovered columns"), "{error}");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.1
    #[test]
    fn invalid_table_is_refused_not_silently_inserted() {
        let mut catalog = ClickHouseSourceCatalog::default();
        let before = catalog.tables.len();
        let mut bad = table();
        bad.columns.clear();
        let result = catalog.insert_table(bad);
        assert!(result.is_err());
        assert_eq!(catalog.tables.len(), before);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.1
    #[test]
    fn catalog_serializes_round_trip() {
        let mut catalog = ClickHouseSourceCatalog::default();
        catalog.insert_table(table()).unwrap();

        let encoded = serde_json::to_string(&catalog).unwrap();
        let decoded: ClickHouseSourceCatalog = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, catalog);
    }
}
