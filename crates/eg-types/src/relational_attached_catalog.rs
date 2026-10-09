//! Typed catalog model for an attached Oracle or Db2 source
//! (EG-UNIFIED-DATA-PLANE-R018): the schema.table catalog entries EG reads
//! through the driver/ODBC path for either engine, and whether a table is
//! captured through the Debezium bridge. This is the typed-model slice
//! (`.1`): the catalog model plus the refusal of a schema or table
//! identifier that cannot be safely double-quoted (both engines delimit
//! identifiers with `"`), and the refusal of a table marked as
//! Debezium-captured with no declared primary key (log-based capture has
//! nothing to key rows by). The driver/ODBC connection, catalog read, and
//! Debezium bridge wiring are later children.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Which of the two engines a catalog entry was read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationalEngine {
    Oracle,
    Db2,
}

/// One column of an attached Oracle/Db2 table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationalColumn {
    pub name: String,
    pub sql_type: String,
    pub is_primary_key: bool,
}

/// One discovered Oracle/Db2 table, named by its `schema.table` pair.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationalTableCatalog {
    pub engine: RelationalEngine,
    pub schema: String,
    pub table: String,
    pub columns: Vec<RelationalColumn>,
    pub captured_via_debezium: bool,
}

impl RelationalTableCatalog {
    /// The double-quoted two-part name (`"schema"."table"`) both engines
    /// accept. Only safe to call after `validate_table` accepts the entry.
    pub fn qualified_name(&self) -> String {
        format!("\"{}\".\"{}\"", self.schema, self.table)
    }
}

/// The catalog for one attached Oracle or Db2 source: every discovered
/// table, keyed by engine and qualified name. Pure data — the driver/ODBC
/// connection and Debezium bridge wiring are later children.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationalSourceCatalog {
    pub tables: BTreeMap<String, RelationalTableCatalog>,
}

/// A table catalog entry failed validation. Carries the offending qualified
/// name and the reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidRelationalTable {
    pub qualified_name: String,
    pub reason: String,
}

impl std::fmt::Display for InvalidRelationalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid Oracle/Db2 table catalog for {:?}: {}",
            self.qualified_name, self.reason
        )
    }
}

impl std::error::Error for InvalidRelationalTable {}

/// Confirm a schema/table identifier is safe to double-quote: non-empty,
/// free of `"` (would close the quote early) and NUL bytes.
fn validate_identifier(identifier: &str) -> bool {
    !identifier.is_empty() && !identifier.contains('"') && !identifier.contains('\0')
}

/// Confirm a table catalog entry is well-formed: both identifiers are safe
/// to double-quote, and a Debezium-captured table declares at least one
/// primary-key column (log-based capture has nothing to key rows by
/// otherwise). Refuses rather than silently admitting an unsafe entry.
pub fn validate_table(table: &RelationalTableCatalog) -> Result<(), InvalidRelationalTable> {
    let qualified_name = format!("{}.{}", table.schema, table.table);
    if !validate_identifier(&table.schema) || !validate_identifier(&table.table) {
        return Err(InvalidRelationalTable {
            qualified_name,
            reason: "schema/table identifier is empty or contains '\"' or a NUL byte".to_string(),
        });
    }
    if table.captured_via_debezium && !table.columns.iter().any(|c| c.is_primary_key) {
        return Err(InvalidRelationalTable {
            qualified_name,
            reason: "Debezium-captured table declares no primary-key column".to_string(),
        });
    }
    Ok(())
}

impl RelationalSourceCatalog {
    /// Insert a discovered table, refusing an invalid entry rather than
    /// admitting it into the catalog.
    pub fn insert_table(
        &mut self,
        table: RelationalTableCatalog,
    ) -> Result<(), InvalidRelationalTable> {
        validate_table(&table)?;
        let key = table.qualified_name();
        self.tables.insert(key, table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(engine: RelationalEngine, captured: bool, has_pk: bool) -> RelationalTableCatalog {
        RelationalTableCatalog {
            engine,
            schema: "APP".to_string(),
            table: "ORDERS".to_string(),
            columns: vec![RelationalColumn {
                name: "ID".to_string(),
                sql_type: "NUMBER".to_string(),
                is_primary_key: has_pk,
            }],
            captured_via_debezium: captured,
        }
    }

    #[test]
    fn valid_table_round_trips_through_insert() {
        let mut catalog = RelationalSourceCatalog::default();
        catalog
            .insert_table(table(RelationalEngine::Oracle, true, true))
            .unwrap();
        assert!(catalog.tables.contains_key("\"APP\".\"ORDERS\""));
    }

    #[test]
    fn quote_in_identifier_is_refused() {
        let mut bad = table(RelationalEngine::Db2, false, false);
        bad.schema = "APP\"; DROP".to_string();
        assert!(validate_table(&bad).is_err());
    }

    #[test]
    fn nul_byte_in_identifier_is_refused() {
        let mut bad = table(RelationalEngine::Db2, false, false);
        bad.table = "ORDERS\0".to_string();
        assert!(validate_table(&bad).is_err());
    }

    #[test]
    fn debezium_captured_table_with_no_primary_key_is_refused() {
        let bad = table(RelationalEngine::Oracle, true, false);
        let Err(error) = validate_table(&bad) else {
            panic!("a Debezium-captured table with no primary key must be refused");
        };
        assert!(error.reason.contains("primary-key"), "{error}");
    }

    #[test]
    fn non_captured_table_with_no_primary_key_is_valid() {
        let ok = table(RelationalEngine::Db2, false, false);
        assert!(validate_table(&ok).is_ok());
    }

    #[test]
    fn invalid_table_is_refused_not_silently_inserted() {
        let mut catalog = RelationalSourceCatalog::default();
        let before = catalog.tables.len();
        let result = catalog.insert_table(table(RelationalEngine::Oracle, true, false));
        assert!(result.is_err());
        assert_eq!(catalog.tables.len(), before);
    }

    #[test]
    fn catalog_serializes_round_trip() {
        let mut catalog = RelationalSourceCatalog::default();
        catalog
            .insert_table(table(RelationalEngine::Db2, false, true))
            .unwrap();

        let encoded = serde_json::to_string(&catalog).unwrap();
        let decoded: RelationalSourceCatalog = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, catalog);
    }
}
