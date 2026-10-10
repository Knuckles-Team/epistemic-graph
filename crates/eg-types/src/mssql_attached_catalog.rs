//! Typed catalog model for an attached Microsoft SQL Server source
//! (EG-UNIFIED-DATA-PLANE-R016): the two-part `schema.table` catalog entries
//! EG reads from `sys.*`, and which change-capture mode each table is
//! declared to use (CDC, Change Tracking, or neither). This is the
//! typed-model slice (`.1`): the catalog model plus the refusal of a schema
//! or table identifier that cannot be safely bracket-quoted into T-SQL
//! (`[schema].[table]`) — an identifier containing a `]` or a NUL byte would
//! break out of the bracket-quoting the renderer uses. The tiberius driver
//! connection, T-SQL rendering, and LSN-polled CDC/Change Tracking capture
//! are later children.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// How change capture observes one table, or that it cannot yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MssqlCaptureMode {
    Cdc,
    ChangeTracking,
    Unsupported,
}

/// One column of an attached SQL Server table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MssqlColumn {
    pub name: String,
    pub sql_type: String,
    pub is_primary_key: bool,
}

/// One discovered SQL Server table, named by its two-part `schema.table`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MssqlTableCatalog {
    pub schema: String,
    pub table: String,
    pub columns: Vec<MssqlColumn>,
    pub capture_mode: MssqlCaptureMode,
}

impl MssqlTableCatalog {
    /// The bracket-quoted two-part name (`[schema].[table]`) the T-SQL
    /// renderer uses. Only safe to call after `validate_table` accepts the
    /// entry.
    pub fn qualified_name(&self) -> String {
        format!("[{}].[{}]", self.schema, self.table)
    }
}

/// The catalog for one attached SQL Server source: every discovered table,
/// keyed by its two-part name. Pure data — the tiberius connection, T-SQL
/// rendering, and LSN-polled capture are later children.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MssqlSourceCatalog {
    pub database: String,
    pub tables: BTreeMap<String, MssqlTableCatalog>,
}

/// A schema or table identifier failed validation. Carries the offending
/// identifier so a caller can report it without re-deriving it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidMssqlIdentifier(pub String);

impl std::fmt::Display for InvalidMssqlIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid SQL Server identifier: {:?}", self.0)
    }
}

impl std::error::Error for InvalidMssqlIdentifier {}

/// Confirm a schema/table identifier is safe to bracket-quote into T-SQL:
/// non-empty, and free of `]` (would close the bracket early) and NUL bytes.
/// Refuses rather than silently rendering a broken or injectable identifier.
pub fn validate_identifier(identifier: &str) -> Result<(), InvalidMssqlIdentifier> {
    if identifier.is_empty() || identifier.contains(']') || identifier.contains('\0') {
        return Err(InvalidMssqlIdentifier(identifier.to_string()));
    }
    Ok(())
}

/// Confirm a table catalog entry's schema and table identifiers are both
/// safe to bracket-quote.
pub fn validate_table(table: &MssqlTableCatalog) -> Result<(), InvalidMssqlIdentifier> {
    validate_identifier(&table.schema)?;
    validate_identifier(&table.table)?;
    Ok(())
}

impl MssqlSourceCatalog {
    /// Insert a discovered table, refusing an entry with an unsafe
    /// identifier rather than admitting it into the catalog.
    pub fn insert_table(&mut self, table: MssqlTableCatalog) -> Result<(), InvalidMssqlIdentifier> {
        validate_table(&table)?;
        let key = table.qualified_name();
        self.tables.insert(key, table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(schema: &str, name: &str, mode: MssqlCaptureMode) -> MssqlTableCatalog {
        MssqlTableCatalog {
            schema: schema.to_string(),
            table: name.to_string(),
            columns: vec![MssqlColumn {
                name: "id".to_string(),
                sql_type: "int".to_string(),
                is_primary_key: true,
            }],
            capture_mode: mode,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.1
    #[test]
    fn valid_table_round_trips_through_insert() {
        let mut catalog = MssqlSourceCatalog {
            database: "AppDb".to_string(),
            tables: BTreeMap::new(),
        };
        catalog
            .insert_table(table("dbo", "orders", MssqlCaptureMode::Cdc))
            .unwrap();
        assert!(catalog.tables.contains_key("[dbo].[orders]"));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.1
    #[test]
    fn empty_identifier_is_refused() {
        assert_eq!(
            validate_identifier(""),
            Err(InvalidMssqlIdentifier(String::new()))
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.1
    #[test]
    fn bracket_in_identifier_is_refused() {
        assert!(validate_identifier("orders]; DROP").is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.1
    #[test]
    fn nul_byte_in_identifier_is_refused() {
        assert!(validate_identifier("orders\0").is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.1
    #[test]
    fn unsafe_identifier_is_refused_not_silently_inserted() {
        let mut catalog = MssqlSourceCatalog::default();
        let before = catalog.tables.len();
        let result = catalog.insert_table(table("dbo", "bad]name", MssqlCaptureMode::Cdc));
        assert!(result.is_err());
        assert_eq!(catalog.tables.len(), before);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.1
    #[test]
    fn capture_mode_unsupported_is_a_distinct_variant() {
        let t = table("dbo", "views_only", MssqlCaptureMode::Unsupported);
        assert_eq!(t.capture_mode, MssqlCaptureMode::Unsupported);
        assert_ne!(t.capture_mode, MssqlCaptureMode::Cdc);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.1
    #[test]
    fn catalog_serializes_round_trip() {
        let mut catalog = MssqlSourceCatalog {
            database: "AppDb".to_string(),
            tables: BTreeMap::new(),
        };
        catalog
            .insert_table(table("dbo", "orders", MssqlCaptureMode::ChangeTracking))
            .unwrap();

        let encoded = serde_json::to_string(&catalog).unwrap();
        let decoded: MssqlSourceCatalog = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, catalog);
    }
}
