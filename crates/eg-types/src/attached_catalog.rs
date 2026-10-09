//! EG-UNIFIED-DATA-PLANE-R003 — the typed, hashed attached-source catalog graph. This is the
//! `.1` typed-model slice: [`AttachedCatalogGraph`] and its content hash, plus the refusal of an
//! ambiguous catalog (two tables sharing the same schema-qualified name). Extracting a real
//! catalog from a live dialect adapter's reader, and wiring an OBDA/`schema_context` consumer to
//! it, are later children.

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

/// One column of an attached-source table, as discovered by a catalog reader.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CatalogColumn {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

/// One table (or view) discovered in an attached source's catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<CatalogColumn>,
}

/// A versioned, typed catalog graph extracted from one attached source. `content_hash` is
/// stable under reordering: it sorts tables and columns before hashing, so two reads of the
/// same unchanged catalog hash identically regardless of the order the source reported them in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachedCatalogGraph {
    pub tables: Vec<CatalogTable>,
    pub version: u32,
}

/// Two tables in the same catalog share a schema-qualified name: the catalog is ambiguous and
/// is refused rather than silently accepted with one table shadowing the other.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuplicateCatalogTable {
    pub schema: String,
    pub name: String,
}

impl fmt::Display for DuplicateCatalogTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "catalog names table {:?}.{:?} more than once",
            self.schema, self.name
        )
    }
}

impl std::error::Error for DuplicateCatalogTable {}

impl AttachedCatalogGraph {
    /// Refuses a catalog that names the same `(schema, table)` pair more than once.
    pub fn validate(&self) -> Result<(), DuplicateCatalogTable> {
        let mut seen: Vec<(&str, &str)> = Vec::with_capacity(self.tables.len());
        for table in &self.tables {
            let key = (table.schema.as_str(), table.name.as_str());
            if seen.contains(&key) {
                return Err(DuplicateCatalogTable {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                });
            }
            seen.push(key);
        }
        Ok(())
    }

    /// A deterministic hash of this catalog's shape: tables sorted by `(schema, name)`, and each
    /// table's columns sorted by name, before hashing. Two graphs built from the same tables in
    /// a different discovery order hash identically.
    pub fn content_hash(&self) -> u64 {
        let mut tables: Vec<&CatalogTable> = self.tables.iter().collect();
        tables.sort_by(|a, b| (a.schema.as_str(), a.name.as_str()).cmp(&(b.schema.as_str(), b.name.as_str())));

        let mut hasher = DefaultHasher::new();
        self.version.hash(&mut hasher);
        for table in tables {
            table.schema.hash(&mut hasher);
            table.name.hash(&mut hasher);
            let mut columns: Vec<&CatalogColumn> = table.columns.iter().collect();
            columns.sort_by(|a, b| a.name.cmp(&b.name));
            for column in columns {
                column.hash(&mut hasher);
            }
        }
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, type_name: &str) -> CatalogColumn {
        CatalogColumn {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable: false,
        }
    }

    fn table(schema: &str, name: &str, columns: Vec<CatalogColumn>) -> CatalogTable {
        CatalogTable {
            schema: schema.to_string(),
            name: name.to_string(),
            columns,
        }
    }

    #[test]
    fn content_hash_is_stable_under_table_and_column_reordering() {
        let a = AttachedCatalogGraph {
            version: 1,
            tables: vec![
                table("public", "users", vec![column("id", "int8"), column("email", "text")]),
                table("public", "orders", vec![column("id", "int8")]),
            ],
        };
        let b = AttachedCatalogGraph {
            version: 1,
            tables: vec![
                table("public", "orders", vec![column("id", "int8")]),
                table("public", "users", vec![column("email", "text"), column("id", "int8")]),
            ],
        };
        assert_eq!(a.content_hash(), b.content_hash());
    }

    #[test]
    fn content_hash_changes_with_the_catalog() {
        let a = AttachedCatalogGraph {
            version: 1,
            tables: vec![table("public", "users", vec![column("id", "int8")])],
        };
        let b = AttachedCatalogGraph {
            version: 1,
            tables: vec![table("public", "users", vec![column("id", "int4")])],
        };
        assert_ne!(a.content_hash(), b.content_hash());
    }

    #[test]
    fn a_catalog_with_one_table_per_name_validates() {
        let graph = AttachedCatalogGraph {
            version: 1,
            tables: vec![
                table("public", "users", vec![]),
                table("public", "orders", vec![]),
                table("reporting", "users", vec![]),
            ],
        };
        graph.validate().unwrap();
    }

    #[test]
    fn a_duplicated_schema_qualified_table_name_is_refused() {
        let graph = AttachedCatalogGraph {
            version: 1,
            tables: vec![
                table("public", "users", vec![column("id", "int8")]),
                table("public", "users", vec![column("id", "int4")]),
            ],
        };
        let err = graph.validate().unwrap_err();
        assert_eq!(err.schema, "public");
        assert_eq!(err.name, "users");
        assert!(err.to_string().contains("more than once"));
    }
}
