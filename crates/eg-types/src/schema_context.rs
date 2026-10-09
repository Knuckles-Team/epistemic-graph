//! Typed model for the `schema_context` query surface
//! (EG-UNIFIED-DATA-PLANE-R029): what a table is, what joins to it, which
//! ontology class and application it belongs to, and the impact of a
//! change -- the same pattern as the existing `code_context` surface. This
//! is the typed-model slice (`.1`): the answer shape and the refusal to
//! answer for a table absent from the catalog. Wiring the real query entry
//! point is a later child.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One join EG's catalog knows reaches this table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaJoin {
    pub joined_table: String,
    pub on_column: String,
    pub joined_column: String,
}

/// `schema_context`'s answer for one table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaContextAnswer {
    pub table: String,
    pub joins: Vec<SchemaJoin>,
    pub ontology_class: String,
    pub application: String,
    /// A description of what a schema change to this table affects.
    pub change_impact: String,
}

/// A query asked about a table the catalog fixture does not know.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownTable(pub String);

impl std::fmt::Display for UnknownTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "schema_context: unknown table {:?}", self.0)
    }
}

impl std::error::Error for UnknownTable {}

/// The catalog fixture `schema_context` answers against: every known
/// table's precomputed answer, keyed by table name.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaContextCatalog {
    pub tables: BTreeMap<String, SchemaContextAnswer>,
}

impl SchemaContextCatalog {
    /// Answer `schema_context` for `table`. Refuses an unknown table rather
    /// than returning an empty/default answer that looks like "no joins,
    /// no class, no impact" for a table EG never catalogued.
    pub fn schema_context(&self, table: &str) -> Result<&SchemaContextAnswer, UnknownTable> {
        self.tables
            .get(table)
            .ok_or_else(|| UnknownTable(table.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> SchemaContextCatalog {
        let mut tables = BTreeMap::new();
        tables.insert(
            "person".to_string(),
            SchemaContextAnswer {
                table: "person".to_string(),
                joins: vec![SchemaJoin {
                    joined_table: "family".to_string(),
                    on_column: "family_id".to_string(),
                    joined_column: "id".to_string(),
                }],
                ontology_class: "gramps:Person".to_string(),
                application: "gramps".to_string(),
                change_impact: "breaks family-tree rendering".to_string(),
            },
        );
        SchemaContextCatalog { tables }
    }

    #[test]
    fn known_table_returns_its_full_answer() {
        let answer = catalog().schema_context("person").unwrap().clone();
        assert_eq!(answer.ontology_class, "gramps:Person");
        assert_eq!(answer.application, "gramps");
        assert_eq!(answer.joins.len(), 1);
        assert_eq!(answer.joins[0].joined_table, "family");
    }

    #[test]
    fn unknown_table_is_refused_not_defaulted() {
        let err = catalog().schema_context("no_such_table").unwrap_err();
        assert_eq!(err, UnknownTable("no_such_table".to_string()));
    }

    #[test]
    fn empty_catalog_refuses_every_table() {
        let catalog = SchemaContextCatalog::default();
        assert!(catalog.schema_context("person").is_err());
    }

    #[test]
    fn answer_serializes_round_trip() {
        let answer = catalog().tables["person"].clone();
        let encoded = serde_json::to_string(&answer).unwrap();
        let decoded: SchemaContextAnswer = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, answer);
    }
}
