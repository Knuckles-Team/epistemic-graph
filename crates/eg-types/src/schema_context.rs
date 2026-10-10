//! Typed model for the `schema_context` query surface
//! (EG-UNIFIED-DATA-PLANE-R029): what a table is, what joins to it, which
//! ontology class and application it belongs to, and the impact of a
//! change -- the same pattern as the existing `code_context` surface. This
//! module holds the typed-model slice (`.1`): the answer shape and the
//! refusal to answer for a table absent from the catalog; plus the `.2`
//! served query entry point (`serve_schema_context`): a structured,
//! wire-serializable request/response pair that Graph OS can dispatch the
//! same way `graph_code action=code_context` dispatches to
//! `agent_utilities.knowledge_graph.retrieval.code_context`. The AU client
//! call and the graph-os `graph_schema` intent-tool registration that
//! invoke this entry point are follow-up child rows (see the PR for their
//! proposed IDs); this module only has to serve the request once it
//! arrives.

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

/// The request shape the `schema_context` query entry point accepts. A
/// struct (rather than a bare `&str`) so the entry point is a real wire
/// boundary: it decodes the same way a `graph_code` request body decodes
/// before reaching `code_context`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaContextRequest {
    pub table: String,
}

/// The `schema_context` entry point's refusal, serialized across the wire
/// rather than EG's internal `UnknownTable` (which only implements
/// `Display`/`Error` for in-process use).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaContextQueryError {
    pub table: String,
    pub reason: String,
}

impl From<UnknownTable> for SchemaContextQueryError {
    fn from(err: UnknownTable) -> Self {
        SchemaContextQueryError {
            table: err.0.clone(),
            reason: err.to_string(),
        }
    }
}

impl std::fmt::Display for SchemaContextQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl std::error::Error for SchemaContextQueryError {}

/// The real `schema_context` query entry point (EG-UNIFIED-DATA-PLANE-
/// R029.2): the served method this requirement wires up. Graph OS's
/// `code_context` pattern is a composed KG traversal invoked by name
/// (`graph_code action=code_context`); `schema_context` is a precomputed
/// catalog lookup, so its entry point takes the same shape -- a decoded
/// request in, a decoded answer or a decoded refusal out -- without
/// requiring a live graph traversal. This is the function the AU client
/// call and the graph-os intent-tool registration (follow-up child rows)
/// dispatch to.
pub fn serve_schema_context(
    catalog: &SchemaContextCatalog,
    request: &SchemaContextRequest,
) -> Result<SchemaContextAnswer, SchemaContextQueryError> {
    catalog
        .schema_context(&request.table)
        .map(|answer| answer.clone())
        .map_err(SchemaContextQueryError::from)
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

    // spec: EG-UNIFIED-DATA-PLANE-R029.2
    #[test]
    fn serve_schema_context_returns_the_full_answer_for_a_known_table() {
        let request = SchemaContextRequest {
            table: "person".to_string(),
        };
        // Round-trip the request itself, proving it is a real wire
        // boundary and not just an in-process `&str` call.
        let encoded_request = serde_json::to_string(&request).unwrap();
        let decoded_request: SchemaContextRequest =
            serde_json::from_str(&encoded_request).unwrap();

        let answer = serve_schema_context(&catalog(), &decoded_request).unwrap();

        assert_eq!(answer.table, "person");
        assert_eq!(answer.ontology_class, "gramps:Person");
        assert_eq!(answer.application, "gramps");
        assert_eq!(answer.change_impact, "breaks family-tree rendering");
        assert_eq!(answer.joins.len(), 1);
        assert_eq!(answer.joins[0].joined_table, "family");
        assert_eq!(answer.joins[0].on_column, "family_id");
        assert_eq!(answer.joins[0].joined_column, "id");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R029.2
    #[test]
    fn serve_schema_context_refuses_an_unknown_table_with_a_serializable_error() {
        let request = SchemaContextRequest {
            table: "no_such_table".to_string(),
        };

        let err = serve_schema_context(&catalog(), &request).unwrap_err();
        assert_eq!(err.table, "no_such_table");

        // The refusal itself must cross the wire: it is what the served
        // entry point returns to a caller, not an in-process-only error.
        let encoded_err = serde_json::to_string(&err).unwrap();
        let decoded_err: SchemaContextQueryError = serde_json::from_str(&encoded_err).unwrap();
        assert_eq!(decoded_err, err);
    }
}
