//! Read-only relations a caller's SQL projection carries beside its tables (EH-066: the
//! decision record views). Every served SQL surface — `Method::Sql`, the KnowledgeStream
//! SQL family and the Postgres wire — reads through the one authorized projection, so a
//! relation added here is visible to all of them, with the same per-caller visibility.
//!
//! The relations are built for THIS caller only (visibility is applied before a row
//! exists), copied into the per-call ephemeral store, and never written back: a write
//! names the tenant's durable catalog, where these names are reserved and do not exist.

use std::sync::Arc;

use eg_query::{TableSchema, TableStore};
use serde_json::Value;

/// One read-only relation: its schema and rows (cells in schema column order).
pub(crate) type Relation = (TableSchema, Vec<Vec<Value>>);

/// A source of read-only relations for one caller, materialized inside the projection
/// build (off the async runtime).
pub(crate) trait ReadOnlyRelations: Send + Sync {
    fn materialize(&self) -> Result<Vec<Relation>, String>;
}

/// A shared, per-caller relation source.
pub(crate) type SharedRelations = Arc<dyn ReadOnlyRelations>;

/// Does `query` name a read-only relation, or read the catalogs that list relations?
/// Only then are the relations materialized — a statement that cannot see them never
/// pays for them. (A false positive only builds relations nobody reads.) The names come
/// from the one reserved list, so a relation added there is never silently skipped here.
pub(crate) fn wants_read_only_relations(query: &str) -> bool {
    let query = query.to_ascii_lowercase();
    ["pg_", "information_schema"]
        .iter()
        .chain(eg_query::READ_ONLY_RELATION_NAMES)
        .any(|word| query.contains(word))
}

/// Copy `relations` into the caller's ephemeral projection `store`.
pub(super) fn add_relations(store: &TableStore, relations: Vec<Relation>) -> Result<(), String> {
    for (schema, rows) in relations {
        store.create_table(&schema, false)?;
        if rows.is_empty() {
            continue;
        }
        let columns: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
        store.insert_rows(&schema.name, &columns, &rows)?;
    }
    Ok(())
}
