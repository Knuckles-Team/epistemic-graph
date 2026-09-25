//! The identity store inside the one authorized SQL projection (IDM-01,
//! operator ruling: "standard SQL tables to store users").
//!
//! Every served SQL read (native `Sql`, the wire protocols, KnowledgeStream)
//! builds its ephemeral projection through `project_read_store`; this module
//! adds the identity store's REDACTED relations to it -- read-only by
//! construction, because the projection is discarded after the statement and
//! writes never reach it -- and only for a caller holding the EXACT
//! `identity:read` or `identity:admin` scope. The relation names carry the
//! reserved prefix [`IDENTITY_RELATION_PREFIX`], which no tenant table may use.
//!
//! The projection reads the snapshot the engine owning `persist_dir`
//! published (`crate::server::identity_view`).
#![cfg(feature = "security")]

use std::path::Path;

use eg_query::{Column, ColumnType, TableSchema};
use eg_types::identity::SqlType;

use super::AuthorizedReadStore;
use crate::server::access::CarrierAuthority;

/// The reserved relation-name prefix (`__identity__users`, …).
pub(crate) const IDENTITY_RELATION_PREFIX: &str = "__identity__";

use crate::server::identity_view::published;

/// Refuse a tenant table name in the reserved identity namespace.
pub(crate) fn refuse_reserved_name(name: &str) -> Result<(), String> {
    if name
        .to_ascii_lowercase()
        .starts_with(IDENTITY_RELATION_PREFIX)
    {
        return Err(format!(
            "SQL error: table names beginning with '{IDENTITY_RELATION_PREFIX}' are reserved for the identity store"
        ));
    }
    Ok(())
}

fn column_type(kind: SqlType) -> ColumnType {
    match kind {
        SqlType::Text => ColumnType::Text,
        SqlType::Integer => ColumnType::BigInt,
        SqlType::Boolean => ColumnType::Bool,
    }
}

/// Add the identity relations to `projection` when `authority` may read them.
pub(super) fn add_identity_relations(
    projection: &AuthorizedReadStore,
    authority: &CarrierAuthority,
    persist_dir: &Path,
) -> Result<(), String> {
    if !authority.is_identity_reader() {
        return Ok(());
    }
    let Some(store) = published(persist_dir) else {
        return Ok(());
    };
    for relation in store.sql_relations() {
        let name = format!("{IDENTITY_RELATION_PREFIX}{}", relation.name);
        let columns: Vec<Column> = relation
            .columns
            .iter()
            .map(|(column, kind)| Column::new(*column, column_type(*kind), true, false))
            .collect();
        projection
            .store()
            .create_table(&TableSchema::new(name.clone(), columns), false)?;
        let names: Vec<String> = relation
            .columns
            .iter()
            .map(|(column, _)| column.to_string())
            .collect();
        if !relation.rows.is_empty() {
            projection
                .store()
                .insert_rows(&name, &names, &relation.rows)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
