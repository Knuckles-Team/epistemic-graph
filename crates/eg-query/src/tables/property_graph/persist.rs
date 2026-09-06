//! Durable SQL/PGQ property-graph catalog rows.
//!
//! Property graphs are catalog objects of the SAME relational catalog the user
//! tables live in, so their rows are written by the store's existing SQL
//! catalog write transaction — never by a second database, store, or authority.
//! The physical catalog file is the catalog boundary, exactly as it is for
//! tables and views, so a graph row is keyed by name alone and the record
//! itself carries the tenant scope its definition was admitted under.
//!
//! This module owns only the durable row shape, the catalog identity/revision
//! allocators, the shared relation-namespace rules, and the base-relation
//! dependency fence; admission itself stays in `catalog.rs`.

use std::collections::BTreeSet;
use std::fmt::Display;

use redb::{ReadTransaction, ReadableTable, TableDefinition, TableHandle, WriteTransaction};

use crate::tables::schema::TableSchema;

use super::alter::apply_alter_action;
use super::{
    decode_property_graph_catalog_record, AlterPropertyGraphAction, DropBehavior, GraphOwner,
    PropertyGraphCatalogRecord, PropertyGraphDefinition, PropertyGraphObjectId, PropertyGraphOwner,
    RelationCatalogSnapshot, RelationKind, RelationObjectId, SqlIdentifier, SqlName,
    MAX_PROPERTY_GRAPH_CATALOG_RECORD_BYTES,
};

/// `graph object name -> canonical catalog-record bytes`.
const PROPERTY_GRAPHS_TABLE: &str = "__sql_property_graphs__";
const PROPERTY_GRAPHS: TableDefinition<&str, &[u8]> = TableDefinition::new(PROPERTY_GRAPHS_TABLE);
/// `counter -> next value`: the catalog-wide stable object-id and
/// catalog-revision allocators. Both are monotonic and never reused, so a
/// renamed graph keeps its object id while its revisions strictly advance.
const PROPERTY_GRAPH_SEQ: TableDefinition<&str, u64> =
    TableDefinition::new("__sql_property_graph_seq__");

const OBJECT_ID_COUNTER: &str = "object_id";
const CATALOG_REVISION_COUNTER: &str = "catalog_revision";
/// The admitted graphs are scanned whole for dependency fencing, so the count
/// is explicitly bounded rather than unbounded.
const MAX_PROPERTY_GRAPHS: usize = 1024;

/// The store's relation catalog as admission needs it: every relation's name,
/// exact schema and governed schema version, plus every name a table or view
/// already occupies. A view has no column schema, so it can only be a namespace
/// occupant, never a base relation of a property graph.
#[derive(Debug, Clone, Default)]
pub(crate) struct RelationCatalogInput {
    pub(crate) relations: Vec<(String, TableSchema, u64)>,
    pub(crate) occupied: BTreeSet<String>,
}

/// The `ALTER PROPERTY GRAPH` target and action, grouped so the durable entry
/// point keeps a small argument list.
pub(crate) struct AlterRequest<'a> {
    /// The VERIFIED request scope, carried from the parsed statement. A record
    /// admitted under another tenant is not alterable through it: the physical
    /// catalog file is the boundary today, and this is the defence in depth
    /// that survives a store ever being opened multi-tenant.
    pub(crate) tenant_scope: &'a str,
    pub(crate) name: &'a SqlName,
    pub(crate) if_exists: bool,
    pub(crate) action: &'a AlterPropertyGraphAction,
}

/// Read one record only when it belongs to `tenant_scope`.
fn record_for_tenant_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    key: &str,
) -> Result<Option<PropertyGraphCatalogRecord>, String> {
    Ok(record_in(wtx, key)?.filter(|record| record.name.tenant_scope == tenant_scope))
}

fn store_error<E: Display>(error: E) -> String {
    format!("sql property graph catalog: {error}")
}

/// Resolve a graph name to its durable row key. Phase 1 admits exactly the
/// tenant `public` schema, so a catalog-qualified or foreign-schema name fails
/// closed rather than being silently folded into `public`.
///
/// COUPLED with `sql::pgq::lower_graph_table`'s canonical-name comparison: a
/// query naming `shop` matches an admitted `public.shop` only because BOTH
/// sites agree that `public` is the one admitted schema. A Phase-2 multi-schema
/// catalog must change them together, or that fold becomes a real cross-schema
/// name collision.
fn canonical_key(name: &SqlName) -> Result<String, String> {
    name.validate()?;
    match name.0.as_slice() {
        [object] => Ok(object.value().to_string()),
        [schema, object] if schema.value() == "public" => Ok(object.value().to_string()),
        _ => Err("property graph identity is limited to the tenant `public` schema".to_string()),
    }
}

/// Build the authoritative base-relation snapshots one admission resolves
/// against.
///
/// The pinned `catalog_revision` is the table's governed schema version plus
/// one. That is a WEAK signal: a table that has never been through a governed
/// migration sits at version 0 forever, so every such relation pins revision 1
/// and the revision alone never detects drift. The SCHEMA DIGEST is the real
/// binding, the base-DDL fence is what prevents drift, and
/// `TableStore::verify_property_graph_dependencies` re-checks the digest at
/// read time so neither is trusted alone. A stored relation whose name cannot be spelled as one SQL
/// identifier still occupies the namespace but supplies no snapshot: it cannot
/// be named by a definition either, so a graph referencing it fails closed with
/// "base relation does not exist".
fn snapshots(
    input: &RelationCatalogInput,
    tenant_scope: &str,
) -> Result<Vec<RelationCatalogSnapshot>, String> {
    let mut out = Vec::with_capacity(input.relations.len());
    for (relation, schema, schema_version) in &input.relations {
        let Ok(identifier) = SqlIdentifier::quoted(relation.clone()) else {
            continue;
        };
        let Ok(name) = SqlName::new(vec![identifier]) else {
            continue;
        };
        // Revision is the table's governed schema version, offset so an
        // unmigrated table still presents the positive revision a catalog
        // record requires.
        out.push(RelationCatalogSnapshot::new(
            RelationObjectId::new(format!("relation/{relation}"))?,
            RelationKind::Table,
            tenant_scope,
            &name,
            schema_version.saturating_add(1),
            schema.clone(),
        )?);
    }
    Ok(out)
}

fn lookup<T>(table: &T, key: &str) -> Result<Option<PropertyGraphCatalogRecord>, String>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let Some(value) = table.get(key).map_err(store_error)? else {
        return Ok(None);
    };
    decode_property_graph_catalog_record(value.value()).map(Some)
}

fn scan<T>(table: &T) -> Result<Vec<PropertyGraphCatalogRecord>, String>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let mut records = Vec::new();
    for row in table.iter().map_err(store_error)? {
        let (_, value) = row.map_err(store_error)?;
        if records.len() == MAX_PROPERTY_GRAPHS {
            return Err("property graph catalog exceeds its bound".to_string());
        }
        records.push(decode_property_graph_catalog_record(value.value())?);
    }
    Ok(records)
}

/// Whether this catalog has ever held a property graph.
///
/// `WriteTransaction::open_table` CREATES a missing table, so every `DROP
/// TABLE`/`ALTER TABLE` on a store that uses no property graphs would otherwise
/// materialise this one as a side effect of being fenced.
fn graphs_exist(wtx: &WriteTransaction) -> Result<bool, String> {
    for handle in wtx.list_tables().map_err(store_error)? {
        if handle.name() == PROPERTY_GRAPHS_TABLE {
            return Ok(true);
        }
    }
    Ok(false)
}

fn record_in(
    wtx: &WriteTransaction,
    key: &str,
) -> Result<Option<PropertyGraphCatalogRecord>, String> {
    if !graphs_exist(wtx)? {
        return Ok(None);
    }
    let table = wtx.open_table(PROPERTY_GRAPHS).map_err(store_error)?;
    lookup(&table, key)
}

/// Read one admitted record from a read snapshot, only for `tenant_scope`.
pub(crate) fn property_graph_snapshot(
    rtx: &ReadTransaction,
    tenant_scope: &str,
    name: &SqlName,
) -> Result<Option<PropertyGraphCatalogRecord>, String> {
    let key = canonical_key(name)?;
    let table = match rtx.open_table(PROPERTY_GRAPHS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(store_error(error)),
    };
    Ok(lookup(&table, &key)?.filter(|record| record.name.tenant_scope == tenant_scope))
}

/// Every admitted graph name, sorted for determinism.
pub(crate) fn list_property_graphs_snapshot(rtx: &ReadTransaction) -> Result<Vec<String>, String> {
    let table = match rtx.open_table(PROPERTY_GRAPHS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(store_error(error)),
    };
    let mut names: Vec<String> = scan(&table)?
        .into_iter()
        .map(|record| record.name.object.value().to_string())
        .collect();
    names.sort();
    Ok(names)
}

fn next_counter(wtx: &WriteTransaction, counter: &str) -> Result<u64, String> {
    let mut seq = wtx.open_table(PROPERTY_GRAPH_SEQ).map_err(store_error)?;
    let next = seq
        .get(counter)
        .map_err(store_error)?
        .map(|value| value.value())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| "property graph catalog counter overflow".to_string())?;
    seq.insert(counter, next).map_err(store_error)?;
    Ok(next)
}

fn put_record(
    wtx: &WriteTransaction,
    key: &str,
    record: &PropertyGraphCatalogRecord,
) -> Result<(), String> {
    let bytes = record.canonical_bytes()?;
    if bytes.len() > MAX_PROPERTY_GRAPH_CATALOG_RECORD_BYTES {
        return Err("property graph catalog record exceeds its storage bound".to_string());
    }
    let mut table = wtx.open_table(PROPERTY_GRAPHS).map_err(store_error)?;
    table.insert(key, bytes.as_slice()).map_err(store_error)?;
    Ok(())
}

fn remove_record(wtx: &WriteTransaction, key: &str) -> Result<(), String> {
    let mut table = wtx.open_table(PROPERTY_GRAPHS).map_err(store_error)?;
    table.remove(key).map_err(store_error)?;
    Ok(())
}

fn ensure_name_free(
    wtx: &WriteTransaction,
    key: &str,
    input: &RelationCatalogInput,
) -> Result<(), String> {
    if input.occupied.contains(key) {
        return Err(format!(
            "property graph name `{key}` collides with a table or view in the shared relation namespace"
        ));
    }
    if record_in(wtx, key)?.is_some() {
        return Err(format!("property graph `{key}` already exists"));
    }
    Ok(())
}

/// `CREATE PROPERTY GRAPH`: admit `draft` against the exact relation namespace,
/// stamp it with a fresh stable object id, owner and catalog revision, and write
/// the record in this catalog transaction.
pub(crate) fn create_property_graph_in(
    wtx: &WriteTransaction,
    owner: &str,
    input: &RelationCatalogInput,
    draft: &PropertyGraphDefinition,
) -> Result<PropertyGraphCatalogRecord, String> {
    let key = canonical_key(&draft.name)?;
    ensure_name_free(wtx, &key, input)?;
    let object_id = PropertyGraphObjectId::new(format!(
        "propertygraph/{}",
        next_counter(wtx, OBJECT_ID_COUNTER)?
    ))?;
    let record = PropertyGraphCatalogRecord::admit(
        object_id,
        PropertyGraphOwner::new(owner)?,
        next_counter(wtx, CATALOG_REVISION_COUNTER)?,
        1,
        draft,
        &snapshots(input, &draft.tenant_scope)?,
    )?;
    put_record(wtx, &key, &record)?;
    Ok(record)
}

/// `ALTER PROPERTY GRAPH`: rebuild the definition, then re-admit it under the
/// SAME object id with strictly advancing revisions, so every change re-resolves
/// its base relations rather than trusting the stored dependency digest.
pub(crate) fn alter_property_graph_in(
    wtx: &WriteTransaction,
    actor: &str,
    input: &RelationCatalogInput,
    request: AlterRequest<'_>,
) -> Result<Option<PropertyGraphCatalogRecord>, String> {
    let key = canonical_key(request.name)?;
    let Some(current) = record_for_tenant_in(wtx, request.tenant_scope, &key)? else {
        if request.if_exists {
            return Ok(None);
        }
        return Err(format!("property graph `{key}` does not exist"));
    };
    let draft = apply_alter_action(&current.accepted_definition, request.action)?;
    let next_key = canonical_key(&draft.name)?;
    if next_key != key {
        ensure_name_free(wtx, &next_key, input)?;
    }
    let record = PropertyGraphCatalogRecord::admit(
        current.object_id.clone(),
        next_owner(&current, request.action, actor)?,
        next_counter(wtx, CATALOG_REVISION_COUNTER)?,
        current
            .definition_revision
            .checked_add(1)
            .ok_or_else(|| "property graph definition revision overflow".to_string())?,
        &draft,
        &snapshots(input, &draft.tenant_scope)?,
    )?;
    if next_key != key {
        remove_record(wtx, &key)?;
    }
    put_record(wtx, &next_key, &record)?;
    Ok(Some(record))
}

fn next_owner(
    current: &PropertyGraphCatalogRecord,
    action: &AlterPropertyGraphAction,
    actor: &str,
) -> Result<PropertyGraphOwner, String> {
    match action {
        AlterPropertyGraphAction::OwnerTo(GraphOwner::Role(role)) => {
            PropertyGraphOwner::new(role.value())
        }
        AlterPropertyGraphAction::OwnerTo(_) => PropertyGraphOwner::new(actor),
        _ => Ok(current.owner.clone()),
    }
}

/// `DROP PROPERTY GRAPH`. Nothing in this catalog depends on a property graph —
/// it is itself a leaf view over base relations — so RESTRICT and CASCADE are
/// EQUIVALENT here and both remove exactly the named graphs. Drop behaviour is
/// meaningful only for base-relation DDL, and for `ALTER … DROP TABLES` inside a
/// graph. `drop_cascade_and_restrict_are_equivalent_for_a_leaf_graph` asserts
/// this rather than leaving the ignored parameter unexplained.
pub(crate) fn drop_property_graphs_in(
    wtx: &WriteTransaction,
    tenant_scope: &str,
    names: &[SqlName],
    if_exists: bool,
    _behavior: DropBehavior,
) -> Result<usize, String> {
    let mut dropped = 0usize;
    for name in names {
        let key = canonical_key(name)?;
        if record_for_tenant_in(wtx, tenant_scope, &key)?.is_none() {
            if if_exists {
                continue;
            }
            return Err(format!("property graph `{key}` does not exist"));
        }
        remove_record(wtx, &key)?;
        dropped = dropped.saturating_add(1);
    }
    Ok(dropped)
}

/// Every admitted graph that pins `relation`'s revision and schema digest.
pub(crate) fn property_graph_dependents_in(
    wtx: &WriteTransaction,
    relation: &str,
) -> Result<Vec<String>, String> {
    if !graphs_exist(wtx)? {
        return Ok(Vec::new());
    }
    let table = wtx.open_table(PROPERTY_GRAPHS).map_err(store_error)?;
    let mut dependents: Vec<String> = scan(&table)?
        .into_iter()
        .filter(|record| {
            record
                .dependencies
                .iter()
                .any(|dependency| dependency.name.object.value() == relation)
        })
        .map(|record| record.name.object.value().to_string())
        .collect();
    dependents.sort();
    Ok(dependents)
}

/// A base relation cannot be dropped or have its schema changed while an
/// admitted property graph pins its revision and schema digest. The graph must
/// be dropped or altered first; there is no silent invalidation.
pub(crate) fn fence_base_relation_ddl_in(
    wtx: &WriteTransaction,
    relation: &str,
) -> Result<(), String> {
    let dependents = property_graph_dependents_in(wtx, relation)?;
    if dependents.is_empty() {
        return Ok(());
    }
    Err(format!(
        "cannot change base relation `{relation}` because property graph(s) {} depend on it",
        dependents.join(", ")
    ))
}

/// A table or view may not take a name an admitted property graph already holds.
pub(crate) fn ensure_relation_name_free_in(
    wtx: &WriteTransaction,
    name: &str,
) -> Result<(), String> {
    if record_in(wtx, name)?.is_some() {
        return Err(format!(
            "`{name}` is a property graph; a table or view cannot share that name"
        ));
    }
    Ok(())
}
