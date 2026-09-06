//! Authoritative resolution of one property-graph element against an exact
//! relational-catalog snapshot: which relation, which key, which columns.
//!
//! Kept apart from `catalog.rs`, which owns admission -- the identity, digest
//! and dependency binding built ON TOP of what this module resolves.

use std::collections::{BTreeMap, BTreeSet};

use crate::tables::schema::{ColumnType, TableConstraint, TableSchema};

use super::model::validate_bounded_text;
use super::validate::{
    add_columns, find_column, identifiers, is_unique_key, primary_key, resolve_labels,
};
use super::{
    CanonicalCatalogName, EdgeEndpoint, EdgeTableDefinition, ElementKeyResolution,
    EndpointResolution, RelationCatalogSnapshot, RelationKind, RelationObjectId, SqlIdentifier,
    SqlName, VertexTableDefinition, MAX_TENANT_SCOPE_BYTES,
};

pub(super) struct RelationIndex<'a> {
    pub(super) by_name: BTreeMap<CanonicalCatalogName, &'a RelationCatalogSnapshot>,
    leaf_counts: BTreeMap<SqlIdentifier, usize>,
}

impl<'a> RelationIndex<'a> {
    pub(super) fn new(
        tenant_scope: &str,
        relations: &'a [RelationCatalogSnapshot],
    ) -> Result<Self, String> {
        validate_bounded_text(tenant_scope, "tenant scope", MAX_TENANT_SCOPE_BYTES)?;
        let mut by_name = BTreeMap::new();
        let mut object_ids = BTreeSet::new();
        let mut leaf_counts = BTreeMap::new();
        for relation in relations {
            relation.validate()?;
            if relation.name.tenant_scope != tenant_scope {
                return Err("relation snapshot belongs to another tenant scope".into());
            }
            if by_name.insert(relation.name.clone(), relation).is_some() {
                return Err("duplicate relation name in authoritative catalog snapshot".into());
            }
            if !object_ids.insert(relation.object_id.clone()) {
                return Err(
                    "duplicate relation object id in authoritative catalog snapshot".into(),
                );
            }
            *leaf_counts.entry(relation.name.object.clone()).or_default() += 1;
        }
        Ok(Self {
            by_name,
            leaf_counts,
        })
    }

    fn resolve(
        &self,
        tenant_scope: &str,
        name: &SqlName,
    ) -> Result<&'a RelationCatalogSnapshot, String> {
        let canonical = CanonicalCatalogName::resolve(tenant_scope, name)?;
        self.by_name
            .get(&canonical)
            .copied()
            .ok_or_else(|| format!("base relation `{}` does not exist", name.quoted_sql()))
    }

    fn has_unambiguous_leaf(&self, name: &SqlIdentifier) -> bool {
        self.leaf_counts.get(name) == Some(&1)
    }
}

#[derive(Clone)]
pub(super) struct ElementBinding<'a> {
    pub(super) table: &'a RelationCatalogSnapshot,
    pub(super) key_columns: Vec<SqlIdentifier>,
    pub(super) used_columns: BTreeMap<SqlIdentifier, ColumnType>,
}

pub(super) type DependencyAccumulator = (
    RelationKind,
    CanonicalCatalogName,
    u64,
    String,
    BTreeMap<SqlIdentifier, ColumnType>,
);

impl ElementBinding<'_> {
    pub(super) fn add_to(
        &self,
        used: &mut BTreeMap<RelationObjectId, DependencyAccumulator>,
    ) -> Result<(), String> {
        let entry = used.entry(self.table.object_id.clone()).or_insert_with(|| {
            (
                self.table.kind,
                self.table.name.clone(),
                self.table.catalog_revision,
                self.table.schema_digest.clone(),
                BTreeMap::new(),
            )
        });
        if entry.0 != self.table.kind
            || entry.1 != self.table.name
            || entry.2 != self.table.catalog_revision
            || entry.3 != self.table.schema_digest
        {
            return Err("one relation object id resolved to inconsistent snapshots".into());
        }
        for (name, column_type) in &self.used_columns {
            if let Some(previous) = entry.4.insert(name.clone(), *column_type) {
                if previous != *column_type {
                    return Err("one dependency column resolved to inconsistent types".into());
                }
            }
        }
        Ok(())
    }
}

pub(super) fn resolve_vertex<'a>(
    vertex: &VertexTableDefinition,
    tenant_scope: &str,
    relations: &RelationIndex<'a>,
) -> Result<(VertexTableDefinition, ElementBinding<'a>), String> {
    let table = relations.resolve(tenant_scope, &vertex.relation)?;
    let key_columns = resolve_element_key(
        &table.schema,
        vertex.key_resolution,
        &vertex.key_columns,
        &vertex.alias,
    )?;
    let (labels, mut used_columns) = resolve_labels(&table.schema, &vertex.labels)?;
    add_columns(&table.schema, &key_columns, &mut used_columns)?;
    Ok((
        VertexTableDefinition {
            relation: table.name.sql_name()?,
            alias: vertex.alias.clone(),
            key_columns: key_columns.clone(),
            key_resolution: ElementKeyResolution::Explicit,
            labels,
        },
        ElementBinding {
            table,
            key_columns,
            used_columns,
        },
    ))
}

pub(super) fn resolve_edge<'a>(
    edge: &EdgeTableDefinition,
    tenant_scope: &str,
    relations: &RelationIndex<'a>,
    vertices: &BTreeMap<SqlIdentifier, ElementBinding<'a>>,
) -> Result<(EdgeTableDefinition, ElementBinding<'a>), String> {
    let table = relations.resolve(tenant_scope, &edge.relation)?;
    let key_columns = resolve_element_key(
        &table.schema,
        edge.key_resolution,
        &edge.key_columns,
        &edge.alias,
    )?;
    let (labels, mut used_columns) = resolve_labels(&table.schema, &edge.labels)?;
    add_columns(&table.schema, &key_columns, &mut used_columns)?;
    let source = resolve_endpoint(
        &table.schema,
        &edge.source,
        relations,
        vertices,
        &mut used_columns,
    )?;
    let destination = resolve_endpoint(
        &table.schema,
        &edge.destination,
        relations,
        vertices,
        &mut used_columns,
    )?;
    Ok((
        EdgeTableDefinition {
            relation: table.name.sql_name()?,
            alias: edge.alias.clone(),
            key_columns: key_columns.clone(),
            key_resolution: ElementKeyResolution::Explicit,
            source,
            destination,
            labels,
        },
        ElementBinding {
            table,
            key_columns,
            used_columns,
        },
    ))
}

fn resolve_element_key(
    schema: &TableSchema,
    resolution: ElementKeyResolution,
    columns: &[SqlIdentifier],
    alias: &SqlIdentifier,
) -> Result<Vec<SqlIdentifier>, String> {
    let resolved = match resolution {
        ElementKeyResolution::Explicit => columns.to_vec(),
        ElementKeyResolution::PrimaryKey => primary_key(schema)?.ok_or_else(|| {
            format!(
                "element `{}` requires a base-table primary key",
                alias.value()
            )
        })?,
    };
    add_columns(schema, &resolved, &mut BTreeMap::new())?;
    if !is_unique_key(schema, &resolved)? {
        return Err(format!(
            "element `{}` key is not a primary or unique key",
            alias.value()
        ));
    }
    Ok(resolved)
}

fn resolve_endpoint(
    edge_schema: &TableSchema,
    endpoint: &EdgeEndpoint,
    relations: &RelationIndex<'_>,
    vertices: &BTreeMap<SqlIdentifier, ElementBinding<'_>>,
    edge_used: &mut BTreeMap<SqlIdentifier, ColumnType>,
) -> Result<EdgeEndpoint, String> {
    let vertex = vertices.get(&endpoint.vertex_alias).ok_or_else(|| {
        format!(
            "edge endpoint references unknown vertex `{}`",
            endpoint.vertex_alias.value()
        )
    })?;
    let (edge_columns, vertex_columns) = match endpoint.resolution {
        EndpointResolution::Explicit => (
            endpoint.edge_key_columns.clone(),
            endpoint.vertex_key_columns.clone(),
        ),
        EndpointResolution::ExplicitEdgeCatalogVertexKey => (
            endpoint.edge_key_columns.clone(),
            vertex.key_columns.clone(),
        ),
        EndpointResolution::ForeignKey => resolve_foreign_key(edge_schema, vertex, relations)?,
    };
    if vertex_columns != vertex.key_columns {
        return Err("edge endpoint must reference the admitted vertex key exactly".into());
    }
    if edge_columns.len() != vertex_columns.len() {
        return Err("edge endpoint key widths differ".into());
    }
    for (edge_column, vertex_column) in edge_columns.iter().zip(&vertex_columns) {
        let edge_type = find_column(edge_schema, edge_column)?.ty;
        let vertex_type = find_column(&vertex.table.schema, vertex_column)?.ty;
        if edge_type != vertex_type {
            return Err(format!(
                "edge column `{}` and vertex column `{}` have different types",
                edge_column.value(),
                vertex_column.value()
            ));
        }
        edge_used.insert(edge_column.clone(), edge_type);
    }
    Ok(EdgeEndpoint {
        edge_key_columns: edge_columns,
        vertex_alias: endpoint.vertex_alias.clone(),
        vertex_key_columns: vertex_columns,
        resolution: EndpointResolution::Explicit,
    })
}

fn resolve_foreign_key(
    edge_schema: &TableSchema,
    vertex: &ElementBinding<'_>,
    relations: &RelationIndex<'_>,
) -> Result<(Vec<SqlIdentifier>, Vec<SqlIdentifier>), String> {
    if !relations.has_unambiguous_leaf(&vertex.table.name.object) {
        return Err("foreign-key relation name is ambiguous across schemas".into());
    }
    let expected_ref: Vec<_> = vertex
        .key_columns
        .iter()
        .map(|column| column.value())
        .collect();
    let mut matches = Vec::new();
    for constraint in edge_schema.constraints() {
        let TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            ..
        } = constraint
        else {
            continue;
        };
        if ref_table == vertex.table.name.object.value()
            && ref_columns
                .iter()
                .map(String::as_str)
                .eq(expected_ref.iter().copied())
        {
            matches.push((identifiers(columns)?, identifiers(ref_columns)?));
        }
    }
    match matches.as_slice() {
        [resolved] => Ok(resolved.clone()),
        [] => Err(format!(
            "no foreign key resolves edge table `{}` to vertex table `{}`",
            edge_schema.name, vertex.table.schema.name
        )),
        _ => Err(format!(
            "multiple foreign keys resolve edge table `{}` to vertex table `{}`",
            edge_schema.name, vertex.table.schema.name
        )),
    }
}
