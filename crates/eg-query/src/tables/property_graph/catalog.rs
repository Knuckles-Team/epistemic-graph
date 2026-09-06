//! Authoritative, storage-independent SQL/PGQ catalog admission resolves one
//! definition against an exact relational-catalog snapshot; persistence and
//! mutation wiring remain owned by the table-store transaction.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::tables::schema::{ColumnType, TableConstraint, TableSchema};

use super::model::{
    digest_value, require, validate_bounded_text, validate_digest, validate_revision,
};
use super::validate::{
    add_columns, find_column, identifiers, is_unique_key, merge_resolved_property_types,
    primary_key, resolve_labels,
};
use super::{
    CanonicalCatalogName, EdgeEndpoint, EdgeTableDefinition, ElementKeyResolution,
    EndpointResolution, PropertyGraphDefinition, RelationKind, SqlIdentifier, SqlName,
    VertexTableDefinition, MAX_CATALOG_ID_BYTES, MAX_CATALOG_OWNER_BYTES,
    MAX_PROPERTY_GRAPH_CATALOG_RECORD_BYTES, MAX_TENANT_SCOPE_BYTES,
    PROPERTY_GRAPH_CATALOG_SCHEMA_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CatalogIdentity<const KIND: u8>(String);

impl<const KIND: u8> CatalogIdentity<KIND> {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_bounded_text(
            &value,
            catalog_identity_field::<KIND>()?,
            catalog_identity_max::<KIND>()?,
        )?;
        Ok(Self(value))
    }

    pub fn value(&self) -> &str {
        &self.0
    }
}

pub type PropertyGraphObjectId = CatalogIdentity<1>;
pub type RelationObjectId = CatalogIdentity<2>;
/// One concrete owner resolved from `CURRENT_USER`/`SESSION_USER` before admission.
pub type PropertyGraphOwner = CatalogIdentity<3>;

fn catalog_identity_field<const KIND: u8>() -> Result<&'static str, String> {
    match KIND {
        1 => Ok("property graph object id"),
        2 => Ok("relation object id"),
        3 => Ok("property graph owner"),
        _ => Err("unsupported SQL catalog identity kind".into()),
    }
}

fn catalog_identity_max<const KIND: u8>() -> Result<usize, String> {
    match KIND {
        1 | 2 => Ok(MAX_CATALOG_ID_BYTES),
        3 => Ok(MAX_CATALOG_OWNER_BYTES),
        _ => Err("unsupported SQL catalog identity kind".into()),
    }
}

/// An authoritative table-or-view snapshot; stable id distinguishes recreate
/// from rename. Callers must provide the complete shared relation namespace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationCatalogSnapshot {
    pub object_id: RelationObjectId,
    pub kind: RelationKind,
    pub name: CanonicalCatalogName,
    pub catalog_revision: u64,
    pub schema_digest: String,
    pub schema: TableSchema,
}

impl RelationCatalogSnapshot {
    pub fn new(
        object_id: RelationObjectId,
        kind: RelationKind,
        tenant_scope: &str,
        name: &SqlName,
        catalog_revision: u64,
        schema: TableSchema,
    ) -> Result<Self, String> {
        schema.validate()?;
        let canonical_name = CanonicalCatalogName::resolve(tenant_scope, name)?;
        if schema.name != canonical_name.object.value() {
            return Err(format!(
                "table schema name `{}` does not match catalog object `{}`",
                schema.name,
                canonical_name.object.value()
            ));
        }
        let snapshot = Self {
            object_id,
            kind,
            name: canonical_name,
            catalog_revision,
            schema_digest: schema.schema_digest()?,
            schema,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_bounded_text(
            self.object_id.value(),
            "relation object id",
            MAX_CATALOG_ID_BYTES,
        )?;
        self.name.validate()?;
        validate_revision(self.catalog_revision, "table catalog revision")?;
        self.schema.validate()?;
        if self.schema.name != self.name.object.value() {
            return Err("table schema and canonical catalog names differ".into());
        }
        let expected = self.schema.schema_digest()?;
        validate_digest(&self.schema_digest, "table schema digest")?;
        if self.schema_digest != expected {
            return Err("table schema digest mismatch".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedColumnDependency {
    pub name: SqlIdentifier,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyGraphDependency {
    pub relation_object_id: RelationObjectId,
    pub kind: RelationKind,
    pub name: CanonicalCatalogName,
    pub catalog_revision: u64,
    pub schema_digest: String,
    pub columns: Vec<ResolvedColumnDependency>,
}

impl PropertyGraphDependency {
    fn validate(&self) -> Result<(), String> {
        validate_bounded_text(
            self.relation_object_id.value(),
            "relation object id",
            MAX_CATALOG_ID_BYTES,
        )?;
        self.name.validate()?;
        validate_revision(self.catalog_revision, "dependency catalog revision")?;
        validate_digest(&self.schema_digest, "dependency schema digest")?;
        if self.columns.is_empty() {
            return Err("property graph dependency must bind at least one column".into());
        }
        let mut names = BTreeSet::new();
        for column in &self.columns {
            column.name.validate()?;
            if !names.insert(&column.name) {
                return Err(format!(
                    "duplicate dependency column `{}`",
                    column.name.value()
                ));
            }
        }
        let mut canonical = self.columns.clone();
        canonical.sort_by(|left, right| left.name.cmp(&right.name));
        require(
            canonical == self.columns,
            "property graph dependency columns are not canonically ordered",
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyGraphCatalogRecord {
    pub schema_version: u16,
    pub object_id: PropertyGraphObjectId,
    pub owner: PropertyGraphOwner,
    pub name: CanonicalCatalogName,
    pub catalog_revision: u64,
    pub definition_revision: u64,
    pub definition_digest: String,
    pub dependency_digest: String,
    pub accepted_definition: PropertyGraphDefinition,
    pub dependencies: Vec<PropertyGraphDependency>,
    pub record_digest: String,
}

impl PropertyGraphCatalogRecord {
    /// Resolve one draft definition against an exact relational-catalog
    /// snapshot. Checks run in a FIXED order and the FIRST failure is reported,
    /// so a caller must expect the earliest applicable error: (1) revision
    /// bounds; (2) draft shape, canonical order and digest; (3) temporary
    /// rejection; (4) snapshot validity, tenant scope and duplicate relation
    /// name/id; (5) graph-name namespace collision; (6) per element in
    /// canonical alias order, vertices before edges: relation existence, then
    /// key resolution and uniqueness, then label/property column existence;
    /// (7) cross-element shared-label property type equality; (8) edge endpoint
    /// reference, key width and column types; (9) dependency consistency and
    /// record-digest binding.
    pub fn admit(
        object_id: PropertyGraphObjectId,
        owner: PropertyGraphOwner,
        catalog_revision: u64,
        definition_revision: u64,
        draft: &PropertyGraphDefinition,
        relations: &[RelationCatalogSnapshot],
    ) -> Result<Self, String> {
        validate_revision(catalog_revision, "property graph catalog revision")?;
        validate_revision(definition_revision, "property graph definition revision")?;
        draft.validate()?;
        if draft.temporary {
            return Err(
                "temporary property graphs require a connection-scoped catalog and are rejected"
                    .into(),
            );
        }

        let relation_index = RelationIndex::new(&draft.tenant_scope, relations)?;
        let name = CanonicalCatalogName::resolve(&draft.tenant_scope, &draft.name)?;
        if relation_index.by_name.contains_key(&name) {
            return Err(format!(
                "property graph name `{}` collides with a relation in the shared namespace",
                name.object.value()
            ));
        }

        let mut used = BTreeMap::<RelationObjectId, DependencyAccumulator>::new();
        let mut property_types = BTreeMap::new();
        let mut vertices = Vec::with_capacity(draft.vertex_tables.len());
        let mut vertex_bindings = BTreeMap::new();
        for vertex in &draft.vertex_tables {
            let (resolved, binding) = resolve_vertex(vertex, &draft.tenant_scope, &relation_index)?;
            merge_resolved_property_types(
                &mut property_types,
                &binding.table.schema,
                &resolved.labels,
            )?;
            binding.add_to(&mut used)?;
            vertex_bindings.insert(vertex.alias.clone(), binding);
            vertices.push(resolved);
        }

        let mut edges = Vec::with_capacity(draft.edge_tables.len());
        for edge in &draft.edge_tables {
            let (resolved, binding) =
                resolve_edge(edge, &draft.tenant_scope, &relation_index, &vertex_bindings)?;
            merge_resolved_property_types(
                &mut property_types,
                &binding.table.schema,
                &resolved.labels,
            )?;
            binding.add_to(&mut used)?;
            edges.push(resolved);
        }

        let accepted_definition = PropertyGraphDefinition::new(
            draft.tenant_scope.clone(),
            name.sql_name()?,
            false,
            vertices,
            edges,
        )?;
        let dependencies = used
            .into_iter()
            .map(
                |(relation_object_id, (kind, name, catalog_revision, schema_digest, columns))| {
                    PropertyGraphDependency {
                        relation_object_id,
                        kind,
                        name,
                        catalog_revision,
                        schema_digest,
                        columns: columns
                            .into_iter()
                            .map(|(name, column_type)| ResolvedColumnDependency {
                                name,
                                column_type,
                            })
                            .collect(),
                    }
                },
            )
            .collect();
        let dependency_digest = digest_value(
            b"epistemic-graph/sql-pgq/dependencies/v1\0",
            &dependencies,
            "property graph dependencies",
        )?;
        let mut record = Self {
            schema_version: PROPERTY_GRAPH_CATALOG_SCHEMA_VERSION,
            object_id,
            owner,
            name,
            catalog_revision,
            definition_revision,
            definition_digest: accepted_definition.digest.clone(),
            dependency_digest,
            accepted_definition,
            dependencies,
            record_digest: String::new(),
        };
        record.record_digest = record.compute_record_digest()?;
        record.validate()?;
        Ok(record)
    }

    fn renamed(
        &self,
        new_name: &SqlName,
        next_catalog_revision: u64,
        next_definition_revision: u64,
    ) -> Result<Self, String> {
        self.validate()?;
        if next_catalog_revision <= self.catalog_revision
            || next_definition_revision <= self.definition_revision
        {
            return Err("rename revisions must advance monotonically".into());
        }
        let name = CanonicalCatalogName::resolve(&self.name.tenant_scope, new_name)?;
        if name == self.name {
            return Err("rename must select a different canonical name".into());
        }
        let accepted_definition = PropertyGraphDefinition::new(
            self.name.tenant_scope.clone(),
            name.sql_name()?,
            false,
            self.accepted_definition.vertex_tables.clone(),
            self.accepted_definition.edge_tables.clone(),
        )?;
        let mut renamed = Self {
            schema_version: self.schema_version,
            object_id: self.object_id.clone(),
            owner: self.owner.clone(),
            name,
            catalog_revision: next_catalog_revision,
            definition_revision: next_definition_revision,
            definition_digest: accepted_definition.digest.clone(),
            dependency_digest: self.dependency_digest.clone(),
            accepted_definition,
            dependencies: self.dependencies.clone(),
            record_digest: String::new(),
        };
        renamed.record_digest = renamed.compute_record_digest()?;
        renamed.validate()?;
        Ok(renamed)
    }

    pub fn validate(&self) -> Result<(), String> {
        require(
            self.schema_version == PROPERTY_GRAPH_CATALOG_SCHEMA_VERSION,
            "unsupported property graph catalog schema version",
        )?;
        validate_bounded_text(
            self.object_id.value(),
            "property graph object id",
            MAX_CATALOG_ID_BYTES,
        )?;
        validate_bounded_text(
            self.owner.value(),
            "property graph owner",
            MAX_CATALOG_OWNER_BYTES,
        )?;
        self.name.validate()?;
        validate_revision(self.catalog_revision, "property graph catalog revision")?;
        validate_revision(
            self.definition_revision,
            "property graph definition revision",
        )?;
        self.accepted_definition.validate()?;
        require(
            !self.accepted_definition.temporary,
            "temporary property graph cannot enter the durable catalog",
        )?;
        require(
            self.accepted_definition.tenant_scope == self.name.tenant_scope
                && CanonicalCatalogName::resolve(
                    &self.accepted_definition.tenant_scope,
                    &self.accepted_definition.name,
                )? == self.name,
            "property graph canonical name does not match its definition",
        )?;
        require(
            self.definition_digest == self.accepted_definition.digest,
            "property graph definition digest mismatch",
        )?;
        validate_digest(&self.definition_digest, "property graph definition digest")?;
        validate_dependencies(&self.dependencies)?;
        require(
            self.dependencies
                .iter()
                .all(|dependency| dependency.name.tenant_scope == self.name.tenant_scope),
            "property graph dependency belongs to another tenant scope",
        )?;
        let dependency_digest = digest_value(
            b"epistemic-graph/sql-pgq/dependencies/v1\0",
            &self.dependencies,
            "property graph dependencies",
        )?;
        require(
            self.dependency_digest == dependency_digest,
            "property graph dependency digest mismatch",
        )?;
        require(
            self.record_digest == self.compute_record_digest()?,
            "property graph catalog record digest mismatch",
        )?;
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec(self)
            .map_err(|error| format!("encode property graph catalog record: {error}"))
    }

    fn compute_record_digest(&self) -> Result<String, String> {
        digest_value(
            b"epistemic-graph/sql-pgq/catalog-record/v1\0",
            &(
                self.schema_version,
                &self.object_id,
                &self.owner,
                &self.name,
                self.catalog_revision,
                self.definition_revision,
                &self.definition_digest,
                &self.dependency_digest,
            ),
            "property graph catalog record",
        )
    }
}

pub fn decode_property_graph_catalog_record(
    bytes: &[u8],
) -> Result<PropertyGraphCatalogRecord, String> {
    if bytes.is_empty() || bytes.len() > MAX_PROPERTY_GRAPH_CATALOG_RECORD_BYTES {
        return Err(format!(
            "property graph catalog record must contain 1..={MAX_PROPERTY_GRAPH_CATALOG_RECORD_BYTES} bytes"
        ));
    }
    let record: PropertyGraphCatalogRecord = serde_json::from_slice(bytes)
        .map_err(|error| format!("decode property graph catalog record: {error}"))?;
    record.validate()?;
    if record.canonical_bytes()? != bytes {
        return Err("property graph catalog record is not canonical JSON".into());
    }
    Ok(record)
}

/// Transaction-local shared namespace and reverse-dependency projection.
#[derive(Debug, Clone)]
pub struct PropertyGraphCatalog {
    tenant_scope: String,
    relation_names: BTreeSet<CanonicalCatalogName>,
    relations: Vec<RelationCatalogSnapshot>,
    records: BTreeMap<PropertyGraphObjectId, PropertyGraphCatalogRecord>,
    names: BTreeMap<CanonicalCatalogName, PropertyGraphObjectId>,
    reverse_dependencies: BTreeMap<RelationObjectId, BTreeSet<PropertyGraphObjectId>>,
}

impl PropertyGraphCatalog {
    pub fn new(
        tenant_scope: impl Into<String>,
        relations: &[RelationCatalogSnapshot],
    ) -> Result<Self, String> {
        let tenant_scope = tenant_scope.into();
        let relation_index = RelationIndex::new(&tenant_scope, relations)?;
        Ok(Self {
            tenant_scope,
            relation_names: relation_index.by_name.keys().cloned().collect(),
            relations: relations.to_vec(),
            records: BTreeMap::new(),
            names: BTreeMap::new(),
            reverse_dependencies: BTreeMap::new(),
        })
    }

    pub fn insert(&mut self, record: PropertyGraphCatalogRecord) -> Result<(), String> {
        record.validate()?;
        let expected = PropertyGraphCatalogRecord::admit(
            record.object_id.clone(),
            record.owner.clone(),
            record.catalog_revision,
            record.definition_revision,
            &record.accepted_definition,
            &self.relations,
        )?;
        require(
            expected == record,
            "property graph record is not the authoritative catalog resolution",
        )?;
        if record.name.tenant_scope != self.tenant_scope {
            return Err("property graph belongs to another tenant scope".into());
        }
        if self.relation_names.contains(&record.name) || self.names.contains_key(&record.name) {
            return Err("property graph name collides in the shared relation namespace".into());
        }
        if self.records.contains_key(&record.object_id) {
            return Err("property graph object id already exists".into());
        }
        self.names
            .insert(record.name.clone(), record.object_id.clone());
        for dependency in &record.dependencies {
            self.reverse_dependencies
                .entry(dependency.relation_object_id.clone())
                .or_default()
                .insert(record.object_id.clone());
        }
        self.records.insert(record.object_id.clone(), record);
        Ok(())
    }

    pub fn rename(
        &mut self,
        object_id: &PropertyGraphObjectId,
        new_name: &SqlName,
        next_catalog_revision: u64,
        next_definition_revision: u64,
    ) -> Result<(), String> {
        let current = self
            .records
            .get(object_id)
            .ok_or_else(|| "property graph object id does not exist".to_string())?;
        let renamed = current.renamed(new_name, next_catalog_revision, next_definition_revision)?;
        if self.relation_names.contains(&renamed.name)
            || self
                .names
                .get(&renamed.name)
                .is_some_and(|existing| existing != object_id)
        {
            return Err("renamed property graph collides in the shared relation namespace".into());
        }
        let old_name = current.name.clone();
        self.names.remove(&old_name);
        self.names.insert(renamed.name.clone(), object_id.clone());
        self.records.insert(object_id.clone(), renamed);
        Ok(())
    }

    pub fn get(&self, object_id: &PropertyGraphObjectId) -> Option<&PropertyGraphCatalogRecord> {
        self.records.get(object_id)
    }

    pub fn dependents_of(&self, relation_id: &RelationObjectId) -> Vec<PropertyGraphObjectId> {
        self.reverse_dependencies
            .get(relation_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect()
    }

    /// A base relation cannot be changed while an admitted graph pins its
    /// revision/digest. The transaction layer may use the returned sorted ids
    /// to perform an explicit cascade in the same commit.
    pub fn fence_base_ddl(
        &self,
        relation_id: &RelationObjectId,
        cascade: bool,
    ) -> Result<Vec<PropertyGraphObjectId>, String> {
        let dependents = self.dependents_of(relation_id);
        require(
            cascade || dependents.is_empty(),
            "base relation has property graph dependents",
        )?;
        Ok(dependents)
    }
}

struct RelationIndex<'a> {
    by_name: BTreeMap<CanonicalCatalogName, &'a RelationCatalogSnapshot>,
    leaf_counts: BTreeMap<SqlIdentifier, usize>,
}

impl<'a> RelationIndex<'a> {
    fn new(tenant_scope: &str, relations: &'a [RelationCatalogSnapshot]) -> Result<Self, String> {
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
struct ElementBinding<'a> {
    table: &'a RelationCatalogSnapshot,
    key_columns: Vec<SqlIdentifier>,
    used_columns: BTreeMap<SqlIdentifier, ColumnType>,
}

type DependencyAccumulator = (
    RelationKind,
    CanonicalCatalogName,
    u64,
    String,
    BTreeMap<SqlIdentifier, ColumnType>,
);

impl ElementBinding<'_> {
    fn add_to(
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

fn resolve_vertex<'a>(
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

fn resolve_edge<'a>(
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

fn validate_dependencies(dependencies: &[PropertyGraphDependency]) -> Result<(), String> {
    if dependencies.is_empty() {
        return Err("property graph catalog record has no dependencies".into());
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for dependency in dependencies {
        dependency.validate()?;
        if !ids.insert(&dependency.relation_object_id) {
            return Err("duplicate relation object id in property graph dependencies".into());
        }
        if !names.insert(&dependency.name) {
            return Err("duplicate relation name in property graph dependencies".into());
        }
    }
    let mut canonical = dependencies.to_vec();
    canonical.sort_by(|left, right| left.relation_object_id.cmp(&right.relation_object_id));
    if canonical != dependencies {
        return Err("property graph dependencies are not canonically ordered".into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
