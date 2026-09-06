//! Property-graph element, DDL, and structural validation contracts.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::model::*;
use super::{DropBehavior, ElementKind, GraphOwner};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VertexTableDefinition {
    pub relation: SqlName,
    pub alias: SqlIdentifier,
    /// Empty means “resolve the base table primary key at admission”.
    pub key_columns: Vec<SqlIdentifier>,
    pub key_resolution: ElementKeyResolution,
    pub labels: Vec<LabelDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeEndpoint {
    /// Empty means “resolve an applicable foreign key at admission”.
    pub edge_key_columns: Vec<SqlIdentifier>,
    pub vertex_alias: SqlIdentifier,
    /// Empty means the referenced vertex key.
    pub vertex_key_columns: Vec<SqlIdentifier>,
    pub resolution: EndpointResolution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeTableDefinition {
    pub relation: SqlName,
    pub alias: SqlIdentifier,
    pub key_columns: Vec<SqlIdentifier>,
    pub key_resolution: ElementKeyResolution,
    pub source: EdgeEndpoint,
    pub destination: EdgeEndpoint,
    pub labels: Vec<LabelDefinition>,
}

/// A parsed omission is an explicit semantic requirement, not an empty-key
/// guess. The TableStore/ACL admission slice must replace PrimaryKey with exact
/// catalog columns before this definition may lower.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ElementKeyResolution {
    Explicit,
    PrimaryKey,
}

/// How an edge endpoint obtains its join columns. ForeignKey asks the catalog
/// to select an applicable FK. ExplicitEdgeCatalogVertexKey preserves the
/// standard KEY (...) REFERENCES vertex shorthand whose referenced columns are
/// the admitted vertex key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointResolution {
    Explicit,
    ExplicitEdgeCatalogVertexKey,
    ForeignKey,
}

pub(super) fn validate_definition_header(
    definition: &PropertyGraphDefinition,
) -> Result<(), String> {
    if definition.schema_version != PROPERTY_GRAPH_SCHEMA_VERSION {
        return Err(format!(
            "unsupported property graph schema version {}",
            definition.schema_version
        ));
    }
    validate_bounded_text(
        &definition.tenant_scope,
        "tenant scope",
        MAX_TENANT_SCOPE_BYTES,
    )?;
    definition.name.validate()?;
    if definition.temporary && definition.name.0.len() != 1 {
        return Err("temporary property graph name cannot be schema-qualified".into());
    }
    let count = definition.vertex_tables.len() + definition.edge_tables.len();
    if count == 0 || count > MAX_PROPERTY_GRAPH_ELEMENTS {
        return Err(format!(
            "property graph must contain 1..={MAX_PROPERTY_GRAPH_ELEMENTS} element tables"
        ));
    }
    Ok(())
}

pub(super) fn validate_vertex_tables(
    tables: &[VertexTableDefinition],
) -> Result<(BTreeSet<SqlIdentifier>, BTreeSet<SqlIdentifier>), String> {
    let mut aliases = BTreeSet::new();
    let mut vertices = BTreeSet::new();
    for vertex in tables {
        validate_vertex(vertex)?;
        if !aliases.insert(vertex.alias.clone()) {
            return Err(format!(
                "duplicate element alias `{}`",
                vertex.alias.value()
            ));
        }
        vertices.insert(vertex.alias.clone());
    }
    Ok((aliases, vertices))
}

pub(super) fn validate_edge_tables(
    tables: &[EdgeTableDefinition],
    vertices: &BTreeSet<SqlIdentifier>,
    aliases: &mut BTreeSet<SqlIdentifier>,
) -> Result<(), String> {
    for edge in tables {
        validate_edge(edge)?;
        if !aliases.insert(edge.alias.clone()) {
            return Err(format!("duplicate element alias `{}`", edge.alias.value()));
        }
        validate_endpoint_reference(edge, &edge.source, vertices)?;
        validate_endpoint_reference(edge, &edge.destination, vertices)?;
    }
    Ok(())
}

fn validate_endpoint_reference(
    edge: &EdgeTableDefinition,
    endpoint: &EdgeEndpoint,
    vertices: &BTreeSet<SqlIdentifier>,
) -> Result<(), String> {
    if !vertices.contains(&endpoint.vertex_alias) {
        return Err(format!(
            "edge `{}` references unknown vertex alias `{}`",
            edge.alias.value(),
            endpoint.vertex_alias.value()
        ));
    }
    if !endpoint.edge_key_columns.is_empty()
        && !endpoint.vertex_key_columns.is_empty()
        && endpoint.edge_key_columns.len() != endpoint.vertex_key_columns.len()
    {
        return Err(format!(
            "edge `{}` endpoint key widths differ",
            edge.alias.value()
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterElementAction {
    AddLabel(LabelDefinition),
    DropLabel {
        label: SqlIdentifier,
        behavior: DropBehavior,
    },
    AddProperties {
        label: SqlIdentifier,
        properties: Vec<PropertyDefinition>,
    },
    DropProperties {
        label: SqlIdentifier,
        properties: Vec<SqlIdentifier>,
        behavior: DropBehavior,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlterPropertyGraphAction {
    Add {
        vertex_tables: Vec<VertexTableDefinition>,
        edge_tables: Vec<EdgeTableDefinition>,
    },
    DropTables {
        kind: ElementKind,
        aliases: Vec<SqlIdentifier>,
        behavior: DropBehavior,
    },
    AlterElement {
        kind: ElementKind,
        alias: SqlIdentifier,
        actions: Vec<AlterElementAction>,
    },
    OwnerTo(GraphOwner),
    RenameTo(SqlIdentifier),
    SetSchema(SqlIdentifier),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PropertyGraphStatement {
    Create(PropertyGraphDefinition),
    Alter {
        tenant_scope: String,
        name: SqlName,
        if_exists: bool,
        action: AlterPropertyGraphAction,
    },
    Drop {
        tenant_scope: String,
        names: Vec<SqlName>,
        if_exists: bool,
        behavior: DropBehavior,
    },
}

impl PropertyGraphStatement {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Create(definition) => definition.validate(),
            Self::Alter {
                tenant_scope,
                name,
                action,
                ..
            } => {
                validate_bounded_text(tenant_scope, "tenant scope", MAX_TENANT_SCOPE_BYTES)?;
                name.validate()?;
                validate_alter_action(action)
            }
            Self::Drop {
                tenant_scope,
                names,
                ..
            } => {
                validate_bounded_text(tenant_scope, "tenant scope", MAX_TENANT_SCOPE_BYTES)?;
                if names.is_empty() || names.len() > MAX_PROPERTY_GRAPH_ELEMENTS {
                    return Err("DROP PROPERTY GRAPH has an invalid name count".into());
                }
                names.iter().try_for_each(SqlName::validate)
            }
        }
    }

    pub fn digest(&self) -> Result<String, String> {
        self.validate()?;
        let payload = serde_json::to_vec(self)
            .map_err(|error| format!("encode property graph statement: {error}"))?;
        let mut hasher = Sha256::new();
        hasher.update(b"epistemic-graph/sql-pgq/statement/v1\0");
        hasher.update(payload);
        Ok(hex::encode(hasher.finalize()))
    }
}

fn validate_vertex(vertex: &VertexTableDefinition) -> Result<(), String> {
    vertex.relation.validate()?;
    vertex.alias.validate()?;
    validate_key(&vertex.key_columns)?;
    validate_element_key_resolution(vertex.key_resolution, &vertex.key_columns)?;
    validate_labels(&vertex.alias, &vertex.labels)
}

fn validate_edge(edge: &EdgeTableDefinition) -> Result<(), String> {
    edge.relation.validate()?;
    edge.alias.validate()?;
    validate_key(&edge.key_columns)?;
    validate_element_key_resolution(edge.key_resolution, &edge.key_columns)?;
    validate_endpoint(&edge.source)?;
    validate_endpoint(&edge.destination)?;
    validate_labels(&edge.alias, &edge.labels)
}

fn validate_endpoint(endpoint: &EdgeEndpoint) -> Result<(), String> {
    validate_key(&endpoint.edge_key_columns)?;
    endpoint.vertex_alias.validate()?;
    validate_key(&endpoint.vertex_key_columns)?;
    match endpoint.resolution {
        EndpointResolution::Explicit
            if endpoint.edge_key_columns.is_empty() || endpoint.vertex_key_columns.is_empty() =>
        {
            Err("explicit edge endpoint requires both key lists".into())
        }
        EndpointResolution::ExplicitEdgeCatalogVertexKey
            if endpoint.edge_key_columns.is_empty() || !endpoint.vertex_key_columns.is_empty() =>
        {
            Err("catalog-vertex-key endpoint has inconsistent key lists".into())
        }
        EndpointResolution::ForeignKey
            if !endpoint.edge_key_columns.is_empty() || !endpoint.vertex_key_columns.is_empty() =>
        {
            Err("foreign-key endpoint must remain unresolved until admission".into())
        }
        _ => Ok(()),
    }
}

fn validate_element_key_resolution(
    resolution: ElementKeyResolution,
    columns: &[SqlIdentifier],
) -> Result<(), String> {
    match resolution {
        ElementKeyResolution::Explicit if columns.is_empty() => {
            Err("explicit element key cannot be empty".into())
        }
        ElementKeyResolution::PrimaryKey if !columns.is_empty() => {
            Err("primary-key requirement cannot contain guessed columns".into())
        }
        _ => Ok(()),
    }
}

fn validate_key(columns: &[SqlIdentifier]) -> Result<(), String> {
    if columns.len() > MAX_PROPERTY_GRAPH_KEY_COLUMNS {
        return Err(format!(
            "element key exceeds {MAX_PROPERTY_GRAPH_KEY_COLUMNS} columns"
        ));
    }
    let mut names = BTreeSet::new();
    for column in columns {
        column.validate()?;
        if !names.insert(column) {
            return Err(format!("duplicate key column `{}`", column.value()));
        }
    }
    Ok(())
}

fn validate_labels(alias: &SqlIdentifier, labels: &[LabelDefinition]) -> Result<(), String> {
    if labels.is_empty() || labels.len() > MAX_PROPERTY_GRAPH_LABELS_PER_ELEMENT {
        return Err(format!(
            "element `{}` must contain 1..={MAX_PROPERTY_GRAPH_LABELS_PER_ELEMENT} labels",
            alias.value()
        ));
    }
    validate_unique_labels(alias, labels)?;
    validate_property_sources(alias, labels)
}

fn validate_unique_labels(alias: &SqlIdentifier, labels: &[LabelDefinition]) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for label in labels {
        label.name.validate()?;
        if !names.insert(&label.name) {
            return Err(format!(
                "element `{}` repeats label `{}`",
                alias.value(),
                label.name.value()
            ));
        }
        validate_properties(&label.properties)?;
    }
    Ok(())
}

fn validate_property_sources(
    alias: &SqlIdentifier,
    labels: &[LabelDefinition],
) -> Result<(), String> {
    let mut property_sources = BTreeMap::new();
    for label in labels {
        if let PropertySet::Explicit(properties) = &label.properties {
            for property in properties {
                validate_property_source(alias, property, &mut property_sources)?;
            }
        }
    }
    Ok(())
}

fn validate_property_source<'a>(
    alias: &SqlIdentifier,
    property: &'a PropertyDefinition,
    property_sources: &mut BTreeMap<&'a SqlIdentifier, &'a SqlIdentifier>,
) -> Result<(), String> {
    let previous = property_sources.insert(&property.property_name, &property.source_column);
    if previous.is_some_and(|source| source != &property.source_column) {
        return Err(format!(
            "element '{}' maps property '{}' to inconsistent source columns",
            alias.value(),
            property.property_name.value()
        ));
    }
    Ok(())
}

fn validate_properties(properties: &PropertySet) -> Result<(), String> {
    let PropertySet::Explicit(properties) = properties else {
        return Ok(());
    };
    if properties.is_empty() || properties.len() > MAX_PROPERTY_GRAPH_PROPERTIES_PER_LABEL {
        return Err(format!(
            "explicit property set must contain 1..={MAX_PROPERTY_GRAPH_PROPERTIES_PER_LABEL} properties"
        ));
    }
    let mut names = BTreeSet::new();
    for property in properties {
        property.source_column.validate()?;
        property.property_name.validate()?;
        if !names.insert(&property.property_name) {
            return Err(format!(
                "duplicate property `{}`",
                property.property_name.value()
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_shared_labels(definition: &PropertyGraphDefinition) -> Result<(), String> {
    let mut shapes: BTreeMap<&SqlIdentifier, Option<BTreeSet<&SqlIdentifier>>> = BTreeMap::new();
    for labels in definition
        .vertex_tables
        .iter()
        .map(|table| &table.labels)
        .chain(definition.edge_tables.iter().map(|table| &table.labels))
    {
        for label in labels {
            merge_shared_label_shape(&mut shapes, label)?;
        }
    }
    Ok(())
}

fn merge_shared_label_shape<'a>(
    shapes: &mut BTreeMap<&'a SqlIdentifier, Option<BTreeSet<&'a SqlIdentifier>>>,
    label: &'a LabelDefinition,
) -> Result<(), String> {
    let names = label_property_names(label);
    let Some(expected) = shapes.get(&label.name) else {
        shapes.insert(&label.name, names);
        return Ok(());
    };
    if expected.is_some() && names.is_some() && expected != &names {
        return Err(format!(
            "label `{}` exposes inconsistent property names",
            label.name.value()
        ));
    }
    if expected.is_none() && names.is_some() {
        shapes.insert(&label.name, names);
    }
    Ok(())
}

fn label_property_names(label: &LabelDefinition) -> Option<BTreeSet<&SqlIdentifier>> {
    match &label.properties {
        PropertySet::None => Some(BTreeSet::new()),
        PropertySet::Explicit(properties) => Some(
            properties
                .iter()
                .map(|property| &property.property_name)
                .collect(),
        ),
        // Requires authoritative base-schema expansion at admission.
        PropertySet::AllColumns => None,
    }
}

fn validate_alter_action(action: &AlterPropertyGraphAction) -> Result<(), String> {
    match action {
        AlterPropertyGraphAction::Add {
            vertex_tables,
            edge_tables,
        } => validate_alter_add(vertex_tables, edge_tables),
        AlterPropertyGraphAction::DropTables { aliases, .. } => validate_alter_drop(aliases),
        AlterPropertyGraphAction::AlterElement { alias, actions, .. } => {
            validate_alter_element(alias, actions)
        }
        AlterPropertyGraphAction::OwnerTo(GraphOwner::Role(owner)) => owner.validate(),
        AlterPropertyGraphAction::OwnerTo(_) => Ok(()),
        AlterPropertyGraphAction::RenameTo(name) | AlterPropertyGraphAction::SetSchema(name) => {
            name.validate()
        }
    }
}

fn validate_alter_add(
    vertex_tables: &[VertexTableDefinition],
    edge_tables: &[EdgeTableDefinition],
) -> Result<(), String> {
    let count = vertex_tables.len() + edge_tables.len();
    if count == 0 || count > MAX_PROPERTY_GRAPH_ELEMENTS {
        return Err("ALTER ADD has an invalid element count".into());
    }
    vertex_tables.iter().try_for_each(validate_vertex)?;
    edge_tables.iter().try_for_each(validate_edge)?;
    let aliases = vertex_tables
        .iter()
        .map(|table| &table.alias)
        .chain(edge_tables.iter().map(|table| &table.alias));
    validate_unique_alter_aliases(aliases)
}

fn validate_unique_alter_aliases<'a>(
    aliases: impl Iterator<Item = &'a SqlIdentifier>,
) -> Result<(), String> {
    let mut unique = BTreeSet::new();
    for alias in aliases {
        if !unique.insert(alias) {
            return Err(format!("duplicate ALTER ADD alias: {}", alias.value()));
        }
    }
    Ok(())
}

fn validate_alter_drop(aliases: &[SqlIdentifier]) -> Result<(), String> {
    if aliases.is_empty() || aliases.len() > MAX_PROPERTY_GRAPH_ELEMENTS {
        return Err("ALTER DROP TABLES has an invalid alias count".into());
    }
    aliases.iter().try_for_each(SqlIdentifier::validate)
}

fn validate_alter_element(
    alias: &SqlIdentifier,
    actions: &[AlterElementAction],
) -> Result<(), String> {
    alias.validate()?;
    if actions.is_empty() || actions.len() > MAX_PROPERTY_GRAPH_LABELS_PER_ELEMENT {
        return Err("ALTER element has an invalid action count".into());
    }
    actions.iter().try_for_each(validate_alter_element_action)
}

fn validate_alter_element_action(action: &AlterElementAction) -> Result<(), String> {
    match action {
        AlterElementAction::AddLabel(label) => {
            label.name.validate()?;
            validate_properties(&label.properties)
        }
        AlterElementAction::DropLabel { label, .. } => label.validate(),
        AlterElementAction::AddProperties { label, properties } => {
            label.validate()?;
            validate_properties(&PropertySet::Explicit(properties.clone()))
        }
        AlterElementAction::DropProperties {
            label, properties, ..
        } => {
            label.validate()?;
            validate_dropped_properties(properties)
        }
    }
}

fn validate_dropped_properties(properties: &[SqlIdentifier]) -> Result<(), String> {
    if properties.is_empty() || properties.len() > MAX_PROPERTY_GRAPH_PROPERTIES_PER_LABEL {
        return Err("ALTER DROP PROPERTIES has an invalid property count".into());
    }
    let mut names = BTreeSet::new();
    for property in properties {
        property.validate()?;
        if !names.insert(property) {
            return Err(format!("duplicate property '{}'", property.value()));
        }
    }
    Ok(())
}
