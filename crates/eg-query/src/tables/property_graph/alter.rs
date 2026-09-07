//! Pure `ALTER PROPERTY GRAPH` transformations over an admitted definition.
//!
//! Every action is expressed as a new draft definition, which the catalog
//! admission path then re-resolves against the authoritative relation
//! snapshots. This module therefore owns no storage, no identity, and no
//! privilege decision — only the shape of the requested change.

use std::collections::BTreeSet;

use super::{
    AlterElementAction, AlterPropertyGraphAction, DropBehavior, EdgeTableDefinition, ElementKind,
    LabelDefinition, PropertyDefinition, PropertyGraphDefinition, PropertySet, SqlIdentifier,
    SqlName, VertexTableDefinition,
};

/// Apply one ALTER action to `definition`, returning the resulting draft.
/// `OWNER TO` changes no definition byte; the caller applies the new owner to
/// the catalog record instead.
pub(super) fn apply_alter_action(
    definition: &PropertyGraphDefinition,
    action: &AlterPropertyGraphAction,
) -> Result<PropertyGraphDefinition, String> {
    let mut name = definition.name.clone();
    let mut vertices = definition.vertex_tables.clone();
    let mut edges = definition.edge_tables.clone();
    match action {
        AlterPropertyGraphAction::RenameTo(new_name) => {
            name = SqlName::new(vec![new_name.clone()])?;
        }
        AlterPropertyGraphAction::SetSchema(_) => {
            return Err(
                "ALTER PROPERTY GRAPH SET SCHEMA requires a multi-schema catalog and is rejected"
                    .into(),
            );
        }
        AlterPropertyGraphAction::OwnerTo(_) => {}
        AlterPropertyGraphAction::Add {
            vertex_tables,
            edge_tables,
        } => add_elements(&mut vertices, &mut edges, vertex_tables, edge_tables)?,
        AlterPropertyGraphAction::DropTables {
            kind,
            aliases,
            behavior,
        } => drop_elements(&mut vertices, &mut edges, *kind, aliases, *behavior)?,
        AlterPropertyGraphAction::AlterElement {
            kind,
            alias,
            actions,
        } => alter_element(&mut vertices, &mut edges, *kind, alias, actions)?,
    }
    PropertyGraphDefinition::new(
        definition.tenant_scope.clone(),
        name,
        false,
        vertices,
        edges,
    )
}

fn element_aliases(
    vertices: &[VertexTableDefinition],
    edges: &[EdgeTableDefinition],
) -> BTreeSet<SqlIdentifier> {
    vertices
        .iter()
        .map(|table| table.alias.clone())
        .chain(edges.iter().map(|table| table.alias.clone()))
        .collect()
}

fn add_elements(
    vertices: &mut Vec<VertexTableDefinition>,
    edges: &mut Vec<EdgeTableDefinition>,
    new_vertices: &[VertexTableDefinition],
    new_edges: &[EdgeTableDefinition],
) -> Result<(), String> {
    let existing = element_aliases(vertices, edges);
    for alias in new_vertices
        .iter()
        .map(|table| &table.alias)
        .chain(new_edges.iter().map(|table| &table.alias))
    {
        if existing.contains(alias) {
            return Err(format!(
                "property graph already contains element `{}`",
                alias.value()
            ));
        }
    }
    vertices.extend_from_slice(new_vertices);
    edges.extend_from_slice(new_edges);
    Ok(())
}

fn drop_elements(
    vertices: &mut Vec<VertexTableDefinition>,
    edges: &mut Vec<EdgeTableDefinition>,
    kind: ElementKind,
    aliases: &[SqlIdentifier],
    behavior: DropBehavior,
) -> Result<(), String> {
    let dropped: BTreeSet<_> = aliases.iter().cloned().collect();
    match kind {
        ElementKind::Vertex => {
            require_present(
                vertices.iter().map(|table| &table.alias),
                &dropped,
                "vertex",
            )?;
            let dependents: Vec<_> = edges
                .iter()
                .filter(|edge| edge_references(edge, &dropped))
                .map(|edge| edge.alias.value().to_string())
                .collect();
            if !dependents.is_empty() && behavior != DropBehavior::Cascade {
                return Err(format!(
                    "vertex tables are referenced by edge tables {}; use CASCADE",
                    dependents.join(", ")
                ));
            }
            edges.retain(|edge| !edge_references(edge, &dropped));
            vertices.retain(|table| !dropped.contains(&table.alias));
        }
        ElementKind::Edge => {
            require_present(edges.iter().map(|table| &table.alias), &dropped, "edge")?;
            edges.retain(|table| !dropped.contains(&table.alias));
        }
    }
    Ok(())
}

fn edge_references(edge: &EdgeTableDefinition, dropped: &BTreeSet<SqlIdentifier>) -> bool {
    dropped.contains(&edge.source.vertex_alias) || dropped.contains(&edge.destination.vertex_alias)
}

fn require_present<'a>(
    present: impl Iterator<Item = &'a SqlIdentifier>,
    requested: &BTreeSet<SqlIdentifier>,
    kind: &str,
) -> Result<(), String> {
    let present: BTreeSet<_> = present.cloned().collect();
    for alias in requested {
        if !present.contains(alias) {
            return Err(format!(
                "property graph has no {kind} table `{}`",
                alias.value()
            ));
        }
    }
    Ok(())
}

fn alter_element(
    vertices: &mut [VertexTableDefinition],
    edges: &mut [EdgeTableDefinition],
    kind: ElementKind,
    alias: &SqlIdentifier,
    actions: &[AlterElementAction],
) -> Result<(), String> {
    let labels = match kind {
        ElementKind::Vertex => vertices
            .iter_mut()
            .find(|table| &table.alias == alias)
            .map(|table| &mut table.labels),
        ElementKind::Edge => edges
            .iter_mut()
            .find(|table| &table.alias == alias)
            .map(|table| &mut table.labels),
    };
    let labels =
        labels.ok_or_else(|| format!("property graph has no element table `{}`", alias.value()))?;
    for action in actions {
        apply_label_action(labels, action)?;
    }
    Ok(())
}

fn apply_label_action(
    labels: &mut Vec<LabelDefinition>,
    action: &AlterElementAction,
) -> Result<(), String> {
    match action {
        AlterElementAction::AddLabel(label) => add_label(labels, label),
        AlterElementAction::DropLabel { label, .. } => drop_label(labels, label),
        AlterElementAction::AddProperties { label, properties } => {
            add_properties(find_label(labels, label)?, properties)
        }
        AlterElementAction::DropProperties {
            label, properties, ..
        } => drop_properties(find_label(labels, label)?, properties),
    }
}

fn add_label(labels: &mut Vec<LabelDefinition>, label: &LabelDefinition) -> Result<(), String> {
    if labels.iter().any(|item| item.name == label.name) {
        return Err(format!(
            "element already exposes label `{}`",
            label.name.value()
        ));
    }
    labels.push(label.clone());
    Ok(())
}

fn drop_label(labels: &mut Vec<LabelDefinition>, label: &SqlIdentifier) -> Result<(), String> {
    let before = labels.len();
    labels.retain(|item| &item.name != label);
    if labels.len() == before {
        return Err(format!("element has no label `{}`", label.value()));
    }
    Ok(())
}

fn find_label<'a>(
    labels: &'a mut [LabelDefinition],
    name: &SqlIdentifier,
) -> Result<&'a mut LabelDefinition, String> {
    labels
        .iter_mut()
        .find(|item| &item.name == name)
        .ok_or_else(|| format!("element has no label `{}`", name.value()))
}

fn add_properties(
    label: &mut LabelDefinition,
    properties: &[PropertyDefinition],
) -> Result<(), String> {
    let mut current = match &label.properties {
        PropertySet::None => Vec::new(),
        PropertySet::Explicit(values) => values.clone(),
        PropertySet::AllColumns => {
            return Err(format!(
                "label `{}` exposes every base column; it has no explicit property set",
                label.name.value()
            ));
        }
    };
    for property in properties {
        if current
            .iter()
            .any(|item| item.property_name == property.property_name)
        {
            return Err(format!(
                "label `{}` already exposes property `{}`",
                label.name.value(),
                property.property_name.value()
            ));
        }
        current.push(property.clone());
    }
    label.properties = PropertySet::Explicit(current);
    Ok(())
}

fn drop_properties(
    label: &mut LabelDefinition,
    properties: &[SqlIdentifier],
) -> Result<(), String> {
    let PropertySet::Explicit(current) = &mut label.properties else {
        return Err(format!(
            "label `{}` has no explicit property set to drop from",
            label.name.value()
        ));
    };
    for property in properties {
        let before = current.len();
        current.retain(|item| &item.property_name != property);
        if current.len() == before {
            return Err(format!(
                "label `{}` has no property `{}`",
                label.name.value(),
                property.value()
            ));
        }
    }
    if current.is_empty() {
        label.properties = PropertySet::None;
    }
    Ok(())
}
