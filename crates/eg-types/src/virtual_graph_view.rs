//! Typed property-graph view over approved R2RML mappings
//! (EG-UNIFIED-DATA-PLANE-R038.6.1). A node label maps to one ontology
//! class. A relationship type maps to a join between two mapped labels
//! through approved mappings (EG-UNIFIED-DATA-PLANE-R005). The view is read
//! only; openCypher and Gremlin patterns resolve against it.

use serde::{Deserialize, Serialize};

/// One node label and the ontology class and approved mapping behind it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeLabelMapping {
    pub label: String,
    pub ontology_class: String,
    pub mapping_id: String,
}

/// One relationship type: a join between two mapped labels.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipTypeMapping {
    pub rel_type: String,
    pub from_label: String,
    pub to_label: String,
    pub join_mapping_ids: Vec<String>,
    pub join_condition: String,
}

/// A read-only graph view over approved mappings, bound to their digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphView {
    pub mapping_digest: String,
    pub node_labels: Vec<NodeLabelMapping>,
    pub relationship_types: Vec<RelationshipTypeMapping>,
}

/// A graph view failed validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidGraphView {
    MissingMappingDigest,
    /// `UNMAPPED_LABEL`: a label with no ontology class or mapping, or a
    /// relationship endpoint that names an undeclared label.
    UnmappedLabel(String),
    /// A relationship type with no approved join mapping.
    UnmappedRelationship(String),
}

impl InvalidGraphView {
    /// Stable refusal code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::MissingMappingDigest => "GRAPH_VIEW_DIGEST_MISSING",
            Self::UnmappedLabel(_) => "UNMAPPED_LABEL",
            Self::UnmappedRelationship(_) => "UNMAPPED_RELATIONSHIP",
        }
    }
}

impl std::fmt::Display for InvalidGraphView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingMappingDigest => write!(f, "graph view names no mapping digest"),
            Self::UnmappedLabel(label) => write!(f, "label {label:?} has no mapping"),
            Self::UnmappedRelationship(rel) => {
                write!(f, "relationship type {rel:?} has no approved join")
            }
        }
    }
}

impl std::error::Error for InvalidGraphView {}

impl GraphView {
    /// The mapping of `label`, if the view declares it.
    pub fn label(&self, label: &str) -> Option<&NodeLabelMapping> {
        self.node_labels.iter().find(|node| node.label == label)
    }

    /// Resolve `label` or refuse with `UNMAPPED_LABEL`.
    pub fn resolve_label(&self, label: &str) -> Result<&NodeLabelMapping, InvalidGraphView> {
        self.label(label)
            .ok_or_else(|| InvalidGraphView::UnmappedLabel(label.to_string()))
    }

    /// Refuse a view with no digest, an unmapped label, or a relationship
    /// over an undeclared label or without an approved join.
    pub fn validate(&self) -> Result<(), InvalidGraphView> {
        if self.mapping_digest.trim().is_empty() {
            return Err(InvalidGraphView::MissingMappingDigest);
        }
        for node in &self.node_labels {
            if node.ontology_class.trim().is_empty() || node.mapping_id.trim().is_empty() {
                return Err(InvalidGraphView::UnmappedLabel(node.label.clone()));
            }
        }
        for rel in &self.relationship_types {
            self.resolve_label(&rel.from_label)?;
            self.resolve_label(&rel.to_label)?;
            if rel.join_mapping_ids.is_empty() || rel.join_condition.trim().is_empty() {
                return Err(InvalidGraphView::UnmappedRelationship(rel.rel_type.clone()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view() -> GraphView {
        GraphView {
            mapping_digest: "sha256:abc".to_string(),
            node_labels: vec![
                NodeLabelMapping {
                    label: "Person".to_string(),
                    ontology_class: "schema:Person".to_string(),
                    mapping_id: "m-person@3".to_string(),
                },
                NodeLabelMapping {
                    label: "Org".to_string(),
                    ontology_class: "schema:Organization".to_string(),
                    mapping_id: "m-org@1".to_string(),
                },
            ],
            relationship_types: vec![RelationshipTypeMapping {
                rel_type: "WORKS_FOR".to_string(),
                from_label: "Person".to_string(),
                to_label: "Org".to_string(),
                join_mapping_ids: vec!["m-person@3".to_string(), "m-org@1".to_string()],
                join_condition: "person.org_id = org.id".to_string(),
            }],
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R038.6.1
    #[test]
    fn graph_view_round_trips() {
        let original = view();
        assert_eq!(original.validate(), Ok(()));
        let json = serde_json::to_string(&original).unwrap();
        let back: GraphView = serde_json::from_str(&json).unwrap();
        assert_eq!(back, original);

        let mut unmapped = view();
        unmapped.relationship_types[0].to_label = "Company".to_string();
        let error = unmapped.validate().unwrap_err();
        assert_eq!(error.code(), "UNMAPPED_LABEL");
        assert_eq!(
            view().resolve_label("Ghost").unwrap_err(),
            InvalidGraphView::UnmappedLabel("Ghost".to_string())
        );

        let mut classless = view();
        classless.node_labels[1].ontology_class = String::new();
        assert_eq!(classless.validate().unwrap_err().code(), "UNMAPPED_LABEL");
    }
}
