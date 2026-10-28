//! Typed hierarchy retrieval policy and stage evidence.
//!
//! `EG-FEDERATED-QUERY-R081.9.7.1` owns the pure model for community and
//! document-section retrieval.  Traversal and serving stay above `eg-types`;
//! this module only validates the identity, tree shape, and bounded policy
//! fields that those runtimes must carry.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The two hierarchy families share one policy shape but have different node
/// contracts.  A document-section policy never silently falls back to a
/// community policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HierarchyRetrievalKind {
    Community,
    DocumentSection,
}

/// The stored granularity of one document-section node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SectionGranularity {
    Document,
    Section,
    SourceSpan,
}

/// The stage whose score and visited evidence a receipt describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HierarchyStage {
    Community,
    DocumentSection,
}

/// A parent relation carried by a hierarchy policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchyParentLink {
    pub parent_id: String,
    pub child_id: String,
}

/// An inclusive, document-revision-bound source span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SectionSourceSpan {
    pub start: u64,
    pub end: u64,
}

/// One stored node in a document-section hierarchy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchySectionNode {
    pub id: String,
    pub document_id: String,
    pub document_revision: String,
    pub parent_id: Option<String>,
    pub ordered_children: Vec<String>,
    pub granularity: SectionGranularity,
    pub summary: String,
    pub source_span: SectionSourceSpan,
}

/// A score input is named and revision-bound so a runtime can reconstruct
/// why a candidate was considered without treating a score as authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchyScoreProvenance {
    pub scorer: String,
    pub scorer_revision: String,
    pub basis: String,
}

/// The bounded policy shared by community and document-section retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchyRetrievalPolicy {
    pub kind: HierarchyRetrievalKind,
    pub hierarchy_revision: String,
    pub community_ids: Vec<String>,
    pub summary_ids: Vec<String>,
    pub entity_ids: Vec<String>,
    pub parent_links: Vec<HierarchyParentLink>,
    pub stage: HierarchyStage,
    pub score_provenance: Vec<HierarchyScoreProvenance>,
    pub beam_width: u32,
    pub max_depth: u32,
    pub max_visited_communities: u32,
    pub max_entity_candidates: u32,
    pub max_reranker_calls: u32,
    pub max_tokens: u32,
    pub max_total_cost_micros: u64,
    pub section_nodes: Vec<HierarchySectionNode>,
}

/// Evidence emitted by a bounded hierarchy stage.  It is deliberately a
/// record, not an authorization or a claim that serving has completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HierarchyStageReceipt {
    pub hierarchy_revision: String,
    pub kind: HierarchyRetrievalKind,
    pub stage: HierarchyStage,
    pub visited_ids: Vec<String>,
    pub selected_id: Option<String>,
    pub pruned_ids: Vec<String>,
    pub score_provenance: Vec<HierarchyScoreProvenance>,
    pub beam_width: u32,
    pub depth: u32,
}

/// Fail-closed policy and tree-shape errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HierarchyValidationError {
    #[error("{0} must not be empty")]
    EmptyField(&'static str),
    #[error("{0} must be greater than zero")]
    ZeroLimit(&'static str),
    #[error("duplicate identifier '{0}'")]
    DuplicateIdentifier(String),
    #[error("community policy cannot carry document-section nodes")]
    CommunityHasSectionNodes,
    #[error("document-section policy requires at least one section node")]
    MissingSectionNodes,
    #[error("unknown node '{0}'")]
    UnknownNode(String),
    #[error("unknown parent-link endpoint '{0}'")]
    UnknownParentLinkEndpoint(String),
    #[error("the document-section hierarchy must contain exactly one root")]
    InvalidRootCount,
    #[error("node '{0}' is its own parent")]
    SelfParent(String),
    #[error("node '{0}' is listed more than once by its parent")]
    DuplicateChild(String),
    #[error("parent '{parent_id}' and child '{child_id}' disagree about their link")]
    ParentChildMismatch { parent_id: String, child_id: String },
    #[error("policy parent_links do not match section-node parent_id fields")]
    ParentLinkMismatch,
    #[error("cycle detected through node '{0}'")]
    Cycle(String),
    #[error("document-section nodes do not share document identity: '{0}'")]
    DocumentIdentityMismatch(String),
    #[error("source span for node '{0}' has start after end")]
    InvalidSpan(String),
    #[error("child '{child_id}' is outside parent '{parent_id}' source span")]
    SpanNotNested { parent_id: String, child_id: String },
    #[error("children of parent '{parent_id}' are not in source-span order at '{child_id}'")]
    ChildOrderMismatch { parent_id: String, child_id: String },
    #[error("node '{0}' has an invalid granularity for its position")]
    InvalidGranularity(String),
    #[error("receipt selected_id is not among visited_ids")]
    SelectedNodeNotVisited,
    #[error("receipt kind and stage do not agree")]
    ReceiptKindStageMismatch,
}

impl HierarchyRetrievalPolicy {
    /// Validate identity, limits, and the complete section tree before any
    /// traversal or serving code consumes this policy.
    pub fn validate(&self) -> Result<(), HierarchyValidationError> {
        validate_text("hierarchy_revision", &self.hierarchy_revision)?;
        validate_limits(self)?;
        validate_unique_ids(&self.community_ids)?;
        validate_unique_ids(&self.summary_ids)?;
        validate_unique_ids(&self.entity_ids)?;
        validate_score_provenance(&self.score_provenance)?;
        if (self.kind == HierarchyRetrievalKind::Community)
            != (self.stage == HierarchyStage::Community)
        {
            return Err(HierarchyValidationError::ReceiptKindStageMismatch);
        }

        match self.kind {
            HierarchyRetrievalKind::Community => {
                if !self.section_nodes.is_empty() {
                    return Err(HierarchyValidationError::CommunityHasSectionNodes);
                }
                validate_links_against_ids(&self.parent_links, &self.community_ids)
            }
            HierarchyRetrievalKind::DocumentSection => self.validate_section_tree(),
        }
    }

    fn validate_section_tree(&self) -> Result<(), HierarchyValidationError> {
        if self.section_nodes.is_empty() {
            return Err(HierarchyValidationError::MissingSectionNodes);
        }
        let nodes = collect_nodes(&self.section_nodes)?;
        validate_document_identity(&nodes)?;
        validate_roots(&nodes)?;
        validate_node_edges(&nodes)?;
        validate_parent_links_match_nodes(&self.parent_links, &nodes)?;
        validate_parent_cycles(&nodes)
    }
}

impl HierarchyStageReceipt {
    /// Validate the receipt's local bounds and stage identity.
    pub fn validate(&self) -> Result<(), HierarchyValidationError> {
        validate_text("hierarchy_revision", &self.hierarchy_revision)?;
        if self.beam_width == 0 {
            return Err(HierarchyValidationError::ZeroLimit("beam_width"));
        }
        validate_unique_ids(&self.visited_ids)?;
        validate_unique_ids(&self.pruned_ids)?;
        validate_score_provenance(&self.score_provenance)?;
        if self
            .selected_id
            .as_ref()
            .is_some_and(|selected| !self.visited_ids.iter().any(|id| id == selected))
        {
            return Err(HierarchyValidationError::SelectedNodeNotVisited);
        }
        if (self.kind == HierarchyRetrievalKind::Community)
            != (self.stage == HierarchyStage::Community)
        {
            return Err(HierarchyValidationError::ReceiptKindStageMismatch);
        }
        Ok(())
    }
}

fn validate_text(name: &'static str, value: &str) -> Result<(), HierarchyValidationError> {
    if value.trim().is_empty() {
        Err(HierarchyValidationError::EmptyField(name))
    } else {
        Ok(())
    }
}

fn validate_limits(policy: &HierarchyRetrievalPolicy) -> Result<(), HierarchyValidationError> {
    let limits = [
        ("beam_width", policy.beam_width as u64),
        ("max_depth", policy.max_depth as u64),
        (
            "max_visited_communities",
            policy.max_visited_communities as u64,
        ),
        ("max_entity_candidates", policy.max_entity_candidates as u64),
        ("max_reranker_calls", policy.max_reranker_calls as u64),
        ("max_tokens", policy.max_tokens as u64),
        ("max_total_cost_micros", policy.max_total_cost_micros),
    ];
    limits
        .into_iter()
        .find(|(_, value)| *value == 0)
        .map_or(Ok(()), |(name, _)| {
            Err(HierarchyValidationError::ZeroLimit(name))
        })
}

fn validate_unique_ids(ids: &[String]) -> Result<(), HierarchyValidationError> {
    let mut seen = BTreeSet::new();
    for id in ids {
        validate_text("identifier", id)?;
        if !seen.insert(id) {
            return Err(HierarchyValidationError::DuplicateIdentifier(id.clone()));
        }
    }
    Ok(())
}

fn validate_score_provenance(
    provenance: &[HierarchyScoreProvenance],
) -> Result<(), HierarchyValidationError> {
    for entry in provenance {
        validate_text("score_provenance.scorer", &entry.scorer)?;
        validate_text("score_provenance.scorer_revision", &entry.scorer_revision)?;
        validate_text("score_provenance.basis", &entry.basis)?;
    }
    Ok(())
}

fn validate_links_against_ids(
    links: &[HierarchyParentLink],
    allowed_ids: &[String],
) -> Result<(), HierarchyValidationError> {
    let allowed = allowed_ids.iter().collect::<BTreeSet<_>>();
    let mut children = BTreeSet::new();
    for link in links {
        if !allowed.contains(&link.parent_id) {
            return Err(HierarchyValidationError::UnknownParentLinkEndpoint(
                link.parent_id.clone(),
            ));
        }
        if !allowed.contains(&link.child_id) {
            return Err(HierarchyValidationError::UnknownParentLinkEndpoint(
                link.child_id.clone(),
            ));
        }
        if link.parent_id == link.child_id {
            return Err(HierarchyValidationError::SelfParent(link.child_id.clone()));
        }
        if !children.insert(&link.child_id) {
            return Err(HierarchyValidationError::DuplicateChild(
                link.child_id.clone(),
            ));
        }
    }
    Ok(())
}

fn collect_nodes(
    section_nodes: &[HierarchySectionNode],
) -> Result<BTreeMap<String, &HierarchySectionNode>, HierarchyValidationError> {
    let mut nodes = BTreeMap::new();
    for node in section_nodes {
        validate_text("section.id", &node.id)?;
        validate_text("section.document_id", &node.document_id)?;
        validate_text("section.document_revision", &node.document_revision)?;
        validate_text("section.summary", &node.summary)?;
        if node.source_span.start > node.source_span.end {
            return Err(HierarchyValidationError::InvalidSpan(node.id.clone()));
        }
        if nodes.insert(node.id.clone(), node).is_some() {
            return Err(HierarchyValidationError::DuplicateIdentifier(
                node.id.clone(),
            ));
        }
    }
    Ok(nodes)
}

fn validate_document_identity(
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    let first = nodes.values().next().expect("section_nodes is non-empty");
    for node in nodes.values() {
        if node.document_id != first.document_id
            || node.document_revision != first.document_revision
        {
            return Err(HierarchyValidationError::DocumentIdentityMismatch(
                node.id.clone(),
            ));
        }
    }
    Ok(())
}

fn validate_roots(
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    let roots = nodes
        .values()
        .filter(|node| node.parent_id.is_none())
        .count();
    if roots == 1 {
        Ok(())
    } else {
        Err(HierarchyValidationError::InvalidRootCount)
    }
}

fn validate_node_edges(
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    for node in nodes.values() {
        validate_node_granularity(node)?;
        let mut seen_children = BTreeSet::new();
        let mut previous_span = None;
        for child_id in &node.ordered_children {
            if !seen_children.insert(child_id) {
                return Err(HierarchyValidationError::DuplicateChild(child_id.clone()));
            }
            let child = nodes
                .get(child_id)
                .ok_or_else(|| HierarchyValidationError::UnknownNode(child_id.clone()))?;
            if child.parent_id.as_deref() != Some(node.id.as_str()) {
                return Err(HierarchyValidationError::ParentChildMismatch {
                    parent_id: node.id.clone(),
                    child_id: child.id.clone(),
                });
            }
            if let Some(previous) = previous_span {
                if (child.source_span.start, child.source_span.end) < previous {
                    return Err(HierarchyValidationError::ChildOrderMismatch {
                        parent_id: node.id.clone(),
                        child_id: child.id.clone(),
                    });
                }
            }
            previous_span = Some((child.source_span.start, child.source_span.end));
            if child.source_span.start < node.source_span.start
                || child.source_span.end > node.source_span.end
            {
                return Err(HierarchyValidationError::SpanNotNested {
                    parent_id: node.id.clone(),
                    child_id: child.id.clone(),
                });
            }
        }
        if let Some(parent_id) = &node.parent_id {
            let parent = nodes
                .get(parent_id)
                .ok_or_else(|| HierarchyValidationError::UnknownNode(parent_id.clone()))?;
            if !parent.ordered_children.iter().any(|id| id == &node.id) {
                return Err(HierarchyValidationError::ParentChildMismatch {
                    parent_id: parent.id.clone(),
                    child_id: node.id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_node_granularity(node: &HierarchySectionNode) -> Result<(), HierarchyValidationError> {
    if node.parent_id.is_none() && node.granularity != SectionGranularity::Document {
        return Err(HierarchyValidationError::InvalidGranularity(
            node.id.clone(),
        ));
    }
    if node.parent_id.is_some() && node.granularity == SectionGranularity::Document {
        return Err(HierarchyValidationError::InvalidGranularity(
            node.id.clone(),
        ));
    }
    if node.granularity == SectionGranularity::SourceSpan && !node.ordered_children.is_empty() {
        return Err(HierarchyValidationError::InvalidGranularity(
            node.id.clone(),
        ));
    }
    Ok(())
}

fn validate_parent_links_match_nodes(
    links: &[HierarchyParentLink],
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    let expected = nodes
        .values()
        .filter_map(|node| {
            node.parent_id
                .as_ref()
                .map(|parent_id| (parent_id.clone(), node.id.clone()))
        })
        .collect::<BTreeSet<_>>();
    let actual = links
        .iter()
        .map(|link| (link.parent_id.clone(), link.child_id.clone()))
        .collect::<BTreeSet<_>>();
    if expected == actual {
        Ok(())
    } else {
        Err(HierarchyValidationError::ParentLinkMismatch)
    }
}

fn validate_parent_cycles(
    nodes: &BTreeMap<String, &HierarchySectionNode>,
) -> Result<(), HierarchyValidationError> {
    for node in nodes.values() {
        let mut visited = BTreeSet::new();
        let mut current = Some(node.id.as_str());
        while let Some(id) = current {
            if !visited.insert(id) {
                return Err(HierarchyValidationError::Cycle(id.to_string()));
            }
            current = nodes
                .get(id)
                .and_then(|candidate| candidate.parent_id.as_deref());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(kind: HierarchyRetrievalKind) -> HierarchyRetrievalPolicy {
        HierarchyRetrievalPolicy {
            kind,
            hierarchy_revision: "hierarchy-r1".to_string(),
            community_ids: vec!["community-1".to_string()],
            summary_ids: vec!["summary-1".to_string()],
            entity_ids: vec!["entity-1".to_string()],
            parent_links: Vec::new(),
            stage: match kind {
                HierarchyRetrievalKind::Community => HierarchyStage::Community,
                HierarchyRetrievalKind::DocumentSection => HierarchyStage::DocumentSection,
            },
            score_provenance: vec![HierarchyScoreProvenance {
                scorer: "declared-score".to_string(),
                scorer_revision: "score-r1".to_string(),
                basis: "stored-summary".to_string(),
            }],
            beam_width: 4,
            max_depth: 8,
            max_visited_communities: 16,
            max_entity_candidates: 32,
            max_reranker_calls: 2,
            max_tokens: 512,
            max_total_cost_micros: 10_000,
            section_nodes: Vec::new(),
        }
    }

    fn node(
        id: &str,
        parent_id: Option<&str>,
        children: &[&str],
        granularity: SectionGranularity,
        start: u64,
        end: u64,
    ) -> HierarchySectionNode {
        HierarchySectionNode {
            id: id.to_string(),
            document_id: "document-1".to_string(),
            document_revision: "document-r1".to_string(),
            parent_id: parent_id.map(str::to_string),
            ordered_children: children.iter().map(|child| (*child).to_string()).collect(),
            granularity,
            summary: format!("summary for {id}"),
            source_span: SectionSourceSpan { start, end },
        }
    }

    #[test]
    fn document_section_policy_validates_nested_tree_and_round_trips() {
        let mut document = policy(HierarchyRetrievalKind::DocumentSection);
        document.section_nodes = vec![
            node(
                "document-1",
                None,
                &["section-1"],
                SectionGranularity::Document,
                0,
                100,
            ),
            node(
                "section-1",
                Some("document-1"),
                &["span-1"],
                SectionGranularity::Section,
                10,
                80,
            ),
            node(
                "span-1",
                Some("section-1"),
                &[],
                SectionGranularity::SourceSpan,
                20,
                40,
            ),
        ];
        document.parent_links = vec![
            HierarchyParentLink {
                parent_id: "document-1".to_string(),
                child_id: "section-1".to_string(),
            },
            HierarchyParentLink {
                parent_id: "section-1".to_string(),
                child_id: "span-1".to_string(),
            },
        ];

        document.validate().expect("nested section tree is valid");
        let encoded = serde_json::to_string(&document).expect("policy serializes");
        let decoded: HierarchyRetrievalPolicy =
            serde_json::from_str(&encoded).expect("policy deserializes");
        assert_eq!(decoded, document);
    }

    #[test]
    fn community_policy_preserves_community_fields_without_section_nodes() {
        let community = policy(HierarchyRetrievalKind::Community);
        community.validate().expect("community policy is valid");
        assert_eq!(community.community_ids, vec!["community-1".to_string()]);
        assert_eq!(community.summary_ids, vec!["summary-1".to_string()]);
        assert_eq!(community.entity_ids, vec!["entity-1".to_string()]);
    }

    #[test]
    fn malformed_tree_refuses_multiple_roots_and_outside_spans() {
        let mut multiple_roots = policy(HierarchyRetrievalKind::DocumentSection);
        multiple_roots.section_nodes = vec![
            node("document-1", None, &[], SectionGranularity::Document, 0, 10),
            node(
                "document-2",
                None,
                &[],
                SectionGranularity::Document,
                20,
                30,
            ),
        ];
        assert_eq!(
            multiple_roots.validate(),
            Err(HierarchyValidationError::InvalidRootCount)
        );

        let mut outside_span = policy(HierarchyRetrievalKind::DocumentSection);
        outside_span.section_nodes = vec![
            node(
                "document-1",
                None,
                &["section-1"],
                SectionGranularity::Document,
                0,
                10,
            ),
            node(
                "section-1",
                Some("document-1"),
                &[],
                SectionGranularity::Section,
                5,
                20,
            ),
        ];
        outside_span.parent_links = vec![HierarchyParentLink {
            parent_id: "document-1".to_string(),
            child_id: "section-1".to_string(),
        }];
        assert!(matches!(
            outside_span.validate(),
            Err(HierarchyValidationError::SpanNotNested { .. })
        ));

        let mut cyclic = policy(HierarchyRetrievalKind::DocumentSection);
        cyclic.section_nodes = vec![
            node(
                "document-1",
                None,
                &[],
                SectionGranularity::Document,
                0,
                100,
            ),
            node(
                "cycle-1",
                Some("cycle-2"),
                &["cycle-2"],
                SectionGranularity::Section,
                20,
                50,
            ),
            node(
                "cycle-2",
                Some("cycle-1"),
                &["cycle-1"],
                SectionGranularity::Section,
                20,
                50,
            ),
        ];
        cyclic.parent_links = vec![
            HierarchyParentLink {
                parent_id: "cycle-2".to_string(),
                child_id: "cycle-1".to_string(),
            },
            HierarchyParentLink {
                parent_id: "cycle-1".to_string(),
                child_id: "cycle-2".to_string(),
            },
        ];
        assert!(matches!(
            cyclic.validate(),
            Err(HierarchyValidationError::Cycle(_))
        ));
    }

    #[test]
    fn receipt_rejects_unvisited_selection_and_kind_stage_mismatch() {
        let receipt = HierarchyStageReceipt {
            hierarchy_revision: "hierarchy-r1".to_string(),
            kind: HierarchyRetrievalKind::DocumentSection,
            stage: HierarchyStage::Community,
            visited_ids: vec!["section-1".to_string()],
            selected_id: Some("section-2".to_string()),
            pruned_ids: Vec::new(),
            score_provenance: Vec::new(),
            beam_width: 1,
            depth: 1,
        };
        assert_eq!(
            receipt.validate(),
            Err(HierarchyValidationError::SelectedNodeNotVisited)
        );

        let mut matching = receipt;
        matching.selected_id = None;
        assert_eq!(
            matching.validate(),
            Err(HierarchyValidationError::ReceiptKindStageMismatch)
        );
    }
}
