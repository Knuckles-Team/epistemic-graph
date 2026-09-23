//! Typed claim/evidence index over a graph snapshot (EH-194).
//!
//! [`BeliefGraph`] projects the confidence/support topology and never looks at what a
//! node *is*. This index is the typed half: every `:Claim`/`:Evidence` node in the
//! snapshot decoded as an [`eg_types::epistemic_node::Claim`] or
//! [`eg_types::epistemic_node::Evidence`], and every node that is labelled as one but
//! does not have the shape reported with its typed error rather than dropped.

use std::collections::BTreeMap;

use eg_core::graph::GraphView;
use eg_types::epistemic_node::{Claim, EpistemicNode, EpistemicNodeError, Evidence};

use crate::adapter::BeliefGraph;
use crate::model::EdgeKind;

/// Every typed claim and evidence node of one snapshot, keyed by node id.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EpistemicNodeIndex {
    pub claims: BTreeMap<String, Claim>,
    pub evidence: BTreeMap<String, Evidence>,
    /// Nodes labelled `Claim`/`Evidence` whose properties do not decode or validate.
    pub malformed: BTreeMap<String, EpistemicNodeError>,
}

/// One typed evidence node bearing directly on a claim, with how it bears.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedEvidence<'a> {
    pub evidence_id: &'a str,
    pub kind: EdgeKind,
    pub evidence: &'a Evidence,
}

impl EpistemicNodeIndex {
    /// Decode every node of `view`. Non-epistemic nodes are skipped; an undecodable
    /// property blob on an unlabelled node is not an epistemic node either.
    pub fn from_graph_view(view: &GraphView) -> Self {
        let mut index = Self::default();
        for (id, blob) in &view.node_properties {
            let Ok(properties) = eg_types::msgpack::decode_property_value(blob) else {
                continue;
            };
            index.insert(id, &properties);
        }
        index
    }

    /// Decode one node's property object into the index.
    pub fn insert(&mut self, id: &str, properties: &serde_json::Value) {
        match EpistemicNode::from_properties(properties) {
            Ok(Some(EpistemicNode::Claim(claim))) => {
                self.claims.insert(id.to_string(), claim);
            }
            Ok(Some(EpistemicNode::Evidence(evidence))) => {
                self.evidence.insert(id.to_string(), evidence);
            }
            Ok(None) => {}
            Err(error) => {
                self.malformed.insert(id.to_string(), error);
            }
        }
    }

    /// The typed evidence nodes with a direct support/contradiction/attack edge into
    /// `claim_id`, in edge order. Incoming edges from non-evidence nodes (a mined
    /// finding, another claim) are not evidence and are left out.
    pub fn evidence_for<'a>(
        &'a self,
        graph: &'a BeliefGraph,
        claim_id: &str,
    ) -> Vec<TypedEvidence<'a>> {
        let Some(incoming) = graph.in_edges.get(claim_id) else {
            return Vec::new();
        };
        incoming
            .iter()
            .filter_map(|(source, kind)| {
                self.evidence
                    .get_key_value(source)
                    .map(|(evidence_id, evidence)| TypedEvidence {
                        evidence_id,
                        kind: *kind,
                        evidence,
                    })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn index() -> EpistemicNodeIndex {
        let mut index = EpistemicNodeIndex::default();
        index.insert(
            "claim:1",
            &json!({"type": "Claim", "family": "f", "about": "x", "confidence": 0.6,
                    "validation_state": "unvalidated", "calibration": null,
                    "invalidation_deps": []}),
        );
        index.insert(
            "ev:1",
            &json!({"type": "Evidence", "family": "f", "about": "x", "provenance": "p",
                    "confidence": 0.9, "validation_state": "unvalidated"}),
        );
        index.insert("ev:bad", &json!({"type": "Evidence", "about": "x"}));
        index.insert("other", &json!({"type": "Activity"}));
        index
    }

    #[test]
    fn labelled_nodes_decode_typed_and_malformed_ones_are_reported() {
        let index = index();
        assert_eq!(index.claims["claim:1"].about, "x");
        assert_eq!(index.evidence["ev:1"].provenance, "p");
        assert!(matches!(
            index.malformed["ev:bad"],
            EpistemicNodeError::Malformed { .. }
        ));
        assert!(!index.claims.contains_key("other"));
        assert!(!index.evidence.contains_key("other"));
    }

    #[test]
    fn evidence_for_keeps_only_typed_evidence_sources() {
        let index = index();
        let graph = BeliefGraph::from_parts(
            [("claim:1", 0.6), ("ev:1", 0.9), ("finding", 1.0)],
            [
                ("ev:1", "claim:1", EdgeKind::Supports),
                ("finding", "claim:1", EdgeKind::Supports),
            ],
        );
        let found = index.evidence_for(&graph, "claim:1");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].evidence_id, "ev:1");
        assert_eq!(found[0].kind, EdgeKind::Supports);
        assert!(index.evidence_for(&graph, "missing").is_empty());
    }
}
