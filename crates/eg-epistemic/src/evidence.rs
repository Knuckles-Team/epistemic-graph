//! Governed multimodal evidence citation resolution.
//!
//! Evidence-bearing graph nodes store one complete `evidence_locus` property. The
//! resolver never synthesizes identity, source, policy, or derivation fields from
//! raw node metadata; malformed or incomplete loci are absent from the citation
//! graph.

use std::collections::{HashSet, VecDeque};

use eg_modality::EvidenceLocus;
use serde::{Deserialize, Serialize};

use crate::adapter::BeliefGraph;
use crate::model::{EdgeKind, JustificationGraph};

/// Decode the sole stored evidence representation from an already-decoded node.
pub(crate) fn decode_locus(parsed: Option<&serde_json::Value>) -> Option<EvidenceLocus> {
    let value = parsed?.get("evidence_locus")?;
    serde_json::from_value::<EvidenceLocus>(value.clone())
        .ok()
        .filter(|locus| locus.validate().is_ok())
}

/// One evidence-bearing node reached while explaining a claim.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvidenceCitation {
    pub evidence_id: String,
    pub kind: EdgeKind,
    pub locus: EvidenceLocus,
}

/// Resolve one evidence node's governed locus directly.
pub fn resolve_locus(bg: &BeliefGraph, evidence_id: &str) -> Option<EvidenceLocus> {
    bg.evidence_loci.get(evidence_id).cloned()
}

/// Resolve every governed locus that transitively bears on `claim_id`.
pub fn evidence_citations(bg: &BeliefGraph, claim_id: &str) -> Vec<EvidenceCitation> {
    let mut citations = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    visited.insert(claim_id.to_string());
    queue.push_back((claim_id.to_string(), 0));

    while let Some((id, depth)) = queue.pop_front() {
        if depth >= crate::propagate::MAX_EXPLAIN_DEPTH {
            continue;
        }
        let Some(incoming) = bg.in_edges.get(&id) else {
            continue;
        };
        for (source, kind) in incoming {
            if !visited.insert(source.clone()) {
                continue;
            }
            if let Some(locus) = bg.evidence_loci.get(source) {
                citations.push(EvidenceCitation {
                    evidence_id: source.clone(),
                    kind: *kind,
                    locus: locus.clone(),
                });
            }
            queue.push_back((source.clone(), depth + 1));
        }
    }

    citations
}

/// Resolve citations for the root of an existing justification graph.
pub fn justification_citations(
    bg: &BeliefGraph,
    graph: &JustificationGraph,
) -> Vec<EvidenceCitation> {
    evidence_citations(bg, &graph.root.claim)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::propagate::explain_belief;
    use crate::AuthorityPolicy;
    use eg_modality::{
        DerivationId, EvidenceAddress, EvidenceLocusId, OccurrenceId, OpaqueRef, ResourceId,
    };

    fn r(namespace: &str, suffix: u8) -> OpaqueRef {
        OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
    }

    fn page_locus(page: u32, suffix: u8) -> EvidenceLocus {
        EvidenceLocus {
            id: EvidenceLocusId::from_token(&format!("00000000000000{suffix:02x}")).unwrap(),
            subject: ResourceId::Occurrence(
                OccurrenceId::from_token(&format!("00000000000001{suffix:02x}")).unwrap(),
            ),
            address: EvidenceAddress::PageRegion {
                page,
                x: 10.0,
                y: 20.0,
                width: 100.0,
                height: 40.0,
            },
            policy_ref: r("policy", suffix),
            derivation_ref: DerivationId::from_token(&format!("00000000000002{suffix:02x}"))
                .unwrap(),
        }
    }

    #[test]
    fn decode_requires_a_complete_valid_locus() {
        let locus = page_locus(4, 1);
        let value = serde_json::json!({
            "confidence": 0.8,
            "evidence_locus": serde_json::to_value(&locus).unwrap()
        });
        assert_eq!(decode_locus(Some(&value)), Some(locus));
        assert_eq!(
            decode_locus(Some(&serde_json::json!({"confidence": 0.8}))),
            None
        );
    }

    #[test]
    fn resolve_locus_is_a_direct_lookup() {
        let locus = page_locus(4, 1);
        let graph =
            BeliefGraph::from_parts([("evidence-1", 0.9)], Vec::<(&str, &str, EdgeKind)>::new())
                .with_evidence_locus("evidence-1", locus.clone());

        assert_eq!(resolve_locus(&graph, "evidence-1"), Some(locus));
        assert_eq!(resolve_locus(&graph, "absent"), None);
    }

    #[test]
    fn citations_resolve_direct_and_transitive_loci() {
        let direct = page_locus(4, 1);
        let transitive = page_locus(7, 2);
        let graph = BeliefGraph::from_parts(
            [("claim-1", 0.5), ("evidence-1", 0.9), ("evidence-2", 0.8)],
            [
                ("evidence-1", "claim-1", EdgeKind::Supports),
                ("evidence-2", "evidence-1", EdgeKind::Supports),
            ],
        )
        .with_evidence_locus("evidence-1", direct.clone())
        .with_evidence_locus("evidence-2", transitive.clone());

        let citations = evidence_citations(&graph, "claim-1");
        assert_eq!(citations.len(), 2);
        assert_eq!(citations[0].locus, direct);
        assert_eq!(citations[1].locus, transitive);

        let justification = explain_belief(&graph, "claim-1", &AuthorityPolicy::default());
        assert_eq!(justification_citations(&graph, &justification), citations);
    }

    #[test]
    fn citations_do_not_fabricate_missing_loci() {
        let graph = BeliefGraph::from_parts(
            [("claim-1", 0.5), ("evidence-1", 0.9)],
            [("evidence-1", "claim-1", EdgeKind::Supports)],
        );
        assert!(evidence_citations(&graph, "claim-1").is_empty());
    }
}
