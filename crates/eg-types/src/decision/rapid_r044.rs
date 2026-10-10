//! EG-DECISION-ENGINE-R044 (`.1` slice): a typed A2A routing resolution.
//!
//! An inbound A2A task must be routed to an existing or assembled agent
//! graph with a recorded resolution kind and evidence class, not an
//! unexplained match. This module gives that resolution a validated typed
//! shape. Wiring the A2A task-submission entry point to construct one of
//! these per inbound task is a later `.2` slice.

use super::record::{EvidenceClass, ResolutionKind};

/// The routing outcome for one inbound A2A task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2ARoutingResolution {
    pub graph_id: String,
    pub resolution_kind: ResolutionKind,
    pub evidence_class: EvidenceClass,
}

/// Why a routing resolution was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum A2ARoutingRefusal {
    /// A resolution must name the graph it routed to.
    EmptyGraphId,
    /// An abstention cannot claim proof- or observation-grade evidence: it
    /// resolved nothing, so its evidence class can only be the weakest.
    AbstentionWithStrongEvidence { evidence_class: EvidenceClass },
}

impl A2ARoutingResolution {
    /// Build a routing resolution, refusing an internally inconsistent one.
    pub fn new(
        graph_id: impl Into<String>,
        resolution_kind: ResolutionKind,
        evidence_class: EvidenceClass,
    ) -> Result<Self, A2ARoutingRefusal> {
        let graph_id = graph_id.into();
        if graph_id.is_empty() {
            return Err(A2ARoutingRefusal::EmptyGraphId);
        }
        if resolution_kind == ResolutionKind::Abstention && evidence_class != EvidenceClass::Claim {
            return Err(A2ARoutingRefusal::AbstentionWithStrongEvidence { evidence_class });
        }
        Ok(Self {
            graph_id,
            resolution_kind,
            evidence_class,
        })
    }

    /// Route one inbound task's requested graph id against the known-graph
    /// registry: an exact match resolves as a constraint with observation
    /// evidence, and no match abstains with claim-grade evidence -- the
    /// registry lookup the A2A task-submission entry point calls per task.
    pub fn route(
        known_graph_ids: &[String],
        requested_graph_id: &str,
    ) -> Result<Self, A2ARoutingRefusal> {
        if known_graph_ids.iter().any(|id| id == requested_graph_id) {
            Self::new(
                requested_graph_id,
                ResolutionKind::Constraint,
                EvidenceClass::Observation,
            )
        } else {
            Self::new(
                requested_graph_id,
                ResolutionKind::Abstention,
                EvidenceClass::Claim,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_consistent_resolution() {
        let resolution = A2ARoutingResolution::new(
            "graph-1",
            ResolutionKind::Constraint,
            EvidenceClass::Observation,
        )
        .unwrap();
        assert_eq!(resolution.graph_id, "graph-1");
    }

    #[test]
    fn refuses_an_empty_graph_id() {
        let refusal =
            A2ARoutingResolution::new("", ResolutionKind::Constraint, EvidenceClass::Observation)
                .unwrap_err();
        assert_eq!(refusal, A2ARoutingRefusal::EmptyGraphId);
    }

    // spec: EG-DECISION-ENGINE-R044.1
    #[test]
    fn refuses_an_abstention_claiming_proof_grade_evidence() {
        let refusal =
            A2ARoutingResolution::new("graph-1", ResolutionKind::Abstention, EvidenceClass::Proof)
                .unwrap_err();
        assert_eq!(
            refusal,
            A2ARoutingRefusal::AbstentionWithStrongEvidence {
                evidence_class: EvidenceClass::Proof
            }
        );
    }

    // spec: EG-DECISION-ENGINE-R044.2.1
    #[test]
    fn routes_to_a_known_graph_and_abstains_otherwise() {
        let known = vec!["graph-1".to_string(), "graph-2".to_string()];

        let routed = A2ARoutingResolution::route(&known, "graph-2").unwrap();
        assert_eq!(routed.graph_id, "graph-2");
        assert_eq!(routed.resolution_kind, ResolutionKind::Constraint);
        assert_eq!(routed.evidence_class, EvidenceClass::Observation);

        let abstained = A2ARoutingResolution::route(&known, "graph-9").unwrap();
        assert_eq!(abstained.resolution_kind, ResolutionKind::Abstention);
        assert_eq!(abstained.evidence_class, EvidenceClass::Claim);
    }
}
