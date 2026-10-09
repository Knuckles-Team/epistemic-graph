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
}
