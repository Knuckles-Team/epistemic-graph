//! Branch outcome and breadth-adaptation records (EG-DECISION-ENGINE-R164.1).

use serde::{Deserialize, Serialize};

use crate::contract::OpaqueId;

/// Aggregate outcomes observed for one branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchOutcome {
    pub branch_id: OpaqueId,
    pub attempt_count: u64,
    pub success_count: u64,
    pub average_cost: u64,
}

/// Prior-branch outcome evidence and whether it was applied to adaptation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeEvidence {
    pub prior_branch_id: OpaqueId,
    pub outcome: BranchOutcome,
    pub adaptation_applied: bool,
}

/// A breadth change linked to the outcome that triggered it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BreadthAdaptation {
    pub branch_id: OpaqueId,
    pub prior_breadth: u32,
    pub new_breadth: u32,
    pub outcome_triggering: OutcomeEvidence,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> OpaqueId {
        OpaqueId::new(value).unwrap()
    }

    #[test]
    fn outcome_types_round_trip() {
        let outcome = BranchOutcome {
            branch_id: id("branch:current"),
            attempt_count: 12,
            success_count: 9,
            average_cost: 420,
        };
        let evidence = OutcomeEvidence {
            prior_branch_id: id("branch:prior"),
            outcome: outcome.clone(),
            adaptation_applied: true,
        };
        let adaptation = BreadthAdaptation {
            branch_id: outcome.branch_id.clone(),
            prior_breadth: 4,
            new_breadth: 6,
            outcome_triggering: evidence.clone(),
        };

        let outcome_bytes = rmp_serde::to_vec_named(&outcome).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<BranchOutcome>(&outcome_bytes).unwrap(),
            outcome
        );
        let evidence_bytes = rmp_serde::to_vec_named(&evidence).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<OutcomeEvidence>(&evidence_bytes).unwrap(),
            evidence
        );
        let adaptation_bytes = rmp_serde::to_vec_named(&adaptation).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<BreadthAdaptation>(&adaptation_bytes).unwrap(),
            adaptation
        );
    }
}
