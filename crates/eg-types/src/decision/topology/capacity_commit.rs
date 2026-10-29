//! MEDULLA-DECISION-ENGINE-R113.1: the typed commit outcome that ties a
//! topology-backed decision's capacity acquisition to its synthesis
//! evidence as one all-or-nothing operation.
//!
//! Split out of R113 per the rapid-delivery contract's sizing rule (the
//! full row also names the native capacity engine and the commit
//! transaction that actually calls it -- further code roots); this slice
//! is the typed model and its refusal test: [`CapacitySynthesisCommit`]
//! can only be constructed as `Committed` with synthesis evidence
//! published and at least one lease, or as `Denied` holding no lease and
//! no published evidence, with the denial carrying exactly one further
//! decision's retry allowance.

use crate::native_control::CapacityDecision;

#[path = "retry_allowance.rs"]
mod retry_allowance;
pub use retry_allowance::{RetryAllowance, RetryAllowanceError};

/// The outcome of committing a topology-backed decision's synthesis
/// evidence together with its capacity acquisition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapacitySynthesisCommit {
    /// Capacity was acquired and the synthesis evidence was published in
    /// the same all-or-nothing step.
    Committed {
        lease_ids: Vec<String>,
        synthesis_evidence_published: bool,
    },
    /// Capacity was denied: no lease is held and no synthesis evidence was
    /// published. The caller may request exactly one further decision.
    Denied {
        decision: CapacityDecision,
        retry: RetryAllowance,
    },
}

impl CapacitySynthesisCommit {
    /// Builds the commit outcome from a capacity decision and the
    /// candidate lease/evidence payload, refusing to ever construct a
    /// `Committed` outcome without synthesis evidence or a lease, and
    /// refusing a `Denied` outcome that holds a lease or published
    /// evidence (a partial hold, which breaks atomicity).
    pub fn from_capacity_decision(
        decision: CapacityDecision,
        lease_ids: Vec<String>,
        synthesis_evidence_published: bool,
    ) -> Result<Self, String> {
        match decision {
            CapacityDecision::Accepted | CapacityDecision::Replayed => {
                if !synthesis_evidence_published {
                    return Err(
                        "capacity was accepted but no synthesis evidence was published in the same commit"
                            .to_string(),
                    );
                }
                if lease_ids.is_empty() {
                    return Err("capacity was accepted but no lease was recorded".to_string());
                }
                Ok(Self::Committed {
                    lease_ids,
                    synthesis_evidence_published,
                })
            }
            other => {
                if !lease_ids.is_empty() {
                    return Err(format!(
                        "capacity was denied ({other:?}) but {} lease(s) are held -- commit is not atomic",
                        lease_ids.len()
                    ));
                }
                if synthesis_evidence_published {
                    return Err(format!(
                        "capacity was denied ({other:?}) but synthesis evidence was published -- commit is not atomic"
                    ));
                }
                Ok(Self::Denied {
                    decision: other,
                    retry: RetryAllowance {
                        further_decisions: 1,
                    },
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-DECISION-ENGINE-R113.1
    #[test]
    fn an_accepted_decision_with_a_lease_and_evidence_commits() {
        let commit = CapacitySynthesisCommit::from_capacity_decision(
            CapacityDecision::Accepted,
            vec!["lease-1".to_string()],
            true,
        )
        .expect("commits");
        assert_eq!(
            commit,
            CapacitySynthesisCommit::Committed {
                lease_ids: vec!["lease-1".to_string()],
                synthesis_evidence_published: true,
            }
        );
    }

    // spec: MEDULLA-DECISION-ENGINE-R113.1
    #[test]
    fn a_denied_decision_with_no_lease_and_no_evidence_grants_exactly_one_retry() {
        let commit = CapacitySynthesisCommit::from_capacity_decision(
            CapacityDecision::Exhausted,
            Vec::new(),
            false,
        )
        .expect("denies cleanly");
        let CapacitySynthesisCommit::Denied { retry, .. } = commit else {
            panic!("denied expected")
        };
        assert_eq!(retry.further_decisions, 1);
    }

    // spec: MEDULLA-DECISION-ENGINE-R113.1
    #[test]
    fn a_denial_holding_a_partial_lease_is_refused() {
        let err = CapacitySynthesisCommit::from_capacity_decision(
            CapacityDecision::Backpressure,
            vec!["lease-1".to_string()],
            false,
        )
        .expect_err("a denial must hold no lease");
        assert!(err.contains("not atomic"));
    }

    // spec: MEDULLA-DECISION-ENGINE-R113.1
    #[test]
    fn an_acceptance_without_published_synthesis_evidence_is_refused() {
        let err = CapacitySynthesisCommit::from_capacity_decision(
            CapacityDecision::Accepted,
            vec!["lease-1".to_string()],
            false,
        )
        .expect_err("evidence must publish atomically with the lease");
        assert!(err.contains("synthesis evidence"));
    }

    // spec: MEDULLA-DECISION-ENGINE-R113.1
    #[test]
    fn accepted_and_replayed_decisions_require_both_commit_parts() {
        for decision in [CapacityDecision::Accepted, CapacityDecision::Replayed] {
            for (leases, evidence) in [
                (Vec::new(), false),
                (Vec::new(), true),
                (vec!["lease-1".to_string()], false),
            ] {
                assert!(CapacitySynthesisCommit::from_capacity_decision(
                    decision, leases, evidence,
                )
                .is_err());
            }
            let leases = vec!["lease-1".to_string(), "lease-2".to_string()];
            assert_eq!(
                CapacitySynthesisCommit::from_capacity_decision(decision, leases.clone(), true),
                Ok(CapacitySynthesisCommit::Committed {
                    lease_ids: leases,
                    synthesis_evidence_published: true,
                })
            );
        }
    }

    // spec: MEDULLA-DECISION-ENGINE-R113.1
    #[test]
    fn every_non_acquisition_decision_refuses_partial_commit_and_preserves_reason() {
        for decision in [
            CapacityDecision::Released,
            CapacityDecision::Renewed,
            CapacityDecision::Reclaimed,
            CapacityDecision::Exhausted,
            CapacityDecision::StaleEpoch,
            CapacityDecision::StaleFence,
            CapacityDecision::Expired,
            CapacityDecision::NotFound,
            CapacityDecision::IdempotencyConflict,
            CapacityDecision::Invalid,
            CapacityDecision::Backpressure,
        ] {
            assert_eq!(
                CapacitySynthesisCommit::from_capacity_decision(decision, Vec::new(), false),
                Ok(CapacitySynthesisCommit::Denied {
                    decision,
                    retry: RetryAllowance {
                        further_decisions: 1,
                    },
                })
            );
            for (leases, evidence) in [
                (Vec::new(), true),
                (vec!["lease-1".to_string()], false),
                (vec!["lease-1".to_string()], true),
            ] {
                let error =
                    CapacitySynthesisCommit::from_capacity_decision(decision, leases, evidence)
                        .expect_err("denied acquisition must retain neither commit part");
                assert!(error.contains("not atomic"));
            }
        }
    }
}
