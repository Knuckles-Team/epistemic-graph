//! Typed, durable contract for Raft membership shrink.
//!
//! Openraft's `change_membership` primitive is deliberately small: it can remove
//! a voter, but it cannot prove that work has drained, a replacement learner is
//! caught up, leadership has moved, or that the post-change topology still
//! satisfies the operator's quorum/failure-domain/headroom/PDB policy. This
//! module owns that missing admission contract. It is a pure state machine; the
//! [`super::multi::MultiRaft`] actuator persists each journal transition through
//! the placement graph and invokes openraft only after `SafetyChecked`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{GroupId, NodeId};

pub const MEMBERSHIP_SHRINK_SCHEMA_VERSION: u16 = 1;
const MAX_VOTERS: usize = 64;
const MAX_EVIDENCE_REF: usize = 256;

/// Explicit, restart-visible shrink phases. A phase is never inferred from a
/// live membership read: the journal is the recovery authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipShrinkPhase {
    Proposed,
    DrainRequested,
    Drained,
    LearnerCaughtUp,
    LeadershipTransferred,
    SafetyChecked,
    RemovalCommitted,
    Completed,
    Aborted,
}

impl MembershipShrinkPhase {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted)
    }
}

/// Bounded observations supplied by the lifecycle/controller adapter. The
/// adapter must bind these observations to the exact term and voter set in the
/// journal; a bare `true` from a previous membership incarnation is not enough.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipShrinkEvidence {
    pub observed_term: u64,
    pub observed_voters: Vec<NodeId>,
    pub observed_learner: Option<NodeId>,
    pub observed_leader: Option<NodeId>,
    pub learner_caught_up: bool,
    pub drained: bool,
    pub leadership_transferred: bool,
    pub quorum_preserved: bool,
    pub failure_domain_preserved: bool,
    pub headroom_preserved: bool,
    pub pdb_preserved: bool,
    pub membership_change_committed: bool,
    pub target_absent: bool,
    /// Opaque bounded evidence reference; raw metrics and workload payloads do
    /// not belong in the membership journal.
    pub evidence_ref: String,
}

impl MembershipShrinkEvidence {
    fn validate(&self) -> Result<(), String> {
        validate_voters(&self.observed_voters)?;
        if self.evidence_ref.is_empty() || self.evidence_ref.len() > MAX_EVIDENCE_REF {
            return Err("membership shrink evidence reference is invalid".to_string());
        }
        if self.evidence_ref.bytes().any(|byte| byte.is_ascii_control()) {
            return Err("membership shrink evidence reference contains control bytes".to_string());
        }
        Ok(())
    }
}

/// Durable shrink intent/progress. The immutable expected and remaining voter
/// sets make restart/replay deterministic and prevent a later membership view
/// from silently changing the operation's target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipShrinkJournal {
    pub schema_version: u16,
    pub operation_id: String,
    pub group_id: GroupId,
    pub target: NodeId,
    pub learner: NodeId,
    pub expected_term: u64,
    pub expected_voters: Vec<NodeId>,
    pub remaining_voters: Vec<NodeId>,
    pub phase: MembershipShrinkPhase,
    pub evidence: Option<MembershipShrinkEvidence>,
    pub abort_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShrinkRecoveryAction {
    Resume,
    Complete,
    Abort,
}

fn validate_voters(voters: &[NodeId]) -> Result<(), String> {
    if voters.is_empty() || voters.len() > MAX_VOTERS || voters.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err("membership voter set must be sorted, unique, and bounded".to_string());
    }
    Ok(())
}

fn operation_id(
    group_id: GroupId,
    target: NodeId,
    learner: NodeId,
    expected_term: u64,
    expected_voters: &[NodeId],
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/membership-shrink/v1\0");
    digest.update(group_id.to_be_bytes());
    digest.update(target.to_be_bytes());
    digest.update(learner.to_be_bytes());
    digest.update(expected_term.to_be_bytes());
    for voter in expected_voters {
        digest.update(voter.to_be_bytes());
    }
    hex::encode(digest.finalize())
}

impl MembershipShrinkJournal {
    pub fn new(
        group_id: GroupId,
        target: NodeId,
        learner: NodeId,
        expected_term: u64,
        mut expected_voters: Vec<NodeId>,
    ) -> Result<Self, String> {
        expected_voters.sort_unstable();
        expected_voters.dedup();
        validate_voters(&expected_voters)?;
        if !expected_voters.contains(&target) {
            return Err("membership shrink target is not a voter".to_string());
        }
        if learner == target {
            return Err("membership shrink learner must differ from target".to_string());
        }
        let remaining_voters: Vec<NodeId> = expected_voters
            .iter()
            .copied()
            .filter(|voter| *voter != target)
            .collect();
        validate_voters(&remaining_voters)?;
        Ok(Self {
            schema_version: MEMBERSHIP_SHRINK_SCHEMA_VERSION,
            operation_id: operation_id(
                group_id,
                target,
                learner,
                expected_term,
                &expected_voters,
            ),
            group_id,
            target,
            learner,
            expected_term,
            expected_voters,
            remaining_voters,
            phase: MembershipShrinkPhase::Proposed,
            evidence: None,
            abort_reason: None,
        })
    }

    pub fn node_id(&self) -> String {
        format!("membership-shrink:{}", self.operation_id)
    }

    pub fn validate(&self) -> bool {
        let expected_remaining: Vec<NodeId> = self
            .expected_voters
            .iter()
            .copied()
            .filter(|voter| *voter != self.target)
            .collect();
        let abort_reason_valid = self.abort_reason.as_ref().map_or(true, |reason| {
            !reason.is_empty()
                && reason.len() <= MAX_EVIDENCE_REF
                && !reason.bytes().any(|byte| byte.is_ascii_control())
        });
        if self.schema_version != MEMBERSHIP_SHRINK_SCHEMA_VERSION
            || self.operation_id
                != operation_id(
                    self.group_id,
                    self.target,
                    self.learner,
                    self.expected_term,
                    &self.expected_voters,
                )
            || validate_voters(&self.expected_voters).is_err()
            || validate_voters(&self.remaining_voters).is_err()
            || self.remaining_voters != expected_remaining
            || !self.expected_voters.contains(&self.target)
            || self.learner == self.target
            || !abort_reason_valid
            || (self.phase != MembershipShrinkPhase::Aborted && self.abort_reason.is_some())
            || self
                .remaining_voters
                .iter()
                .any(|voter| *voter == self.target || !self.expected_voters.contains(voter))
        {
            return false;
        }
        self.evidence
            .as_ref()
            .map_or(true, |evidence| evidence.validate().is_ok())
    }

    fn evidence_matches(
        &self,
        next: MembershipShrinkPhase,
        evidence: &MembershipShrinkEvidence,
    ) -> bool {
        let expected_voters = match next {
            MembershipShrinkPhase::RemovalCommitted | MembershipShrinkPhase::Completed
                if evidence.membership_change_committed => &self.remaining_voters,
            _ => &self.expected_voters,
        };
        evidence.validate().is_ok()
            && evidence.observed_term == self.expected_term
            && evidence.observed_learner == Some(self.learner)
            && evidence.observed_voters.as_slice() == expected_voters.as_slice()
    }

    /// Validate and persist the next phase. There are no skips: each safety
    /// gate is independently evidenced and a stale term/voter set is rejected.
    pub fn advance(
        &self,
        next: MembershipShrinkPhase,
        evidence: MembershipShrinkEvidence,
    ) -> Result<Self, String> {
        if !self.validate() || self.phase.terminal() {
            return Err("membership shrink journal is not advanceable".to_string());
        }
        if !self.evidence_matches(next, &evidence) {
            return Err("membership shrink evidence is stale or targets another set".to_string());
        }
        let valid = match (self.phase, next) {
            (MembershipShrinkPhase::Proposed, MembershipShrinkPhase::DrainRequested) => true,
            (MembershipShrinkPhase::DrainRequested, MembershipShrinkPhase::Drained) => {
                evidence.drained
            }
            (MembershipShrinkPhase::Drained, MembershipShrinkPhase::LearnerCaughtUp) => {
                evidence.drained && evidence.learner_caught_up
            }
            (
                MembershipShrinkPhase::LearnerCaughtUp,
                MembershipShrinkPhase::LeadershipTransferred,
            ) => {
                evidence.learner_caught_up
                    && evidence.leadership_transferred
                    && matches!(evidence.observed_leader, Some(leader) if leader != self.target)
            }
            (
                MembershipShrinkPhase::LeadershipTransferred,
                MembershipShrinkPhase::SafetyChecked,
            ) => {
                evidence.leadership_transferred
                    && evidence.quorum_preserved
                    && evidence.failure_domain_preserved
                    && evidence.headroom_preserved
                    && evidence.pdb_preserved
            }
            (
                MembershipShrinkPhase::SafetyChecked,
                MembershipShrinkPhase::RemovalCommitted,
            ) => {
                evidence.membership_change_committed
                    && evidence.target_absent
                    && evidence.observed_voters == self.remaining_voters
            }
            (MembershipShrinkPhase::RemovalCommitted, MembershipShrinkPhase::Completed) => {
                evidence.membership_change_committed
                    && evidence.target_absent
                    && evidence.observed_voters == self.remaining_voters
            }
            _ => false,
        };
        if !valid {
            return Err(format!(
                "membership shrink phase {:?} cannot advance to {:?} with supplied evidence",
                self.phase, next
            ));
        }
        let mut updated = self.clone();
        updated.phase = next;
        updated.evidence = Some(evidence);
        Ok(updated)
    }

    /// Abort is itself durable recovery evidence and never deletes the journal.
    pub fn abort(&self, reason: &str) -> Result<Self, String> {
        if !self.validate() || self.phase.terminal() {
            return Err("membership shrink journal is not abortable".to_string());
        }
        if reason.is_empty()
            || reason.len() > MAX_EVIDENCE_REF
            || reason.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err("membership shrink abort reason is invalid".to_string());
        }
        let mut updated = self.clone();
        updated.phase = MembershipShrinkPhase::Aborted;
        updated.abort_reason = Some(reason.to_string());
        Ok(updated)
    }

    /// Decide what a restarted controller may do from the retained journal and
    /// the currently observed committed voter set. Ambiguous state aborts.
    pub fn recovery_action(&self, observed_voters: &[NodeId]) -> ShrinkRecoveryAction {
        if !self.validate() || self.phase == MembershipShrinkPhase::Aborted {
            return ShrinkRecoveryAction::Abort;
        }
        let mut observed = observed_voters.to_vec();
        observed.sort_unstable();
        observed.dedup();
        match self.phase {
            MembershipShrinkPhase::SafetyChecked
            | MembershipShrinkPhase::RemovalCommitted
            | MembershipShrinkPhase::Completed
                if observed == self.remaining_voters => ShrinkRecoveryAction::Complete,
            phase if !phase.terminal() && observed == self.expected_voters => {
                ShrinkRecoveryAction::Resume
            }
            _ => ShrinkRecoveryAction::Abort,
        }
    }

    /// The removal actuator may run only after every drain/safety gate is
    /// represented by one durable `SafetyChecked` journal state.
    pub fn ready_for_removal(&self) -> bool {
        self.validate() && self.phase == MembershipShrinkPhase::SafetyChecked
    }

    pub fn permits_successor(&self, next: &Self) -> bool {
        if !self.validate()
            || !next.validate()
            || self.operation_id != next.operation_id
            || self.group_id != next.group_id
            || self.target != next.target
            || self.learner != next.learner
            || self.expected_term != next.expected_term
            || self.expected_voters != next.expected_voters
            || self.remaining_voters != next.remaining_voters
        {
            return false;
        }
        if self.phase == next.phase {
            return self.evidence == next.evidence;
        }
        if self.phase.terminal() {
            return false;
        }
        matches!(
            (self.phase, next.phase),
            (MembershipShrinkPhase::Proposed, MembershipShrinkPhase::DrainRequested)
                | (MembershipShrinkPhase::DrainRequested, MembershipShrinkPhase::Drained)
                | (MembershipShrinkPhase::Drained, MembershipShrinkPhase::LearnerCaughtUp)
                | (
                    MembershipShrinkPhase::LearnerCaughtUp,
                    MembershipShrinkPhase::LeadershipTransferred
                )
                | (
                    MembershipShrinkPhase::LeadershipTransferred,
                    MembershipShrinkPhase::SafetyChecked
                )
                | (
                    MembershipShrinkPhase::SafetyChecked,
                    MembershipShrinkPhase::RemovalCommitted
                )
                | (
                    MembershipShrinkPhase::RemovalCommitted,
                    MembershipShrinkPhase::Completed
                )
                | (_, MembershipShrinkPhase::Aborted)
        )
    }

    pub fn expected_voter_set(&self) -> BTreeSet<NodeId> {
        self.expected_voters.iter().copied().collect()
    }

    pub fn remaining_voter_set(&self) -> BTreeSet<NodeId> {
        self.remaining_voters.iter().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(voters: Vec<NodeId>) -> MembershipShrinkEvidence {
        MembershipShrinkEvidence {
            observed_term: 7,
            observed_voters: voters,
            observed_learner: Some(4),
            observed_leader: Some(1),
            learner_caught_up: false,
            drained: false,
            leadership_transferred: false,
            quorum_preserved: false,
            failure_domain_preserved: false,
            headroom_preserved: false,
            pdb_preserved: false,
            membership_change_committed: false,
            target_absent: false,
            evidence_ref: "evidence:shrink-test".into(),
        }
    }

    #[test]
    fn shrink_requires_each_gate_in_order_and_retains_recovery_state() {
        let journal = MembershipShrinkJournal::new(0, 3, 4, 7, vec![1, 2, 3]).unwrap();
        let mut current = journal
            .advance(
                MembershipShrinkPhase::DrainRequested,
                evidence(vec![1, 2, 3]),
            )
            .unwrap();
        let mut next = evidence(vec![1, 2, 3]);
        next.drained = true;
        current = current
            .advance(MembershipShrinkPhase::Drained, next)
            .unwrap();
        let mut next = evidence(vec![1, 2, 3]);
        next.drained = true;
        next.learner_caught_up = true;
        current = current
            .advance(MembershipShrinkPhase::LearnerCaughtUp, next)
            .unwrap();
        let mut next = evidence(vec![1, 2, 3]);
        next.drained = true;
        next.learner_caught_up = true;
        next.leadership_transferred = true;
        current = current
            .advance(MembershipShrinkPhase::LeadershipTransferred, next)
            .unwrap();
        let mut next = evidence(vec![1, 2, 3]);
        next.drained = true;
        next.learner_caught_up = true;
        next.leadership_transferred = true;
        next.quorum_preserved = true;
        next.failure_domain_preserved = true;
        next.headroom_preserved = true;
        next.pdb_preserved = true;
        current = current
            .advance(MembershipShrinkPhase::SafetyChecked, next)
            .unwrap();
        assert!(current.ready_for_removal());
        assert_eq!(current.node_id(), journal.node_id());
        assert_eq!(
            current.recovery_action(&[1, 2, 3]),
            ShrinkRecoveryAction::Resume
        );
    }

    #[test]
    fn stale_term_and_ambiguous_restart_abort() {
        let journal = MembershipShrinkJournal::new(0, 3, 4, 7, vec![1, 2, 3]).unwrap();
        let mut stale = evidence(vec![1, 2, 3]);
        stale.observed_term = 8;
        assert!(journal
            .advance(MembershipShrinkPhase::DrainRequested, stale)
            .is_err());
        assert_eq!(
            journal.recovery_action(&[1, 3]),
            ShrinkRecoveryAction::Abort
        );
    }
}
