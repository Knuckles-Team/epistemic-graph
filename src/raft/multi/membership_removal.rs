//! Evidence-gated voter removal through the durable membership-shrink journal.
//!
//! Split out of `multi.rs` (CCCC burn-down lane L-raft-b).
//! `remove_group_member_with_evidence` read live metrics, checked eleven
//! preconditions, and drove every journal phase in one body; here each gate and
//! phase group is named. `multi.rs` was already over the KISS whole-file
//! thresholds, so the steps live beside the method and the parent's own counts
//! only go down.

use std::collections::BTreeSet;

use openraft::async_runtime::watch::WatchReceiver;

use super::super::membership_shrink::{
    MembershipShrinkEvidence, MembershipShrinkJournal, MembershipShrinkPhase,
};
use super::{EgRaft, GroupId, MultiRaft, NodeId};

impl MultiRaft {
    /// Remove one voter only through the durable drain/safety state machine.
    /// Every phase is retained in the placement graph before the next side
    /// effect. A crash after `change_membership` therefore leaves enough state
    /// for restart reconciliation to complete or abort deterministically.
    pub async fn remove_group_member_with_evidence(
        &self,
        gid: GroupId,
        node: NodeId,
        evidence: MembershipShrinkEvidence,
    ) -> Result<(), String> {
        let raft = self
            .groups
            .read()
            .await
            .get(&gid)
            .cloned()
            .ok_or_else(|| format!("group {gid} not running on node {}", self.node_id))?;
        let mut live = LiveMembership::observe(&raft, evidence.observed_learner);
        if !live.voters.remove(&node) {
            return Ok(());
        }
        let voters = live.voters.clone();
        check_retained_voters(gid, node, &voters)?;
        let learner = check_learner_and_leadership(self.node_id, node, &live, &evidence)?;
        let expected_voters = voters_with(&voters, node);
        check_evidence_matches_live(&evidence, &live, &expected_voters)?;
        let journal =
            MembershipShrinkJournal::new(gid, node, learner, live.current_term, expected_voters)?;
        if retained_shrink_is_complete(self, &journal).await? {
            return Ok(());
        }
        let journal = persist_safety_phases(self, journal, &evidence).await?;

        raft.change_membership(voters.clone(), false)
            .await
            .map_err(|e| format!("change_membership group {gid} remove {node}: {e}"))?;

        let committed = committed_removal_evidence(&raft, &voters, node, evidence)?;
        complete_shrink_journal(self, journal, committed).await
    }
}

/// The group's live membership as its leader's metrics report it.
struct LiveMembership {
    voters: BTreeSet<NodeId>,
    learners: BTreeSet<NodeId>,
    current_term: u64,
    current_leader: Option<NodeId>,
    /// The observed learner's replicated index has reached the leader's last log index.
    learner_caught_up: bool,
}

impl LiveMembership {
    fn observe(raft: &EgRaft, observed_learner: Option<NodeId>) -> Self {
        let metrics = raft.metrics();
        let watched = metrics.borrow_watched();
        let learner_index = observed_learner.and_then(|learner| {
            watched
                .replication
                .as_ref()?
                .get(&learner)?
                .as_ref()
                .map(|log_id| log_id.index)
        });
        let voters: BTreeSet<NodeId> = watched.membership_config.voter_ids().collect();
        let learners: BTreeSet<NodeId> = watched
            .membership_config
            .membership()
            .learner_ids()
            .collect();
        Self {
            voters,
            learners,
            current_term: watched.current_term,
            current_leader: watched.current_leader,
            learner_caught_up: learner_index
                .zip(watched.last_log_index)
                .is_some_and(|(learner_index, leader_index)| learner_index >= leader_index),
        }
    }
}

/// The voters retained after removal must keep a two-voter quorum.
fn check_retained_voters(
    gid: GroupId,
    node: NodeId,
    retained: &BTreeSet<NodeId>,
) -> Result<(), String> {
    if retained.is_empty() {
        return Err(format!(
            "refusing to remove the last voter {node} from group {gid}"
        ));
    }
    if retained.len() < 2 {
        return Err(format!(
            "refusing to shrink group {gid} below two retained voters"
        ));
    }
    Ok(())
}

/// A committed, caught-up learner exists, and this node leads the group while
/// the removal target does not. Returns the learner.
fn check_learner_and_leadership(
    local_node: NodeId,
    node: NodeId,
    live: &LiveMembership,
    evidence: &MembershipShrinkEvidence,
) -> Result<NodeId, String> {
    let learner = evidence
        .observed_learner
        .ok_or_else(|| "membership shrink requires a caught-up learner".to_string())?;
    if !live.learners.contains(&learner) {
        return Err("membership shrink learner is not in the committed learner set".to_string());
    }
    if live.current_leader != Some(local_node) {
        return Err("membership shrink must be proposed by the current group leader".to_string());
    }
    if live.current_leader == Some(node) {
        return Err("membership shrink requires leadership transfer before removal".to_string());
    }
    if !live.learner_caught_up {
        return Err("membership shrink learner has not caught up to the leader log".to_string());
    }
    Ok(learner)
}

/// The retained voters plus the removal target, sorted.
fn voters_with(retained: &BTreeSet<NodeId>, node: NodeId) -> Vec<NodeId> {
    let mut voters: Vec<NodeId> = retained.iter().copied().collect();
    voters.push(node);
    voters.sort_unstable();
    voters
}

fn check_evidence_matches_live(
    evidence: &MembershipShrinkEvidence,
    live: &LiveMembership,
    expected_voters: &[NodeId],
) -> Result<(), String> {
    if evidence.observed_term != live.current_term
        || evidence.observed_voters != expected_voters
        || evidence.observed_leader != live.current_leader
    {
        return Err("membership shrink evidence does not match live term or voter set".to_string());
    }
    Ok(())
}

/// `true` when this exact operation already completed; an existing abort or an
/// active recovery journal refuses a second driver.
async fn retained_shrink_is_complete(
    multi: &MultiRaft,
    journal: &MembershipShrinkJournal,
) -> Result<bool, String> {
    let Some(existing) = multi
        .placement
        .membership_shrink_journal(&journal.operation_id)
        .await?
    else {
        return Ok(false);
    };
    if !existing.phase.terminal() {
        return Err("membership shrink already has an active recovery journal".to_string());
    }
    if existing.phase != MembershipShrinkPhase::Completed {
        return Err("membership shrink has a retained terminal abort".to_string());
    }
    Ok(true)
}

/// Persist the new journal and every pre-removal phase, each before the next,
/// then require the durable safety gate.
async fn persist_safety_phases(
    multi: &MultiRaft,
    mut journal: MembershipShrinkJournal,
    evidence: &MembershipShrinkEvidence,
) -> Result<MembershipShrinkJournal, String> {
    multi.persist_membership_shrink_journal(&journal).await?;
    for phase in [
        MembershipShrinkPhase::DrainRequested,
        MembershipShrinkPhase::Drained,
        MembershipShrinkPhase::LearnerCaughtUp,
        MembershipShrinkPhase::LeadershipTransferred,
        MembershipShrinkPhase::SafetyChecked,
    ] {
        journal = journal.advance(phase, evidence.clone())?;
        multi.persist_membership_shrink_journal(&journal).await?;
    }
    if !journal.ready_for_removal() {
        return Err("membership shrink did not reach its durable safety gate".to_string());
    }
    Ok(journal)
}

/// Re-observe the committed membership: it must be exactly the retained voters
/// with the removed node not leading. Returns the evidence of that commit.
fn committed_removal_evidence(
    raft: &EgRaft,
    retained: &BTreeSet<NodeId>,
    node: NodeId,
    evidence: MembershipShrinkEvidence,
) -> Result<MembershipShrinkEvidence, String> {
    let (observed_voters, observed_leader) = {
        let metrics = raft.metrics();
        let watched = metrics.borrow_watched();
        let mut observed: Vec<NodeId> = watched.membership_config.voter_ids().collect();
        observed.sort_unstable();
        (observed, watched.current_leader)
    };
    if observed_voters != retained.iter().copied().collect::<Vec<_>>()
        || observed_leader == Some(node)
    {
        return Err(
            "membership shrink commit did not produce the expected voter fence".to_string(),
        );
    }
    let mut committed = evidence;
    committed.observed_voters = observed_voters;
    committed.observed_leader = observed_leader;
    committed.membership_change_committed = true;
    committed.target_absent = true;
    Ok(committed)
}

/// Retain `RemovalCommitted`, then `Completed`.
async fn complete_shrink_journal(
    multi: &MultiRaft,
    journal: MembershipShrinkJournal,
    committed: MembershipShrinkEvidence,
) -> Result<(), String> {
    let journal = journal.advance(MembershipShrinkPhase::RemovalCommitted, committed.clone())?;
    multi.persist_membership_shrink_journal(&journal).await?;
    let journal = journal.advance(MembershipShrinkPhase::Completed, committed)?;
    multi.persist_membership_shrink_journal(&journal).await
}
