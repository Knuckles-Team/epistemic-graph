//! One leader-balancing pass over the groups running on a node.
//!
//! Split out of `multi.rs` (CCCC burn-down lane L-raft-b). `rebalance_leaders`
//! observed, aggregated, and gated every group in one body; here each step is
//! named. `multi.rs` was already over the KISS whole-file thresholds, so the
//! steps live beside the method and the parent's own counts only go down.

use std::collections::BTreeMap;

use openraft::async_runtime::watch::WatchReceiver;

use super::{
    desired_leader, failure_domain_safe, EgRaft, GroupId, MultiRaft, NodeId, RebalanceReport,
    LEADER_TRANSFER_MIN_MARGIN, MAX_AUTOMATIC_TRANSFERS_PER_PASS,
};

// ── R1: leader balancing across groups (CONCEPT:EG-KG.sharding.multi-raft → KG-2.273) ────
//
// With N groups over M nodes, leaders cluster on the bootstrap node (it
// single-member-initializes every group). [`rebalance_leaders`] spreads leadership by
// a deterministic round-robin: each group has a target leader computed identically on
// every node ([`desired_leader`]). EVERY node runs this pass (like a real cluster);
// each only acts on the groups it currently LEADS:
//
//   * **Transfer** — if THIS node IS the leader of a group whose round-robin target
//     is ELSEWHERE, it issues the native openraft-0.10
//     `trigger().transfer_leader(target)`. openraft hands a fresh term + the leader
//     vote to the target and notifies it (over `NetTransferLeader`) to campaign at
//     once — a GRACEFUL, near-instant handoff. No cooperative heartbeat-yield is
//     needed any more (that was the 0.9 workaround for the missing transfer RPC).
//
// A follower never acts (only the current leader can transfer). Converges to the
// round-robin spread within roughly one heartbeat, not a couple of election timeouts.

impl MultiRaft {
    /// Run one leader-balancing pass over the groups running on THIS node
    /// (CONCEPT:AU-KG.backend.authority-has-already-acked). For each group THIS node leads whose round-robin target is a
    /// different node, it issues the native `trigger().transfer_leader(target)` for an
    /// instant graceful handoff (rate-limited per group by [`super::TRANSFER_COOLDOWN`] so it
    /// never spams transfers while one settles). A no-op for single-voter groups and for
    /// groups this node already leads correctly (or does not lead), so repeated passes on
    /// a balanced cluster do nothing. Returns a [`RebalanceReport`].
    pub async fn rebalance_leaders(&self) -> RebalanceReport {
        let mut report = RebalanceReport::default();
        let observations = observe_groups(self, &mut report).await;
        // Build a cluster-consistent, best-effort load view from committed Raft
        // metrics.  Every node sees the same leader assignment once caught up,
        // while an unsettled/unknown leader simply contributes no load and cannot
        // trigger a speculative transfer.
        let loads = LeaderLoadView::build(&observations);
        record_failure_domains(self, &observations);
        let failure_domains = self.failure_domains.read().clone();
        for observation in observations {
            balance_group(self, &mut report, &loads, &failure_domains, observation).await;
        }
        report.transferred.sort_unstable();
        report.skipped.sort_unstable_by_key(|(gid, _)| *gid);
        report
    }
}

/// One local group's committed leadership view and its round-robin target.
struct GroupLeadership {
    gid: GroupId,
    raft: EgRaft,
    voters: Vec<NodeId>,
    current_leader: Option<NodeId>,
    local_is_leader: bool,
    target: NodeId,
    discovered_domains: Vec<(NodeId, String)>,
}

impl GroupLeadership {
    /// Observe one group; `None` for an empty voter set (no target exists).
    fn observe(gid: GroupId, raft: &EgRaft) -> Option<Self> {
        let metrics = raft.metrics();
        let watched = metrics.borrow_watched();
        let mut voters: Vec<NodeId> = watched.membership_config.voter_ids().collect();
        voters.sort_unstable();
        let target = desired_leader(gid, &voters)?;
        let discovered_domains: Vec<(NodeId, String)> = watched
            .membership_config
            .nodes()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    super::super::config::failure_domain_for_peer(*node_id, &node.addr),
                )
            })
            .collect();
        let observation = Self {
            gid,
            raft: raft.clone(),
            voters,
            current_leader: watched.current_leader,
            local_is_leader: matches!(watched.state, openraft::ServerState::Leader),
            target,
            discovered_domains,
        };
        Some(observation)
    }

    /// The observed leader, only when it is one of the group's voters.
    fn voting_leader(&self) -> Option<NodeId> {
        self.current_leader
            .filter(|leader| self.voters.contains(leader))
    }
}

/// Observe every group this node runs and record each round-robin target.
async fn observe_groups(multi: &MultiRaft, report: &mut RebalanceReport) -> Vec<GroupLeadership> {
    let groups = multi.groups.read().await;
    let observations: Vec<GroupLeadership> = groups
        .iter()
        .filter_map(|(&gid, raft)| GroupLeadership::observe(gid, raft))
        .collect();
    for observation in &observations {
        report.targets.insert(observation.gid, observation.target);
    }
    observations
}

/// Leaders per node across the observed groups, and whether every group's
/// leader is known and a voter.
struct LeaderLoadView {
    loads: BTreeMap<NodeId, usize>,
    complete: bool,
}

impl LeaderLoadView {
    fn build(observations: &[GroupLeadership]) -> Self {
        let mut loads: BTreeMap<NodeId, usize> = BTreeMap::new();
        for leader in observations
            .iter()
            .filter_map(GroupLeadership::voting_leader)
        {
            *loads.entry(leader).or_default() += 1;
        }
        Self {
            loads,
            complete: observations
                .iter()
                .all(|observation| observation.voting_leader().is_some()),
        }
    }

    fn load(&self, node: NodeId) -> usize {
        self.loads.get(&node).copied().unwrap_or_default()
    }
}

/// Remember the first failure domain discovered for every member node.
fn record_failure_domains(multi: &MultiRaft, observations: &[GroupLeadership]) {
    let mut known_domains = multi.failure_domains.write();
    for (node_id, domain) in observations
        .iter()
        .flat_map(|observation| &observation.discovered_domains)
    {
        known_domains
            .entry(*node_id)
            .or_insert_with(|| domain.clone());
    }
}

/// What the balance gate decided for one group.
enum TransferDecision {
    /// Not this node's decision to make (or nothing to balance).
    Ignore,
    /// Deliberately left untouched, with the reason reported.
    Skip(String),
    /// Hand leadership to the round-robin target.
    Transfer,
}

/// The ordered safety and benefit gates for one group.
fn transfer_decision(
    local_node: NodeId,
    report: &RebalanceReport,
    loads: &LeaderLoadView,
    failure_domains: &BTreeMap<NodeId, String>,
    observation: &GroupLeadership,
) -> TransferDecision {
    // Nothing to balance for a single-voter group.
    if observation.voters.len() <= 1 {
        return TransferDecision::Ignore;
    }
    let Some(current) = observation.current_leader else {
        return TransferDecision::Skip("current leader is not yet observed".to_string());
    };
    // Only the current local leader can hand off, and only when the target is
    // elsewhere.  This also prevents every follower from competing to transfer
    // the same group.
    let target = observation.target;
    if !observation.local_is_leader || current != local_node || target == local_node {
        return TransferDecision::Ignore;
    }
    if !loads.complete {
        return TransferDecision::Skip(
            "leader-load view is incomplete while membership/terms settle".to_string(),
        );
    }
    if !failure_domain_safe(failure_domains, current, target) {
        return TransferDecision::Skip(
            "target shares the current leader failure domain or has no domain".to_string(),
        );
    }
    let (current_load, target_load) = (loads.load(current), loads.load(target));
    if current_load.saturating_sub(target_load) < LEADER_TRANSFER_MIN_MARGIN {
        return TransferDecision::Skip(format!(
            "leader-load margin below hysteresis (current={current_load}, target={target_load})"
        ));
    }
    if report.transferred.len() + report.errors.len() >= MAX_AUTOMATIC_TRANSFERS_PER_PASS {
        return TransferDecision::Skip("per-pass automatic transfer bound reached".to_string());
    }
    TransferDecision::Transfer
}

/// Apply the gate's decision for one group, recording the outcome.
async fn balance_group(
    multi: &MultiRaft,
    report: &mut RebalanceReport,
    loads: &LeaderLoadView,
    failure_domains: &BTreeMap<NodeId, String>,
    observation: GroupLeadership,
) {
    let gid = observation.gid;
    match transfer_decision(multi.node_id, report, loads, failure_domains, &observation) {
        TransferDecision::Ignore => {}
        TransferDecision::Skip(reason) => report.skipped.push((gid, reason)),
        TransferDecision::Transfer => {
            if multi.may_transfer(gid) {
                match observation
                    .raft
                    .trigger()
                    .transfer_leader(observation.target)
                    .await
                {
                    Ok(()) => report.transferred.push(gid),
                    Err(error) => report.errors.push((gid, error.to_string())),
                }
            }
        }
    }
}
