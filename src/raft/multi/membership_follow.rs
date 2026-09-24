//! Leader-following membership admin (EH-534).
//!
//! Membership changes are leader-only in Raft, and the leader can change under an
//! admin call: a legal election in the middle of add-learner → change-membership
//! used to surface openraft's "has to forward request to" error to the caller. The
//! three entry points below resolve the group's CURRENT leader on every attempt,
//! apply the change locally when that is this node, or forward it over the
//! authenticated peer channel (`network::MembershipChange`) when it is not, and
//! re-resolve after a leadership move until [`FOLLOW_BUDGET`] runs out. Every
//! change is idempotent, so re-sending one whose commit raced a leadership move is
//! safe.

use std::collections::BTreeSet;
use std::sync::{OnceLock, Weak};
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use openraft::BasicNode;

use super::super::network::{self, GroupRpc, GroupRpcReply, MembershipChange, MembershipOutcome};
use super::super::xread::ReadPageService;
use super::{EgRaft, GroupId, MultiRaft, NodeId};

/// How long one membership change keeps following a moving leader.
const FOLLOW_BUDGET: Duration = Duration::from_secs(60);
/// Pause before re-resolving a leader an election has not settled yet.
const SETTLE_PAUSE: Duration = Duration::from_millis(200);

/// The listener's late-bound handle to its node manager: the listener starts
/// before the `MultiRaft` that owns it exists.
pub(super) type MembershipOwner = OnceLock<Weak<MultiRaft>>;

/// Where the next attempt must run.
enum Route {
    Here,
    Leader(String),
    Unsettled,
}

impl MultiRaft {
    /// Attach `new_node` (reachable at `addr`) to group `gid` as a NON-VOTING LEARNER
    /// (CONCEPT:EG-KG.storage.kg-kg-2): the leader registers `addr` with its peer
    /// pool, starts replicating, and BLOCKS until the learner's log is caught up.
    /// The voter set is untouched; promote with [`Self::change_group_voters`] or use
    /// [`Self::add_group_member`]. Callable on any member: it follows the leader.
    pub async fn add_group_learner(
        &self,
        gid: GroupId,
        new_node: NodeId,
        addr: String,
    ) -> Result<(), String> {
        let change = MembershipChange::AddLearner {
            node: new_node,
            addr,
        };
        self.follow_leader(gid, change).await
    }

    /// Set group `gid`'s VOTER set to exactly `voters` (openraft
    /// `change_membership`). Refuses an EMPTY voter set, which would leave the group
    /// leaderless. Idempotent; callable on any member: it follows the leader.
    pub async fn change_group_voters(
        &self,
        gid: GroupId,
        voters: BTreeSet<NodeId>,
    ) -> Result<(), String> {
        if voters.is_empty() {
            return Err(format!(
                "refusing to set an empty voter set for group {gid}"
            ));
        }
        self.follow_leader(gid, MembershipChange::SetVoters(voters))
            .await
    }

    /// Add `new_node` to group `gid` as a VOTER: add it as a learner, then promote
    /// it into the voter set the leader has committed at that moment.
    pub async fn add_group_member(
        &self,
        gid: GroupId,
        new_node: NodeId,
        addr: String,
    ) -> Result<(), String> {
        self.add_group_learner(gid, new_node, addr).await?;
        self.follow_leader(gid, MembershipChange::Promote(new_node))
            .await
    }

    async fn follow_leader(&self, gid: GroupId, change: MembershipChange) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + FOLLOW_BUDGET;
        loop {
            let raft = self.group_raft(gid).await?;
            let outcome = match self.route(&raft).await {
                Route::Here => self.apply_membership(&raft, gid, change.clone()).await?,
                Route::Leader(addr) => {
                    network::forward_membership(&self.pool, &addr, gid, change.clone()).await?
                }
                Route::Unsettled => MembershipOutcome::LeaderMoved,
            };
            if outcome == MembershipOutcome::Applied {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "membership change on group {gid} found no settled leader within {}s",
                    FOLLOW_BUDGET.as_secs()
                ));
            }
            tokio::time::sleep(SETTLE_PAUSE).await;
        }
    }

    /// This node's handle on group `gid`.
    async fn group_raft(&self, gid: GroupId) -> Result<EgRaft, String> {
        self.groups
            .read()
            .await
            .get(&gid)
            .cloned()
            .ok_or_else(|| format!("group {gid} not running on node {}", self.node_id))
    }

    /// Resolve the group's current leader and its committed address.
    async fn route(&self, raft: &EgRaft) -> Route {
        let Some(leader) = raft.current_leader().await else {
            return Route::Unsettled;
        };
        if leader == self.node_id {
            return Route::Here;
        }
        let metrics = raft.metrics();
        let current = metrics.borrow_watched();
        match current.membership_config.get_node(&leader) {
            Some(node) => Route::Leader(node.addr.clone()),
            None => Route::Unsettled,
        }
    }

    /// Apply `change` on this node's replica, which believed it led the group.
    /// A replica that has lost leadership answers [`MembershipOutcome::LeaderMoved`].
    async fn apply_membership(
        &self,
        raft: &EgRaft,
        gid: GroupId,
        change: MembershipChange,
    ) -> Result<MembershipOutcome, String> {
        let committed = match change {
            MembershipChange::AddLearner { node, addr } => {
                self.register_member_peer(node, &addr)?;
                raft.add_learner(node, BasicNode::new(addr), true).await
            }
            MembershipChange::SetVoters(voters) => raft.change_membership(voters, false).await,
            MembershipChange::Promote(node) => {
                let mut voters = committed_voters(raft);
                voters.insert(node);
                raft.change_membership(voters, false).await
            }
        };
        match committed {
            Ok(_) => Ok(MembershipOutcome::Applied),
            Err(error) if error.forward_to_leader().is_some() => Ok(MembershipOutcome::LeaderMoved),
            Err(error) => Err(format!("membership change on group {gid}: {error}")),
        }
    }

    /// Make a new member reachable from this node before replicating to it.
    fn register_member_peer(&self, node: NodeId, addr: &str) -> Result<(), String> {
        self.pool
            .register_peer(node, addr)
            .map_err(|_| "invalid or conflicting Raft peer registration".to_string())?;
        self.failure_domains
            .write()
            .entry(node)
            .or_insert_with(|| super::super::config::failure_domain_for_peer(node, addr));
        Ok(())
    }
}

fn committed_voters(raft: &EgRaft) -> BTreeSet<NodeId> {
    let metrics = raft.metrics();
    let watched = metrics.borrow_watched();
    watched.membership_config.voter_ids().collect()
}

/// Serve one un-batched peer RPC. A forwarded membership change is applied by
/// this node's manager (it owns peer registration); everything else goes to the
/// group's replica as before.
pub(super) async fn serve_one(
    groups: &tokio::sync::RwLock<std::collections::BTreeMap<GroupId, EgRaft>>,
    owner: &MembershipOwner,
    rpc: GroupRpc,
    read_service: &ReadPageService,
) -> GroupRpcReply {
    let gid = rpc.group_id();
    if let GroupRpc::Membership(_, change) = rpc {
        return GroupRpcReply::Membership(serve_forwarded(owner, gid, change).await);
    }
    let raft = groups.read().await.get(&gid).cloned();
    network::dispatch_group(raft, gid, rpc, read_service).await
}

/// Apply a forwarded change here, never forwarding it again: the ORIGINATOR
/// re-resolves the leader, so a change cannot bounce between nodes.
async fn serve_forwarded(
    owner: &MembershipOwner,
    gid: GroupId,
    change: MembershipChange,
) -> Result<MembershipOutcome, String> {
    let multi = owner
        .get()
        .and_then(Weak::upgrade)
        .ok_or_else(|| "this node's Raft manager is not running".to_string())?;
    let raft = multi.group_raft(gid).await?;
    multi.apply_membership(&raft, gid, change).await
}
