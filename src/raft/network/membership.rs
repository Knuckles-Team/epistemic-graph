//! The membership-admin frame (EH-534): a group's membership change forwarded
//! over the authenticated peer channel to the node that currently leads it.
//!
//! Membership changes are leader-only in Raft. An admin call that reaches a
//! follower -- or a leader that loses an election mid-sequence -- used to fail
//! with openraft's "has to forward request to" error. The originator now
//! resolves the current leader and sends it this frame; the leader applies the
//! change to its own replica and answers whether it still led.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::super::{GroupId, NodeId};
use super::{decode_wire, GroupRpc, GroupRpcReply, PeerPool, RaftFrame, RaftFrameReply};

/// One membership change, resolved against the applying leader's own view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipChange {
    /// Attach `node` (reachable at `addr`) as a learner and wait for catch-up.
    AddLearner { node: NodeId, addr: String },
    /// Set the voter set to exactly these nodes.
    SetVoters(BTreeSet<NodeId>),
    /// Add `node` to the voter set the leader has committed.
    Promote(NodeId),
}

/// What the node a change was sent to did with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipOutcome {
    /// Committed by that node as leader.
    Applied,
    /// That node no longer leads the group; resolve the leader again.
    LeaderMoved,
}

/// Send `change` to the node at `addr`, which the caller believes leads `group_id`.
pub(crate) async fn forward_membership(
    pool: &PeerPool,
    addr: &str,
    group_id: GroupId,
    change: MembershipChange,
) -> Result<MembershipOutcome, String> {
    let body = rmp_serde::to_vec_named(&RaftFrame::One(Box::new(GroupRpc::Membership(
        group_id, change,
    ))))
    .map_err(|_| "unable to encode a Raft membership change".to_string())?;
    let response = pool
        .round_trip(addr, &body)
        .await
        .map_err(|error| format!("Raft membership forward transport failed: {error}"))?;
    match decode_wire::<RaftFrameReply>(&response)
        .map_err(|error| format!("Raft membership forward reply is invalid: {error}"))?
    {
        RaftFrameReply::One(GroupRpcReply::Membership(outcome)) => outcome,
        _ => Err("Raft membership forward returned an unexpected reply".to_string()),
    }
}
