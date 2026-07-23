//! Raft cluster-membership ADMIN RPC (CONCEPT:EG-KG.storage.kg-kg-2 —
//! `cluster_deployment.md` §5 item 2).
//!
//! `MultiRaft::add_group_learner` / `change_group_voters` (src/raft/multi.rs) already
//! implement the openraft add-learner / change-membership lifecycle — they simply had
//! NO external caller, so a fresh node could never actually be attached to a LIVE
//! cluster outside the in-process test harness. This module is that caller:
//! `Method::RaftAddLearner` / `Method::RaftChangeMembership`. Always declared (like
//! `placement`); the real answer is `raft`-gated (the only build where `MultiRaft`
//! exists), and a non-raft build answers a clean typed error.

use std::sync::Arc;

use tokio::sync::RwLock;

#[cfg(feature = "raft")]
use crate::protocol::ResultPayload;
use crate::protocol::{Method, Response};
use crate::server::state::ServerState;

#[cfg(feature = "raft")]
enum RaftAdminOp {
    AddLearner { node_id: u64, addr: String },
    ChangeMembership { voters: Vec<u64> },
}

#[cfg(feature = "raft")]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    let (group, op) = match method {
        Method::RaftAddLearner {
            group,
            node_id,
            addr,
        } => (
            group.unwrap_or(crate::raft::DEFAULT_GROUP),
            RaftAdminOp::AddLearner { node_id, addr },
        ),
        Method::RaftChangeMembership { group, voters } => (
            group.unwrap_or(crate::raft::DEFAULT_GROUP),
            RaftAdminOp::ChangeMembership { voters },
        ),
        other => return Err(other),
    };

    let (multi, standalone_raft) = {
        let current = state.read().await;
        (current.multi_raft.clone(), current.raft.is_some())
    };
    let Some(multi) = multi else {
        return Ok(Response::err(
            req_id,
            if standalone_raft {
                "CLUSTER_CONFIGURATION_INVALID: MultiRaft membership authority is required"
            } else {
                "RAFT_NOT_CONFIGURED: this node is not running a Raft cluster"
            },
        ));
    };

    let control = match multi.group(group).await {
        Some(g) => g,
        None => {
            return Ok(Response::stale_route(
                req_id,
                "__raft_membership__",
                group,
                0,
                None,
                "raft group is not running on this node",
            ))
        }
    };
    let leader = control.current_leader().await;
    if leader != Some(control.node_id) {
        return Ok(Response::stale_route(
            req_id,
            "__raft_membership__",
            group,
            0,
            leader,
            "raft membership changes require the current group leader",
        ));
    }

    let result = match op {
        RaftAdminOp::AddLearner { node_id, addr } => {
            multi.add_group_learner(group, node_id, addr).await
        }
        RaftAdminOp::ChangeMembership { voters } => {
            let voters: std::collections::BTreeSet<u64> = voters.into_iter().collect();
            multi.change_group_voters(group, voters).await
        }
    };
    match result {
        Ok(()) => Ok(Response::ok(req_id, ResultPayload::Bool(true))),
        Err(e) => Ok(Response::err(req_id, e)),
    }
}

#[cfg(not(feature = "raft"))]
pub(crate) async fn try_handle(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::RaftAddLearner { .. } | Method::RaftChangeMembership { .. } => Ok(Response::err(
            req_id,
            "raft cluster membership admin requires the raft feature",
        )),
        other => Err(other),
    }
}
