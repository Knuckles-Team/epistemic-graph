//! Cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1,
//! `reports/wave1/ADR-scale-trio.md` §ADR-1) — engine-authoritative client-side
//! cluster discovery, replacing the static hand-maintained
//! `GRAPH_RAFT_GROUP_ENDPOINTS` map.
//!
//! `Method::ClusterMembers` is a read, gated `cluster:topology-read` (deliberately
//! NOT `admin:cluster-read` — an ordinary service role needs it to re-resolve
//! after a failover, see `eg_capabilities::policy`). It cross-references every
//! live `MultiRaft` group's membership/leader against the durable
//! [`crate::server::persistence::node_info_store::NodeInfoStore`] to answer
//! `{groups: [{group_id, members: [{node_id, role, client_endpoint, tls_name}]}],
//! epoch}` from ANY reachable node — unlike `PlacementRoute`, a follower answers
//! identically to the leader, matching ADR-1's "any healthy seed" client
//! resolution story.
//!
//! `Method::NodeInfoUpsert` is the self-report write each node issues at Raft
//! startup (`raft::node::start` via `MultiRaft::commit_node_info`) — see the
//! store module for the durable side and its replication story.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::state::ServerState;

fn node_info_unavailable(req_id: u64) -> Response {
    Response::err(
        req_id,
        "cluster topology requires a durable redb backend (no persist dir / not a redb build)",
    )
}

async fn handle_node_info_upsert(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    node_id: u64,
    raft_addr: String,
    advertised_client_addr: String,
    tls_server_name: Option<String>,
) -> Response {
    let backend = { state.read().await.persistence.clone() };
    let Some(store) = backend
        .as_ref()
        .and_then(|p| p.as_redb())
        .map(|b| b.node_info())
    else {
        tracing::warn!(
            req_id,
            node_id,
            "NodeInfoUpsert applied with no durable redb backend attached; self-report dropped"
        );
        return node_info_unavailable(req_id);
    };
    let info = crate::server::persistence::node_info_store::NodeInfo {
        node_id,
        raft_addr: raft_addr.clone(),
        advertised_client_addr: advertised_client_addr.clone(),
        tls_server_name,
    };
    match store.upsert(info) {
        Ok(()) => {
            tracing::debug!(
                req_id,
                node_id,
                raft_addr,
                advertised_client_addr,
                generation = store.generation(),
                "applied cluster-topology NodeInfoUpsert (ADR-1 / W1.1)"
            );
            Response::ok(req_id, ResultPayload::Bool(true))
        }
        Err(error) => {
            tracing::warn!(req_id, node_id, %error, "cluster-topology NodeInfoUpsert rejected");
            Response::err(req_id, format!("node info upsert failed: {error}"))
        }
    }
}

#[cfg(feature = "raft")]
fn member_role(
    node_id: crate::raft::NodeId,
    leader: Option<crate::raft::NodeId>,
    voters: &[crate::raft::NodeId],
) -> &'static str {
    if Some(node_id) == leader {
        "leader"
    } else if voters.contains(&node_id) {
        "follower"
    } else {
        "learner"
    }
}

#[cfg(feature = "raft")]
async fn handle_cluster_members(state: &Arc<RwLock<ServerState>>, req_id: u64) -> Response {
    let backend = { state.read().await.persistence.clone() };
    let Some(store) = backend
        .as_ref()
        .and_then(|p| p.as_redb())
        .map(|b| b.node_info())
    else {
        tracing::warn!(
            req_id,
            "ClusterMembers requested with no durable redb backend attached"
        );
        return node_info_unavailable(req_id);
    };
    let epoch = store.generation();

    let multi = { state.read().await.multi_raft.clone() };
    let Some(multi) = multi else {
        // No live MultiRaft (single-node, or a configured-but-not-running Raft
        // node): a well-formed empty topology, exactly like `PlacementRoute`'s
        // single-node fallback returns a complete unplaced answer rather than an
        // error. The client's single-contact fallback (ADR-1 decision 3c) applies.
        tracing::debug!(
            req_id,
            epoch,
            "ClusterMembers: no live MultiRaft, answering empty topology"
        );
        return Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({"groups": [], "epoch": epoch})),
        );
    };

    let mut groups = Vec::new();
    for group_id in multi.known_groups().await {
        let Some(voters) = multi.group_membership(group_id).await else {
            continue;
        };
        let learners = multi.group_learners(group_id).await.unwrap_or_default();
        let leader_id = match multi.group(group_id).await {
            Some(group) => group.current_leader().await,
            None => None,
        };
        let members: Vec<serde_json::Value> = store
            .ordered_members(&voters, &learners, leader_id)
            .into_iter()
            .map(|info| {
                serde_json::json!({
                    "node_id": info.node_id,
                    "role": member_role(info.node_id, leader_id, &voters),
                    "client_endpoint": info.advertised_client_addr,
                    "tls_name": info.tls_server_name,
                })
            })
            .collect();
        if members.len() < voters.len() + learners.len() {
            tracing::debug!(
                req_id,
                group_id,
                known_members = members.len(),
                raft_members = voters.len() + learners.len(),
                "ClusterMembers: some raft members have no durable self-report yet"
            );
        }
        groups.push(serde_json::json!({"group_id": group_id, "members": members}));
    }
    tracing::debug!(
        req_id,
        group_count = groups.len(),
        epoch,
        "served ClusterMembers"
    );
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({"groups": groups, "epoch": epoch})),
    )
}

#[cfg(feature = "raft")]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::ClusterMembers => Ok(handle_cluster_members(state, req_id).await),
        Method::NodeInfoUpsert {
            node_id,
            raft_addr,
            advertised_client_addr,
            tls_server_name,
        } => Ok(handle_node_info_upsert(
            state,
            req_id,
            node_id,
            raft_addr,
            advertised_client_addr,
            tls_server_name,
        )
        .await),
        other => Err(other),
    }
}

#[cfg(not(feature = "raft"))]
pub(crate) async fn try_handle(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::ClusterMembers => Ok(Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({"groups": [], "epoch": 0})),
        )),
        Method::NodeInfoUpsert { .. } => Ok(Response::err(
            req_id,
            "CLUSTER_CONFIGURATION_INVALID: cluster topology self-report requires a \
             `raft`-feature cluster build",
        )),
        other => Err(other),
    }
}
