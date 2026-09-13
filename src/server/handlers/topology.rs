//! Authenticated cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology,
//! ADR-1 / W1.1, `reports/wave1/ADR-scale-trio.md` §ADR-1).
//!
//! `Method::ClusterMembers` is a bounded read, gated by
//! `cluster:topology-read`.  It cross-references every live `MultiRaft` group's
//! committed membership with the durable [`NodeInfoStore`].  The result is
//! signed with the engine request secret and bound to the already verified
//! tenant/principal/agent context.  A client therefore cannot accept a stale,
//! cross-cluster, unsigned, or differently scoped endpoint snapshot.
//!
//! Node-info writes are engine-owned typed Raft commands issued by
//! `raft::node::start`; there is no public method that accepts caller-supplied
//! endpoint authority.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

type HmacSha256 = Hmac<Sha256>;

const DISCOVERY_SCHEMA_VERSION: u64 = 1;
const DISCOVERY_DOMAIN: &[u8] = b"epistemic-graph/cluster-discovery/v1\0";
const MAX_DISCOVERY_GROUPS: usize = 1_024;
const MAX_DISCOVERY_MEMBERS: usize = 4_096;
const MAX_DISCOVERY_FIELD_BYTES: usize = 4 * 1024;

#[cfg(feature = "raft")]
fn node_info_unavailable(req_id: u64) -> Response {
    Response::err(
        req_id,
        "cluster topology requires a durable redb backend (no persist dir / not a redb build)",
    )
}

fn sha256_ref(value: &str) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(value.as_bytes())))
}

fn context_binding(
    context: &VerifiedRequestContext,
) -> eg_types::result_contract::cluster::DiscoveryAuthBinding {
    eg_types::result_contract::cluster::DiscoveryAuthBinding {
        tenant_digest: sha256_ref(context.tenant()),
        principal_digest: sha256_ref(context.principal()),
        agent_digest: sha256_ref(context.agent_id()),
    }
}

/// The three parallel topology projections a signed snapshot carries. Grouped so
/// the signing function keeps a readable arity (clippy::too_many_arguments) and
/// so the three can never be passed in the wrong order.
struct SnapshotProjections {
    groups: Vec<eg_types::result_contract::cluster::ClusterGroup>,
    canonical_groups: Vec<Value>,
    leaders: Vec<eg_types::result_contract::cluster::ClusterLeader>,
}

fn signed_snapshot(
    secret: &str,
    cluster_id: &str,
    membership_epoch: u64,
    placement_epoch: u64,
    context: &VerifiedRequestContext,
    projections: SnapshotProjections,
) -> Result<eg_types::result_contract::cluster::ClusterDiscoverySnapshot, String> {
    let SnapshotProjections {
        groups,
        canonical_groups,
        leaders,
    } = projections;
    if secret.is_empty() {
        return Err("cluster discovery signing authority is unavailable".to_string());
    }
    if cluster_id.len() > MAX_DISCOVERY_FIELD_BYTES
        || !cluster_id.strip_prefix("sha256:").is_some_and(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err("cluster discovery has an invalid cluster identity".to_string());
    }
    let leader = leaders.first().copied();
    let payload = serde_json::to_string(&serde_json::json!([
        "cluster-discovery-v1",
        cluster_id,
        membership_epoch,
        placement_epoch,
        context.tenant(),
        context.principal(),
        context.agent_id(),
        canonical_groups,
    ]))
    .map_err(|_| "cluster discovery canonicalization failed".to_string())?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| "cluster discovery signing authority is invalid".to_string())?;
    mac.update(DISCOVERY_DOMAIN);
    mac.update(payload.as_bytes());
    let signature = format!("hmac-sha256:{}", hex::encode(mac.finalize().into_bytes()));
    Ok(
        eg_types::result_contract::cluster::ClusterDiscoverySnapshot {
            schema_version: DISCOVERY_SCHEMA_VERSION,
            cluster_id: cluster_id.to_string(),
            membership_epoch,
            placement_epoch,
            leader,
            leaders,
            groups,
            auth_binding: context_binding(context),
            signature,
        },
    )
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_node_info(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    info: crate::server::persistence::node_info_store::NodeInfo,
) -> Result<bool, String> {
    let node_id = info.node_id;
    let raft_addr = info.raft_addr.clone();
    let advertised_client_addr = info.advertised_client_addr.clone();
    let backend = { state.read().await.persistence.clone() };
    let Some(store) = backend
        .as_ref()
        .and_then(|p| p.as_redb())
        .map(|b| b.node_info())
    else {
        tracing::warn!(
            req_id,
            node_id,
            "typed node self-report applied with no durable redb backend attached; self-report dropped"
        );
        return Err("cluster topology requires a durable redb backend".to_string());
    };
    match store.upsert(info) {
        Ok(()) => {
            tracing::debug!(
                req_id,
                node_id,
                raft_addr,
                advertised_client_addr,
                generation = store.generation(),
                "applied typed cluster-topology node self-report (ADR-1 / W1.1)"
            );
            Ok(true)
        }
        Err(error) => {
            tracing::warn!(req_id, node_id, %error, "cluster-topology node self-report rejected");
            Err(format!("node info upsert failed: {error}"))
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
fn certificate_value(
    info: &crate::server::persistence::node_info_store::NodeInfo,
) -> eg_types::result_contract::cluster::ClusterMemberCertificate {
    eg_types::result_contract::cluster::ClusterMemberCertificate {
        id: info.certificate_id.clone(),
        rotation_epoch: info.certificate_rotation_epoch,
        not_before_ms: info.certificate_not_before_ms,
        not_after_ms: info.certificate_not_after_ms,
    }
}

#[cfg(feature = "raft")]
fn member_health(leader_id: Option<crate::raft::NodeId>) -> &'static str {
    // Membership is the only health authority available on this RPC.  Do not
    // imply a transport heartbeat: an elected group is "healthy" from the
    // membership authority's perspective, while a leaderless group is
    // explicitly degraded.
    if leader_id.is_some() {
        "healthy"
    } else {
        "degraded"
    }
}

#[cfg(feature = "raft")]
async fn handle_cluster_members(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
) -> Response {
    let (backend, secret) = {
        let current = state.read().await;
        (current.persistence.clone(), current.auth_secret.clone())
    };
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
    let Some(cluster_id) = store.cluster_id() else {
        return Response::err(
            req_id,
            "CLUSTER_DISCOVERY_UNAVAILABLE: no authenticated cluster identity has been self-reported",
        );
    };

    let multi = { state.read().await.multi_raft.clone() };
    let Some(multi) = multi else {
        let membership_epoch = store.generation();
        return match signed_snapshot(
            &secret,
            &cluster_id,
            membership_epoch,
            0,
            verified_context,
            SnapshotProjections {
                groups: Vec::new(),
                canonical_groups: Vec::new(),
                leaders: Vec::new(),
            },
        ) {
            Ok(snapshot) => Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::cluster::ClusterMembers>(snapshot),
            ),
            Err(error) => Response::err(req_id, error),
        };
    };

    let membership_epoch = store.generation().max(multi.membership_epoch().await);
    let placement_epoch = multi.placement().current_epoch().await;
    let mut groups = Vec::new();
    let mut canonical_groups = Vec::new();
    let mut leaders = Vec::new();
    let mut member_count = 0usize;

    for group_id in multi.known_groups().await {
        if groups.len() >= MAX_DISCOVERY_GROUPS {
            return Response::err(req_id, "CLUSTER_DISCOVERY_LIMIT: too many Raft groups");
        }
        let Some(voters) = multi.group_membership(group_id).await else {
            return Response::err(
                req_id,
                format!("CLUSTER_DISCOVERY_INCOMPLETE: group {group_id} membership unavailable"),
            );
        };
        let Some(learners) = multi.group_learners(group_id).await else {
            return Response::err(
                req_id,
                format!("CLUSTER_DISCOVERY_INCOMPLETE: group {group_id} learners unavailable"),
            );
        };
        if voters.windows(2).any(|pair| pair[0] == pair[1])
            || learners.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Response::err(
                req_id,
                format!("CLUSTER_DISCOVERY_INVALID: group {group_id} repeats a member"),
            );
        }
        if voters.iter().any(|node_id| learners.contains(node_id)) {
            return Response::err(
                req_id,
                format!("CLUSTER_DISCOVERY_INVALID: group {group_id} overlaps voters and learners"),
            );
        }
        let leader_id = match multi.group(group_id).await {
            Some(group) => group.current_leader().await,
            None => {
                return Response::err(
                    req_id,
                    format!("CLUSTER_DISCOVERY_INCOMPLETE: group {group_id} is unavailable"),
                )
            }
        };
        if let Some(node_id) = leader_id {
            leaders.push(eg_types::result_contract::cluster::ClusterLeader { group_id, node_id });
        }

        let mut ids = voters.clone();
        ids.extend(learners.iter().copied());
        ids.sort_unstable();
        if leader_id.is_some_and(|node_id| !ids.contains(&node_id)) {
            return Response::err(
                req_id,
                format!("CLUSTER_DISCOVERY_INVALID: group {group_id} leader is not a member"),
            );
        }
        if ids.len() > MAX_DISCOVERY_MEMBERS.saturating_sub(member_count) {
            return Response::err(req_id, "CLUSTER_DISCOVERY_LIMIT: too many cluster members");
        }
        let health = member_health(leader_id);
        let mut members = Vec::with_capacity(ids.len());
        let mut canonical_members = Vec::with_capacity(ids.len());
        for node_id in ids {
            let Some(info) = store.get(node_id) else {
                return Response::err(
                    req_id,
                    format!(
                        "CLUSTER_DISCOVERY_INCOMPLETE: member {node_id} has no durable self-report"
                    ),
                );
            };
            if info.cluster_id != cluster_id
                || info.member_identity
                    != crate::server::persistence::node_info_store::member_identity_for(
                        &cluster_id,
                        node_id,
                    )
            {
                return Response::err(
                    req_id,
                    format!("CLUSTER_DISCOVERY_INVALID: member {node_id} identity mismatch"),
                );
            }
            let role = member_role(node_id, leader_id, &voters);
            let certificate = certificate_value(&info);
            canonical_members.push(serde_json::json!([
                node_id,
                info.member_identity,
                role,
                info.advertised_client_addr,
                info.tls_server_name,
                health,
                info.certificate_id,
                info.certificate_rotation_epoch,
                info.certificate_not_before_ms,
                info.certificate_not_after_ms,
            ]));
            members.push(eg_types::result_contract::cluster::ClusterMember {
                node_id,
                member_identity: info.member_identity.clone(),
                role: role.to_string(),
                client_endpoint: info.advertised_client_addr.clone(),
                tls_name: info.tls_server_name.clone(),
                health: health.to_string(),
                certificate,
            });
            member_count += 1;
        }
        canonical_groups.push(serde_json::json!([group_id, leader_id, canonical_members,]));
        groups.push(eg_types::result_contract::cluster::ClusterGroup {
            group_id,
            leader_id,
            members,
        });
    }

    match signed_snapshot(
        &secret,
        &cluster_id,
        membership_epoch,
        placement_epoch,
        verified_context,
        SnapshotProjections {
            groups,
            canonical_groups,
            leaders,
        },
    ) {
        Ok(snapshot) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::cluster::ClusterMembers>(snapshot),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "raft")]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
    verified_context: &VerifiedRequestContext,
) -> Result<Response, Method> {
    match method {
        Method::ClusterMembers => Ok(handle_cluster_members(state, req_id, verified_context).await),
        other => Err(other),
    }
}

#[cfg(not(feature = "raft"))]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
    verified_context: &VerifiedRequestContext,
) -> Result<Response, Method> {
    match method {
        Method::ClusterMembers => {
            let secret = { state.read().await.auth_secret.clone() };
            let snapshot = signed_snapshot(
                &secret,
                &sha256_ref("epistemic-graph/single-node-cluster/v1"),
                0,
                0,
                verified_context,
                SnapshotProjections {
                    groups: Vec::new(),
                    canonical_groups: Vec::new(),
                    leaders: Vec::new(),
                },
            );
            Ok(match snapshot {
                Ok(snapshot) => Response::ok(
                    req_id,
                    ResultPayload::of::<eg_types::result_contract::cluster::ClusterMembers>(
                        snapshot,
                    ),
                ),
                Err(error) => Response::err(req_id, error),
            })
        }
        other => Err(other),
    }
}
