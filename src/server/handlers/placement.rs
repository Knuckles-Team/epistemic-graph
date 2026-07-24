//! Engine-authoritative placement-route RPC, plus the placement-catalog ADMIN
//! mutation trio (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc, DIST-P2-5).
//!
//! `PlacementRoute` is a read: every successful response contains a complete
//! `(group, epoch, fencing_token)` route. An absent durable catalog row is not
//! delegated to a client-side hash ring: the engine returns its current unplaced
//! policy. In a clustered build only the placement-control leader answers,
//! preventing a lagging follower from publishing an obsolete epoch.
//!
//! `PlacementAdmin` is the wire-exposed admin mutation that DRIVES the catalog
//! (`op` selects `Assign`/`Move`/`AbortMove`, see [`crate::protocol::PlacementAdminOp`]):
//! before it existed, `PlacementCatalog`'s assign/split/merge/online-move machinery
//! (`src/raft/placement.rs`, `src/raft/reshard.rs::TenantManager`) was fully
//! implemented and unit/harness tested, but reachable ONLY from in-process Rust —
//! there was no way for an external caller (a real multi-node cluster's operator, or
//! an automated placement control loop) to trigger a placement decision or drive an
//! online move over the wire. These handlers call the SAME already-proven
//! `MultiRaft`/`TenantManager` API the harness tests exercise directly, so no new
//! placement logic is introduced here — only its external RPC surface.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::epistemic_operations::PlacementRouteSchemaVersion;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::state::ServerState;

/// The actual `Method::PlacementRoute` WIRE response (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 —
/// `PlacementRoute.endpoints`, `reports/wave1/ADR-scale-trio.md` §ADR-1 decision 2).
///
/// Deliberately a SEPARATE type from [`crate::epistemic_operations::PlacementRoute`]:
/// that schema-locked cross-repo DTO is explicitly documented "without
/// deployment endpoint material" (it doubles as an audit/CDC-safe route-decision
/// record and is digest-pinned against the authoritative agent-utilities JSON
/// Schema catalog) and must not gain network topology fields. `endpoints` is
/// genuinely additive on the wire: every field below matches that locked shape
/// field-for-field, plus this one extra key — an old client that only knows the
/// locked shape simply never reads it.
#[derive(serde::Serialize)]
struct PlacementRouteWire {
    schema_version: PlacementRouteSchemaVersion,
    route_id: String,
    tenant_ref: String,
    partition_ref: String,
    authoritative: bool,
    placed: bool,
    group: u64,
    epoch: u64,
    fencing_token: u64,
    stale: bool,
    leader_ref: Option<String>,
    /// Client-reachable endpoints of the resolved group's members, LEADER FIRST
    /// (ADR-1). Empty when no cluster topology is known yet (single-node, a
    /// non-raft build, or no member has self-reported) — the client's
    /// static-map override / single-contact fallback (ADR-1 decision 3b/3c)
    /// applies.
    endpoints: Vec<String>,
}

fn route_response(
    req_id: u64,
    group: u64,
    epoch: u64,
    placed: bool,
    client_epoch: u64,
    tenant_ref: String,
    partition_ref: String,
    endpoints: Vec<String>,
) -> Response {
    if placed && epoch == 0 {
        return Response::err(req_id, "placement catalog contains an invalid epoch");
    }
    Response::ok(
        req_id,
        ResultPayload::raw(&PlacementRouteWire {
            schema_version: PlacementRouteSchemaVersion::V1,
            route_id: format!("request:{req_id}"),
            tenant_ref,
            partition_ref,
            authoritative: true,
            placed,
            group,
            epoch,
            fencing_token: group,
            stale: client_epoch < epoch,
            leader_ref: None,
            endpoints,
        }),
    )
}

/// Resolve `group`'s client-reachable endpoints, leader first (CONCEPT:EG-KG.sharding.cluster-topology,
/// ADR-1). Best effort: an unknown group, an unattached `MultiRaft`, or no
/// durable redb backend all resolve to an empty list rather than an error —
/// `PlacementRoute` must still answer the route itself even when topology
/// discovery is unavailable.
#[cfg(feature = "raft")]
async fn resolve_group_endpoints(
    state: &Arc<RwLock<ServerState>>,
    multi: &crate::raft::multi::MultiRaft,
    group: u64,
) -> Vec<String> {
    let backend = { state.read().await.persistence.clone() };
    let Some(store) = backend
        .as_ref()
        .and_then(|p| p.as_redb())
        .map(|b| b.node_info())
    else {
        tracing::debug!(
            group,
            "PlacementRoute.endpoints: no durable redb backend attached"
        );
        return Vec::new();
    };
    let Some(voters) = multi.group_membership(group).await else {
        tracing::debug!(
            group,
            "PlacementRoute.endpoints: group not running on this node"
        );
        return Vec::new();
    };
    let learners = multi.group_learners(group).await.unwrap_or_default();
    let leader = match multi.group(group).await {
        Some(handle) => handle.current_leader().await,
        None => None,
    };
    let endpoints: Vec<String> = store
        .ordered_members(&voters, &learners, leader)
        .into_iter()
        .map(|info| info.advertised_client_addr)
        .collect();
    if endpoints.is_empty() {
        tracing::debug!(
            group,
            voter_count = voters.len(),
            "PlacementRoute.endpoints: group has raft members but none have self-reported yet"
        );
    }
    endpoints
}

#[cfg(feature = "raft")]
async fn handle_route(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    request: crate::epistemic_operations::PlacementRouteRequest,
) -> Response {
    let tenant = request.tenant_ref;
    let sub_key = request.partition_ref;
    let client_epoch = request.client_epoch;

    let (multi, standalone_raft) = {
        let current = state.read().await;
        (current.multi_raft.clone(), current.raft.is_some())
    };
    let Some(multi) = multi else {
        if standalone_raft {
            return Response::err(
                req_id,
                "CLUSTER_CONFIGURATION_INVALID: MultiRaft placement authority is required",
            );
        }
        return route_response(
            req_id,
            crate::raft::DEFAULT_GROUP,
            0,
            false,
            client_epoch,
            tenant,
            sub_key,
            Vec::new(),
        );
    };

    let control = match multi.group(crate::raft::DEFAULT_GROUP).await {
        Some(group) => group,
        None => {
            return Response::stale_route(
                req_id,
                "__placement_catalog__",
                crate::raft::DEFAULT_GROUP,
                0,
                None,
                "placement control group is unavailable",
            )
        }
    };
    let leader = control.current_leader().await;
    if leader != Some(control.node_id) {
        return Response::stale_route(
            req_id,
            "__placement_catalog__",
            crate::raft::DEFAULT_GROUP,
            0,
            leader,
            "placement routes require the current control-group leader",
        );
    }

    let route = multi.route_partition(&tenant, &sub_key).await;
    let endpoints = resolve_group_endpoints(state, &multi, route.group).await;
    route_response(
        req_id,
        route.group,
        route.epoch,
        route.placed,
        client_epoch,
        tenant,
        sub_key,
        endpoints,
    )
}

/// Resolve the live `MultiRaft` placement authority + durable backend a placement
/// ADMIN mutation needs (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc). Both `PlacementAssign`
/// and the `TenantManager`-backed `PlacementMove`/`PlacementAbortMove` require a
/// running `MultiRaft` AND a durable backend to verify the moved data against — a
/// configured-but-not-running Raft node, or a build with no persist dir, fails closed
/// with a typed error rather than silently no-op'ing.
#[cfg(feature = "raft")]
async fn resolve_admin_authority(
    state: &Arc<RwLock<ServerState>>,
) -> Result<
    (
        std::sync::Arc<crate::raft::multi::MultiRaft>,
        std::sync::Arc<dyn crate::server::persistence::PersistenceBackend>,
    ),
    String,
> {
    let current = state.read().await;
    let multi = current.multi_raft.clone().ok_or_else(|| {
        "CLUSTER_CONFIGURATION_INVALID: MultiRaft placement authority is required for placement \
         admin mutations"
            .to_string()
    })?;
    let backend = current
        .persistence
        .clone()
        .ok_or_else(|| "placement admin mutations require a durable backend".to_string())?;
    Ok((multi, backend))
}

#[cfg(feature = "raft")]
async fn handle_assign(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    tenant: String,
    group: u64,
) -> Response {
    let multi = match resolve_admin_authority(state).await {
        Ok((multi, _backend)) => multi,
        Err(error) => return Response::err(req_id, error),
    };
    match multi.placement_assign(&tenant, group).await {
        Ok(epoch) => Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({"epoch": epoch})),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "raft")]
async fn handle_placement_admin_op(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    op: crate::protocol::PlacementAdminOp,
) -> Response {
    match op {
        crate::protocol::PlacementAdminOp::Assign { tenant, group } => {
            handle_assign(state, req_id, tenant, group).await
        }
        crate::protocol::PlacementAdminOp::Move {
            tenant,
            range_start,
            range_end,
            target,
        } => handle_move(state, req_id, tenant, range_start, range_end, target).await,
        crate::protocol::PlacementAdminOp::AbortMove { move_id } => {
            handle_abort_move(state, req_id, move_id).await
        }
    }
}

#[cfg(feature = "raft")]
fn reshard_report_json(report: &crate::raft::reshard::ReshardReport) -> serde_json::Value {
    serde_json::json!({
        "graph": report.graph,
        "from_group": report.from_group,
        "to_group": report.to_group,
        "nodes_transferred": report.nodes_transferred,
    })
}

#[cfg(feature = "raft")]
fn move_report_json(report: &crate::raft::reshard::PlacementMoveReport) -> serde_json::Value {
    serde_json::json!({
        "tenant": report.tenant,
        "range": [report.range.0, report.range.1],
        "target": report.target,
        "epoch": report.epoch,
        "graphs": report.graphs.iter().map(reshard_report_json).collect::<Vec<_>>(),
    })
}

#[cfg(feature = "raft")]
async fn handle_move(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    tenant: String,
    range_start: u64,
    range_end: u64,
    target: u64,
) -> Response {
    if range_start > range_end {
        return Response::err(req_id, "placement move range is invalid");
    }
    let (multi, backend) = match resolve_admin_authority(state).await {
        Ok(authority) => authority,
        Err(error) => return Response::err(req_id, error),
    };
    let tenants = crate::raft::reshard::TenantManager::new(multi, backend);
    match tenants
        .move_partition(&tenant, (range_start, range_end), target)
        .await
    {
        Ok(report) => Response::ok(req_id, ResultPayload::Json(move_report_json(&report))),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "raft")]
async fn handle_abort_move(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    move_id: String,
) -> Response {
    let (multi, backend) = match resolve_admin_authority(state).await {
        Ok(authority) => authority,
        Err(error) => return Response::err(req_id, error),
    };
    let tenants = crate::raft::reshard::TenantManager::new(multi, backend);
    match tenants.abort_move(&move_id).await {
        Ok(()) => Response::ok(req_id, ResultPayload::Bool(true)),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "raft")]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::PlacementRoute { request } => Ok(handle_route(state, req_id, request).await),
        Method::PlacementAdmin { op } => Ok(handle_placement_admin_op(state, req_id, op).await),
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
        Method::PlacementRoute { request } => Ok(route_response(
            req_id,
            0,
            0,
            false,
            request.client_epoch,
            request.tenant_ref,
            request.partition_ref,
            Vec::new(),
        )),
        Method::PlacementAdmin { .. } => Ok(Response::err(
            req_id,
            "CLUSTER_CONFIGURATION_INVALID: placement admin mutations require a `raft`-feature \
             cluster build",
        )),
        other => Err(other),
    }
}
