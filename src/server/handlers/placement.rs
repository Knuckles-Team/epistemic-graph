//! Engine-authoritative placement-route RPC.
//!
//! Every successful response contains a complete `(group, epoch,
//! fencing_token)` route. An absent durable catalog row is not delegated to a
//! client-side hash ring: the engine returns its current unplaced policy. In a
//! clustered build only the placement-control leader answers, preventing a lagging
//! follower from publishing an obsolete epoch.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::epistemic_operations::{PlacementRoute, PlacementRouteSchemaVersion};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::state::ServerState;

fn route_response(
    req_id: u64,
    group: u64,
    epoch: u64,
    placed: bool,
    client_epoch: u64,
    tenant_ref: String,
    partition_ref: String,
) -> Response {
    if placed && epoch == 0 {
        return Response::err(req_id, "placement catalog contains an invalid epoch");
    }
    Response::ok(
        req_id,
        ResultPayload::raw(&PlacementRoute {
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
        }),
    )
}

#[cfg(feature = "raft")]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    let Method::PlacementRoute { request } = method else {
        return Err(method);
    };
    let tenant = request.tenant_ref;
    let sub_key = request.partition_ref;
    let client_epoch = request.client_epoch;

    let (multi, standalone_raft) = {
        let current = state.read().await;
        (current.multi_raft.clone(), current.raft.is_some())
    };
    let Some(multi) = multi else {
        if standalone_raft {
            return Ok(Response::err(
                req_id,
                "CLUSTER_CONFIGURATION_INVALID: MultiRaft placement authority is required",
            ));
        }
        return Ok(route_response(
            req_id,
            crate::raft::DEFAULT_GROUP,
            0,
            false,
            client_epoch,
            tenant,
            sub_key,
        ));
    };

    let control = match multi.group(crate::raft::DEFAULT_GROUP).await {
        Some(group) => group,
        None => {
            return Ok(Response::stale_route(
                req_id,
                "__placement_catalog__",
                crate::raft::DEFAULT_GROUP,
                0,
                None,
                "placement control group is unavailable",
            ))
        }
    };
    let leader = control.current_leader().await;
    if leader != Some(control.node_id) {
        return Ok(Response::stale_route(
            req_id,
            "__placement_catalog__",
            crate::raft::DEFAULT_GROUP,
            0,
            leader,
            "placement routes require the current control-group leader",
        ));
    }

    let route = multi.route_partition(&tenant, &sub_key).await;
    Ok(route_response(
        req_id,
        route.group,
        route.epoch,
        route.placed,
        client_epoch,
        tenant,
        sub_key,
    ))
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
        )),
        other => Err(other),
    }
}
