//! Explicit server owner for native development-lane operations.
//!
//! The six durable write methods are enumerated beside the two authority reads.
//! No classifier or wildcard commit arm may silently acquire a future Method.
//! Writes retain the single `PersistenceBackend::commit_development_lane`
//! authority and the graph-scoped mutation lock used by the prior route.

use std::sync::Arc;

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::persistence::PersistenceBackend;

/// Already-authorized graph and placement context for one development-lane op.
pub(crate) struct HandleContext<'a> {
    pub(crate) req_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) persistence: &'a Option<Arc<dyn PersistenceBackend>>,
    #[cfg(feature = "raft")]
    pub(crate) multi_raft: &'a Option<Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "raft")]
    pub(crate) routed_raft: &'a Option<crate::raft::multi::RoutedRaftHandle>,
}

#[cfg(feature = "raft")]
async fn enforce_leadership(ctx: &HandleContext<'_>) -> Result<(), Response> {
    let Some(routed) = ctx.routed_raft.as_ref() else {
        return Ok(());
    };
    let leader = routed.handle.current_leader().await;
    if leader != Some(routed.handle.node_id) {
        return Err(Response::stale_route(
            ctx.req_id,
            ctx.graph_name,
            routed.group_id,
            routed.epoch,
            leader,
            "development-lane operations require the current placement leader",
        ));
    }
    let Some(multi) = ctx.multi_raft.as_ref() else {
        return Ok(());
    };
    multi
        .read_barrier_group(routed.group_id)
        .await
        .map_err(|error| {
            Response::err(
                ctx.req_id,
                format!("development-lane linearizability barrier failed: {error:?}"),
            )
        })
}

struct PreparedOperation {
    backend: Arc<dyn PersistenceBackend>,
    graph: String,
    now_ms: u64,
    _mutation_guard: tokio::sync::OwnedMutexGuard<()>,
}

async fn prepare(ctx: &HandleContext<'_>) -> Result<PreparedOperation, Response> {
    #[cfg(feature = "raft")]
    enforce_leadership(ctx).await?;
    let backend = ctx.persistence.as_ref().cloned().ok_or_else(|| {
        Response::err(
            ctx.req_id,
            "native development-lane persistence is unavailable",
        )
    })?;
    Ok(PreparedOperation {
        backend,
        graph: crate::persist::sanitize(ctx.graph_name),
        now_ms: crate::server::dispatch::authoritative_now_ms(),
        _mutation_guard: crate::server::mutation_batch::lock_graph(ctx.graph_name).await,
    })
}

/// Handle the complete native development-lane surface.
pub(crate) async fn try_handle(ctx: HandleContext<'_>, method: Method) -> Result<Response, Method> {
    let response = match method {
        Method::QueryDevelopmentLane { request } => {
            let operation = match prepare(&ctx).await {
                Ok(operation) => operation,
                Err(response) => return Ok(response),
            };
            operation
                .backend
                .read_development_lane(&operation.graph, &request, operation.now_ms)
                .await
                .map(|result| Response::ok(ctx.req_id, ResultPayload::raw(&result)))
                .unwrap_or_else(|error| {
                    Response::err(
                        ctx.req_id,
                        format!("development-lane query failed: {error}"),
                    )
                })
        }
        Method::DevelopmentLaneStatus { request } => {
            let operation = match prepare(&ctx).await {
                Ok(operation) => operation,
                Err(response) => return Ok(response),
            };
            operation
                .backend
                .read_development_lane_status(&operation.graph, &request, operation.now_ms)
                .await
                .map(|result| Response::ok(ctx.req_id, ResultPayload::raw(&result)))
                .unwrap_or_else(|error| {
                    Response::err(
                        ctx.req_id,
                        format!("development-lane status read failed: {error}"),
                    )
                })
        }
        method @ (Method::ReserveDevelopmentLane { .. }
        | Method::RenewDevelopmentLane { .. }
        | Method::ObserveDevelopmentLane { .. }
        | Method::FinishDevelopmentLane { .. }
        | Method::CleanupDevelopmentLane { .. }
        | Method::UpdateDevelopmentLaneQuota { .. }) => {
            let operation = match prepare(&ctx).await {
                Ok(operation) => operation,
                Err(response) => return Ok(response),
            };
            operation
                .backend
                .commit_development_lane(&operation.graph, method, operation.now_ms)
                .await
                .map(|bytes| Response::ok(ctx.req_id, ResultPayload::Raw(bytes)))
                .unwrap_or_else(|error| {
                    Response::err(
                        ctx.req_id,
                        format!("development-lane commit failed: {error}"),
                    )
                })
        }
        other => return Err(other),
    };
    Ok(response)
}
