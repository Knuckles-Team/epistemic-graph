//! Explicit server owner for native WorkItem lifecycle transitions.
//!
//! The six transition methods are selected here rather than by dispatch's
//! `is_work_item_mutation_method` classifier. Their authoritative effect is
//! unchanged: `mutation_batch::commit_work_item` atomically persists the
//! transition/result/outbox before refreshing the in-memory graph projection.

use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::{Method, Response};
use crate::server::persistence::PersistenceBackend;

/// Already-authorized graph and placement context for one WorkItem transition.
pub(crate) struct HandleContext<'a> {
    pub(crate) req_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) caller: Option<&'a str>,
    pub(crate) core: &'a Arc<GraphCore>,
    pub(crate) persistence: &'a Option<Arc<dyn PersistenceBackend>>,
    #[cfg(feature = "raft")]
    pub(crate) routed_raft: &'a Option<crate::raft::multi::RoutedRaftHandle>,
}

/// Handle exactly the six result-producing WorkItem lifecycle transitions.
///
/// Submission and resource-reservation methods intentionally return `Err` and
/// remain with their distinct admission/authority routes.
pub(crate) async fn try_handle(ctx: HandleContext<'_>, method: Method) -> Result<Response, Method> {
    let method = match method {
        method @ (Method::ClaimWorkItem { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CommitWorkItemResult { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. }
        | Method::CasWorkItemMetadata { .. }) => method,
        other => return Err(other),
    };

    #[cfg(feature = "raft")]
    let (placement_epoch, placement_fence) = if let Some(routed) = ctx.routed_raft.as_ref() {
        let leader = routed.handle.current_leader().await;
        if leader != Some(routed.handle.node_id) {
            return Ok(Response::stale_route(
                ctx.req_id,
                ctx.graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "WorkItem transitions require the current placement leader",
            ));
        }
        (routed.epoch, Some(routed.group_id))
    } else {
        (0, None)
    };
    #[cfg(not(feature = "raft"))]
    let (placement_epoch, placement_fence) = (0, None);

    let response = match crate::server::mutation_batch::commit_work_item(
        ctx.persistence.as_ref(),
        ctx.core,
        ctx.req_id,
        ctx.caller,
        ctx.graph_name,
        placement_epoch,
        placement_fence,
        method,
    )
    .await
    {
        Ok(result) => Response::ok(ctx.req_id, result),
        Err(error) => Response::err(ctx.req_id, format!("WorkItem mutation failed: {error}")),
    };
    Ok(response)
}
