//! Explicit server owner for native WorkItem lifecycle transitions.
//!
//! The six transition methods are selected here rather than by dispatch's
//! `is_work_item_mutation_method` classifier. Their authoritative effect is
//! unchanged: `mutation_batch::commit_work_item` atomically persists the
//! transition/result/outbox before refreshing the in-memory graph projection.

use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::{Method, Response};
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::PersistenceBackend;

/// Already-authorized graph and placement context for one WorkItem transition.
pub(crate) struct HandleContext<'a> {
    pub(crate) req_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) caller: Option<&'a str>,
    pub(crate) verified_context: &'a VerifiedRequestContext,
    pub(crate) core: &'a Arc<GraphCore>,
    pub(crate) persistence: &'a Option<Arc<dyn PersistenceBackend>>,
    #[cfg(feature = "raft")]
    pub(crate) routed_raft: &'a Option<crate::raft::multi::RoutedRaftHandle>,
}

/// Handle exactly the six result-producing WorkItem lifecycle transitions and
/// the two native control-lease writes (graph-os EG-2), which share the same
/// durable WorkItem MutationBatch kernel. Dispatch has already bound a lease
/// write's tenant to the verified carrier (`route_control_lease_writes`).
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
        | Method::CasWorkItemMetadata { .. }
        | Method::IssueControlLease { .. }
        | Method::TransitionControlLease { .. }) => method,
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

    let response =
        match commit_verified_work_item(&ctx, placement_epoch, placement_fence, method).await {
            Ok(result) => Response::ok(ctx.req_id, result),
            Err(error) => Response::err(ctx.req_id, format!("WorkItem mutation failed: {error}")),
        };
    Ok(response)
}

/// Commit one native WorkItem method through the durable MutationBatch path under
/// the request's verified idempotency key, attempt nonce and placement fence.
/// Shared by the lifecycle transitions here and by kg-delegate admission.
pub(crate) async fn commit_verified_work_item(
    ctx: &HandleContext<'_>,
    placement_epoch: u64,
    placement_fence: Option<u64>,
    method: Method,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::server::mutation_batch::commit_work_item(
        crate::server::mutation_batch::WorkItemCommitRequest::new(
            ctx.persistence.as_ref(),
            ctx.core,
            crate::server::mutation_batch::CommitOrigin {
                request_id: ctx.req_id,
                principal: ctx.caller,
            },
            Some(ctx.verified_context.idempotency_key()),
            ctx.graph_name,
            placement_epoch,
            method,
        )
        .with_attempt_nonce(ctx.verified_context.attempt_nonce())
        .with_placement_fencing_token(placement_fence),
    )
    .await
}
