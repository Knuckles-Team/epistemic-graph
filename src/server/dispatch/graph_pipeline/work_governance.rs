use super::*;

/// A native durable read must run on the CURRENT placement leader and behind a
/// read barrier, or a follower would answer from its own stale log. `stale_hint`
/// and `barrier_failure` name the surface in the two refusals so each caller's
/// message is unchanged.
#[cfg(feature = "raft")]
async fn enforce_native_read_leadership(
    req_id: u64,
    graph_name: &str,
    multi_raft: Option<&std::sync::Arc<crate::raft::multi::MultiRaft>>,
    routed_raft: Option<&crate::raft::multi::RoutedRaftHandle>,
    stale_hint: &str,
    barrier_failure: &str,
) -> Result<(), Response> {
    let Some(routed) = routed_raft else {
        return Ok(());
    };
    let leader = routed.handle.current_leader().await;
    if leader != Some(routed.handle.node_id) {
        return Err(Response::stale_route(
            req_id,
            graph_name,
            routed.group_id,
            routed.epoch,
            leader,
            stale_hint,
        ));
    }
    let Some(multi) = multi_raft else {
        return Ok(());
    };
    match multi.read_barrier_group(routed.group_id).await {
        Ok(_) => Ok(()),
        Err(error) => Err(Response::err(
            req_id,
            format!("{barrier_failure}: {error:?}"),
        )),
    }
}

pub(super) async fn dispatch_op_resource_reservation_query(
    req_id: u64,
    graph_name: &str,
    verified_context: &VerifiedRequestContext,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "raft")] multi_raft: Option<std::sync::Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "raft")] routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    method: Method,
) -> Response {
    #[cfg(feature = "raft")]
    if let Err(response) = enforce_native_read_leadership(
        req_id,
        graph_name,
        multi_raft.as_ref(),
        routed_raft.as_ref(),
        "native reservation reads require the current placement leader",
        "native reservation read linearizability barrier failed",
    )
    .await
    {
        return response;
    }
    let Some(backend) = persistence.as_ref() else {
        return Response::err(req_id, "native reservation persistence is unavailable");
    };
    let fname = crate::persist::sanitize(graph_name);
    match &method {
        crate::protocol::Method::QueryWorkItemReservation { request } => {
            match backend.read_resource_reservation(&fname, request).await {
                Ok(result) => Response::ok(
                    req_id,
                    ResultPayload::of::<
                        eg_types::result_contract::coordination::QueryWorkItemReservation,
                    >(result),
                ),
                Err(error) => {
                    Response::err(req_id, format!("native reservation read failed: {error}"))
                }
            }
        }
        crate::protocol::Method::ResourceReservationStatus { request } => {
            let aggregate_reader = verified_context.allows_action("resource:read:aggregate")
                || verified_context.allows_action("kg:admin");
            match backend
                .read_resource_reservation_status(&fname, request)
                .await
            {
                Ok(result) => Response::ok(
                    req_id,
                    redact_resource_status_result(result, request, aggregate_reader),
                ),
                Err(error) => Response::err(
                    req_id,
                    format!("native reservation status read failed: {error}"),
                ),
            }
        }
        _ => unreachable!("resource query classifier and dispatch method diverged"),
    }
}

/// The authenticated, placement-resolved routing context the native durable-op
/// dispatchers below share (capacity leases, WorkItem claim capability). These
/// values are resolved once per request in `dispatch_graph_op` and travel
/// together; bundling them keeps each dispatcher's parameter list at the
/// documented cap, the same shape as `crate::server::mutation::MutationCtx`.
pub(super) struct NativeOpCtx<'a> {
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) verified_context: &'a VerifiedRequestContext,
    pub(super) persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "raft")]
    pub(super) multi_raft: Option<std::sync::Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "raft")]
    pub(super) routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
}

/// A capacity lease may only be taken, renewed or released by the principal
/// that owns it; every other capacity method is owner-agnostic.
fn capacity_owner_matches(method: &Method, verified_context: &VerifiedRequestContext) -> bool {
    let verified_owner = verified_context.principal_persistence_id();
    match method {
        Method::AcquireCapacity { request } => request.owner_digest == verified_owner,
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            request.owner_digest == verified_owner
        }
        _ => true,
    }
}

/// The declared result a capacity-ledger commit receipt is served as.
fn capacity_commit_result(method: &Method) -> fn(&[u8]) -> Result<ResultPayload, String> {
    match method {
        Method::AcquireCapacity { .. } => {
            ResultPayload::of_receipt::<eg_types::result_contract::coordination::AcquireCapacity>
        }
        Method::RenewCapacity { .. } => {
            ResultPayload::of_receipt::<eg_types::result_contract::coordination::RenewCapacity>
        }
        Method::ReleaseCapacity { .. } => {
            ResultPayload::of_receipt::<eg_types::result_contract::coordination::ReleaseCapacity>
        }
        Method::ReclaimExpiredCapacity { .. } => {
            ResultPayload::of_receipt::<
                eg_types::result_contract::coordination::ReclaimExpiredCapacity,
            >
        }
        Method::UpdateCapacityCell { .. } => {
            ResultPayload::of_receipt::<eg_types::result_contract::coordination::UpdateCapacityCell>
        }
        _ => |_| Err("capacity commit receipt for a non-capacity method".to_string()),
    }
}

pub(super) async fn dispatch_op_capacity_ops(
    ctx: NativeOpCtx<'_>,
    state_machine_authorized: bool,
    method: Method,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let verified_context = ctx.verified_context;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let multi_raft = ctx.multi_raft;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    if let Err(response) = enforce_native_read_leadership(
        req_id,
        graph_name,
        multi_raft.as_ref(),
        routed_raft.as_ref(),
        "native capacity operations require the current placement leader",
        "native capacity linearizability barrier failed",
    )
    .await
    {
        return response;
    }
    let Some(backend) = persistence.as_ref() else {
        return Response::err(req_id, "native capacity persistence is unavailable");
    };
    if !backend.supports_native_capacity_leases() {
        return Response::err(req_id, "native capacity persistence is unavailable");
    }
    if !state_machine_authorized && !capacity_owner_matches(&method, verified_context) {
        return Response::err(req_id, "ACCESS_DENIED: capacity owner digest mismatch");
    }
    let fname = crate::persist::sanitize(graph_name);
    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    match method {
        Method::CapacityStatus { ref request } | Method::ReconcileCapacity { ref request } => {
            capacity_status_response(
                backend,
                &fname,
                request,
                capacity_status_result(&method),
                req_id,
            )
            .await
        }
        method @ (Method::AcquireCapacity { .. }
        | Method::RenewCapacity { .. }
        | Method::ReleaseCapacity { .. }
        | Method::ReclaimExpiredCapacity { .. }
        | Method::UpdateCapacityCell { .. }) => {
            capacity_commit_response(backend, &fname, method, req_id).await
        }
        _ => unreachable!("capacity classifier and dispatch diverged"),
    }
}

/// The declared result a capacity status read answers `method` with.
fn capacity_status_result(
    method: &Method,
) -> fn(eg_types::native_control::CapacityStatusResult) -> Result<ResultPayload, String> {
    match method {
        Method::ReconcileCapacity { .. } => {
            ResultPayload::of::<eg_types::result_contract::coordination::ReconcileCapacity>
        }
        _ => ResultPayload::of::<eg_types::result_contract::coordination::CapacityStatus>,
    }
}

async fn capacity_status_response(
    backend: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    fname: &str,
    request: &eg_types::native_control::CapacityStatusRequest,
    declared: fn(eg_types::native_control::CapacityStatusResult) -> Result<ResultPayload, String>,
    req_id: u64,
) -> Response {
    backend
        .read_capacity_status(fname, request)
        .await
        .map(|result| Response::ok(req_id, declared(result)))
        .unwrap_or_else(|error| {
            Response::err(
                req_id,
                format!("native capacity status read failed: {error}"),
            )
        })
}

async fn capacity_commit_response(
    backend: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    fname: &str,
    method: Method,
    req_id: u64,
) -> Response {
    let declared = capacity_commit_result(&method);
    backend
        .commit_capacity_lease(fname, method)
        .await
        .map(|receipt| Response::ok(req_id, declared(&receipt)))
        .unwrap_or_else(|error| {
            Response::err(req_id, format!("native capacity commit failed: {error}"))
        })
}

pub(super) async fn dispatch_op_workitem_claim_capability(
    ctx: NativeOpCtx<'_>,
    #[cfg(feature = "redb")] graph_incarnation_id: String,
    method: Method,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let verified_context = ctx.verified_context;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let multi_raft = ctx.multi_raft;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    if is_replicated_apply() {
        // A replicated apply has only the bounded Raft routing context;
        // capability authority requires the original cryptographically
        // verified principal and session envelope.
        return Response::err(
            req_id,
            crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE,
        );
    }
    #[cfg(feature = "raft")]
    if let Some(routed) = routed_raft.as_ref() {
        let leader = routed.handle.current_leader().await;
        if leader != Some(routed.handle.node_id) {
            return Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "WorkItem claim capabilities require the current placement leader",
            );
        }
        if let Some(multi) = multi_raft.as_ref() {
            if let Err(error) = multi.read_barrier_group(routed.group_id).await {
                return Response::err(
                    req_id,
                    format!("WorkItem claim capability linearizability barrier failed: {error:?}"),
                );
            }
        }
    }
    let Some(backend) = persistence.as_ref() else {
        return Response::err(
            req_id,
            "native WorkItem claim capability persistence is unavailable",
        );
    };
    #[cfg(feature = "redb")]
    {
        let Some(redb) = backend.as_redb() else {
            return Response::err(
                req_id,
                "native WorkItem claim capability persistence is unavailable",
            );
        };
        let authority = crate::redb_store::work_item_capability::AuthenticatedAuthority {
            tenant: verified_context.tenant().to_string(),
            audience: verified_context.claims().audience.clone(),
            principal: verified_context.principal_persistence_id(),
            agent_id: verified_context.agent_id().to_string(),
            session: verified_context.idempotency_key().to_string(),
            authority_epoch: work_item_capability_authority_epoch(&graph_incarnation_id),
            incarnation_id: graph_incarnation_id.clone(),
            now_ms: authoritative_now_ms(),
        };
        let fname = crate::persist::sanitize(graph_name);
        let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
        match method {
            Method::MintWorkItemClaimCapability { request } => redb
                .mint_work_item_claim_capability(&fname, request, authority)
                .await
                .map(|result| {
                    Response::ok(
                        req_id,
                        ResultPayload::of::<
                            eg_types::result_contract::coordination::MintWorkItemClaimCapability,
                        >(result),
                    )
                })
                .unwrap_or_else(|error| {
                    Response::err(
                        req_id,
                        format!("WorkItem claim capability mint failed: {error}"),
                    )
                }),
            Method::VerifyWorkItemClaimCapability { request } => redb
                .verify_work_item_claim_capability(&fname, request, authority)
                .await
                .map(|result| {
                    Response::ok(
                        req_id,
                        ResultPayload::of::<
                            eg_types::result_contract::coordination::VerifyWorkItemClaimCapability,
                        >(result),
                    )
                })
                .unwrap_or_else(|error| {
                    Response::err(
                        req_id,
                        format!("WorkItem claim capability verify failed: {error}"),
                    )
                }),
            _ => unreachable!("capability classifier and dispatch diverged"),
        }
    }
    #[cfg(not(feature = "redb"))]
    {
        // `graph_name`/`verified_context`/`method` are consumed only by the `redb`
        // (and, for `graph_name`, `raft`) arms above, so a build with neither leaves
        // them bound but unread. Discharged the same way `backend` already was --
        // the author's own idiom three lines up -- rather than with
        // `#[allow(unused_variables)]`, which would also hide a genuinely dead binding.
        let _ = (backend, graph_name, verified_context, method);
        Response::err(
            req_id,
            "native WorkItem claim capability requires redb persistence",
        )
    }
}

/// Submission and resource-reservation methods, under the same already-authorized
/// graph and placement context the WorkItem lifecycle handler receives.
pub(super) async fn dispatch_op_workitem_submission_or_resources(
    ctx: crate::server::handlers::work_item::HandleContext<'_>,
    method: Method,
) -> Response {
    let crate::server::handlers::work_item::HandleContext {
        req_id,
        graph_name,
        caller,
        verified_context,
        core,
        persistence,
        #[cfg(feature = "raft")]
        routed_raft,
    } = ctx;
    #[cfg(feature = "raft")]
    let (placement_epoch, placement_fence) = if let Some(routed) = routed_raft.as_ref() {
        let leader = routed.handle.current_leader().await;
        if leader != Some(routed.handle.node_id) {
            return Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "WorkItem transitions require the current placement leader",
            );
        }
        (routed.epoch, Some(routed.group_id))
    } else {
        (0, None)
    };
    #[cfg(not(feature = "raft"))]
    let (placement_epoch, placement_fence) = (0, None);

    return match crate::server::mutation_batch::commit_work_item(
        crate::server::mutation_batch::WorkItemCommitRequest::new(
            persistence.as_ref(),
            &core,
            req_id,
            Some(verified_context.idempotency_key()),
            caller,
            graph_name,
            placement_epoch,
            method,
        )
        .with_attempt_nonce(verified_context.attempt_nonce())
        .with_placement_fencing_token(placement_fence),
    )
    .await
    {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, format!("WorkItem mutation failed: {error}")),
    };
}
