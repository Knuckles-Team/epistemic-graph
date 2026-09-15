use super::*;
/// The caller/isolation-scoping fields shared by every `dispatch_graph_op_inner`
/// entry point, bundled so the function stays under the clippy argument-count
/// ceiling once the feature-gated authority parameters are unified in.
pub(super) struct GraphOpContext<'a> {
    pub(super) graph_name: &'a str,
    pub(super) req_id: u64,
    pub(super) caller: Option<&'a str>,
    pub(super) verified_context: &'a VerifiedRequestContext,
}

/// Fence a `series.redb` WRITE to the current placement leader.
///
/// `Some(response)` is the stale-route rejection the caller must return; `None`
/// means the fence passed and dispatch CONTINUES — the original inline form fell
/// through, so an unconditional `return` here would strand every `TsAppend`.
/// The method guard lives inside so the call site stays a single `if let`.
#[cfg(all(feature = "raft", feature = "tsdb"))]
pub(super) async fn dispatch_op_tsdb_write_fence(
    req_id: u64,
    graph_name: &str,
    routed_raft: Option<&crate::raft::multi::RoutedRaftHandle>,
    method: &Method,
) -> Option<Response> {
    if !matches!(
        method,
        Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. }
    ) {
        return None;
    }
    let routed = routed_raft?;
    let leader = routed.handle.current_leader().await;
    if leader == Some(routed.handle.node_id) {
        return None;
    }
    Some(Response::stale_route(
        req_id,
        graph_name,
        routed.group_id,
        routed.epoch,
        leader,
        "time-series writes require the current placement leader",
    ))
}

/// Everything [`dispatch_op_knowledge_stream`] needs beyond the method itself:
/// the resolved request identity, the graph core it pulls from, and the
/// placement/RLS/authority handles the cursor must stay inside. Bundled to keep
/// the dispatcher at the documented parameter cap.
#[cfg(feature = "knowledge-batch")]
pub(super) struct KnowledgeStreamCtx<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) verified_context: &'a VerifiedRequestContext,
    pub(super) read_authority: &'a Option<GraphReadAuthority>,
    pub(super) verified_actor: &'a str,
    pub(super) core: Arc<crate::graph::GraphCore>,
    #[cfg(feature = "security")]
    pub(super) rls: std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(feature = "raft")]
    pub(super) routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    pub(super) knowledge_stream_authority:
        Option<handlers::knowledge_stream::KnowledgeStreamAuthority>,
}

#[cfg(feature = "knowledge-batch")]
pub(super) async fn dispatch_op_knowledge_stream(
    ctx: KnowledgeStreamCtx<'_>,
    method: Method,
) -> Response {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let verified_context = ctx.verified_context;
    let read_authority = ctx.read_authority;
    let verified_actor = ctx.verified_actor;
    let core = ctx.core;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    let knowledge_stream_authority = ctx.knowledge_stream_authority;
    let Some(authority) = knowledge_stream_authority.as_ref() else {
        return Response::err(
            req_id,
            "KnowledgeStream authority was not derived from verified context",
        );
    };
    let carrier = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    #[cfg(feature = "raft")]
    let (stream_placement_epoch, stream_fencing_token) = if let Some(routed) = routed_raft.as_ref()
    {
        (routed.epoch, Some(routed.group_id))
    } else {
        (0, None)
    };
    #[cfg(not(feature = "raft"))]
    let (stream_placement_epoch, stream_fencing_token) = (0, None);
    let handler_ctx = handlers::knowledge_stream::KnowledgeStreamHandlerCtx {
        state,
        req_id,
        graph_name,
        core: core.clone(),
        caller: verified_actor,
        carrier: &carrier,
        authority,
        placement_epoch: stream_placement_epoch,
        fencing_token: stream_fencing_token,
        read_authority: read_authority
            .as_ref()
            .expect("KnowledgeStream is classified as a graph read"),
        #[cfg(feature = "security")]
        rls: &rls,
    };
    return match handlers::knowledge_stream::try_handle(handler_ctx, method).await {
        Ok(response) => response,
        Err(_) => Response::err(req_id, "KnowledgeStream dispatch routing error"),
    };
}

#[cfg(feature = "tsdb")]
pub(super) async fn dispatch_op_tsdb_ops(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    verified_context: &VerifiedRequestContext,
    ts_placement_epoch: u64,
    ts_fencing_token: Option<u64>,
    method: Method,
) -> Response {
    let carrier = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    return match handlers::timeseries::try_handle_with_nonce(
        state,
        req_id,
        &carrier,
        verified_context.attempt_nonce(),
        handlers::timeseries::SeriesPlacement {
            graph: graph_name,
            placement_epoch: ts_placement_epoch,
            fencing_token: ts_fencing_token,
        },
        method,
    )
    .await
    {
        Ok(resp) => resp,
        Err(_) => Response::err(req_id, "timeseries dispatch routing error"),
    };
}

#[cfg(feature = "security")]
pub(super) async fn dispatch_op_audit_verify(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
) -> Response {
    let fname = crate::persist::sanitize(graph_name);
    match persistence.as_ref().and_then(|p| p.as_redb()) {
        Some(redb) => match redb.audit_verify_blocking(&fname) {
            Ok(report) => Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::security::AuditVerify>(report),
            ),
            Err(e) => Response::err(req_id, format!("AuditVerify error: {e}")),
        },
        None => Response::err(
            req_id,
            "AuditVerify requires a durable redb backend (no persist dir configured)".to_string(),
        ),
    }
}

#[cfg(feature = "security")]
pub(super) async fn prove_audit_inclusion(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    node_id: String,
    anchor_seq: Option<u64>,
) -> Response {
    let fname = crate::persist::sanitize(graph_name);
    match persistence.as_ref().and_then(|p| p.as_redb()) {
        Some(redb) => match redb.audit_prove_inclusion_blocking(&fname, &node_id, anchor_seq) {
            Ok(report) => Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::security::AuditProveInclusion>(
                    report,
                ),
            ),
            Err(e) => Response::err(req_id, format!("AuditProveInclusion error: {e}")),
        },
        None => Response::err(
            req_id,
            "AuditProveInclusion requires a durable redb backend (no persist dir configured)"
                .to_string(),
        ),
    }
}

/// Everything [`apply_served_modality`] needs beyond the operation: the
/// resolved request identity, the gateway authz capture, the graph core and its
/// durability/CDC/materialization handles, plus the placement route and the
/// derived modality authority. Bundled to keep the dispatcher at the documented
/// parameter cap.
#[cfg(feature = "modality-serving")]
pub(super) struct ServedModalityCtx<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) caller: Option<&'a str>,
    pub(super) verified_context: &'a VerifiedRequestContext,
    pub(super) tenant_scope: &'a str,
    pub(super) gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    pub(super) core: Arc<crate::graph::GraphCore>,
    pub(super) materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    pub(super) persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    pub(super) cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    #[cfg(feature = "raft")]
    pub(super) routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    pub(super) modality_authority: Option<handlers::modality::ModalityAuthority>,
}

#[cfg(feature = "modality-serving")]
pub(super) async fn apply_served_modality(
    ctx: ServedModalityCtx<'_>,
    op: eg_types::ServedModalityOp,
) -> Response {
    if op.mutates() && ctx.persistence.is_none() {
        return Response::err(
            ctx.req_id,
            "served modality operations require authoritative redb persistence",
        );
    }
    let authority = match ctx.modality_authority.as_ref() {
        Some(authority) => authority.clone(),
        None => {
            return Response::err(
                ctx.req_id,
                "served modality authority was not derived from verified context",
            )
        }
    };
    let method = Method::ServedModality { op: op.clone() };
    #[cfg(feature = "raft")]
    if let Some(response) = try_replicate_served_modality(&ctx, &op, &method, &authority).await {
        return response;
    }
    commit_served_modality(ctx, method, op, authority).await
}

#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn try_replicate_served_modality(
    ctx: &ServedModalityCtx<'_>,
    op: &eg_types::ServedModalityOp,
    method: &Method,
    authority: &handlers::modality::ModalityAuthority,
) -> Option<Response> {
    if !op.mutates() {
        return None;
    }
    let backend = ctx
        .persistence
        .as_ref()
        .expect("mutating ServedModality requires persistence");
    let principal_fingerprint = ctx.verified_context.principal_persistence_id();
    let routed = ctx.routed_raft.as_ref()?;
    Some(
        replicate_served_modality(ModalityReplicationRequest {
            state: ctx.state,
            handle: routed.handle.clone(),
            group_id: routed.group_id,
            placement_epoch: routed.epoch,
            fencing_token: Some(routed.group_id),
            graph_name: ctx.graph_name,
            graph_type: ctx
                .gateway_authz_ctx
                .as_ref()
                .expect("ServedModality must be registered in the mutation gateway")
                .1,
            req_id: ctx.req_id,
            attempt_nonce: ctx.verified_context.attempt_nonce(),
            idempotency_key: ctx.verified_context.idempotency_key(),
            tenant_scope: ctx.tenant_scope,
            principal_fingerprint: &principal_fingerprint,
            core: &ctx.core,
            persistence: backend,
            method: method.clone(),
            authority,
        })
        .await,
    )
}

#[cfg(feature = "modality-serving")]
async fn commit_served_modality(
    ctx: ServedModalityCtx<'_>,
    method: Method,
    op: eg_types::ServedModalityOp,
    authority: handlers::modality::ModalityAuthority,
) -> Response {
    let ServedModalityCtx {
        req_id,
        caller,
        graph_name,
        verified_context,
        tenant_scope,
        gateway_authz_ctx,
        core,
        materialization_manifest,
        persistence,
        #[cfg(feature = "streaming")]
        cdc,
        ..
    } = ctx;
    let (isolation, graph_type, owner) = gateway_authz_ctx
        .as_ref()
        .expect("ServedModality must be registered in the mutation gateway");
    let plan = crate::server::mutation::MutationPlan::for_method(&method);
    let mutation_ctx = crate::server::mutation::MutationCtx {
        req_id,
        caller,
        attempt_nonce: verified_context.attempt_nonce(),
        idempotency_key: verified_context.idempotency_key(),
        tenant_scope,
        graph_name,
        graph_type: *graph_type,
        owner: owner.as_deref(),
        isolation,
        core: &core,
        persistence: persistence.as_ref(),
        #[cfg(feature = "streaming")]
        cdc: cdc.as_ref(),
        materialization_manifest: materialization_manifest.as_ref(),
        write_coalescer: None,
    };
    crate::server::mutation::commit_conditional_mutation(
        &mutation_ctx,
        &plan,
        &method,
        op.mutates(),
        move |staged_core| handlers::modality::handle(staged_core, &authority, op),
    )
    .await
}

/// The resolved request identity the Raft write-routing barrier replays into
/// the consensus entry. Bundled to keep the barrier at the documented parameter
/// cap; `routed` and `method` stay explicit because the caller's
/// `if let Some(routed)` is what proves the barrier applies at all.
#[cfg(feature = "raft")]
pub(super) struct RaftWriteBarrierCtx<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) verified_context: &'a VerifiedRequestContext,
    pub(super) tenant_scope: &'a str,
    pub(super) graph_type: crate::protocol::GraphType,
}

/// Stable replay identity for a replicated graph mutation.
///
/// The inner digest binds both names carried by `RaftRequest`: the logical
/// protocol name and the sanitized physical storage key. The outer digest adds
/// authenticated tenant, principal, and caller idempotency identity. Transport
/// request ids, attempt nonces, and operation bytes are deliberately absent so
/// a retry reaches the same durable row; the replicated command authentication
/// then distinguishes an exact replay from a changed-operation conflict.
#[cfg(feature = "raft")]
pub(super) fn replicated_graph_batch_id(
    namespace: &str,
    tenant_scope: &str,
    graph_name: &str,
    graph_fname: &str,
    principal_fingerprint: &str,
    idempotency_key: &str,
) -> String {
    let graph_scope = crate::server::mutation_batch::opaque_idempotency_key_for_context(
        "raft-graph-scope",
        tenant_scope,
        graph_name,
        None,
        graph_fname,
    );
    crate::server::mutation_batch::opaque_idempotency_key_for_context(
        namespace,
        tenant_scope,
        &graph_scope,
        Some(principal_fingerprint),
        idempotency_key,
    )
}

/// Replicate one durable mutation through Raft consensus instead of applying it
/// locally.
///
/// Takes the ALREADY-UNWRAPPED [`RoutedRaftHandle`]: the barrier is only reachable
/// with a routed group, and the caller's `if let Some(routed)` is that proof. The
/// durability guard stays at the call site because a non-durable mutation must keep
/// ownership of `method` for the local pipeline below.
#[cfg(feature = "raft")]
pub(super) async fn dispatch_op_raft_write_routing_barrier(
    ctx: RaftWriteBarrierCtx<'_>,
    routed: crate::raft::multi::RoutedRaftHandle,
    method: Method,
) -> Response {
    let RaftWriteBarrierCtx {
        state,
        req_id,
        graph_name,
        verified_context,
        tenant_scope,
        graph_type,
    } = ctx;
    let created_at_ms = authoritative_now_ms();
    let graph_fname = crate::persist::sanitize(graph_name);
    let principal_fingerprint = verified_context.principal_persistence_id();
    let batch_id = replicated_graph_batch_id(
        "raft-rpc",
        tenant_scope,
        graph_name,
        &graph_fname,
        &principal_fingerprint,
        verified_context.idempotency_key(),
    );
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        req_id,
        verified_context.attempt_nonce(),
        tenant_scope,
        principal_fingerprint,
        false,
        routed.epoch,
        crate::raft::RaftMutationTiming {
            fencing_token: Some(routed.group_id),
            created_at_ms,
        },
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    let server_secret = timed_read(state).await.auth_secret.clone();
    let req = match build_bound_graph_request(BoundGraphRequest {
        graph_name,
        graph_fname,
        graph_type,
        committed_at_ms: created_at_ms,
        mutation,
        method,
        server_secret: &server_secret,
        group_id: routed.group_id,
    }) {
        Ok(request) => request,
        Err(error) => return Response::err(req_id, error),
    };
    match routed.handle.client_write(req).await {
        Ok(response) => {
            if let Some(error) = response.native_error {
                Response::err(req_id, error)
            } else {
                Response::ok(
                    req_id,
                    ResultPayload::Json(serde_json::json!({
                        "replicated": true,
                        "group": routed.group_id,
                        "epoch": routed.epoch,
                        "fencing_token": routed.fencing_token(),
                    })),
                )
            }
        }
        Err(e) => {
            let leader = routed.handle.current_leader().await;
            Response::stale_route(req_id, graph_name, routed.group_id, routed.epoch, leader, e)
        }
    }
}

#[cfg(feature = "raft")]
struct BoundGraphRequest<'a> {
    graph_name: &'a str,
    graph_fname: String,
    graph_type: crate::protocol::GraphType,
    committed_at_ms: u64,
    mutation: crate::raft::RaftMutationContext,
    method: Method,
    server_secret: &'a str,
    group_id: crate::raft::GroupId,
}

#[cfg(feature = "raft")]
fn build_bound_graph_request(
    input: BoundGraphRequest<'_>,
) -> Result<crate::raft::RaftRequest, String> {
    let command = crate::raft::ReplicatedMutation::caller_graph(input.method, input.server_secret)?;
    let mut request = crate::raft::RaftRequest {
        graph_fname: input.graph_fname,
        graph_name: input.graph_name.to_string(),
        graph_type: input.graph_type,
        committed_at_ms: input.committed_at_ms,
        mutation: input.mutation,
        command,
    };
    request.bind_graph_command(input.server_secret, input.group_id)?;
    Ok(request)
}

#[cfg(feature = "raft")]
pub(super) async fn resolve_routed_raft(
    req_id: u64,
    graph_name: &str,
    multi_raft: Option<&std::sync::Arc<crate::raft::multi::MultiRaft>>,
) -> Result<Option<crate::raft::multi::RoutedRaftHandle>, Response> {
    let Some(multi) = multi_raft else {
        return Ok(None);
    };
    match multi.handle_for_graph(graph_name).await {
        Some(routed) => Ok(Some(routed)),
        None => {
            let route = multi.route_graph(graph_name).await;
            Err(Response::stale_route(
                req_id,
                graph_name,
                route.group,
                route.epoch,
                None,
                "authoritative placement group is not running on this node",
            ))
        }
    }
}
