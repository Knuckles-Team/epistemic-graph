use super::*;
struct GraphDispatchCapture<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    graph_name: &'a str,
    req_id: u64,
    caller: Option<&'a str>,
    verified_context: &'a VerifiedRequestContext,
    state_machine_authorized: bool,
    access: AccessLevel,
    read_authority: Option<GraphReadAuthority>,
    verified_actor: String,
    tenant_scope: String,
    gateway_authz_ctx: Option<crate::server::mutation::GatewayAuthzCtx>,
    core: Arc<crate::graph::GraphCore>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    routed_write_coalescer:
        Arc<crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry>,
    #[cfg(feature = "raft")]
    placement_authority: crate::server::state::PlacementAuthorityKind,
    #[cfg(feature = "raft")]
    multi_raft: Option<Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "raft")]
    graph_type: crate::protocol::GraphType,
    #[cfg(feature = "redb")]
    graph_incarnation_id: String,
    #[cfg(feature = "redb")]
    cold_tracker: Arc<crate::server::persistence::cold_offload::ColdTenantTracker>,
    #[cfg(feature = "security")]
    rls: std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    tsdb_store: Option<Arc<eg_tsdb::store::SeriesStore>>,
}

async fn capture_graph_dispatch<'a>(
    state: &'a Arc<RwLock<ServerState>>,
    ctx: GraphOpContext<'a>,
    method: &Method,
) -> Result<GraphDispatchCapture<'a>, Response> {
    let GraphOpContext {
        graph_name,
        req_id,
        caller,
        verified_context,
    } = ctx;
    #[cfg(feature = "cypher")]
    if let Err(error) = handlers::query::validate_cypher_mode(method) {
        return Err(Response::err(req_id, error));
    }
    #[cfg(feature = "redb")]
    let mut s = timed_read(state).await;
    #[cfg(not(feature = "redb"))]
    let s = timed_read(state).await;
    #[cfg(feature = "redb")]
    if s.registry.get(graph_name).is_none() {
        drop(s);
        lazy_open_graph(state, graph_name).await;
        s = timed_read(state).await;
    }

    let access = graph_op_access_level(method);
    #[cfg(feature = "raft")]
    let state_machine_authorized = is_replicated_apply();
    #[cfg(not(feature = "raft"))]
    let state_machine_authorized = false;
    let gate = gate_graph_op_under_lock(
        &s,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        method,
        access,
        state_machine_authorized,
    )?;
    let GraphOpGate {
        entry,
        read_authority,
        tenant_scope,
        verified_actor,
        gateway_authz_ctx,
    } = gate;
    let capture = GraphDispatchCapture {
        state,
        graph_name,
        req_id,
        caller,
        verified_context,
        state_machine_authorized,
        access,
        read_authority: Some(read_authority),
        verified_actor,
        tenant_scope,
        gateway_authz_ctx,
        core: entry.core.clone(),
        #[cfg(feature = "redb")]
        graph_incarnation_id: entry.incarnation_id.clone(),
        materialization_manifest: s.registry.materialization_handle(graph_name),
        persistence: s.persistence.clone(),
        #[cfg(feature = "streaming")]
        cdc: s.cdc.clone(),
        routed_write_coalescer: s.routed_write_coalescer.clone(),
        #[cfg(feature = "raft")]
        placement_authority: s.placement_authority(),
        #[cfg(feature = "raft")]
        multi_raft: resolve_multi_raft(&s, &s.placement_authority()),
        #[cfg(feature = "raft")]
        graph_type: entry.graph_type,
        #[cfg(feature = "redb")]
        cold_tracker: s.cold_tracker.clone(),
        #[cfg(feature = "security")]
        rls: std::sync::Arc::new(s.isolation.clone()),
        #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
        tsdb_store: s.tsdb_store.clone(),
    };
    drop(s);
    Ok(capture)
}

pub(super) async fn dispatch_graph_op_inner(
    state: &Arc<RwLock<ServerState>>,
    ctx: GraphOpContext<'_>,
    mut method: Method,
    #[cfg(feature = "modality-serving")] modality_authority: Option<
        handlers::modality::ModalityAuthority,
    >,
    #[cfg(feature = "knowledge-batch")] knowledge_stream_authority: Option<
        handlers::knowledge_stream::KnowledgeStreamAuthority,
    >,
) -> Response {
    let capture = match capture_graph_dispatch(state, ctx, &method).await {
        Ok(capture) => capture,
        Err(response) => return response,
    };
    #[cfg(feature = "raft")]
    if let Some(error) = capture.placement_authority.missing_error() {
        return Response::err(capture.req_id, error);
    }
    #[cfg(feature = "redb")]
    capture
        .cold_tracker
        .touch_with_incarnation(capture.graph_name, &capture.graph_incarnation_id);
    #[cfg(feature = "raft")]
    let routed_raft = match resolve_routed_raft(
        capture.req_id,
        capture.graph_name,
        capture.multi_raft.as_ref(),
    )
    .await
    {
        Ok(routed_raft) => routed_raft,
        Err(resp) => return resp,
    };
    stamp_resource_and_capacity_timestamps(&mut method);

    let routing = GraphOpRouting {
        state: capture.state,
        req_id: capture.req_id,
        graph_name: capture.graph_name,
        caller: capture.caller,
        verified_context: capture.verified_context,
        state_machine_authorized: capture.state_machine_authorized,
        read_authority: &capture.read_authority,
        verified_actor: &capture.verified_actor,
        tenant_scope: &capture.tenant_scope,
        gateway_authz_ctx: &capture.gateway_authz_ctx,
        core: &capture.core,
        materialization_manifest: &capture.materialization_manifest,
        persistence: &capture.persistence,
        #[cfg(feature = "streaming")]
        cdc: &capture.cdc,
        #[cfg(feature = "security")]
        rls: &capture.rls,
        #[cfg(feature = "raft")]
        routed_raft: &routed_raft,
        #[cfg(feature = "raft")]
        graph_type: capture.graph_type,
        #[cfg(feature = "raft")]
        multi_raft: &capture.multi_raft,
        #[cfg(feature = "redb")]
        graph_incarnation_id: &capture.graph_incarnation_id,
        #[cfg(feature = "modality-serving")]
        modality_authority: &modality_authority,
        #[cfg(feature = "knowledge-batch")]
        knowledge_stream_authority: &knowledge_stream_authority,
    };
    let method = match route_graph_op_method(routing, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };

    crate::metrics::graph_op(capture.graph_name);
    let response = run_dispatch_pipeline(
        DispatchPipelineCtx {
            state: capture.state,
            req_id: capture.req_id,
            graph_name: capture.graph_name,
            caller: capture.caller,
            attempt_nonce: capture.verified_context.attempt_nonce(),
            idempotency_key: capture.verified_context.idempotency_key(),
            read_authority: capture.read_authority.clone(),
            verified_actor: &capture.verified_actor,
            tenant_id: capture.verified_context.tenant().to_string(),
            tenant_scope: capture.tenant_scope.clone(),
            gateway_authz_ctx: capture.gateway_authz_ctx.clone(),
            core: capture.core.clone(),
            materialization_manifest: capture.materialization_manifest.clone(),
            persistence: capture.persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: capture.cdc.clone(),
            routed_write_coalescer: capture.routed_write_coalescer.clone(),
            #[cfg(feature = "security")]
            rls: capture.rls.clone(),
            #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
            tsdb_store: capture.tsdb_store.clone(),
        },
        method,
    )
    .await;
    finalize_graph_op_response(
        capture.state,
        capture.graph_name,
        &capture.core,
        capture.access,
        capture.gateway_authz_ctx.is_some(),
        response,
    )
    .await
}

/// Everything the post-lock routers below need, snapshotted out of the registry
/// lock by `dispatch_graph_op_inner`. Every field is a shared reference or a
/// `Copy` scalar, so this is `Copy` and each router call is free. `read_authority`
/// and `verified_actor` are separate borrows of the CALLER's locals — the actor
/// string borrows from the authority, so the two cannot live in one owned struct.
#[derive(Clone, Copy)]
pub(in crate::server::dispatch) struct GraphOpRouting<'a> {
    pub(in crate::server::dispatch) state: &'a Arc<RwLock<ServerState>>,
    pub(in crate::server::dispatch) req_id: u64,
    pub(in crate::server::dispatch) graph_name: &'a str,
    pub(super) caller: Option<&'a str>,
    pub(super) verified_context: &'a VerifiedRequestContext,
    pub(super) state_machine_authorized: bool,
    pub(super) read_authority: &'a Option<GraphReadAuthority>,
    pub(super) verified_actor: &'a str,
    pub(in crate::server::dispatch) tenant_scope: &'a str,
    pub(super) gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    pub(in crate::server::dispatch) core: &'a Arc<crate::graph::GraphCore>,
    pub(super) materialization_manifest:
        &'a Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    pub(in crate::server::dispatch) persistence:
        &'a Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    pub(super) cdc: &'a Option<Arc<crate::server::cdc::CdcHub>>,
    #[cfg(feature = "security")]
    pub(super) rls: &'a std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(feature = "raft")]
    pub(in crate::server::dispatch) routed_raft: &'a Option<crate::raft::multi::RoutedRaftHandle>,
    #[cfg(feature = "raft")]
    pub(in crate::server::dispatch) graph_type: crate::protocol::GraphType,
    #[cfg(feature = "raft")]
    pub(super) multi_raft: &'a Option<Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "redb")]
    pub(super) graph_incarnation_id: &'a String,
    #[cfg(feature = "modality-serving")]
    pub(super) modality_authority: &'a Option<handlers::modality::ModalityAuthority>,
    #[cfg(feature = "knowledge-batch")]
    pub(super) knowledge_stream_authority:
        &'a Option<handlers::knowledge_stream::KnowledgeStreamAuthority>,
}
