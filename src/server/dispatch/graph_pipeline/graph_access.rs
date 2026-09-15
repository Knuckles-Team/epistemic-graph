use super::*;
pub(super) fn stamp_resource_reservation_timestamp(method: &mut Method) {
    if !crate::server::mutation_batch::is_resource_reservation_method(method) {
        return;
    }
    let now_ms = authoritative_now_ms();
    match method {
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => request.now_ms = now_ms,
        Method::QueryWorkItemReservation { request }
        | Method::ResourceReservationStatus { request } => request.now_ms = now_ms,
        Method::UpdateResourceHost { request } => request.now_ms = now_ms,
        _ => unreachable!("resource method classifier and timestamp binding diverged"),
    }
}

pub(super) fn stamp_capacity_timestamp(method: &mut Method) {
    if !crate::server::mutation_batch::is_capacity_method(method) {
        return;
    }
    let now_ms = authoritative_now_ms();
    match method {
        Method::AcquireCapacity { request } => request.now_ms = now_ms,
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            request.now_ms = now_ms
        }
        Method::ReclaimExpiredCapacity { request } => request.now_ms = now_ms,
        Method::UpdateCapacityCell { request } => request.now_ms = now_ms,
        Method::ReconcileCapacity { .. } | Method::CapacityStatus { .. } => {}
        _ => unreachable!("capacity method classifier and timestamp binding diverged"),
    }
}

pub(super) fn stamp_resource_and_capacity_timestamps(method: &mut Method) {
    stamp_resource_reservation_timestamp(method);
    stamp_capacity_timestamp(method);
}

/// Cold-path lazy open (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3).
/// Only a registry MISS escalates to a write lock. The graph may be
/// catalog-known but not yet materialized (a lazy-startup boot scan, or a graph
/// the bounded hot-context cache evicted back to catalog-only); `lazy_open` is a
/// no-op for a genuinely unknown name, so the caller's "not found" error is
/// unchanged for that case.
#[cfg(feature = "redb")]
pub(super) async fn lazy_open_graph(state: &Arc<RwLock<ServerState>>, graph_name: &str) {
    let cap = crate::server::persistence::cold_offload::max_resident_graphs();
    let page_size = crate::server::persistence::cold_offload::lazy_open_page_size();
    crate::server::persistence::cold_offload::lazy_open(state, graph_name, cap, page_size).await;
}

/// Universal served-data authority (CONCEPT:EG-P0-4): derive the row actor from
/// the cryptographically verified RequestContext while the authoritative
/// IsolationLayer is under the registry lock. Every graph read either consumes
/// this authority's detached projection or an existing handler that receives the
/// same IsolationLayer. Mutation documents can contain read phases (GraphQL
/// staged CONSTRUCT/UQL), so write requests carry the same verified tenant/actor
/// projection rather than gaining access to the raw committed core.
pub(super) fn resolve_graph_read_authority(
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    isolation: &crate::isolation::IsolationLayer,
) -> Result<(GraphReadAuthority, String), Response> {
    let authority = match GraphReadAuthority::from_verified(verified_context, isolation) {
        Ok(authority) => authority,
        Err(denied) => return Err(Response::err(req_id, denied)),
    };
    let tenant_scope = authority
        .carrier()
        .expect("GraphReadAuthority always carries verified tenant authority")
        .tenant_scope()
        .to_string();
    Ok((authority, tenant_scope))
}

/// Mutation-gateway authz context (CONCEPT:EG-P0-2): for a
/// `mutation::GATEWAY_ROUTED` method, `commit_mutation` re-derives its OWN authz
/// decision from `(isolation, graph_type, owner)` rather than trusting the graph
/// ACL check — captured before the registry lock drops, ONLY for the routed set
/// (an `IsolationLayer` clone is not free, so this is skipped entirely for the
/// other ~330 methods).
pub(super) fn graph_op_gateway_authz_ctx(
    s: &ServerState,
    method: &Method,
    entry: &GraphEntryFacts,
) -> Option<crate::server::mutation::GatewayAuthzCtx> {
    if !crate::server::mutation::is_gateway_routed(method) {
        return None;
    }
    Some((s.isolation.clone(), entry.graph_type, entry.owner.clone()))
}

/// Everything resolved while the registry read lock is still held.
pub(super) struct GraphOpGate {
    pub(super) entry: GraphEntryFacts,
    pub(super) read_authority: GraphReadAuthority,
    pub(super) tenant_scope: String,
    pub(super) verified_actor: String,
    pub(super) gateway_authz_ctx: Option<crate::server::mutation::GatewayAuthzCtx>,
}

/// Gate and resolve one graph operation under the registry lock, in the ORIGINAL
/// order: caller / existence / materialization / graph ACL first, then the
/// verified read authority, then the gateway authz context.
pub(super) fn gate_graph_op_under_lock(
    s: &ServerState,
    ctx: GraphOpContext<'_>,
    method: &Method,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<GraphOpGate, Response> {
    let GraphOpContext {
        graph_name,
        req_id,
        caller,
        verified_context,
    } = ctx;
    let entry = check_graph_op_access(
        s,
        req_id,
        caller,
        graph_name,
        access,
        state_machine_authorized,
    )?;
    let (read_authority, tenant_scope) =
        resolve_graph_read_authority(req_id, verified_context, &s.isolation)?;
    let verified_actor = match read_authority.verified_actor() {
        Ok(actor) => actor.to_string(),
        Err(denied) => return Err(Response::err(req_id, denied)),
    };
    let gateway_authz_ctx = graph_op_gateway_authz_ctx(s, method, &entry);
    Ok(GraphOpGate {
        entry,
        read_authority,
        tenant_scope,
        verified_actor,
        gateway_authz_ctx,
    })
}

pub(super) fn graph_op_access_level(method: &Method) -> AccessLevel {
    // TsAppend used to self-route before this boundary, which accidentally classified
    // it as neither a graph read nor write. It is now graph-scoped and requires the
    // same Write ACL as every other mutation; so do the other two `series.redb`
    // mutations, TsEvict/TsDeleteSeries (retention) -- a Read-only caller must not be
    // able to evict points or delete a whole series any more than they could append
    // to one. All other Ts methods (including the read-only TsListSeries enumeration)
    // require only Read.
    if requires_write(method)
        || matches!(
            method,
            Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. }
        )
    {
        AccessLevel::Write
    } else {
        AccessLevel::Read
    }
}

/// The registry facts a graph operation needs after the registry lock is
/// released. Copied out under the lock rather than held by reference.
pub(super) struct GraphEntryFacts {
    pub(super) graph_type: crate::protocol::GraphType,
    pub(super) owner: Option<String>,
    pub(super) core: Arc<crate::graph::GraphCore>,
    #[cfg(feature = "redb")]
    pub(super) incarnation_id: String,
}

/// Gate one graph operation, in the ORIGINAL order: an unregistered or
/// unauthenticated caller is denied BEFORE existence is resolved -- never let
/// "Graph not found" vs "ACCESS_DENIED" tell a caller who could never pass ACL
/// for any graph whether the target graph exists (see
/// `access::check_caller_is_known`'s doc). A registered caller falls through to
/// existence, materialization validity, and then the real graph-type/owner-aware
/// decision.
pub(super) fn check_graph_op_access(
    s: &ServerState,
    req_id: u64,
    caller: Option<&str>,
    graph_name: &str,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<GraphEntryFacts, Response> {
    check_known_caller(
        &s.isolation,
        req_id,
        caller,
        graph_name,
        access,
        state_machine_authorized,
    )?;
    let Some(entry) = s.registry.get(graph_name) else {
        return Err(Response::err(
            req_id,
            format!("Graph '{graph_name}' not found"),
        ));
    };
    check_materialization_valid(&s.registry, req_id, graph_name)?;
    if !state_machine_authorized {
        if let Err(denied) = check_graph_access(
            &s.isolation,
            caller,
            graph_name,
            entry.graph_type,
            entry.owner.as_deref(),
            access,
        ) {
            return Err(Response::err(req_id, denied));
        }
    }
    Ok(GraphEntryFacts {
        graph_type: entry.graph_type,
        owner: entry.owner.clone(),
        core: entry.core.clone(),
        #[cfg(feature = "redb")]
        incarnation_id: entry.incarnation_id.clone(),
    })
}

/// MultiRaft is the sole clustered authority: a replicated apply proposes
/// nothing, and a node without MultiRaft placement has no write routing.
#[cfg(feature = "raft")]
pub(super) fn resolve_multi_raft(
    s: &ServerState,
    placement_authority: &crate::server::state::PlacementAuthorityKind,
) -> Option<Arc<crate::raft::multi::MultiRaft>> {
    if is_replicated_apply() {
        return None;
    }
    if !matches!(
        placement_authority,
        crate::server::state::PlacementAuthorityKind::MultiRaft
    ) {
        return None;
    }
    s.multi_raft.clone()
}

#[cfg(all(feature = "raft", feature = "tsdb"))]
pub(super) fn ts_placement_fence(
    routed: Option<&crate::raft::multi::RoutedRaftHandle>,
) -> (u64, Option<u64>) {
    match routed {
        Some(routed) => (routed.epoch, Some(routed.group_id)),
        None => (0, None),
    }
}

/// The dispatch shell's write tail: size gauges, the single projection
/// publication non-gateway writes rely on, and the semantic-ANN warm hook.
#[allow(unused_variables)]
pub(super) async fn finalize_graph_op_response(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    core: &Arc<crate::graph::GraphCore>,
    access: AccessLevel,
    gateway_routed: bool,
    response: Response,
) -> Response {
    // Refresh the per-graph size gauges after mutations — both petgraph
    // counts are O(1), so this adds no meaningful write-path cost.
    #[cfg(feature = "metrics")]
    if matches!(access, AccessLevel::Write) {
        let topo = core.topo.read();
        crate::metrics::set_graph_size(
            graph_name,
            topo.graph.node_count() as i64,
            topo.graph.edge_count() as i64,
        );
    }

    // Non-gateway writes still rely on the dispatch shell for their single
    // projection publication. Gateway writes already publish exactly once in
    // `commit_finalize`; marking them here as well would advance the resident OCC
    // version past the authoritative MutationBatch version.
    if matches!(access, AccessLevel::Write) && response.error.is_none() && !gateway_routed {
        core.mark_dirty();
    }

    // W0.4 semantic-ANN activation (CONCEPT:EG-KG.storage.semantic-index-directory): a graph created, or
    // one whose embedding count crosses `ANN_BUILD_THRESHOLD`, AFTER the boot-time
    // warm task's one-shot snapshot never gets a trigger from it otherwise. Every
    // write that adds embeddings (`AddEmbedding`, and every mining/graph-learning
    // writeback) flows through this SAME dispatch tail, so one hook here — spawned,
    // never inline on the request path — covers them all. No-op below the threshold.
    #[cfg(feature = "ann")]
    if matches!(access, AccessLevel::Write) && response.error.is_none() {
        crate::server::semantic_activation::maybe_activate_after_write(state, graph_name, core)
            .await;
    }

    response
}

pub(super) fn check_known_caller(
    isolation: &crate::isolation::IsolationLayer,
    req_id: u64,
    caller: Option<&str>,
    graph_name: &str,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<(), Response> {
    if !state_machine_authorized {
        if let Err(denied) = check_caller_is_known(isolation, caller, graph_name, access) {
            return Err(Response::err(req_id, denied));
        }
    }
    Ok(())
}

pub(super) fn check_materialization_valid(
    registry: &crate::registry::GraphRegistry,
    req_id: u64,
    graph_name: &str,
) -> Result<(), Response> {
    if let Some(manifest) = registry.materialization_manifest(graph_name) {
        if !manifest.valid {
            let phase = match manifest.phase {
                crate::registry::MaterializationPhase::CatalogOnly => "catalog_only",
                crate::registry::MaterializationPhase::Partial => "partial",
                crate::registry::MaterializationPhase::Complete => "complete",
                crate::registry::MaterializationPhase::Failed => "failed",
            };
            return Err(Response::err(
                req_id,
                serde_json::json!({
                    "code": "PARTIAL_MATERIALIZATION",
                    "phase": phase,
                    "source_snapshot_version": manifest.source_snapshot_version,
                    "completeness_cursor": manifest.completeness_cursor.as_ref().map(|cursor| serde_json::json!({
                        "node_offset": cursor.node_offset,
                        "edge_offset": cursor.edge_offset,
                    })),
                    "retryable": manifest.phase != crate::registry::MaterializationPhase::Failed,
                })
                .to_string(),
            ));
        }
    }
    Ok(())
}
