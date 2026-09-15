use super::*;
use crate::server::handlers::TryHandleContext;

pub(in crate::server) fn try_handle<'a>(
    state: &'a Arc<RwLock<ServerState>>,
    ctx: TryHandleContext<'a>,
    core: Arc<GraphCore>,
    method: Method,
    #[cfg(feature = "security")] rls: &'a Arc<crate::isolation::IsolationLayer>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, Method>> + Send + 'a>> {
    Box::pin(try_handle_inner(
        state,
        ctx,
        core,
        method,
        #[cfg(feature = "security")]
        rls,
    ))
}

async fn try_handle_inner(
    state: &Arc<RwLock<ServerState>>,
    ctx: TryHandleContext<'_>,
    core: Arc<GraphCore>,
    method: Method,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    let TryHandleContext {
        req_id,
        graph_name,
        read_authority,
        caller,
    } = ctx;
    #[cfg(not(feature = "security"))]
    let _ = caller;
    // `state` is consumed only by the `query`-gated in-txn cross-modal RYOW arms
    // (CONCEPT:EG-KG.query.txn-cross-modal-ryow — TxnUnifiedQuery{,Text}); keep it referenced in a
    // cypher/graphql-only build (no `query`) so no dead-param warning fires.
    #[cfg(not(feature = "query"))]
    let _ = (state, read_authority);
    // `graph_name` is consumed only by the `graphql`-gated cross-modal durable commit
    // (CONCEPT:EG-KG.query.facade-reconcile-hook); keep it referenced in a query/cypher-only build so no dead-param
    // warning fires.
    #[cfg(not(feature = "graphql"))]
    let _ = graph_name;
    let hctx = QueryHandlerCtx {
        state,
        req_id,
        graph_name,
        read_authority,
        caller,
        core: &core,
        #[cfg(feature = "security")]
        rls,
    };

    #[cfg(feature = "query")]
    if is_sql_query_method(&method) {
        return dispatch_sql_query(&hctx, method).await;
    }
    #[cfg(feature = "query")]
    if is_explain_method(&method) {
        return dispatch_explain_method(&hctx, method).await;
    }
    #[cfg(all(feature = "query", feature = "epistemic-tms"))]
    if is_epistemic_tms_method(&method) {
        return dispatch_epistemic_tms_method(&hctx, method).await;
    }
    #[cfg(all(
        feature = "query",
        any(feature = "evidence-graph", feature = "epistemic-causal")
    ))]
    if is_evidence_causal_method(&method) {
        return dispatch_evidence_causal_method(&hctx, method).await;
    }
    #[cfg(feature = "query")]
    if is_txn_query_method(&method) {
        return dispatch_txn_query_method(&hctx, method).await;
    }
    #[cfg(any(feature = "nl-query", feature = "graphql", feature = "cypher"))]
    return dispatch_external_query_method(&hctx, method).await;
    #[cfg(not(any(feature = "nl-query", feature = "graphql", feature = "cypher")))]
    Err(method)
}

#[cfg(feature = "query")]
fn is_sql_query_method(method: &Method) -> bool {
    matches!(
        method,
        Method::Sql { .. } | Method::UnifiedQuery { .. } | Method::UnifiedQueryText { .. }
    )
}

#[cfg(feature = "query")]
async fn dispatch_sql_query(ctx: &QueryHandlerCtx<'_>, method: Method) -> Result<Response, Method> {
    match method {
        Method::Sql {
            query,
            params_msgpack,
        } => handle_sql(ctx, query, params_msgpack).await,
        Method::UnifiedQuery { plan } => handle_unified_query(ctx, plan).await,
        Method::UnifiedQueryText { text } => handle_unified_query_text(ctx, text).await,
        other => Err(other),
    }
}

#[cfg(feature = "query")]
fn is_explain_method(method: &Method) -> bool {
    let core = matches!(
        method,
        Method::ExplainPlan { .. }
            | Method::ExplainProvenance { .. }
            | Method::ExplainProvenanceByIds { .. }
            | Method::ExplainPolicy { .. }
    );
    #[cfg(feature = "epistemic")]
    return core || matches!(method, Method::ExplainBelief { .. });
    #[cfg(not(feature = "epistemic"))]
    core
}

#[cfg(feature = "query")]
async fn dispatch_explain_method(
    ctx: &QueryHandlerCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::ExplainPlan { plan } => handle_explain_plan(ctx, plan).await,
        Method::ExplainProvenance { plan } => handle_explain_provenance(ctx, plan).await,
        Method::ExplainProvenanceByIds { ids } => handle_explain_provenance_by_ids(ctx, ids).await,
        Method::ExplainPolicy { plan } => handle_explain_policy(ctx, plan).await,
        #[cfg(feature = "epistemic-redaction")]
        Method::ExplainBelief {
            node_id,
            disclosure_level,
        } => handle_explain_belief(ctx, node_id, disclosure_level).await,
        #[cfg(all(feature = "epistemic", not(feature = "epistemic-redaction")))]
        Method::ExplainBelief {
            node_id,
            disclosure_level,
        } => handle_explain_belief(ctx, node_id, disclosure_level).await,
        other => Err(other),
    }
}

#[cfg(all(feature = "query", feature = "epistemic-tms"))]
fn is_epistemic_tms_method(method: &Method) -> bool {
    matches!(
        method,
        Method::EpistemicStatus { .. }
            | Method::WhatChanged { .. }
            | Method::RecomputeMaterialization { .. }
            | Method::MaterializationStatus { .. }
            | Method::StaleMaterializations
            | Method::ResolveConflict { .. }
    )
}

#[cfg(all(feature = "query", feature = "epistemic-tms"))]
async fn dispatch_epistemic_tms_method(
    ctx: &QueryHandlerCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        #[cfg(feature = "epistemic-tms")]
        Method::EpistemicStatus { node_id } => handle_epistemic_status(ctx, node_id).await,
        #[cfg(feature = "epistemic-tms")]
        Method::WhatChanged { tx_from, tx_to } => handle_what_changed(ctx, tx_from, tx_to).await,
        #[cfg(feature = "epistemic-tms")]
        Method::RecomputeMaterialization {
            derived_id,
            expected_source_graph_version,
        } => handle_recompute_materialization(ctx, derived_id, expected_source_graph_version).await,
        #[cfg(feature = "epistemic-tms")]
        Method::MaterializationStatus { id } => handle_materialization_status(ctx, id).await,
        #[cfg(feature = "epistemic-tms")]
        Method::StaleMaterializations => handle_stale_materializations(ctx).await,
        #[cfg(feature = "epistemic-tms")]
        Method::ResolveConflict {
            node_ids,
            semantics,
        } => handle_resolve_conflict(ctx, node_ids, semantics).await,
        other => Err(other),
    }
}

#[cfg(all(
    feature = "query",
    any(feature = "evidence-graph", feature = "epistemic-causal")
))]
fn is_evidence_causal_method(method: &Method) -> bool {
    #[cfg(feature = "evidence-graph")]
    if matches!(method, Method::ExplainEvidence { .. }) {
        return true;
    }
    #[cfg(feature = "epistemic-causal")]
    if matches!(
        method,
        Method::CausalEstimate { .. }
            | Method::CausalCounterfactual { .. }
            | Method::RankByProvenance { .. }
    ) {
        return true;
    }
    false
}

#[cfg(all(
    feature = "query",
    any(feature = "evidence-graph", feature = "epistemic-causal")
))]
async fn dispatch_evidence_causal_method(
    ctx: &QueryHandlerCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        #[cfg(all(feature = "evidence-graph", feature = "alignment"))]
        Method::ExplainEvidence { node_id } => handle_explain_evidence(ctx, node_id).await,
        #[cfg(all(feature = "evidence-graph", not(feature = "alignment")))]
        Method::ExplainEvidence { node_id } => handle_explain_evidence(ctx, node_id).await,
        #[cfg(feature = "epistemic-causal")]
        Method::CausalEstimate {
            variables,
            do_values,
            mode,
        } => handle_causal_estimate(ctx, variables, do_values, mode).await,
        #[cfg(feature = "epistemic-causal")]
        Method::CausalCounterfactual {
            variables,
            actual,
            do_values,
        } => handle_causal_counterfactual(ctx, variables, actual, do_values).await,
        #[cfg(feature = "epistemic-causal")]
        Method::RankByProvenance {
            candidates,
            weights,
        } => handle_rank_by_provenance(ctx, candidates, weights).await,
        other => Err(other),
    }
}

#[cfg(feature = "query")]
fn is_txn_query_method(method: &Method) -> bool {
    matches!(
        method,
        Method::TxnUnifiedQuery { .. } | Method::TxnUnifiedQueryText { .. }
    )
}

#[cfg(feature = "query")]
async fn dispatch_txn_query_method(
    ctx: &QueryHandlerCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::TxnUnifiedQuery { txn_id, plan } => {
            Ok(run_unified_overlaid::<query_results::TxnUnifiedQuery>(
                ctx.state,
                ctx.req_id,
                &txn_id,
                plan,
                ctx.read_authority,
                ctx.caller,
                #[cfg(feature = "security")]
                ctx.rls,
            )
            .await)
        }
        Method::TxnUnifiedQueryText { txn_id, text } => {
            handle_txn_unified_query_text(ctx, txn_id, text).await
        }
        other => Err(other),
    }
}

#[cfg(any(feature = "nl-query", feature = "graphql", feature = "cypher"))]
async fn dispatch_external_query_method(
    ctx: &QueryHandlerCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        #[cfg(feature = "nl-query")]
        Method::NlQuery { text, graph } => handle_nl_query(ctx, text, graph).await,
        #[cfg(feature = "graphql")]
        Method::GraphQl { query, variables } => handle_graphql(ctx, query, variables).await,
        #[cfg(feature = "cypher")]
        Method::CypherQuery { query, mode } => handle_cypher_query(ctx, query, mode).await,
        other => Err(other),
    }
}

// ── Policy-lease-aware entry point (GRAPH-POLICY-LEASE-CONTRACT.md §6) ──────
//
// `KnowledgeStream`'s `execute_sql`/`execute_cross_modal` families
// (`server::handlers::knowledge_stream::families`) delegate their read here
// instead of the shared `try_handle` above, so that a lease-bearing call uses
// `lease.filter_view` as the SOLE row-visibility authority — never
// `rls.filter_view(caller, ..)` — exactly like `execute_graph`'s own
// `filtered_snapshot` comment requires for graph/vector reads. A closed,
// two-variant `PolicyAwareQuery` (R7) rather than the full `Method` +
// `Err(method)` fallthrough `try_handle` uses: a fallthrough would let a
// future `Method` variant silently inherit stream authorization without a
// compile-time decision to add lease-based filtering for it.
#[cfg(feature = "query")]
pub(in crate::server) enum PolicyAwareQuery {
    Sql {
        query: String,
        params_msgpack: Vec<u8>,
    },
    UnifiedQueryText {
        text: String,
    },
}

#[cfg(all(feature = "query", feature = "security"))]
pub(in crate::server) fn try_handle_with_policy<'a>(
    state: &'a Arc<RwLock<ServerState>>,
    ctx: TryHandleContext<'a>,
    core: Arc<GraphCore>,
    query: PolicyAwareQuery,
    policy_lease: &'a Arc<crate::isolation::PolicyDecisionLease>,
    rls: &'a Arc<crate::isolation::IsolationLayer>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, String>> + Send + 'a>> {
    Box::pin(try_handle_with_policy_inner(
        state,
        ctx,
        core,
        query,
        policy_lease,
        rls,
    ))
}

/// A build without `security` has no `PolicyDecisionLease` to bind — and every
/// production caller of this function is itself gated `feature = "security"`
/// upstream (`KnowledgeStreamAuthority::validate_request_binding` denies
/// unconditionally without it, mod.rs). This arm exists purely so
/// `families.rs`'s call site — which references this function unconditionally,
/// only its trailing `policy_lease`/`rls` ARGUMENTS are `#[cfg]`-gated —
/// compiles in that configuration too; it is never reachable in practice.
#[cfg(all(feature = "query", not(feature = "security")))]
pub(in crate::server) fn try_handle_with_policy<'a>(
    state: &'a Arc<RwLock<ServerState>>,
    ctx: TryHandleContext<'a>,
    core: Arc<GraphCore>,
    query: PolicyAwareQuery,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, String>> + Send + 'a>> {
    let _ = (state, ctx, core, query);
    Box::pin(async {
        Err("KnowledgeStream requires the security policy lease feature".to_string())
    })
}

#[cfg(all(feature = "query", feature = "security"))]
async fn try_handle_with_policy_inner(
    state: &Arc<RwLock<ServerState>>,
    ctx: TryHandleContext<'_>,
    core: Arc<GraphCore>,
    query: PolicyAwareQuery,
    policy_lease: &Arc<crate::isolation::PolicyDecisionLease>,
    rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, String> {
    let TryHandleContext {
        req_id,
        graph_name,
        read_authority,
        caller,
    } = ctx;
    // R6/§8 item 13's mint-time case has an execution-time analogue: a lease
    // was minted against a bound store, but this specific call's
    // `IsolationLayer` has none. Fail closed with the same generic message
    // `KnowledgeStreamAuthority::filter_view`/`validate_lease` already use
    // for exactly this condition (mod.rs).
    let store = rls
        .policy_store()
        .ok_or_else(|| "KnowledgeStream policy authority is unavailable".to_string())?;
    let lease_ctx = LeaseQueryCtx {
        state,
        req_id,
        graph_name,
        read_authority,
        caller,
        core: &core,
        policy_lease,
        store: store.as_ref(),
    };
    match query {
        PolicyAwareQuery::Sql {
            query,
            params_msgpack,
        } => handle_sql_with_lease(&lease_ctx, query, params_msgpack).await,
        PolicyAwareQuery::UnifiedQueryText { text } => {
            handle_unified_query_text_with_lease(&lease_ctx, text).await
        }
    }
}

/// The fields `handle_sql_with_lease`/`handle_unified_query_text_with_lease`
/// need beyond their own per-query payload, bundled so each stays under the
/// clippy argument-count ceiling — the same `QueryHandlerCtx`/
/// `FamilyExecutionCtx` bundling idiom this file and `families.rs` already
/// use for the same reason.
#[cfg(all(feature = "query", feature = "security"))]
struct LeaseQueryCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    read_authority: Option<&'a GraphReadAuthority>,
    caller: &'a str,
    core: &'a Arc<GraphCore>,
    policy_lease: &'a Arc<crate::isolation::PolicyDecisionLease>,
    store: &'a dyn eg_core::rbac_persist::RbacPolicyStore,
}

/// Fresh, per-call row-visibility snapshot using the lease as the sole
/// filtering authority (contract §5/§6, `execute_graph`'s `filtered_snapshot`
/// comment). Deliberately bypasses BOTH `FilteredViewCache`
/// (`(actor, version)`-keyed, `crate::graph::GraphCore::cached_filtered_view`)
/// and `ResultCache` entirely — R2/§4's option (a) — rather than re-keying
/// them to fold in the lease's policy/identity digest: neither cache's key
/// includes anything RBAC-derived today (SEC-FINDING-RLS-VIEW-CACHE-STALENESS-20260902.md),
/// so a policy/identity mutation with no accompanying graph write leaves a
/// stale entry servable through EITHER cache indefinitely; always recomputing
/// `lease.filter_view` here closes that hole for this lease-bound path without
/// having to widen every probe/put call site `versioned_rls_snapshot`/
/// `rls_snapshot`/`ResultCache` use elsewhere in this file.
#[cfg(all(feature = "query", feature = "security"))]
fn lease_filtered_snapshot(
    core: &Arc<GraphCore>,
    lease: &Arc<crate::isolation::PolicyDecisionLease>,
    store: &dyn eg_core::rbac_persist::RbacPolicyStore,
) -> Result<(Arc<crate::graph::GraphView>, u64), String> {
    // `version` is read before the snapshot, the same "safe LOWER BOUND"
    // idiom `rls_snapshot`'s `not(result-cache)` branch documents above — a
    // concurrent write racing the two reads can only make `view` reflect
    // content NEWER than `version` claims, never older. This path never
    // stores the pair under `version` as a cache key (bypass, not reuse), so
    // the only consumer of `version` here is the SQL executor's node-batch
    // sub-cache key, a perf concern, not a correctness one.
    let version = core.version();
    let mut view = core.analysis_snapshot();
    lease
        .filter_view(store, &mut view)
        .map_err(|_| "KnowledgeStream policy decision lease is stale".to_string())?;
    Ok((Arc::new(view), version))
}

#[cfg(all(feature = "query", feature = "security"))]
async fn handle_sql_with_lease(
    ctx: &LeaseQueryCtx<'_>,
    query: String,
    params_msgpack: Vec<u8>,
) -> Result<Response, String> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let read_authority = ctx.read_authority;
    let core = ctx.core;
    let policy_lease = ctx.policy_lease;
    let store = ctx.store;
    bind_sql_text_embedder();
    // KnowledgeStream's policy-aware SQL surface is READ-ONLY by contract
    // (§2.1 — the lease only ever binds `AccessLevel::Read`).
    // `families::execute_sql` already rejects a write statement before this
    // is reached; reject again here so this function is safe to call on its
    // own, never a second, wider-scoped SQL entry point.
    if crate::server::access::sql_is_write(&query) {
        return Err("KnowledgeStream SQL accepts read-only statements".to_string());
    }
    // `params_msgpack` is unused on the read path, exactly like `handle_sql`'s
    // own read arm (it is only ever consumed by the write-classification arm
    // this function deliberately does not implement).
    let _ = params_msgpack;
    let Some(read_authority) = read_authority else {
        crate::metrics::access_denied();
        return Err("ACCESS_DENIED: current signed tenant authority is required".to_string());
    };
    let Some(authority) = read_authority.carrier() else {
        crate::metrics::access_denied();
        return Err("ACCESS_DENIED: current signed tenant authority is required".to_string());
    };
    let persist_dir = state.read().await.persist_dir.clone().ok_or_else(|| {
        "SQL error: tenant SQL catalog requires the configured persistence directory".to_string()
    })?;
    let (snap, _graph_version) = lease_filtered_snapshot(core, policy_lease, store)?;
    let cancel = eg_query::CancellationToken::new();
    let _cancel_guard = crate::server::request_cancel::register(req_id, cancel.clone());
    let timeout_task = crate::server::request_cancel::spawn_timeout(cancel.clone());
    let cancel_for_task = cancel.clone();
    let authority = authority.clone();
    let resp = match compute_off_lock(req_id, move || {
        let authorized = crate::server::sql_catalog_acl::authorized_read_store(
            &authority,
            std::path::Path::new(&persist_dir),
        )?;
        eg_query::exec_sql_typed_with_tables_cancellable(
            &snap,
            authorized.store(),
            &query,
            &cancel_for_task,
        )
    })
    .await
    {
        Ok(Ok(typed)) => match typed.rows.iter().map(msgpack_bytes).collect() {
            Ok(rows) => dynamic_response::<query_results::Sql, _>(
                req_id,
                &crate::protocol::QueryResult {
                    columns: typed.columns.iter().map(|c| c.name.clone()).collect(),
                    rows,
                },
            ),
            Err(error) => Response::err(req_id, error),
        },
        Ok(Err(msg)) => Response::err(req_id, format!("SQL error: {msg}")),
        Err(resp) => resp,
    };
    if let Some(t) = timeout_task {
        t.abort();
    }
    Ok(resp)
}

#[cfg(all(feature = "query", feature = "security"))]
async fn handle_unified_query_text_with_lease(
    ctx: &LeaseQueryCtx<'_>,
    text: String,
) -> Result<Response, String> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    #[cfg(feature = "tsdb")]
    let graph_name = ctx.graph_name;
    #[cfg(feature = "tsdb")]
    let read_authority = ctx.read_authority;
    let core = ctx.core;
    let policy_lease = ctx.policy_lease;
    let store = ctx.store;
    let plan = match eg_plan::uql::parse(&text) {
        Ok(plan) => plan,
        Err(e) => return Ok(Response::err(req_id, e.render(&text))),
    };
    #[cfg(feature = "tsdb")]
    let tsdb_scope = served_tsdb_scope(&plan, graph_name, read_authority)?;
    let (snap, _version) = lease_filtered_snapshot(core, policy_lease, store)?;
    let resp = match run_unified_off_lock(
        state,
        req_id,
        core,
        snap,
        plan,
        #[cfg(feature = "tsdb")]
        tsdb_scope,
    )
    .await
    {
        Ok(Ok(rows)) => result_response::<query_results::UnifiedQueryText>(req_id, &rows),
        Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}
