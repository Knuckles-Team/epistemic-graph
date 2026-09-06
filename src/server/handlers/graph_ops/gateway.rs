use super::*;

/// Keep the mutation kernel behind a heap indirection so this module's large
/// gateway match does not embed one copy of the kernel future in every arm.
/// Without this boundary the generated `try_handle_gateway` future exceeds
/// Tokio's default worker-thread stack on ordinary mutation requests.
pub(super) async fn commit_gateway<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    Box::pin(mutation::commit_mutation(ctx, plan, method, apply)).await
}

/// Same boundary as [`commit_gateway`], for the five coalescable structural
/// writes (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`/CAS) — routed through
/// `mutation::commit_coalescable_mutation` instead of `commit_mutation` so
/// their prepare→durable-commit→RAM-publish sequence can be queued onto the
/// per-graph routed-write-coalescer worker (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18
/// rewrite) rather than run inline under this request's own lock hold. `apply`
/// must be `Send + 'static`: it may run on the worker task, after this
/// request's own stack frame is gone — every call site below already passes an
/// owned `move` closure over cloned `String`/`Vec<u8>` fields, so this is not a
/// new constraint on what those closures may capture.
pub(super) async fn commit_gateway_coalescable<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String> + Send + 'static,
{
    Box::pin(mutation::commit_coalescable_mutation(
        ctx, plan, method, apply,
    ))
    .await
}

/// Shared conversion tail every `Mine*` gateway closure ends with (CONCEPT:EG-KG.mining.tsdb-typed-absent):
/// a mining `Response` carries its own `error`/`result`, so folding it into the
/// `apply: FnOnce(&GraphCore) -> Result<ResultPayload, String>` shape `commit_conditional_mutation`
/// expects is identical work at every one of the 17 `Mine*` call sites, plus the
/// GraphLearn*/MiningPipeline* families (same `commit_conditional_mutation` shape,
/// gated on different features). Pure extract-method, no behaviour change:
/// byte-identical to what each closure did inline before this extraction. Not
/// itself `mining`-gated since GraphLearn*/MiningPipeline* need it under their
/// own, different features.
pub(super) fn mining_response_to_gateway_result(resp: Response) -> Result<ResultPayload, String> {
    match resp.error {
        Some(e) => Err(e),
        None => Ok(resp
            .result
            .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
    }
}

/// Route a [`mutation::GATEWAY_ROUTED`] method through the single commit gateway
/// (CONCEPT:EG-P0-2). Called from `dispatch_graph_op` AHEAD of both the write-
/// coalescer and the terminal exhaustive match below, so a routed method NEVER falls
/// through to `g.add_node(...)` etc. directly — the only path left for it is this
/// one, which builds a [`MutationPlan`] straight from `eg_capabilities::policy` and
/// calls [`mutation::commit_mutation`]. A method NOT in the routed set is handed
/// straight back (`Err(method)`), unchanged, exactly like every other domain
/// router in `dispatch.rs`'s routing chain.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn try_handle_gateway(
    req_id: u64,
    caller: Option<&str>,
    tenant_scope: &str,
    graph_name: &str,
    core: &Arc<GraphCore>,
    materialization_manifest: Option<
        &Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>,
    >,
    read_authority: Option<&GraphReadAuthority>,
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    #[cfg(feature = "streaming")] cdc: Option<&Arc<crate::server::cdc::CdcHub>>,
    write_coalescer: Option<
        &Arc<crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry>,
    >,
    authz_ctx: Option<&GatewayAuthzCtx>,
    // CONCEPT:EG-KG.mining.tsdb-typed-absent — the server's live tsdb store, needed ONLY by
    // the gateway-routed `Mine*` arms below to bind a plan-sourced `Op::TsScan` leg (mirrors
    // `mining::try_handle`'s own binding for the one Mine* method NOT gateway-routed).
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))] tsdb_store: Option<
        &Arc<eg_tsdb::store::SeriesStore>,
    >,
    method: Method,
) -> Result<Response, Method> {
    if !mutation::is_gateway_routed(&method) {
        return Err(method);
    }
    // L11 batch 4: the query surface (`Sql`/`CypherQuery`/`GraphQl`) and the native
    // RDF write surface (`AddTriples`/`RemoveTriples`/`DropNamedGraph`) ARE
    // `GATEWAY_ROUTED`, but their execution is `async` and needs `state`/`rls` that this graph-ops entry point
    // does not carry — so they are routed via `commit_conditional_mutation_async` at
    // their OWN dispatch sites in `dispatch.rs`. Hand them back here so they reach
    // those sites; the `record_method`/`cdc_*` gating in `dispatch.rs` already keys
    // off the SAME `is_gateway_routed`, so nothing double-applies.
    if mutation::is_query_gateway_method(&method) || mutation::is_rdf_gateway_method(&method) {
        return Err(method);
    }
    // Runtime-conditional gateway methods may be reads (`writeback = false`).
    // Give those closures a detached, row-filtered core; writes ignore any read
    // authority and must keep operating on the authoritative graph. This is
    // deliberately after both hand-back checks above, so SQL/RDF reads retain
    // their existing snapshot-level RLS path without paying for a second copy.
    let mutates = requires_write(&method);
    let projected_core = if mutates {
        None
    } else {
        read_authority.map(|authority| authority.project_core(core))
    };
    let selected_core = projected_core.as_ref().unwrap_or(core);
    debug_assert!(
        !mutates || Arc::ptr_eq(selected_core, core),
        "mutation gateway must retain the authoritative serving projection"
    );
    let core = selected_core;
    let (isolation, graph_type, owner) = authz_ctx.expect(
        "dispatch_graph_op must capture a GatewayAuthzCtx for every mutation::is_gateway_routed method",
    );

    #[cfg(feature = "security")]
    let method = super::gateway_graph::stamp_owner_id_if_applicable(method, caller, isolation);

    let plan = MutationPlan::for_method(&method);
    let ctx = MutationCtx {
        req_id,
        caller,
        tenant_scope,
        graph_name,
        graph_type: *graph_type,
        owner: owner.as_deref(),
        isolation,
        core,
        persistence,
        #[cfg(feature = "streaming")]
        cdc,
        materialization_manifest,
        write_coalescer,
    };
    if let Some(resp) = super::gateway_graph::try_handle(&ctx, &plan, &method).await {
        return Ok(resp);
    }
    #[cfg(feature = "broker")]
    if let Some(resp) = super::gateway_broker::try_handle(&ctx, &plan, &method).await {
        return Ok(resp);
    }
    #[cfg(any(feature = "graphlearn", feature = "ml-pipeline"))]
    if let Some(resp) = super::gateway_mining_ml::try_handle(&ctx, &plan, &method).await {
        return Ok(resp);
    }
    #[cfg(feature = "mining")]
    if let Some(resp) = super::gateway_mining::try_handle(
        &ctx,
        &plan,
        &method,
        graph_name,
        read_authority,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb_store,
    )
    .await
    {
        return Ok(resp);
    }
    unreachable!("mutation::is_gateway_routed guarantees method is one of the routed variants")
}
