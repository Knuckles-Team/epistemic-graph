use super::*;

#[cfg(feature = "query")]
pub(crate) async fn handle_txn_unified_query_text(
    ctx: &QueryHandlerCtx<'_>,
    txn_id: String,
    text: String,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let read_authority = ctx.read_authority;
    let caller = ctx.caller;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    // UQL front-end: parse to the SAME `wire::Plan`, then run the IDENTICAL
    // overlaid in-txn executor. A parse error is a caret-annotated Response.
    let plan = match eg_plan::uql::parse(&text) {
        Ok(p) => p,
        Err(e) => return Ok(Response::err(req_id, e.render(&text))),
    };
    Ok(run_unified_overlaid(
        state,
        req_id,
        &txn_id,
        plan,
        read_authority,
        caller,
        #[cfg(feature = "security")]
        rls,
    )
    .await)
}

/// The `Op::TsScan` leg-resolution shared by [`handle_nl_query`] (and any sibling
/// served-query handler that needs the SAME committed-tsdb-store + tenant/graph
/// scope triple `run_unified`'s `TsdbLegBind` takes): the caller's RLS-checked
/// scope for `plan`, then the committed store handle only when that scope is
/// non-empty (never touch the tsdb store for a plan that doesn't reference one).
#[cfg(all(feature = "nl-query", feature = "tsdb"))]
pub(crate) async fn resolve_tsdb_leg(
    state: &Arc<RwLock<ServerState>>,
    plan: &eg_plan::Plan,
    graph_name: &str,
    read_authority: Option<&GraphReadAuthority>,
    req_id: u64,
) -> Result<
    (
        Option<Arc<eg_tsdb::store::SeriesStore>>,
        Option<String>,
        Option<String>,
    ),
    Response,
> {
    let tsdb_scope = served_tsdb_scope(plan, graph_name, read_authority)
        .map_err(|denied| Response::err(req_id, denied))?;
    let tsdb = if tsdb_scope.is_some() {
        state.read().await.tsdb_store.clone()
    } else {
        None
    };
    let (tsdb_tenant, tsdb_graph) = match tsdb_scope {
        Some((tenant, graph)) => (Some(tenant), Some(graph)),
        None => (None, None),
    };
    Ok((tsdb, tsdb_tenant, tsdb_graph))
}

#[cfg(feature = "nl-query")]
/// The NL → executable-UQL-plan resolution of [`handle_nl_query`]: resolve the
/// configured/injected `NlPlanner`, turn `text` into a UQL query STRING, then
/// parse it into the SAME `wire::Plan` `UnifiedQueryText` carries. NO LLM in the
/// engine core and NO new execution path — the produced query rides the
/// deterministic pipeline.
#[cfg(feature = "nl-query")]
pub(crate) fn handle_nl_query_plan(
    text: &str,
    core: &Arc<GraphCore>,
    req_id: u64,
) -> Result<eg_plan::Plan, Response> {
    let planner = crate::server::nl::resolve_planner().ok_or_else(|| {
        Response::err(
            req_id,
            "NlQuery: no NL planner configured — set an OpenAI-compatible \
                     endpoint in agent-utilities config.json (or \
                     EPISTEMIC_GRAPH_NL_ENDPOINT), or inject one via \
                     server::set_nl_planner"
                .to_string(),
        )
    })?;
    let hint = nl_schema_hint(core);
    let uql = planner
        .plan(text, &hint)
        .map_err(|e| Response::err(req_id, format!("NlQuery planner error: {e}")))?;
    eg_plan::uql::parse(&uql).map_err(|e| {
        Response::err(
            req_id,
            format!("NlQuery produced invalid UQL: {}", e.render(&uql)),
        )
    })
}

#[cfg(feature = "nl-query")]
pub(crate) async fn handle_nl_query(
    ctx: &QueryHandlerCtx<'_>,
    text: String,
    graph: String,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let read_authority = ctx.read_authority;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    // CONCEPT:EG-KG.query.core-query-input/EG-080 — natural-language → executable query → rows. Resolve
    // the configured/injected `NlPlanner`, turn the NL into a UQL query STRING,
    // then run it through the IDENTICAL `UnifiedQueryText` pipeline
    // (`eg_plan::uql::parse` + `run_unified`). NO LLM in the engine core and NO
    // new execution path — the produced query rides the deterministic pipeline.
    // The graph was already used for routing; the handler runs against `core`.
    let _ = graph;
    let plan = match handle_nl_query_plan(&text, &core, req_id) {
        Ok(plan) => plan,
        Err(resp) => return Ok(resp),
    };
    // RECONCILE (CONCEPT:EG-KG.query.native-time-series): committed tsdb store + scope for `Op::TsScan` fusion.
    #[cfg(feature = "tsdb")]
    let (tsdb, tsdb_tenant, tsdb_graph) =
        match resolve_tsdb_leg(state, &plan, graph_name, read_authority, req_id).await {
            Ok(leg) => leg,
            Err(resp) => return Ok(resp),
        };
    // RLS-filtered off-lock snapshot, exactly like the Sql/UnifiedQueryText reads.
    // NOT result-cached: an LLM plan is non-deterministic, so keying a cache on the
    // NL text would risk serving a stale/foreign result.
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
    // See the `UnifiedQuery` arm: push vector + lexical legs into the live
    // persistent indexes via a guard taken INSIDE the off-lock closure.
    let core_for_ctx = core.clone();
    // CONCEPT:EG-KG.query.closure-backed-source — the server's REGISTERED foreign sources,
    // cloned (a cheap `Arc` handle) for the off-lock closure exactly like the tsdb store
    // above, so `run_unified` can resolve a `FOREIGN "<name>"` / `Named` `ForeignScan`
    // leg through `ServerState::foreign_sources` instead of erroring on every named
    // source `Method::RegisterForeignSource` accepted.
    #[cfg(feature = "federation")]
    let foreign_sources = state.read().await.foreign_sources.clone();
    let resp = match compute_off_lock(req_id, move || {
        #[cfg(feature = "text")]
        let served_text =
            crate::server::secondary_indexes::ServedTextIndex::new(core_for_ctx.clone());
        #[cfg(feature = "geo")]
        let served_spatial =
            crate::server::secondary_indexes::ServedSpatialIndex::new(core_for_ctx.clone());
        let semantic_guard = core_for_ctx.semantic_store.read();
        run_unified(
            plan,
            &snap,
            &semantic_guard,
            ServedIndexes {
                #[cfg(feature = "text")]
                text: Some(&served_text),
                #[cfg(feature = "geo")]
                spatial: Some(&served_spatial),
                #[cfg(feature = "federation")]
                foreign: Some(&*foreign_sources),
                #[cfg(not(any(feature = "text", feature = "geo")))]
                _marker: std::marker::PhantomData,
            },
            #[cfg(feature = "tsdb")]
            TsdbLegBind {
                tsdb: tsdb.as_deref(),
                tsdb_tenant: tsdb_tenant.as_deref(),
                tsdb_graph: tsdb_graph.as_deref(),
                // Off-txn: no staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
                staged_series: None,
            },
        )
    })
    .await
    {
        Ok(Ok(rows)) => raw_response(req_id, &rows),
        Ok(Err(msg)) => Response::err(req_id, format!("NlQuery error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}
