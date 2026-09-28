use super::*;

/// `Method::TxnUql`: a UQL statement over the txn-overlaid snapshot (read your own
/// writes) — the same statement runner `Method::Uql` uses (EH-434). Never cached.
#[cfg(feature = "query")]
pub(crate) async fn handle_txn_uql(
    ctx: &QueryHandlerCtx<'_>,
    txn_id: String,
    text: String,
    params: UqlParams,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let txn = OverlaidTxn {
        state: ctx.state,
        req_id,
        txn_id: &txn_id,
        read_authority: ctx.read_authority,
        caller: ctx.caller,
        #[cfg(feature = "security")]
        rls: ctx.rls,
    };
    let (stmt, binding) = match bind_served_statement(req_id, &text, &params) {
        Ok(bound) => bound,
        Err(response) => return Ok(response),
    };
    let result = run_unified_overlaid_with(txn, binding, move |_plan, plan_ctx| {
        eg_plan::uql::serve::run_statement(&stmt, plan_ctx)
    })
    .await;
    Ok(served_response::<query_results::TxnUql>(
        req_id,
        result,
        "UQL",
        #[cfg(feature = "result-cache")]
        ctx.core,
        #[cfg(feature = "result-cache")]
        None,
    ))
}

#[cfg(feature = "nl-query")]
/// The NL → executable-UQL-plan resolution of [`handle_nl_query`]: resolve the
/// configured/injected `NlPlanner`, turn `text` into a UQL query STRING, then
/// parse it into the SAME `wire::Plan` a UQL pipeline parses to. NO LLM in the
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
            "ENGINE_UNAVAILABLE: NlQuery: no NL planner configured — set an OpenAI-compatible \
                     endpoint in agent-utilities config.json (or \
                     EPISTEMIC_GRAPH_NL_ENDPOINT), or inject one via \
                     server::set_nl_planner"
                .to_string(),
        )
    })?;
    let hint = nl_schema_hint(core);
    let uql = planner.plan(text, &hint).map_err(|e| {
        Response::err(
            req_id,
            eg_types::contract::classify_refusal("INVALID_ARGUMENT", "", &e),
        )
    })?;
    eg_plan::uql::parse(&uql).map_err(|e| {
        Response::err(
            req_id,
            format!(
                "INVALID_ARGUMENT: NlQuery produced invalid UQL: {}",
                e.render(&uql)
            ),
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
    // then run it through the IDENTICAL UQL pipeline
    // (`eg_plan::uql::parse` + `run_unified`). NO LLM in the engine core and NO
    // new execution path — the produced query rides the deterministic pipeline.
    // The graph was already used for routing; the handler runs against `core`.
    let _ = graph;
    let plan = match handle_nl_query_plan(&text, &core, req_id) {
        Ok(plan) => plan,
        Err(resp) => return Ok(resp),
    };
    // Verified-carrier legs: the committed tsdb scope for `Op::TsScan` fusion
    // (CONCEPT:EG-KG.query.native-time-series) and the CALLER'S owner-scoped foreign
    // registry (EH-373), so an NL-planned `FOREIGN "<name>"` / `Named` `ForeignScan`
    // leg resolves only sources the caller (tenant+principal) registered.
    let legs = match ServedPlanLegs::resolve(state, graph_name, read_authority, &plan).await {
        Ok(legs) => legs,
        Err(denied) => return Ok(Response::err(req_id, denied)),
    };
    // RLS-filtered off-lock snapshot, exactly like the Sql/Uql reads.
    // NOT result-cached: an LLM plan is non-deterministic, so keying a cache on the
    // NL text would risk serving a stale/foreign result.
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
    // The same off-lock run as `Uql`: persistent vector/lexical/spatial/
    // shape indexes and the caller's owner-scoped foreign registry are bound inside it.
    let resp = match run_unified_off_lock(state, req_id, &core, Arc::new(snap), plan, legs).await {
        Ok(Ok(rows)) => result_response::<query_results::NlQuery>(req_id, &rows),
        Ok(Err(msg)) => Response::err(
            req_id,
            eg_types::contract::classify_refusal("INVALID_ARGUMENT", "", &msg),
        ),
        Err(resp) => resp,
    };
    Ok(resp)
}
