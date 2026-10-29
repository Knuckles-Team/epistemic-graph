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

/// The `NlQuery` refusal for invalid planner request config: the registered
/// `ENGINE_UNAVAILABLE` code plus a detail naming the bad key (EG-FEDERATED-QUERY-R058.5).
#[cfg(feature = "nl-query")]
pub(crate) fn nl_config_refusal(e: &eg_plan::NlConfigError) -> String {
    format!("ENGINE_UNAVAILABLE: NlQuery: invalid NL planner config: {e}")
}

#[cfg(feature = "nl-query")]
/// The NL → executable-UQL-plan resolution of [`handle_nl_query`]: resolve the
/// configured/injected `NlPlanner`, ground it on the caller's snapshot (labels,
/// stored property keys, relationships and the composed GraphSchema's ontology
/// names, EG-FEDERATED-QUERY-R058.4.7.1/.4.8), and run the bounded correction loop:
/// every candidate is budget-checked, parsed with the caller grammar and
/// validated against that schema before anything executes. NO LLM in the engine
/// core and NO new execution path — the grounded plan rides the deterministic
/// pipeline.
#[cfg(feature = "nl-query")]
pub(crate) fn handle_nl_query_plan(
    text: &str,
    view: &crate::graph::GraphView,
    req_id: u64,
    foreign_sources: Option<&std::collections::BTreeSet<String>>,
) -> Result<(String, eg_plan::Plan), Response> {
    let planner = crate::server::nl::resolve_planner()
        .map_err(|e| Response::err(req_id, nl_config_refusal(&e)))?
        .ok_or_else(|| {
            Response::err(
                req_id,
                "ENGINE_UNAVAILABLE: NlQuery: no NL planner configured — set an OpenAI-compatible \
                     endpoint in the Arbora config.json (or \
                     EPISTEMIC_GRAPH_NL_ENDPOINT), or inject one via \
                     server::set_nl_planner"
                    .to_string(),
            )
        })?;
    let context = nl_schema_context(view, foreign_sources);
    // EG-FEDERATED-QUERY-R058.2.1: the SAME typed budget refuses an over-budget
    // answer before it reaches the parser, inside every correction step.
    let candidate = eg_plan::nl::context::plan_grounded(
        planner.as_ref(),
        text,
        &context,
        eg_plan::nl::NlQueryBudget::default(),
        eg_plan::nl::context::DEFAULT_MAX_STEPS,
    )
    .map_err(|e| Response::err(req_id, nl_refusal(&e)))?;
    Ok((candidate.uql, candidate.plan))
}

/// The schema the planner may target: what the caller's snapshot stores, with
/// the composed GraphSchema's ontology property names linked onto stored keys.
#[cfg(feature = "nl-query")]
pub(crate) fn nl_schema_context(
    view: &crate::graph::GraphView,
    foreign_sources: Option<&std::collections::BTreeSet<String>>,
) -> eg_plan::nl::context::SchemaContext {
    let mut context = eg_plan::nl::context::SchemaContext::from_view(view);
    #[cfg(all(feature = "rdf", feature = "shacl"))]
    context.link_ontology(&crate::server::handlers::rdf::composed_ontology_properties(
        &view.schema_sources,
    ));
    if let Some(sources) = foreign_sources {
        context.declare_foreign_sources(sources.clone());
    }
    context
}

/// Names visible to NL grounding come from the same verified owner-scoped
/// catalog used later by execution. A missing carrier leaves the context
/// unbound, so a foreign stage receives the typed grounding refusal before
/// any served leg or remote source can execute.
#[cfg(all(feature = "nl-query", feature = "federation"))]
async fn nl_foreign_sources(
    state: &Arc<RwLock<ServerState>>,
    read_authority: Option<&GraphReadAuthority>,
) -> Option<std::collections::BTreeSet<String>> {
    let carrier = read_authority.and_then(GraphReadAuthority::carrier)?;
    let state = state.read().await;
    let registry = state
        .foreign_sources
        .registry_for(carrier, &state.isolation);
    Some(registry.registry().names().into_iter().collect())
}

/// The served refusal for a planner answer that was never executed.
#[cfg(feature = "nl-query")]
pub(crate) fn nl_refusal(error: &eg_plan::nl::NlQueryError) -> String {
    use eg_plan::nl::NlQueryError as E;
    match error {
        E::Planner(message) => {
            eg_types::contract::classify_refusal("INVALID_ARGUMENT", "", message)
        }
        E::BudgetExceeded { .. } => format!(
            "{}: {error}",
            eg_types::contract::EngineErrorCode::UqlBudgetExceeded.as_str()
        ),
        E::Ungrounded { steps } => format!(
            "INVALID_ARGUMENT: NlQuery produced invalid UQL: {}",
            ungrounded_detail(steps)
        ),
        other => format!("INVALID_ARGUMENT: NlQuery: {other}"),
    }
}

/// The last step's typed reason first, then every step's text and outcome.
#[cfg(feature = "nl-query")]
fn ungrounded_detail(steps: &[eg_plan::nl::context::NlPlanStep]) -> String {
    let last = steps.last().map_or("no step ran", |s| s.outcome.as_str());
    let trail: Vec<String> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| format!("step {}: `{}` -> {}", i + 1, s.query_text, s.outcome))
        .collect();
    format!("{last} ({})", trail.join("; "))
}

#[cfg(feature = "nl-query")]
pub(crate) async fn handle_nl_query(
    ctx: &QueryHandlerCtx<'_>,
    text: String,
    graph: String,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    Ok(match run_nl_query(ctx, text, graph).await {
        Ok((_uql, rows)) => result_response::<query_results::NlQuery>(req_id, &rows),
        Err(resp) => resp,
    })
}

/// `Method::NlQueryExplain` (EG-FEDERATED-QUERY-R058.2.2.1): exactly [`handle_nl_query`]
/// — the same planner, budget, parse, authority, legs, snapshot and refusals — answered
/// as `{uql, rows}` so the caller sees the exact UQL the engine executed.
#[cfg(feature = "nl-query")]
pub(crate) async fn handle_nl_query_explain(
    ctx: &QueryHandlerCtx<'_>,
    text: String,
    graph: String,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    Ok(match run_nl_query(ctx, text, graph).await {
        Ok((uql, rows)) => result_response::<query_results::NlQueryExplain>(
            req_id,
            &eg_types::nl_query::NlQueryExplainResult { uql, rows },
        ),
        Err(resp) => resp,
    })
}

/// `Method::NlQueryPlan` (EG-FEDERATED-QUERY-R058.2.2.2): expose the typed
/// `eg_plan::nl::plan_only` candidate and pins without taking a graph view,
/// resolving served legs, or executing a query/source read. Schema and source
/// authority are deliberately deferred to the later exact-UQL/foreign-read
/// admission path; plan-only itself has no data context by construction.
#[cfg(feature = "nl-query")]
pub(crate) async fn handle_nl_query_plan_only(
    ctx: &QueryHandlerCtx<'_>,
    text: String,
    graph: String,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let _ = graph;
    let planner = match crate::server::nl::resolve_planner() {
        Ok(Some(planner)) => planner,
        Ok(None) => {
            return Ok(Response::err(
                req_id,
                "ENGINE_UNAVAILABLE: NlQuery: no NL planner configured — set an OpenAI-compatible \
                     endpoint in the Arbora config.json (or \
                     EPISTEMIC_GRAPH_NL_ENDPOINT), or inject one via \
                     server::set_nl_planner"
                    .to_string(),
            ));
        }
        Err(error) => return Ok(Response::err(req_id, nl_config_refusal(&error))),
    };
    let (uql, plan) = match plan_nl_only(planner.as_ref(), &text, req_id) {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let plan_digest = match super::planning::canonical_plan_digest(&plan) {
        Ok(digest) => digest,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let query_digest = match super::planning::foreign_uql_query_digest(&uql, &UqlParams::default())
    {
        Ok(digest) => digest,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    Ok(result_response::<query_results::NlQueryPlan>(
        req_id,
        &eg_types::nl_query::NlQueryPlanResult {
            uql,
            plan_digest,
            query_digest,
        },
    ))
}

/// Plan one NL request through the typed plan-only seam. The empty schema hint
/// is intentional: [`eg_plan::nl::plan_only`] has no graph view or `PlanCtx`,
/// so this helper cannot read data or claim graph/source authority. The exact
/// UQL is parsed through the existing served statement/binding path a second
/// time only to obtain the canonical pin input used by `Uql prepare=true`.
#[cfg(feature = "nl-query")]
fn plan_nl_only(
    planner: &dyn eg_plan::NlPlanner,
    text: &str,
    req_id: u64,
) -> Result<(String, eg_plan::Plan), Response> {
    let candidate =
        eg_plan::nl::plan_only(planner, text, "", eg_plan::nl::NlQueryBudget::default())
            .map_err(|e| Response::err(req_id, nl_refusal(&e)))?;
    let params = UqlParams::default();
    let statement = eg_plan::uql::parse_statement(&candidate.uql, &params)
        .map_err(|e| Response::err(req_id, format!("{}: {}", e.code, e.render(&candidate.uql))))?;
    let plan = eg_plan::uql::serve::binding_plan(&statement);
    Ok((candidate.uql, plan))
}

/// The one NL run both NL handlers share: plan `text` to UQL, then run it on the
/// caller's RLS-filtered snapshot. Answers the planner's exact UQL and the rows, or the
/// refusal response.
#[cfg(feature = "nl-query")]
async fn run_nl_query(
    ctx: &QueryHandlerCtx<'_>,
    text: String,
    graph: String,
) -> Result<(String, Vec<(String, Option<f32>)>), Response> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let core = ctx.core.clone();
    // CONCEPT:EG-KG.query.core-query-input/EG-080 — natural-language → executable query → rows. Resolve
    // the configured/injected `NlPlanner`, turn the NL into a UQL query STRING,
    // then run it through the IDENTICAL UQL pipeline
    // (`eg_plan::uql::parse` + `run_unified`). NO LLM in the engine core and NO
    // new execution path — the produced query rides the deterministic pipeline.
    // The graph was already used for routing; the handler runs against `core`.
    let _ = graph;
    // RLS-filtered off-lock snapshot, exactly like the Sql/Uql reads. The planner
    // is grounded on this same view, so it never sees a name the caller cannot.
    // NOT result-cached: an LLM plan is non-deterministic, so keying a cache on the
    // NL text would risk serving a stale/foreign result.
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    ctx.rls.filter_view(ctx.caller, &mut snap);
    #[cfg(feature = "federation")]
    let foreign_sources = nl_foreign_sources(ctx.state, ctx.read_authority).await;
    #[cfg(not(feature = "federation"))]
    let foreign_sources = None;
    let (uql, plan) = handle_nl_query_plan(&text, &snap, req_id, foreign_sources.as_ref())?;
    // Verified-carrier legs: the committed tsdb scope for `Op::TsScan` fusion
    // (CONCEPT:EG-KG.query.native-time-series) and the CALLER'S owner-scoped foreign
    // registry (EH-373), so an NL-planned `FOREIGN "<name>"` / `Named` `ForeignScan`
    // leg resolves only sources the caller (tenant+principal) registered.
    let legs = ServedPlanLegs::resolve(state, ctx.graph_name, ctx.read_authority, &plan, None, &[])
        .await
        .map_err(|denied| Response::err(req_id, denied))?;
    // The same off-lock run as `Uql`: persistent vector/lexical/spatial/
    // shape indexes and the caller's owner-scoped foreign registry are bound inside it.
    match run_unified_off_lock(state, req_id, &core, Arc::new(snap), plan, legs).await? {
        Ok(rows) => Ok((uql, rows)),
        Err(msg) => Err(Response::err(
            req_id,
            eg_types::contract::classify_refusal("INVALID_ARGUMENT", "", &msg),
        )),
    }
}

#[cfg(all(test, feature = "nl-query"))]
mod tests {
    // Keep the scan outside its own fixtures and the unrelated transaction handler.
    // Missing boundaries must fail rather than silently scan an empty slice.
    fn check_nl_cache_isolation(src: &str) -> Result<(), &'static str> {
        let (production, _) = src
            .split_once("\n#[cfg(all(test, feature = \"nl-query\"))]\nmod tests {")
            .ok_or("missing test-module boundary")?;
        let (_, nl) = production
            .split_once("\npub(crate) fn handle_nl_query_plan(")
            .ok_or("missing NL planner boundary")?;
        if !nl.contains("pub(crate) async fn handle_nl_query(")
            || !nl.contains("result_response::<query_results::NlQuery>")
        {
            return Err("missing uncached NL handler response path");
        }
        if nl.contains("ResultCache") || nl.contains("served_response") {
            return Err("NL handlers reference the result cache");
        }
        Ok(())
    }

    // spec: EG-FEDERATED-QUERY-R058.3.1
    #[test]
    fn nl_query_handler_never_touches_result_cache() {
        assert_eq!(check_nl_cache_isolation(include_str!("nl.rs")), Ok(()));
    }

    // spec: EG-FEDERATED-QUERY-R058.3.1
    #[test]
    fn nl_cache_guard_rejects_injected_cache_references() {
        let src = include_str!("nl.rs");
        for reference in [
            "ResultCache::default()",
            "served_response(req_id, rows)",
            "served_response::<query_results::NlQuery>(req_id, rows)",
        ] {
            let mutated = src.replacen(
                "    let state = ctx.state;",
                &format!("    let _ = {reference};\n    let state = ctx.state;"),
                1,
            );
            assert_ne!(mutated, src, "injection anchor must exist");
            assert_eq!(
                check_nl_cache_isolation(&mutated),
                Err("NL handlers reference the result cache"),
                "missed {reference}"
            );
        }
    }

    // spec: EG-FEDERATED-QUERY-R058.3.1
    #[test]
    fn nl_cache_guard_fails_closed_on_missing_boundaries_or_response() {
        let src = include_str!("nl.rs");
        for required in [
            "\n#[cfg(all(test, feature = \"nl-query\"))]\nmod tests {",
            "\npub(crate) fn handle_nl_query_plan(",
            "pub(crate) async fn handle_nl_query(",
            "result_response::<query_results::NlQuery>",
        ] {
            let mutated = src.replacen(required, "missing", 1);
            assert_ne!(mutated, src, "boundary anchor must exist");
            assert!(check_nl_cache_isolation(&mutated).is_err());
        }
    }
}

#[cfg(all(test, feature = "nl-query"))]
mod nl_config_refusal_tests {
    // spec: EG-FEDERATED-QUERY-R058.5
    #[test]
    fn invalid_planner_config_is_refused_with_code_and_key() {
        let msg = super::nl_config_refusal(&eg_plan::NlConfigError::ReadTimeoutOutOfRange(0));
        assert!(msg.starts_with("ENGINE_UNAVAILABLE:"), "{msg}");
        assert!(msg.contains("read_timeout_secs"), "{msg}");
        let msg = super::nl_config_refusal(&eg_plan::NlConfigError::ExtraBodyReservedKey(
            "model".into(),
        ));
        assert!(msg.contains("extra_body") && msg.contains("model"), "{msg}");
    }
}

#[cfg(all(test, feature = "nl-query"))]
mod nl_plan_only_tests {
    struct PurePlanner;

    impl eg_plan::NlPlanner for PurePlanner {
        fn plan(&self, nl: &str, schema_hint: &str) -> Result<String, String> {
            assert!(
                schema_hint.is_empty(),
                "plan-only must not derive a graph hint"
            );
            Ok(nl.to_string())
        }
    }

    // spec: EG-FEDERATED-QUERY-R058.1
    #[test]
    fn served_plan_helper_uses_the_view_free_typed_seam() {
        let (uql, plan) = super::plan_nl_only(&PurePlanner, "MATCH (:Doc)", 1).expect("plan");
        assert_eq!(uql, "MATCH (:Doc)");
        assert_eq!(plan.ops.len(), 1);
    }
}
