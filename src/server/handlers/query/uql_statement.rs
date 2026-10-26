//! `Method::Uql` (UQL-07/08/09, EH-434 — the one query-text surface): a full UQL
//! statement — typed `$name` params bound as values, `EXPLAIN`/`PROFILE`, `LET …
//! FROM/JOIN` programs, `RETURN` score channels and `WITH PROOF`/`WITH KNOWLEDGE` row
//! annotations — run over the SAME RLS-filtered off-lock snapshot and served index
//! bindings as `UnifiedQuery` (`run_unified_off_lock_with`). Read-only.
//!
//! Result cache: an executed statement's answer is cached exactly like a `UnifiedQuery`
//! answer — keyed on the text, the bound params and the verified legs under the caller's
//! RLS context; a plain pipeline with a bounded dependency set in the dependency-scoped
//! namespace. A `PROFILE` answer carries wall times and is never cached, nor is a
//! statement whose legs read the decision log (it moves without the graph version).

use super::*;

/// Parse and run one UQL statement.
#[cfg(feature = "query")]
pub(crate) async fn handle_uql(
    ctx: &QueryHandlerCtx<'_>,
    text: String,
    params: UqlParams,
    prepare: bool,
    foreign_execution: Option<eg_types::foreign_read::ForeignReadExecutionRef>,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    if prepare && foreign_execution.is_some() {
        return Ok(Response::err(
            req_id,
            "INVALID_ARGUMENT: prepare cannot carry a foreign-read execution",
        ));
    }
    // Compute both pins once from the exact request and bound plan.  The
    // foreign-read lifecycle accepts only these server-owned values; callers
    // never need to reproduce Rust's named-MessagePack serialization.
    let query_digest = match super::planning::foreign_uql_query_digest(&text, &params) {
        Ok(digest) => digest,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    if let Some(execution) = foreign_execution.as_ref() {
        if execution.query_digest != query_digest {
            return Ok(Response::err(
                req_id,
                "FOREIGN_READ_QUERY_MISMATCH: query digest differs",
            ));
        }
    }
    let (stmt, binding) = match bind_served_statement(req_id, &text, &params) {
        Ok(bound) => bound,
        Err(response) => return Ok(response),
    };
    let stmt_for_run = if prepare {
        let mut prepared = stmt.clone();
        prepared.mode = eg_plan::uql::Mode::Explain;
        prepared
    } else {
        stmt
    };
    #[cfg(feature = "federation")]
    let plan_digest = match crate::server::foreign_read::canonical_plan_digest(&binding) {
        Ok(digest) => digest,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let legs = match ctx
        .served_legs_with_foreign_execution(&binding, foreign_execution.as_ref())
        .await
    {
        Ok(legs) => legs,
        Err(resp) => return Ok(resp),
    };
    #[cfg(feature = "result-cache")]
    let key = match uql_cache_key(ctx, &text, &params, &stmt_for_run, &legs) {
        Ok(key) => key,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    #[cfg(feature = "result-cache")]
    if let Some(response) = cached_served_response::<query_results::Uql>(req_id, ctx.core, &key) {
        return Ok(response);
    }
    let (snap, version) = sql_read_snapshot(
        ctx.core,
        #[cfg(feature = "security")]
        ctx.caller,
        #[cfg(feature = "security")]
        ctx.rls,
    );
    #[cfg(not(feature = "result-cache"))]
    let _ = version;
    let result = run_unified_off_lock_with(
        ctx.state,
        req_id,
        ctx.core,
        snap,
        binding,
        legs,
        move |_plan, plan_ctx| {
            let mut result = eg_plan::uql::serve::run_statement(&stmt_for_run, plan_ctx)?;
            #[cfg(feature = "federation")]
            if let eg_types::wire::UqlResult::Explain {
                plan_digest: reported_plan_digest,
                query_digest: reported_query_digest,
                ..
            } = &mut result
            {
                *reported_plan_digest = Some(plan_digest);
                *reported_query_digest = Some(query_digest);
            }
            Ok(result)
        },
    )
    .await;
    Ok(served_response::<query_results::Uql>(
        req_id,
        result,
        "UQL",
        #[cfg(feature = "result-cache")]
        ctx.core,
        #[cfg(feature = "result-cache")]
        key.map(|(hash, dep)| CacheSlot { hash, dep, version }),
    ))
}

/// The statement's cache key and dependency set; `None` when its answer is not
/// cacheable (`PROFILE`, or legs outside the graph version).
#[cfg(all(feature = "query", feature = "result-cache"))]
fn uql_cache_key(
    ctx: &QueryHandlerCtx<'_>,
    text: &str,
    params: &std::collections::BTreeMap<String, eg_types::wire::UqlParam>,
    stmt: &eg_plan::uql::Statement,
    legs: &ServedPlanLegs,
) -> Result<Option<(u128, Option<eg_core::dep_scope::DepSet>)>, String> {
    use eg_plan::uql::{Body, Mode};
    if stmt.mode == Mode::Profile || !legs.cache_admissible() {
        return Ok(None);
    }
    let payload = msgpack_bytes(&(text, params))?;
    let hash = served_cache_hash(ctx, "uql", payload, legs);
    let dep = match (&stmt.body, stmt.mode, stmt.annotations.any()) {
        (Body::Pipeline(plan), Mode::Run, false) => {
            plan_dependency_set(plan, ctx.core.dep_probe().embedding_generation())
        }
        _ => None,
    };
    Ok(Some((hash, dep)))
}

/// Served `Method::Uql`: typed params bind as values, EXPLAIN returns the report, and a
/// parse error carries its stable code.
#[cfg(all(test, feature = "query", feature = "redb", feature = "security"))]
mod served_tests {
    use super::super::current_auth_test_support::prelude::*;
    use eg_types::wire::{UqlParam, UqlResult};

    const SECRET: &str = "uql-served-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        persisted_state(SECRET, current_isolation())
    }

    fn req(id: u64, method: Method) -> Request {
        current_request(SECRET, id, "__commons__", method)
    }

    async fn add_doc(state: &Arc<RwLock<ServerState>>, id: u64, node: &str, year: i64) {
        let props = serde_json::json!({ "type": "Doc", "year": year });
        let bytes = rmp_serde::to_vec_named(&props).unwrap();
        let method = Method::AddNode {
            node_id: node.into(),
            properties_msgpack: bytes,
        };
        let r = dispatch_on_heap(state, req(id, method)).await;
        assert!(r.error.is_none(), "AddNode failed: {:?}", r.error);
    }

    async fn uql(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        text: &str,
        params: &[(&str, UqlParam)],
    ) -> Response {
        let params = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let method = Method::Uql {
            text: text.into(),
            params,
            prepare: false,
            foreign_execution: None,
        };
        dispatch_on_heap(state, req(id, method)).await
    }

    fn body(resp: &Response) -> UqlResult {
        match &resp.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).unwrap(),
            other => panic!("expected Raw result, got {other:?} / {:?}", resp.error),
        }
    }

    #[tokio::test]
    async fn params_bind_explain_reports_and_errors_carry_codes() {
        let state = state();
        add_doc(&state, 1, "d1", 2020).await;
        add_doc(&state, 2, "d2", 2024).await;
        let run = uql(
            &state,
            3,
            "MATCH (:Doc) WHERE year >= $min",
            &[("min", UqlParam::Num(2021.0))],
        )
        .await;
        let UqlResult::Rows { rows, .. } = body(&run) else {
            panic!("rows expected")
        };
        assert_eq!(
            rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["d2"]
        );

        let explain = uql(&state, 4, "EXPLAIN MATCH (:Doc) |> LIMIT 1", &[]).await;
        assert!(matches!(body(&explain), UqlResult::Explain { .. }));

        let unbound = uql(&state, 5, "MATCH (:Doc) |> LIMIT $k", &[]).await;
        let error = unbound.error.expect("an unbound parameter is an error");
        assert_eq!(error, "UQL_UNBOUND_PARAMETER");
        assert!(unbound
            .error_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("UQL_UNBOUND_PARAMETER")));
    }

    #[cfg(feature = "federation")]
    #[tokio::test]
    async fn explain_returns_server_owned_foreign_query_pins() {
        let state = state();
        let text = "EXPLAIN MATCH (:Doc) |> LIMIT 1";
        let response = uql(&state, 6, text, &[]).await;
        let UqlResult::Explain {
            plan_digest: Some(plan_digest),
            query_digest: Some(query_digest),
            ..
        } = body(&response)
        else {
            panic!("EXPLAIN must carry server-owned foreign-read pins: {response:?}");
        };

        let params = std::collections::BTreeMap::new();
        let statement = eg_plan::uql::parse_statement(text, &params).expect("parse explain");
        let binding = eg_plan::uql::serve::binding_plan(&statement);
        assert_eq!(
            query_digest,
            super::super::planning::foreign_uql_query_digest(text, &params).expect("query digest")
        );
        assert_eq!(
            plan_digest,
            crate::server::foreign_read::canonical_plan_digest(&binding).expect("plan digest")
        );
    }

    #[cfg(feature = "federation")]
    #[tokio::test]
    async fn prepare_returns_pins_for_the_exact_executable_text() {
        let state = state();
        let text = "MATCH (:Doc) |> LIMIT 1";
        let response = dispatch_on_heap(
            &state,
            req(
                7,
                Method::Uql {
                    text: text.into(),
                    params: Default::default(),
                    prepare: true,
                    foreign_execution: None,
                },
            ),
        )
        .await;
        let UqlResult::Explain {
            plan_digest: Some(plan_digest),
            query_digest: Some(query_digest),
            ..
        } = body(&response)
        else {
            panic!("prepare must return server-owned foreign-read pins: {response:?}");
        };
        let params = std::collections::BTreeMap::new();
        let statement = eg_plan::uql::parse_statement(text, &params).expect("parse query");
        let binding = eg_plan::uql::serve::binding_plan(&statement);
        assert_eq!(
            query_digest,
            super::super::planning::foreign_uql_query_digest(text, &params).expect("query digest")
        );
        assert_eq!(
            plan_digest,
            crate::server::foreign_read::canonical_plan_digest(&binding).expect("plan digest")
        );
    }
}
