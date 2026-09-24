//! `Method::Uql` (UQL-07/08/09): a full UQL statement — typed `$name` params bound as
//! values, `EXPLAIN`/`PROFILE`, `LET … FROM/JOIN` programs and `RETURN` score channels —
//! run over the SAME RLS-filtered off-lock snapshot and served index bindings as
//! `UnifiedQueryText` (`run_unified_off_lock_with`). Read-only; no result cache (a
//! statement's EXPLAIN/PROFILE output is not a cacheable row set).

use super::*;

/// Parse and run one UQL statement.
#[cfg(feature = "query")]
pub(crate) async fn handle_uql(
    ctx: &QueryHandlerCtx<'_>,
    text: String,
    params: std::collections::BTreeMap<String, eg_types::wire::UqlParam>,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let stmt = match eg_plan::uql::parse_statement(&text, &params) {
        Ok(stmt) => stmt,
        Err(e) => return Ok(Response::err(req_id, e.render(&text))),
    };
    let binding = eg_plan::uql::serve::binding_plan(&stmt);
    #[cfg(feature = "tsdb")]
    let tsdb_scope = match served_tsdb_scope(&binding, ctx.graph_name, ctx.read_authority) {
        Ok(scope) => scope,
        Err(denied) => return Ok(Response::err(req_id, denied)),
    };
    let snap = uql_snapshot(ctx);
    let result = run_unified_off_lock_with(
        ctx.state,
        req_id,
        ctx.core,
        snap,
        binding,
        #[cfg(feature = "tsdb")]
        tsdb_scope,
        move |_plan, plan_ctx| eg_plan::uql::serve::run_statement(&stmt, plan_ctx),
    )
    .await;
    Ok(match result {
        Ok(Ok(body)) => result_response::<query_results::Uql>(req_id, &body),
        Ok(Err(msg)) => Response::err(req_id, format!("UQL error: {msg}")),
        Err(resp) => resp,
    })
}

/// The caller's RLS-filtered snapshot (the same helpers `UnifiedQueryText` uses).
#[cfg(feature = "query")]
fn uql_snapshot(ctx: &QueryHandlerCtx<'_>) -> Arc<crate::graph::GraphView> {
    #[cfg(all(feature = "result-cache", feature = "security"))]
    let (snap, _version) = versioned_rls_snapshot(ctx.core, ctx.caller, ctx.rls);
    #[cfg(all(feature = "result-cache", not(feature = "security")))]
    let snap = Arc::new(ctx.core.analysis_snapshot());
    #[cfg(not(feature = "result-cache"))]
    let snap = rls_snapshot(
        ctx.core,
        #[cfg(feature = "security")]
        ctx.caller,
        #[cfg(feature = "security")]
        ctx.rls,
    );
    snap
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
        assert!(error.contains("UQL_UNBOUND_PARAMETER"), "{error}");
    }
}
