//! `NlQueryExplain` through the FULL served dispatch chain (EG-FEDERATED-QUERY-R058.2.2.1):
//! it answers the planner's exact UQL with the SAME rows `NlQuery` answers, and refuses
//! exactly as `NlQuery` refuses.

use super::*;

/// Echoes the NL text back as the UQL to run, so the planner's output is known exactly.
/// Behaves like the federation tests' echo planner, so whichever test injects the
/// process-wide planner first, every NL test sees the same echo behaviour.
struct EchoPlanner;

impl eg_plan::NlPlanner for EchoPlanner {
    fn plan(&self, nl: &str, _schema_hint: &str) -> Result<String, String> {
        Ok(nl.to_string())
    }
}

fn nl(text: &str) -> Method {
    Method::NlQuery {
        text: text.into(),
        graph: "__commons__".into(),
    }
}

fn nl_explain(text: &str) -> Method {
    Method::NlQueryExplain {
        text: text.into(),
        graph: "__commons__".into(),
    }
}

async fn send(state: &Arc<RwLock<ServerState>>, id: u64, method: Method) -> Response {
    dispatch_on_heap(state, request(id, "__commons__", None, method)).await
}

fn explain_body(resp: &Response) -> eg_types::nl_query::NlQueryExplainResult {
    match &resp.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).unwrap(),
        other => panic!(
            "expected Raw explain result, got {other:?} / {:?}",
            resp.error
        ),
    }
}

// spec: EG-FEDERATED-QUERY-R058.2.2.1
#[tokio::test]
async fn nl_query_explain_returns_planner_uql_and_nl_query_rows() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    crate::server::set_nl_planner(Arc::new(EchoPlanner));
    let state = test_state();
    build_unified_fixture(&state).await;
    let text = "MATCH (:Doc) |> LIMIT 3";

    let plain = send(&state, 940, nl(text)).await;
    assert_ok(&plain);
    let explained = send(&state, 941, nl_explain(text)).await;
    assert_ok(&explained);

    let body = explain_body(&explained);
    assert_eq!(
        body.uql, text,
        "the explain result carries the planner's exact UQL"
    );
    let plain_rows: Vec<(String, Option<f32>)> = match &plain.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).unwrap(),
        other => panic!("expected Raw rows, got {other:?}"),
    };
    assert_eq!(plain_rows.len(), 3);
    assert_eq!(body.rows, plain_rows, "explain rows equal NlQuery rows");
    // The NlQuery wire shape is unchanged: a bare `[id, score|nil]` array, no `uql`.
    assert_eq!(unified_ids(&plain).len(), 3);
}

// spec: EG-FEDERATED-QUERY-R058.2.2.1
#[tokio::test]
async fn nl_query_explain_refuses_exactly_like_nl_query() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    crate::server::set_nl_planner(Arc::new(EchoPlanner));
    let state = test_state();
    build_unified_fixture(&state).await;
    let over_budget = "x".repeat(eg_plan::nl::NlQueryBudget::default().max_uql_len + 1);
    for (id, text, code) in [
        (950, "MATCH ((( not uql", "INVALID_ARGUMENT"),
        (952, over_budget.as_str(), "UQL_BUDGET_EXCEEDED"),
    ] {
        let plain = send(&state, id, nl(text)).await;
        let explained = send(&state, id, nl_explain(text)).await;
        assert!(plain.result.is_none() && explained.result.is_none());
        assert_eq!(
            plain.error.as_deref(),
            Some(code),
            "{:?}",
            plain.error_detail
        );
        assert_eq!(explained.error, plain.error, "refusal code is identical");
        assert_eq!(
            explained.error_detail, plain.error_detail,
            "refusal detail is identical"
        );
    }
}
