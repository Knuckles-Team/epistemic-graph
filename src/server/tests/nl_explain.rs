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

/// The unified fixture graph with the echo planner injected.
async fn echo_planned_fixture() -> Arc<RwLock<ServerState>> {
    crate::server::set_nl_planner(Arc::new(EchoPlanner));
    let state = test_state();
    build_unified_fixture(&state).await;
    state
}

// spec: EG-FEDERATED-QUERY-R058.2.2.1
#[tokio::test]
async fn nl_query_explain_returns_planner_uql_and_nl_query_rows() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = echo_planned_fixture().await;
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
    let state = echo_planned_fixture().await;
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

// spec: EG-FEDERATED-QUERY-R058.3.2
#[tokio::test]
async fn nl_mutating_candidates_refuse_without_changing_graph_data() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = echo_planned_fixture().await;
    let core = state
        .read()
        .await
        .registry
        .get("__commons__")
        .expect("fixture graph")
        .core
        .clone();
    let before = core.analysis_snapshot();
    assert_eq!(before.graph.node_count(), 7);
    assert_eq!(before.graph.edge_count(), 4);

    for (index, text) in [
        "CREATE (n:Doc {id: 'injected'})",
        "INSERT INTO nodes (id, type) VALUES ('injected', 'Doc')",
        "MATCH (:Doc) |> DELETE",
        "MATCH (:Doc) |> SET year = 1900",
        "MATCH (:Doc) |> LIMIT 1; DELETE FROM nodes",
    ]
    .into_iter()
    .enumerate()
    {
        let id = 980 + 2 * index as u64;
        let plain = send(&state, id, nl(text)).await;
        let explained = send(&state, id + 1, nl_explain(text)).await;
        assert_eq!(
            plain.error.as_deref(),
            Some("INVALID_ARGUMENT"),
            "{text}: {:?}",
            plain.error_detail
        );
        assert!(plain.result.is_none(), "{text}: no partial rows");
        assert!(explained.result.is_none(), "{text}: no explain result");
        assert_eq!(explained.error, plain.error, "{text}");
        assert_eq!(explained.error_detail, plain.error_detail, "{text}");
        let detail = plain.error_detail.as_deref().expect("parse refusal detail");
        assert!(detail.contains(text), "{detail}");
        assert!(detail.contains("step 2"), "{detail}");

        let after = core.analysis_snapshot();
        assert_eq!(
            after.graph.node_count(),
            before.graph.node_count(),
            "{text}"
        );
        assert_eq!(
            after.graph.edge_count(),
            before.graph.edge_count(),
            "{text}"
        );
        assert_eq!(after.node_map, before.node_map, "{text}: node identities");
        assert_eq!(after.node_properties, before.node_properties, "{text}");
        assert_eq!(after.edge_properties, before.edge_properties, "{text}");
    }
}

// spec: EG-FEDERATED-QUERY-R058.4.7.1
#[tokio::test]
async fn nl_query_grounds_names_on_the_graph_schema_before_running() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = echo_planned_fixture().await;

    // A spelling variant of a stored label and property runs as the stored name,
    // and the explain result discloses the UQL that actually ran.
    let explained = send(&state, 960, nl_explain("MATCH (:doc) |> WHERE Year > 2024")).await;
    assert_ok(&explained);
    let body = explain_body(&explained);
    assert!(body.uql.contains("(:Doc)"), "{}", body.uql);
    assert!(body.uql.contains("year"), "{}", body.uql);
    let mut ids: Vec<&str> = body.rows.iter().map(|(id, _)| id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["d1", "d2", "d5"]);

    // A property no Doc stores is refused with the typed reason, never run.
    let text = "MATCH (:Doc) |> WHERE source = 'ServiceNow'";
    let plain = send(&state, 961, nl(text)).await;
    let explained = send(&state, 962, nl_explain(text)).await;
    assert!(plain.result.is_none() && explained.result.is_none());
    assert_eq!(plain.error.as_deref(), Some("INVALID_ARGUMENT"));
    let detail = plain.error_detail.clone().unwrap_or_default();
    assert!(detail.contains("NL_SCHEMA_UNKNOWN_PROPERTY"), "{detail}");
    assert!(detail.contains("step 2"), "{detail}");
    assert_eq!(explained.error_detail, plain.error_detail);
}

// spec: EG-FEDERATED-QUERY-R058.4.7.1
#[cfg(feature = "federation")]
#[tokio::test]
async fn nl_query_grounds_foreign_names_from_the_verified_owner_registry() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = {
        crate::server::set_nl_planner(Arc::new(EchoPlanner));
        let state = multi_tenant_state().await;
        build_unified_fixture(&state).await;
        state
    };

    // Register under another principal. The query below defaults to `system`,
    // whose owner-scoped catalog must not disclose this source.
    let registered = dispatch_as(
        &state,
        963,
        "worker1",
        Method::RegisterForeignSource {
            name: "known".into(),
            source: eg_types::wire::ForeignSourceSpec::HttpJson {
                url: "http://example.invalid/worker1-secret".into(),
                json_path: "data".into(),
                field_map: eg_types::wire::HttpFieldMap {
                    id: "id".into(),
                    score: None,
                    columns: Default::default(),
                },
            },
        },
    )
    .await;
    assert_ok(&registered);

    // `known` never reaches the executor or the remote endpoint: grounding
    // rejects it as absent from system's registry and does not disclose the
    // other principal's source name, owner or endpoint.
    let response = send(&state, 964, nl("MATCH (:Doc) |> FOREIGN \"known\"")).await;
    assert!(response.result.is_none());
    assert_eq!(response.error.as_deref(), Some("INVALID_ARGUMENT"));
    let detail = response.error_detail.unwrap_or_default();
    assert!(detail.contains("NL_SCHEMA_UNKNOWN_SOURCE"), "{detail}");
    assert!(detail.contains("sources: none"), "{detail}");
    assert!(!detail.contains("worker1"), "{detail}");
    assert!(
        !detail.contains("example.invalid/worker1-secret"),
        "{detail}"
    );
}
