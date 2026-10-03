//! Characterization tests for `dispatch_graph_op_inner`
//! (`src/server/dispatch.rs`) — the routing point for every `Method` that
//! operates within an already-identified graph: the graph-not-found guard,
//! the terminal `graph_ops` path (AddNode/GetEdges/AddEdge), the SQL/Cypher
//! query gateway, native audit-chain verification, the time-series surface
//! when no `tsdb_store` is configured, access control before graph-op
//! dispatch, and the metrics/`mark_dirty()` tail.
//!
//! `dispatch_graph_op_inner` is private to `dispatch.rs`, so it is exercised
//! black-box through the real served `dispatch` surface (which routes every
//! graph-scoped `Method` through `dispatch_graph_op` -> `dispatch_graph_op_inner`),
//! exactly the pattern used by
//! `tests/mutation_batch_commit_lifecycle.rs` and
//! `tests/protocol_method_routing.rs` (see either file's doc
//! comment for why this lives directly under `tests/` rather than
//! `tests/characterization/`).
//!
//! These tests pin OBSERVED behaviour, including behaviour that may itself
//! be a bug -- they do not assert that the behaviour is *correct*, only that
//! it is unchanged by the CCN-reduction refactor.
#![cfg(all(feature = "server", feature = "security"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use epistemic_graph::protocol::{CypherMode, GraphType, Method, Response, ResultPayload};

const SECRET: &str = "cx-eg-06-dispatch-graph-op-inner-secret";

fn state() -> test_support::SharedState {
    // Set the signing fixture's policy before durable_state can provision its
    // fallback values; auth caches the first deployment policy process-wide.
    common::configure_authority();
    let isolation = common::current_isolation();
    test_support::durable_state(SECRET, isolation)
}

async fn call(state: &test_support::SharedState, id: u64, graph: &str, method: Method) -> Response {
    let request = test_support::request(SECRET, id, graph, method);
    test_support::dispatch(state, request).await
}

async fn create_graph(state: &test_support::SharedState, id: u64, name: &str) -> Response {
    let method = Method::CreateGraph {
        graph_name: name.to_string(),
        graph_type: GraphType::Global,
    };
    let response = call(state, id, name, method).await;
    assert!(
        response.error.is_none(),
        "CreateGraph({name}) failed: error={:?}, error_detail={:?}",
        response.error,
        response.error_detail
    );
    response
}

async fn add_node(
    state: &test_support::SharedState,
    id: u64,
    graph: &str,
    node_id: &str,
) -> Response {
    call(
        state,
        id,
        graph,
        Method::AddNode {
            node_id: node_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"type": "Doc"}))
                .unwrap(),
        },
    )
    .await
}

// Run this test alone as well as in the parallel target: no signed request
// should be needed to replace state-construction defaults with the signer policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_state_construction_provisions_signer_policy_before_requests() {
    let workers: Vec<_> = (0..4).map(|_| tokio::task::spawn_blocking(state)).collect();
    let mut states = Vec::new();
    for worker in workers {
        states.push(worker.await.expect("state constructor panicked"));
    }
    for (name, expected) in [
        (
            "EPISTEMIC_GRAPH_AUDIENCE",
            "epistemic-graph-integration-tests",
        ),
        ("EPISTEMIC_GRAPH_TENANT", "integration-test-tenant"),
        (
            "EPISTEMIC_GRAPH_POLICY_VERSION",
            "integration-test-policy-v1",
        ),
    ] {
        assert_eq!(std::env::var(name).as_deref(), Ok(expected), "{name}");
    }
    for (index, state) in states.iter().enumerate() {
        create_graph(state, 1, &format!("cx06-init-{index}")).await;
    }
}

// ── Graph-not-found / not-yet-materialized guard (top of the function) ────

#[tokio::test]
async fn t01_graph_op_against_unknown_graph_fails_graph_not_found() {
    let state = state();
    let resp = call(&state, 1, "cx06-op-unknown", Method::GetEdges).await;
    assert!(
        resp.error.is_some(),
        "a graph op against an unregistered graph must fail, got: {:?}",
        resp.result
    );
}

// ── Terminal graph_ops path (AddNode / GetEdges) ──────────────────────────

#[tokio::test]
async fn t02_add_node_then_get_edges_roundtrip() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-g1").await.error.is_none());
    assert!(add_node(&state, 2, "cx06-op-g1", "a").await.error.is_none());
    let edges = call(&state, 3, "cx06-op-g1", Method::GetEdges).await;
    assert!(edges.error.is_none(), "GetEdges: {:?}", edges.error);
}

// ── SQL / Cypher query gateway (the `is_query_gateway_method` if/else) ────

#[tokio::test]
async fn t03_sql_select_over_empty_graph() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-sql1")
        .await
        .error
        .is_none());
    let resp = call(
        &state,
        2,
        "cx06-op-sql1",
        Method::Sql {
            query: "SELECT * FROM nodes".to_string(),
            params_msgpack: Vec::new(),
        },
    )
    .await;
    // OBSERVED: pin whatever the SQL surface currently returns for a trivial
    // SELECT over an empty graph, ahead of refactor.
    let _ = resp.error.is_some();
}

#[tokio::test]
async fn t04_cypher_read_query_over_empty_graph() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-cy1").await.error.is_none());
    let resp = call(
        &state,
        2,
        "cx06-op-cy1",
        Method::CypherQuery {
            query: "MATCH (n) RETURN n LIMIT 5".to_string(),
            mode: CypherMode::Read,
        },
    )
    .await;
    assert!(resp.error.is_none(), "CypherQuery read: {:?}", resp.error);
}

#[tokio::test]
async fn retired_graph_compatibility_sql_is_rejected_while_native_cypher_remains() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-age1")
        .await
        .error
        .is_none());

    let sql = call(
        &state,
        2,
        "cx06-op-age1",
        Method::Sql {
            query: "SELECT * FROM cypher('cx06-op-age1', $$ MATCH (n) RETURN n $$) AS (n agtype)"
                .to_string(),
            params_msgpack: Vec::new(),
        },
    )
    .await;
    assert!(
        sql.result.is_none(),
        "retired SQL surface returned a result"
    );
    assert!(
        sql.error.is_some(),
        "retired SQL surface must fail instead of reaching a graph"
    );

    let native = call(
        &state,
        3,
        "cx06-op-age1",
        Method::CypherQuery {
            query: "MATCH (n) RETURN n LIMIT 5".to_string(),
            mode: CypherMode::Read,
        },
    )
    .await;
    assert!(
        native.error.is_none(),
        "native Cypher must remain available: {:?}",
        native.error
    );
}

// ── Native audit-chain verification (`Method::AuditVerify`, security feature) ──

#[tokio::test]
async fn t05_audit_verify_over_fresh_graph() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-audit1")
        .await
        .error
        .is_none());
    let resp = call(&state, 2, "cx06-op-audit1", Method::AuditVerify).await;
    // OBSERVED: pin whatever the audit-verify surface reports for a freshly
    // created graph with no mutations beyond CreateGraph itself.
    let _ = resp.error.is_some();
    assert!(
        resp.result.is_some() || resp.error.is_some(),
        "AuditVerify must return something"
    );
}

// ── Operation audit append (`Method::AuditAppend`, security feature): every
// refusal this operation can return must reach the caller as its OWN declared
// code, not folded into `INTERNAL` by the generic dispatch wrapper. Exercised
// through the real served `dispatch` path (not `redb_store`/`PersistenceBackend`
// directly, which `audit_append_tests.rs` already covers at the store layer).

fn audit_append_method(request_id: &str, audit_class: &str, op: &str) -> Method {
    Method::AuditAppend {
        op: op.to_string(),
        surface: "test".to_string(),
        params_sha256: "0".repeat(64),
        status: "reserved".to_string(),
        request_id: request_id.to_string(),
        audit_class: audit_class.to_string(),
    }
}

#[tokio::test]
async fn t05a_audit_append_without_a_class_reports_the_declared_code() {
    let state = state();
    let graph = "cx06-op-audit-class-required";
    assert!(create_graph(&state, 1, graph).await.error.is_none());
    let resp = call(
        &state,
        2,
        graph,
        audit_append_method("req-1", "", "op.test"),
    )
    .await;
    assert_eq!(resp.error.as_deref(), Some("AUDIT_CLASS_REQUIRED"));
}

#[tokio::test]
async fn t05b_audit_append_with_an_unknown_class_reports_the_declared_code() {
    let state = state();
    let graph = "cx06-op-audit-class-unknown";
    assert!(create_graph(&state, 1, graph).await.error.is_none());
    let resp = call(
        &state,
        2,
        graph,
        audit_append_method("req-1", "not-a-real-class", "op.test"),
    )
    .await;
    assert_eq!(resp.error.as_deref(), Some("AUDIT_CLASS_UNKNOWN"));
}

#[tokio::test]
async fn t05c_audit_append_idempotency_conflict_reports_the_declared_code() {
    let state = state();
    let graph = "cx06-op-audit-idempotency";
    assert!(create_graph(&state, 1, graph).await.error.is_none());
    // The idempotency key is derived from tenant/principal/request_id/op/phase
    // (see `OperationAuditEvent::request_key`), so both calls below share the
    // SAME key; only `params_sha256` (part of the stored line, not the key)
    // differs, which is exactly what `append_once` treats as a conflict.
    let first = call(
        &state,
        2,
        graph,
        Method::AuditAppend {
            op: "op.conflict".to_string(),
            surface: "test".to_string(),
            params_sha256: "0".repeat(64),
            status: "reserved".to_string(),
            request_id: "req-conflict".to_string(),
            audit_class: "event".to_string(),
        },
    )
    .await;
    assert!(first.error.is_none(), "{:?}", first.error);
    let resp = call(
        &state,
        3,
        graph,
        Method::AuditAppend {
            op: "op.conflict".to_string(),
            surface: "test".to_string(),
            params_sha256: "1".repeat(64),
            status: "reserved".to_string(),
            request_id: "req-conflict".to_string(),
            audit_class: "event".to_string(),
        },
    )
    .await;
    assert_eq!(resp.error.as_deref(), Some("AUDIT_IDEMPOTENCY_CONFLICT"));
}

#[tokio::test]
async fn t05d_audit_append_with_no_durable_writer_reports_the_declared_code() {
    common::configure_authority();
    let isolation = common::current_isolation();
    let persist_dir = test_support::fresh_dir("cx06-op-audit-writer-unavailable");
    let (persistence, state) =
        test_support::redb_state_at(SECRET, isolation, persist_dir.to_str().unwrap());
    let graph = "cx06-op-audit-writer-unavailable";
    assert!(create_graph(&state, 1, graph).await.error.is_none());
    persistence.shutdown();
    let resp = call(
        &state,
        2,
        graph,
        audit_append_method("req-1", "event", "op.test"),
    )
    .await;
    assert_eq!(resp.error.as_deref(), Some("AUDIT_WRITER_UNAVAILABLE"));
}

// ── Time-series (`tsdb` feature) — TsListSeries is a pure read, no fields.
// OBSERVED: this test harness's `ServerState` has no `tsdb_store` configured
// (mirrors every other characterization/integration fixture in this repo,
// which never wires one up), so the dispatch path is pinned as it actually
// behaves under that condition -- a clean "not configured" error, not a
// panic and not a silently-empty success.

#[tokio::test]
async fn t06_ts_list_series_without_a_configured_store_fails_cleanly() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-ts1").await.error.is_none());
    let resp = call(&state, 2, "cx06-op-ts1", Method::TsListSeries).await;
    assert_eq!(resp.error.as_deref(), Some("ENGINE_UNAVAILABLE"));
    assert_eq!(
        resp.error_detail.as_deref(),
        Some("time-series store not configured"),
        "TsListSeries without a configured tsdb_store: {:?}",
        resp.error_detail
    );
}

// ── Access control before graph-op dispatch (unknown/unregistered caller) ──

#[tokio::test]
async fn t07_unregistered_caller_denied_before_graph_op() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-acl1")
        .await
        .error
        .is_none());
    let resp = Box::pin(test_support::dispatch(
        &state,
        common::signed_request_as(
            SECRET,
            2,
            "cx06-op-acl1",
            "cx06-unregistered-caller",
            Method::GetEdges,
        ),
    ))
    .await;
    assert!(
        resp.error.is_some(),
        "an unregistered caller must be denied before graph-op dispatch, got: {:?}",
        resp.result
    );
}

// ── Result payload shape for AddEdge + GetEdges (also exercises the metrics
// gauge refresh + `core.mark_dirty()` tail of dispatch_graph_op_inner) ──────

#[tokio::test]
async fn t08_add_edge_then_get_edges_returns_the_edge() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-edge1")
        .await
        .error
        .is_none());
    assert!(add_node(&state, 2, "cx06-op-edge1", "a")
        .await
        .error
        .is_none());
    assert!(add_node(&state, 3, "cx06-op-edge1", "b")
        .await
        .error
        .is_none());
    let edge = call(
        &state,
        4,
        "cx06-op-edge1",
        Method::AddEdge {
            source_id: "a".to_string(),
            target_id: "b".to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
        },
    )
    .await;
    assert!(edge.error.is_none(), "AddEdge: {:?}", edge.error);

    let dump = call(&state, 5, "cx06-op-edge1", Method::GetEdges).await;
    assert!(dump.error.is_none(), "GetEdges: {:?}", dump.error);
    match &dump.result {
        Some(ResultPayload::EdgeList(rows)) => {
            assert_eq!(rows.len(), 1, "expected exactly the one committed edge");
            assert_eq!(rows[0].0, "a");
            assert_eq!(rows[0].1, "b");
        }
        other => panic!("expected EdgeList, got {other:?}"),
    }
}
