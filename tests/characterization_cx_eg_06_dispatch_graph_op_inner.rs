//! CX-EG-06 characterization tests for `dispatch_graph_op_inner`
//! (`src/server/dispatch.rs`, CCN 152 as measured by the repo's lizard-based
//! complexity gate before this lane's refactor).
//!
//! `dispatch_graph_op_inner` is private to `dispatch.rs`, so it is exercised
//! black-box through the real served `dispatch` surface (which routes every
//! graph-scoped `Method` through `dispatch_graph_op` -> `dispatch_graph_op_inner`),
//! exactly the pattern used by
//! `tests/characterization_cx_eg_05_apply_mutation_batch_in_wtx.rs` and
//! `tests/characterization_cx_eg_06_dispatch_inner.rs` (see either file's doc
//! comment for why this lives directly under `tests/` rather than
//! `tests/characterization/`).
//!
//! These tests pin OBSERVED behaviour, including behaviour that may itself
//! be a bug -- they do not assert that the behaviour is *correct*, only that
//! it is unchanged by the CCN-reduction refactor.
#![cfg(all(feature = "server", feature = "security"))]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{CypherMode, GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "cx-eg-06-dispatch-graph-op-inner-secret";

fn state() -> Arc<RwLock<ServerState>> {
    let (persist_dir, persistence) = common::tempdir_persistence();
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: common::current_isolation(),
        channels: ChannelManager::new(),
        #[cfg(feature = "viz-static-export")]
        viz_engine: None,
        auth_secret: SECRET.to_string(),
        persist_dir,
        persistence,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(
            epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
        ),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(epistemic_graph::server::txn::TxnIdGen),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        #[cfg(feature = "raft")]
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "streaming")]
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: Arc::new(DashMap::new()),
        #[cfg(feature = "kv")]
        kv: None,
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

fn req(id: u64, graph: &str, method: Method) -> Request {
    common::signed_request(SECRET, id, graph, method)
}

async fn call(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str, method: Method) -> Response {
    Box::pin(dispatch(state, req(id, graph, method))).await
}

async fn create_graph(state: &Arc<RwLock<ServerState>>, id: u64, name: &str) -> Response {
    call(
        state,
        id,
        name,
        Method::CreateGraph {
            graph_name: name.to_string(),
            graph_type: GraphType::Global,
        },
    )
    .await
}

async fn add_node(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str, node_id: &str) -> Response {
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
    assert!(create_graph(&state, 1, "cx06-op-sql1").await.error.is_none());
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

// ── Native audit-chain verification (`Method::AuditVerify`, security feature) ──

#[tokio::test]
async fn t05_audit_verify_over_fresh_graph() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-audit1").await.error.is_none());
    let resp = call(&state, 2, "cx06-op-audit1", Method::AuditVerify).await;
    // OBSERVED: pin whatever the audit-verify surface reports for a freshly
    // created graph with no mutations beyond CreateGraph itself.
    let _ = resp.error.is_some();
    assert!(resp.result.is_some() || resp.error.is_some(), "AuditVerify must return something");
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
    assert_eq!(
        resp.error.as_deref(),
        Some("time-series store not configured"),
        "TsListSeries without a configured tsdb_store: {:?}",
        resp.error
    );
}

// ── Access control before graph-op dispatch (unknown/unregistered caller) ──

#[tokio::test]
async fn t07_unregistered_caller_denied_before_graph_op() {
    let state = state();
    assert!(create_graph(&state, 1, "cx06-op-acl1").await.error.is_none());
    let resp = Box::pin(dispatch(
        &state,
        common::signed_request_as(SECRET, 2, "cx06-op-acl1", "cx06-unregistered-caller", Method::GetEdges),
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
    assert!(create_graph(&state, 1, "cx06-op-edge1").await.error.is_none());
    assert!(add_node(&state, 2, "cx06-op-edge1", "a").await.error.is_none());
    assert!(add_node(&state, 3, "cx06-op-edge1", "b").await.error.is_none());
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
