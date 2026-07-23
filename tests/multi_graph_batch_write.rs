//! CONCEPT:EG-KG.storage.multi-graph-batch-write — batched CROSS-GRAPH write over the
//! REAL served `dispatch` surface. ONE `MultiGraphBatchUpdate` request carries a
//! `BatchUpdate`-shaped op list for MANY named graphs; the server applies each
//! graph's sub-batch through the ordinary per-graph write path CONCURRENTLY, so N
//! distinct graphs commit across N of the K redb shard writers in parallel — the
//! client pays ONE round-trip instead of N that each re-acquire a lock. Because it
//! reuses the existing per-graph `BatchUpdate` primitive, a single-graph
//! multi-graph write is byte-for-byte a plain `BatchUpdate`, and one graph's
//! failure never aborts the others (partial-success contract).
//!
//! Everything goes through the SERVED RPC: `dispatch(state, Request{ Method::* })`.
#![cfg(feature = "server")]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "multi-graph-batch-write-secret";

fn state() -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: common::current_isolation(),
        channels: ChannelManager::new(),
        auth_secret: SECRET.to_string(),
        persist_dir: None,
        persistence: None,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(epistemic_graph::server::txn::TxnIdGen::default()),
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

fn req(id: u64, method: Method) -> Request {
    common::signed_request(SECRET, id, "__commons__", method)
}

/// One graph's `BatchUpdate` op list, encoded as the inner `operations_msgpack`.
fn ops_blob(ops: serde_json::Value) -> Vec<u8> {
    let arr = ops.as_array().expect("ops is an array");
    rmp_serde::to_vec_named(arr).unwrap()
}

/// The whole `MultiGraphBatchUpdate` payload: `Vec<(graph_name, operations_msgpack)>`.
fn batches_blob(entries: Vec<(&str, serde_json::Value)>) -> Vec<u8> {
    let v: Vec<(String, serde_bytes::ByteBuf)> = entries
        .into_iter()
        .map(|(g, ops)| (g.to_string(), serde_bytes::ByteBuf::from(ops_blob(ops))))
        .collect();
    rmp_serde::to_vec_named(&v).unwrap()
}

async fn has_node(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str, node: &str) -> bool {
    let request = common::signed_request(
        SECRET,
        id,
        graph,
        Method::HasNode {
            node_id: node.to_string(),
        },
    );
    let resp = dispatch(state, request).await;
    matches!(resp.result, Some(ResultPayload::Bool(true)))
}

fn results_and_errors(
    resp: &Response,
) -> (
    serde_json::Map<String, serde_json::Value>,
    serde_json::Map<String, serde_json::Value>,
) {
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    let v = match &resp.result {
        Some(ResultPayload::Json(v)) => v.clone(),
        other => panic!("expected Json result, got {other:?}"),
    };
    let results = v
        .get("results")
        .and_then(|x| x.as_object())
        .cloned()
        .unwrap_or_default();
    let errors = v
        .get("errors")
        .and_then(|x| x.as_object())
        .cloned()
        .unwrap_or_default();
    (results, errors)
}

/// K distinct graphs each get their node in ONE round-trip; the reply reports a
/// per-graph success and every node lands in its OWN graph (no cross-contamination).
#[tokio::test]
async fn multi_graph_batch_write_fans_across_graphs() {
    let state = state();
    // Create three routed content graphs (would hash to different shard writers).
    for g in ["src:a", "src:b", "src:c"] {
        let s = &mut *state.write().await;
        s.registry
            .create_graph(g, GraphType::Commons, None)
            .unwrap();
    }

    let payload = batches_blob(vec![
        (
            "src:a",
            json!([{"op": "add_node", "id": "n-a", "properties": {"type": "Doc", "src": "a"}}]),
        ),
        (
            "src:b",
            json!([{"op": "add_node", "id": "n-b", "properties": {"type": "Doc", "src": "b"}}]),
        ),
        (
            "src:c",
            json!([{"op": "add_node", "id": "n-c", "properties": {"type": "Doc", "src": "c"}}]),
        ),
    ]);

    let resp = dispatch(
        &state,
        req(
            1,
            Method::MultiGraphBatchUpdate {
                batches_msgpack: payload,
            },
        ),
    )
    .await;
    let (results, errors) = results_and_errors(&resp);
    assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    assert_eq!(results.len(), 3, "one result per graph");
    // Each sub-batch reports added_nodes == 1.
    for g in ["src:a", "src:b", "src:c"] {
        let r = results
            .get(g)
            .unwrap_or_else(|| panic!("no result for {g}"));
        assert_eq!(
            r.get("added_nodes").and_then(|x| x.as_u64()),
            Some(1),
            "{g}"
        );
    }

    // Every node landed in its OWN graph — and NOT in a sibling graph.
    assert!(has_node(&state, 10, "src:a", "n-a").await);
    assert!(has_node(&state, 11, "src:b", "n-b").await);
    assert!(has_node(&state, 12, "src:c", "n-c").await);
    assert!(
        !has_node(&state, 13, "src:a", "n-b").await,
        "no cross-graph leak"
    );
    assert!(
        !has_node(&state, 14, "src:b", "n-a").await,
        "no cross-graph leak"
    );
}

/// Partial success: a missing target graph surfaces its error without aborting the
/// healthy sub-batches (the partial-success contract).
#[tokio::test]
async fn multi_graph_batch_write_is_partial_success() {
    let state = state();
    {
        let s = &mut *state.write().await;
        s.registry
            .create_graph("src:ok", GraphType::Commons, None)
            .unwrap();
    }
    let payload = batches_blob(vec![
        (
            "src:ok",
            json!([{"op": "add_node", "id": "ok-1", "properties": {"type": "Doc"}}]),
        ),
        (
            "src:missing",
            json!([{"op": "add_node", "id": "x-1", "properties": {"type": "Doc"}}]),
        ),
    ]);
    let resp = dispatch(
        &state,
        req(
            2,
            Method::MultiGraphBatchUpdate {
                batches_msgpack: payload,
            },
        ),
    )
    .await;
    let (results, errors) = results_and_errors(&resp);
    assert!(results.contains_key("src:ok"), "healthy sub-batch applied");
    assert!(
        errors.contains_key("src:missing"),
        "missing graph surfaced as error"
    );
    assert!(has_node(&state, 20, "src:ok", "ok-1").await);
}

/// A single-graph multi-graph write is equivalent to a plain `BatchUpdate` on that
/// graph — the op reuses the same per-graph primitive.
#[tokio::test]
async fn single_graph_multi_batch_equals_plain_batch() {
    let state = state();
    {
        let s = &mut *state.write().await;
        s.registry
            .create_graph("src:solo", GraphType::Commons, None)
            .unwrap();
    }
    let payload = batches_blob(vec![(
        "src:solo",
        json!([
            {"op": "add_node", "id": "s1", "properties": {"type": "Doc"}},
            {"op": "add_node", "id": "s2", "properties": {"type": "Doc"}},
            {"op": "add_edge", "source": "s1", "target": "s2", "properties": {"relationship": "LINKS"}}
        ]),
    )]);
    let resp = dispatch(
        &state,
        req(
            3,
            Method::MultiGraphBatchUpdate {
                batches_msgpack: payload,
            },
        ),
    )
    .await;
    let (results, errors) = results_and_errors(&resp);
    assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    let r = results.get("src:solo").unwrap();
    assert_eq!(r.get("added_nodes").and_then(|x| x.as_u64()), Some(2));
    assert_eq!(r.get("added_edges").and_then(|x| x.as_u64()), Some(1));
    assert!(has_node(&state, 30, "src:solo", "s1").await);
    assert!(has_node(&state, 31, "src:solo", "s2").await);
}
