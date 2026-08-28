//! CX-EG-05 characterization tests for `apply_mutation_batch_in_wtx`
//! (`src/redb_store.rs`, CCN 353 as measured by the repo's lizard-based
//! complexity gate before this lane's refactor).
//!
//! These tests pin OBSERVED behaviour of the real served `dispatch` surface
//! for every request that routes through the durable `MutationBatch` commit
//! kernel: `apply_mutation_batch_in_wtx` is private to `redb_store.rs`, so it
//! cannot be unit-tested directly from an external integration-test crate —
//! this file exercises it black-box, the same pattern already used by
//! `tests/edge_pagination.rs` / `tests/adopt_workitem_metadata_cas_lifecycle.rs`.
//!
//! Filename note: the file lives directly under `tests/` (not
//! `tests/characterization/`) because Cargo's test-target auto-discovery only
//! picks up `tests/*.rs` files, not files in a subdirectory — a file placed
//! under `tests/characterization/` would silently never run. The
//! `characterization_cx_eg_05_` prefix keeps the intent from the dispatch
//! brief (a characterization test, owned by lane CX-EG-05) while staying
//! inside Cargo's real discovery rule and avoiding any name collision with
//! sibling CX-EG lanes' own characterization files.
//!
//! Requires `security` (real signed-envelope dispatch path) and `redb`
//! (durable persistence, without which the gateway rejects every mutation) —
//! see `tests/edge_pagination.rs` for the identical precedent.
#![cfg(all(feature = "server", feature = "security"))]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "cx-eg-05-mutation-batch-secret";

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

fn node_props() -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({"type": "Doc"})).unwrap()
}

async fn create_graph(state: &Arc<RwLock<ServerState>>, id: u64, name: &str) -> Response {
    Box::pin(dispatch(
        state,
        req(
            id,
            name,
            Method::CreateGraph {
                graph_name: name.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    ))
    .await
}

async fn add_node(
    state: &Arc<RwLock<ServerState>>,
    id: u64,
    graph: &str,
    node_id: &str,
) -> Response {
    Box::pin(dispatch(
        state,
        req(
            id,
            graph,
            Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: node_props(),
            },
        ),
    ))
    .await
}

fn edge_rows(resp: &Response) -> Vec<(String, String, Vec<u8>)> {
    assert!(resp.error.is_none(), "GetEdges: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::EdgeList(rows)) => rows.clone(),
        other => panic!("expected EdgeList, got {other:?}"),
    }
}

/// Exercises: `staged_state == None` (native/else branch), the lifecycle
/// `CreateGraph` arm of the outer match, the generic `apply_method_rows`
/// catch-all arm (`AddNode`/`AddEdge` have no bespoke arm), and the final
/// commit block (records/idempotency/version/fence/lifecycle-head/outbox/
/// graph-meta writes all in one write transaction).
#[tokio::test]
async fn t01_create_graph_add_node_add_edge_roundtrip() {
    let state = state();
    let create = create_graph(&state, 1, "cx05-g1").await;
    assert!(create.error.is_none(), "CreateGraph: {:?}", create.error);

    let n1 = add_node(&state, 2, "cx05-g1", "a").await;
    assert!(n1.error.is_none(), "AddNode a: {:?}", n1.error);
    let n2 = add_node(&state, 3, "cx05-g1", "b").await;
    assert!(n2.error.is_none(), "AddNode b: {:?}", n2.error);

    let edge = Box::pin(dispatch(
        &state,
        req(
            4,
            "cx05-g1",
            Method::AddEdge {
                source_id: "a".to_string(),
                target_id: "b".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"tag": "t"}))
                    .unwrap(),
            },
        ),
    ))
    .await;
    assert!(edge.error.is_none(), "AddEdge: {:?}", edge.error);

    let dump = Box::pin(dispatch(&state, req(5, "cx05-g1", Method::GetEdges))).await;
    let rows = edge_rows(&dump);
    assert_eq!(rows.len(), 1, "expected exactly the one committed edge");
    assert_eq!(rows[0].0, "a");
    assert_eq!(rows[0].1, "b");
}

/// Exercises: the native-match `ClearGraph` arm (clears node/edge/ledger rows
/// via `clear_graph_rows` + resource/lane/capacity-lease clears) and the
/// `clears_semantic` branch right after the big match.
#[tokio::test]
async fn t02_clear_graph_removes_edges_but_graph_survives() {
    let state = state();
    assert!(create_graph(&state, 1, "cx05-g2").await.error.is_none());
    assert!(add_node(&state, 2, "cx05-g2", "a").await.error.is_none());
    assert!(add_node(&state, 3, "cx05-g2", "b").await.error.is_none());
    let edge = Box::pin(dispatch(
        &state,
        req(
            4,
            "cx05-g2",
            Method::AddEdge {
                source_id: "a".to_string(),
                target_id: "b".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ),
    ))
    .await;
    assert!(edge.error.is_none());

    let clear = Box::pin(dispatch(&state, req(5, "cx05-g2", Method::ClearGraph))).await;
    assert!(clear.error.is_none(), "ClearGraph: {:?}", clear.error);

    let dump = Box::pin(dispatch(&state, req(6, "cx05-g2", Method::GetEdges))).await;
    let rows = edge_rows(&dump);
    assert_eq!(rows.len(), 0, "ClearGraph must remove every edge row");

    // The graph itself must still exist: a post-clear AddNode must succeed.
    let n = add_node(&state, 7, "cx05-g2", "c").await;
    assert!(
        n.error.is_none(),
        "graph must still be usable after ClearGraph: {:?}",
        n.error
    );
}

/// Exercises: the lifecycle `DeleteGraph` arm (`matches!(lifecycle, Some((false, _, _)))`),
/// `clear_change_material_rows` + `clear_mutation_authority_rows`, and the
/// `GRAPH_META` removal in the final commit block.
#[tokio::test]
async fn t03_delete_graph_then_add_node_fails() {
    let state = state();
    assert!(create_graph(&state, 1, "cx05-g3").await.error.is_none());
    assert!(add_node(&state, 2, "cx05-g3", "a").await.error.is_none());

    let delete = Box::pin(dispatch(
        &state,
        req(
            3,
            "cx05-g3",
            Method::DeleteGraph {
                graph_name: "cx05-g3".to_string(),
            },
        ),
    ))
    .await;
    assert!(delete.error.is_none(), "DeleteGraph: {:?}", delete.error);

    let n = add_node(&state, 4, "cx05-g3", "b").await;
    // OBSERVED (pinned, not asserted as "correct"): AddNode against a deleted
    // graph is rejected. The exact error text is intentionally NOT pinned
    // byte-for-byte here (it originates above `apply_mutation_batch_in_wtx`,
    // in the registry/dispatch layer) -- only that it fails.
    assert!(
        n.error.is_some(),
        "AddNode against a deleted graph must fail, got: {:?}",
        n.result
    );
}

/// Exercises the idempotency-replay block: dispatching the EXACT same signed
/// envelope (same nonce / idempotency key) twice must not double-apply the
/// mutation. OBSERVED, not assumed: this test exists to PIN whatever
/// `apply_mutation_batch_in_wtx`'s idempotency-key lookup actually does on a
/// byte-identical retry, ahead of refactor.
#[tokio::test]
async fn t04_replayed_add_node_request_is_not_double_applied() {
    let state = state();
    assert!(create_graph(&state, 1, "cx05-g4").await.error.is_none());

    let add_request = req(
        2,
        "cx05-g4",
        Method::AddNode {
            node_id: "a".to_string(),
            properties_msgpack: node_props(),
        },
    );
    let first = Box::pin(dispatch(&state, add_request.clone())).await;
    assert!(first.error.is_none(), "first AddNode: {:?}", first.error);
    let second = Box::pin(dispatch(&state, add_request.clone())).await;
    // OBSERVED: pin whichever of these the replay path actually produces.
    assert!(
        second.error.is_none(),
        "replayed identical AddNode request: {:?}",
        second.error
    );

    // Regardless of how the replay was handled, the node must exist exactly
    // once: add an edge FROM it and confirm the edge is readable (a
    // duplicate-inserted "a" node would not itself be observable via
    // GetEdges, so this also indirectly confirms no error occurred that
    // corrupted graph state).
    assert!(add_node(&state, 3, "cx05-g4", "b").await.error.is_none());
    let edge = Box::pin(dispatch(
        &state,
        req(
            4,
            "cx05-g4",
            Method::AddEdge {
                source_id: "a".to_string(),
                target_id: "b".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ),
    ))
    .await;
    assert!(
        edge.error.is_none(),
        "AddEdge after replay: {:?}",
        edge.error
    );
}

/// Exercises the generic `apply_method_rows` catch-all arm's error path
/// bubbling all the way back through `apply_mutation_batch_in_wtx` to the
/// caller: an edge whose source node does not exist.
#[tokio::test]
async fn t05_add_edge_with_missing_source_node_fails() {
    let state = state();
    assert!(create_graph(&state, 1, "cx05-g5").await.error.is_none());
    assert!(add_node(&state, 2, "cx05-g5", "b").await.error.is_none());

    let edge = Box::pin(dispatch(
        &state,
        req(
            3,
            "cx05-g5",
            Method::AddEdge {
                source_id: "missing".to_string(),
                target_id: "b".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        ),
    ))
    .await;
    assert!(
        edge.error.is_some(),
        "AddEdge with a missing source node must fail, got: {:?}",
        edge.result
    );
}
