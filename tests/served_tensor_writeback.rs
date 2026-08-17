//! SERVED TENSOR WRITEBACK (CONCEPT:EG-KG.storage.derived-tensor-writeback-sink, W0.5) — proof
//! that a served `UnifiedQuery` carrying a `TensorScan` + `TensorOp` plan actually EXECUTES
//! the writeback op against a LIVE in-process server, not just `eg-plan`'s own executor
//! (`crates/eg-plan/src/tensor_tests.rs`) or `run_unified` called directly (the existing
//! `tensor_served_round_trip_tests` unit tests inside `src/server/handlers/query.rs`).
//!
//! Before `run_unified` bound a `.with_tensor_store(...)` (CONCEPT:EG-KG.storage.derived-tensor-writeback-sink),
//! this EXACT request deterministically failed with "TensorOp requires a bound tensor
//! store" — a served `Op::TensorScan` read still worked (it reads its input straight off
//! the queried `GraphView`), but a served `Op::TensorOp` derived-tensor writeback was dead.
//!
//! Everything here goes through the SERVED RPC surface: `Box::pin(dispatch(state, Request{ Method::* }))`
//! — the same `ServerState`/`dispatch` harness `served_query_completeness.rs` and
//! `advanced_crossmodal_roundtrip.rs` use. Module-gated on `tensor` (which implies `query`);
//! runs under `--features full`.
#![cfg(feature = "tensor")]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use eg_plan::{Op, Plan};
use eg_tensor::{Buffer, Tensor};
use eg_types::wire::{TensorElementwiseOp, TensorOpKind, TensorReduceKind};
use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "served-tensor-writeback-secret";

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

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

fn req(id: u64, method: Method) -> Request {
    common::signed_request(SECRET, id, "__commons__", method)
}

/// Decode a served `UnifiedQuery` result (a `Raw` MessagePack `Vec<(id, score|nil)>`).
fn rows_of(resp: &Response) -> Vec<(String, Option<f32>)> {
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).expect("row decode"),
        other => panic!("expected Raw result, got {other:?}"),
    }
}

/// Seed a `Frame` layer of three nodes, each carrying the SAME dense 2x3 tensor in its
/// conventional `tensor` property, over the served write path (`Method::AddNode`) — the
/// same fixture shape `query.rs`'s own `tensor_served_round_trip_tests::frames_view` builds
/// directly against a `GraphCore`, seeded here through the wire instead.
async fn seed_frames(state: &Arc<RwLock<ServerState>>) {
    let t = Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])).unwrap();
    let tv = serde_json::to_value(&t).unwrap();
    for (id, node_id) in ["F1", "F2", "F3"].into_iter().enumerate() {
        let r = Box::pin(dispatch(
            state,
            req(
                id as u64 + 1,
                Method::AddNode {
                    node_id: node_id.to_string(),
                    properties_msgpack: blob(json!({ "type": "Frame", "tensor": tv })),
                },
            ),
        ))
        .await;
        assert!(r.error.is_none(), "AddNode {node_id}: {:?}", r.error);
    }
}

/// CONCEPT:EG-KG.storage.derived-tensor-writeback-sink — a served `UnifiedQuery` whose plan
/// is `TensorScan → TensorOp(Reduce)` now EXECUTES against a live in-process server and
/// returns the scanned rows, instead of deterministically erroring with "TensorOp requires
/// a bound tensor store" (the pre-fix behavior: `run_unified` built a `PlanCtx` with no
/// `tensor_store` binding at all).
#[tokio::test]
async fn served_tensor_scan_and_reduce_writeback_succeeds() {
    let state = state();
    seed_frames(&state).await;

    let plan = Plan::new(vec![
        Op::TensorScan {
            layer: "Frame".into(),
        },
        Op::TensorOp {
            kind: TensorOpKind::Reduce {
                axis: 1,
                kind: TensorReduceKind::Mean,
            },
        },
    ]);
    let resp = Box::pin(dispatch(&state, req(100, Method::UnifiedQuery { plan }))).await;
    let rows = rows_of(&resp);
    let mut ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["F1", "F2", "F3"],
        "served TensorScan + TensorOp writeback must execute and return every scanned row \
         now that run_unified binds a tensor store: {rows:?}"
    );
}

/// The same served path with an `Elementwise` op (the OTHER `TensorOpKind` variant),
/// run TWICE back-to-back over the SAME plan — proving the served writeback sink is
/// durable ACROSS requests (a process-lifetime singleton, not a fresh, immediately-
/// discarded store per call) rather than merely tolerating a single call.
#[tokio::test]
async fn served_tensor_elementwise_writeback_repeatable_across_requests() {
    let state = state();
    seed_frames(&state).await;

    let plan = || {
        Plan::new(vec![
            Op::TensorScan {
                layer: "Frame".into(),
            },
            Op::TensorOp {
                kind: TensorOpKind::Elementwise {
                    op: TensorElementwiseOp::Mul,
                    scalar: 2.0,
                },
            },
        ])
    };

    for req_id in [200, 201] {
        let resp = Box::pin(dispatch(
            &state,
            req(req_id, Method::UnifiedQuery { plan: plan() }),
        ))
        .await;
        let rows = rows_of(&resp);
        assert_eq!(
            rows.len(),
            3,
            "served TensorOp elementwise writeback must succeed on request {req_id}: {rows:?}"
        );
    }
}
