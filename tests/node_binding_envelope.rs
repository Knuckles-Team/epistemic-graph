//! CONCEPT:EG-KG.security.node-bound-envelope — ADR-3 / W1.9 node-bound
//! envelopes (`reports/wave1/ADR-scale-trio.md`), exercised over the REAL
//! served `dispatch` surface (not just the `auth` module's own unit tests).
//!
//! No `raft` feature and no `EPISTEMIC_GRAPH_NODE_ID` override are configured
//! here, so this process's `node_identity()` resolves to the documented
//! single-node default: the literal `"single"`.
#![cfg(feature = "server")]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{GraphType, Method};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "node-binding-envelope-secret";

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
        auth_secret: SECRET.to_string(),
        persist_dir,
        persistence,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new()),
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

async fn ready_state() -> Arc<RwLock<ServerState>> {
    let state = state();
    {
        let s = &mut *state.write().await;
        s.registry
            .create_graph("g", GraphType::Commons, None)
            .unwrap();
    }
    state
}

/// An old client that predates node binding never sends the `node` claim at
/// all. Under the shipped default posture (`EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING`
/// unset ⇒ `warn`), the request must still succeed end-to-end.
#[tokio::test]
async fn old_client_without_node_claim_still_dispatches_under_default_warn_posture() {
    let state = ready_state().await;
    let req = common::signed_request_with_node(SECRET, 1, "g", Method::Ping, None);
    let resp = Box::pin(dispatch(&state, req)).await;
    assert!(resp.error.is_none(), "got: {:?}", resp.error);
}

/// A node claim that exact-matches this process's own identity (the
/// documented single-node default, `"single"`) must dispatch normally.
#[tokio::test]
async fn matching_node_claim_dispatches_normally() {
    let state = ready_state().await;
    let req = common::signed_request_with_node(SECRET, 2, "g", Method::Ping, Some("single"));
    let resp = Box::pin(dispatch(&state, req)).await;
    assert!(resp.error.is_none(), "got: {:?}", resp.error);
}

/// A node claim bound to a DIFFERENT node must be rejected end-to-end, with
/// the distinct `NODE_MISMATCH` error surfacing through the real `dispatch`
/// response -- not merely inside the `auth` module's own unit tests. This is
/// the exact scenario ADR-3 exists for: a captured envelope replayed against
/// the wrong cluster member.
#[tokio::test]
async fn wrong_node_claim_is_rejected_by_the_real_dispatch_path() {
    let state = ready_state().await;
    let req =
        common::signed_request_with_node(SECRET, 3, "g", Method::Ping, Some("some-other-node"));
    let resp = Box::pin(dispatch(&state, req)).await;
    let error = resp
        .error
        .expect("a mismatched node claim must be rejected");
    assert!(error.starts_with("NODE_MISMATCH"), "got: {error}");
}
