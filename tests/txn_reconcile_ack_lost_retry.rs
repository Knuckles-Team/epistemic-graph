//! Ack-lost `Commit` retry reconciliation (CONCEPT:EG-KG.txn.multi-op-occ-acid),
//! covering the W1d hot-path fix in `handlers::txn::reconcile_committed_txn`.
//!
//! When a client's `Commit` response is lost after the transaction already
//! committed durably, the retried `Commit { txn_id }` finds nothing in
//! `open_txns` (the first attempt already consumed it) and falls through to
//! `reconcile_committed_txn`, which durably re-discovers the committed child and
//! replays the SAME terminal result. That reconcile path previously walked every
//! resident graph x {"txn","crossmodal"} namespace fully sequentially before
//! giving up — this test registers several resident graphs so a retry must, in
//! the pre-fix code, pay several serialized durable round-trips before finding
//! the one graph that actually committed. It proves the fanned-out lookup still
//! returns the exact same terminal result as the original (uncontested) commit,
//! for both the graph that DID commit and, transitively, that unrelated resident
//! graphs don't change the outcome.
//!
//! Driven through the REAL `dispatch` shell over an in-process `ServerState`
//! backed by a `RedbBackend` (persistence present), exactly as a client.

#![cfg(feature = "redb")]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::durability::DurabilityPolicy;
use epistemic_graph::protocol::{GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "txn-reconcile-ack-lost-secret";

fn state_with(backend: Arc<dyn PersistenceBackend>, dir: String) -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: common::current_isolation(),
        channels: ChannelManager::new(),
        auth_secret: SECRET.to_string(),
        persist_dir: Some(dir),
        persistence: Some(backend),
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
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

fn pack(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

/// The fixed integration-test JWT tenant claim `common::signed_request` embeds
/// (`tests/common/mod.rs`'s private `TEST_TENANT`). Mirrored here (not exported)
/// so this test can derive the SAME opaque `CarrierAuthority::tenant_scope()` a
/// verified request produces.
const TEST_TENANT_CLAIM: &str = "integration-test-tenant";

/// Reproduce `server::mutation_batch::opaque_coordinator_key` (crate-private) so
/// this integration test can predict the exact `CarrierAuthority::tenant_scope()`
/// a signed request resolves to: `opaque_coordinator_key("carrier-tenant",
/// "verified", <tenant claim>)`. The single-graph OCC commit path stamps a
/// durable `MutationBatch.tenant` with this same value (CONCEPT:
/// EG-KG.txn.multi-op-occ-acid), and `reconcile_committed_txn`'s ack-lost-retry
/// discovery requires `batch.tenant == batch.graph` for the resident graph it is
/// currently checking — so this test names its target graph identically to that
/// opaque tenant scope, exercising the exact scope-match branch a real ack-lost
/// retry hits.
fn opaque_coordinator_key(namespace: &str, graph: &str, coordinator_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(graph.as_bytes());
    digest.update([0]);
    digest.update(coordinator_id.as_bytes());
    format!("{namespace}:{}", hex::encode(digest.finalize()))
}

fn carrier_tenant_scope(raw_tenant: &str) -> String {
    opaque_coordinator_key("carrier-tenant", "verified", raw_tenant)
}

#[tokio::test]
async fn commit_retry_after_ack_loss_reconciles_across_resident_graphs() {
    let dir = std::env::temp_dir().join(format!(
        "eg-txn-reconcile-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_string_lossy().to_string();

    // The single/multi-graph OCC `Commit` receipt is sealed via the transaction
    // recovery cipher (CONCEPT:EG-KG.txn.multi-op-occ-acid), so a durable Commit
    // requires an encryption key configured. This test is the only one in this
    // binary, so setting the process-global env var here is race-free.
    std::env::set_var(
        epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
        "txn-reconcile-ack-lost-retry-test-key",
    );

    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 8192).unwrap());
    let state = state_with(backend.clone(), dir_s.clone());

    // Register several resident graphs so the retry's reconcile walk actually has
    // more than one (graph x namespace) candidate to consider — the target graph
    // is deliberately the LAST one registered/committed-to. Its name is the
    // opaque `tenant_scope` a signed request resolves to (see
    // `carrier_tenant_scope`), which the reconcile scope-match check requires.
    let target = carrier_tenant_scope(TEST_TENANT_CLAIM);
    let graphs: Vec<String> = vec![
        "gragone".to_string(),
        "gragtwo".to_string(),
        "gragthree".to_string(),
        target.clone(),
    ];
    for graph in &graphs {
        let cr: Response = dispatch(
            &state,
            req(
                1,
                graph,
                Method::CreateGraph {
                    graph_name: graph.clone(),
                    graph_type: GraphType::Global,
                },
            ),
        )
        .await;
        assert!(
            cr.error.is_none(),
            "CreateGraph {graph} failed: {:?}",
            cr.error
        );
    }
    let target = target.as_str();

    // begin → stage → commit on the target graph, exactly as a normal client.
    let begun: Response = dispatch(
        &state,
        req(
            10,
            target,
            Method::BeginTxn {
                graph: Some(target.to_string()),
                isolation: None,
            },
        ),
    )
    .await;
    assert!(begun.error.is_none(), "BeginTxn failed: {:?}", begun.error);
    let txn_id = match begun.result {
        Some(ResultPayload::String(value)) => value,
        other => panic!("unexpected BeginTxn result shape: {other:?}"),
    };

    let staged: Response = dispatch(
        &state,
        req(
            11,
            target,
            Method::TxnAddNode {
                txn_id: txn_id.clone(),
                node_id: "n1".to_string(),
                properties_msgpack: pack(serde_json::json!({"kind": "widget"})),
                graph: None,
            },
        ),
    )
    .await;
    assert!(
        staged.error.is_none(),
        "TxnAddNode failed: {:?}",
        staged.error
    );

    let first_commit: Response = dispatch(
        &state,
        req(
            12,
            target,
            Method::Commit {
                txn_id: txn_id.clone(),
            },
        ),
    )
    .await;
    assert!(
        first_commit.error.is_none(),
        "first Commit failed: {:?}",
        first_commit.error
    );
    assert!(
        matches!(first_commit.result, Some(ResultPayload::Bool(true))),
        "first Commit should report success: {:?} / {:?}",
        first_commit.result,
        first_commit.error
    );

    // The first Commit already consumed `open_txns[txn_id]`. Simulate the client
    // never having seen that response (an ack-lost retry) by resending the
    // IDENTICAL Commit request. This is the path that used to scan every resident
    // graph x namespace fully sequentially before finding `target`.
    let retried_commit: Response = dispatch(
        &state,
        req(
            13,
            target,
            Method::Commit {
                txn_id: txn_id.clone(),
            },
        ),
    )
    .await;
    assert!(
        retried_commit.error.is_none(),
        "retried Commit (ack-lost reconcile) failed: {:?}",
        retried_commit.error
    );
    assert!(
        matches!(retried_commit.result, Some(ResultPayload::Bool(true))),
        "retried Commit must replay the SAME terminal result as the original commit: {:?} / {:?}",
        retried_commit.result,
        retried_commit.error
    );

    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
