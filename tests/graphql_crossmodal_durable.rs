//! GraphQL cross-modal DURABLE-commit roundtrip (CONCEPT:EG-KG.query.facade-reconcile-hook).
//!
//! Proves the facade reconcile hook the `eg-graphql` crate left open (EG-383): a GraphQL
//! cross-modal `commitTransaction`, routed through the facade carrier
//! (`handlers::query.rs` `Method::GraphQl` → `handlers::txn::commit_graphql_cross_modal`),
//! lands its staged modalities in the redb durable tier via `commit_cross_modal_txn` — the
//! SAME committed machinery pgwire's `commit_txn_state` drives — so the write SURVIVES a
//! reopen of the persist dir. Before EG-419 the crate committed graph+vector in-memory
//! only, so nothing would be on disk.
//!
//! Driven through the REAL `dispatch` shell over an in-process `ServerState` backed by a
//! `RedbBackend` (persistence present), exactly as a client: begin → stage (sparqlUpdate +
//! stageEmbedding) → commit, each a separate request sharing the process-wide
//! `CrossModalTxnRegistry`. Durability is asserted by SHUTTING DOWN the backend (releasing
//! the per-file lock) and REOPENING a fresh `RedbBackend` on the same dir, then reading the
//! committed node back from the durable tier (`read_node`).
//!
//! Gated on `graphql` (the seam) + `redb` (a durable tier to reopen); runs under `--features full`.

#![cfg(all(feature = "graphql", feature = "redb"))]

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

const SECRET: &str = "gql-crossmodal-durable-secret";
const GRAPH: &str = "gqlxmdurable"; // lowercase-alnum ⇒ sanitize() is identity ⇒ fname == GRAPH
const NODE: &str = "<http://ex/n1>"; // lower_triples subject id for <http://ex/n1>

/// A fully-featured `ServerState` backed by the given redb persistence tier.
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

fn req(id: u64, graph: &str, method: Method) -> Request {
    common::signed_request(SECRET, id, graph, method)
}

/// Run a GraphQL doc against `GRAPH`; assert no error; return the decoded `{data:…}` JSON.
async fn gql(state: &Arc<RwLock<ServerState>>, id: u64, query: &str) -> serde_json::Value {
    let r: Response = Box::pin(dispatch(
        state,
        req(
            id,
            GRAPH,
            Method::GraphQl {
                query: query.to_string(),
                variables: None,
            },
        ),
    ))
    .await;
    assert!(r.error.is_none(), "graphql op {id} failed: {:?}", r.error);
    match r.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
        other => panic!("expected Raw graphql result, got {other:?}"),
    }
}

#[tokio::test]
async fn graphql_cross_modal_commit_survives_reopen() {
    // A unique, self-cleaning persist dir under the system temp dir (no tempfile dep).
    let dir = std::env::temp_dir().join(format!(
        "eg-gqlxm-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let dir_s = dir.to_string_lossy().to_string();

    // ── Phase 1: open the durable tier, create the graph, run the cross-modal txn ──
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 8192).unwrap());
    let state = state_with(backend.clone(), dir_s.clone());

    // Create the graph (dispatch registers it in BOTH the registry and the durable tier).
    let cr = Box::pin(dispatch(
        &state,
        req(
            1,
            GRAPH,
            Method::CreateGraph {
                graph_name: GRAPH.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    ))
    .await;
    assert!(cr.error.is_none(), "CreateGraph failed: {:?}", cr.error);

    // begin → mint a txnId.
    let begun = gql(&state, 2, "mutation { beginTransaction { txnId } }").await;
    let txn_id = begun["data"]["beginTransaction"]["txnId"]
        .as_str()
        .expect("txnId")
        .to_string();

    // Stage a graph node (SPARQL INSERT DATA — a type triple, no string literals to escape)
    // + its embedding, in the SAME txn.
    let staged = gql(
        &state,
        3,
        &format!(
            "mutation {{ sparqlUpdate(txnId: \"{txn_id}\", update: \"INSERT DATA {{ \
             <http://ex/n1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Widget> }}\") {{ staged }} }}"
        ),
    )
    .await;
    assert_eq!(
        staged["data"]["sparqlUpdate"]["staged"],
        serde_json::json!(true)
    );

    let staged_vec = gql(
        &state,
        4,
        &format!(
            "mutation {{ stageEmbedding(txnId: \"{txn_id}\", id: \"{NODE}\", vector: [0.1, 0.2, 0.3]) {{ staged }} }}"
        ),
    )
    .await;
    assert_eq!(
        staged_vec["data"]["stageEmbedding"]["staged"],
        serde_json::json!(true)
    );

    // Commit — this is the DURABLE path (EG-419).
    let committed = gql(
        &state,
        5,
        &format!("mutation {{ commitTransaction(txnId: \"{txn_id}\") {{ committed }} }}"),
    )
    .await;
    assert_eq!(
        committed["data"]["commitTransaction"]["committed"],
        serde_json::json!(true),
        "GraphQL commitTransaction should report a durable commit"
    );

    // ── Phase 2: shut the tier down (release the file lock) + REOPEN a fresh backend ──
    // `shutdown()` stops the writer thread but does not close the underlying redb
    // `Database` handle -- that only happens when the LAST owning value is dropped, and
    // redb keeps its advisory per-file lock until then (`state` holds its own `Arc`
    // clone of `backend`, so both must go). Reopening the SAME file IN-PROCESS then
    // races that drop's async teardown actually releasing the lock (no `JoinHandle` to
    // await here), so bound it with a short retry rather than a flat sleep -- identical
    // rationale to `redb_backend::tests::delete_then_recreate_same_name_keeps_new_writes`
    // and `advanced_crossmodal_roundtrip.rs::encryption_at_rest_wrong_key_fails_eg394`.
    backend.shutdown();
    drop(backend);
    drop(state);

    let reopened: Arc<dyn PersistenceBackend> = {
        let mut attempt = 0;
        loop {
            match RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 8192) {
                Ok(backend) => break Arc::new(backend),
                Err(error) if attempt < 100 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    let _ = error;
                }
                Err(error) => panic!("reopen durable tier: {error:?}"),
            }
        }
    };

    // The committed node must be on disk — read it back from the durable tier.
    let blob = reopened
        .read_node(GRAPH, NODE)
        .await
        .expect("read_node ok")
        .expect("committed node must be durable (survives reopen)");
    let props: serde_json::Map<String, serde_json::Value> =
        rmp_serde::from_slice(&blob).expect("decode durable node blob");
    assert_eq!(
        props.get("type").and_then(|v| v.as_str()),
        Some("http://ex/Widget"),
        "the durable node must carry the committed rdf:type property"
    );

    reopened.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
