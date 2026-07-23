//! HIGH-VALUE USE-CASE SUITE #3 — hybrid RAG + IN-ENGINE analytics (CONCEPT:EG-KG.query.usecase-hybrid-rag-analytics).
//!
//! The differentiator: retrieve candidates via a graph+vector+text FUSED plan, then run
//! numeric/ML analytics (PCA + kmeans) over the JOINED properties of exactly those
//! candidates — IN THE ENGINE PROCESS (compute-near-data). No candidate set is shipped to
//! an external numpy/sklearn service; the same store that retrieved the rows computes over
//! them. This tests the RANK → ANALYTICS seam.
//!
//! Two real engine surfaces are wired end-to-end:
//!   1. RETRIEVAL — `eg_plan::execute` over a live `PlanCtx` (graph seed → RRF of vector +
//!      BM25 legs) returns the fused candidate ids.
//!   2. ANALYTICS — the candidates' embeddings, hydrated from the SAME in-engine
//!      `SemanticStore`, are fed to `Method::DsPca` / `Method::DsKMeans` through the REAL
//!      server `dispatch` (the served analytics surface), so the numeric compute runs in
//!      the engine process over the retrieved data.
//!
//! Asserts: the analytics run over exactly the fused-retrieval result set; kmeans
//! separates the two latent semantic groups among the retrieved docs; PCA's explained
//! variance concentrates on the dominant axis of that same set.
//!
//! SEAMS exercised: graph⇄vector⇄text retrieval fused, then Rank→analytics (compute-near-data).
//! Module-gated on `query` + `text` + `datascience`; runs under `--features full`.
#![cfg(all(feature = "query", feature = "text", feature = "datascience"))]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, Op, Plan, PlanCtx};
use eg_text::TextIndex;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "usecase-analytics-secret";

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

const QUERY_TEXT: &str = "vector database retrieval";
fn query_vec() -> Vec<f32> {
    vec![1.0, 0.0, 0.0, 0.0]
}

/// A small doc corpus with TWO latent semantic clusters in embedding space:
///   * cluster A (databases): `db1,db2,db3` — vectors near `[1,0,·,·]`.
///   * cluster B (ml): `ml1,ml2` — vectors near `[0,0,1,·]`.
///
/// Every doc is text-indexed; the query "vector database retrieval" is lexically strongest
/// on the database cluster, so the fused retrieval favors cluster A but still surfaces the
/// ml docs that are vector-adjacent — a realistic hybrid candidate set spanning both groups.
fn build_corpus() -> (GraphView, SemanticStore, TextIndex) {
    let core = GraphCore::new();
    for id in ["db1", "db2", "db3", "ml1", "ml2", "far"] {
        core.add_node(id.into(), blob(json!({ "type": "Doc" })));
    }
    let mut s = SemanticStore::new();
    // cluster A — databases (tight around [1,0,0,0]).
    s.add_embedding("db1".into(), vec![0.98, 0.05, 0.02, 0.0]);
    s.add_embedding("db2".into(), vec![0.95, 0.10, 0.05, 0.0]);
    s.add_embedding("db3".into(), vec![0.93, 0.08, 0.10, 0.0]);
    // cluster B — ml (tight around [0,0,1,0]).
    s.add_embedding("ml1".into(), vec![0.10, 0.05, 0.97, 0.0]);
    s.add_embedding("ml2".into(), vec![0.08, 0.10, 0.95, 0.0]);
    // an off-topic doc that the fused retrieval should rank out of the top set.
    s.add_embedding("far".into(), vec![0.0, 0.99, 0.0, 0.10]);

    let mut text = TextIndex::in_memory().unwrap();
    text.upsert(
        "db1",
        "vector database indexing and retrieval over embeddings",
    );
    text.upsert("db2", "a database for vector retrieval and search");
    text.upsert("db3", "database storage engine with vector retrieval");
    text.upsert("ml1", "machine learning model training and gradients");
    text.upsert("ml2", "neural network learning and optimization");
    text.upsert("far", "gardening tips for tomatoes in summer");
    text.commit().unwrap();
    (core.analysis_snapshot(), s, text)
}

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

fn as_json(resp: &Response) -> serde_json::Value {
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::Json(v)) => v.clone(),
        other => panic!("expected Json result, got {other:?}"),
    }
}

/// THE hybrid-RAG + analytics proof (CONCEPT:EG-KG.query.usecase-hybrid-rag-analytics): fused retrieval feeds in-engine
/// analytics; the compute runs over exactly the retrieved candidates, in-process.
#[tokio::test]
async fn fused_retrieval_feeds_in_engine_analytics_eg436() {
    let (view, semantic, text) = build_corpus();
    let ctx = PlanCtx::new(&view, &semantic).with_text(&text);

    // ── 1. RETRIEVE: graph seed → RRF(vector ⊕ BM25) → top-k. The REAL query engine. ──
    let retrieval = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::FuseRrf {
            branches: vec![
                vec![Op::Rank { query: query_vec() }],
                vec![Op::RankText {
                    query: QUERY_TEXT.into(),
                }],
            ],
            k: 0.0,
        },
        Op::Limit { k: 5 },
    ]);
    let candidates = execute(&retrieval, &ctx).unwrap().ids();
    // The off-topic doc is retrieved out of the top set; the top set spans both clusters.
    assert!(
        !candidates.contains(&"far".to_string()),
        "the fused retrieval ranks the off-topic doc out of the candidate set: {candidates:?}"
    );
    assert!(
        candidates.iter().any(|id| id.starts_with("db"))
            && candidates.iter().any(|id| id.starts_with("ml")),
        "the hybrid candidate set spans BOTH latent clusters: {candidates:?}"
    );

    // ── 2. JOIN: hydrate the candidates' embeddings from the SAME in-engine store ──
    // (compute-near-data: the retrieved rows' vectors never leave the engine process).
    let data: Vec<Vec<f64>> = candidates
        .iter()
        .map(|id| {
            semantic
                .get_embedding(id)
                .expect("retrieved candidate must have an in-engine embedding")
                .iter()
                .map(|&x| x as f64)
                .collect()
        })
        .collect();
    assert_eq!(
        data.len(),
        candidates.len(),
        "every retrieved candidate contributes a joined feature row to the analytics"
    );

    // ── 3. ANALYTICS in-engine via the REAL server dispatch (Method::DsKMeans) ──
    let state = state();
    let km = dispatch(
        &state,
        req(
            1,
            Method::DsKMeans {
                data: data.clone(),
                k: 2,
                max_iter: 25,
            },
        ),
    )
    .await;
    let km = as_json(&km);
    let labels: Vec<usize> = serde_json::from_value(km["labels"].clone()).unwrap();
    assert_eq!(
        labels.len(),
        candidates.len(),
        "kmeans labels one cluster per retrieved candidate: {labels:?}"
    );
    // The two latent groups (db* vs ml*) land in DIFFERENT clusters: every db-doc shares a
    // label distinct from every ml-doc.
    let label_of = |prefix: &str| -> Option<usize> {
        candidates
            .iter()
            .zip(&labels)
            .find(|(id, _)| id.starts_with(prefix))
            .map(|(_, &l)| l)
    };
    if let (Some(db_label), Some(ml_label)) = (label_of("db"), label_of("ml")) {
        assert_ne!(
            db_label, ml_label,
            "kmeans over the retrieved set separates the database and ml clusters: {labels:?}"
        );
        // Every db-doc shares db_label; every ml-doc shares ml_label.
        for (id, &l) in candidates.iter().zip(&labels) {
            if id.starts_with("db") {
                assert_eq!(l, db_label, "all db docs in one cluster: {id}={l}");
            } else if id.starts_with("ml") {
                assert_eq!(l, ml_label, "all ml docs in one cluster: {id}={l}");
            }
        }
    }

    // ── 4. PCA in-engine over the SAME retrieved set: variance concentrates on the axis
    // separating the two clusters (a real dimensionality-reduction over the join). ──
    let pca = dispatch(
        &state,
        req(
            2,
            Method::DsPca {
                data,
                n_components: 2,
            },
        ),
    )
    .await;
    let pca = as_json(&pca);
    let ratio: Vec<f64> = serde_json::from_value(pca["explained_variance_ratio"].clone()).unwrap();
    assert!(
        !ratio.is_empty() && ratio[0] > 0.5,
        "PCA over the retrieved candidates concentrates variance on the dominant axis: {ratio:?}"
    );
    assert!(
        ratio.windows(2).all(|w| w[0] >= w[1] - 1e-9),
        "explained-variance ratios are in descending order: {ratio:?}"
    );
}
