//! SERVED-QUERY COMPLETENESS — the two seam gaps the use-case suites surfaced, closed on
//! the REAL served `dispatch` surface (not just the `eg_plan::execute` engine surface):
//!
//!  * CONCEPT:EG-KG.query.served-text-index-binding — a served `UnifiedQuery` whose plan
//!    carries `Op::RankText` / an `Op::FuseRrf` text branch now returns REAL BM25 lexical
//!    hits (it previously degraded to ZERO lexical hits: the
//!    EG-KG.query.served-text-index-unbound-finding). `run_unified` binds a snapshot-derived
//!    `eg_text::TextIndex` into the `PlanCtx`.
//!  * CONCEPT:EG-KG.query.served-plan-optimize-routing — the served path routes the plan
//!    through the FULL cost optimizer (via `eg_plan::execute`'s `plan_optimize`) instead of
//!    the single legacy `reorder_filter_rank` swap; the `reorder_filter_selectivity` hint is
//!    now a no-op the optimizer supersedes, and served results are unchanged (differential).
//!
//! Everything goes through the SERVED RPC: `dispatch(state, Request{ Method::* })`.
//! Module-gated on `query` + `text`; runs under `--features full`.
#![cfg(all(feature = "query", feature = "text"))]

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use eg_plan::{Op, Plan};
use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{compute_auth_token, dispatch, ServerState};

const SECRET: &str = "served-query-completeness-secret";

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

fn state() -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: SECRET.to_string(),
        persist_dir: None,
        persistence: None,
        redb_authoritative: false,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(
            epistemic_graph::write_coalescer::WriteCoalescerRegistry::from_env(),
        ),
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
        #[cfg(feature = "rdf-redb")]
        rdf_quads: None,
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
        #[cfg(feature = "dataset-handle")]
        dataset_handles: std::sync::Arc::new(
            epistemic_graph::server::dataset_handle::DatasetHandleRegistry::new(),
        ),
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

fn req(id: u64, method: Method) -> Request {
    Request {
        id,
        graph: "__commons__".into(),
        auth_token: compute_auth_token(SECRET, id),
        agent_id: None,
        method,
    }
}

/// Decode a served `UnifiedQuery` result (a `Raw` MessagePack `Vec<(id, score|nil)>`).
fn rows_of(resp: &Response) -> Vec<(String, Option<f32>)> {
    assert!(resp.error.is_none(), "dispatch error: {:?}", resp.error);
    match &resp.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).expect("row decode"),
        other => panic!("expected Raw result, got {other:?}"),
    }
}

/// Seed a small text-bearing Doc corpus over the SERVED write path (AddNode + AddEmbedding).
/// Each doc's `text` field is what the served snapshot-derived BM25 index will tokenize.
async fn seed_corpus(state: &Arc<RwLock<ServerState>>) {
    let docs: &[(&str, &str, [f32; 3])] = &[
        (
            "kube",
            "kubernetes deployment rollout failure crashloop",
            [0.10, 0.99, 0.0],
        ),
        (
            "db",
            "postgres vacuum autovacuum bloat tuning",
            [0.99, 0.10, 0.0],
        ),
        (
            "ml",
            "gradient descent neural network training",
            [0.0, 0.10, 0.99],
        ),
    ];
    let mut id = 1_u64;
    for (node, text, emb) in docs {
        let r = dispatch(
            state,
            req(
                id,
                Method::AddNode {
                    node_id: (*node).to_string(),
                    properties_msgpack: blob(json!({ "type": "Doc", "text": text })),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "AddNode {node}: {:?}", r.error);
        id += 1;
        let r = dispatch(
            state,
            req(
                id,
                Method::AddEmbedding {
                    node_id: (*node).to_string(),
                    embedding: emb.to_vec(),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "AddEmbedding {node}: {:?}", r.error);
        id += 1;
    }
}

/// CONCEPT:EG-KG.query.served-text-index-binding — a served pure-lexical `Op::RankText` plan
/// returns real BM25 hits. Before the fix this degraded to ZERO hits (no index bound).
#[tokio::test]
async fn served_ranktext_returns_lexical_hits() {
    let state = state();
    seed_corpus(&state).await;

    // Scan Doc → RankText "kubernetes rollout" → Limit. The lexically-matching doc is `kube`.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::RankText {
            query: "kubernetes rollout crashloop".into(),
        },
        Op::Limit { k: 5 },
    ]);
    let resp = dispatch(
        &state,
        req(
            100,
            Method::UnifiedQuery {
                plan,
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    let rows = rows_of(&resp);
    assert!(
        !rows.is_empty(),
        "served RankText now returns real lexical hits (was ZERO before the binding): {rows:?}"
    );
    assert_eq!(
        rows.first().map(|(id, _)| id.as_str()),
        Some("kube"),
        "the lexically-strongest doc ranks first served: {rows:?}"
    );
}

/// CONCEPT:EG-KG.query.served-text-index-binding — a served `Op::FuseRrf[RankText, Rank]`
/// hybrid plan fuses REAL lexical hits with the vector leg. The text-only-relevant doc is
/// surfaced by the RRF fusion precisely because the lexical branch now contributes.
#[tokio::test]
async fn served_fuserrf_text_branch_contributes_lexical_hits() {
    let state = state();
    seed_corpus(&state).await;

    // Query vector points at the `ml` cluster ([0,0,1]); the text query points at `kube`.
    // A working FuseRrf text branch must pull `kube` into the fused top set — a pure-vector
    // ranking would rank it last.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::FuseRrf {
            branches: vec![
                vec![Op::Rank {
                    query: vec![0.0, 0.0, 1.0],
                }],
                vec![Op::RankText {
                    query: "kubernetes rollout crashloop".into(),
                }],
            ],
            k: 0.0,
        },
        Op::Limit { k: 3 },
    ]);
    let resp = dispatch(
        &state,
        req(
            200,
            Method::UnifiedQuery {
                plan,
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    let rows = rows_of(&resp);
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        ids.contains(&"kube"),
        "the served FuseRrf text branch contributes real lexical hits (kube fused in): {ids:?}"
    );
    assert!(
        ids.contains(&"ml"),
        "the vector branch still contributes its cluster hit: {ids:?}"
    );
}

/// CONCEPT:EG-KG.query.served-plan-optimize-routing — differential: the `reorder_filter_selectivity`
/// hint is now a no-op the full optimizer supersedes, and served results are UNCHANGED whether
/// or not it is set (the optimizer's reorder is answer-preserving within the EG-405 guard).
#[tokio::test]
async fn served_reorder_hint_is_answer_preserving_noop() {
    let state = state();
    seed_corpus(&state).await;

    // A Filter(before Rank) plan — the exact pair the legacy reorder targeted.
    let mk = || {
        Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![eg_plan::Pred::Eq {
                    prop: "type".into(),
                    value: "Doc".into(),
                }],
            },
            Op::Rank {
                query: vec![0.99, 0.10, 0.0],
            },
            Op::Limit { k: 5 },
        ])
    };

    let with_hint = rows_of(
        &dispatch(
            &state,
            req(
                300,
                Method::UnifiedQuery {
                    plan: mk(),
                    reorder_filter_selectivity: Some(0.01),
                },
            ),
        )
        .await,
    );
    let no_hint = rows_of(
        &dispatch(
            &state,
            req(
                301,
                Method::UnifiedQuery {
                    plan: mk(),
                    reorder_filter_selectivity: None,
                },
            ),
        )
        .await,
    );
    assert!(
        !with_hint.is_empty(),
        "the plan returns rows: {with_hint:?}"
    );
    assert_eq!(
        with_hint, no_hint,
        "the legacy reorder hint no longer changes served results (optimizer supersedes it)"
    );
}

/// CONCEPT:EG-KG.query.served-text-index-binding / EG-P1-4 — a served `RankText` pushes down
/// into the MAINTAINED persistent `GraphTextIndex` (via `ServedTextIndex`) when a
/// `ServerIndexFactory` is installed, instead of rebuilding a throwaway snapshot-derived
/// index per query.
///
/// Proven DIFFERENTIALLY rather than by inspection: the persistent index derives a node's
/// indexable body from a FIXED canonical key list (`GraphTextIndex`'s `TEXT_KEYS` —
/// text/content/body/description/summary/title/name, see `server::secondary_indexes`),
/// while the snapshot-derived fallback (`build_text_index_from_view`) concatenates EVERY
/// string leaf in a node's property blob regardless of key. So a node whose only textual
/// field lives under a NON-canonical key matches the snapshot fallback but is INVISIBLE to
/// the persistent index. A served `RankText` for that exact phrase returning hits ONLY for
/// the canonical-keyed doc — never the non-canonical one — is possible ONLY if the served
/// path is genuinely searching the persistent index; a silent fallback to the
/// snapshot-derived index (the per-scan-rebuild bug this closes) would instead match BOTH.
#[tokio::test]
async fn served_ranktext_pushes_down_into_persistent_index_not_snapshot_fallback() {
    let state = state();
    // Install the SAME server-layer secondary-index factory `main.rs` wires at startup: an
    // in-memory persistent `GraphTextIndex` per graph, maintained incrementally by the write
    // coalescer (CONCEPT:EG-KG.storage.incremental-text) — no on-disk dir needed for this test.
    state.write().await.registry.set_secondary_index_factory(
        epistemic_graph::server::secondary_indexes::ServerIndexFactory::new()
            .with_text_dir(None)
            .into_arc(),
    );

    // `canonical` carries its body under the persistent index's own `text` key (matches
    // BOTH the persistent index and a snapshot rebuild). `noncanonical` carries the exact
    // SAME phrase under a key (`note`) the persistent index's fixed key list does not
    // recognize — it matches ONLY a snapshot-derived rebuild.
    for (id, key) in [("canonical", "text"), ("noncanonical", "note")] {
        let r = dispatch(
            &state,
            req(
                if id == "canonical" { 1 } else { 2 },
                Method::AddNode {
                    node_id: id.to_string(),
                    properties_msgpack: blob(
                        json!({ "type": "Doc", key: "kubernetes rollout crashloop" }),
                    ),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "AddNode {id}: {:?}", r.error);
    }

    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::RankText {
            query: "kubernetes rollout crashloop".into(),
        },
        Op::Limit { k: 5 },
    ]);
    let resp = dispatch(
        &state,
        req(
            3,
            Method::UnifiedQuery {
                plan,
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    let rows = rows_of(&resp);
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        ids.contains(&"canonical"),
        "the canonical `text`-keyed doc must hit the persistent index: {ids:?}"
    );
    assert!(
        !ids.contains(&"noncanonical"),
        "a non-canonical-keyed doc hitting here would prove the served path silently fell \
         back to the snapshot-derived index (which indexes EVERY string leaf) instead of the \
         persistent one (which does not — a per-scan-rebuild regression this test guards \
         against): {ids:?}"
    );
}
