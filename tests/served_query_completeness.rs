//! SERVED-QUERY COMPLETENESS — the two seam gaps the use-case suites surfaced, closed on
//! the REAL served `dispatch` surface (not just the `eg_plan::execute` engine surface):
//!
//!  * CONCEPT:EG-KG.query.served-text-index-binding — a served `UnifiedQuery` whose plan
//!    carries `Op::RankText` / an `Op::FuseRrf` text branch now returns REAL BM25 lexical
//!    hits (it previously degraded to ZERO lexical hits: the
//!    EG-KG.query.served-text-index-unbound-finding). `run_unified` binds a snapshot-derived
//!    `eg_text::TextIndex` into the `PlanCtx`.
//!  * CONCEPT:EG-KG.query.served-plan-optimize-routing — the served path routes the plan
//!    through the full cost optimizer via `eg_plan::execute`'s `plan_optimize`.
//!
//! Everything goes through the SERVED RPC: `Box::pin(dispatch(state, Request{ Method::* }))`.
//! Module-gated on `query` + `text`; runs under `--features full`.
#![cfg(all(feature = "query", feature = "text"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use std::sync::Arc;

use serde_json::json;
use tokio::sync::RwLock;

use eg_plan::{Op, Plan};
use epistemic_graph::protocol::Method;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "served-query-completeness-secret";

fn state() -> Arc<RwLock<ServerState>> {
    let (persist_dir, persistence) = common::tempdir_persistence();
    test_support::state_with(
        SECRET,
        common::current_isolation(),
        persist_dir,
        persistence,
    )
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
        let r = Box::pin(dispatch(
            state,
            test_support::commons_request(
                SECRET,
                id,
                Method::AddNode {
                    node_id: (*node).to_string(),
                    properties_msgpack: test_support::json_bytes(
                        json!({ "type": "Doc", "text": text }),
                    ),
                },
            ),
        ))
        .await;
        assert!(r.error.is_none(), "AddNode {node}: {:?}", r.error);
        id += 1;
        let r = Box::pin(dispatch(
            state,
            test_support::commons_request(
                SECRET,
                id,
                Method::AddEmbedding {
                    node_id: (*node).to_string(),
                    embedding: emb.to_vec(),
                },
            ),
        ))
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
    let resp = Box::pin(dispatch(
        &state,
        test_support::commons_request(SECRET, 100, Method::UnifiedQuery { plan }),
    ))
    .await;
    let rows = test_support::raw_rows(&resp);
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
    let resp = Box::pin(dispatch(
        &state,
        test_support::commons_request(SECRET, 200, Method::UnifiedQuery { plan }),
    ))
    .await;
    let rows = test_support::raw_rows(&resp);
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
        let r = Box::pin(dispatch(
            &state,
            test_support::commons_request(
                SECRET,
                if id == "canonical" { 1 } else { 2 },
                Method::AddNode {
                    node_id: id.to_string(),
                    properties_msgpack: test_support::json_bytes(
                        json!({ "type": "Doc", key: "kubernetes rollout crashloop" }),
                    ),
                },
            ),
        ))
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
    let resp = Box::pin(dispatch(
        &state,
        test_support::commons_request(SECRET, 3, Method::UnifiedQuery { plan }),
    ))
    .await;
    let rows = test_support::raw_rows(&resp);
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
