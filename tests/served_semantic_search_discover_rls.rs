//! GOC-08 (vector/text/hybrid retrieval quality) security check: does the eg-side
//! retrieval surface order/trim BEFORE authorization is applied, the same defect class
//! an independent audit confirmed on the au side (`HybridRetriever` reranks and trims
//! candidates BEFORE ACL enforcement, so a denied high-score row can consume the
//! visible top-k budget and influence rankings)?
//!
//! Code-path finding (recorded in the GOC-08 handoff, not just this test): `eg-plan`'s
//! `Op::Rank`/`Op::RankEmbed`/`Op::RankText`/`Op::FuseRrf`/`Op::RankMmr`/
//! `Op::RankNodeDistance`/`Op::RankMentions` (`crates/eg-plan/src/exec.rs`) only ever
//! REORDER an already-seeded candidate `RowSet` — a bare `Rank` on an empty input (the
//! executor's starting state, `RowSet::new()`) yields nothing, so a candidate can enter
//! the pipeline only through a SOURCE op (`Scan`/`Reason`/`SparqlBgp`/`ForeignScan`),
//! every one of which reads `ctx.view` — and every served caller of
//! `eg_plan::execute`/`run_unified` (`src/server/handlers/query.rs`) calls
//! `rls.filter_view(caller, &mut snap)` on that view BEFORE the plan ever runs (already
//! proven end-to-end for `Reason -> Rank` by `advanced_crossmodal_roundtrip.rs`'s
//! `rls_per_agent_fused_reason_rank_overlay_eg391`, EG-391). So the eg-plan hybrid
//! surface authorizes the SOURCE, not a downstream trim -- structurally immune to the
//! au bug's ordering.
//!
//! The OTHER retrieval surface -- `Method::SemanticSearch` / `Method::Discover`
//! (`src/server/handlers/graph_ops.rs`), which call `SemanticStore::semantic_search`
//! directly rather than going through `eg-plan` -- had NO wire-level RLS regression
//! test anywhere in this repo before this file (grep-confirmed: zero references to
//! either method under `tests/`). Their authorization argument is different from
//! eg-plan's: `try_handle` (`src/server/handlers/graph_ops.rs`) unconditionally shadows
//! its `core` parameter with `read_authority.project_core(&core)` BEFORE the method
//! `match` runs, and `GraphReadAuthority::build_projection` (`src/server/access.rs`)
//! REBUILDS the semantic store's embedding arena from scratch containing ONLY the
//! embeddings of nodes that survived `IsolationLayer::filter_view` ("Semantic search is
//! a row read too. Rebuild only the visible portion so ANN candidates cannot reveal
//! hidden ids or alter result cardinality via a post-hoc serializer filter." --
//! `access.rs`'s own doc comment on that step). So a denied node's embedding is not
//! merely filtered out of the RESULT, it never exists in the store `semantic_search`
//! scans in the first place -- again the opposite order from the au bug (authorize the
//! source, then rank/trim), just via a different mechanism (arena projection instead of
//! an id-allowlist closure).
//!
//! This file closes the missing-coverage gap: it drives the REAL `dispatch()` ->
//! `try_handle` path (never a stub retriever -- the exact class of gap the au-side
//! audit's warning called out: "the existing test used a stub retriever that bypassed
//! the real code path, so it passed regardless") for both `Method::SemanticSearch` and
//! `Method::Discover`, with an ADVERSARIAL fixture where the other agent's private node
//! is the CLOSEST/STRONGEST match on every axis (vector distance AND keyword overlap)
//! and the result budget (`n_results`/`k`) is small enough that an unfiltered top-k
//! would necessarily be consumed by it. A "sanity" half of each test proves the private
//! node really is a contender (its OWNER can retrieve it via the identical query), so
//! the negative assertion is not vacuous.

#![cfg(feature = "security")]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::isolation::AgentRole;
use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "served-semantic-search-discover-rls-secret";

/// RBAC role every non-`System` registered peer needs on `__commons__` -- under
/// `feature = "security"` there is no pre-RBAC Commons-open-to-all fall-through for a
/// non-`System` identity (see `commons_isolation` below and
/// `advanced_crossmodal_roundtrip.rs`'s identical fixture for the full rationale).
const COMMONS_USER_ROLE: &str = "commons-user";

async fn state() -> Arc<RwLock<ServerState>> {
    let (persist_dir, persistence) = common::tempdir_persistence();
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: commons_isolation(),
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

/// `common::current_isolation()` plus a `"commons-user"` RBAC role granted Read+Write on
/// `__commons__` -- the identical fixture shape `advanced_crossmodal_roundtrip.rs`'s
/// `commons_isolation` uses, needed for the same reason: `common::TEST_AGENT` is
/// `System`-role and bypasses RBAC, but the `alice`/`bob` peers this file registers
/// (plain `AgentRole::Agent`) do not, or every RPC they make is `ACCESS_DENIED` before
/// row-level RLS is ever reached.
fn commons_isolation() -> epistemic_graph::isolation::IsolationLayer {
    let mut isolation = common::current_isolation();
    use epistemic_graph::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
    isolation.add_role(Role::new(COMMONS_USER_ROLE));
    let grant = |action: RbacAction| Grant {
        role: COMMONS_USER_ROLE.to_string(),
        resource: ResourceSelector::Graph("__commons__".to_string()),
        action,
        effect: GrantEffect::Allow,
    };
    isolation.add_grant(grant(RbacAction::Read));
    isolation.add_grant(grant(RbacAction::Write));
    isolation
}

fn req(id: u64, method: Method) -> Request {
    common::signed_request(SECRET, id, "__commons__", method)
}

fn req_as(id: u64, agent: &str, method: Method) -> Request {
    common::signed_request_as(SECRET, id, "__commons__", agent, method)
}

fn pack(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

async fn ok(state: &Arc<RwLock<ServerState>>, id: u64, method: Method) {
    let r = Box::pin(dispatch(state, req(id, method))).await;
    assert!(r.error.is_none(), "op {id} failed: {:?}", r.error);
}

fn register_identity_req(id: u64, actor: &str, agent_id: &str, role: AgentRole) -> Request {
    common::signed_register_identity_request(common::SignedRegisterIdentity {
        secret: SECRET,
        id,
        graph: "__commons__",
        actor,
        registered_agent: agent_id,
        role,
        teams: Vec::new(),
        roles: vec![COMMONS_USER_ROLE.to_string()],
    })
}

/// Three `Robot` nodes -- `alice-private` (owner `alice`), `bob-private` (owner `bob`),
/// and `public` (visible to both) -- seeded as the provisioned `System` test identity
/// (writes are not row-gated). `alice`/`bob` are then registered as plain `Agent`-role
/// peers, flipping the graph into RLS-enforcing mode for every subsequent read.
///
/// Deliberately adversarial: `bob-private`'s embedding is set to EXACTLY the probe
/// vector every test below queries with, and its `name`/`description` carry every
/// keyword the `Discover` test searches for -- the worst case for a leak (it would win
/// an unfiltered top-k on every signal at once).
async fn seed(state: &Arc<RwLock<ServerState>>) {
    ok(
        state,
        1,
        Method::AddNode {
            node_id: "alice-private".into(),
            properties_msgpack: pack(json!({
                "type": "Robot",
                "_owner": "alice",
                "_visibility": "private",
                "name": "alice private plan",
                "description": "alice confidential notes",
            })),
        },
    )
    .await;
    ok(
        state,
        2,
        Method::AddEmbedding {
            node_id: "alice-private".into(),
            embedding: vec![0.9, 0.1],
        },
    )
    .await;

    ok(
        state,
        3,
        Method::AddNode {
            node_id: "bob-private".into(),
            properties_msgpack: pack(json!({
                "type": "Robot",
                "_owner": "bob",
                "_visibility": "private",
                "name": "bob secret rollout plan",
                "description": "bob confidential rollout notes",
            })),
        },
    )
    .await;
    ok(
        state,
        4,
        Method::AddEmbedding {
            node_id: "bob-private".into(),
            embedding: vec![0.0, 1.0],
        },
    )
    .await;

    ok(
        state,
        5,
        Method::AddNode {
            node_id: "public".into(),
            properties_msgpack: pack(json!({
                "type": "Robot",
                "_visibility": "public",
                "name": "shared rollout plan",
                "description": "public rollout notes",
            })),
        },
    )
    .await;
    ok(
        state,
        6,
        Method::AddEmbedding {
            node_id: "public".into(),
            embedding: vec![0.5, 0.5],
        },
    )
    .await;

    // Register the two peer identities -- the SAME two-step (System actor registers
    // `root`, `root` registers the peers) `advanced_crossmodal_roundtrip.rs` uses.
    let r = Box::pin(dispatch(
        state,
        register_identity_req(900, common::TEST_AGENT, "root", AgentRole::System),
    ))
    .await;
    assert!(r.error.is_none(), "root registration failed: {:?}", r.error);
    for (i, agent) in [(901u64, "alice"), (902, "bob")] {
        let r = Box::pin(dispatch(
            state,
            register_identity_req(i, "root", agent, AgentRole::Agent),
        ))
        .await;
        assert!(
            r.error.is_none(),
            "RegisterIdentity {agent} failed: {:?}",
            r.error
        );
    }
}

fn semantic_search_rows(r: &Response) -> Vec<(String, f32)> {
    assert!(r.error.is_none(), "SemanticSearch failed: {:?}", r.error);
    match &r.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).expect("row decode"),
        other => panic!("expected Raw result, got {other:?}"),
    }
}

fn discover_ids(r: &Response) -> Vec<String> {
    assert!(r.error.is_none(), "Discover failed: {:?}", r.error);
    match &r.result {
        Some(ResultPayload::Json(serde_json::Value::Array(items))) => items
            .iter()
            .map(|v| {
                v.get("id")
                    .and_then(|x| x.as_str())
                    .unwrap_or_default()
                    .to_string()
            })
            .collect(),
        other => panic!("expected Json array result, got {other:?}"),
    }
}

/// GOC-08 security check (au-audit follow-up): `Method::SemanticSearch` must never
/// return another agent's private node -- even when that node's embedding is the exact
/// query vector and the result budget is small enough that an unfiltered ranking would
/// necessarily spend it there.
#[tokio::test]
async fn served_semantic_search_never_returns_another_agents_private_node() {
    let state = state().await;
    seed(&state).await;

    // alice queries with bob's OWN embedding -- the worst case: if RLS were bypassed,
    // or (the au bug's shape) applied only AFTER ranking/trimming, bob-private would
    // score highest and consume the tight n_results=2 budget entirely.
    let r = Box::pin(dispatch(
        &state,
        req_as(
            10,
            "alice",
            Method::SemanticSearch {
                query_embedding: vec![0.0, 1.0],
                n_results: 2,
            },
        ),
    ))
    .await;
    let rows = semantic_search_rows(&r);
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert!(
        !ids.contains(&"bob-private"),
        "alice's SemanticSearch must never surface bob's private node, even as the exact \
         query match and even under a budget an unfiltered ranking would spend entirely \
         on it: {ids:?}"
    );
    assert!(
        ids.contains(&"alice-private") && ids.contains(&"public"),
        "alice must still see her own row and the public row (proves the query was not \
         simply denied outright): {ids:?}"
    );

    // Sanity: bob himself CAN see his own private node for the identical query --
    // proves bob-private really is a top contender, so the negative assertion above is
    // not vacuously true because it was never a candidate in the first place.
    let r = Box::pin(dispatch(
        &state,
        req_as(
            11,
            "bob",
            Method::SemanticSearch {
                query_embedding: vec![0.0, 1.0],
                n_results: 2,
            },
        ),
    ))
    .await;
    let rows = semantic_search_rows(&r);
    assert!(
        rows.iter().any(|(id, _)| id == "bob-private"),
        "sanity: bob must see his own private node for the same query: {rows:?}"
    );
}

/// Sibling proof for `Method::Discover` (CONCEPT:EG-KG.retrieval.one-round-trip-discovery):
/// the hybrid keyword+vector discovery surface must not hydrate or return another
/// agent's private node's id/name/description, even when both its embedding AND its
/// keywords are the strongest possible match.
#[tokio::test]
async fn served_discover_never_returns_another_agents_private_node() {
    let state = state().await;
    seed(&state).await;

    let r = Box::pin(dispatch(
        &state,
        req_as(
            20,
            "alice",
            Method::Discover {
                keywords: vec!["bob".into(), "secret".into(), "rollout".into()],
                query_embedding: vec![0.0, 1.0],
                k: 2,
            },
        ),
    ))
    .await;
    let ids = discover_ids(&r);
    assert!(
        !ids.contains(&"bob-private".to_string()),
        "alice's Discover must never surface bob's private node, even though its name/\
         description are the strongest keyword match ('bob', 'secret', 'rollout') and \
         its embedding is the strongest vector match: {ids:?}"
    );

    // Sanity: bob himself can discover his own node with the identical query.
    let r = Box::pin(dispatch(
        &state,
        req_as(
            21,
            "bob",
            Method::Discover {
                keywords: vec!["bob".into(), "secret".into(), "rollout".into()],
                query_embedding: vec![0.0, 1.0],
                k: 2,
            },
        ),
    ))
    .await;
    let ids = discover_ids(&r);
    assert!(
        ids.contains(&"bob-private".to_string()),
        "sanity: bob must discover his own private node for the same query: {ids:?}"
    );
}
