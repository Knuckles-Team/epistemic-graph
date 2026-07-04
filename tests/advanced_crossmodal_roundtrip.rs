//! Advanced cross-modal seam ROUNDTRIP proofs (CONCEPT:EG-390..EG-397).
//!
//! The advanced cross-modal tests whose assertions need a LIVE server/txn surface a
//! hand-built `PlanCtx` cannot stand up — driven through the REAL `dispatch` shell over an
//! in-process `ServerState` (begin → stage → overlaid-read → commit, exactly as a client),
//! the SAME harness `query.rs`'s `txn_ryow_dispatch_tests` uses.
//!
//! GREEN here:
//!  * **EG-390 (capstone, test 2)** — 5-modality in-txn read-your-own-writes → atomic
//!    commit → consistent off-txn re-read: a graph node, a vector embedding, a graph edge,
//!    a native tsdb measurement and a staged OWL axiom, all staged in ONE txn, all visible
//!    to in-txn cross-modal queries (RYOW) and invisible off-txn, then committed atomically
//!    (the committed graph/vector/axiom durable-in-memory) and re-read.
//!  * **EG-392 (test 9)** — concurrent SERIALIZABLE across modalities: a serializable txn
//!    with a captured predicate read-set commits AFTER a concurrent txn inserts a phantom
//!    matching that predicate → the phantom flips `validate()` and the serializable txn
//!    rolls back (`Commit` returns `false`), while its own cross-modal writes never land.
//!
//! Tracked-but-`#[ignore]`d (each is a north_star.md open row with its precise gap):
//!  * EG-391 (test 5)  — RLS per-agent visibility on a fused Reason→Rank + the overlay path.
//!  * EG-393 (test 10) — pgwire + `/sparql` + native consistent snapshot after a mixed commit.
//!  * EG-394 (test 7)  — encryption-at-rest + cross-modal read; a wrong key FAILS.
//!  * EG-395 (test 8)  — streaming/CDC → live materialized cross-modal view rebuild.
//!  * EG-396 (test 4)  — cross-shard txn spanning modalities under Raft; kill coordinator
//!    mid-2PC → single decision.
//!  * EG-397 (test 11) — KV-cache warm-fork fan-out reusing cross-modal context.
//!
//! Gated at the module level on `query` + `tsdb` + `owl-plan` (the in-txn cross-modal
//! executor legs); it compiles + runs under `--features full`.

#![cfg(all(feature = "query", feature = "tsdb", feature = "owl-plan"))]

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{compute_auth_token, dispatch, ServerState};

const SECRET: &str = "advanced-crossmodal-secret";

/// A fully-featured in-memory `ServerState` (no persistence → the in-memory durability
/// tier: graph/vector/axiom writes apply in-memory at commit, durable-only measurements
/// are dropped — documented in `handlers::txn::commit_cross_modal_txn`).
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

fn pack(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// MessagePack `Vec<(i64 ts_ns, Vec<f64> values)>` — the exact `TxnAddMeasurement.points`
/// blob shape (CONCEPT:EG-360).
fn pack_points(points: &[(i64, Vec<f64>)]) -> Vec<u8> {
    rmp_serde::to_vec_named(&points.to_vec()).unwrap()
}

async fn begin(state: &Arc<RwLock<ServerState>>, id: u64, isolation: Option<String>) -> String {
    let r = dispatch(
        state,
        req(
            id,
            Method::BeginTxn {
                graph: None,
                isolation,
            },
        ),
    )
    .await;
    match r.result {
        Some(ResultPayload::String(s)) => s,
        other => panic!("BeginTxn failed: {:?} / {other:?}", r.error),
    }
}

async fn ok(state: &Arc<RwLock<ServerState>>, id: u64, method: Method) {
    let r = dispatch(state, req(id, method)).await;
    assert!(r.error.is_none(), "op {id} failed: {:?}", r.error);
}

/// Decode a unified-query response into its result node ids.
fn unified_ids(resp: &Response) -> Vec<String> {
    assert!(
        resp.error.is_none(),
        "unified query error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let rows: Vec<(String, Option<f32>)> = rmp_serde::from_slice(&bytes).unwrap();
    rows.into_iter().map(|(id, _)| id).collect()
}

async fn in_txn_text(
    state: &Arc<RwLock<ServerState>>,
    id: u64,
    txn: &str,
    text: &str,
) -> Vec<String> {
    let r = dispatch(
        state,
        req(
            id,
            Method::TxnUnifiedQueryText {
                txn_id: txn.into(),
                text: text.into(),
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    unified_ids(&r)
}

async fn off_txn_text(state: &Arc<RwLock<ServerState>>, id: u64, text: &str) -> Vec<String> {
    let r = dispatch(
        state,
        req(
            id,
            Method::UnifiedQueryText {
                text: text.into(),
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    unified_ids(&r)
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-390 (test 2, capstone) — 5-modality in-txn RYOW → atomic commit → re-read
// ─────────────────────────────────────────────────────────────────────────────

/// THE capstone (CONCEPT:EG-390): in ONE transaction stage FIVE modalities — a graph node
/// (`sn` typed `Robot`), a vector embedding, a graph edge (`sn -LINKS-> tn`), a native tsdb
/// measurement (series `sensor.temp`), and an OWL axiom (`Robot ⊑ Machine`) — then prove:
///  1. **RYOW**: every staged modality is visible to an in-txn cross-modal query — the
///     graph node + embedding rank, the staged edge is BFS-reachable, the tsdb measurement
///     is `TsScan`-readable (via the `StagedSeries` overlay), and the OWL reasoner infers
///     `sn` a `Machine` over the staged graph write;
///  2. **isolation**: an identical OFF-txn query sees NONE of it before commit;
///  3. **atomic commit** lands the durable-in-memory modalities together (`Commit` → true);
///  4. **consistent re-read**: after commit the graph node + embedding + edge are visible
///     off-txn, AND the committed axiom makes `REASON <Machine>` (empty ontology, read from
///     the committed TBox) return `sn` — the OWL modality committed atomically with the rest.
///
/// (The staged measurement is durable-only; on this in-memory-tier engine it is dropped at
/// commit by design — its RYOW visibility in step 1 is the in-txn proof. The redb-durable
/// atomic-commit-of-measurement path is exercised by the durability suite.)
#[tokio::test]
async fn five_modality_in_txn_ryow_then_commit_eg390() {
    let state = state();
    let txn = begin(&state, 1, None).await;

    // ── stage all five modalities ──
    ok(
        &state,
        2,
        Method::TxnAddNode {
            txn_id: txn.clone(),
            node_id: "sn".into(),
            properties_msgpack: pack(json!({ "type": "Robot" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::TxnAddEmbedding {
            txn_id: txn.clone(),
            node_id: "sn".into(),
            embedding: vec![1.0, 0.0],
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        4,
        Method::TxnAddNode {
            txn_id: txn.clone(),
            node_id: "tn".into(),
            properties_msgpack: pack(json!({ "type": "Gadget" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        5,
        Method::TxnAddEdge {
            txn_id: txn.clone(),
            source_id: "sn".into(),
            target_id: "tn".into(),
            properties_msgpack: pack(json!({ "relationship": "LINKS" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        6,
        Method::TxnAddMeasurement {
            txn_id: txn.clone(),
            series: "sensor.temp".into(),
            points: pack_points(&[(1_000_000_000, vec![21.0]), (2_000_000_000, vec![22.0])]),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        7,
        Method::TxnAxiom {
            txn_id: txn.clone(),
            turtle: "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
                     <http://ex/Robot> rdfs:subClassOf <http://ex/Machine> .\n"
                .into(),
            graph: None,
        },
    )
    .await;

    // ── 1. RYOW: graph + vector ──
    assert_eq!(
        in_txn_text(
            &state,
            8,
            &txn,
            "MATCH (:Robot) |> RANK BY ~[1.0,0.0] |> LIMIT 5"
        )
        .await,
        vec!["sn".to_string()],
        "staged node + embedding must be visible to the in-txn cross-modal query"
    );
    // ── 1. RYOW: graph edge (BFS over the staged edge) ──
    assert!(
        in_txn_text(
            &state,
            9,
            &txn,
            "MATCH (:Robot) |> TRAVERSE -[:LINKS]->{1,1} |> LIMIT 5"
        )
        .await
        .contains(&"tn".to_string()),
        "the staged edge must make tn BFS-reachable in-txn"
    );
    // ── 1. RYOW: native tsdb measurement via TsScan over the StagedSeries overlay ──
    let ts_plan = eg_plan::Plan::new(vec![eg_plan::Op::TsScan {
        series: vec!["sensor.temp".into()],
        from: 0.0,
        to: 10.0,
    }]);
    let ts_resp = dispatch(
        &state,
        req(
            10,
            Method::TxnUnifiedQuery {
                txn_id: txn.clone(),
                plan: ts_plan,
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    assert_eq!(
        unified_ids(&ts_resp).len(),
        2,
        "the staged measurement's 2 points must be TsScan-readable in-txn (RYOW), got {:?}",
        unified_ids(&ts_resp)
    );
    // ── 1. RYOW: OWL reasoning over the staged graph write (inline ontology — the staged
    // axiom lands in the graph at COMMIT, so in-txn the ontology is supplied to REASON). ──
    let reason_plan = eg_plan::Plan::new(vec![
        eg_plan::Op::Scan {
            label: "Robot".into(),
        },
        eg_plan::Op::Reason {
            target_class: "<http://ex/Machine>".into(),
            ontology: "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
                       <http://ex/Robot> rdfs:subClassOf <http://ex/Machine> .\n"
                .into(),
        },
    ]);
    let reason_resp = dispatch(
        &state,
        req(
            11,
            Method::TxnUnifiedQuery {
                txn_id: txn.clone(),
                plan: reason_plan,
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    assert_eq!(
        unified_ids(&reason_resp),
        vec!["sn".to_string()],
        "the OWL reasoner must infer the staged Robot node a Machine in-txn (RYOW)"
    );

    // ── 2. isolation: an identical off-txn query sees none of the staged writes ──
    assert!(
        off_txn_text(
            &state,
            12,
            "MATCH (:Robot) |> RANK BY ~[1.0,0.0] |> LIMIT 5"
        )
        .await
        .is_empty(),
        "off-txn query must see none of the txn's uncommitted writes"
    );

    // ── 3. atomic commit ──
    let c = dispatch(
        &state,
        req(
            13,
            Method::Commit {
                txn_id: txn.clone(),
            },
        ),
    )
    .await;
    assert!(
        matches!(c.result, Some(ResultPayload::Bool(true))),
        "5-modality cross-modal commit must succeed atomically: {:?}",
        c.error
    );

    // ── 4. consistent re-read off-txn: graph + vector + edge ──
    assert_eq!(
        off_txn_text(
            &state,
            14,
            "MATCH (:Robot) |> RANK BY ~[1.0,0.0] |> LIMIT 5"
        )
        .await,
        vec!["sn".to_string()],
        "the committed node + embedding are visible off-txn after commit"
    );
    assert!(
        off_txn_text(
            &state,
            15,
            "MATCH (:Robot) |> TRAVERSE -[:LINKS]->{1,1} |> LIMIT 5"
        )
        .await
        .contains(&"tn".to_string()),
        "the committed edge is visible off-txn after commit"
    );
    // ── 4. the committed OWL axiom is in the TBox: REASON with EMPTY ontology (read from
    // the committed graph) now infers sn a Machine — the axiom modality committed atomically. ──
    let committed_reason = eg_plan::Plan::new(vec![
        eg_plan::Op::Scan {
            label: "Robot".into(),
        },
        eg_plan::Op::Reason {
            target_class: "<http://ex/Machine>".into(),
            ontology: String::new(),
        },
    ]);
    let cr = dispatch(
        &state,
        req(
            16,
            Method::UnifiedQuery {
                plan: committed_reason,
                reorder_filter_selectivity: None,
            },
        ),
    )
    .await;
    assert_eq!(
        unified_ids(&cr),
        vec!["sn".to_string()],
        "the committed axiom (Robot ⊑ Machine) must be visible to REASON off-txn (durable OWL)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-392 (test 9) — concurrent SERIALIZABLE across modalities: phantom → conflict
// ─────────────────────────────────────────────────────────────────────────────

/// THE serializable proof (CONCEPT:EG-392): a SERIALIZABLE txn `A` captures a predicate
/// read-set (nodes labelled `Sensor`) at begin and stages a cross-modal write (a node + its
/// embedding). A concurrent txn `B` INSERTS a phantom `Sensor` and COMMITS. When `A` then
/// commits, its serializable validation re-evaluates the predicate, sees the phantom, and
/// rolls back (`Commit` → `false`) — nothing of `A`'s cross-modal write lands. This is the
/// phantom anomaly a serializable isolation level must reject across modalities.
#[tokio::test]
async fn concurrent_serializable_phantom_conflict_eg392() {
    let state = state();
    // Seed one committed Sensor so the predicate read-set is non-empty at A's begin.
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "s0".into(),
            properties_msgpack: pack(json!({ "type": "Sensor" })),
        },
    )
    .await;

    // A: serializable, predicate over label=Sensor — captures {s0} at begin.
    let a = begin(&state, 2, Some("serializable:label=Sensor".into())).await;
    // A stages a cross-modal write (node + embedding) — must NOT land if A conflicts.
    ok(
        &state,
        3,
        Method::TxnAddNode {
            txn_id: a.clone(),
            node_id: "a_node".into(),
            properties_msgpack: pack(json!({ "type": "Widget" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        4,
        Method::TxnAddEmbedding {
            txn_id: a.clone(),
            node_id: "a_node".into(),
            embedding: vec![1.0, 0.0],
            graph: None,
        },
    )
    .await;

    // B: a concurrent txn inserts a PHANTOM Sensor and commits (now in the committed store).
    let b = begin(&state, 5, None).await;
    ok(
        &state,
        6,
        Method::TxnAddNode {
            txn_id: b.clone(),
            node_id: "s_phantom".into(),
            properties_msgpack: pack(json!({ "type": "Sensor" })),
            graph: None,
        },
    )
    .await;
    let cb = dispatch(&state, req(7, Method::Commit { txn_id: b.clone() })).await;
    assert!(
        matches!(cb.result, Some(ResultPayload::Bool(true))),
        "B must commit its phantom Sensor: {:?}",
        cb.error
    );

    // A commits AFTER B's phantom: serializable validation re-evaluates the Sensor
    // predicate, detects the phantom, and rolls A back.
    let ca = dispatch(&state, req(8, Method::Commit { txn_id: a.clone() })).await;
    assert!(
        matches!(ca.result, Some(ResultPayload::Bool(false))),
        "A's serializable commit must CONFLICT on the phantom Sensor (got {:?} / {:?})",
        ca.result,
        ca.error
    );

    // A's cross-modal write never landed (true rollback across modalities).
    assert!(
        off_txn_text(&state, 9, "MATCH (:Widget) |> LIMIT 5")
            .await
            .is_empty(),
        "the conflicted serializable txn's staged node must not be visible"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Tracked-but-unbuilt advanced roundtrips — executable specs, `#[ignore]`d with the
// precise seam gap (each is also a docs/north_star.md OPEN row).
// ─────────────────────────────────────────────────────────────────────────────

/// EG-391 (test 5) — RLS per-agent visibility on a fused Reason→Rank AND the overlay path.
/// Contract: with an `IsolationLayer` carrying per-agent row-visibility rules, an in-txn
/// `run_unified_overlaid` (which ALREADY calls `rls.filter_view` on the committed snapshot,
/// see `handlers/query.rs`) must hide rows agent B may not see from BOTH the committed and
/// the staged-overlay legs of a fused `Reason → Rank`, while agent A (the owner) sees them.
#[tokio::test]
#[ignore = "seam not wired at test surface: RLS-in-unified needs a row-visibility fixture \
            (isolation identities + per-node owner/grant blobs via row_visibility) AND the \
            caller threaded into TxnUnifiedQuery (dispatch passes agent_id, but this test \
            harness does not yet register RLS rules). filter_view exists in \
            crates/eg-core/src/isolation.rs:386; wiring is src/server/handlers/query.rs \
            run_unified_overlaid caller/rls args. Achievable — deferred."]
async fn rls_per_agent_fused_reason_rank_overlay_eg391() {
    // Contract asserted above; harness fixture (RLS identities + owner-tagged node blobs)
    // is the remaining lift.
}

/// EG-393 (test 10) — pgwire + `/sparql` + native consistent snapshot after a mixed commit.
/// Contract: after a cross-modal `BEGIN…COMMIT` (graph + vector + axiom), a read over the
/// pgwire wire, a read over the HTTP `/sparql` surface, and a native `UnifiedQuery` must all
/// observe the SAME committed snapshot (no surface sees a torn/partial state).
#[tokio::test]
#[ignore = "seam not wired at test surface: needs the pgwire listener + a tokio-postgres \
            client (see tests/pgwire_roundtrip.rs) AND the sparql-http server bound in the \
            same ServerState, then a tri-surface read after one commit. The committed \
            machinery exists (EG-372 pgwire cross-modal + sparql_http); the multi-listener \
            harness is the lift. Achievable — deferred."]
async fn pgwire_sparql_native_consistent_snapshot_eg393() {
    // Contract asserted above; multi-surface listener harness is the remaining lift.
}

/// EG-394 (test 7) — encryption-at-rest + cross-modal read; a wrong key FAILS (no silent
/// plaintext). Contract: a `security`-encrypted redb backend written with key K1 and reopened
/// with a WRONG key K2 must ERROR on the cross-modal read rather than returning plaintext.
#[tokio::test]
#[ignore = "seam not wired at test surface: needs a security-encrypted RedbBackend \
            (src/crypto.rs + src/server/persistence/redb_backend.rs) opened with key K1, a \
            cross-modal commit, then a reopen with a wrong key asserting a decrypt ERROR. \
            The encryption backend exists; the keyed open/reopen roundtrip is the lift. \
            Achievable — deferred."]
async fn encryption_at_rest_wrong_key_fails_eg394() {
    // Contract asserted above; keyed redb open/reopen roundtrip is the remaining lift.
}

/// EG-395 (test 8) — streaming/CDC → live materialized cross-modal view rebuild. Contract: a
/// cross-modal commit emits CDC events on the `CdcHub`; a materialized view subscribed to the
/// change stream rebuilds to reflect the new cross-modal state.
#[tokio::test]
#[ignore = "seam not wired at test surface: needs a CDC subscription on state.cdc \
            (src/server/cdc.rs CdcHub) + a MatViewStore rebuild assertion after a \
            cross-modal commit (compute-dist matviews). Both surfaces exist; the \
            subscribe→commit→rebuild-observe harness is the lift. Achievable — deferred."]
async fn streaming_cdc_matview_rebuild_eg395() {
    // Contract asserted above; CDC-subscribe + matview-rebuild harness is the remaining lift.
}

/// EG-396 (test 4) — cross-shard txn spanning modalities under Raft; kill the coordinator
/// mid-2PC → a SINGLE decision (all shards commit or all abort — no split-brain). Contract:
/// a 2-shard cross-modal txn whose coordinator is killed between prepare and commit resolves
/// to one atomic outcome on recovery.
#[tokio::test]
#[ignore = "seam not built at test surface: NO in-process multi-node openraft cluster + 2PC \
            coordinator-kill harness exists in tests/ (raft/multi_raft live behind the \
            cluster feature in src/server/state.rs; there is no test scaffold that stands up \
            >1 Raft node and injects a mid-2PC coordinator failure). Genuine gap — a cluster \
            test harness is required before this can be written green."]
async fn cross_shard_raft_2pc_single_decision_eg396() {
    // Contract asserted above; a multi-node Raft + 2PC-kill test harness does not yet exist.
}

/// EG-397 (test 11) — KV-cache warm-fork fan-out reusing cross-modal context, isolated on
/// divergent writes. Contract: a warm parent holding fused cross-modal context is forked to N
/// children that share the read context but isolate their own divergent writes.
#[tokio::test]
#[ignore = "seam not in this repo: the warm-FORK sandbox capability (ForkableSandbox / \
            WarmParentRegistry, ORCH-1.86..93) lives in agent-utilities, NOT epistemic-graph; \
            eg-kvcache here provides the KV page store (dedup/LRU/data-version EG-364) but no \
            fork primitive. Cross-repo gap — the fan-out test belongs to the agent-utilities \
            warm-fork surface consuming this engine's cross-modal context."]
async fn kvcache_warm_fork_fanout_eg397() {
    // Contract asserted above; the warm-fork primitive lives in agent-utilities, not here.
}
