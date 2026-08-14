//! Advanced cross-modal seam ROUNDTRIP proofs (CONCEPT:EG-KG.txn.one-transaction-stage-five..EG-397).
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
//!  * **EG-391 (test 5, CONCEPT:EG-KG.query.overlay-leg-rls-filter)** — RLS per-agent visibility on a fused Reason→Rank
//!    over BOTH the committed base and the staged-overlay legs (identities + `_owner`/
//!    `_visibility` node blobs + caller-threaded `agent_id`; the overlay-leg seam is a
//!    post-overlay `rls.filter_view` in `run_unified_overlaid`).
//!  * **EG-393 (test 10, CONCEPT:EG-KG.query.tri-surface-snapshot-harness)** — pgwire + `/sparql` + native consistent snapshot:
//!    three listeners on ONE `ServerState` all observe the SAME mixed committed snapshot.
//!  * **EG-394 (test 7, CONCEPT:EG-KG.storage.encryption-reopen-roundtrip)** — encryption-at-rest + cross-modal read; a keyed
//!    `RedbBackend` reopened with the WRONG key FAILS the read (no silent plaintext).
//!  * **EG-395 (test 8, CONCEPT:EG-KG.query.cdc-live-view-rebuild)** — streaming/CDC → live materialized cross-modal
//!    view rebuild via a `CdcHub` continuous query maintained off the change stream.
//!
//! EG-396 (test 4, CONCEPT:EG-KG.txn.crossshard-2pc-modality-harness) is now GREEN under
//! `--features cluster`: it is `cfg(feature = "cluster")`-gated (compiles out of the
//! `--features full` gate, runs under `--features cluster`) and drives the in-crate
//! `raft::xshard_modality_harness` multi-group 2PC coordinator-kill proof.
//!
//! Tracked-but-`#[ignore]`d (a north_star.md open row with its refined precise gap):
//!  * EG-397 (test 11, CONCEPT:EG-KG.query.warmfork-fanout-open-reason) — KV-cache warm-fork fan-out. CROSS-REPO gap (the
//!    warm-fork primitive lives in agent-utilities, not this engine).
//!
//! Gated at the module level on `query` + `tsdb` + `owl-plan` (the in-txn cross-modal
//! executor legs); it compiles + runs under `--features full`.

#![cfg(all(feature = "query", feature = "tsdb", feature = "owl-plan"))]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "advanced-crossmodal-secret";

/// A fully-featured state with a REAL redb persistence backend. This was `persist_dir:
/// Some(..)` / `persistence: None` -- a scratch dir with no durable backend behind it,
/// which satisfied the owner-scoped SQL catalog requirement but made every OTHER
/// authoritative mutation fail closed the instant `dispatch()` routed it: "authoritative
/// MutationBatch commit requires a persistence backend". That error is CORRECT (the
/// dispatch path deliberately fail-closes durable-domain methods against a backend-less
/// state) -- the FIXTURE was wrong. Same fix shape as `server::mod::tests::test_state` /
/// `bolt_wire::tests::durable_state` / every other `tests/*.rs` fixture that calls
/// `common::tempdir_persistence()` (its unique per-call tempdir doubles as the SQL
/// adapter's owner-scoped catalog directory, so nothing is lost).
///
/// The multi-op `BeginTxn`..`Commit` path additionally seals its transaction recovery
/// plan (`server::handlers::txn::seal_txn_recovery_plan`), which fail-closed REQUIRES
/// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` at the backend's `open()` call -- the same
/// requirement `redb_backend::tests::cm_dir` provisions. Encryption is symmetric and
/// transparent to every round-trip these tests make, so a keyed store behaves
/// identically for their assertions.
///
/// This USED to provision the env var via a `std::sync::Once` -- set it a single time
/// for the whole process and never touch it again. That was the ambient-process-state
/// bug this fixture had: `Once` only guarantees the *set* runs once, not that the
/// value *stays* set, and `encryption_at_rest_wrong_key_fails_eg394` (same file)
/// legitimately mutates this SAME process-global env var (K1 -> K2-wrong -> restore)
/// while it runs. Every call to `state()` opens its OWN fresh `RedbBackend`
/// (`common::tempdir_persistence()`), and `RedbBackend::open` resolves + caches the
/// cipher freshly, synchronously, EVERY time it is called -- so a `state()` call whose
/// `open()` happened to land in the window after eg394 removed the var (but after
/// `Once` had already fired for some earlier, unrelated test) saw no key configured at
/// all and failed closed with "transaction durability requires
/// EPISTEMIC_GRAPH_ENCRYPTION_KEY to be configured", regardless of what THIS test
/// actually needed. On a highly parallel dev host that interleaving rarely landed on
/// the removed window; on a 2-core CI runner it reliably did.
///
/// Fix: every call re-provisions the var (not just the first) AND does so under the
/// SAME `ENC_ENV_LOCK` eg394 holds for its entire body, so the two participants that
/// mutate this process-global never interleave. This still mutates a process-global
/// (see the module doc on `RedbBackend::open` for why: `ValueCipher::from_env` is the
/// only key-provisioning seam that exists today -- there is no `RedbBackend::open`
/// overload or `DurabilityPolicy` variant that accepts key material directly). A
/// non-ambient fix would need a new constructor seam, e.g.
/// `RedbBackend::open_with_cipher(persist_dir, policy, capacity, Option<ValueCipher>)`,
/// so a test could pass `ValueCipher::from_key_material(..)` straight in without
/// touching `std::env` at all; see the CLAUDE.md / program notes for that as a
/// follow-up rather than a silent scope-creep here.
async fn state() -> Arc<RwLock<ServerState>> {
    let (persist_dir, persistence) = {
        #[cfg(all(feature = "security", feature = "redb"))]
        let _guard = ENC_ENV_LOCK.lock().await;
        #[cfg(feature = "redb")]
        std::env::set_var(
            epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
            "advanced-crossmodal-recovery-key",
        );
        common::configure_authority();
        common::tempdir_persistence()
    };
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: commons_isolation(),
        channels: ChannelManager::new(),
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
        // A real per-test temp series store (mirrors `server::mod::tests::test_state` /
        // `served_mining_tsdb_scan.rs`'s `tmp_series`). This was `None`, which made a
        // cross-modal commit that staged a `TxnAddMeasurement` fail closed at Commit time:
        // "committed measurements require the served time-series store" -- the same
        // authoritative-projection requirement `project_committed_measurements`
        // (`server::handlers::txn`) enforces repo-wide, not something this test's own
        // doc comment (staged measurements are "dropped at commit by design") still holds
        // now that that projection exists. Once staged, a measurement's commit MUST have a
        // served store to project into.
        #[cfg(feature = "tsdb")]
        tsdb_store: Some({
            static NEXT_TSDB_SEQ: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(1);
            let path = std::env::temp_dir().join(format!(
                "eg-crossmodal-tsdb-test-{}-{}.redb",
                std::process::id(),
                NEXT_TSDB_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::sync::Arc::new(
                eg_tsdb::store::SeriesStore::open(&path).expect("open test series store"),
            )
        }),
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

/// `common::current_isolation()` plus a "commons-user" RBAC role granted Read+Write on
/// `__commons__`. RBAC (CONCEPT:EG-KG.compute.feature) is the mandatory access decision under
/// `feature = "security"` (`isolation.rs::check_access`) -- there is no pre-RBAC
/// Commons-open-to-all fall-through any more for a non-`System` identity (see the
/// `#[cfg(not(feature = "security"))]` branch vs. the RBAC branch in `check_access`).
/// `common::TEST_AGENT` is registered `System`-role, so it always bypasses RBAC and never
/// needed this; but the `agent_a`/`agent_b`/etc. peers this file registers through
/// `register_identity_req` (plain `AgentRole::Agent`) do, or every RPC they make against
/// `__commons__` is `ACCESS_DENIED`. Same fix shape as `server::mod::multi_tenant_state`'s
/// `"commons-user"` role (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence): the role +
/// grants live on the policy, `register_identity_req` threads the role NAME onto each
/// registered peer's `AgentIdentity.roles`.
fn commons_isolation() -> epistemic_graph::isolation::IsolationLayer {
    let mut isolation = common::current_isolation();
    #[cfg(feature = "security")]
    {
        use epistemic_graph::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
        isolation.add_role(Role::new("commons-user"));
        let grant = |action: RbacAction| Grant {
            role: "commons-user".to_string(),
            resource: ResourceSelector::Graph("__commons__".to_string()),
            action,
            effect: GrantEffect::Allow,
        };
        isolation.add_grant(grant(RbacAction::Read));
        isolation.add_grant(grant(RbacAction::Write));
    }
    isolation
}

/// The RBAC role name every non-`System` peer this file registers needs on `__commons__`
/// (see [`commons_isolation`]).
#[cfg(feature = "security")]
const COMMONS_USER_ROLE: &str = "commons-user";

fn req(id: u64, method: Method) -> Request {
    common::signed_request(SECRET, id, "__commons__", method)
}

fn pack(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// MessagePack `Vec<(i64 ts_ns, Vec<f64> values)>` — the exact `TxnAddMeasurement.points`
/// blob shape (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
fn pack_points(points: &[(i64, Vec<f64>)]) -> Vec<u8> {
    rmp_serde::to_vec_named(&points.to_vec()).unwrap()
}

async fn begin(state: &Arc<RwLock<ServerState>>, id: u64, isolation: Option<String>) -> String {
    let r = Box::pin(dispatch(
        state,
        req(
            id,
            Method::BeginTxn {
                graph: None,
                isolation,
            },
        ),
    ))
    .await;
    match r.result {
        Some(ResultPayload::String(s)) => s,
        other => panic!("BeginTxn failed: {:?} / {other:?}", r.error),
    }
}

async fn ok(state: &Arc<RwLock<ServerState>>, id: u64, method: Method) {
    let r = Box::pin(dispatch(state, req(id, method))).await;
    assert!(r.error.is_none(), "op {id} failed: {:?}", r.error);
}

#[cfg(feature = "security")]
async fn begin_as(state: &Arc<RwLock<ServerState>>, id: u64, agent: &str) -> String {
    let response = Box::pin(dispatch(
        state,
        req_as(
            id,
            agent,
            Method::BeginTxn {
                graph: None,
                isolation: None,
            },
        ),
    ))
    .await;
    match response.result {
        Some(ResultPayload::String(txn_id)) => txn_id,
        other => panic!(
            "BeginTxn as {agent} failed: {:?} / {other:?}",
            response.error
        ),
    }
}

#[cfg(feature = "security")]
async fn ok_as(state: &Arc<RwLock<ServerState>>, id: u64, agent: &str, method: Method) {
    let response = Box::pin(dispatch(state, req_as(id, agent, method))).await;
    assert!(
        response.error.is_none(),
        "op {id} as {agent} failed: {:?}",
        response.error
    );
}

#[cfg(feature = "security")]
async fn stage_rls_overlay(state: &Arc<RwLock<ServerState>>, first_id: u64, agent: &str) -> String {
    let txn_id = begin_as(state, first_id, agent).await;
    ok_as(
        state,
        first_id + 1,
        agent,
        Method::TxnAddNode {
            txn_id: txn_id.clone(),
            node_id: "stg_pub".into(),
            // Default-deny RLS (`eg_core::isolation::row_visibility`) hides ANY
            // untagged row from a non-`System` actor -- an unowned row needs an
            // EXPLICIT `_visibility: "public"` tag to be seen by `agent_b` at all.
            properties_msgpack: pack(json!({ "type": "Robot", "_visibility": "public" })),
            graph: None,
        },
    )
    .await;
    ok_as(
        state,
        first_id + 2,
        agent,
        Method::TxnAddEmbedding {
            txn_id: txn_id.clone(),
            node_id: "stg_pub".into(),
            embedding: vec![1.0, 0.0],
            graph: None,
        },
    )
    .await;
    ok_as(
        state,
        first_id + 3,
        agent,
        Method::TxnAddNode {
            txn_id: txn_id.clone(),
            node_id: "stg_sec".into(),
            properties_msgpack: pack(
                json!({ "type": "Robot", "_owner": "agent_a", "_visibility": "private" }),
            ),
            graph: None,
        },
    )
    .await;
    ok_as(
        state,
        first_id + 4,
        agent,
        Method::TxnAddEmbedding {
            txn_id: txn_id.clone(),
            node_id: "stg_sec".into(),
            embedding: vec![1.0, 0.0],
            graph: None,
        },
    )
    .await;
    txn_id
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
    let r = Box::pin(dispatch(
        state,
        req(
            id,
            Method::TxnUnifiedQueryText {
                txn_id: txn.into(),
                text: text.into(),
            },
        ),
    ))
    .await;
    unified_ids(&r)
}

async fn off_txn_text(state: &Arc<RwLock<ServerState>>, id: u64, text: &str) -> Vec<String> {
    let r = Box::pin(dispatch(
        state,
        req(id, Method::UnifiedQueryText { text: text.into() }),
    ))
    .await;
    unified_ids(&r)
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-390 (test 2, capstone) — 5-modality in-txn RYOW → atomic commit → re-read
// ─────────────────────────────────────────────────────────────────────────────

/// THE capstone (CONCEPT:EG-KG.txn.one-transaction-stage-five): in ONE transaction stage FIVE modalities — a graph node
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
    let state = state().await;
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
    let ts_resp = Box::pin(dispatch(
        &state,
        req(
            10,
            Method::TxnUnifiedQuery {
                txn_id: txn.clone(),
                plan: ts_plan,
            },
        ),
    ))
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
    let reason_resp = Box::pin(dispatch(
        &state,
        req(
            11,
            Method::TxnUnifiedQuery {
                txn_id: txn.clone(),
                plan: reason_plan,
            },
        ),
    ))
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
    let c = Box::pin(dispatch(
        &state,
        req(
            13,
            Method::Commit {
                txn_id: txn.clone(),
                idempotency_key: None,
            },
        ),
    ))
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
    let cr = Box::pin(dispatch(
        &state,
        req(
            16,
            Method::UnifiedQuery {
                plan: committed_reason,
            },
        ),
    ))
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

/// THE serializable proof (CONCEPT:EG-KG.txn.serializable-txn-captures-predicate): a SERIALIZABLE txn `A` captures a predicate
/// read-set (nodes labelled `Sensor`) at begin and stages a cross-modal write (a node + its
/// embedding). A concurrent txn `B` INSERTS a phantom `Sensor` and COMMITS. When `A` then
/// commits, its serializable validation re-evaluates the predicate, sees the phantom, and
/// rolls back (`Commit` → `false`) — nothing of `A`'s cross-modal write lands. This is the
/// phantom anomaly a serializable isolation level must reject across modalities.
#[tokio::test]
async fn concurrent_serializable_phantom_conflict_eg392() {
    let state = state().await;
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
    let cb = Box::pin(dispatch(
        &state,
        req(7, Method::Commit { txn_id: b.clone(), idempotency_key: None }),
    ))
    .await;
    assert!(
        matches!(cb.result, Some(ResultPayload::Bool(true))),
        "B must commit its phantom Sensor: {:?}",
        cb.error
    );

    // A commits AFTER B's phantom: serializable validation re-evaluates the Sensor
    // predicate, detects the phantom, and rolls A back.
    let ca = Box::pin(dispatch(
        &state,
        req(8, Method::Commit { txn_id: a.clone(), idempotency_key: None }),
    ))
    .await;
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
// Advanced roundtrips CLOSED (handoff-1 track F, CONCEPT:EG-KG.query.overlay-leg-rls-filter..EG-KG.query.cdc-live-view-rebuild) — the
// achievable specs turned GREEN by building the missing test-surface fixtures /
// harnesses. The two genuinely-hard rows (EG-396 cross-shard Raft 2PC, EG-397
// warm-fork) stay `#[ignore]`d below with a refined, precise reason + a north_star row.
// ─────────────────────────────────────────────────────────────────────────────

/// A request carrying an explicit caller `agent_id` so the RLS-aware read path can
/// filter to that agent's visible rows (dispatch threads `req.agent_id` → `caller`).
#[cfg(feature = "security")]
fn req_as(id: u64, agent: &str, method: Method) -> Request {
    common::signed_request_as(SECRET, id, "__commons__", agent, method)
}

/// Registers `agent_id` with the `"commons-user"` RBAC role ([`commons_isolation`]) so
/// every peer this file registers can Read+Write `__commons__` -- without it, EVERY RPC a
/// non-`System` registered peer makes against `__commons__` is `ACCESS_DENIED` under
/// `feature = "security"`'s mandatory RBAC (there is no pre-RBAC Commons-open-to-all
/// fall-through for a non-`System` identity). Harmless for a `System`-role registrant
/// (e.g. `"root"`): `check_access` short-circuits `System` identities before RBAC is ever
/// consulted, so the extra role name is simply unused.
#[cfg(feature = "security")]
fn register_identity_req(
    id: u64,
    actor: &str,
    agent_id: &str,
    role: epistemic_graph::acl::AgentRole,
) -> Request {
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

// ─────────────────────────────────────────────────────────────────────────────
// EG-391 (test 5) — RLS per-agent visibility on a fused Reason→Rank + the overlay path
// ─────────────────────────────────────────────────────────────────────────────

/// EG-391 (test 5) — RLS per-agent visibility on a fused Reason→Rank AND the overlay path.
/// GREEN (CONCEPT:EG-391 / seam CONCEPT:EG-KG.query.overlay-leg-rls-filter): with an `IsolationLayer` carrying per-agent
/// row-visibility rules, an in-txn `run_unified_overlaid` must hide rows agent B may not see
/// from BOTH the committed base AND the staged-overlay legs of a fused `Reason → Rank`, while
/// agent A (the owner) sees them.
///
/// Fixture: two COMMITTED `Robot` rows (one unowned/public, one `_owner=agent_a`+`_visibility=
/// private`) each with an embedding, seeded by the provisioned system test identity;
/// then identities `agent_a`/`agent_b` are registered through the signed admin operation.
/// A txn STAGES two more `Robot` rows (one public, one `agent_a`-private) with embeddings.
/// The fused plan `[Reason<Machine> |> Rank ~[1,0] |> Limit]` runs in-txn as each agent:
///   * agent_b sees ONLY the public rows (`pub_r`, `stg_pub`) — the committed private row is
///     dropped by `filter_view` on the base, the STAGED private row by the EG-KG.query.overlay-leg-rls-filter post-overlay
///     `filter_view` (without it the staged private row would leak);
///   * agent_a (the owner) sees ALL FOUR.
#[cfg(feature = "security")]
#[tokio::test]
async fn rls_per_agent_fused_reason_rank_overlay_eg391() {
    use epistemic_graph::isolation::AgentRole;

    let state = state().await;

    // ── Seed the committed base as the provisioned system identity. One PUBLIC
    // (unowned) row + one agent_a-owned PRIVATE row, each with an embedding so the
    // vector RANK ranks it. ──
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "pub_r".into(),
            // Default-deny RLS hides an untagged row from a non-`System` actor --
            // an EXPLICIT `_visibility: "public"` tag is required for an unowned
            // row to be visible at all (see the `stg_pub` note above).
            properties_msgpack: pack(json!({ "type": "Robot", "_visibility": "public" })),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddEmbedding {
            node_id: "pub_r".into(),
            embedding: vec![1.0, 0.0],
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::AddNode {
            node_id: "sec_r".into(),
            properties_msgpack: pack(
                json!({ "type": "Robot", "_owner": "agent_a", "_visibility": "private" }),
            ),
        },
    )
    .await;
    ok(
        &state,
        4,
        Method::AddEmbedding {
            node_id: "sec_r".into(),
            embedding: vec![1.0, 0.0],
        },
    )
    .await;

    // ── register the two agent identities → RLS enforcing mode. ──
    //
    // EG-P0-6: the provisioned system test actor registers a `System`-role `root`
    // using a detached operation signature. Root then registers `agent_a`/`agent_b`
    // as plain `Agent`-role identities for the RLS peer-isolation fixture below.
    let r = Box::pin(dispatch(
        &state,
        register_identity_req(999_000, common::TEST_AGENT, "root", AgentRole::System),
    ))
    .await;
    assert!(r.error.is_none(), "root registration failed: {:?}", r.error);

    for (i, agent) in [(5u64, "agent_a"), (6, "agent_b")] {
        let r = Box::pin(dispatch(
            &state,
            register_identity_req(i, "root", agent, AgentRole::Agent),
        ))
        .await;
        assert!(
            r.error.is_none(),
            "RegisterIdentity {agent} failed: {:?}",
            r.error
        );
    }

    // Each peer owns its own transaction. Both stage the same public and
    // agent_a-private rows so strict transaction ownership and overlay RLS are
    // exercised together without sharing a transaction as a bearer capability.
    let txn_b = stage_rls_overlay(&state, 7, "agent_b").await;
    let txn_a = stage_rls_overlay(&state, 20, "agent_a").await;

    // The fused Reason→Rank plan: infer Machine members over the (RLS-filtered) view, then
    // vector-rank them. `Reason` bridges the bare `Robot` type ↔ `<http://ex/Robot>`.
    let fused = || {
        eg_plan::Plan::new(vec![
            eg_plan::Op::Reason {
                target_class: "<http://ex/Machine>".into(),
                ontology: "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
                           <http://ex/Robot> rdfs:subClassOf <http://ex/Machine> .\n"
                    .into(),
            },
            eg_plan::Op::Rank {
                query: vec![1.0, 0.0],
            },
            eg_plan::Op::Limit { k: 10 },
        ])
    };
    let run_as = |id: u64, agent: &'static str, txn: String| {
        let state = state.clone();
        let plan = fused();
        async move {
            let r = Box::pin(dispatch(
                &state,
                req_as(id, agent, Method::TxnUnifiedQuery { txn_id: txn, plan }),
            ))
            .await;
            let mut ids = unified_ids(&r);
            ids.sort();
            ids
        }
    };

    // agent_b: ONLY the public rows — the committed private row is hidden on the base leg,
    // the STAGED private row on the overlay leg (the EG-KG.query.overlay-leg-rls-filter post-overlay filter).
    assert_eq!(
        run_as(30, "agent_b", txn_b).await,
        vec!["pub_r".to_string(), "stg_pub".to_string()],
        "agent_b must see only the public committed + staged Robot rows (RLS hides agent_a's \
         private rows on BOTH the committed and the staged-overlay legs)"
    );

    // agent_a (the owner): sees ALL FOUR rows (committed + staged, public + its own private).
    assert_eq!(
        run_as(31, "agent_a", txn_a).await,
        vec![
            "pub_r".to_string(),
            "sec_r".to_string(),
            "stg_pub".to_string(),
            "stg_sec".to_string()
        ],
        "agent_a (owner) must see every row it owns plus the public rows"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-393 (test 10) — pgwire + /sparql + native consistent snapshot after a mixed commit
// ─────────────────────────────────────────────────────────────────────────────

/// The `/sparql` SELECT/CONSTRUCT/ASK read leg's carrier credential (A18) —
/// set into [`sparql_http::SPARQL_BEARER_TOKEN_ENV`] before the listener binds
/// (it resolves its bearer/JWT config once at `serve()` startup, the SAME
/// pattern `kvcache_http` uses) and sent as `Authorization: Bearer …` by
/// [`sparql_post`] below, proving the route now authenticates a VALID
/// credential instead of having been merely relaxed.
#[cfg(feature = "sparql-http")]
const SPARQL_BEARER: &str = "advanced-crossmodal-sparql-bearer";

/// POST a SPARQL query to the `/sparql` HTTP endpoint over a raw TCP socket and return the
/// response body (SPARQL-results JSON). Dep-free — the endpoint is a hand-rolled HTTP/1.1
/// listener, so a raw request avoids pulling an HTTP client into the test. Sends the
/// [`SPARQL_BEARER`] static-secret credential the A18 read-leg fix now requires.
#[cfg(feature = "sparql-http")]
async fn sparql_post(addr: &str, query: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("sparql connect");
    let reqs = format!(
        "POST /sparql HTTP/1.1\r\nHost: localhost\r\ncontent-type: application/sparql-query\r\n\
         authorization: Bearer {SPARQL_BEARER}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        query.len(),
        query
    );
    stream
        .write_all(reqs.as_bytes())
        .await
        .expect("sparql write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("sparql read");
    let raw = String::from_utf8_lossy(&buf).to_string();
    // Split headers from body at the blank line.
    raw.split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or(raw)
}

/// EG-393 (test 10) — pgwire + `/sparql` + native consistent snapshot after a mixed commit.
/// GREEN (CONCEPT:EG-KG.query.tri-surface-snapshot-harness): a pgwire listener, a `/sparql` HTTP listener and the native
/// `dispatch` path all bound to the SAME in-process `ServerState`. After ONE mixed
/// cross-modal `BEGIN…COMMIT` (graph node + embedding + a second node + a `subClassOf`
/// graph edge + an OWL axiom), a read over EACH of the three surfaces observes the SAME
/// committed snapshot — and BEFORE the commit, none of the three sees any of it (no surface
/// sees a torn/partial state).
#[cfg(all(feature = "pgwire", feature = "sparql-http"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_sparql_native_consistent_snapshot_eg393() {
    use epistemic_graph::server::{pgwire, sparql_http};

    let state = state().await;

    // ── bind the pgwire listener on an ephemeral port with mandatory SCRAM. ──
    let pg_probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_addr = pg_probe.local_addr().unwrap().to_string();
    drop(pg_probe);
    {
        let state = state.clone();
        let pg_addr = pg_addr.clone();
        tokio::spawn(async move {
            let mode = pgwire::PgWireAuthMode::resolve(SECRET).expect("SCRAM test config");
            let _ = pgwire::serve_with_auth(&pg_addr, state, mode).await;
        });
    }

    // ── bind the /sparql listener on an ephemeral port over the SAME state. ──
    // A18: `serve()` resolves its bearer/JWT read-leg credential from the
    // environment ONCE at startup (mirroring `kvcache_http`), so the env var
    // must be set before it is spawned. CORRECTION (found while fixing the
    // `EPISTEMIC_GRAPH_ENCRYPTION_KEY` ambient-env defect elsewhere in this file):
    // the previous comment here claimed "`--test-threads=1` (this crate's
    // mandatory test-run mode)" makes this safe -- that is FALSE; this binary is
    // run with the default parallel test-threads in CI (`cargo test --test
    // advanced_crossmodal_roundtrip`, no `--test-threads=1` anywhere in this
    // repo's CI config). What actually makes this one safe today is narrower and
    // more fragile: `pgwire_sparql_native_consistent_snapshot_eg393` is the ONLY
    // test in this binary that reads OR mutates `SPARQL_BEARER_TOKEN_ENV` (grep
    // confirmed), so there is no second participant to race. That is exactly the
    // same shape of false assumption ("this is the ONLY test that touches X")
    // that made the `EPISTEMIC_GRAPH_ENCRYPTION_KEY` bug invisible — it holds only
    // as long as nobody adds a second sparql-serving test to this file without
    // also serializing it against this one (the `ENC_ENV_LOCK` pattern above is
    // the template to copy if that happens).
    std::env::set_var(sparql_http::SPARQL_BEARER_TOKEN_ENV, SPARQL_BEARER);
    let sparql_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sparql_addr = sparql_listener.local_addr().unwrap().to_string();
    {
        let state = state.clone();
        tokio::spawn(async move {
            sparql_http::serve(sparql_listener, state).await;
        });
    }
    // GOC-70: bounded-retry readiness probe instead of a fixed sleep. Only the
    // pgwire side needs this: `sparql_http::serve` is handed an ALREADY-bound
    // `TcpListener` (bound synchronously above, before spawn), so the OS
    // backlog accepts connections to `sparql_addr` as soon as `bind()`
    // returned, independent of whether the `serve` task has reached its first
    // `accept()` poll yet. `pgwire::serve_with_auth`, by contrast, does its
    // OWN internal bind inside the spawned task, so `pg_addr` genuinely isn't
    // listening until that task runs far enough — a flat sleep here assumed
    // it would within an arbitrary window, not guaranteed under scheduler
    // contention (a low-core CI runner sharing cycles across many tokio
    // workers). 1s budget (50 * 20ms), matching
    // `tests/{mysql,mssql}_roundtrip.rs`'s established pattern.
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&pg_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // The pgwire user "tester" IS the engine `agent_id` (`server::pgwire::auth`: "a pg
    // connection's `user` IS an engine `agent_id`" -- SCRAM binds the login to that
    // `AgentIdentity` for every subsequent query's ACL check). Register it up front with
    // the `commons-user` RBAC role ([`commons_isolation`]) so its post-login queries can
    // Read `__commons__` once mandatory RBAC (`feature = "security"`) is enforced --
    // without this an unregistered "tester" hits `IsolationLayer::check_access`'s
    // default-deny (`self.agents.get(agent_id)` misses) on its very first SELECT.
    {
        let mut s = state.write().await;
        s.isolation
            .register_agent(epistemic_graph::isolation::AgentIdentity {
                agent_id: "tester".to_string(),
                role: epistemic_graph::isolation::AgentRole::Agent,
                teams: Vec::new(),
                roles: vec!["commons-user".to_string()],
            });
    }

    // ── a real tokio-postgres client (extended-protocol driver) on the pgwire surface. ──
    let pg_port = pg_addr.rsplit(':').next().unwrap();
    let password = pgwire::derive_pg_password(SECRET, "tester");
    let (pg, pg_conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={pg_port} user=tester password={password} dbname=__commons__"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("pgwire connect");
    tokio::spawn(async move {
        let _ = pg_conn.await;
    });

    // Closures reading the SAME committed fact off each of the three surfaces.
    let native_robots = |id: u64| {
        let state = state.clone();
        async move { off_txn_text(&state, id, "MATCH (:Robot) |> LIMIT 5").await }
    };
    let pg_robots = || async {
        let rows = pg
            .simple_query("SELECT id FROM nodes WHERE type = 'Robot'")
            .await
            .expect("pgwire SELECT");
        rows.into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
                _ => None,
            })
            .collect::<Vec<String>>()
    };
    // The SPARQL surface reads the committed node as an RDF subject (its literal property
    // yields a `?s ?p ?o` triple). We assert the subject IRI appears / is absent in the body.
    let sparql_sees_robot = |addr: String| async move {
        sparql_post(&addr, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
            .await
            .contains("ex/robot")
    };

    // Seed ONE committed non-Robot node so the pgwire `nodes` table's schema-on-read
    // (`infer_nodes` unions the observed property keys) always exposes a `type` column —
    // otherwise `WHERE type = 'Robot'` over the empty pre-commit graph errors "no field
    // named type". `Seed` matches neither the `:Robot` label nor the `ex/robot` subject,
    // so every before-commit "sees nothing" assertion still holds. Tagged
    // `_visibility: "public"`: default-deny RLS hides an untagged, unowned row from the
    // pgwire "tester" actor (`server::pgwire::auth`: the pg user IS the ACL actor) --
    // an untagged seed would drop out of `infer_nodes`' RLS-filtered snapshot entirely,
    // reproducing the exact "no field named type" this seed exists to prevent.
    ok(
        &state,
        99,
        Method::AddNode {
            node_id: "seed0".into(),
            properties_msgpack: pack(json!({ "type": "Seed", "_visibility": "public" })),
        },
    )
    .await;

    // ── BEFORE the commit: none of the three surfaces sees the (uncommitted) data. ──
    assert!(
        native_robots(100).await.is_empty(),
        "native must see nothing before commit"
    );
    assert!(
        pg_robots().await.is_empty(),
        "pgwire must see nothing before commit"
    );
    assert!(
        !sparql_sees_robot(sparql_addr.clone()).await,
        "/sparql must see nothing before commit"
    );

    // ── ONE mixed cross-modal txn: graph node (+ literal prop) + embedding + a second node
    // + a subClassOf graph edge + an OWL axiom. IRI-shaped ids so the RDF surface is clean. ──
    let txn = begin(&state, 1, None).await;
    ok(
        &state,
        2,
        Method::TxnAddNode {
            txn_id: txn.clone(),
            node_id: "<http://ex/robot>".into(),
            // `_visibility: "public"`: the pgwire "tester" actor (a non-`System`
            // registered identity) must see this row post-commit; default-deny RLS
            // hides an untagged, unowned row from any non-`System` actor.
            properties_msgpack: pack(
                json!({ "type": "Robot", "name": "unit-1", "_visibility": "public" }),
            ),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::TxnAddEmbedding {
            txn_id: txn.clone(),
            node_id: "<http://ex/robot>".into(),
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
            node_id: "<http://ex/machine>".into(),
            properties_msgpack: pack(json!({ "type": "Machine", "_visibility": "public" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        5,
        Method::TxnAddEdge {
            txn_id: txn.clone(),
            source_id: "<http://ex/robot>".into(),
            target_id: "<http://ex/machine>".into(),
            properties_msgpack: pack(
                json!({ "relationship": "subClassOf", "type": "http://www.w3.org/2000/01/rdf-schema#subClassOf" }),
            ),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        6,
        Method::TxnAxiom {
            txn_id: txn.clone(),
            turtle: "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
                     <http://ex/Robot> rdfs:subClassOf <http://ex/Machine> .\n"
                .into(),
            graph: None,
        },
    )
    .await;
    let c = Box::pin(dispatch(
        &state,
        req(
            7,
            Method::Commit {
                txn_id: txn.clone(),
                idempotency_key: None,
            },
        ),
    ))
    .await;
    assert!(
        matches!(c.result, Some(ResultPayload::Bool(true))),
        "mixed cross-modal commit must succeed: {:?}",
        c.error
    );

    // ── AFTER the commit: all three surfaces observe the SAME committed snapshot. ──
    let native = native_robots(200).await;
    assert_eq!(
        native,
        vec!["<http://ex/robot>".to_string()],
        "native must see the committed Robot node"
    );
    let pgwire_rows = pg_robots().await;
    assert_eq!(
        pgwire_rows, native,
        "pgwire and native must agree on the committed Robot node id (one consistent snapshot)"
    );
    assert!(
        sparql_sees_robot(sparql_addr.clone()).await,
        "/sparql must observe the committed node as an RDF subject (same snapshot as pgwire+native)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-394 (test 7) — encryption-at-rest + cross-modal read; a wrong key FAILS
// ─────────────────────────────────────────────────────────────────────────────

/// Serialize the process-global `EPISTEMIC_GRAPH_ENCRYPTION_KEY` env toggle so the keyed
/// open/reopen roundtrip below never races another test reading OR mutating it.
/// **Not** this test's private lock: `state()` above (the fixture nearly every other
/// test in this binary calls) ALSO provisions this same env var before opening its own
/// `RedbBackend`, and must hold this SAME lock while doing so -- see `state()`'s doc for
/// the ambient-process-state bug that shape produced (`plan_writeback_stages_and_commits_inferred_edges_atomically_d7`
/// panicking on a low-core-count CI runner because this test's set/restore interleaved
/// with `state()`'s stale one-time `Once` outside any lock).
#[cfg(all(feature = "security", feature = "redb"))]
static ENC_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// EG-394 (test 7) — encryption-at-rest + cross-modal read; a wrong key FAILS (no silent
/// plaintext). GREEN (CONCEPT:EG-KG.storage.encryption-reopen-roundtrip): a keyed `RedbBackend` (`EPISTEMIC_GRAPH_ENCRYPTION_KEY`
/// = K1) takes a cross-modal commit (graph node + edge + a vector embedding, sealed with
/// ChaCha20-Poly1305). Reopened with K1 the fused cross-modal read (`read_graph_dump` = nodes
/// + edges + the semantic/embedding blob) DECRYPTS; reopened with a WRONG key K2 the read
/// ERRORS (`unseal` → wrong-key) rather than returning plaintext. The raw `.redb` bytes never
/// contain the plaintext secret.
#[cfg(all(feature = "security", feature = "redb"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encryption_at_rest_wrong_key_fails_eg394() {
    use epistemic_graph::crypto::ENCRYPTION_KEY_ENV;
    use epistemic_graph::durability::DurabilityPolicy;
    use epistemic_graph::server::persistence::redb_backend::RedbBackend;
    use epistemic_graph::server::persistence::PersistenceBackend;

    let _guard = ENC_ENV_LOCK.lock().await;
    let prev = std::env::var(ENCRYPTION_KEY_ENV).ok();

    let dir = std::env::temp_dir().join(format!("eg-enc-xmodal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir_s = dir.to_string_lossy().to_string();

    const SECRET_PROP: &str = "top-secret-serial-42";
    let policy = || DurabilityPolicy::Interval(std::time::Duration::from_millis(20));

    // ── K1: open a keyed backend, commit a cross-modal write (node + edge + embedding). ──
    std::env::set_var(ENCRYPTION_KEY_ENV, "key-one-K1");
    let backend = RedbBackend::open(dir_s.clone(), policy(), 64).expect("open K1");
    let methods = vec![
        Method::AddNode {
            node_id: "robot".into(),
            properties_msgpack: pack(json!({ "type": "Robot", "secret": SECRET_PROP })),
        },
        Method::AddNode {
            node_id: "machine".into(),
            properties_msgpack: pack(json!({ "type": "Machine" })),
        },
        Method::AddEdge {
            source_id: "robot".into(),
            target_id: "machine".into(),
            properties_msgpack: pack(json!({ "type": "LINKS" })),
        },
    ];
    let vectors = vec![("robot".to_string(), vec![1.0f32, 0.0])];
    backend
        .commit_crossmodal("__commons__", &methods, &vectors, &[], &[])
        .await
        .expect("cross-modal commit under K1");
    backend.shutdown();
    // `shutdown()` stops the writer thread but does not close the underlying redb
    // `Database` handle -- that only happens when the LAST owning value is dropped,
    // and redb keeps its advisory per-file lock until then. `backend` is not read
    // again below, so drop it explicitly rather than let it linger to end-of-scope
    // (same rationale as `redb_backend::tests::delete_then_recreate_same_name_keeps_new_writes`).
    drop(backend);

    // The raw redb bytes must not hold the plaintext secret (only sealed values on disk).
    let mut leaked = false;
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            if let Ok(bytes) = std::fs::read(e.path()) {
                if bytes
                    .windows(SECRET_PROP.len())
                    .any(|w| w == SECRET_PROP.as_bytes())
                {
                    leaked = true;
                }
            }
        }
    }
    assert!(
        !leaked,
        "plaintext secret must NOT appear in the raw .redb bytes"
    );

    // ── K1 reopen: the fused cross-modal read (nodes + edges + semantic) DECRYPTS. ──
    // Reopening the SAME redb file IN-PROCESS races the just-dropped `backend`'s file
    // lock actually releasing (the write-coalescer worker's async exit has no
    // `JoinHandle` here to await directly), so bound it with a short retry rather
    // than a flat sleep -- identical rationale to the redb_backend.rs reopen tests.
    let reopened = {
        let mut attempt = 0;
        loop {
            match RedbBackend::open(dir_s.clone(), policy(), 64) {
                Ok(backend) => break backend,
                Err(error) if attempt < 100 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    let _ = error;
                }
                Err(error) => panic!("reopen K1: {error:?}"),
            }
        }
    };
    let dump = reopened
        .read_graph_dump_blocking("__commons__")
        .expect("read_graph_dump under K1")
        .expect("graph present");
    let node_blob = dump
        .nodes
        .iter()
        .find(|(id, _)| id.as_str() == "robot")
        .map(|(_, b)| b.clone())
        .expect("robot node present");
    let props: serde_json::Value =
        rmp_serde::from_slice(&node_blob).expect("decode decrypted node");
    assert_eq!(
        props.get("secret").and_then(|v| v.as_str()),
        Some(SECRET_PROP),
        "the correct key must decrypt the cross-modal node property"
    );
    assert!(
        !dump.edges.is_empty(),
        "the committed edge must decrypt too"
    );
    assert!(
        !dump.semantic.is_empty(),
        "the committed embedding (semantic blob) must be present under the right key"
    );
    // A point read decrypts as well.
    assert!(
        reopened
            .read_node("__commons__", "robot")
            .await
            .unwrap()
            .is_some(),
        "point read decrypts under the correct key"
    );
    reopened.shutdown();
    drop(reopened);

    // ── K2 reopen (WRONG key): the cross-modal read must ERROR, never return plaintext. ──
    std::env::set_var(ENCRYPTION_KEY_ENV, "key-two-WRONG");
    let wrong = {
        let mut attempt = 0;
        loop {
            match RedbBackend::open(dir_s.clone(), policy(), 64) {
                Ok(backend) => break backend,
                Err(error) if attempt < 100 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    let _ = error;
                }
                Err(error) => panic!("reopen K2: {error:?}"),
            }
        }
    };
    let read = wrong.read_node("__commons__", "robot").await;
    assert!(
        read.is_err(),
        "a wrong key must FAIL the cross-modal read (no silent plaintext), got {read:?}"
    );
    wrong.shutdown();

    // Restore the env + clean up.
    match prev {
        Some(v) => std::env::set_var(ENCRYPTION_KEY_ENV, v),
        None => std::env::remove_var(ENCRYPTION_KEY_ENV),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-395 (test 8) — streaming/CDC → live materialized cross-modal view rebuild
// ─────────────────────────────────────────────────────────────────────────────

/// EG-395 (test 8) — streaming/CDC → live materialized cross-modal view rebuild. GREEN
/// (CONCEPT:EG-KG.query.cdc-live-view-rebuild): a materialized view (a `CdcHub` continuous query — the streaming-native
/// live view available in the `full` build; the `MatViewStore` variant needs the cluster-tier
/// `compute-dist`/`raft` layer, NOT in `full`) is subscribed to `state.cdc`. A cross-modal
/// write (graph nodes + a vector embedding + a graph edge) emits CDC change events; the view
/// is maintained INCREMENTALLY off that change stream (count of `Robot` nodes), and a
/// subsequent native cross-modal read reflects the SAME new state.
#[cfg(feature = "streaming")]
#[tokio::test]
async fn streaming_cdc_matview_rebuild_eg395() {
    use epistemic_graph::wire::{ContinuousAgg, ContinuousQuerySpec};

    let state = state().await;
    let hub = {
        let s = state.read().await;
        s.cdc.clone().expect("streaming build has a CdcHub")
    };

    // Subscribe a live materialized view: a continuous query counting `Robot` nodes,
    // seeded at 0 and maintained incrementally as CDC changes land.
    hub.register_query(
        "robot_count".into(),
        ContinuousQuerySpec {
            graph: "__commons__".into(),
            label: "Robot".into(),
            agg: ContinuousAgg::Count,
        },
        0.0,
    );
    let start = hub.head_seq("__commons__");

    // ── a cross-modal write: two graph nodes + a vector embedding + a graph edge. Each
    // single-op durable mutation emits a CDC change into `__commons__`'s feed. ──
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "r1".into(),
            properties_msgpack: pack(json!({ "type": "Robot" })),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddEmbedding {
            node_id: "r1".into(),
            embedding: vec![1.0, 0.0],
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::AddNode {
            node_id: "r2".into(),
            properties_msgpack: pack(json!({ "type": "Robot" })),
        },
    )
    .await;
    ok(
        &state,
        4,
        Method::AddEdge {
            source_id: "r1".into(),
            target_id: "r2".into(),
            properties_msgpack: pack(json!({ "relationship": "LINKS" })),
        },
    )
    .await;

    // The CDC subscription observes the ordered cross-modal change stream (the two node
    // AddNode events at least; the edge emits too).
    let events = hub.read("__commons__", start, 100).events;
    let robot_adds = events
        .iter()
        .filter(|e| e.label == "Robot" && format!("{:?}", e.kind).contains("AddNode"))
        .count();
    assert_eq!(
        robot_adds, 2,
        "CDC feed must carry the two Robot AddNode changes"
    );

    // The materialized view REBUILT incrementally off the change stream: it now reflects the
    // two committed Robot nodes (invalidated + re-counted on each delta, not re-run).
    let view = hub
        .read_query("robot_count")
        .expect("continuous query registered");
    assert_eq!(
        view.value, 2.0,
        "the live matview must reflect the cross-modal write"
    );
    assert!(
        view.through_seq >= start,
        "the view folded in the new changes"
    );

    // A subsequent native read reflects the SAME new state — both committed Robot nodes
    // (a plain label scan; `r2` carries no embedding, so a vector RANK would drop it).
    let mut robots = off_txn_text(&state, 5, "MATCH (:Robot) |> LIMIT 5").await;
    robots.sort();
    assert_eq!(
        robots,
        vec!["r1".to_string(), "r2".to_string()],
        "a subsequent read reflects the rebuilt cross-modal state"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// D7 (CONCEPT:EG-KG.query.plan-dag) — the planner-writeback ACID seam: a `TxnPlanWriteback`
// staged alongside an ordinary `TxnAddNode`, in ONE txn, commits atomically.
// ─────────────────────────────────────────────────────────────────────────────

/// `TxnPlanWriteback` materializes a plan's result `RowSet` as `AddEdge`s, staged in the
/// SAME txn as an ordinary `TxnAddNode` (the edges' own anchor), and both land atomically
/// on `Commit` — never the anchor node without its inferred edges or vice versa. Proves:
/// (1) staging alone does not touch the committed graph; (2) after commit, the anchor node
/// AND every inferred edge are visible together off-txn; (3) the plan itself ran against
/// the COMMITTED snapshot (the two `Entity` nodes it scans were committed BEFORE the txn
/// began, mirroring `TxnConstruct`'s "evaluated now" semantics).
#[tokio::test]
async fn plan_writeback_stages_and_commits_inferred_edges_atomically_d7() {
    let state = state().await;

    // Committed BEFORE the txn: the entities the writeback plan will scan.
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "e1".into(),
            properties_msgpack: pack(json!({ "type": "Entity" })),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddNode {
            node_id: "e2".into(),
            properties_msgpack: pack(json!({ "type": "Entity" })),
        },
    )
    .await;

    // Before ANY txn: no hub, no inferred edges.
    assert!(
        off_txn_text(&state, 3, "MATCH (:Hub) |> LIMIT 5")
            .await
            .is_empty(),
        "the hub must not exist before the txn"
    );

    let txn = begin(&state, 4, None).await;
    // The anchor node ITSELF is staged in the SAME txn as the writeback.
    ok(
        &state,
        5,
        Method::TxnAddNode {
            txn_id: txn.clone(),
            node_id: "hub".into(),
            properties_msgpack: pack(json!({ "type": "Hub" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        6,
        Method::TxnPlanWriteback {
            txn_id: txn.clone(),
            plan: eg_plan::Plan::new(vec![eg_plan::Op::Scan {
                label: "Entity".into(),
            }]),
            anchor_id: "hub".into(),
            relationship: "INFERRED_LINK".into(),
            graph: None,
        },
    )
    .await;

    // Staging alone must not touch the committed graph — no hub, no edges yet.
    assert!(
        off_txn_text(&state, 7, "MATCH (:Hub) |> LIMIT 5")
            .await
            .is_empty(),
        "staged-but-uncommitted writeback must be invisible off-txn"
    );

    let commit_resp = Box::pin(dispatch(&state, req(8, Method::Commit { txn_id: txn, idempotency_key: None }))).await;
    assert!(
        matches!(commit_resp.result, Some(ResultPayload::Bool(true))),
        "the cross-modal commit (anchor node + writeback edges) must succeed: {:?}",
        commit_resp.error
    );

    // After commit: the anchor node AND both inferred edges are visible TOGETHER —
    // atomicity (never the node without its edges, per the module docs).
    let mut reached = off_txn_text(
        &state,
        9,
        "MATCH (:Hub) |> TRAVERSE -[:INFERRED_LINK]->{1,1} |> LIMIT 5",
    )
    .await;
    reached.sort();
    assert_eq!(
        reached,
        vec!["e1".to_string(), "e2".to_string()],
        "the committed hub must reach both entities via the materialized INFERRED_LINK edges"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EXPLAIN surfaces (CONCEPT:EG-KG.query.plan-dag, E5 phase 4).
// ─────────────────────────────────────────────────────────────────────────────

/// `EXPLAIN PLAN` over a `[Scan, Rank, Filter{selective}]` plan surfaces the DAG-aware
/// optimizer's rewrite: the `after` dag pushes the selective filter ahead of the vector
/// `Rank` (the SAME decision `optimizer_never_reorders_across_a_branch_boundary`
/// unit-tests in eg-plan), and the active rule set is non-empty.
#[tokio::test]
async fn explain_plan_surfaces_the_optimizer_rewrite() {
    let state = state().await;
    for k in 0..50u32 {
        ok(
            &state,
            (k * 2 + 1) as u64,
            Method::AddNode {
                node_id: format!("d{k}"),
                properties_msgpack: pack(json!({ "type": "Doc", "year": 2000 + k as i64 })),
            },
        )
        .await;
        // Every Doc carries an embedding so the vector `Rank` has real coverage (an
        // uncovered candidate set would zero out `Rank`'s output, tripping the EG-405
        // "both orders stay non-empty" guard and blocking the reorder entirely).
        ok(
            &state,
            (k * 2 + 2) as u64,
            Method::AddEmbedding {
                node_id: format!("d{k}"),
                embedding: vec![1.0, k as f32 * 0.01],
            },
        )
        .await;
    }
    let plan = eg_plan::Plan::new(vec![
        eg_plan::Op::Scan {
            label: "Doc".into(),
        },
        eg_plan::Op::Rank {
            query: vec![1.0, 0.0],
        },
        eg_plan::Op::Filter {
            preds: vec![eg_plan::Pred::GtNum {
                prop: "year".into(),
                n: 2045.0,
            }],
        },
    ]);
    let resp = Box::pin(dispatch(&state, req(200, Method::ExplainPlan { plan }))).await;
    assert!(resp.error.is_none(), "ExplainPlan error: {:?}", resp.error);
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result: epistemic_graph::protocol::ExplainPlanResult =
        rmp_serde::from_slice(&bytes).expect("ExplainPlanResult decodes");
    assert_eq!(result.before.len(), 3, "before dag has all 3 ops");
    assert_eq!(
        result.after.len(),
        3,
        "after dag has all 3 ops (a permutation)"
    );
    assert!(
        !result.applied_rules.is_empty(),
        "the active optimizer rule set must be non-empty"
    );
    assert!(
        result.after[1].op.contains("Filter"),
        "the selective filter must be pushed ahead of Rank, got {:?}",
        result.after
    );
}

/// `EXPLAIN BELIEF` returns the FULL E1 justification tree (not the flattened
/// `Op::ExplainBelief` RowSet projection) — a claim supported by one piece of evidence.
#[cfg(feature = "epistemic")]
#[tokio::test]
async fn explain_belief_returns_full_justification_tree() {
    let state = state().await;
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "claim1".into(),
            properties_msgpack: pack(json!({ "type": "Claim", "confidence": 0.5 })),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddNode {
            node_id: "evidence1".into(),
            properties_msgpack: pack(json!({ "type": "Evidence", "confidence": 0.9 })),
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::AddEdge {
            source_id: "evidence1".into(),
            target_id: "claim1".into(),
            properties_msgpack: pack(json!({ "relationship": "SUPPORTS" })),
        },
    )
    .await;

    let resp = Box::pin(dispatch(
        &state,
        req(
            4,
            Method::ExplainBelief {
                node_id: "claim1".into(),
                disclosure_level: None,
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "ExplainBelief error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result: epistemic_graph::protocol::ExplainBeliefResult =
        rmp_serde::from_slice(&bytes).expect("ExplainBeliefResult decodes");
    assert_eq!(result.root.claim, "claim1");
    assert!(
        result.root.premises.iter().any(|p| p.claim == "evidence1"),
        "the justification tree must cite evidence1 as a premise, got {:?}",
        result.root
    );
}

/// L51 (EPI-P3-4 wiring) — `Method::ExplainBelief` with `disclosure_level` SET routes
/// through the redaction-aware path end-to-end: a `stranger` actor with no visibility
/// into a private evidence node gets a `Skeleton` proof (evidence content masked, proof
/// SHAPE preserved) via the served RPC surface, not just the crate-internal unit tests.
/// Fixture mirrors `eg_epistemic::redact`'s own: `claim <- evidence(public) <-
/// secret(private, owned by agent_a)`.
#[cfg(feature = "epistemic-redaction")]
#[tokio::test]
async fn explain_belief_disclosure_level_returns_redacted_skeleton_over_rpc() {
    use epistemic_graph::isolation::AgentRole;
    use epistemic_graph::protocol::{DisclosureLevelWire, ExplainBeliefRedactedResult};

    let state = state().await;

    // Seed the claim/evidence/secret topology as the provisioned system identity.
    // `claim1`/`evidence1` are meant PUBLIC (visible to `stranger` too, so its earned
    // level is `Skeleton` rather than `ExistenceOnly`) -- default-deny RLS
    // (`eg_core::isolation::row_visibility`) hides an untagged, unowned row from a
    // non-`System` actor just like `filter_view` would, so they need an EXPLICIT
    // `_visibility: "public"` tag (see `eg_epistemic::redact`'s own `untagged_hidden`).
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "claim1".into(),
            properties_msgpack: pack(
                json!({ "type": "Claim", "confidence": 0.5, "_visibility": "public" }),
            ),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddNode {
            node_id: "evidence1".into(),
            properties_msgpack: pack(
                json!({ "type": "Evidence", "confidence": 0.9, "_visibility": "public" }),
            ),
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::AddNode {
            node_id: "secret1".into(),
            properties_msgpack: pack(json!({
                "type": "Evidence",
                "confidence": 0.95,
                "_owner": "agent_a",
                "_visibility": "private",
            })),
        },
    )
    .await;
    ok(
        &state,
        4,
        Method::AddEdge {
            source_id: "evidence1".into(),
            target_id: "claim1".into(),
            properties_msgpack: pack(json!({ "relationship": "SUPPORTS" })),
        },
    )
    .await;
    ok(
        &state,
        5,
        Method::AddEdge {
            source_id: "secret1".into(),
            target_id: "evidence1".into(),
            properties_msgpack: pack(json!({ "relationship": "SUPPORTS" })),
        },
    )
    .await;

    // Register root as System through the signed admin operation, then have root
    // register agent_a + stranger — the same governed two-step flow EG-391 uses.
    let r = Box::pin(dispatch(
        &state,
        register_identity_req(100, common::TEST_AGENT, "root", AgentRole::System),
    ))
    .await;
    assert!(r.error.is_none(), "root registration failed: {:?}", r.error);
    for (i, agent) in [(101u64, "agent_a"), (102, "stranger")] {
        let r = Box::pin(dispatch(
            &state,
            register_identity_req(i, "root", agent, AgentRole::Agent),
        ))
        .await;
        assert!(
            r.error.is_none(),
            "RegisterIdentity {agent} failed: {:?}",
            r.error
        );
    }

    // `stranger` requests `Full` (asking for MORE than it earns) — the cap can only
    // narrow, never grant, so it must land at the actor's OWN earned `Skeleton`.
    let resp = Box::pin(dispatch(
        &state,
        req_as(
            200,
            "stranger",
            Method::ExplainBelief {
                node_id: "claim1".into(),
                disclosure_level: Some(DisclosureLevelWire::Full),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "ExplainBelief error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result: ExplainBeliefRedactedResult =
        rmp_serde::from_slice(&bytes).expect("ExplainBeliefRedactedResult decodes");

    assert_eq!(result.level, DisclosureLevelWire::Skeleton);
    let root = result
        .root
        .expect("claim1 itself is public, must render a root");
    assert_eq!(root.claim.as_deref(), Some("claim1"));
    // Shape preserved: exactly one premise (evidence1), itself with exactly one
    // premise (the redacted secret1) — the argument's structure survives redaction.
    assert_eq!(root.premises.len(), 1);
    let evidence_node = &root.premises[0];
    assert_eq!(evidence_node.claim.as_deref(), Some("evidence1"));
    assert_eq!(evidence_node.premises.len(), 1);
    let secret_node = &evidence_node.premises[0];
    assert!(
        secret_node.claim.is_none(),
        "secret1's content must be masked from stranger"
    );
    assert!(secret_node.redaction_label.is_some());
    assert!(!secret_node
        .redaction_label
        .as_ref()
        .unwrap()
        .contains("secret1"));

    // `agent_a` (the owner) earns `Full` — the SAME cap request now yields no redaction.
    let resp_owner = Box::pin(dispatch(
        &state,
        req_as(
            201,
            "agent_a",
            Method::ExplainBelief {
                node_id: "claim1".into(),
                disclosure_level: Some(DisclosureLevelWire::Full),
            },
        ),
    ))
    .await;
    let bytes = match &resp_owner.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result_owner: ExplainBeliefRedactedResult =
        rmp_serde::from_slice(&bytes).expect("ExplainBeliefRedactedResult decodes");
    assert_eq!(result_owner.level, DisclosureLevelWire::Full);
    assert!(!result_owner
        .root
        .as_ref()
        .unwrap()
        .premises
        .iter()
        .flat_map(|p| p.premises.iter())
        .any(|n| n.claim.is_none()));
}

/// L53 (EPI-P3-5 wiring) — `Method::EpistemicStatus`, the acceptance capstone, callable
/// end-to-end through the served RPC surface: belief + evidence + authority + time +
/// uncertainty + invalidation-deps all come back for one claim in ONE typed call.
#[cfg(feature = "epistemic-tms")]
#[tokio::test]
async fn epistemic_status_returns_every_facet_over_rpc() {
    use epistemic_graph::protocol::EpistemicStatusResult;

    let state = state().await;
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "claim1".into(),
            properties_msgpack: pack(json!({
                "type": "Claim",
                "confidence": 0.9,
                "valid_from": 10u64,
                "tx_from": 20u64,
            })),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddNode {
            node_id: "evidence1".into(),
            properties_msgpack: pack(json!({ "type": "Evidence", "confidence": 0.9 })),
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::AddEdge {
            source_id: "evidence1".into(),
            target_id: "claim1".into(),
            properties_msgpack: pack(json!({ "relationship": "SUPPORTS" })),
        },
    )
    .await;

    let resp = Box::pin(dispatch(
        &state,
        req(
            4,
            Method::EpistemicStatus {
                node_id: "claim1".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "EpistemicStatus error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result: EpistemicStatusResult =
        rmp_serde::from_slice(&bytes).expect("EpistemicStatusResult decodes");
    let status = result.status;

    // belief + evidence
    assert!(status.believed);
    assert!(status.confidence > 0.5);
    assert_eq!(status.evidence, vec!["evidence1".to_string()]);
    assert_eq!(status.why_not, None);
    // authority
    assert_eq!(status.authority.source_reliability, 1.0);
    assert_eq!(status.authority.attack_multiplier, 1.5);
    assert_eq!(status.authority.prior_strength, 2.0);
    // time
    assert_eq!(status.valid_time, Some((Some(10), None)));
    assert_eq!(status.tx_time, Some((Some(20), None)));
    // uncertainty + proof shape ("why")
    assert!(status.uncertainty >= 0.0);
    assert_eq!(status.proof.claim, "claim1");
    assert!(status.proof.premises.iter().any(|p| p.claim == "evidence1"));
    // invalidation deps: unattacked evidence ⇒ no counterfactual flip within the bound.
    assert_eq!(status.what_would_invalidate, None);
}

/// X-1 (CONCEPT:EG-X1) — `Method::ExplainEvidence`, over the served RPC surface: a
/// claim whose evidence node carries one complete governed page-region locus.
#[cfg(feature = "evidence-graph")]
#[tokio::test]
async fn explain_evidence_resolves_a_located_citation_over_rpc() {
    use epistemic_graph::protocol::{
        EvidenceAddressWire, EvidenceLocusWire, EvidenceResourceWire, ExplainEvidenceResult,
    };

    let state = state().await;

    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "claim1".into(),
            properties_msgpack: pack(json!({ "type": "Claim", "confidence": 0.5 })),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddNode {
            node_id: "evidence1".into(),
            properties_msgpack: pack(json!({
                "type": "Evidence",
                "confidence": 0.9,
                "evidence_locus": {
                    "id": "eg:locus:0000000000000001",
                    "subject": {
                        "kind": "occurrence",
                        "id": "eg:occurrence:0000000000000002"
                    },
                    "address": {
                        "kind": "page_region",
                        "page": 4,
                        "x": 12.0, "y": 34.0, "width": 200.0, "height": 50.0
                    },
                    "policy_ref": "eg:policy:0000000000000003",
                    "derivation_ref": "eg:derivation:0000000000000004"
                }
            })),
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::AddEdge {
            source_id: "evidence1".into(),
            target_id: "claim1".into(),
            properties_msgpack: pack(json!({ "relationship": "SUPPORTS" })),
        },
    )
    .await;

    let resp = Box::pin(dispatch(
        &state,
        req(
            4,
            Method::ExplainEvidence {
                node_id: "claim1".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "ExplainEvidence error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result: ExplainEvidenceResult =
        rmp_serde::from_slice(&bytes).expect("ExplainEvidenceResult decodes");

    assert_eq!(
        result.citations.len(),
        1,
        "exactly one located citation, got {:?}",
        result.citations
    );
    let citation = &result.citations[0];
    assert_eq!(citation.evidence_id, "evidence1");
    assert_eq!(citation.kind, "Supports");
    assert_eq!(
        citation.locus,
        EvidenceLocusWire {
            id: "eg:locus:0000000000000001".into(),
            subject: EvidenceResourceWire::Occurrence("eg:occurrence:0000000000000002".into(),),
            address: EvidenceAddressWire::PageRegion {
                page: 4,
                x: 12.0,
                y: 34.0,
                width: 200.0,
                height: 50.0,
            },
            policy_ref: "eg:policy:0000000000000003".into(),
            derivation_ref: "eg:derivation:0000000000000004".into(),
        },
        "the governed locus must round-trip over the wire byte-for-byte"
    );
}

/// L-RLS-1 (RLS_ROUTED regression proof): `Method::ExplainEvidence` must not resolve a
/// citation through an evidence node the caller cannot see. Fixture: `claim1` is
/// publicly SUPPORTED by two evidence nodes carrying an identical complete governed
/// locus — `evidence1` (public) and `secret1` (`_owner=agent_a`, `_visibility=private`)
/// — so BOTH would resolve to a citation if the snapshot were not RLS-filtered before
/// `BeliefGraph::from_graph_view`/`evidence_citations` walk it. Mirrors the identity
/// setup `explain_belief_disclosure_level_returns_redacted_skeleton_over_rpc` uses.
#[cfg(all(feature = "evidence-graph", feature = "security"))]
#[tokio::test]
async fn explain_evidence_hides_other_agents_private_evidence_over_rpc() {
    use epistemic_graph::isolation::AgentRole;
    use epistemic_graph::protocol::ExplainEvidenceResult;

    let state = state().await;
    let locus = json!({
        "id": "eg:locus:0000000000000001",
        "subject": { "kind": "occurrence", "id": "eg:occurrence:0000000000000002" },
        "address": {
            "kind": "page_region", "page": 4, "x": 12.0, "y": 34.0, "width": 200.0, "height": 50.0
        },
        "policy_ref": "eg:policy:0000000000000003",
        "derivation_ref": "eg:derivation:0000000000000004"
    });

    // `claim1`/`evidence1` are meant PUBLIC (this test's own doc comment: "claim1 is
    // publicly SUPPORTED ... evidence1 (public)") -- default-deny RLS hides an
    // untagged, unowned row from a non-`System` actor, so an EXPLICIT
    // `_visibility: "public"` tag is required (see the identical note in
    // `explain_belief_disclosure_level_returns_redacted_skeleton_over_rpc`, whose
    // setup this mirrors).
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "claim1".into(),
            properties_msgpack: pack(
                json!({ "type": "Claim", "confidence": 0.5, "_visibility": "public" }),
            ),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddNode {
            node_id: "evidence1".into(),
            properties_msgpack: pack(json!({
                "type": "Evidence",
                "confidence": 0.9,
                "_visibility": "public",
                "evidence_locus": locus,
            })),
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::AddNode {
            node_id: "secret1".into(),
            properties_msgpack: pack(json!({
                "type": "Evidence",
                "confidence": 0.95,
                "_owner": "agent_a",
                "_visibility": "private",
                "evidence_locus": locus,
            })),
        },
    )
    .await;
    ok(
        &state,
        4,
        Method::AddEdge {
            source_id: "evidence1".into(),
            target_id: "claim1".into(),
            properties_msgpack: pack(json!({ "relationship": "SUPPORTS" })),
        },
    )
    .await;
    ok(
        &state,
        5,
        Method::AddEdge {
            source_id: "secret1".into(),
            target_id: "claim1".into(),
            properties_msgpack: pack(json!({ "relationship": "SUPPORTS" })),
        },
    )
    .await;

    let r = Box::pin(dispatch(
        &state,
        register_identity_req(100, common::TEST_AGENT, "root", AgentRole::System),
    ))
    .await;
    assert!(r.error.is_none(), "root registration failed: {:?}", r.error);
    for (i, agent) in [(101u64, "agent_a"), (102, "stranger")] {
        let r = Box::pin(dispatch(
            &state,
            register_identity_req(i, "root", agent, AgentRole::Agent),
        ))
        .await;
        assert!(
            r.error.is_none(),
            "RegisterIdentity {agent} failed: {:?}",
            r.error
        );
    }

    // stranger: only the public evidence1 citation -- agent_a's private secret1 must
    // be invisible (never resolved), not merely omitted after the fact.
    let resp = Box::pin(dispatch(
        &state,
        req_as(
            200,
            "stranger",
            Method::ExplainEvidence {
                node_id: "claim1".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "ExplainEvidence error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result: ExplainEvidenceResult =
        rmp_serde::from_slice(&bytes).expect("ExplainEvidenceResult decodes");
    let ids: Vec<&str> = result
        .citations
        .iter()
        .map(|c| c.evidence_id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec!["evidence1"],
        "stranger must not see agent_a's private secret1 evidence citation, got {ids:?}"
    );

    // agent_a (the owner): sees BOTH citations.
    let resp_owner = Box::pin(dispatch(
        &state,
        req_as(
            201,
            "agent_a",
            Method::ExplainEvidence {
                node_id: "claim1".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp_owner.error.is_none(),
        "ExplainEvidence error: {:?}",
        resp_owner.error
    );
    let bytes = match &resp_owner.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result_owner: ExplainEvidenceResult =
        rmp_serde::from_slice(&bytes).expect("ExplainEvidenceResult decodes");
    let mut owner_ids: Vec<&str> = result_owner
        .citations
        .iter()
        .map(|c| c.evidence_id.as_str())
        .collect();
    owner_ids.sort();
    assert_eq!(
        owner_ids,
        vec!["evidence1", "secret1"],
        "agent_a (owner) must see both citations, got {owner_ids:?}"
    );
}

/// EPI-P3-3 — `Method::CausalEstimate`, over the served RPC surface: the SAME
/// confounded-graph fixture (`Z -> X`, `Z -> Y`, `X -> Y`) `eg_epistemic::causal`'s
/// own unit tests use, proving `do(X)` genuinely differs from a naive conditional —
/// the confounder's contribution is severed by graph surgery, not merely
/// conditioned away.
#[cfg(feature = "epistemic-causal")]
#[tokio::test]
async fn causal_estimate_do_calculus_matches_hand_derivation_over_rpc() {
    use epistemic_graph::protocol::{
        CausalEstimateResult, CausalQueryModeWire, StructuralEquationWire,
    };
    use std::collections::BTreeMap;

    let state = state().await;

    let variables = vec![
        StructuralEquationWire {
            id: "z".into(),
            parents: vec![],
            bias: 0.0,
            noise_var: 1.0,
        },
        StructuralEquationWire {
            id: "x".into(),
            parents: vec![("z".into(), 1.0)],
            bias: 0.0,
            noise_var: 0.25,
        },
        StructuralEquationWire {
            id: "y".into(),
            parents: vec![("z".into(), 1.0), ("x".into(), 0.5)],
            bias: 0.0,
            noise_var: 0.25,
        },
    ];
    let mut do_values = BTreeMap::new();
    do_values.insert("x".to_string(), 2.0);

    let resp = Box::pin(dispatch(
        &state,
        req(
            1,
            Method::CausalEstimate {
                variables,
                do_values,
                mode: CausalQueryModeWire::Intervene,
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "CausalEstimate error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result: CausalEstimateResult =
        rmp_serde::from_slice(&bytes).expect("CausalEstimateResult decodes");

    let estimates: std::collections::HashMap<String, _> = result.estimates.into_iter().collect();
    let x_est = &estimates["x"];
    let y_est = &estimates["y"];
    let z_est = &estimates["z"];

    // do(X=2) pins X exactly (zero variance).
    assert!((x_est.mean - 2.0).abs() < 1e-6);
    assert_eq!(x_est.variance, 0.0);
    // E[Y|do(X=2)] = w_xy * 2 = 1.0 exactly — the confounder Z's edge into X was cut,
    // so Z's belief (and its contribution to Y) stays at the prior mean 0.
    assert!(
        (y_est.mean - 1.0).abs() < 1e-6,
        "expected the TRUE structural effect 1.0, got {}",
        y_est.mean
    );
    assert!(
        z_est.mean.abs() < 1e-6,
        "do(X) must leave the edge-cut confounder Z's belief untouched, got {}",
        z_est.mean
    );
    // Every estimate carries a calibrated interval bracketing its mean.
    for est in [x_est, y_est, z_est] {
        assert!(est.interval.0 <= est.mean && est.mean <= est.interval.1);
    }
}

/// EPI-P3-3 — `Method::RankByProvenance`, over the served RPC surface: a
/// better-sourced, well-corroborated candidate must outrank a MERELY
/// more-similar, unsourced one — the same "provenance beats raw similarity"
/// fixture `eg_epistemic::ranking`'s own unit tests use.
#[cfg(feature = "epistemic-causal")]
#[tokio::test]
async fn rank_by_provenance_favors_corroborated_evidence_over_raw_similarity_over_rpc() {
    use epistemic_graph::protocol::{
        CalibrationWire, RankByProvenanceResult, RetrievalCandidateWire,
    };

    let state = state().await;

    let candidates = vec![
        RetrievalCandidateWire {
            id: "well-sourced".into(),
            similarity: 0.7,
            source_reliability: 0.95,
            freshness: 0.9,
            calibration: Some(CalibrationWire {
                interval: (0.85, 0.95),
                level: 0.95,
                evidence_count: 5,
            }),
        },
        RetrievalCandidateWire {
            id: "more-similar".into(),
            similarity: 0.9,
            source_reliability: 0.3,
            freshness: 0.3,
            calibration: None,
        },
    ];

    let resp = Box::pin(dispatch(
        &state,
        req(
            1,
            Method::RankByProvenance {
                candidates,
                weights: Default::default(),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "RankByProvenance error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let result: RankByProvenanceResult =
        rmp_serde::from_slice(&bytes).expect("RankByProvenanceResult decodes");

    assert_eq!(result.ranked.len(), 2);
    assert_eq!(
        result.ranked[0].id,
        "well-sourced",
        "the better-sourced/corroborated candidate should rank first despite lower \
         similarity, got order {:?}",
        result.ranked.iter().map(|r| &r.id).collect::<Vec<_>>()
    );
    assert!(result.ranked[0].score > result.ranked[1].score);
}

/// EPI-P3-7 (gap-fill) — `Method::ResolveConflict`, over the served RPC surface: the
/// SAME textbook mutual-conflict AF `eg_epistemic::tms`'s own unit tests use — `a` and
/// `b` directly ATTACK each other (symmetric conflict, no other evidence either way),
/// `c` is unattacked/unattacking. Proves the standalone op matches the crate's own
/// grounded/preferred/stable semantics: grounded leaves the conflict pair UNDECIDED
/// (paraconsistent, no explosion) while `c` survives; preferred/stable resolve the
/// conflict credulously (both `a` and `b` are "undecided" — each accepted under only
/// one of the two extensions — while `c` survives every extension).
#[cfg(feature = "epistemic-tms")]
#[tokio::test]
async fn resolve_conflict_matches_tms_crate_semantics_over_rpc() {
    use epistemic_graph::protocol::ResolveConflictResult;

    let state = state().await;
    let mut setup_id = 0u64;
    for (id, confidence) in [("a", 0.5), ("b", 0.5), ("c", 0.9)] {
        setup_id += 1;
        ok(
            &state,
            setup_id,
            Method::AddNode {
                node_id: id.into(),
                properties_msgpack: pack(json!({ "type": "Claim", "confidence": confidence })),
            },
        )
        .await;
    }
    setup_id += 1;
    ok(
        &state,
        setup_id,
        Method::AddEdge {
            source_id: "a".into(),
            target_id: "b".into(),
            properties_msgpack: pack(json!({ "relationship": "ATTACKS" })),
        },
    )
    .await;
    setup_id += 1;
    ok(
        &state,
        setup_id,
        Method::AddEdge {
            source_id: "b".into(),
            target_id: "a".into(),
            properties_msgpack: pack(json!({ "relationship": "ATTACKS" })),
        },
    )
    .await;

    let node_ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];

    // ── grounded: c survives; a/b are BOTH undecided (paraconsistent, no explosion) ──
    let resp = Box::pin(dispatch(
        &state,
        req(
            1,
            Method::ResolveConflict {
                node_ids: node_ids.clone(),
                semantics: "grounded".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "ResolveConflict error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let grounded: ResolveConflictResult =
        rmp_serde::from_slice(&bytes).expect("ResolveConflictResult decodes");
    assert_eq!(grounded.semantics, "grounded");
    assert_eq!(grounded.surviving, vec!["c".to_string()]);
    assert_eq!(
        {
            let mut u = grounded.undecided.clone();
            u.sort();
            u
        },
        vec!["a".to_string(), "b".to_string()]
    );
    assert!(grounded.defeated.is_empty());
    assert_eq!(grounded.extension_sets.len(), 1);
    assert_eq!(grounded.extension_sets[0], vec!["c".to_string()]);

    // ── preferred: two credulous extensions {a,c} and {b,c} — c survives every
    // extension, a and b are each accepted in only ONE (undecided, contested) ──
    let resp = Box::pin(dispatch(
        &state,
        req(
            2,
            Method::ResolveConflict {
                node_ids: node_ids.clone(),
                semantics: "preferred".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "ResolveConflict error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let preferred: ResolveConflictResult =
        rmp_serde::from_slice(&bytes).expect("ResolveConflictResult decodes");
    assert_eq!(preferred.surviving, vec!["c".to_string()]);
    assert_eq!(
        {
            let mut u = preferred.undecided.clone();
            u.sort();
            u
        },
        vec!["a".to_string(), "b".to_string()]
    );
    assert!(preferred.defeated.is_empty());
    assert_eq!(
        preferred.extension_sets.len(),
        2,
        "expected the two maximal admissible sets {{a,c}} and {{b,c}}, got {:?}",
        preferred.extension_sets
    );

    // ── an unknown semantics string is a clear engine error, never a panic/mis-route ──
    let resp = Box::pin(dispatch(
        &state,
        req(
            3,
            Method::ResolveConflict {
                node_ids: node_ids.clone(),
                semantics: "bogus".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_some(),
        "expected an explicit error for an unknown semantics"
    );
}

/// L-RLS-1 (RLS_ROUTED regression proof): `Method::ResolveConflict` must not classify an
/// RLS-invisible node's real conflict status, NOR let its attack edges leak into another
/// (visible) node's computed status. Fixture: the SAME textbook mutual-conflict AF
/// `resolve_conflict_matches_tms_crate_semantics_over_rpc` uses (`a` attacks `b`, `b`
/// attacks `a`, `c` uninvolved) except `b` is now `_owner=agent_a`+`_visibility=private`.
#[cfg(all(feature = "epistemic-tms", feature = "security"))]
#[tokio::test]
async fn resolve_conflict_hides_other_agents_private_argument_over_rpc() {
    use epistemic_graph::isolation::AgentRole;
    use epistemic_graph::protocol::ResolveConflictResult;

    let state = state().await;
    // `a`/`c` are meant PUBLIC (this test's own doc comment: the SAME textbook AF
    // `resolve_conflict_matches_tms_crate_semantics_over_rpc` uses, "except `b` is now"
    // private) -- default-deny RLS hides an untagged, unowned row from a non-`System`
    // actor, so they need an EXPLICIT `_visibility: "public"` tag; `b` stays private.
    ok(
        &state,
        1,
        Method::AddNode {
            node_id: "a".into(),
            properties_msgpack: pack(
                json!({ "type": "Claim", "confidence": 0.5, "_visibility": "public" }),
            ),
        },
    )
    .await;
    ok(
        &state,
        2,
        Method::AddNode {
            node_id: "b".into(),
            properties_msgpack: pack(json!({
                "type": "Claim",
                "confidence": 0.5,
                "_owner": "agent_a",
                "_visibility": "private",
            })),
        },
    )
    .await;
    ok(
        &state,
        3,
        Method::AddNode {
            node_id: "c".into(),
            properties_msgpack: pack(
                json!({ "type": "Claim", "confidence": 0.9, "_visibility": "public" }),
            ),
        },
    )
    .await;
    ok(
        &state,
        4,
        Method::AddEdge {
            source_id: "a".into(),
            target_id: "b".into(),
            properties_msgpack: pack(json!({ "relationship": "ATTACKS" })),
        },
    )
    .await;
    ok(
        &state,
        5,
        Method::AddEdge {
            source_id: "b".into(),
            target_id: "a".into(),
            properties_msgpack: pack(json!({ "relationship": "ATTACKS" })),
        },
    )
    .await;

    let r = Box::pin(dispatch(
        &state,
        register_identity_req(100, common::TEST_AGENT, "root", AgentRole::System),
    ))
    .await;
    assert!(r.error.is_none(), "root registration failed: {:?}", r.error);
    for (i, agent) in [(101u64, "agent_a"), (102, "stranger")] {
        let r = Box::pin(dispatch(
            &state,
            register_identity_req(i, "root", agent, AgentRole::Agent),
        ))
        .await;
        assert!(
            r.error.is_none(),
            "RegisterIdentity {agent} failed: {:?}",
            r.error
        );
    }

    let node_ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];

    // stranger: `b` (and both edges naming it) is invisible, so its mutual attack on
    // `a` vanishes WITH it -- `a` is now unattacked (surviving) and `b` itself reports
    // no signal (undecided), never its real defeated/surviving verdict.
    let resp = Box::pin(dispatch(
        &state,
        req_as(
            200,
            "stranger",
            Method::ResolveConflict {
                node_ids: node_ids.clone(),
                semantics: "grounded".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp.error.is_none(),
        "ResolveConflict error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let stranger_view: ResolveConflictResult =
        rmp_serde::from_slice(&bytes).expect("ResolveConflictResult decodes");
    let mut surviving = stranger_view.surviving.clone();
    surviving.sort();
    assert_eq!(
        surviving,
        vec!["a".to_string(), "c".to_string()],
        "with agent_a's private 'b' (and its attack edges) hidden, 'a' has no visible \
         attacker left and must survive, got {stranger_view:?}"
    );
    assert_eq!(
        stranger_view.undecided,
        vec!["b".to_string()],
        "an RLS-invisible node degrades to undecided (no signal), never a fabricated \
         surviving/defeated verdict, got {stranger_view:?}"
    );
    assert!(stranger_view.defeated.is_empty());

    // agent_a (the owner): sees the REAL mutual conflict -- a and b both undecided,
    // only c survives (matches the unfiltered semantics test above).
    let resp_owner = Box::pin(dispatch(
        &state,
        req_as(
            201,
            "agent_a",
            Method::ResolveConflict {
                node_ids,
                semantics: "grounded".into(),
            },
        ),
    ))
    .await;
    assert!(
        resp_owner.error.is_none(),
        "ResolveConflict error: {:?}",
        resp_owner.error
    );
    let bytes = match &resp_owner.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let owner_view: ResolveConflictResult =
        rmp_serde::from_slice(&bytes).expect("ResolveConflictResult decodes");
    assert_eq!(owner_view.surviving, vec!["c".to_string()]);
    let mut owner_undecided = owner_view.undecided.clone();
    owner_undecided.sort();
    assert_eq!(owner_undecided, vec!["a".to_string(), "b".to_string()]);
    assert!(owner_view.defeated.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Genuinely-hard rows — kept `#[ignore]`d with a REFINED precise reason (north_star OPEN).
// ─────────────────────────────────────────────────────────────────────────────

/// EG-396 (test 4) — cross-shard txn spanning modalities under Raft; kill the coordinator
/// mid-2PC → a SINGLE decision (all shards commit or all abort — no split-brain).
///
/// GREEN under `--features cluster` (CONCEPT:EG-KG.txn.crossshard-2pc-modality-harness):
/// the `CrossShardCoordinator` + `multi_raft` live behind `cluster`/`raft`+`compute-dist`,
/// NOT in `full` — so this spec is `cfg`-gated to `cluster` rather than `#[ignore]`d
/// (under `--features full` it simply compiles out; under `--features cluster` it runs).
/// The heavy in-process multi-group 2PC coordinator-kill proof lives in the crate at
/// `raft::xshard_modality_harness`; this spec drives its public entry end-to-end. It
/// spins up a live two-group cluster, runs a cross-shard txn spanning the property-graph
/// modality (group A) and the RDF/triple modality (group B), kills the coordinator at
/// BOTH 2PC windows (post-COMMIT-decision and pre-decision) via a full node+backend drop,
/// and asserts recovery resolves to a SINGLE all-or-nothing decision — no half-committed
/// modality. Real multi-HOST cross-node soak + ANN/TSDB cross-shard modalities are the
/// documented remainder (see the harness module docs).
#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_shard_raft_2pc_single_decision_eg396() {
    let report =
        epistemic_graph::raft::xshard_modality_harness::prove_crossshard_modality_2pc_single_decision()
            .await
            .expect("cross-shard modality 2PC coordinator-kill proof runs");
    assert!(
        report.all_atomic(),
        "cross-shard 2PC must resolve to a single all-or-nothing decision across modalities: {report:?}"
    );
}

/// EG-397 (test 11) — KV-cache warm-fork fan-out reusing cross-modal context, isolated on
/// divergent writes.
#[tokio::test]
#[ignore = "CROSS-REPO GAP (CONCEPT:EG-KG.query.warmfork-fanout-open-reason): the warm-FORK sandbox primitive (ForkableSandbox / \
            WarmParentRegistry / forkserver, ORCH-1.86..93) lives in agent-utilities, NOT this \
            engine. Reachable engine-side seams are the KV page store (eg-kvcache: dedup/LRU/\
            data-version EG-364) and the cross-modal read surface this file already proves — but \
            NEITHER is a fork primitive: there is no in-engine `os.fork`/forkserver/CoW child that \
            shares a warm parent's read context while isolating divergent writes. The fan-out test \
            belongs to the agent-utilities warm-fork surface CONSUMING this engine's cross-modal \
            context (the child would open its own client/txn against the same engine); it cannot \
            be written green inside epistemic-graph. Tracked in docs/north_star.md."]
async fn kvcache_warm_fork_fanout_eg397() {
    // Contract asserted above; the warm-fork primitive lives in agent-utilities, not here.
}
