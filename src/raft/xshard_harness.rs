//! Cross-shard 2PC atomicity + recovery gauntlet (CONCEPT:EG-KG.storage.lane-n-increment).
//!
//! The nemesis harness for the cross-shard distributed transaction: it spins a live
//! two-group cluster (one in-process node, two Raft groups on the shared listener,
//! one shared `graph.redb`) — the SAME machinery the single-group failover test uses
//! — and proves the atomicity invariant under fault injection:
//!
//!   * **No partial commit.** A cross-shard txn either commits on EVERY participant
//!     group or on NONE. An acked commit is durable on all participants; an aborted
//!     one leaves none.
//!   * **Recovery resolves in-doubt txns deterministically.** A crash AFTER the
//!     coordinator logged COMMIT re-applies on restart; a crash BEFORE any decision
//!     (presumed-abort) applies nowhere.
//!
//! The faults injected: (1) a participant whose OCC slice cannot prepare (a NO vote /
//! the partition-equivalent of an unreachable participant — a closed group); (2) a
//! coordinator crash BETWEEN prepare and the durable decision; (3) a coordinator
//! crash AFTER the COMMIT decision but BEFORE phase-2 apply. Each asserts the
//! invariant on the durable state, not on timing.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::sync::RwLock;

use super::cross_shard_txn::{CrossShardCoordinator, CrossShardTxn, GraphSlice, TxnOutcome};
use super::multi::MultiRaft;
use super::{AppCtx, NodeId};
use crate::channels::ChannelManager;
use crate::isolation::IsolationLayer;
use crate::protocol::{GraphType, Method};
use crate::registry::GraphRegistry;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;
use crate::wal_service::FsyncPolicy;

const GROUP_A: u64 = 100;
const GROUP_B: u64 = 200;
const GRAPH_A: &str = "shardA";
const GRAPH_B: &str = "shardB";

/// Build a redb-AUTHORITATIVE ServerState over an already-open backend (redb is
/// single-handle-per-process, so a test that needs the concrete handle must SHARE it
/// with the state, never open a second over the same dir).
async fn make_state(dir: &str, backend: Arc<dyn PersistenceBackend>) -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            crate::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "xshard-test".to_string(),
        persist_dir: Some(dir.to_string()),
        persistence: Some(backend),
        redb_authoritative: true,
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(64)),
        read_admission: Arc::new(tokio::sync::Semaphore::new(64)),
        per_graph_inflight: Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit: 16,
        write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
        open_txns: Arc::new(dashmap::DashMap::new()),
        txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "rdf-redb")]
        rdf_quads: None,
        #[cfg(feature = "streaming")]
        cdc: None,
        #[cfg(feature = "wasm-udf")]
        udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: std::sync::Arc::new(parking_lot::Mutex::new(
            crate::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
        #[cfg(feature = "kv")]
        kv: None,
    }))
}

fn fresh_dir(tag: &str) -> String {
    let d = std::env::temp_dir().join(format!("eg-xshard-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d.to_string_lossy().to_string()
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Bring up a one-node, two-group cluster over `dir`'s redb, with `shardA`→100 and
/// `shardB`→200 assigned in the router. Returns the manager + the coordinator + the
/// state (so the test can read graph data back and stop the listener).
async fn bring_up(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
) -> (
    Arc<MultiRaft>,
    CrossShardCoordinator,
    Arc<RwLock<ServerState>>,
) {
    let state = make_state(dir, backend.clone()).await;
    let ctx = AppCtx {
        state: state.clone(),
        router: None,
    };
    let port = free_port();
    let node_id: NodeId = 1;
    let peers: BTreeMap<NodeId, BasicNode> =
        [(node_id, BasicNode::new(format!("127.0.0.1:{port}")))].into();

    let multi = MultiRaft::start(node_id, format!("127.0.0.1:{port}"), backend.clone(), ctx)
        .await
        .expect("start multi");
    multi
        .create_group(GROUP_A, peers.clone(), true)
        .await
        .unwrap();
    multi
        .create_group(GROUP_B, peers.clone(), true)
        .await
        .unwrap();
    multi.router().assign(GRAPH_A, GROUP_A);
    multi.router().assign(GRAPH_B, GROUP_B);

    // Each group elects its (single-node) leader before we route writes.
    for gid in [GROUP_A, GROUP_B] {
        let g = multi.group(gid).await.expect("group exists");
        wait_until(Duration::from_secs(15), || {
            let g = g.clone();
            async move { g.current_leader().await == Some(node_id) }
        })
        .await
        .unwrap_or_else(|_| panic!("group {gid} must elect a leader"));
    }

    let coord = CrossShardCoordinator::new(multi.clone(), backend);
    (multi, coord, state)
}

/// Count nodes in a named graph on a state.
async fn node_count(state: &Arc<RwLock<ServerState>>, graph: &str) -> usize {
    let s = state.read().await;
    s.registry
        .get(graph)
        .map(|e| e.core.node_count())
        .unwrap_or(0)
}

/// A two-graph cross-shard txn inserting `a_node` into shardA and `b_node` into shardB.
fn two_shard_txn(txn_id: &str, a_node: &str, b_node: &str) -> CrossShardTxn {
    let slice = |graph: &str, node: &str| GraphSlice {
        graph_name: graph.to_string(),
        graph_fname: crate::persist::sanitize(graph),
        graph_type: GraphType::Global,
        methods: vec![Method::AddNode {
            node_id: node.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"n": node})).unwrap(),
        }],
    };
    CrossShardTxn {
        txn_id: txn_id.to_string(),
        slices: vec![slice(GRAPH_A, a_node), slice(GRAPH_B, b_node)],
    }
}

/// A two-graph cross-shard txn where shardA WRITES a node and shardB is READ-ONLY
/// (its slice carries no methods) — the EG-081 read-only-participant fast-path shape.
fn writer_plus_readonly_txn(txn_id: &str, a_node: &str) -> CrossShardTxn {
    let writer = GraphSlice {
        graph_name: GRAPH_A.to_string(),
        graph_fname: crate::persist::sanitize(GRAPH_A),
        graph_type: GraphType::Global,
        methods: vec![Method::AddNode {
            node_id: a_node.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"n": a_node})).unwrap(),
        }],
    };
    let read_only = GraphSlice {
        graph_name: GRAPH_B.to_string(),
        graph_fname: crate::persist::sanitize(GRAPH_B),
        graph_type: GraphType::Global,
        methods: vec![], // EMPTY write-set → read-only participant.
    };
    CrossShardTxn {
        txn_id: txn_id.to_string(),
        slices: vec![writer, read_only],
    }
}

async fn wait_until<F, Fut>(timeout: Duration, mut pred: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if pred().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    Err(())
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Span detection — the FAST PATH gate (single-group stays single-group).
// ─────────────────────────────────────────────────────────────────────────

/// A txn over graphs in ONE group is NOT cross-shard (single-group fast path);
/// a txn spanning two groups IS. This is exactly the gate `Commit` checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn span_detection_routes_single_group_to_fast_path() {
    let dir = fresh_dir("span");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, _state) = bring_up(&dir, backend.clone()).await;

    let router = multi.router();
    // Two graphs in the SAME group → single-group, fast path, NOT cross-shard.
    assert!(!router.is_cross_shard([GRAPH_A]));
    router.assign("alsoA", GROUP_A);
    assert!(!router.is_cross_shard([GRAPH_A, "alsoA"]));
    assert_eq!(router.span([GRAPH_A, "alsoA"]).len(), 1);
    // Graphs that span two groups → cross-shard, must use 2PC.
    assert!(router.is_cross_shard([GRAPH_A, GRAPH_B]));
    assert_eq!(router.span([GRAPH_A, GRAPH_B]).len(), 2);

    // A single-group "cross-shard" call is rejected (use the single-group path).
    let one_group = CrossShardTxn {
        txn_id: "t-one".into(),
        slices: vec![two_shard_txn("x", "a", "b").slices[0].clone()],
    };
    assert!(coord.commit_cross_shard(&one_group).await.is_err());

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 2. Happy path — a cross-shard commit lands on BOTH participants atomically.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_commit_is_atomic_on_all_participants() {
    let dir = fresh_dir("happy");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;

    let txn = two_shard_txn("t-happy", "a1", "b1");
    let outcome = coord.commit_cross_shard(&txn).await.expect("commit");
    assert_eq!(outcome, TxnOutcome::Committed);

    // BOTH graphs have their node — an all-or-nothing commit landed on all.
    assert_eq!(node_count(&state, GRAPH_A).await, 1, "shardA committed");
    assert_eq!(node_count(&state, GRAPH_B).await, 1, "shardB committed");

    // The durable 2PC records are CLEARED after a resolved commit (no leak).
    let redb = backend.as_redb().unwrap();
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "prepares cleared"
    );
    assert_eq!(
        redb.xshard_decision_get("t-happy").unwrap(),
        None,
        "decision cleared"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 3. Participant kill / partition during PREPARE → NO PARTIAL COMMIT (abort).
// ─────────────────────────────────────────────────────────────────────────

/// Nemesis: kill a participant (close its group) so it cannot prepare. The txn must
/// ABORT — and crucially the OTHER (live) participant must NOT have applied. No
/// partial commit. (A closed group is the in-process analog of a network partition:
/// the participant is unreachable to the coordinator.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_participant_during_prepare_aborts_with_no_partial_commit() {
    let dir = fresh_dir("killprep");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;

    // KILL participant B (close group 200) — it is now unreachable to prepare.
    multi.close_group(GROUP_B).await.unwrap();
    assert!(multi.group(GROUP_B).await.is_none(), "B is killed");

    let txn = two_shard_txn("t-killed", "a2", "b2");
    let outcome = coord
        .commit_cross_shard(&txn)
        .await
        .expect("commit returns");
    assert_eq!(
        outcome,
        TxnOutcome::Aborted,
        "a killed participant aborts the txn"
    );

    // NO PARTIAL COMMIT: the LIVE participant A must NOT have applied its slice.
    assert_eq!(
        node_count(&state, GRAPH_A).await,
        0,
        "no partial commit on A"
    );
    assert_eq!(node_count(&state, GRAPH_B).await, 0, "B never applied");

    // The decision is durably ABORT and prepares are cleared — clean abort.
    let redb = backend.as_redb().unwrap();
    assert_eq!(
        redb.xshard_decision_get("t-killed").unwrap(),
        None,
        "decision cleared"
    );
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "no leaked prepares"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 4. Recovery — crash AFTER a COMMIT decision re-applies every slice on restart.
// ─────────────────────────────────────────────────────────────────────────

/// Nemesis: drive PHASE 1 (durable prepares) + log a COMMIT decision, then CRASH
/// before phase-2 apply (drop the whole node + backend, leaving only the durable
/// redb records — exactly what `kill -9` leaves, since every record was fsynced).
/// On restart, recovery reads the COMMIT decision and re-applies → BOTH graphs land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_commits_in_doubt_txn_after_crash_post_decision() {
    let dir = fresh_dir("recovercommit");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let txn_id = "t-recover-commit";
    {
        let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
        let txn = two_shard_txn(txn_id, "a3", "b3");
        // PHASE 1: prepare both (durable), then log COMMIT — but do NOT apply.
        assert!(coord.prepare_only(&txn).await.expect("prepare"));
        coord.decide_only(txn_id, true).await.expect("decide");
        // Nothing applied yet: both graphs are still empty.
        assert_eq!(node_count(&state, GRAPH_A).await, 0);
        assert_eq!(node_count(&state, GRAPH_B).await, 0);
        // CRASH: stop the listener + drop the groups (the node is gone). Durable
        // prepare+decision records remain on disk.
        multi.stop_listener();
        multi.close_group(GROUP_A).await.unwrap();
        multi.close_group(GROUP_B).await.unwrap();
    }
    // Simulate a process restart: drop ALL handles to the backend so its writer
    // thread + file lock release, then reopen a brand-new backend over the SAME files.
    backend.shutdown();
    drop(backend);

    let backend2: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("reopen redb"));
    let (multi2, coord2, state2) = bring_up(&dir, backend2.clone()).await;

    // RECOVERY: the in-doubt txn's decision is COMMIT → re-apply both slices.
    let resolved = coord2.recover_in_doubt().await.expect("recover");
    assert_eq!(resolved, 1, "exactly one in-doubt txn resolved");
    assert_eq!(
        node_count(&state2, GRAPH_A).await,
        1,
        "recovery committed A"
    );
    assert_eq!(
        node_count(&state2, GRAPH_B).await,
        1,
        "recovery committed B"
    );

    // Records cleared after recovery resolved the txn.
    let redb = backend2.as_redb().unwrap();
    assert!(redb.xshard_scan_prepares().unwrap().is_empty());
    assert_eq!(redb.xshard_decision_get(txn_id).unwrap(), None);

    multi2.stop_listener();
    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 5. Recovery — crash BEFORE any decision (presumed-abort) applies NOWHERE.
// ─────────────────────────────────────────────────────────────────────────

/// Nemesis: drive PHASE 1 (durable prepares) but CRASH before the coordinator logs
/// ANY decision. On restart, recovery finds prepares with no decision → presumed
/// ABORT → applies nowhere (correct: no participant could have applied, since apply
/// only happens after a durable decision).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_aborts_in_doubt_txn_with_no_decision_record() {
    let dir = fresh_dir("recoverabort");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let txn_id = "t-recover-abort";
    {
        let (multi, coord, _state) = bring_up(&dir, backend.clone()).await;
        let txn = two_shard_txn(txn_id, "a4", "b4");
        // PHASE 1 only — prepares are durable, but NO decision is ever logged.
        assert!(coord.prepare_only(&txn).await.expect("prepare"));
        // Sanity: the prepares ARE on disk (the in-doubt state we recover from).
        let redb = backend.as_redb().unwrap();
        assert_eq!(
            redb.xshard_scan_prepares().unwrap().len(),
            2,
            "two prepares durable"
        );
        assert_eq!(
            redb.xshard_decision_get(txn_id).unwrap(),
            None,
            "no decision logged"
        );
        multi.stop_listener();
        multi.close_group(GROUP_A).await.unwrap();
        multi.close_group(GROUP_B).await.unwrap();
    }
    backend.shutdown();
    drop(backend);

    let backend2: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("reopen redb"));
    let (multi2, coord2, state2) = bring_up(&dir, backend2.clone()).await;

    let resolved = coord2.recover_in_doubt().await.expect("recover");
    assert_eq!(resolved, 1, "the in-doubt txn is resolved (as abort)");
    // Presumed-abort: applied NOWHERE — no partial commit from an undecided crash.
    assert_eq!(node_count(&state2, GRAPH_A).await, 0, "no apply on A");
    assert_eq!(node_count(&state2, GRAPH_B).await, 0, "no apply on B");

    let redb = backend2.as_redb().unwrap();
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "prepares cleared on abort"
    );

    multi2.stop_listener();
    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 6. Lane N WIRE (CONCEPT:EG-KG.txn.routes-cross-shard-txn) — a USER multi-graph txn across 2 groups,
//    driven through the BeginTxn→stage→Commit HANDLER, is atomic.
// ─────────────────────────────────────────────────────────────────────────

use crate::protocol::{Response, ResultPayload};
use crate::server::handlers::txn::try_handle as txn_handle;

/// Register `shardA`/`shardB` in the registry + wire `state.multi_raft` so the
/// user-facing commit path can resolve the cross-shard span (CONCEPT:EG-KG.txn.routes-cross-shard-txn).
async fn wire_user_graphs(state: &Arc<RwLock<ServerState>>, multi: &Arc<MultiRaft>) {
    let mut s = state.write().await;
    let _ = s.registry.create_graph(GRAPH_A, GraphType::Global, None);
    let _ = s.registry.create_graph(GRAPH_B, GraphType::Global, None);
    s.multi_raft = Some(multi.clone());
}

/// Unwrap a handler `Response` to its `Bool` payload (the txn ack), or panic.
fn as_bool(r: Response) -> bool {
    match r.result {
        Some(ResultPayload::Bool(b)) => b,
        other => panic!("expected Bool payload, got {other:?} (error={:?})", r.error),
    }
}

/// Drive BeginTxn (default=shardA) → TxnAddNode(shardA) → TxnAddNode(graph=shardB)
/// through the HANDLER. Returns the txn id so the caller can Commit it.
async fn begin_two_graph_txn(
    state: &Arc<RwLock<ServerState>>,
    a_node: &str,
    b_node: &str,
) -> String {
    let begin = txn_handle(
        state,
        1,
        None,
        Method::BeginTxn {
            graph: Some(GRAPH_A.to_string()),
            isolation: None,
        },
    )
    .await
    .expect("BeginTxn is a txn method");
    let txn_id = match begin.result {
        Some(ResultPayload::String(id)) => id,
        other => panic!("BeginTxn must return a txn id, got {other:?}"),
    };
    let mk = |node: &str| rmp_serde::to_vec_named(&serde_json::json!({ "n": node })).unwrap();
    // Op 1: default graph (shardA).
    assert!(as_bool(
        txn_handle(
            state,
            2,
            None,
            Method::TxnAddNode {
                txn_id: txn_id.clone(),
                node_id: a_node.to_string(),
                properties_msgpack: mk(a_node),
                graph: None,
            },
        )
        .await
        .unwrap()
    ));
    // Op 2: a DIFFERENT graph (shardB) — makes the txn cross-shard.
    assert!(as_bool(
        txn_handle(
            state,
            3,
            None,
            Method::TxnAddNode {
                txn_id: txn_id.clone(),
                node_id: b_node.to_string(),
                properties_msgpack: mk(b_node),
                graph: Some(GRAPH_B.to_string()),
            },
        )
        .await
        .unwrap()
    ));
    txn_id
}

/// HAPPY: a user multi-graph txn across 2 groups commits atomically on BOTH (the
/// staged multi-graph write-set routed through the 2PC coordinator).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_multigraph_txn_commits_atomically_across_groups() {
    let dir = fresh_dir("userhappy");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, _coord, state) = bring_up(&dir, backend.clone()).await;
    wire_user_graphs(&state, &multi).await;

    let txn_id = begin_two_graph_txn(&state, "ua1", "ub1").await;
    let committed = as_bool(
        txn_handle(&state, 4, None, Method::Commit { txn_id })
            .await
            .unwrap(),
    );
    assert!(committed, "user cross-shard txn commits");

    // BOTH graphs got their node — all-or-nothing landed everywhere.
    assert_eq!(node_count(&state, GRAPH_A).await, 1, "shardA committed");
    assert_eq!(node_count(&state, GRAPH_B).await, 1, "shardB committed");
    // Durable 2PC records cleared (no leak) after a resolved commit.
    let redb = backend.as_redb().unwrap();
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "prepares cleared"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// NEMESIS: kill participant B (close its group) so its slice cannot prepare, then
/// commit a USER multi-graph txn through the handler. It must ABORT with NO PARTIAL
/// COMMIT — the live participant A must NOT have applied. This proves the user wire
/// inherits the coordinator's atomicity under a participant kill (CONCEPT:EG-KG.txn.routes-cross-shard-txn).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_multigraph_txn_atomic_under_participant_kill() {
    let dir = fresh_dir("userkill");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, _coord, state) = bring_up(&dir, backend.clone()).await;
    wire_user_graphs(&state, &multi).await;

    // Stage the multi-graph txn first (both graphs resident), THEN kill participant B.
    let txn_id = begin_two_graph_txn(&state, "ua2", "ub2").await;
    multi.close_group(GROUP_B).await.unwrap();
    assert!(multi.group(GROUP_B).await.is_none(), "B is killed");

    let committed = as_bool(
        txn_handle(&state, 4, None, Method::Commit { txn_id })
            .await
            .unwrap(),
    );
    assert!(!committed, "a killed participant aborts the user txn");

    // NO PARTIAL COMMIT: neither graph applied (the live A must NOT have its node).
    assert_eq!(
        node_count(&state, GRAPH_A).await,
        0,
        "no partial commit on A"
    );
    assert_eq!(node_count(&state, GRAPH_B).await, 0, "B never applied");
    // Clean abort: no leaked prepares.
    let redb = backend.as_redb().unwrap();
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "no leaked prepares"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 7. EG-081 read-only fast path — a participant with an EMPTY write-set slice
//    is validated but SKIPS its prepare-log + phase-2 write; the writer still
//    commits atomically and no durable 2PC record leaks.
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_participant_skips_prepare_and_phase2() {
    let dir = fresh_dir("readonly");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;

    // shardA writes, shardB is read-only (empty slice). Still a 2-group span → the
    // cross-shard path runs, but only shardA enters the durable 2PC protocol.
    let txn = writer_plus_readonly_txn("t-ro", "ro1");
    let outcome = coord.commit_cross_shard(&txn).await.expect("commit");
    assert_eq!(outcome, TxnOutcome::Committed);

    // The writer landed; the read-only participant applied NOTHING.
    assert_eq!(node_count(&state, GRAPH_A).await, 1, "writer committed");
    assert_eq!(
        node_count(&state, GRAPH_B).await,
        0,
        "read-only applied nothing"
    );

    // OBSERVABLE: exactly one group took the read-only fast path.
    assert_eq!(
        coord.readonly_skipped(),
        1,
        "shardB skipped the durable prepare via the read-only fast path"
    );

    // The read-only group (200) NEVER wrote a durable prepare record; the writer's
    // records are cleared after the resolved commit → no 2PC state leaks anywhere.
    let redb = backend.as_redb().unwrap();
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "no prepare records (writer cleared, read-only never wrote one)"
    );
    assert_eq!(
        redb.xshard_decision_get("t-ro").unwrap(),
        None,
        "decision cleared"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 8. EG-081 parallel prepare — a 3-WRITER cross-shard txn prepared CONCURRENTLY
//    reaches the SAME atomic decision as the sequential protocol: it commits on
//    every writing participant, and recovery re-applies after a post-decision
//    crash. Proves join_all with >2 futures preserves correctness + recovery.
// ─────────────────────────────────────────────────────────────────────────

const GROUP_C: u64 = 300;
const GRAPH_C: &str = "shardC";

/// Add a THIRD single-member group (shardC→300) to an already-running node and wait
/// for it to elect its leader (uses `ensure_group`, the self-bootstrap seam).
async fn add_third_group(multi: &Arc<MultiRaft>) {
    multi.ensure_group(GROUP_C).await.expect("ensure group C");
    multi.router().assign(GRAPH_C, GROUP_C);
    let g = multi.group(GROUP_C).await.expect("group C exists");
    wait_until(Duration::from_secs(15), || {
        let g = g.clone();
        async move { g.current_leader().await == Some(1u64) }
    })
    .await
    .expect("group C must elect a leader");
}

/// A three-graph cross-shard txn inserting one node into each of shardA/B/C — three
/// WRITING participants, so PHASE 1 joins three prepare futures.
fn three_writer_txn(txn_id: &str, a: &str, b: &str, c: &str) -> CrossShardTxn {
    let slice = |graph: &str, node: &str| GraphSlice {
        graph_name: graph.to_string(),
        graph_fname: crate::persist::sanitize(graph),
        graph_type: GraphType::Global,
        methods: vec![Method::AddNode {
            node_id: node.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"n": node})).unwrap(),
        }],
    };
    CrossShardTxn {
        txn_id: txn_id.to_string(),
        slices: vec![slice(GRAPH_A, a), slice(GRAPH_B, b), slice(GRAPH_C, c)],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_prepare_multi_writer_commits_atomically() {
    let dir = fresh_dir("parcommit");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
    add_third_group(&multi).await;

    // Three writing participants prepared CONCURRENTLY → one atomic COMMIT decision.
    let txn = three_writer_txn("t-par", "pa", "pb", "pc");
    let outcome = coord.commit_cross_shard(&txn).await.expect("commit");
    assert_eq!(outcome, TxnOutcome::Committed);

    // The parallel-prepared txn landed on ALL THREE participants (all-or-nothing).
    assert_eq!(node_count(&state, GRAPH_A).await, 1, "A committed");
    assert_eq!(node_count(&state, GRAPH_B).await, 1, "B committed");
    assert_eq!(node_count(&state, GRAPH_C).await, 1, "C committed");
    assert_eq!(
        coord.readonly_skipped(),
        0,
        "no read-only participants here"
    );

    // Durable records cleared after a resolved commit — same end-state as sequential.
    let redb = backend.as_redb().unwrap();
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "prepares cleared"
    );
    assert_eq!(
        redb.xshard_decision_get("t-par").unwrap(),
        None,
        "decision cleared"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_prepare_multi_writer_recovers_after_post_decision_crash() {
    let dir = fresh_dir("parrecover");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let txn_id = "t-par-recover";
    {
        let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
        add_third_group(&multi).await;
        let txn = three_writer_txn(txn_id, "ra", "rb", "rc");
        // PHASE 1 (durable prepares) + log COMMIT, but crash before phase-2 apply.
        assert!(coord.prepare_only(&txn).await.expect("prepare"));
        coord.decide_only(txn_id, true).await.expect("decide");
        assert_eq!(node_count(&state, GRAPH_A).await, 0);
        assert_eq!(node_count(&state, GRAPH_B).await, 0);
        assert_eq!(node_count(&state, GRAPH_C).await, 0);
        multi.stop_listener();
        multi.close_group(GROUP_A).await.unwrap();
        multi.close_group(GROUP_B).await.unwrap();
        multi.close_group(GROUP_C).await.unwrap();
    }
    backend.shutdown();
    drop(backend);

    let backend2: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("reopen redb"));
    let (multi2, coord2, state2) = bring_up(&dir, backend2.clone()).await;
    add_third_group(&multi2).await;

    // RECOVERY resolves the in-doubt multi-writer txn from its COMMIT decision.
    let resolved = coord2.recover_in_doubt().await.expect("recover");
    assert_eq!(resolved, 1, "one in-doubt txn resolved");
    assert_eq!(
        node_count(&state2, GRAPH_A).await,
        1,
        "recovery committed A"
    );
    assert_eq!(
        node_count(&state2, GRAPH_B).await,
        1,
        "recovery committed B"
    );
    assert_eq!(
        node_count(&state2, GRAPH_C).await,
        1,
        "recovery committed C"
    );

    let redb = backend2.as_redb().unwrap();
    assert!(redb.xshard_scan_prepares().unwrap().is_empty());
    assert_eq!(redb.xshard_decision_get(txn_id).unwrap(), None);

    multi2.stop_listener();
    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────
// 9. EG-082 NON-BLOCKING commit (Paxos-Commit-lite) — the decision is
//    REPLICATED through a Raft group, not held in the coordinator's private
//    redb, so a coordinator crash between decision and apply does NOT block:
//    a different resolver learns the decision from the replicated log and
//    finishes the txn. Proves (a) atomicity, (b) the removed blocking window,
//    (c) agreement with 2PC on commit/abort for the same inputs.
// ─────────────────────────────────────────────────────────────────────────

/// The dedicated cross-shard DECISION Raft group (CONCEPT:EG-KG.txn.harness-crash): the commit/abort
/// record is Raft-replicated into a graph here instead of the coordinator's redb.
const GROUP_D: u64 = 400;

/// Add the decision group to a running node and wait for it to elect its leader.
async fn add_decision_group(multi: &Arc<MultiRaft>) {
    multi
        .ensure_group(GROUP_D)
        .await
        .expect("ensure decision group D");
    let g = multi.group(GROUP_D).await.expect("decision group D exists");
    wait_until(Duration::from_secs(15), || {
        let g = g.clone();
        async move { g.current_leader().await == Some(1u64) }
    })
    .await
    .expect("decision group D must elect a leader");
}

/// (a) ATOMICITY + the replicated-decision mechanic: a non-blocking cross-shard commit
/// lands on BOTH participants, the decision is replicated (NOT in coordinator redb), and
/// the replicated decision node is GC'd after resolution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_commit_is_atomic_via_replicated_decision() {
    let dir = fresh_dir("nbhappy");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
    add_decision_group(&multi).await;

    let txn = two_shard_txn("t-nb-happy", "na1", "nb1");
    let outcome = coord
        .commit_cross_shard_nonblocking(&txn, GROUP_D)
        .await
        .expect("commit");
    assert_eq!(outcome, TxnOutcome::Committed);

    // BOTH graphs committed — all-or-nothing landed everywhere.
    assert_eq!(node_count(&state, GRAPH_A).await, 1, "shardA committed");
    assert_eq!(node_count(&state, GRAPH_B).await, 1, "shardB committed");

    let redb = backend.as_redb().unwrap();
    // THE DISTINGUISHING INVARIANT: the non-blocking path NEVER writes the
    // coordinator-private redb decision record — the decision lived only in the
    // replicated log.
    assert_eq!(
        redb.xshard_decision_get("t-nb-happy").unwrap(),
        None,
        "non-blocking commit never writes the coordinator-private redb decision"
    );
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "prepares cleared"
    );
    // The replicated decision node is GC'd after the txn resolved.
    assert_eq!(
        coord.learn_decision("t-nb-happy").await.unwrap(),
        None,
        "replicated decision GC'd after commit"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// (b) THE NON-BLOCKING WIN: replicate a COMMIT decision, then DROP the coordinator that
/// made it (a crash between decision and apply). A DIFFERENT resolver — a fresh
/// coordinator over the SAME live groups, the in-process analog of a surviving replica /
/// another node — learns the decision from the REPLICATED log and drives phase 2 to
/// completion. Progress happens WITHOUT the original coordinator: no blocking window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_coordinator_crash_between_decision_and_apply_does_not_block() {
    let dir = fresh_dir("nblive");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
    add_decision_group(&multi).await;

    let txn_id = "t-nb-live";
    let txn = two_shard_txn(txn_id, "la", "lb");
    // PHASE 1 durable prepare, then REPLICATE the COMMIT decision — but do NOT apply.
    assert!(coord.prepare_only(&txn).await.expect("prepare"));
    coord
        .decide_replicated_only(txn_id, GROUP_D, true)
        .await
        .expect("replicate decision");

    // The decision is readable from the REPLICATED state, and is NOT in coordinator redb.
    assert_eq!(
        coord.learn_decision(txn_id).await.unwrap(),
        Some(true),
        "decision readable from the replicated log"
    );
    let redb = backend.as_redb().unwrap();
    assert_eq!(
        redb.xshard_decision_get(txn_id).unwrap(),
        None,
        "decision is only in the replicated log, not the coordinator-private redb"
    );
    // Nothing applied yet (crash is before phase 2).
    assert_eq!(node_count(&state, GRAPH_A).await, 0);
    assert_eq!(node_count(&state, GRAPH_B).await, 0);

    // CRASH THE COORDINATOR: drop the instance that made the decision.
    drop(coord);

    // A DIFFERENT resolver finishes the in-doubt txn purely from the replicated decision.
    let resolver = CrossShardCoordinator::new(multi.clone(), backend.clone());
    let resolved = resolver
        .recover_in_doubt_nonblocking(GROUP_D)
        .await
        .expect("recover");
    assert_eq!(
        resolved, 1,
        "the in-doubt txn is resolved by a fresh resolver"
    );
    assert_eq!(
        node_count(&state, GRAPH_A).await,
        1,
        "resolver committed A without the original coordinator"
    );
    assert_eq!(
        node_count(&state, GRAPH_B).await,
        1,
        "resolver committed B without the original coordinator"
    );
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "prepares cleared by the resolver"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// (c) AGREEMENT WITH 2PC — a killed participant makes the non-blocking path ABORT with
/// NO partial commit, exactly as the 2PC path does for the same inputs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_aborts_like_2pc_on_killed_participant() {
    let dir = fresh_dir("nbabort");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
    add_decision_group(&multi).await;

    // KILL participant B (close group 200) — it cannot prepare.
    multi.close_group(GROUP_B).await.unwrap();
    assert!(multi.group(GROUP_B).await.is_none(), "B is killed");

    let txn = two_shard_txn("t-nb-abort", "aa", "ab");
    let outcome = coord
        .commit_cross_shard_nonblocking(&txn, GROUP_D)
        .await
        .expect("commit returns");
    assert_eq!(
        outcome,
        TxnOutcome::Aborted,
        "agrees with 2PC: a killed participant aborts the txn"
    );

    // NO PARTIAL COMMIT: the live participant A must NOT have applied.
    assert_eq!(
        node_count(&state, GRAPH_A).await,
        0,
        "no partial commit on A"
    );
    assert_eq!(node_count(&state, GRAPH_B).await, 0, "B never applied");

    let redb = backend.as_redb().unwrap();
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "no leaked prepares after abort"
    );
    assert_eq!(
        coord.learn_decision("t-nb-abort").await.unwrap(),
        None,
        "replicated ABORT decision GC'd after resolution"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// (b'/presumed-abort) A crash BEFORE any decision is replicated resolves as ABORT and
/// applies NOWHERE — the non-blocking analog of 2PC's presumed-abort (no partial commit
/// from an undecided crash, learned from the ABSENCE of a replicated decision).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_recovery_presumed_abort_with_no_replicated_decision() {
    let dir = fresh_dir("nbpresumed");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
    add_decision_group(&multi).await;

    let txn_id = "t-nb-presumed";
    let txn = two_shard_txn(txn_id, "pa", "pb");
    // PHASE 1 only — prepares durable, but NO decision is ever replicated.
    assert!(coord.prepare_only(&txn).await.expect("prepare"));
    assert_eq!(
        coord.learn_decision(txn_id).await.unwrap(),
        None,
        "no replicated decision exists"
    );

    // Recovery finds prepares with no replicated decision → presumed ABORT everywhere.
    let resolved = coord
        .recover_in_doubt_nonblocking(GROUP_D)
        .await
        .expect("recover");
    assert_eq!(
        resolved, 1,
        "the in-doubt txn is resolved (as presumed-abort)"
    );
    assert_eq!(node_count(&state, GRAPH_A).await, 0, "no apply on A");
    assert_eq!(node_count(&state, GRAPH_B).await, 0, "no apply on B");

    let redb = backend.as_redb().unwrap();
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "prepares cleared on presumed-abort"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── EG-324: FULL Calvin deterministic-ordering commit — live proofs ─────────────

/// (a) Calvin HAPPY PATH: a deterministic-ordering commit lands on BOTH participants
/// with NO vote round, the ORDER is replicated (not in coordinator redb), the txn gets a
/// monotone global sequence, and the replicated sequence node is GC'd after resolution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_deterministic_commit_is_atomic_and_vote_free() {
    use super::cross_shard_txn::{CalvinSequencer, GlobalSeq};
    let dir = fresh_dir("calvinhappy");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
    add_decision_group(&multi).await;
    let seq = CalvinSequencer::new();

    let txn = two_shard_txn("t-calvin-happy", "ca1", "cb1");
    let (outcome, gs) = coord
        .commit_cross_shard_calvin(&txn, &seq, GROUP_D)
        .await
        .expect("calvin commit");
    assert_eq!(outcome, TxnOutcome::Committed);
    assert_eq!(gs, GlobalSeq(1), "first sequenced txn draws slot 1");

    // BOTH graphs committed deterministically — all-or-nothing landed everywhere.
    assert_eq!(node_count(&state, GRAPH_A).await, 1, "shardA committed");
    assert_eq!(node_count(&state, GRAPH_B).await, 1, "shardB committed");

    let redb = backend.as_redb().unwrap();
    // Calvin NEVER runs a prepare/vote round and NEVER writes the coordinator-private
    // redb decision — the ORDER lived only in the replicated log.
    assert_eq!(
        redb.xshard_decision_get("t-calvin-happy").unwrap(),
        None,
        "calvin writes no coordinator-private redb decision"
    );
    assert!(
        redb.xshard_scan_prepares().unwrap().is_empty(),
        "calvin runs no prepare round"
    );
    // The replicated sequence node is GC'd after the txn resolved.
    assert_eq!(
        coord.learn_sequence("t-calvin-happy").await.unwrap(),
        None,
        "replicated sequence GC'd after commit"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// (b) THE CALVIN RECOVERY WIN: replicate the ORDER, then DROP the coordinator before it
/// executes. A DIFFERENT resolver — a fresh coordinator over the SAME live groups (the
/// in-process analog of a surviving replica) — reads the replicated sequence and REPLAYS
/// the txn deterministically to completion. Progress happens WITHOUT the original
/// coordinator and WITHOUT any vote: agreement on the order was agreement on the outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_crash_after_sequencing_is_resolved_by_replay() {
    use super::cross_shard_txn::CalvinSequencer;
    let dir = fresh_dir("calvinreplay");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
    add_decision_group(&multi).await;
    let seq = CalvinSequencer::new();

    let txn_id = "t-calvin-replay";
    let txn = two_shard_txn(txn_id, "cra", "crb");
    // Draw the order + REPLICATE it, but do NOT execute — the crash window (order durable,
    // apply not yet done).
    let gs = seq.assign();
    coord
        .sequence_replicated_only(txn_id, GROUP_D, gs)
        .await
        .expect("replicate order");

    // The order is readable from the REPLICATED state, and is NOT in coordinator redb.
    assert_eq!(
        coord.learn_sequence(txn_id).await.unwrap(),
        Some(gs),
        "order readable from the replicated log"
    );
    let redb = backend.as_redb().unwrap();
    assert_eq!(
        redb.xshard_decision_get(txn_id).unwrap(),
        None,
        "calvin keeps no coordinator-private redb decision"
    );
    // Nothing executed yet (crash is before execute).
    assert_eq!(node_count(&state, GRAPH_A).await, 0);
    assert_eq!(node_count(&state, GRAPH_B).await, 0);

    // CRASH THE COORDINATOR: a DIFFERENT resolver over the same live groups replays the
    // replicated order to completion — no vote, no original coordinator.
    drop(coord);
    let resolver = CrossShardCoordinator::new(multi.clone(), backend.clone());
    let replayed = resolver
        .recover_sequenced(&txn, GROUP_D)
        .await
        .expect("resolver replays the replicated order");
    assert_eq!(replayed, Some(gs), "resolved from the replicated sequence");
    assert_eq!(node_count(&state, GRAPH_A).await, 1, "replayed onto A");
    assert_eq!(node_count(&state, GRAPH_B).await, 1, "replayed onto B");
    assert_eq!(
        resolver.learn_sequence(txn_id).await.unwrap(),
        None,
        "replicated sequence GC'd after replay"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── EG-342: Calvin OLLP reconnaissance + ordered read-lock phase — live proof ────
// ── EG-343: multi-node sequencer epoch fan-in — determinism proof ───────────────

/// Write a single node's properties into `graph` through its owning Raft group (the
/// deterministic-execution write primitive the Calvin OLLP tests apply under lock).
async fn write_node(
    multi: &Arc<MultiRaft>,
    gid: u64,
    graph: &str,
    node: &str,
    props: serde_json::Value,
) {
    let g = multi.group(gid).await.expect("group exists");
    let req = super::RaftRequest {
        graph_fname: crate::persist::sanitize(graph),
        graph_name: graph.to_string(),
        graph_type: GraphType::Global,
        method: Method::AddNode {
            node_id: node.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&props).unwrap(),
        },
    };
    g.client_write(req).await.expect("client_write");
}

/// Decode a reconnaissance read's msgpack blob back to JSON (`None` ⇒ record absent).
fn decode_props(blob: Option<Vec<u8>>) -> Option<serde_json::Value> {
    blob.map(|b| rmp_serde::from_slice(&b).expect("decode props"))
}

/// EG-342: the OLLP reconnaissance + deterministic ordered read-lock phase gives FULL
/// serializable isolation of CONFLICTING sequenced txns.
///
/// Setup: a directory node `dir`→`{target: "k"}` in shardA. Two sequenced txns share the
/// record `k`:
///   * T1 (seq 1): statically writes `k = {v: "v1"}` (write-set known up front).
///   * T2 (seq 2, OLLP): its footprint is DATA-DEPENDENT — it must reconnoiter `dir` to
///     DISCOVER that `k` is in its read-set, then read `k` and write a result `r` in
///     shardB derived from `k`'s value.
///
/// The ordered lock phase registers both in sequence order and forces T2's shared lock
/// on `k` to WAIT behind T1's exclusive write lock. So even though T2's execution task is
/// spawned FIRST, it reads `k` only AFTER T1 has committed `v1` → `r = {saw: "v1"}`: T2
/// observes T1's committed write per the sequence order. That is serializability
/// equivalent to running T1 then T2 — the guarantee EG-324 deferred.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_ollp_ordered_readlock_serializes_conflicting_txns() {
    use super::cross_shard_txn::{CalvinSequencer, OrderedLockManager, RecordKey, RwSet};
    use std::collections::BTreeSet;

    let dir = fresh_dir("calvinollp");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, _state) = bring_up(&dir, backend.clone()).await;
    let coord = Arc::new(coord);

    // Seed the directory node the OLLP recon reads to discover its footprint. `dir` is
    // NOT written by either txn, so the recon of it is stable (no restart needed).
    write_node(
        &multi,
        GROUP_A,
        GRAPH_A,
        "dir",
        serde_json::json!({ "target": "k" }),
    )
    .await;

    let seq = CalvinSequencer::new();
    let lockmgr = OrderedLockManager::new();

    // ── T1 (seq 1): static write-set {shardA::k} ────────────────────────────────
    let s1 = seq.assign();
    let rw1 = RwSet {
        reads: BTreeSet::new(),
        writes: [RecordKey::new(GRAPH_A, "k")].into_iter().collect(),
    };

    // ── T2 (seq 2, OLLP): reconnoiter `dir` to DISCOVER the read of `k` ──────────
    let s2 = seq.assign();
    let dir_key = RecordKey::new(GRAPH_A, "dir");
    let rw2 = coord
        .predict_rwset(std::slice::from_ref(&dir_key), |observed| {
            // Data-dependent set discovery: the directory tells us WHICH record to read.
            let dir_val =
                decode_props(observed.get(&dir_key).cloned().flatten()).expect("dir seeded");
            let target = dir_val
                .get("target")
                .and_then(|t| t.as_str())
                .expect("dir.target")
                .to_string();
            RwSet {
                reads: [dir_key.clone(), RecordKey::new(GRAPH_A, target)]
                    .into_iter()
                    .collect(),
                writes: [RecordKey::new(GRAPH_B, "r")].into_iter().collect(),
            }
        })
        .await
        .expect("predict rwset");
    // The read of `k` was DYNAMICALLY discovered by the recon (not statically declared).
    assert!(
        rw2.reads.contains(&RecordKey::new(GRAPH_A, "k")),
        "OLLP recon discovered the data-dependent read of `k`"
    );

    // Register BOTH in sequence order (the Calvin lock-manager invariant), THEN run the
    // deterministic phase concurrently. `k` is the conflict record.
    let t1 = lockmgr.register(s1, &rw1);
    let t2 = lockmgr.register(s2, &rw2);

    // Spawn T2's execution FIRST — if the lock phase did NOT order by sequence, T2 could
    // read a missing/stale `k`. It must instead block until T1 commits.
    let (lm2, co2, mu2) = (lockmgr.clone(), coord.clone(), multi.clone());
    let exec_t2 = tokio::spawn(async move {
        let guard = lm2.granted(t2).await; // waits behind T1 on `k`
                                           // Under the held lock, read the (now committed) value of `k`.
        let k_val = decode_props(
            co2.reconnoiter(&RecordKey::new(GRAPH_A, "k"))
                .await
                .unwrap(),
        )
        .expect("k present once T1 committed");
        let seen = k_val.get("v").and_then(|v| v.as_str()).unwrap().to_string();
        write_node(
            &mu2,
            GROUP_B,
            GRAPH_B,
            "r",
            serde_json::json!({ "saw": seen }),
        )
        .await;
        guard.release();
    });

    // Give T2 a chance to run first, proving the ordering is enforced by the lock phase,
    // not by task scheduling luck.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (lm1, mu1) = (lockmgr.clone(), multi.clone());
    let exec_t1 = tokio::spawn(async move {
        let guard = lm1.granted(t1).await; // granted immediately (front, seq 1)
        write_node(
            &mu1,
            GROUP_A,
            GRAPH_A,
            "k",
            serde_json::json!({ "v": "v1" }),
        )
        .await;
        guard.release();
    });

    exec_t1.await.unwrap();
    exec_t2.await.unwrap();

    // SERIALIZABLE OUTCOME: T2 saw T1's committed write to `k` (sequence order 1 → 2).
    let r_val = decode_props(
        coord
            .reconnoiter(&RecordKey::new(GRAPH_B, "r"))
            .await
            .unwrap(),
    )
    .expect("r written by T2");
    assert_eq!(
        r_val.get("saw").and_then(|s| s.as_str()),
        Some("v1"),
        "T2 observed T1's committed write per the sequencer total order (serializable)"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// EG-348: Calvin OLLP recon-staleness RESTART — a txn whose reconnaissance goes STALE
/// between recon and lock acquisition is automatically re-sequenced (re-reconnoitered +
/// re-submitted at a NEW `GlobalSeq`) and ultimately commits with a SERIALIZABLE result.
///
/// EG-342 shipped only the DETECTION half (`recon_still_valid`); this proves the wired
/// restart loop (`acquire_ollp_with_restart`).
///
/// Setup — a directory `dir`→`{target}` in shardA that a lower-sequence txn MUTATES, so
/// the OLLP txn's first recon predicts against a target that is stale by lock-acquire time:
///   * `dir = {target: "k1"}`, `k1 = {v: "old"}`, `k2 = {v: "new"}` seeded up front.
///   * T_writer (seq 1): exclusive write of `dir` → sets `dir = {target: "k2"}`.
///   * T_ollp  (seq 2, OLLP): reconnoiters `dir` to discover WHICH record to read.
///
/// Timeline forced by the ordered lock phase (no correctness-bearing sleeps):
///   1. T_writer registers seq 1 (exclusive on `dir`) and is granted — but holds without
///      writing yet.
///   2. T_ollp's driver recons `dir` (still `{target: "k1"}` — the writer has not written),
///      predicts read-set `{dir, k1}`, registers seq 2 (shared on `dir`) and WAITS behind
///      the writer on `dir`.
///   3. Once the driver is waiting (`queue_depth(dir) >= 2`), T_writer writes
///      `dir = {target: "k2"}` and RELEASES.
///   4. T_ollp's seq-2 lock is granted; re-validation sees `dir` now `{target: "k2"}` !=
///      the reconned `{target: "k1"}` → STALE → the driver RELEASES and RESTARTS at seq 3.
///   5. The restart recons `dir = {target: "k2"}`, predicts `{dir, k2}`, locks (uncontended
///      now), re-validates OK, and returns the held guard at seq 3 with `attempts == 2`.
///   6. Deterministic execution under the fresh guard reads `k2 = {v: "new"}` and writes
///      `r = {saw: "new"}` in shardB.
///
/// The SERIALIZABLE outcome: `r.saw == "new"` — the OLLP txn restarted and read the
/// post-writer committed state (`dir`→`k2`), equivalent to running T_writer then T_ollp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_ollp_stale_recon_is_restarted_and_commits_serializably() {
    use super::cross_shard_txn::{CalvinSequencer, OrderedLockManager, RecordKey, RwSet};
    use std::collections::BTreeSet;

    let dir = fresh_dir("calvinrestart");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, _state) = bring_up(&dir, backend.clone()).await;
    let coord = Arc::new(coord);

    // Seed the directory + both candidate targets. `dir` is what the writer mutates.
    write_node(
        &multi,
        GROUP_A,
        GRAPH_A,
        "dir",
        serde_json::json!({ "target": "k1" }),
    )
    .await;
    write_node(
        &multi,
        GROUP_A,
        GRAPH_A,
        "k1",
        serde_json::json!({ "v": "old" }),
    )
    .await;
    write_node(
        &multi,
        GROUP_A,
        GRAPH_A,
        "k2",
        serde_json::json!({ "v": "new" }),
    )
    .await;

    let seq = CalvinSequencer::new();
    let lockmgr = OrderedLockManager::new();
    let dir_key = RecordKey::new(GRAPH_A, "dir");

    // ── T_writer (seq 1): register the exclusive lock on `dir` SYNCHRONOUSLY FIRST, so the
    //    OLLP txn's later seq-2 registration is strictly in sequence order. ──────────────
    let s1 = seq.assign();
    let rw_writer = RwSet {
        reads: BTreeSet::new(),
        writes: [dir_key.clone()].into_iter().collect(),
    };
    let tw = lockmgr.register(s1, &rw_writer);

    let (lm_w, mu_w, dk_w) = (lockmgr.clone(), multi.clone(), dir_key.clone());
    let writer = tokio::spawn(async move {
        let g = lm_w.granted(tw).await; // granted immediately (front, seq 1)
                                        // Wait until the OLLP txn has registered its seq-2 shared request behind us on
                                        // `dir` (a lock-manager fact — the recon has already read the OLD `dir`).
        wait_until(Duration::from_secs(5), || async {
            lm_w.queue_depth(&dk_w) >= 2
        })
        .await
        .expect("ollp txn registers behind the writer on dir");
        // NOW mutate the directory the OLLP recon depended on, then release.
        write_node(
            &mu_w,
            GROUP_A,
            GRAPH_A,
            "dir",
            serde_json::json!({ "target": "k2" }),
        )
        .await;
        g.release();
    });

    // ── T_ollp (seq 2, restart→seq 3): drive the OLLP restart loop. ─────────────────────
    let derive_dir = dir_key.clone();
    let acquired = coord
        .acquire_ollp_with_restart(
            &seq,
            &lockmgr,
            std::slice::from_ref(&dir_key),
            move |observed| {
                // Data-dependent set discovery: the directory says WHICH record to read.
                let dv =
                    decode_props(observed.get(&derive_dir).cloned().flatten()).expect("dir seeded");
                let target = dv
                    .get("target")
                    .and_then(|t| t.as_str())
                    .expect("dir.target")
                    .to_string();
                RwSet {
                    reads: [derive_dir.clone(), RecordKey::new(GRAPH_A, &target)]
                        .into_iter()
                        .collect(),
                    writes: [RecordKey::new(GRAPH_B, "r")].into_iter().collect(),
                }
            },
            5,
        )
        .await
        .expect("OLLP acquisition ultimately succeeds after restart");

    // The first recon went stale (dir k1→k2) → EXACTLY one restart (attempt 2 validated).
    assert_eq!(
        acquired.attempts, 2,
        "stale first recon forces exactly one OLLP restart"
    );
    assert_eq!(
        acquired.seq,
        super::cross_shard_txn::GlobalSeq(3),
        "the restart re-sequenced the txn at a fresh, higher GlobalSeq (1=writer, 2=stale, 3=valid)"
    );
    // The restart re-discovered the NEW target `k2` (not the stale `k1`).
    assert!(
        acquired
            .rwset
            .reads
            .contains(&RecordKey::new(GRAPH_A, "k2")),
        "the restarted recon discovered the post-mutation target k2"
    );

    // Deterministic execution under the held (fresh) lock: read the discovered target, write r.
    let k2_val = decode_props(
        coord
            .reconnoiter(&RecordKey::new(GRAPH_A, "k2"))
            .await
            .unwrap(),
    )
    .expect("k2 present");
    let seen = k2_val
        .get("v")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    write_node(
        &multi,
        GROUP_B,
        GRAPH_B,
        "r",
        serde_json::json!({ "saw": seen }),
    )
    .await;
    acquired.guard.release();

    writer.await.unwrap();

    // SERIALIZABLE OUTCOME: the OLLP txn restarted and read the post-writer state (dir→k2).
    let r_val = decode_props(
        coord
            .reconnoiter(&RecordKey::new(GRAPH_B, "r"))
            .await
            .unwrap(),
    )
    .expect("r written by the restarted OLLP txn");
    assert_eq!(
        r_val.get("saw").and_then(|s| s.as_str()),
        Some("new"),
        "the restarted OLLP txn read k2's committed value (serializable: T_writer then T_ollp)"
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// EG-343: two nodes, each with its own local sequencer, fan their per-epoch input
/// batches into ONE global total order — and DERIVE THE IDENTICAL ORDER. This is the
/// multi-node generalization of the single EG-324 sequencer: the merge is a pure,
/// data-only function, so every node computes byte-identical output and can then run the
/// vote-free deterministic execution phase without disagreeing on the order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calvin_two_node_epoch_fan_in_derives_identical_order() {
    use super::cross_shard_txn::{epoch_fan_in, GlobalSeq, NodeInput};

    // Node 1 and node 2 each locally sequenced two txns for epoch 9. Both nodes exchange
    // these batches (gossip/replicate) and independently run the fan-in.
    let mut per_node: BTreeMap<u64, Vec<NodeInput>> = BTreeMap::new();
    per_node.insert(
        1,
        vec![NodeInput::new(1, 1, "n1-a"), NodeInput::new(1, 2, "n1-b")],
    );
    per_node.insert(
        2,
        vec![NodeInput::new(2, 1, "n2-a"), NodeInput::new(2, 2, "n2-b")],
    );

    // Each node runs the SAME pure fan-in over the SAME batches.
    let order_on_node_1 = epoch_fan_in(9, &per_node);
    let order_on_node_2 = epoch_fan_in(9, &per_node);

    assert_eq!(
        order_on_node_1, order_on_node_2,
        "both nodes derive the identical global epoch order"
    );
    // Deterministic round-robin interleave by (local_seq, node): a,a then b,b.
    let ids: Vec<&str> = order_on_node_1
        .inputs
        .iter()
        .map(|si| si.txn_id.as_str())
        .collect();
    assert_eq!(ids, vec!["n1-a", "n2-a", "n1-b", "n2-b"]);
    assert_eq!(order_on_node_1.inputs[0].global_seq, GlobalSeq(1));
    assert_eq!(order_on_node_1.inputs.len(), 4);
    assert_eq!(order_on_node_1.epoch, 9);
}

/// EG-349: a Calvin OLLP txn whose reconnaissance goes stale is restarted (as in
/// [`calvin_ollp_stale_recon_is_restarted_and_commits_serializably`]) but this time the
/// restart is routed into a SPECIFIC epoch of the MULTI-NODE `epoch_fan_in` rather than a
/// fresh `GlobalSeq` in one sequencer's ordering domain — closing the gap
/// `acquire_ollp_with_restart`'s doc comment left open. Proves:
///
///   1. The txn's first attempt is placed in the base epoch ALONGSIDE another node's
///      (NODE_2) unrelated txn — a genuine multi-node epoch batch, not a single-node one.
///   2. The stale-recon restart routes the SECOND attempt into `base_epoch + 1`
///      (`epoch_for_restart`), and the txn ultimately commits with the SAME serializable
///      outcome as the single-domain test (it observed the writer's committed write).
///   3. ANY node re-deriving the merge from the identical exchanged per-epoch batches —
///      simulated here by an independent [`EpochFanInRegistry`] fed the same
///      registrations in a DIFFERENT order — computes the byte-identical [`EpochBatch`]
///      for both epochs the txn touched, and in particular the SAME final packed seq for
///      the restarted txn. That is "all nodes agree on the final serializable order."
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_ollp_epoch_routing_restart_agrees_across_nodes() {
    use super::cross_shard_txn::{
        epoch_fan_in, EpochFanInRegistry, OrderedLockManager, RecordKey, RwSet,
    };
    use std::collections::BTreeSet;

    const NODE_1: u64 = 1; // home of the writer + the restarting OLLP txn
    const NODE_2: u64 = 2; // a peer node contributing an unrelated txn to the same epoch
    const BASE_EPOCH: u64 = 5;

    let dir = fresh_dir("calvinepochrt");
    let backend: Arc<dyn PersistenceBackend> =
        Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).expect("open redb"));
    let (multi, coord, _state) = bring_up(&dir, backend.clone()).await;
    let coord = Arc::new(coord);

    // Seed the directory + both candidate targets — identical setup to the single-domain
    // stale-recon test: `dir` starts pointing at `k1` (old); the writer flips it to `k2`.
    write_node(
        &multi,
        GROUP_A,
        GRAPH_A,
        "dir",
        serde_json::json!({ "target": "k1" }),
    )
    .await;
    write_node(
        &multi,
        GROUP_A,
        GRAPH_A,
        "k1",
        serde_json::json!({ "v": "old" }),
    )
    .await;
    write_node(
        &multi,
        GROUP_A,
        GRAPH_A,
        "k2",
        serde_json::json!({ "v": "new" }),
    )
    .await;

    let fan_in = EpochFanInRegistry::new();
    let lockmgr = OrderedLockManager::new();
    let dir_key = RecordKey::new(GRAPH_A, "dir");

    // ── A genuine MULTI-NODE base epoch: NODE_2 contributes an unrelated txn, NODE_1
    //    contributes the writer — both land in BASE_EPOCH before the OLLP txn registers. ─
    fan_in.register_local(NODE_2, BASE_EPOCH, "t-peer");
    fan_in.register_local(NODE_1, BASE_EPOCH, "t-writer");
    let writer_seq = fan_in
        .packed_seq_for(BASE_EPOCH, "t-writer")
        .expect("t-writer registered in the base epoch");

    // ── T_writer: register the exclusive lock on `dir` SYNCHRONOUSLY FIRST at its
    //    epoch-packed seq, so the OLLP txn's later registration is strictly behind it. ──
    let rw_writer = RwSet {
        reads: BTreeSet::new(),
        writes: [dir_key.clone()].into_iter().collect(),
    };
    let tw = lockmgr.register(writer_seq, &rw_writer);

    let (lm_w, mu_w, dk_w) = (lockmgr.clone(), multi.clone(), dir_key.clone());
    let writer = tokio::spawn(async move {
        let g = lm_w.granted(tw).await; // granted immediately (front of the queue)
        // Wait until the OLLP txn's first attempt has registered behind us on `dir` (a
        // lock-manager fact — its recon has already read the OLD `dir`).
        wait_until(Duration::from_secs(5), || async {
            lm_w.queue_depth(&dk_w) >= 2
        })
        .await
        .expect("ollp txn registers behind the writer on dir");
        // NOW mutate the directory the OLLP recon depended on, then release.
        write_node(
            &mu_w,
            GROUP_A,
            GRAPH_A,
            "dir",
            serde_json::json!({ "target": "k2" }),
        )
        .await;
        g.release();
    });

    // ── T_ollp: drive the multi-node epoch-routing OLLP restart loop. ──────────────────
    let derive_dir = dir_key.clone();
    let acquired = coord
        .acquire_ollp_with_restart_epoch(
            &fan_in,
            NODE_1,
            BASE_EPOCH,
            "t-ollp",
            &lockmgr,
            std::slice::from_ref(&dir_key),
            move |observed| {
                let dv =
                    decode_props(observed.get(&derive_dir).cloned().flatten()).expect("dir seeded");
                let target = dv
                    .get("target")
                    .and_then(|t| t.as_str())
                    .expect("dir.target")
                    .to_string();
                RwSet {
                    reads: [derive_dir.clone(), RecordKey::new(GRAPH_A, &target)]
                        .into_iter()
                        .collect(),
                    writes: [RecordKey::new(GRAPH_B, "r")].into_iter().collect(),
                }
            },
            5,
        )
        .await
        .expect("OLLP acquisition ultimately succeeds after the epoch restart");

    // The first recon (in BASE_EPOCH) went stale (dir k1→k2) → exactly one restart, into
    // the NEXT epoch.
    assert_eq!(
        acquired.attempts, 2,
        "stale first recon forces exactly one epoch restart"
    );
    assert_eq!(
        acquired.epoch,
        BASE_EPOCH + 1,
        "the restart routed the txn into the NEXT epoch of the multi-node fan-in"
    );
    assert!(
        acquired.rwset.reads.contains(&RecordKey::new(GRAPH_A, "k2")),
        "the restarted recon discovered the post-mutation target k2"
    );

    // Deterministic execution under the held (fresh, epoch-routed) lock.
    let k2_val = decode_props(
        coord
            .reconnoiter(&RecordKey::new(GRAPH_A, "k2"))
            .await
            .unwrap(),
    )
    .expect("k2 present");
    let seen = k2_val
        .get("v")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    write_node(
        &multi,
        GROUP_B,
        GRAPH_B,
        "r",
        serde_json::json!({ "saw": seen }),
    )
    .await;
    acquired.guard.release();

    writer.await.unwrap();

    // SERIALIZABLE OUTCOME: identical to the single-domain test — the OLLP txn restarted
    // and read the post-writer committed state.
    let r_val = decode_props(
        coord
            .reconnoiter(&RecordKey::new(GRAPH_B, "r"))
            .await
            .unwrap(),
    )
    .expect("r written by the restarted OLLP txn");
    assert_eq!(
        r_val.get("saw").and_then(|s| s.as_str()),
        Some("new"),
        "the restarted OLLP txn read k2's committed value (serializable: T_writer then T_ollp)"
    );

    // ── ALL NODES AGREE: an INDEPENDENT registry, fed the identical registrations in a
    //    DIFFERENT order (simulating a peer node that received the same gossiped batches
    //    in a different arrival order), derives the byte-identical merged order for BOTH
    //    epochs the txn touched — and in particular the SAME final packed seq for the
    //    restarted txn. This is the property that lets every node run the deterministic
    //    execution phase without a vote. ────────────────────────────────────────────────
    let fan_in_peer = EpochFanInRegistry::new();
    fan_in_peer.register_local(NODE_1, BASE_EPOCH, "t-writer");
    fan_in_peer.register_local(NODE_2, BASE_EPOCH, "t-peer");
    fan_in_peer.register_local(NODE_1, BASE_EPOCH + 1, "t-ollp");

    assert_eq!(
        fan_in_peer.fan_in(BASE_EPOCH),
        fan_in.fan_in(BASE_EPOCH),
        "a peer node registering the same base-epoch inputs in a different order \
         derives the identical merged order"
    );
    assert_eq!(
        fan_in_peer.fan_in(BASE_EPOCH + 1),
        fan_in.fan_in(BASE_EPOCH + 1),
        "...and the identical merged restart-epoch order"
    );
    assert_eq!(
        fan_in_peer.packed_seq_for(BASE_EPOCH + 1, "t-ollp"),
        Some(acquired.seq),
        "every node derives the SAME final packed seq for the restarted txn"
    );
    // Sanity: the peer's independently-derived merge also agrees with the pure
    // `epoch_fan_in` function called directly over the peer's own snapshot.
    assert_eq!(
        fan_in_peer.fan_in(BASE_EPOCH + 1),
        epoch_fan_in(BASE_EPOCH + 1, &fan_in_peer.snapshot(BASE_EPOCH + 1))
    );

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
