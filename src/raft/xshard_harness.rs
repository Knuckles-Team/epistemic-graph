//! Cross-shard 2PC atomicity + recovery gauntlet (CONCEPT:KG-2.222).
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
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "xshard-test".to_string(),
        persist_dir: Some(dir.to_string()),
        persistence: Some(backend),
        redb_authoritative: true,
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(64)),
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
// 6. Lane N WIRE (CONCEPT:KG-2.226) — a USER multi-graph txn across 2 groups,
//    driven through the BeginTxn→stage→Commit HANDLER, is atomic.
// ─────────────────────────────────────────────────────────────────────────

use crate::protocol::{Response, ResultPayload};
use crate::server::handlers::txn::try_handle as txn_handle;

/// Register `shardA`/`shardB` in the registry + wire `state.multi_raft` so the
/// user-facing commit path can resolve the cross-shard span (CONCEPT:KG-2.226).
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
/// inherits the coordinator's atomicity under a participant kill (CONCEPT:KG-2.226).
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
