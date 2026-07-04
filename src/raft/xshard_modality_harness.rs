//! In-process cross-shard 2PC **modality-spanning** coordinator-kill harness
//! (CONCEPT:EG-KG.txn.crossshard-2pc-modality-harness) — the `--features cluster`
//! proof for EG-396 (the row previously tracked open as
//! `EG-KG.txn.crossshard-2pc-open-reason`).
//!
//! ## What this proves
//!
//! It stands up a **live in-process multi-group cluster** (one in-process node running
//! TWO independent openraft groups on a shared listener, over one redb-authoritative
//! `graph.redb` — the SAME machinery the single-group failover + `xshard_harness` tests
//! use) and drives a **cross-shard transaction that spans TWO modalities across the two
//! groups**:
//!
//!   * **Group A / `modalA`** — the **property-graph** modality (`Method::AddNode`).
//!   * **Group B / `modalB`** — the **RDF / semantic-triple** modality
//!     (`Method::AddTriples { turtle }`), which the Raft state-machine apply path
//!     (`wal::apply` → `eg_rdf::mapping::load_triples`) projects into the group's graph.
//!
//! Both modalities are replicated through their owning group's `client_write` and are
//! durably captured in the 2PC PREPARE record (the serialized slice), so recovery
//! re-applies them. The harness then **kills the coordinator mid-2PC** at each of the
//! two dangerous windows and asserts a **SINGLE all-or-nothing decision** — the two
//! modalities are NEVER split (no half-committed node without its triple, or vice
//! versa):
//!
//!   1. **Happy commit** — `commit_cross_shard` lands BOTH modalities on BOTH groups.
//!   2. **Participant kill during PREPARE** — a group closed before prepare cannot vote
//!      → the txn ABORTS and the LIVE group applied NOTHING (no partial commit).
//!   3. **Coordinator kill AFTER the COMMIT decision, before phase-2 apply** — drop the
//!      whole node + backend (a `kill -9` analog: only fsynced redb records survive),
//!      reopen over the same files, `recover_in_doubt()` reads the durable COMMIT
//!      decision → re-applies BOTH modalities. Single decision = COMMIT.
//!   4. **Coordinator kill BEFORE any decision (presumed-abort)** — durable prepares,
//!      no decision; recovery resolves to ABORT → NEITHER modality lands. Single
//!      decision = ABORT.
//!
//! The nemesis "kills" the coordinator via the phase-granular `prepare_only` /
//! `decide_only` entry points on [`CrossShardCoordinator`] (available under
//! `test`/`harness`/`compute-dist`) plus a full node+backend drop and reopen — the
//! deterministic fault injection at the 2PC prepare/commit boundary that the ignored
//! EG-396 spec called for, now running fully in-process under `--features cluster`.
//!
//! ## What real multi-HOST hardware still can't cover here (documented remainder)
//!
//!   * **Cross-NODE participants.** All groups' state machines run in ONE process; the
//!     coordinator routes to LOCAL group state (`prepare_participant`: "cross-NODE
//!     participants are a follow-up"). A participant living on a DIFFERENT physical host
//!     — with real network RPC, packet loss and cross-host clock skew — is only
//!     exercised by the process-level `scripts/validate-raft-cluster.sh` and needs real
//!     multi-node hardware for the live-cadence / cross-host soak AGENTS.md flags.
//!   * **ANN vector + TSDB measurement modalities across shards.** The cross-shard 2PC
//!     replicates GRAPH-CORE methods (property graph, RDF triples, broker, streaming —
//!     everything `wal::apply` handles). ANN embeddings and time-series measurements are
//!     a **single-graph** cross-modal barrier: they land atomically in ONE redb
//!     `WriteTransaction` via `handlers::txn::commit_cross_modal` (they are NOT part of
//!     a cross-shard slice's `write_set`/`extra_writes`, and `wal::apply` no-ops them).
//!     So "no half-committed vector/measurement" is proven WITHIN a group; carrying
//!     staged vector/measurement batches into the cross-shard slice apply + a per-group
//!     cross-modal apply is the remaining engine work to span ANN/TSDB across shards.

#![cfg(feature = "compute-dist")]

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

/// Group + graph names for the two modalities the cross-shard txn spans.
const GROUP_A: u64 = 100;
const GROUP_B: u64 = 200;
/// `modalA` — the property-graph modality (nodes).
const GRAPH_A: &str = "modalA";
/// `modalB` — the RDF / semantic-triple modality (triples projected into a graph).
const GRAPH_B: &str = "modalB";

/// The Turtle the `modalB` slice stages — one triple whose subject+object project to
/// nodes, so `graph_node_count(modalB) > 0` iff the RDF modality landed.
const MODAL_B_TURTLE: &str = "@prefix ex: <http://ex/> .\nex:s ex:p ex:o .\n";

/// Build a redb-AUTHORITATIVE `ServerState` over an already-open backend (redb is
/// single-handle-per-process, so a test that needs the concrete handle SHARES it with
/// the state, never opens a second over the same dir). Mirrors `xshard_harness::make_state`.
async fn make_state(dir: &str, backend: Arc<dyn PersistenceBackend>) -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            crate::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "xshard-modality-test".to_string(), // # sanitizer:ignore
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
    let d = std::env::temp_dir().join(format!("eg-xshard-modal-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d.to_string_lossy().to_string()
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
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

/// Bring up a one-node, two-group cluster over `dir`'s redb, with `modalA`→100 and
/// `modalB`→200 assigned in the router, each group's single-node leader elected.
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

/// Count nodes in a named graph on a state (the read-back probe for BOTH modalities:
/// the property-graph node count for `modalA`, and the projected-triple node count for
/// `modalB`).
async fn graph_node_count(state: &Arc<RwLock<ServerState>>, graph: &str) -> usize {
    let s = state.read().await;
    s.registry
        .get(graph)
        .map(|e| e.core.node_count())
        .unwrap_or(0)
}

/// A cross-shard txn spanning TWO modalities: a property-graph node into `modalA`
/// (group A) and an RDF triple into `modalB` (group B).
fn modality_spanning_txn(txn_id: &str, node: &str) -> CrossShardTxn {
    let graph_slice = GraphSlice {
        graph_name: GRAPH_A.to_string(),
        graph_fname: crate::persist::sanitize(GRAPH_A),
        graph_type: GraphType::Global,
        methods: vec![Method::AddNode {
            node_id: node.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"n": node})).unwrap(),
        }],
    };
    let rdf_slice = GraphSlice {
        graph_name: GRAPH_B.to_string(),
        graph_fname: crate::persist::sanitize(GRAPH_B),
        graph_type: GraphType::Global,
        methods: vec![Method::AddTriples {
            turtle: MODAL_B_TURTLE.to_string(),
            ntriples: String::new(),
        }],
    };
    CrossShardTxn {
        txn_id: txn_id.to_string(),
        slices: vec![graph_slice, rdf_slice],
    }
}

/// Outcome of one modality read-back: are the property-graph (A) and RDF (B) modalities
/// present? The single-decision invariant is `a_present == b_present` ALWAYS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ModalityState {
    a_present: bool,
    b_present: bool,
}

impl ModalityState {
    async fn read(state: &Arc<RwLock<ServerState>>) -> Self {
        Self {
            a_present: graph_node_count(state, GRAPH_A).await > 0,
            b_present: graph_node_count(state, GRAPH_B).await > 0,
        }
    }
    /// The all-or-nothing invariant: the two modalities agree (both landed or neither).
    fn is_atomic(&self) -> bool {
        self.a_present == self.b_present
    }
}

/// A structured report of the four scenarios, returned by [`prove_crossshard_modality_2pc_single_decision`].
#[derive(Debug, Clone)]
pub struct ProofReport {
    /// Scenario 1 — happy `commit_cross_shard` landed BOTH modalities.
    pub happy_committed_both: bool,
    /// Scenario 2 — a participant killed during PREPARE aborted with NO partial commit.
    pub participant_kill_aborted_clean: bool,
    /// Scenario 3 — coordinator killed AFTER the COMMIT decision recovered to COMMIT
    /// (both modalities re-applied — single decision).
    pub coord_kill_post_decision_recovered_commit: bool,
    /// Scenario 4 — coordinator killed BEFORE any decision recovered to ABORT
    /// (neither modality landed — single decision).
    pub coord_kill_pre_decision_recovered_abort: bool,
}

impl ProofReport {
    /// Every scenario proved atomic (all-or-nothing) — the EG-396 contract.
    pub fn all_atomic(&self) -> bool {
        self.happy_committed_both
            && self.participant_kill_aborted_clean
            && self.coord_kill_post_decision_recovered_commit
            && self.coord_kill_pre_decision_recovered_abort
    }
}

/// Run the full four-scenario cross-shard 2PC modality-spanning coordinator-kill proof
/// (CONCEPT:EG-KG.txn.crossshard-2pc-modality-harness). Returns `Ok(ProofReport)` with
/// every flag `true` when atomicity held in ALL scenarios; `Err(_)` on any atomicity
/// violation (a half-committed modality) or infrastructure failure. Callable from an
/// external `--features cluster` integration test (it does not require the `test` cfg,
/// only `compute-dist`).
pub async fn prove_crossshard_modality_2pc_single_decision() -> Result<ProofReport, String> {
    let happy_committed_both = scenario_happy().await?;
    let participant_kill_aborted_clean = scenario_participant_kill().await?;
    let coord_kill_post_decision_recovered_commit = scenario_coord_kill_post_decision().await?;
    let coord_kill_pre_decision_recovered_abort = scenario_coord_kill_pre_decision().await?;

    let report = ProofReport {
        happy_committed_both,
        participant_kill_aborted_clean,
        coord_kill_post_decision_recovered_commit,
        coord_kill_pre_decision_recovered_abort,
    };
    if !report.all_atomic() {
        return Err(format!(
            "cross-shard modality 2PC atomicity violated: {report:?}"
        ));
    }
    Ok(report)
}

/// Scenario 1 — happy commit lands BOTH modalities atomically on BOTH groups.
async fn scenario_happy() -> Result<bool, String> {
    let dir = fresh_dir("happy");
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).map_err(|e| e.to_string())?,
    );
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;

    let txn = modality_spanning_txn("t-modal-happy", "n1");
    let outcome = coord.commit_cross_shard(&txn).await?;
    let modal = ModalityState::read(&state).await;

    let redb = backend.as_redb().ok_or("redb")?;
    let no_leaked_prepares = redb
        .xshard_scan_prepares()
        .map_err(|e| e.to_string())?
        .is_empty();
    let decision_cleared = redb
        .xshard_decision_get("t-modal-happy")
        .map_err(|e| e.to_string())?
        .is_none();

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);

    Ok(outcome == TxnOutcome::Committed
        && modal.is_atomic()
        && modal.a_present
        && modal.b_present
        && no_leaked_prepares
        && decision_cleared)
}

/// Scenario 2 — a participant killed (group closed) before PREPARE cannot vote → the
/// txn ABORTS and the LIVE participant applied NOTHING (no partial commit).
async fn scenario_participant_kill() -> Result<bool, String> {
    let dir = fresh_dir("killprep");
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).map_err(|e| e.to_string())?,
    );
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;

    // KILL the RDF participant (close group 200) — unreachable to prepare.
    multi.close_group(GROUP_B).await?;
    let b_killed = multi.group(GROUP_B).await.is_none();

    let txn = modality_spanning_txn("t-modal-killed", "n2");
    let outcome = coord.commit_cross_shard(&txn).await?;
    let modal = ModalityState::read(&state).await;

    let redb = backend.as_redb().ok_or("redb")?;
    let no_leaked_prepares = redb
        .xshard_scan_prepares()
        .map_err(|e| e.to_string())?
        .is_empty();

    multi.stop_listener();
    backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);

    // ABORT, no partial commit (neither modality present), clean 2PC state.
    Ok(b_killed
        && outcome == TxnOutcome::Aborted
        && modal.is_atomic()
        && !modal.a_present
        && !modal.b_present
        && no_leaked_prepares)
}

/// Scenario 3 — coordinator killed AFTER a COMMIT decision (before phase-2 apply):
/// drop the node+backend, reopen, recover → COMMIT re-applies BOTH modalities.
async fn scenario_coord_kill_post_decision() -> Result<bool, String> {
    let dir = fresh_dir("recovercommit");
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).map_err(|e| e.to_string())?,
    );
    let txn_id = "t-modal-recover-commit";
    {
        let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
        let txn = modality_spanning_txn(txn_id, "n3");
        // PHASE 1: prepare both (durable), log COMMIT — but do NOT apply.
        if !coord.prepare_only(&txn).await? {
            return Err("prepare_only voted NO in the happy recovery setup".into());
        }
        coord.decide_only(txn_id, true).await?;
        // Nothing applied yet: both modalities still absent.
        let staged = ModalityState::read(&state).await;
        if staged.a_present || staged.b_present {
            return Err("applied before phase 2 in post-decision setup".into());
        }
        // KILL the coordinator + node: stop listener, drop groups.
        multi.stop_listener();
        multi.close_group(GROUP_A).await?;
        multi.close_group(GROUP_B).await?;
    }
    backend.shutdown();
    drop(backend);

    // Process restart: reopen a brand-new backend over the SAME files.
    let backend2: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).map_err(|e| e.to_string())?,
    );
    let (multi2, coord2, state2) = bring_up(&dir, backend2.clone()).await;

    let resolved = coord2.recover_in_doubt().await?;
    let modal = ModalityState::read(&state2).await;

    let redb = backend2.as_redb().ok_or("redb")?;
    let cleared = redb
        .xshard_scan_prepares()
        .map_err(|e| e.to_string())?
        .is_empty()
        && redb
            .xshard_decision_get(txn_id)
            .map_err(|e| e.to_string())?
            .is_none();

    multi2.stop_listener();
    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);

    // Single decision = COMMIT: BOTH modalities re-applied, records cleared.
    Ok(resolved == 1 && modal.is_atomic() && modal.a_present && modal.b_present && cleared)
}

/// Scenario 4 — coordinator killed BEFORE any decision (presumed-abort): durable
/// prepares, no decision; recovery resolves to ABORT → NEITHER modality lands.
async fn scenario_coord_kill_pre_decision() -> Result<bool, String> {
    let dir = fresh_dir("recoverabort");
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).map_err(|e| e.to_string())?,
    );
    let txn_id = "t-modal-recover-abort";
    {
        let (multi, coord, _state) = bring_up(&dir, backend.clone()).await;
        let txn = modality_spanning_txn(txn_id, "n4");
        // PHASE 1 only — prepares durable, but NO decision ever logged.
        if !coord.prepare_only(&txn).await? {
            return Err("prepare_only voted NO in the abort recovery setup".into());
        }
        let redb = backend.as_redb().ok_or("redb")?;
        let two_prepares = redb
            .xshard_scan_prepares()
            .map_err(|e| e.to_string())?
            .len()
            == 2;
        let no_decision = redb
            .xshard_decision_get(txn_id)
            .map_err(|e| e.to_string())?
            .is_none();
        if !two_prepares || !no_decision {
            return Err("unexpected 2PC state before the pre-decision crash".into());
        }
        multi.stop_listener();
        multi.close_group(GROUP_A).await?;
        multi.close_group(GROUP_B).await?;
    }
    backend.shutdown();
    drop(backend);

    let backend2: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir.clone(), FsyncPolicy::Each, 4096).map_err(|e| e.to_string())?,
    );
    let (multi2, coord2, state2) = bring_up(&dir, backend2.clone()).await;

    let resolved = coord2.recover_in_doubt().await?;
    let modal = ModalityState::read(&state2).await;

    let redb = backend2.as_redb().ok_or("redb")?;
    let prepares_cleared = redb
        .xshard_scan_prepares()
        .map_err(|e| e.to_string())?
        .is_empty();

    multi2.stop_listener();
    backend2.shutdown();
    let _ = std::fs::remove_dir_all(&dir);

    // Single decision = ABORT (presumed): NEITHER modality landed, prepares cleared.
    Ok(resolved == 1
        && modal.is_atomic()
        && !modal.a_present
        && !modal.b_present
        && prepares_cleared)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The umbrella proof — runs all four scenarios (CONCEPT:EG-KG.txn.crossshard-2pc-modality-harness).
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn crossshard_modality_2pc_single_decision_eg396() {
        let report = prove_crossshard_modality_2pc_single_decision()
            .await
            .expect("cross-shard modality 2PC proof runs");
        assert!(report.all_atomic(), "all scenarios atomic: {report:?}");
    }

    /// Scenario 1 in isolation — happy commit spans both modalities atomically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn happy_commit_spans_both_modalities() {
        assert!(scenario_happy().await.expect("happy scenario"));
    }

    /// Scenario 2 in isolation — participant kill during prepare, no partial commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn participant_kill_aborts_no_partial_modality() {
        assert!(scenario_participant_kill().await.expect("kill scenario"));
    }

    /// Scenario 3 in isolation — coordinator kill after COMMIT decision → recover commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coord_kill_post_decision_recovers_commit_both_modalities() {
        assert!(scenario_coord_kill_post_decision()
            .await
            .expect("recover-commit scenario"));
    }

    /// Scenario 4 in isolation — coordinator kill before decision → recover abort.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coord_kill_pre_decision_recovers_abort_neither_modality() {
        assert!(scenario_coord_kill_pre_decision()
            .await
            .expect("recover-abort scenario"));
    }
}
