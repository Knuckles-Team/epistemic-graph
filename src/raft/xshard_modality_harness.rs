//! In-process cross-shard 2PC **modality-spanning** coordinator-kill harness
//! (CONCEPT:EG-KG.txn.crossshard-2pc-modality-harness) — the `--features cluster`
//! proof for EG-396 (the row previously tracked open as
//! `EG-KG.txn.crossshard-2pc-open-reason`).
//!
//! ## What this proves
//!
//! It stands up a **live in-process multi-group cluster** (one in-process node running
//! TWO independent openraft groups on a shared listener, over one redb-authoritative
//! authoritative shard — the SAME machinery the single-group failover + `xshard_harness` tests
//! use) and drives a **cross-shard transaction that spans TWO modalities across the two
//! groups**:
//!
//!   * **Group A / `modalA`** — the **property-graph** modality (`Method::AddNode`).
//!   * **Group B / `modalB`** — the **RDF / semantic-triple** modality
//!     (`Method::AddTriples { turtle }`), which the Raft state-machine apply path
//!     (`mutation_apply::apply` → `eg_rdf::mapping::load_triples`) projects into the group's graph.
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
//!     everything `mutation_apply::apply` handles). ANN embeddings and time-series measurements are
//!     a **single-graph** cross-modal barrier: they land atomically in ONE redb
//!     `WriteTransaction` via `handlers::txn::commit_cross_modal` (they are NOT part of
//!     a cross-shard slice's `write_set`/`extra_writes`, and `mutation_apply::apply` no-ops them).
//!     So "no half-committed vector/measurement" is proven WITHIN a group; carrying
//!     staged vector/measurement batches into the cross-shard slice apply + a per-group
//!     cross-modal apply is the remaining engine work to span ANN/TSDB across shards.

#![cfg(feature = "compute-dist")]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use super::cross_shard_txn::{CrossShardCoordinator, CrossShardTxn, GraphSlice, TxnOutcome};
use super::multi::MultiRaft;
use crate::protocol::{GraphType, Method};

use super::fixture;

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

/// Bring up a one-node, two-group cluster over `dir`'s redb, with `modalA`→100 and
/// `modalB`→200 assigned in the router, each group's single-node leader elected.
async fn bring_up(
    dir: &str,
    backend: fixture::Backend,
) -> (
    Arc<MultiRaft>,
    CrossShardCoordinator,
    Arc<RwLock<crate::server::ServerState>>,
) {
    let (multi, state) = fixture::start_routed_groups(
        dir,
        backend.clone(),
        crate::isolation::IsolationLayer::new(),
        "test-xshard-modality-secret",
        &[GROUP_A, GROUP_B],
        &[(GRAPH_A, GROUP_A), (GRAPH_B, GROUP_B)],
    )
    .await;
    let coord = CrossShardCoordinator::new(multi.clone(), backend);
    (multi, coord, state)
}

/// Count nodes in a named graph on a state (the read-back probe for BOTH modalities:
/// the property-graph node count for `modalA`, and the projected-triple node count for
/// `modalB`).
async fn graph_node_count(state: &Arc<RwLock<crate::server::ServerState>>, graph: &str) -> usize {
    fixture::node_count(state, graph).await
}

/// Synchronous directory removal operation run only by [`run_scenario_cleanup`].
fn remove_scenario_dir(dir: &str) -> std::io::Result<()> {
    std::fs::remove_dir_all(dir)
}

/// Run one completed scenario's best-effort cleanup without blocking the async runtime.
async fn run_scenario_cleanup<F>(dir: &str, cleanup: F)
where
    F: FnOnce(&str) -> std::io::Result<()> + Send + 'static,
{
    let owned_dir = dir.to_string();
    match ::tokio::task::spawn_blocking(move || cleanup(&owned_dir)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(directory = dir, %error, "scenario directory cleanup failed");
        }
        Err(error) => {
            tracing::warn!(directory = dir, %error, "scenario cleanup task failed");
        }
    }
}

/// A cross-shard txn spanning TWO modalities: a property-graph node into `modalA`
/// (group A) and an RDF triple into `modalB` (group B).
fn modality_spanning_txn(txn_id: &str, node: &str) -> CrossShardTxn {
    fenced_modality_txn(txn_id, node, 0, None)
}

/// [`modality_spanning_txn`] with `modalA`'s slice carrying an explicit placement
/// epoch and fence. `modalB` never goes through the placement catalog.
fn fenced_modality_txn(
    txn_id: &str,
    node: &str,
    placement_epoch: u64,
    fencing_token: Option<u64>,
) -> CrossShardTxn {
    let graph_slice = fixture::add_node_slice(GRAPH_A, node, placement_epoch, fencing_token);
    let rdf_slice = GraphSlice {
        graph_name: GRAPH_B.to_string(),
        graph_fname: crate::persist::sanitize(GRAPH_B),
        graph_type: GraphType::Global,
        methods: vec![Method::AddTriples {
            turtle: MODAL_B_TURTLE.to_string(),
            ntriples: String::new(),
        }],
        placement_epoch: 0,
        fencing_token: None,
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
    const NEITHER: Self = Self {
        a_present: false,
        b_present: false,
    };
    const BOTH: Self = Self {
        a_present: true,
        b_present: true,
    };

    async fn read(state: &Arc<RwLock<crate::server::ServerState>>) -> Self {
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

/// Reopen the durable modality fixture and resolve its in-doubt transaction. Both crash
/// scenarios use this exact restart boundary so their recovery assertions remain distinct
/// while backend initialization, placement wiring, and read-back happen in one fixture.
async fn reopen_and_recover(
    dir: &str,
) -> Result<
    (
        fixture::Backend,
        Arc<MultiRaft>,
        Arc<RwLock<crate::server::ServerState>>,
        usize,
        ModalityState,
    ),
    String,
> {
    let backend2 = fixture::open_backend(dir)?;
    let (multi2, coord2, state2) = bring_up(dir, backend2.clone()).await;
    let resolved = coord2.recover_in_doubt().await?;
    let modal = ModalityState::read(&state2).await;
    Ok((backend2, multi2, state2, resolved, modal))
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
    let dir = fixture::fresh_dir("eg-xshard-modal", "happy");
    let backend = fixture::open_backend(&dir)?;
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

    multi.shutdown().await;
    run_scenario_cleanup(&dir, remove_scenario_dir).await;

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
    let dir = fixture::fresh_dir("eg-xshard-modal", "killprep");
    let backend = fixture::open_backend(&dir)?;
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

    multi.shutdown().await;
    run_scenario_cleanup(&dir, remove_scenario_dir).await;

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
    let dir = fixture::fresh_dir("eg-xshard-modal", "recovercommit");
    let backend = fixture::open_backend(&dir)?;
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
        // KILL the coordinator + node: drain listener, connection tasks, groups,
        // and writers before the restart boundary.
        multi.shutdown().await;
    }
    backend.shutdown();
    drop(backend);

    // Process restart: reopen a brand-new backend over the SAME files.
    let (backend2, multi2, _state2, resolved, modal) = reopen_and_recover(&dir).await?;

    let redb = backend2.as_redb().ok_or("redb")?;
    let cleared = redb
        .xshard_scan_prepares()
        .map_err(|e| e.to_string())?
        .is_empty()
        && redb
            .xshard_decision_get(txn_id)
            .map_err(|e| e.to_string())?
            .is_none();

    multi2.shutdown().await;
    run_scenario_cleanup(&dir, remove_scenario_dir).await;

    // Single decision = COMMIT: BOTH modalities re-applied, records cleared.
    Ok(resolved == 1 && modal.is_atomic() && modal.a_present && modal.b_present && cleared)
}

/// Scenario 4 — coordinator killed BEFORE any decision (presumed-abort): durable
/// prepares, no decision; recovery resolves to ABORT → NEITHER modality lands.
async fn scenario_coord_kill_pre_decision() -> Result<bool, String> {
    let dir = fixture::fresh_dir("eg-xshard-modal", "recoverabort");
    let backend = fixture::open_backend(&dir)?;
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
        multi.shutdown().await;
    }
    backend.shutdown();
    drop(backend);

    let (backend2, multi2, _state2, resolved, modal) = reopen_and_recover(&dir).await?;

    let redb = backend2.as_redb().ok_or("redb")?;
    let prepares_cleared = redb
        .xshard_scan_prepares()
        .map_err(|e| e.to_string())?
        .is_empty();

    multi2.shutdown().await;
    run_scenario_cleanup(&dir, remove_scenario_dir).await;

    // Single decision = ABORT (presumed): NEITHER modality landed, prepares cleared.
    Ok(resolved == 1
        && modal.is_atomic()
        && !modal.a_present
        && !modal.b_present
        && prepares_cleared)
}

/// GOC-13 — a participant whose GraphSlice carries a STALE placement fencing
/// (captured before a placement cutover bumped the epoch) is REJECTED, never
/// durably prepared or applied. Distinct from `scenario_participant_kill` (an
/// unreachable group): here the group is perfectly healthy and reachable, but the
/// PLACEMENT it was resolved against has moved on — the exact race the crash-vs-
/// prepare window can produce (a slice built against route epoch E, but by the time
/// its prepare actually lands the catalog has cut over to E+1).
///
/// Modeled directly against [`CrossShardCoordinator`] (bypassing
/// `handlers::txn::commit_cross_shard`, which always re-reads the CURRENT route at
/// build time and therefore cannot itself construct a stale slice) so the race
/// window can be deterministically reproduced without needing to win an actual
/// timing race.
async fn scenario_stale_fenced_participant_rejected() -> Result<bool, String> {
    let dir = fixture::fresh_dir("eg-xshard-modal", "stalefence");
    let backend = fixture::open_backend(&dir)?;
    let (multi, coord, state) = bring_up(&dir, backend.clone()).await;
    await_default_placement_leader(&multi).await?;
    let cutover = cut_over_modal_a_placement(&multi).await?;

    // Build the txn with GRAPH_A's slice carrying the NOW-STALE epoch1/fence.
    let stale_txn = fenced_modality_txn(
        "t-modal-stale-fence",
        "n-stale",
        cutover.stale_epoch,
        cutover.stale_fence,
    );
    let stale_rejected = commit_is_rejected_cleanly(&coord, &backend, &state, &stale_txn).await?;

    // Contrast (PASS-on-good): the SAME shape of txn, but carrying the CURRENT
    // epoch2/fence, commits normally — proving the rejection above is specifically
    // about staleness, not a general break in participant A.
    let route2 = multi.route_graph(GRAPH_A).await;
    let current_fence = route2.placed.then_some(route2.fencing_token());
    let fresh_txn = fenced_modality_txn(
        "t-modal-fresh-fence",
        "n-fresh",
        cutover.current_epoch,
        current_fence,
    );
    let (fresh_outcome, fresh_modal) = commit_and_read_back(&coord, &state, &fresh_txn).await?;
    let fresh_accepted =
        fresh_outcome == TxnOutcome::Committed && fresh_modal == ModalityState::BOTH;

    multi.shutdown().await;
    run_scenario_cleanup(&dir, remove_scenario_dir).await;

    Ok(stale_rejected && fresh_accepted)
}

/// Placement admin (`placement_assign`) commits through the DEFAULT group
/// (auto-created by `ensure_group` on first use), which — like GROUP_A/GROUP_B
/// in `bring_up` — needs its own election tick before `client_write_group` can
/// find a leader. Wait for it explicitly, the same way `bring_up` does for A/B.
async fn await_default_placement_leader(multi: &MultiRaft) -> Result<(), String> {
    multi.ensure_group(super::DEFAULT_GROUP).await?;
    let default_group = multi
        .group(super::DEFAULT_GROUP)
        .await
        .ok_or("default placement group missing after ensure_group")?;
    fixture::wait_until(Duration::from_secs(15), || {
        let g = default_group.clone();
        async move { g.current_leader().await == Some(1u64) }
    })
    .await
    .map_err(|_| "default placement group must elect a leader".to_string())
}

/// The route a coordinator captured before a placement cutover, and the epoch
/// the catalog moved to afterwards.
struct PlacementCutover {
    stale_epoch: u64,
    stale_fence: Option<u64>,
    current_epoch: u64,
}

/// Assign modalA → group A, capture that route, then cut over (re-assign the
/// SAME group — `plan_assign` always allocates a fresh epoch even when the group
/// is unchanged, exactly like a real move/split/merge would): the catalog is
/// then at a STRICTLY NEWER epoch than the captured route.
async fn cut_over_modal_a_placement(multi: &MultiRaft) -> Result<PlacementCutover, String> {
    let stale_epoch = multi.placement_assign(GRAPH_A, GROUP_A).await?;
    let route1 = multi.route_graph(GRAPH_A).await;
    if !route1.placed || route1.epoch != stale_epoch {
        return Err("unexpected initial placement route in stale-fence setup".into());
    }
    let current_epoch = multi.placement_assign(GRAPH_A, GROUP_A).await?;
    if current_epoch <= stale_epoch {
        return Err("placement re-assignment did not bump the epoch".into());
    }
    Ok(PlacementCutover {
        stale_epoch,
        stale_fence: Some(route1.fencing_token()),
        current_epoch,
    })
}

/// Commit `txn` and read back both modalities.
async fn commit_and_read_back(
    coord: &CrossShardCoordinator,
    state: &Arc<RwLock<crate::server::ServerState>>,
    txn: &CrossShardTxn,
) -> Result<(TxnOutcome, ModalityState), String> {
    let outcome = coord.commit_cross_shard(txn).await?;
    Ok((outcome, ModalityState::read(state).await))
}

/// The txn aborts with neither modality applied and no durable prepare leaked.
async fn commit_is_rejected_cleanly(
    coord: &CrossShardCoordinator,
    backend: &fixture::Backend,
    state: &Arc<RwLock<crate::server::ServerState>>,
    txn: &CrossShardTxn,
) -> Result<bool, String> {
    let (outcome, modal) = commit_and_read_back(coord, state, txn).await?;
    let redb = backend.as_redb().ok_or("redb")?;
    let no_leaked_prepares = redb
        .xshard_scan_prepares()
        .map_err(|e| e.to_string())?
        .is_empty();
    Ok(outcome == TxnOutcome::Aborted && modal == ModalityState::NEITHER && no_leaked_prepares)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_rendezvous::meet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;

    #[tokio::test(flavor = "current_thread")]
    async fn scenario_cleanup_runs_once_off_runtime_and_is_awaited() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));

        let cleanup = {
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                run_scenario_cleanup("barrier-probe", move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    meet(&entered, "cleanup reached its blocking work");
                    meet(&release, "cleanup released by the test");
                    Ok(())
                })
                .await;
            })
        };

        let entered_wait = entered.clone();
        ::tokio::task::spawn_blocking(move || meet(&entered_wait, "test saw cleanup enter"))
            .await
            .expect("barrier entry wait joins");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            !cleanup.is_finished(),
            "cleanup must await its blocking work"
        );

        ::tokio::task::spawn_blocking(move || meet(&release, "test released cleanup"))
            .await
            .expect("barrier release wait joins");
        cleanup.await.expect("cleanup task joins");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scenario_cleanup_io_and_join_errors_remain_best_effort() {
        let calls = Arc::new(AtomicUsize::new(0));
        let io_calls = calls.clone();
        run_scenario_cleanup("io-error-probe", move |_| {
            io_calls.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("expected cleanup failure"))
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let join_calls = calls.clone();
        run_scenario_cleanup("join-error-probe", move |_| {
            join_calls.fetch_add(1, Ordering::SeqCst);
            panic!("expected cleanup worker failure");
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// The umbrella proof — runs all four scenarios (CONCEPT:EG-KG.txn.crossshard-2pc-modality-harness).
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn crossshard_modality_2pc_single_decision_eg396() {
        // Held for the whole test: two of the four scenarios below
        // (`scenario_coord_kill_post_decision`, `scenario_coord_kill_pre_decision`)
        // open a backend TWICE each (initial + a restart reopen of the SAME dir) via
        // `fresh_dir`'s ambient-env provisioning, and both opens in each pair must
        // resolve the same encryption-at-rest cipher. `prove_crossshard_modality_2pc_
        // single_decision` is also called from a non-`cfg(test)` `--features cluster`
        // integration-test entrypoint, so it cannot hold this crate-private lock
        // itself; the in-crate test wrapper (here) is the right place. See
        // `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let report = prove_crossshard_modality_2pc_single_decision()
            .await
            .expect("cross-shard modality 2PC proof runs");
        assert!(report.all_atomic(), "all scenarios atomic: {report:?}");
    }

    /// Scenario 1 in isolation — happy commit spans both modalities atomically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn happy_commit_spans_both_modalities() {
        // Opens a durable store, so the ambient encryption env must hold still for
        // this whole body. READ guard: it excludes only a key MUTATOR, never another
        // opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        assert!(scenario_happy().await.expect("happy scenario"));
    }

    /// Scenario 2 in isolation — participant kill during prepare, no partial commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn participant_kill_aborts_no_partial_modality() {
        // Opens a durable store, so the ambient encryption env must hold still for
        // this whole body. READ guard: it excludes only a key MUTATOR, never another
        // opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        assert!(scenario_participant_kill().await.expect("kill scenario"));
    }

    /// Scenario 3 in isolation — coordinator kill after COMMIT decision → recover commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coord_kill_post_decision_recovers_commit_both_modalities() {
        // Held for the whole test: `scenario_coord_kill_post_decision` opens the
        // backend TWICE (initial + a restart reopen of the SAME dir); both opens
        // must resolve the same encryption-at-rest cipher. See
        // `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        assert!(scenario_coord_kill_post_decision()
            .await
            .expect("recover-commit scenario"));
    }

    /// Scenario 4 in isolation — coordinator kill before decision → recover abort.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coord_kill_pre_decision_recovers_abort_neither_modality() {
        // Held for the whole test: `scenario_coord_kill_pre_decision` opens the
        // backend TWICE (initial + a restart reopen of the SAME dir); both opens
        // must resolve the same encryption-at-rest cipher. See
        // `crate::crypto::acquire_test_env_lock`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        assert!(scenario_coord_kill_pre_decision()
            .await
            .expect("recover-abort scenario"));
    }

    /// GOC-13 — a participant presenting a STALE placement epoch/fence is rejected
    /// (no partial commit, no leaked prepare), while the identical txn shape at the
    /// CURRENT epoch commits normally.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_fenced_participant_is_rejected_current_epoch_accepted() {
        // Opens a durable store, so the ambient encryption env must hold still for
        // this whole body. READ guard: it excludes only a key MUTATOR, never another
        // opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        assert!(scenario_stale_fenced_participant_rejected()
            .await
            .expect("stale-fence rejection scenario"));
    }
}
