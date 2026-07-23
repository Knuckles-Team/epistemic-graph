//! Correctness + load harness (CONCEPT:AU-KG.ontology.emits-database-ontology-entities) — the standing proof-engine.
//!
//! The authoritative-flip migration deadlock proved correctness-tests-passing ≠
//! scale/fault-proven. This harness is the standing proof-engine that gates every
//! distributed/durability claim in the Waves-4-8 program: multi-raft (A), M2
//! durability, distributed txns (N), PITR/replication (E). It does NOT unit-test the
//! happy path — it INJECTS faults against a live cluster and asserts invariants over
//! the resulting history.
//!
//! ## The pieces
//!
//! * [`cluster::Cluster`] — a managed N-node in-process redb-AUTHORITATIVE Raft
//!   cluster with kill/restart/partition controls.
//! * [`nemesis::Nemesis`] — the four fault primitives (kill / partition / skew-pause /
//!   saturate), schedulable and seeded.
//! * [`loadgen`] — concurrent writers recording an op [`history::History`].
//! * [`checker`] — the invariant model (acked-write-survives, single-leader-per-term,
//!   no-phantom) over the history + final cluster state.
//! * [`run_gauntlet`] — wires them: elect → load → fault → heal → assert.
//!
//! ## How a FUTURE distributed lane plugs in
//!
//! * **A new invariant** (e.g. lane N cross-shard atomicity, or E PITR-recovers-to-T):
//!   add a check arm in [`checker::check`] reading the relevant final state, and an
//!   accumulator like [`checker::LeadershipLog`] if it needs run-time samples. The
//!   load gen already records a full timestamped history to assert against.
//! * **A new nemesis** (e.g. lane resharding: split a group mid-write; E: kill a
//!   replication follower): add a [`nemesis::Fault`] variant + an arm in
//!   [`nemesis::Nemesis::inject`]. The partition gate ([`super::network::partition`])
//!   and the kill/restart cluster controls are reusable primitives.
//! * **A new gauntlet**: compose `Cluster::start` → `loadgen::run` → `Nemesis::inject`
//!   → `checker::check` exactly as [`run_gauntlet`] does, with the new fault schedule.
//!
//! ## Deliberately deferred (documented)
//!
//! * Full Knossos-style linearizability (per-key register history + a wall-clock
//!   real-time order check). This increment asserts the weaker but high-value
//!   acked-survives + single-leader-per-term + no-phantom set.
//! * Real monotonic-clock skew of one node (we model a paused/skewed node as a
//!   network isolation — peers see missed heartbeats, which is the observable effect).
//! * Multi-PROCESS (not in-process) nodes — this is a single-process harness; a true
//!   `kill -9` of a separate OS process is a later increment (the in-process kill drops
//!   the node's Raft + redb backend, leaving the on-disk redb a crash would leave).

pub mod checker;
pub mod cluster;
pub mod history;
pub mod loadgen;
pub mod nemesis;

#[cfg(test)]
mod gauntlet_test;

#[cfg(test)]
mod catchup_test;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use checker::{LeadershipLog, Verdict};
use cluster::Cluster;
use history::History;
use loadgen::LoadConfig;
use nemesis::{Fault, FaultReport, Nemesis};

/// The outcome of a full gauntlet run.
pub struct GauntletOutcome {
    pub verdict: Verdict,
    pub fault_reports: Vec<FaultReport>,
}

/// Run a bounded fault gauntlet against a fresh `n`-node cluster:
/// elect → start load → inject each fault in `faults` (while load runs) → heal →
/// assert the invariants. Returns the verdict + per-fault reports. Used by both the
/// deterministic test and the soak bin.
pub async fn run_gauntlet(
    n: usize,
    tag: &str,
    seed: u64,
    load: LoadConfig,
    faults: Vec<Fault>,
) -> Result<GauntletOutcome, String> {
    // ── Elect ──────────────────────────────────────────────────────────────
    let cluster = Cluster::start(n, tag).await?;
    let cluster = Arc::new(Mutex::new(cluster));
    {
        let c = cluster.lock().await;
        c.wait_for_leader(Duration::from_secs(15))
            .await
            .ok_or_else(|| "no initial leader elected".to_string())?;
    }

    let history = Arc::new(History::new());
    let leadership = Arc::new(Mutex::new(LeadershipLog::new()));
    let all_ids = cluster.lock().await.all_ids();

    // ── Start the load gen (background) ─────────────────────────────────────
    let load_handle = {
        let cluster = cluster.clone();
        let history = history.clone();
        tokio::spawn(async move { loadgen::run(cluster, load, history).await })
    };

    // ── Start the leadership sampler (background) — feeds the split-brain check ─
    let sampler_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler_handle = {
        let cluster = cluster.clone();
        let leadership = leadership.clone();
        let stop = sampler_stop.clone();
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let view = {
                    let c = cluster.lock().await;
                    c.leadership_view().await
                };
                leadership.lock().await.push(view);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    // ── Inject the fault schedule (concurrently with load) ──────────────────
    let mut nemesis = Nemesis::new(seed);
    let mut fault_reports = Vec::new();
    // Let some writes land first so the no-loss bar has acked writes to lose.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for fault in faults {
        let report = nemesis.inject(&cluster, fault).await;
        fault_reports.push(report);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // ── Heal + drain ────────────────────────────────────────────────────────
    super::network::partition::heal();
    let _ = load_handle.await;
    sampler_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = sampler_handle.await;

    // ── Wait for a final stable leader on the fully-healed cluster ──────────
    let final_leader = {
        let c = cluster.lock().await;
        c.wait_for_leader(Duration::from_secs(20)).await
    };
    // Give followers a beat to apply the last committed entries before reading.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // ── Check invariants ────────────────────────────────────────────────────
    let verdict = {
        let c = cluster.lock().await;
        let l = leadership.lock().await;
        checker::check(&c, &history, &l, final_leader).await
    };

    // ── Teardown ────────────────────────────────────────────────────────────
    let _ = all_ids;
    let cluster = Arc::try_unwrap(cluster)
        .map_err(|_| "cluster still shared at teardown".to_string())?
        .into_inner();
    cluster.teardown().await;

    Ok(GauntletOutcome {
        verdict,
        fault_reports,
    })
}
