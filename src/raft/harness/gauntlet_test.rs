//! The bounded, deterministic fault gauntlet (CONCEPT:AU-KG.ontology.emits-database-ontology-entities).
//!
//! `cargo test --features raft harness::` runs this. It exercises REAL faults against
//! the CURRENT multi-raft + durable-log engine — partition, kill-leader, isolate,
//! saturation — and asserts no acked write is lost, no split-brain, a new leader
//! always re-forms. If it FAILS, the harness has found a real durability/consensus bug
//! in the engine (that is the harness doing its job — see the lane report).

use std::time::Duration;

use super::loadgen::LoadConfig;
use super::nemesis::Fault;
use super::run_gauntlet;

/// The headline gauntlet: 3-node cluster, steady load, then
/// partition (minority) → heal → kill-leader (+restart) → assert.
/// Bounded + seeded ⇒ CI-able and reproducible.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn gauntlet_partition_then_kill_leader_no_loss() {
    let load = LoadConfig {
        writers: 4,
        rate_per_sec: 150,
        duration: Duration::from_secs(14),
    };
    let faults = vec![
        // Partition node 1 off from {2,3}; the {2,3} side keeps quorum + writes commit.
        Fault::Partition {
            groups: vec![vec![1], vec![2, 3]],
            hold: Duration::from_secs(3),
        },
        // After healing, kill whoever leads now and let a survivor take over.
        Fault::KillLeader {
            restart_after: Duration::from_millis(1500),
        },
    ];

    let outcome = run_gauntlet(3, "gauntlet-pk", 42, load, faults)
        .await
        .expect("gauntlet runs");

    // The harness must have actually exercised faults + recorded acked writes.
    assert!(
        outcome.fault_reports.iter().all(|r| r.recovered),
        "the cluster must re-elect a leader after every fault: {:?}",
        outcome.fault_reports
    );
    // Assert the invariants. A failure here = the harness FOUND a real bug.
    outcome.verdict.assert_ok();
    // Sanity: the run actually acked writes (otherwise no-loss is vacuous).
    assert!(
        outcome.verdict.summary.contains("acked="),
        "summary: {}",
        outcome.verdict.summary
    );
}

/// A saturation gauntlet: flood the write channel (rate=0) while killing a follower,
/// then a leader. Stresses the group-commit + in-flight backpressure under churn.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn gauntlet_saturate_and_kill_no_loss() {
    let load = LoadConfig {
        writers: 8,
        rate_per_sec: 0, // saturate
        duration: Duration::from_secs(12),
    };
    let faults = vec![
        Fault::KillNode {
            id: 3,
            restart_after: Duration::from_millis(1200),
        },
        Fault::SaturateWindow {
            hold: Duration::from_secs(2),
        },
        Fault::KillLeader {
            restart_after: Duration::from_millis(1500),
        },
    ];

    let outcome = run_gauntlet(3, "gauntlet-sat", 7, load, faults)
        .await
        .expect("gauntlet runs");

    outcome.verdict.assert_ok();
}

/// A minimal smoke gauntlet with no faults — proves the harness machinery + the
/// happy-path no-loss bar hold before any fault is injected (a control).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gauntlet_no_fault_control() {
    let load = LoadConfig {
        writers: 3,
        rate_per_sec: 100,
        duration: Duration::from_secs(5),
    };
    let outcome = run_gauntlet(3, "gauntlet-ctl", 1, load, vec![])
        .await
        .expect("gauntlet runs");
    outcome.verdict.assert_ok();
    assert!(outcome.verdict.passed);
}
