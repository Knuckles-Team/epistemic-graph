//! The Nemesis: a fault scheduler against a running [`Cluster`] (CONCEPT:KG-2.212).
//!
//! Four fault primitives, exactly as the lane spec calls for:
//!
//! * **Kill** — `kill -9` a node mid-write ([`Cluster::kill`]: drop its Raft + redb
//!   backend; the on-disk redb is what a crash leaves).
//! * **Partition** — drop the group-tagged TCP frames between subsets, via the
//!   process-global [`network::partition`] gate the Raft network consults per-RPC.
//! * **Skew/Pause** — pause a node so it stops heartbeating (clock-skew analog): we
//!   isolate it from the network for a window, which is what a clock-skewed /
//!   GC-paused node looks like to its peers (missed heartbeats → election). Real
//!   monotonic-clock skew of a single tokio task is deferred (documented).
//! * **Saturate** — flood the write channel: the load gen's `rate=0` burst mode
//!   carries this; a [`Fault::SaturateWindow`] simply marks the window.
//!
//! The nemesis shares the cluster with the load gen via `Arc<Mutex<Cluster>>` and
//! locks it only for the actual kill/restart calls — never across a sleep — so the
//! load gen keeps driving writes through every fault window. A seeded RNG makes a
//! soak run reproducible.

use std::sync::Arc;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Mutex;

use super::super::network::partition;
use super::super::NodeId;
use super::cluster::Cluster;

/// A single scheduled fault.
#[derive(Debug, Clone)]
pub enum Fault {
    /// Kill the current leader, then (after `restart_after`) restart it.
    KillLeader { restart_after: Duration },
    /// Kill a specific node, then restart it.
    KillNode { id: NodeId, restart_after: Duration },
    /// Partition the cluster into `groups` for `hold`, then heal.
    Partition {
        groups: Vec<Vec<NodeId>>,
        hold: Duration,
    },
    /// Isolate (pause/skew analog) a node for `hold`, then heal.
    Isolate { id: NodeId, hold: Duration },
    /// Saturate window — the load gen's burst carries the actual flood.
    SaturateWindow { hold: Duration },
}

/// The result of injecting one fault (for the soak bin's running log).
#[derive(Debug, Clone)]
pub struct FaultReport {
    pub fault: String,
    /// Whether a leader was present again within the recovery window after the fault.
    pub recovered: bool,
    pub detail: String,
}

/// A nemesis with a seeded RNG for reproducible soak runs.
pub struct Nemesis {
    rng: StdRng,
}

impl Nemesis {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// How long to wait for the cluster to re-elect/heal after a fault before the
    /// checker runs. A quorum that still exists should re-elect well within this.
    const RECOVERY: Duration = Duration::from_secs(20);

    /// Inject one fault, never holding the cluster lock across a sleep, so the load
    /// gen keeps writing. Returns a report.
    pub async fn inject(&mut self, cluster: &Arc<Mutex<Cluster>>, fault: Fault) -> FaultReport {
        match fault {
            Fault::KillLeader { restart_after } => {
                let leader = {
                    let c = cluster.lock().await;
                    c.current_leader().await.filter(|l| c.is_running(*l))
                };
                match leader {
                    Some(l) => {
                        cluster.lock().await.kill(l).await.ok();
                        let recovered = wait_leader_excluding(cluster, l, Self::RECOVERY).await;
                        tokio::time::sleep(restart_after).await;
                        cluster.lock().await.restart(l).await.ok();
                        FaultReport {
                            fault: format!("KillLeader({l})"),
                            recovered,
                            detail: format!("killed leader {l}, restarted after {restart_after:?}"),
                        }
                    }
                    None => FaultReport {
                        fault: "KillLeader".into(),
                        recovered: wait_leader(cluster, Self::RECOVERY).await,
                        detail: "no running leader to kill".into(),
                    },
                }
            }
            Fault::KillNode { id, restart_after } => {
                cluster.lock().await.kill(id).await.ok();
                let recovered = wait_leader(cluster, Self::RECOVERY).await;
                tokio::time::sleep(restart_after).await;
                cluster.lock().await.restart(id).await.ok();
                FaultReport {
                    fault: format!("KillNode({id})"),
                    recovered,
                    detail: format!("killed node {id}, restarted after {restart_after:?}"),
                }
            }
            Fault::Partition { groups, hold } => {
                let refs: Vec<&[NodeId]> = groups.iter().map(|g| g.as_slice()).collect();
                partition::partition(&refs);
                tokio::time::sleep(hold).await;
                partition::heal();
                let recovered = wait_leader(cluster, Self::RECOVERY).await;
                FaultReport {
                    fault: "Partition".into(),
                    recovered,
                    detail: format!("partitioned {groups:?} for {hold:?}, healed"),
                }
            }
            Fault::Isolate { id, hold } => {
                partition::isolate(id);
                tokio::time::sleep(hold).await;
                partition::heal();
                let recovered = wait_leader(cluster, Self::RECOVERY).await;
                FaultReport {
                    fault: format!("Isolate({id})"),
                    recovered,
                    detail: format!("isolated {id} for {hold:?}, healed"),
                }
            }
            Fault::SaturateWindow { hold } => {
                tokio::time::sleep(hold).await;
                FaultReport {
                    fault: "SaturateWindow".into(),
                    recovered: true,
                    detail: format!("saturation window {hold:?}"),
                }
            }
        }
    }

    /// Pick a random fault for a soak run (a minority partition or a leader/node kill
    /// or an isolate), using the seeded RNG so a `--seed N` run is reproducible. All
    /// faults keep a quorum so writes can still commit (the no-loss bar is meaningful).
    pub fn random_fault(&mut self, cluster_ids: &[NodeId]) -> Fault {
        let ids = cluster_ids.to_vec();
        let n = ids.len();
        match self.rng.gen_range(0..4) {
            0 => Fault::KillLeader {
                restart_after: Duration::from_millis(self.rng.gen_range(500..2500)),
            },
            1 => {
                let id = ids[self.rng.gen_range(0..n)];
                Fault::KillNode {
                    id,
                    restart_after: Duration::from_millis(self.rng.gen_range(500..2500)),
                }
            }
            2 => {
                // A MINORITY partition keeps a quorum so writes can still commit.
                let minority = (n - 1) / 2;
                let mut shuffled = ids.clone();
                for i in (1..shuffled.len()).rev() {
                    let j = self.rng.gen_range(0..=i);
                    shuffled.swap(i, j);
                }
                let small: Vec<NodeId> = shuffled[..minority].to_vec();
                let big: Vec<NodeId> = shuffled[minority..].to_vec();
                Fault::Partition {
                    groups: vec![small, big],
                    hold: Duration::from_millis(self.rng.gen_range(1500..5000)),
                }
            }
            _ => {
                let id = ids[self.rng.gen_range(0..n)];
                Fault::Isolate {
                    id,
                    hold: Duration::from_millis(self.rng.gen_range(1500..5000)),
                }
            }
        }
    }
}

/// Wait until a leader is reported (lock briefly each poll, never across the sleep).
async fn wait_leader(cluster: &Arc<Mutex<Cluster>>, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        let got = {
            let c = cluster.lock().await;
            c.current_leader().await.filter(|l| c.is_running(*l))
        };
        if got.is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    false
}

async fn wait_leader_excluding(
    cluster: &Arc<Mutex<Cluster>>,
    excluded: NodeId,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        let got = {
            let c = cluster.lock().await;
            c.current_leader()
                .await
                .filter(|l| *l != excluded && c.is_running(*l))
        };
        if got.is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    false
}
