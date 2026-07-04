//! Concurrent load generator (CONCEPT:AU-KG.ontology.emits-database-ontology-entities).
//!
//! Spawns `writers` tasks that issue durable AddNode mutations through the cluster's
//! CURRENT leader (re-resolving the leader each op, so a failover just reroutes the
//! next write) at a configurable rate, recording every (op, ack/err, timestamp) into
//! a shared [`History`]. Each op gets a globally-unique `seq` (so the checker can ask
//! "did acked seq S survive?"). A `saturate` knob removes the inter-op delay to flood
//! the write channel — the saturation nemesis.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::cluster::Cluster;
use super::history::{History, Op, Outcome};

/// Knobs for a load run.
#[derive(Clone)]
pub struct LoadConfig {
    /// Concurrent writer tasks.
    pub writers: usize,
    /// Target ops/sec ACROSS all writers (0 ⇒ as fast as possible = saturation).
    pub rate_per_sec: u64,
    /// How long to run.
    pub duration: Duration,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            writers: 4,
            rate_per_sec: 200,
            duration: Duration::from_secs(5),
        }
    }
}

/// Run the load against `cluster`, recording into `history`. Drives until
/// `cfg.duration` elapses OR `stop` is set. Returns when all writers drain.
///
/// The cluster is shared `&Arc<...>` read-only by the writers — the nemesis mutates
/// the cluster (kill/restart) from the SAME task that owns the `&mut Cluster`, so we
/// take an `Arc` clone of the *handles* needed for writes. To keep the borrow model
/// simple and faithful (the nemesis owns `&mut Cluster`), the load gen resolves the
/// leader + issues writes through a cheap leader lookup each op; killing the leader
/// just makes the next lookup return a survivor or `None` (recorded as an error).
pub async fn run(
    cluster: Arc<tokio::sync::Mutex<Cluster>>,
    cfg: LoadConfig,
    history: Arc<History>,
) {
    let seq = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let per_writer_delay = if cfg.rate_per_sec == 0 {
        Duration::ZERO
    } else {
        // Spread the target rate across writers.
        Duration::from_micros(1_000_000 * cfg.writers as u64 / cfg.rate_per_sec.max(1))
    };

    let mut handles = Vec::new();
    for w in 0..cfg.writers {
        let cluster = cluster.clone();
        let seq = seq.clone();
        let stop = stop.clone();
        let history = history.clone();
        let deadline = Instant::now() + cfg.duration;
        handles.push(tokio::spawn(async move {
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                let s = seq.fetch_add(1, Ordering::Relaxed);
                let invoked = Instant::now();
                // Resolve the current leader and issue the write. Hold the cluster
                // lock only for the brief lookup+dispatch — the nemesis acquires it
                // to kill/restart between ops.
                let outcome = {
                    let guard = cluster.lock().await;
                    match guard.current_leader().await {
                        Some(l) if guard.is_running(l) => match guard.write_via(l, s).await {
                            Ok(()) => Outcome::Acked,
                            Err(e) => Outcome::Err(e),
                        },
                        _ => Outcome::Err("no leader".into()),
                    }
                };
                let completed = Instant::now();
                history.record(Op {
                    seq: s,
                    writer: w,
                    invoked,
                    completed,
                    outcome,
                });
                if per_writer_delay > Duration::ZERO {
                    tokio::time::sleep(per_writer_delay).await;
                } else {
                    // Saturation: still yield so other tasks (incl. the nemesis) run.
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}
