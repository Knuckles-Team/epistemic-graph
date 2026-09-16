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
    let delay = per_writer_delay(&cfg);

    let mut handles = Vec::new();
    for writer in 0..cfg.writers {
        let deadline = Instant::now() + cfg.duration;
        handles.push(tokio::spawn(run_writer(
            cluster.clone(),
            seq.clone(),
            stop.clone(),
            history.clone(),
            writer,
            deadline,
            delay,
        )));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Target per-op delay for one writer, spreading `cfg.rate_per_sec` across all
/// writers. Zero rate means saturation: no delay, no cap.
fn per_writer_delay(cfg: &LoadConfig) -> Duration {
    if cfg.rate_per_sec == 0 {
        Duration::ZERO
    } else {
        Duration::from_micros(1_000_000 * cfg.writers as u64 / cfg.rate_per_sec.max(1))
    }
}

/// One writer's op loop until `deadline` passes or `stop` is set: issue a write,
/// record its outcome, then pace to `delay` (or yield under saturation).
async fn run_writer(
    cluster: Arc<tokio::sync::Mutex<Cluster>>,
    seq: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    history: Arc<History>,
    writer: usize,
    deadline: Instant,
    delay: Duration,
) {
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        let s = seq.fetch_add(1, Ordering::Relaxed);
        let invoked = Instant::now();
        let outcome = issue_op(&cluster, s).await;
        let completed = Instant::now();
        history.record(Op {
            seq: s,
            writer,
            invoked,
            completed,
            outcome,
        });
        if delay > Duration::ZERO {
            tokio::time::sleep(delay).await;
        } else {
            // Saturation: still yield so other tasks (incl. the nemesis) run.
            tokio::task::yield_now().await;
        }
    }
}

/// Resolve the current leader and issue one write through it. Holds the cluster
/// lock only for the brief lookup+dispatch — the nemesis mutates the cluster
/// (kill/restart) from the same task that owns `&mut Cluster`, so this is the
/// only contention point; see [`run`]'s doc.
async fn issue_op(cluster: &tokio::sync::Mutex<Cluster>, s: u64) -> Outcome {
    let guard = cluster.lock().await;
    match guard.current_leader().await {
        Some(l) if guard.is_running(l) => match guard.write_via(l, s).await {
            Ok(()) => Outcome::Acked,
            Err(e) => Outcome::Err(e),
        },
        _ => Outcome::Err("no leader".into()),
    }
}
