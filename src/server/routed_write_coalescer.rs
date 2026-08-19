//! Per-graph batching worker for the four coalescable `GATEWAY_ROUTED`
//! structural writes (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`) —
//! CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18 rewrite.
//!
//! ## Why this exists (and why it replaces the old design)
//!
//! The original coalescer (`crate::write_coalescer`) batched only the RAM
//! (topology) publish of a structural write. That was safe as long as the
//! CALLER held `mutation_batch::lock_graph` across BOTH its own durable commit
//! AND the coalesced RAM publish — which is exactly what
//! `mutation::commit_mutation_inner` did, and which made the coalescer's own
//! queue never hold more than one op at a time (the bug this rewrite fixes:
//! `stats().batches() == stats().ops()` always).
//!
//! The tempting fix — release the caller's lock around just the RAM-publish
//! enqueue+await, and have the coalescer's worker separately re-acquire
//! `lock_graph` around the RAM flush — is UNSAFE. It opens a window, between
//! "the caller's durable commit finished" and "the worker re-acquired the
//! lock to publish it to RAM", in which another `lock_graph` holder (most
//! severely `handlers::txn::commit_transaction`, which validates
//! `txn.validate(&core)`, durably commits, AND RAM-publishes atomically under
//! ONE lock hold) can run its entire sequence while `core` is still stale —
//! producing a RAM apply order that diverges from the durable commit order.
//! See `mutation::commit_coalescable_mutation`'s doc for the full account.
//!
//! ## What this module does instead
//!
//! [`RoutedWriteCoalescerRegistry`] queues a [`RoutedCommitJob`] per coalescable
//! op — a boxed `'static` future that runs `mutation::commit_mutation_body`'s
//! FULL prepare→durable-commit→RAM-publish sequence for that ONE op — onto a
//! per-graph [`RoutedGraphWriter`]. [`run_worker`] drains up to `max_batch`
//! queued jobs (with the same greedy-drain + short linger shape as
//! `write_coalescer::run_worker`), acquires `mutation_batch::lock_graph` for the
//! WHOLE flushed batch, and runs each job's sequence to completion, in FIFO
//! order, one at a time, before releasing the lock once. Durable commits are
//! NOT merged across ops (each job's own `commit_mutation_batch` call keeps its
//! own principal/tenant/idempotency-key/audit/CDC — merging those across
//! different callers would misattribute provenance and is deliberately never
//! done here); the batching win is `lock_graph` ACQUISITIONS (⌈N / max_batch⌉
//! instead of N), not durable writes.
//!
//! Because jobs run sequentially inside the SAME lock hold, and each job's own
//! `commit_mutation_body` call reads/writes `core` synchronously before the
//! next job starts, a later job's CDC pre-image capture and
//! `prepublish_success` checks always see an earlier job's already-applied RAM
//! state — closing the same-batch CDC-staleness gap alongside the
//! cross-caller OCC race.
//!
//! Each [`RoutedCommitJob`] carries its OWN captured `Arc<GraphCore>`, so —
//! unlike the old `write_coalescer::WriteCoalescerRegistry` — this registry
//! does not need a `remove`-on-delete correctness fix for a stale core: a
//! writer never touches `core` itself, only whatever each already-enqueued job
//! captured. `remove` is still provided for resource hygiene (so a
//! long-deleted graph's queue/worker task doesn't linger forever), not
//! correctness.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::protocol::Response;
use crate::write_coalescer::{
    operations_applied, queue_admitted, queue_released, BatchStats, CoalescerConfig,
};

/// One queued unit of work: a boxed `'static` future that, when polled, runs
/// the WHOLE `mutation::commit_mutation_body` sequence for one coalescable op
/// (detached from the original request's borrowed `MutationCtx` — see
/// `mutation::commit_via_coalescer`), plus the oneshot the original caller is
/// awaiting for its `Response`.
pub struct RoutedCommitJob {
    run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>>,
    reply: oneshot::Sender<Response>,
}

impl RoutedCommitJob {
    pub fn new(
        run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>>,
        reply: oneshot::Sender<Response>,
    ) -> Self {
        Self { run, reply }
    }

    /// Consume this job and run its sequence directly, WITHOUT sending on its
    /// reply channel — used by the backpressure/closed-worker fallback, whose
    /// caller awaits the returned `Response` itself instead of a oneshot.
    pub async fn into_future(self) -> Response {
        let response = self.run.await;
        // The fallback never enters `flush_batch`, so account for it on the
        // same process-wide operation signal as a queued job.
        operations_applied(1);
        response
    }

    /// Conservative queue footprint for aggregate telemetry.  The future's
    /// captured payload is intentionally opaque to this layer; counting the
    /// job envelope still gives a bounded lower-bound signal without walking
    /// arbitrary request data or allocating per-job metadata.
    fn approx_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

/// Per-graph queue + single worker task for coalescable routed-write jobs.
/// Cloneable via `Arc`; held in [`RoutedWriteCoalescerRegistry`].
pub struct RoutedGraphWriter {
    tx: mpsc::Sender<RoutedCommitJob>,
    config: CoalescerConfig,
    stats: Arc<BatchStats>,
}

impl RoutedGraphWriter {
    /// Spawn the drain worker for `graph_name` (one Tokio task that owns the
    /// receiver and is the sole taker of `lock_graph(graph_name)` on behalf of
    /// every job it flushes).
    pub fn spawn(graph_name: String, config: CoalescerConfig) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<RoutedCommitJob>(config.queue_capacity);
        let stats = Arc::new(BatchStats::default());
        tokio::spawn(run_worker(graph_name, rx, config, stats.clone()));
        Arc::new(Self { tx, config, stats })
    }

    /// Coalescing counters for this graph (batches vs ops). Mainly for tests /
    /// in-process diagnostics; the Prometheus counters are the operator surface.
    pub fn stats(&self) -> &Arc<BatchStats> {
        &self.stats
    }

    /// Try to enqueue `job` onto this graph's worker, WITHOUT blocking.
    ///
    /// * `Ok(())` — accepted; the worker will run its sequence (as part of a
    ///   batch) and reply on its own oneshot.
    /// * `Err(job)` — the bounded queue is full (backpressure) OR the worker is
    ///   gone: the job is handed BACK so the caller runs it inline itself
    ///   (`RoutedCommitJob::into_future`, under its own `lock_graph`
    ///   acquisition). Coalescing is an optimization, never a stall.
    pub fn try_enqueue(&self, job: RoutedCommitJob) -> Result<(), RoutedCommitJob> {
        let queued_bytes = job.approx_bytes();
        queue_admitted(queued_bytes);
        match self.tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(job))
            | Err(mpsc::error::TrySendError::Closed(job)) => {
                queue_released(queued_bytes);
                Err(job)
            }
        }
    }

    /// The active batch size, for diagnostics/tests.
    pub fn max_batch(&self) -> usize {
        self.config.max_batch
    }
}

/// The drain worker: receive jobs, batch them, run each batch's sequences
/// back-to-back inside ONE `mutation_batch::lock_graph` acquisition.
async fn run_worker(
    graph_name: String,
    mut rx: mpsc::Receiver<RoutedCommitJob>,
    config: CoalescerConfig,
    stats: Arc<BatchStats>,
) {
    let mut batch: Vec<RoutedCommitJob> = Vec::with_capacity(config.max_batch);
    while let Some(first) = rx.recv().await {
        queue_released(first.approx_bytes());
        batch.push(first);

        // Greedily pull everything already queued (no await) up to max_batch —
        // the common firehose case where producers are ahead of the worker.
        while batch.len() < config.max_batch {
            match rx.try_recv() {
                Ok(job) => {
                    queue_released(job.approx_bytes());
                    batch.push(job);
                }
                Err(_) => break,
            }
        }

        // If we only got the one job, linger briefly to let a concurrent burst
        // land in the same lock acquisition — but never longer than
        // max_linger, so a lone write is essentially undelayed.
        if batch.len() == 1 && config.max_linger > Duration::ZERO {
            if let Ok(Some(job)) = tokio::time::timeout(config.max_linger, rx.recv()).await {
                queue_released(job.approx_bytes());
                batch.push(job);
                while batch.len() < config.max_batch {
                    match rx.try_recv() {
                        Ok(job) => {
                            queue_released(job.approx_bytes());
                            batch.push(job);
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        flush_batch(&graph_name, std::mem::take(&mut batch), &stats).await;
        batch = Vec::with_capacity(config.max_batch);
    }
}

/// Run a whole batch's job sequences under ONE `lock_graph` acquisition,
/// strictly sequentially and in FIFO (enqueue) order — no job's
/// `commit_mutation_body` call starts until the previous one has fully
/// completed (durable commit AND RAM publish AND CDC emit), so:
///   * a later job's CDC pre-image / `prepublish_success` check in this SAME
///     batch always sees an earlier job's already-applied RAM state;
///   * per-graph write order is preserved (single worker, FIFO channel, no
///     concurrent execution within a batch);
///   * no other `lock_graph` holder (Transaction Commit, ApplyChangeEnvelope,
///     a non-coalescable gateway write, WorkItem commit, …) can interleave
///     ANY of its own durable-commit-then-RAM-publish sequence with this
///     batch's, because the lock is held continuously for the whole batch.
async fn flush_batch(graph_name: &str, batch: Vec<RoutedCommitJob>, stats: &BatchStats) {
    if batch.is_empty() {
        return;
    }
    let n = batch.len();
    // `tracing::Span` (unentered) is `Send`; `EnteredSpan` is NOT, so it cannot be
    // held across the `.await`s below (this function spans lock_graph AND every
    // job's own await chain, unlike the sync `write_coalescer::apply_batch` this
    // mirrors). `Instrument` attaches the span to every poll of this async fn's
    // own body without holding a non-Send guard live across an await point.
    async move {
        let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
        for job in batch {
            let RoutedCommitJob { run, reply } = job;
            let response = run.await;
            let _ = reply.send(response);
        }
        // One lock ACQUISITION covered `n` ops' durable-commit-then-RAM-publish
        // sequences — this is the contention win (`stats().batches() <
        // stats().ops()` under concurrent load), even though each op still issued
        // its own separate durable commit.
        stats.record(n);
        operations_applied(n);
    }
    .instrument(tracing::debug_span!(
        "routed_write_coalescer.flush_batch",
        graph = graph_name,
        batch = n
    ))
    .await;
}

/// Lazily-created per-graph writers, keyed by graph name. Mirrors
/// `write_coalescer::WriteCoalescerRegistry`'s shape but does not need a
/// `core` handle at all: each queued job already carries its own captured
/// `Arc<GraphCore>` (see module docs), so there is no "stale writer applies to
/// a deleted graph's orphaned core" hazard to guard against here.
pub struct RoutedWriteCoalescerRegistry {
    writers: DashMap<String, Arc<RoutedGraphWriter>>,
    config: CoalescerConfig,
}

impl RoutedWriteCoalescerRegistry {
    /// Build an always-on, hardware-sized bounded coalescer registry.
    pub fn new() -> Self {
        Self {
            writers: DashMap::new(),
            config: CoalescerConfig::auto(),
        }
    }

    /// Explicit constructor (tests): coalescing on, with the given config.
    pub fn with_config(config: CoalescerConfig) -> Self {
        Self {
            writers: DashMap::new(),
            config,
        }
    }

    /// Get (or lazily create) the writer for `graph_name`, spawning its worker
    /// on first use.
    pub fn writer_for(&self, graph_name: &str) -> Arc<RoutedGraphWriter> {
        if let Some(w) = self.writers.get(graph_name) {
            return w.clone();
        }
        self.writers
            .entry(graph_name.to_string())
            .or_insert_with(|| RoutedGraphWriter::spawn(graph_name.to_string(), self.config))
            .clone()
    }

    /// Drop the cached writer for `graph_name` (resource hygiene on graph
    /// delete — mirrors `write_coalescer::WriteCoalescerRegistry::remove`,
    /// called alongside it from `DeleteGraph` handling). Not required for
    /// correctness here (see module docs), only to avoid leaking a queue +
    /// worker task per historically-deleted graph name. No-op if no writer
    /// exists yet.
    pub fn remove(&self, graph_name: &str) {
        self.writers.remove(graph_name);
    }
}

impl Default for RoutedWriteCoalescerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
