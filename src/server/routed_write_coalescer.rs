//! Per-graph batching worker for the five coalescable `GATEWAY_ROUTED`
//! structural writes (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`/
//! `CompareAndSetNodeFields`) —
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
//! per-graph [`RoutedGraphWriter`]. Admission assigns a monotonic ticket while
//! linearizing the bounded `try_send`; [`run_worker`] drains up to `max_batch`
//! queued jobs (with the same greedy-drain + short linger shape as
//! `write_coalescer::run_worker`), acquires `mutation_batch::lock_graph` for the
//! WHOLE flushed batch, and runs each job's sequence to completion, in ticket
//! order, one at a time, before releasing the lock once. A full/closed queue
//! rejects the new request with explicit `BUSY`; it never executes that request
//! inline, because an inline overflow path could overtake an already accepted
//! ticket. Durable commits are NOT merged across ops (each job's own
//! `commit_mutation_batch` call keeps its own principal/tenant/idempotency-key/
//! audit/CDC — merging those across different callers would misattribute
//! provenance and is deliberately never done here); the batching win is
//! `lock_graph` ACQUISITIONS (⌈N / max_batch⌉ instead of N), not durable writes.
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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::protocol::Response;
use crate::write_coalescer::{
    operations_applied, queue_admitted, queue_released, BatchStats, CoalescerConfig,
};

/// Keep a panic-isolating child future from outliving the graph worker if the
/// worker is canceled (for example during shutdown). Dropping a bare Tokio
/// `JoinHandle` detaches its task; detaching a commit sequence could let it
/// mutate RAM after the worker released `lock_graph`.
struct AbortOnDrop<T> {
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let handle = this
            .handle
            .as_mut()
            .expect("abort-on-drop join handle polled after completion");
        let polled = std::pin::Pin::new(handle).poll(cx);
        if polled.is_ready() {
            this.handle.take();
        }
        polled
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// One queued unit of work: a boxed `'static` future that, when polled, runs
/// the WHOLE `mutation::commit_mutation_body` sequence for one coalescable op
/// (detached from the original request's borrowed `MutationCtx` — see
/// `mutation::commit_via_coalescer`), plus the oneshot the original caller is
/// awaiting for its `Response`.
pub struct RoutedCommitJob {
    run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>>,
    reply: oneshot::Sender<Response>,
    request_id: u64,
}

impl RoutedCommitJob {
    pub fn new(
        run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>>,
        reply: oneshot::Sender<Response>,
    ) -> Self {
        Self {
            run,
            reply,
            request_id: 0,
        }
    }

    /// Bind the request id used if the worker catches a panic in this job. The
    /// default constructor remains useful for source-level/unit tests that only
    /// care about ordering.
    pub fn with_request_id(mut self, request_id: u64) -> Self {
        self.request_id = request_id;
        self
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
    tx: mpsc::Sender<(u64, RoutedCommitJob)>,
    /// Serializes ticket assignment with `try_send`, making the channel's FIFO
    /// order an explicit linearization order across concurrent producers.
    admission: std::sync::Mutex<AdmissionState>,
    config: CoalescerConfig,
    stats: Arc<BatchStats>,
}

#[derive(Debug, Default)]
struct AdmissionState {
    next_ticket: u64,
}

impl RoutedGraphWriter {
    /// Spawn the drain worker for `graph_name` (one Tokio task that owns the
    /// receiver and is the sole taker of `lock_graph(graph_name)` on behalf of
    /// every job it flushes).
    pub fn spawn(graph_name: String, config: CoalescerConfig) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<(u64, RoutedCommitJob)>(config.queue_capacity);
        let stats = Arc::new(BatchStats::default());
        tokio::spawn(run_worker(graph_name, rx, config, stats.clone()));
        Arc::new(Self {
            tx,
            admission: std::sync::Mutex::new(AdmissionState::default()),
            config,
            stats,
        })
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
    ///   gone. The caller must drop the job and return explicit `BUSY`; it must
    ///   not execute the job inline. Accepted tickets are the sole ordering
    ///   authority for this graph, so rejected work can never overtake them.
    pub fn try_enqueue(&self, job: RoutedCommitJob) -> Result<(), RoutedCommitJob> {
        let mut admission = self
            .admission
            .lock()
            .expect("routed write coalescer admission mutex poisoned");
        if admission.next_ticket == u64::MAX {
            return Err(job);
        }
        let ticket = admission.next_ticket;
        let queued_bytes = job.approx_bytes();
        queue_admitted(queued_bytes);
        match self.tx.try_send((ticket, job)) {
            Ok(()) => {
                admission.next_ticket = ticket + 1;
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full((_, job)))
            | Err(mpsc::error::TrySendError::Closed((_, job))) => {
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
    mut rx: mpsc::Receiver<(u64, RoutedCommitJob)>,
    config: CoalescerConfig,
    stats: Arc<BatchStats>,
) {
    let mut batch: Vec<RoutedCommitJob> = Vec::with_capacity(config.max_batch);
    let mut next_ticket = 0u64;
    while let Some((ticket, first)) = rx.recv().await {
        queue_released(first.approx_bytes());
        assert_eq!(
            ticket, next_ticket,
            "routed write coalescer admission order must be contiguous"
        );
        next_ticket = next_ticket
            .checked_add(1)
            .expect("routed write coalescer ticket overflow");
        batch.push(first);

        // Greedily pull everything already queued (no await) up to max_batch —
        // the common firehose case where producers are ahead of the worker.
        while batch.len() < config.max_batch {
            match rx.try_recv() {
                Ok((ticket, job)) => {
                    queue_released(job.approx_bytes());
                    assert_eq!(
                        ticket, next_ticket,
                        "routed write coalescer admission order must be contiguous"
                    );
                    next_ticket = next_ticket
                        .checked_add(1)
                        .expect("routed write coalescer ticket overflow");
                    batch.push(job);
                }
                Err(_) => break,
            }
        }

        // If we only got the one job, linger briefly to let a concurrent burst
        // land in the same lock acquisition — but never longer than
        // max_linger, so a lone write is essentially undelayed.
        if batch.len() == 1 && config.max_linger > Duration::ZERO {
            if let Ok(Some((ticket, job))) =
                tokio::time::timeout(config.max_linger, rx.recv()).await
            {
                queue_released(job.approx_bytes());
                assert_eq!(
                    ticket, next_ticket,
                    "routed write coalescer admission order must be contiguous"
                );
                next_ticket = next_ticket
                    .checked_add(1)
                    .expect("routed write coalescer ticket overflow");
                batch.push(job);
                while batch.len() < config.max_batch {
                    match rx.try_recv() {
                        Ok((ticket, job)) => {
                            queue_released(job.approx_bytes());
                            assert_eq!(
                                ticket, next_ticket,
                                "routed write coalescer admission order must be contiguous"
                            );
                            next_ticket = next_ticket
                                .checked_add(1)
                                .expect("routed write coalescer ticket overflow");
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
    // A caller that cancels before the worker starts has not crossed the durable
    // admission boundary. Drop that job without polling it; later tickets still
    // drain in order. Cancellation after this check is deliberately not
    // interruptible: once the job starts, commit-before-ack requires completing
    // the durable+RAM sequence even if its response receiver disappears.
    let batch: Vec<RoutedCommitJob> = batch
        .into_iter()
        .filter(|job| !job.reply.is_closed())
        .collect();
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
            let RoutedCommitJob {
                run,
                reply,
                request_id,
            } = job;
            // Isolate a malformed/test job panic at this worker boundary. A
            // panic must produce an error response (and release this batch's
            // lock), not kill the sole graph drain and strand later tickets.
            let response = match AbortOnDrop::new(tokio::spawn(run)).await {
                Ok(response) => response,
                Err(error) => {
                    tracing::error!(?error, "routed write coalescer job panicked");
                    Response::err(
                        request_id,
                        "routed write worker crashed before acknowledging the write",
                    )
                }
            };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn job(
        id: u64,
        seen: &Arc<std::sync::Mutex<Vec<u64>>>,
    ) -> (RoutedCommitJob, oneshot::Receiver<Response>) {
        let (reply, response) = oneshot::channel();
        let seen = seen.clone();
        let run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> =
            Box::pin(async move {
                seen.lock().expect("test order mutex poisoned").push(id);
                Response::err(id, "test-complete")
            });
        (
            RoutedCommitJob::new(run, reply).with_request_id(id),
            response,
        )
    }

    /// A full bounded queue rejects the new ticket. The rejected future is never
    /// run inline, so it cannot overtake the two tickets already admitted ahead of
    /// it; their execution order remains the declared FIFO order.
    #[tokio::test(flavor = "current_thread")]
    async fn saturated_queue_rejects_without_scheduled_overtaking() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = RoutedGraphWriter::spawn(
            "ne-169-saturated-order".into(),
            CoalescerConfig {
                max_batch: 1,
                queue_capacity: 2,
                max_linger: Duration::ZERO,
            },
        );

        let (first, first_rx) = job(1, &seen);
        let (second, second_rx) = job(2, &seen);
        let (third, third_rx) = job(3, &seen);
        assert!(writer.try_enqueue(first).is_ok());
        assert!(writer.try_enqueue(second).is_ok());
        let rejected = writer.try_enqueue(third);
        assert!(
            rejected.is_err(),
            "third ticket must receive BUSY/backpressure"
        );
        drop(rejected);
        assert!(
            third_rx.await.is_err(),
            "rejected job must never be acknowledged"
        );

        assert_eq!(first_rx.await.unwrap().id, 1);
        assert_eq!(second_rx.await.unwrap().id, 2);
        assert_eq!(
            *seen.lock().expect("test order mutex poisoned"),
            vec![1, 2],
            "only accepted tickets run, in admission order"
        );
    }

    /// A caller may cancel a queued request before the worker starts it. That ticket
    /// is dropped without polling its future, while a later accepted ticket still
    /// drains. This is the cancellation boundary before durable admission.
    #[tokio::test(flavor = "current_thread")]
    async fn canceled_queued_job_does_not_run_and_later_ticket_drains() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = RoutedGraphWriter::spawn(
            "ne-169-cancel-boundary".into(),
            CoalescerConfig {
                max_batch: 3,
                queue_capacity: 3,
                max_linger: Duration::ZERO,
            },
        );
        let (first, first_rx) = job(1, &seen);
        let (canceled, canceled_rx) = job(2, &seen);
        let (barrier, barrier_rx) = job(3, &seen);
        assert!(writer.try_enqueue(first).is_ok());
        assert!(writer.try_enqueue(canceled).is_ok());
        assert!(writer.try_enqueue(barrier).is_ok());
        drop(canceled_rx);

        assert_eq!(first_rx.await.unwrap().id, 1);
        assert_eq!(barrier_rx.await.unwrap().id, 3);
        assert_eq!(
            *seen.lock().expect("test order mutex poisoned"),
            vec![1, 3],
            "a canceled pre-start ticket must not mutate RAM or durable state"
        );
    }

    /// A panic in one accepted job is converted into an error response and does not
    /// kill the sole graph worker. The next ticket remains executable, which is the
    /// worker-crash boundary required for forward progress.
    #[tokio::test]
    async fn panicked_job_releases_worker_for_later_ticket() {
        let writer = RoutedGraphWriter::spawn(
            "ne-169-crash-boundary".into(),
            CoalescerConfig {
                max_batch: 1,
                queue_capacity: 2,
                max_linger: Duration::ZERO,
            },
        );
        let (panic_reply, panic_rx) = oneshot::channel();
        let panic_run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> =
            Box::pin(async { panic!("adversarial routed job panic") });
        assert!(writer
            .try_enqueue(RoutedCommitJob::new(panic_run, panic_reply).with_request_id(11))
            .is_ok());
        let (next, next_rx) = job(12, &Arc::new(std::sync::Mutex::new(Vec::new())));
        assert!(writer.try_enqueue(next).is_ok());

        let panic_response = panic_rx
            .await
            .expect("panic must be converted to a response");
        assert_eq!(panic_response.id, 11);
        assert!(panic_response
            .error
            .as_deref()
            .is_some_and(|error| error.contains("crashed")));
        assert_eq!(next_rx.await.unwrap().id, 12);
    }

    /// Once an accepted job starts, cancellation of its response receiver cannot
    /// interrupt the durable-before-ack sequence. Reopening a fresh graph writer
    /// then sees the same durable marker and can continue the ordered stream.
    #[tokio::test]
    async fn accepted_job_commits_before_ack_and_reopened_writer_continues() {
        let graph = "ne-169-restart-boundary";
        let durable = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        let started = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(tokio::sync::Notify::new());
        let writer = RoutedGraphWriter::spawn(
            graph.into(),
            CoalescerConfig {
                max_batch: 1,
                queue_capacity: 2,
                max_linger: Duration::ZERO,
            },
        );
        let (reply, response) = oneshot::channel();
        let durable_for_job = durable.clone();
        let started_for_job = started.clone();
        let finished_for_job = finished.clone();
        let run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> =
            Box::pin(async move {
                durable_for_job
                    .lock()
                    .expect("test durable mutex poisoned")
                    .push(1);
                started_for_job.notify_one();
                tokio::task::yield_now().await;
                finished_for_job.notify_one();
                Response::err(21, "test-complete")
            });
        assert!(writer
            .try_enqueue(RoutedCommitJob::new(run, reply).with_request_id(21))
            .is_ok());
        started.notified().await;
        drop(response);
        finished.notified().await;
        assert_eq!(
            *durable.lock().expect("test durable mutex poisoned"),
            vec![1],
            "accepted work commits even when its response receiver is canceled"
        );
        drop(writer);

        let reopened = RoutedGraphWriter::spawn(
            graph.into(),
            CoalescerConfig {
                max_batch: 1,
                queue_capacity: 1,
                max_linger: Duration::ZERO,
            },
        );
        let durable_for_reopen = durable.clone();
        let (reply, response) = oneshot::channel();
        let run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> =
            Box::pin(async move {
                durable_for_reopen
                    .lock()
                    .expect("test durable mutex poisoned")
                    .push(2);
                Response::err(22, "test-complete")
            });
        assert!(reopened
            .try_enqueue(RoutedCommitJob::new(run, reply).with_request_id(22))
            .is_ok());
        assert_eq!(response.await.unwrap().id, 22);
        assert_eq!(
            *durable.lock().expect("test durable mutex poisoned"),
            vec![1, 2],
            "a reopened worker must continue from the durable commit stream"
        );
    }
}
