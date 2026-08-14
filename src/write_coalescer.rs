//! Per-graph write coalescer — turns N concurrent single-op writes to ONE graph
//! into ONE topology-lock acquisition per batch (CONCEPT:EG-KG.sharding.per-graph-write-coalescer).
//!
//! ## Why
//! Every structural write to a graph (`add_node`/`add_edge`/CAS/…) opens a one-shot
//! [`GraphTxn`](crate::graph::GraphTxn), which acquires that graph's single
//! `topo.write()` lock for the duration of the op. DIFFERENT graphs already isolate
//! (each has its own lock — see `test_writers_to_distinct_graphs_do_not_serialize`),
//! but many writers to the SAME hot graph (the `__commons__` ingestion firehose)
//! still serialize one-op-at-a-time on that one lock: M writers → M lock
//! acquire/release cycles, each paying the lock + per-op ledger overhead, and the
//! control plane sharing that graph starves behind the ingestion stream.
//!
//! ## What
//! For each graph, a lazily-created [`GraphWriter`] owns a bounded
//! [`mpsc`](tokio::sync::mpsc) channel and a single worker task. Producers enqueue a
//! [`WriteOp`] (carrying a [`oneshot`] reply) instead of taking the lock themselves;
//! the worker drains up to `max_batch` queued ops, opens **ONE** `core.txn()`, applies
//! the whole batch under that single guard, then replies to each producer. So M
//! concurrent writers cost ⌈M / batch⌉ lock acquisitions instead of M — the
//! contention win — while each op still gets its individual result (CAS stays
//! exactly-once; an add_edge to a missing node still returns its own `Err`).
//!
//! ## Lazy, dynamic, NOT hardcoded
//! Writers live in a [`DashMap<String, Arc<GraphWriter>>`] keyed by graph name and
//! are created on first write to a name ([`WriteCoalescerRegistry::writer_for`]),
//! exactly like `ServerState::per_graph_inflight`. A brand-new graph — a future
//! per-connector channel, or `__commons__` — gets its own writer automatically with
//! **no code change and no hardcoded list**.
//!
//! ## Side-effects stay in the dispatch shell
//! The worker applies ONLY the in-memory mutation under the txn and returns its
//! outcome. `mark_dirty` / WAL append / size gauges remain centralized in
//! `dispatch::dispatch_graph_op`, run per-op against the returned outcome — so the
//! crash-consistency (WAL) and checkpoint (dirty) contracts are byte-for-byte
//! unchanged; only WHERE the lock is taken moved.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use crate::graph::GraphCore;

/// Process-local coalescer counters for one graph: lock acquisitions (batches) vs
/// the single-op writes those acquisitions applied. `ops / batches` is the average
/// batch size = the lock-acquisitions-saved ratio. Shared (`Arc`) between the worker
/// and the inline-fallback path so both are counted. Cheap relaxed atomics.
#[derive(Debug, Default)]
pub struct BatchStats {
    /// Topology-lock acquisitions on the coalesced path (= committed batches).
    pub batches: AtomicU64,
    /// Single-op writes applied across those batches.
    pub ops: AtomicU64,
}

impl BatchStats {
    pub fn record(&self, ops: usize) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.ops.fetch_add(ops as u64, Ordering::Relaxed);
    }
    pub fn batches(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }
}

/// One queued structural write against a graph, with a reply channel. These mirror
/// the five high-frequency single-op write methods that today each open their own
/// one-shot txn; multi-step writes (BatchUpdate, ClearGraph, reasoning, …) keep
/// their existing inline path — they already hold one txn for the whole operation,
/// so coalescing buys them nothing.
pub enum WriteOp {
    AddNode {
        node_id: String,
        properties_msgpack: Vec<u8>,
        reply: oneshot::Sender<WriteOutcome>,
    },
    RemoveNode {
        node_id: String,
        reply: oneshot::Sender<WriteOutcome>,
    },
    AddEdge {
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
        reply: oneshot::Sender<WriteOutcome>,
    },
    RemoveEdge {
        source_id: String,
        target_id: String,
        reply: oneshot::Sender<WriteOutcome>,
    },
    CompareAndSet {
        node_id: String,
        conditions: serde_json::Map<String, serde_json::Value>,
        updates: serde_json::Map<String, serde_json::Value>,
        reply: oneshot::Sender<WriteOutcome>,
    },
}

/// The result of applying one [`WriteOp`] under the batch txn, handed back to the
/// producer so the dispatch shell can reconstruct the exact same `Response` it would
/// have produced inline. The shell keys dirty/WAL off that `Response` (as it always
/// did), so durability/checkpoint semantics are identical to the inline path.
#[derive(Debug)]
pub enum WriteOutcome {
    /// Op succeeded (add_node/remove_node/remove_edge — they never fail).
    Ok,
    /// add_edge failed (missing endpoint): carries the same error string.
    Err(String),
    /// compare_and_set result (true = claimed, false = condition failed).
    Cas(bool),
    /// The writer worker is gone (channel/oneshot dropped). Treated as a transport
    /// error by the caller; should not happen in steady state.
    WriterGone,
}

/// Auto-sized coalescer tuning, derived from the host (NOT a per-connector knob).
#[derive(Debug, Clone, Copy)]
pub struct CoalescerConfig {
    /// Max ops applied under one txn before the worker commits and re-acquires the
    /// lock (bounds the worst-case time any reader waits behind a batch).
    pub max_batch: usize,
    /// Bounded per-graph queue depth (backpressure: a full queue makes
    /// `try_enqueue` fall back to the inline path rather than stall).
    pub queue_capacity: usize,
    /// Tiny linger after the first op in an empty batch, to let a burst coalesce.
    /// Auto-sized small (sub-millisecond default) so a lone write is not delayed.
    pub max_linger: Duration,
}

impl CoalescerConfig {
    /// Auto-size from cpu count. More cores mean more concurrent producers, so a
    /// larger batch amortizes the lock while all queues remain bounded.
    pub fn auto() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        // 8 ops/cpu, clamped to a sane window. At 8 cpus that's a 64-op batch —
        // big enough to collapse a firehose, small enough to keep p99 lock-hold low.
        let max_batch = (cpus * 8).clamp(16, 256);
        CoalescerConfig {
            max_batch,
            // Room for several batches in flight without unbounded growth.
            queue_capacity: (max_batch * 4).clamp(256, 4096),
            max_linger: Duration::from_micros(200),
        }
    }
}

impl Default for CoalescerConfig {
    fn default() -> Self {
        Self::auto()
    }
}

/// Per-graph write coalescer: a bounded channel + one drain worker over a graph's
/// [`GraphCore`]. Cloneable via `Arc`; held in [`WriteCoalescerRegistry`].
pub struct GraphWriter {
    // CONCEPT:EG-KG.compute.parse-resolve-span — the channel carries the op's enqueue `Instant` alongside it so
    // the worker can record `write_lock_wait` = (lock acquired − enqueued). Stamped in
    // `try_enqueue`/`apply_one_inline`, so producer call sites are unchanged.
    tx: mpsc::Sender<(Instant, WriteOp)>,
    config: CoalescerConfig,
    stats: Arc<BatchStats>,
}

impl GraphWriter {
    /// Spawn the drain worker for `core` (one Tokio task that owns the receiver and
    /// the only writer of this graph's topology lock on the coalesced path).
    pub fn spawn(graph_name: String, core: Arc<GraphCore>, config: CoalescerConfig) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<(Instant, WriteOp)>(config.queue_capacity);
        let stats = Arc::new(BatchStats::default());
        tokio::spawn(run_worker(graph_name, core, rx, config, stats.clone()));
        Arc::new(Self { tx, config, stats })
    }

    /// Coalescing counters for this graph (batches vs ops). Mainly for tests /
    /// in-process diagnostics; the Prometheus counters are the operator surface.
    pub fn stats(&self) -> &Arc<BatchStats> {
        &self.stats
    }

    /// Try to enqueue a pre-built op (its oneshot reply already wired) onto this
    /// graph's batch worker, WITHOUT blocking.
    ///
    /// * `Ok(())` — accepted; the worker will apply it under a batch txn and reply on
    ///   the op's oneshot.
    /// * `Err(op)` — the bounded queue is full (backpressure) OR the worker is gone:
    ///   the op (still owning its reply sender) is handed BACK so the caller applies
    ///   it inline and replies itself. Coalescing is an optimization, never a stall.
    pub fn try_enqueue(&self, op: WriteOp) -> Result<(), WriteOp> {
        // CONCEPT:EG-KG.compute.parse-resolve-span — stamp the enqueue instant here (the moment the producer
        // hands the op off) so the worker measures the true enqueue→acquire wait.
        match self.tx.try_send((Instant::now(), op)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full((_, op)))
            | Err(mpsc::error::TrySendError::Closed((_, op))) => Err(op),
        }
    }

    /// Apply ONE op inline (the backpressure fallback when the worker's queue is
    /// full): a single-op txn, identical to the engine's pre-coalescer convenience
    /// methods. The op's oneshot is replied to here too, so the submitter awaits the
    /// same channel whether the op was batched or applied inline. Counted in the
    /// same stats as the batched path.
    pub fn apply_one_inline(&self, core: &Arc<GraphCore>, graph_name: &str, op: WriteOp) {
        // CONCEPT:EG-KG.compute.parse-resolve-span — stamp now; the inline fallback acquires the lock essentially
        // immediately, so its recorded wait reflects only the (tiny) acquire cost.
        apply_batch(core, graph_name, vec![(Instant::now(), op)], &self.stats);
    }

    /// The active batch size, for diagnostics/tests.
    pub fn max_batch(&self) -> usize {
        self.config.max_batch
    }
}

/// The drain worker: receive ops, batch them, apply each batch under ONE txn.
async fn run_worker(
    graph_name: String,
    core: Arc<GraphCore>,
    mut rx: mpsc::Receiver<(Instant, WriteOp)>,
    config: CoalescerConfig,
    stats: Arc<BatchStats>,
) {
    // CONCEPT:EG-KG.compute.parse-resolve-span — each entry carries its enqueue instant (set by `try_enqueue`).
    let mut batch: Vec<(Instant, WriteOp)> = Vec::with_capacity(config.max_batch);
    while let Some(first) = rx.recv().await {
        batch.push(first);

        // Greedily pull everything already queued (no await) up to max_batch — the
        // common firehose case where producers are ahead of the worker.
        while batch.len() < config.max_batch {
            match rx.try_recv() {
                Ok(op) => batch.push(op),
                Err(_) => break,
            }
        }

        // If we only got the one op, linger briefly to let a concurrent burst land
        // in the same lock acquisition — but never longer than max_linger, so a lone
        // write is essentially undelayed.
        if batch.len() == 1 && config.max_linger > Duration::ZERO {
            if let Ok(Some(op)) = tokio::time::timeout(config.max_linger, rx.recv()).await {
                batch.push(op);
                while batch.len() < config.max_batch {
                    match rx.try_recv() {
                        Ok(op) => batch.push(op),
                        Err(_) => break,
                    }
                }
            }
        }

        apply_batch(&core, &graph_name, std::mem::take(&mut batch), &stats);
        batch = Vec::with_capacity(config.max_batch);
    }
}

/// Apply a whole batch under ONE `core.txn()` (one `topo.write()` acquisition),
/// replying on each op's oneshot. The single guard makes the batch atomic w.r.t.
/// other writers/readers — same as a single inline op, just amortized.
fn apply_batch(
    core: &Arc<GraphCore>,
    graph_name: &str,
    batch: Vec<(Instant, WriteOp)>,
    stats: &BatchStats,
) {
    if batch.is_empty() {
        return;
    }
    let n = batch.len();
    // CONCEPT:EG-KG.compute.parse-resolve-span — span the batch apply so the hot write path is a trace span
    // carrying the graph + batch size (mirrors the AST/ANN phase spans).
    let _span = tracing::debug_span!("write_coalescer.apply_batch", graph = graph_name, batch = n)
        .entered();
    let mut txn = core.txn(); // ← the SINGLE lock acquisition for the whole batch.
                              // CONCEPT:EG-KG.compute.parse-resolve-span — the topology write lock is now HELD. Record each op's wait
                              // (enqueue → acquire) = the starvation it paid behind the firehose, then time the
                              // hold window (acquire → release) = the contention readers/other writers block on.
    let acquired = Instant::now();
    for (enqueued, _) in &batch {
        crate::metrics::observe_write_lock_wait(
            graph_name,
            acquired.saturating_duration_since(*enqueued).as_secs_f64(),
        );
    }
    // CONCEPT:EG-KG.storage.write-changeset — collect the per-WriteOp delta for this batch so the
    // heavy secondary indexes can be maintained incrementally under this SAME lock.
    // CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal — a content-derived
    // server index (text/temporal) is registered on this graph, so capture each
    // added/updated node's property blob into the ChangeSet. The adapter reads its
    // field from the captured blob rather than re-reading `core` under this batch's
    // topology lock (which would deadlock). `false` (the vector-only default) keeps
    // the hot path zero-clone — no blob is cloned.
    let capture_content = core.wants_change_content();
    let mut change = crate::index::ChangeSet::new();
    for (_, op) in batch {
        match op {
            WriteOp::AddNode {
                node_id,
                properties_msgpack,
                reply,
            } => {
                if capture_content {
                    change
                        .added_nodes
                        .push(crate::index::NodeChange::with_properties(
                            node_id.clone(),
                            properties_msgpack.clone(),
                        ));
                } else {
                    change.record_add_node(node_id.clone());
                }
                txn.add_node(node_id, properties_msgpack);
                let _ = reply.send(WriteOutcome::Ok);
            }
            WriteOp::RemoveNode { node_id, reply } => {
                // Capture the property blob BEFORE deletion (W1.6/P7,
                // CONCEPT:EG-KG.storage.incremental-index-stamp) so incremental index maintenance removes
                // the id from exactly its label/property postings, and the dependency clock bumps
                // exactly the label/key dimensions it touched, instead of a coarse drop/floor. The
                // node is gone from the property store the instant `txn.remove_node` runs.
                match txn.get_node_properties(&node_id) {
                    Some(props) => {
                        change.record_remove_node_with_properties(node_id.clone(), props)
                    }
                    None => change.record_remove_node(node_id.clone()),
                }
                txn.remove_node(node_id.clone());
                // Node identity owns its vector in every indexing mode. The
                // incremental descriptor repeats this as an idempotent safety net.
                core.semantic_store.write().remove_embedding(&node_id);
                let _ = reply.send(WriteOutcome::Ok);
            }
            WriteOp::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
                reply,
            } => {
                // Only a SUCCESSFUL add-edge is a real change (a missing endpoint is
                // an Err that touches nothing), so record it after the outcome.
                let recorded = (source_id.clone(), target_id.clone());
                let outcome = match txn.add_edge(source_id, target_id, properties_msgpack) {
                    Ok(()) => {
                        change.record_add_edge(recorded.0, recorded.1);
                        WriteOutcome::Ok
                    }
                    Err(e) => WriteOutcome::Err(e),
                };
                let _ = reply.send(outcome);
            }
            WriteOp::RemoveEdge {
                source_id,
                target_id,
                reply,
            } => {
                change.record_remove_edge(source_id.clone(), target_id.clone());
                txn.remove_edge(source_id, target_id);
                let _ = reply.send(WriteOutcome::Ok);
            }
            WriteOp::CompareAndSet {
                node_id,
                conditions,
                updates,
                reply,
            } => {
                let ok = txn.compare_and_set_fields(&node_id, &conditions, &updates);
                // A lost CAS mutates nothing; only a won claim is a change.
                if ok {
                    if capture_content {
                        // Capture the CAS `updates` as the blob: a field-scoped content
                        // index (text/temporal) reads only its own field from it, so a
                        // CAS that touches that field re-indexes it and one that does not
                        // is a no-op for the index (its prior entry stays correct).
                        let blob =
                            rmp_serde::to_vec_named(&serde_json::Value::Object(updates.clone()))
                                .unwrap_or_default();
                        change.updated_nodes.push(
                            crate::index::NodeChange::with_properties_and_fields(
                                node_id,
                                blob,
                                updates.keys().cloned().collect(),
                            ),
                        );
                    } else {
                        change
                            .updated_nodes
                            .push(crate::index::NodeChange::with_fields(
                                node_id,
                                updates.keys().cloned().collect(),
                            ));
                    }
                }
                let _ = reply.send(WriteOutcome::Cas(ok));
            }
        }
    }
    // CONCEPT:EG-KG.storage.write-changeset — maintain the heavy secondary indexes (vector today;
    // text/temporal/derived-OWL via the same seam in future) BEFORE releasing the
    // topology lock, so a concurrent hybrid read sees the topology mutation and the
    // index moves together — never a torn index.
    if !change.is_empty() {
        core.maintain_indexes_at(
            &change,
            core.version().saturating_add(change.len() as u64),
            txn.node_count(),
            txn.edge_count(),
        );
    }
    drop(txn); // release the lock; reads + the next batch can proceed.
               // CONCEPT:EG-KG.compute.parse-resolve-span — hold = acquire → release: the window readers were blocked.
    crate::metrics::observe_write_lock_hold(graph_name, acquired.elapsed().as_secs_f64());
    stats.record(n);
    crate::metrics::write_batch_committed(graph_name, n);
}

/// Lazily-created per-graph writers, keyed by graph name. Mirrors
/// `ServerState::per_graph_inflight` (a `DashMap<String, Arc<_>>`): a write to a
/// graph that has no writer yet creates one on the spot — automatic per new
/// connector/graph, with no registration list.
pub struct WriteCoalescerRegistry {
    writers: DashMap<String, Arc<GraphWriter>>,
    config: CoalescerConfig,
}

impl WriteCoalescerRegistry {
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

    /// Get (or lazily create) the writer for `graph_name`, spawning its worker over
    /// `core` on first use. The `core` passed must be the same `Arc` for a given
    /// name, so a graph has exactly one writer over its one core.
    pub fn writer_for(&self, graph_name: &str, core: &Arc<GraphCore>) -> Arc<GraphWriter> {
        if let Some(w) = self.writers.get(graph_name) {
            return w.clone();
        }
        self.writers
            .entry(graph_name.to_string())
            .or_insert_with(|| {
                GraphWriter::spawn(graph_name.to_string(), core.clone(), self.config)
            })
            .clone()
    }

    /// Drop the cached writer for `graph_name` (CONCEPT:EG-KG.backend.many-repeated-create-delete). Called by
    /// `DeleteGraph` so a same-name recreate does NOT inherit the deleted
    /// incarnation's writer — whose worker task owns an `Arc<GraphCore>` of the OLD,
    /// now-orphaned core. Without this, `writer_for` would return the stale writer
    /// (it is keyed by name and IGNORES the `core` passed once an entry exists), and
    /// every write to the recreated graph would land on the deleted core — silently
    /// dropping the new tenant's in-memory writes. Removing the entry drops the only
    /// `Sender`; the worker's `rx.recv()` then returns `None` and the task exits,
    /// releasing its hold on the old core. The next write to the name lazily spawns a
    /// fresh writer over the NEW core. No-op if no writer exists yet.
    pub fn remove(&self, graph_name: &str) {
        self.writers.remove(graph_name);
    }
}

impl Default for WriteCoalescerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    fn node_props(k: i64) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({ "k": k })).unwrap()
    }

    async fn submit_node(writer: &Arc<GraphWriter>, id: &str, k: i64) -> WriteOutcome {
        let (reply, rx) = oneshot::channel();
        let op = WriteOp::AddNode {
            node_id: id.to_string(),
            properties_msgpack: node_props(k),
            reply,
        };
        // Mirror the dispatch fallback: inline-apply if the queue is full.
        if let Err(op) = writer.try_enqueue(op) {
            writer.apply_one_inline(&dummy_core(), "test", op);
        }
        rx.await.unwrap()
    }

    fn dummy_core() -> Arc<GraphCore> {
        // Only reached if try_enqueue ever fails in these tests (it shouldn't with an
        // ample queue) — never used to apply against a *different* core in practice.
        Arc::new(GraphCore::new())
    }

    /// N producers writing distinct nodes to ONE graph must (a) all land (no lost
    /// writes) and (b) be applied in FEWER topology-lock acquisitions than ops —
    /// i.e. the coalescer actually batched (the contention win), not one
    /// lock-per-op.
    ///
    /// GOC-70: the pile-up that makes batching happen is constructed
    /// DETERMINISTICALLY rather than hoped for from the scheduler. The original
    /// shape spawned 500 `tokio::spawn` tasks under `worker_threads = 4` and relied
    /// on the OS scheduler actually overlapping enough of them within the 2ms
    /// linger window to fill a batch — true on a lightly-loaded many-core host,
    /// false on a 2-core CI runner where four tokio workers contend for two cores
    /// and uncontended enqueues can drain one-by-one before the next is even
    /// submitted (the exact failure class that broke
    /// `dispatch_coalesces_concurrent_writes_to_one_graph` in 2.25.0). Here every
    /// op is enqueued from ONE synchronous loop on the test's own task, with no
    /// `.await` inside the loop: `try_enqueue` is a non-blocking `try_send`, so
    /// this task cannot be preempted mid-loop (tokio only yields a task at an
    /// await point) and the worker task — spawned separately — gets no
    /// opportunity to run until the loop finishes and this task starts awaiting
    /// replies. That guarantees all `N` ops sit in the queue before the worker
    /// drains its first batch, on ANY core count, including 1. The batching
    /// outcome is then a structural consequence of `max_batch` (500 ops /
    /// 128-per-batch ⇒ at most 4 batches), not a timing race.
    #[tokio::test]
    async fn concurrent_writes_coalesce_into_fewer_lock_acquisitions() {
        let core = Arc::new(GraphCore::new());
        // Big batch window so the deterministic pile-up (below) coalesces into few
        // batches. `max_linger` is irrelevant here — the queue is already full of
        // every op before the worker's first poll, so it never has to wait out a
        // linger window to see a full batch.
        let cfg = CoalescerConfig {
            max_batch: 128,
            queue_capacity: 4096,
            max_linger: Duration::from_millis(2),
        };
        let writer = GraphWriter::spawn("hot".into(), core.clone(), cfg);

        const N: usize = 500;
        let mut receivers = Vec::with_capacity(N);
        for i in 0..N {
            let (reply, rx) = oneshot::channel();
            let op = WriteOp::AddNode {
                node_id: format!("n{i}"),
                properties_msgpack: node_props(i as i64),
                reply,
            };
            if let Err(op) = writer.try_enqueue(op) {
                writer.apply_one_inline(&core, "hot", op);
            }
            receivers.push(rx);
        }
        for rx in receivers {
            assert!(matches!(rx.await.unwrap(), WriteOutcome::Ok));
        }

        // Correctness: every write landed exactly once.
        assert_eq!(core.node_count(), N, "all {N} writes must land");

        // The win: ops applied in strictly fewer lock acquisitions than ops. (If the
        // engine still took one lock per op, batches == ops.)
        let batches = writer.stats().batches();
        let ops = writer.stats().ops();
        assert_eq!(ops as usize, N, "stats must account for every op");
        assert!(
            batches < ops,
            "coalescer should batch: {ops} ops in {batches} lock acquisitions \
             (expected < {ops})",
        );
        // Sanity: average batch > 1.
        assert!(ops / batches.max(1) >= 2, "avg batch should be >= 2 ops");
    }

    /// CAS stays exactly-once under coalescing: many concurrent claimers of the SAME
    /// node, all batched under shared txns, must yield EXACTLY ONE winner — the
    /// claim-path invariant the control plane depends on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cas_is_exactly_once_under_coalescing() {
        let core = Arc::new(GraphCore::new());
        let cfg = CoalescerConfig {
            max_batch: 64,
            queue_capacity: 1024,
            max_linger: Duration::from_millis(2),
        };
        let writer = GraphWriter::spawn("claim".into(), core.clone(), cfg);

        // Seed an unclaimed task node.
        let seed = submit_node(&writer, "task:1", 0).await;
        assert!(matches!(seed, WriteOutcome::Ok));
        // Give it an explicit unclaimed marker.
        {
            let (reply, rx) = oneshot::channel();
            let mut conds = serde_json::Map::new();
            conds.insert("k".into(), serde_json::json!(0));
            let mut ups = serde_json::Map::new();
            ups.insert("owner".into(), serde_json::Value::Null);
            let op = WriteOp::CompareAndSet {
                node_id: "task:1".into(),
                conditions: conds,
                updates: ups,
                reply,
            };
            writer.try_enqueue(op).ok();
            assert!(matches!(rx.await.unwrap(), WriteOutcome::Cas(true)));
        }

        // 50 concurrent workers each try to claim by CAS owner: null -> their id.
        const C: usize = 50;
        let mut handles = Vec::with_capacity(C);
        for i in 0..C {
            let w = writer.clone();
            handles.push(tokio::spawn(async move {
                let (reply, rx) = oneshot::channel();
                let mut conds = serde_json::Map::new();
                conds.insert("owner".into(), serde_json::Value::Null);
                let mut ups = serde_json::Map::new();
                ups.insert("owner".into(), serde_json::json!(format!("w{i}")));
                let op = WriteOp::CompareAndSet {
                    node_id: "task:1".into(),
                    conditions: conds,
                    updates: ups,
                    reply,
                };
                w.try_enqueue(op).ok();
                match rx.await.unwrap() {
                    WriteOutcome::Cas(b) => b,
                    other => panic!("unexpected outcome {other:?}"),
                }
            }));
        }
        let winners = {
            let mut count = 0usize;
            for h in handles {
                if h.await.unwrap() {
                    count += 1;
                }
            }
            count
        };
        assert_eq!(winners, 1, "EXACTLY ONE CAS claimer may win (exactly-once)");
    }

    /// A brand-new graph name gets a writer automatically (lazy keyed creation) —
    /// the "auto per new connector" guarantee. No code change, no hardcoded list.
    #[tokio::test]
    async fn lazy_writer_is_created_per_new_graph_name() {
        let reg = WriteCoalescerRegistry::with_config(CoalescerConfig::auto());
        let core_a = Arc::new(GraphCore::new());
        let core_b = Arc::new(GraphCore::new());

        let wa = reg.writer_for("__commons__", &core_a);
        let wb = reg.writer_for("connector:brand_new", &core_b);
        // Distinct graphs → distinct writers.
        assert!(!Arc::ptr_eq(&wa, &wb));
        // Same name → same writer (one worker per graph).
        let wa2 = reg.writer_for("__commons__", &core_a);
        assert!(Arc::ptr_eq(&wa, &wa2));
    }

    /// Micro-benchmark (run with `--ignored --nocapture`): concurrent producers
    /// writing to ONE graph via the STRICTLY-INLINE path (one `core.txn()` per op =
    /// the pre-coalescer behavior) vs the COALESCED path (batched under one txn).
    /// Prints wall-clock and lock-acquisition counts for both so the win is
    /// quantified. Ignored by default (timing-sensitive; not a correctness gate).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "micro-benchmark; run with --ignored --nocapture"]
    async fn bench_inline_vs_coalesced() {
        const N: usize = 50_000;
        const PRODUCERS: usize = 64;

        // ── Inline baseline: every op opens its own one-shot txn (= one
        // topo.write() acquisition per op), exactly like GraphCore::add_node. ──
        let inline_core = Arc::new(GraphCore::new());
        let t0 = std::time::Instant::now();
        {
            let mut handles = Vec::with_capacity(PRODUCERS);
            for p in 0..PRODUCERS {
                let c = inline_core.clone();
                handles.push(tokio::spawn(async move {
                    for i in (p..N).step_by(PRODUCERS) {
                        c.add_node(format!("n{i}"), node_props(i as i64)); // one txn each
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
        }
        let inline_elapsed = t0.elapsed();
        assert_eq!(inline_core.node_count(), N);
        // Inline = exactly one lock acquisition per op.
        let inline_locks = N as u64;

        // ── Coalesced path: producers PIPELINE (fire ops without blocking on each
        // reply, as a real ingestion firehose does) so the worker sees a deep queue
        // and batches it; replies are awaited after the burst. ──
        let co_core = Arc::new(GraphCore::new());
        let cfg = CoalescerConfig {
            max_batch: 256,
            queue_capacity: 8192,
            max_linger: Duration::from_micros(100),
        };
        let writer = GraphWriter::spawn("bench".into(), co_core.clone(), cfg);
        let t1 = std::time::Instant::now();
        {
            let mut handles = Vec::with_capacity(PRODUCERS);
            for p in 0..PRODUCERS {
                let w = writer.clone();
                let c = co_core.clone();
                handles.push(tokio::spawn(async move {
                    let mut pending = Vec::new();
                    for i in (p..N).step_by(PRODUCERS) {
                        let (reply, rx) = oneshot::channel();
                        let op = WriteOp::AddNode {
                            node_id: format!("n{i}"),
                            properties_msgpack: node_props(i as i64),
                            reply,
                        };
                        // Backpressure: if the bounded queue is full, drain a few
                        // replies before retrying (never block the worker forever).
                        match w.try_enqueue(op) {
                            Ok(()) => pending.push(rx),
                            Err(op) => {
                                w.apply_one_inline(&c, "bench", op);
                                pending.push(rx);
                                // Let the worker catch up on the backlog.
                                if let Some(front) = pending.drain(..pending.len() / 2).next_back()
                                {
                                    let _ = front.await;
                                }
                            }
                        }
                    }
                    for rx in pending {
                        rx.await.unwrap();
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
        }
        let co_elapsed = t1.elapsed();
        assert_eq!(co_core.node_count(), N);
        let co_locks = writer.stats().batches();
        let co_ops = writer.stats().ops();
        assert_eq!(co_ops as usize, N);

        eprintln!("\n── write-coalescer micro-benchmark (N={N}, producers={PRODUCERS}) ──");
        eprintln!(
            "INLINE   : {:?}  lock-acquisitions = {} (1/op)",
            inline_elapsed, inline_locks
        );
        eprintln!(
            "COALESCED: {:?}  lock-acquisitions = {} (avg batch {:.1} ops/lock)",
            co_elapsed,
            co_locks,
            co_ops as f64 / co_locks.max(1) as f64
        );
        eprintln!(
            "lock-acquisitions reduced {:.1}x; wall-clock {:.2}x",
            inline_locks as f64 / co_locks.max(1) as f64,
            inline_elapsed.as_secs_f64() / co_elapsed.as_secs_f64()
        );
        // The headline invariant: materially fewer topology-lock acquisitions
        // (the contention reduction). Wall-clock is reported but NOT asserted —
        // it is dominated by oneshot/channel overhead in this synthetic loop and
        // only improves under real lock contention (concurrent readers + a hot
        // graph), which a unit micro-bench can't faithfully reproduce.
        assert!(
            co_locks < inline_locks / 2,
            "coalescer must cut lock acquisitions >2x: {co_locks} vs {inline_locks}"
        );
    }

    /// After a delete (`remove`) + recreate, a write to the SAME name must land on the
    /// NEW core, never the deleted incarnation's orphaned core (CONCEPT:EG-KG.backend.many-repeated-create-delete).
    /// Without `remove`, `writer_for` returns the stale writer (it is name-keyed and
    /// ignores the new core) and the write lands on the old core — the tenant-churn
    /// corruption.
    #[tokio::test]
    async fn recreate_after_remove_routes_to_new_core() {
        let reg = WriteCoalescerRegistry::with_config(CoalescerConfig::auto());
        let old_core = Arc::new(GraphCore::new());
        let new_core = Arc::new(GraphCore::new());

        // First incarnation gets a writer over old_core.
        let _w1 = reg.writer_for("g", &old_core);
        // Delete: drop the cached writer (the registry entry / old_core is gone too).
        reg.remove("g");
        // Recreate with a brand-new core; the next write lazily spawns a fresh writer
        // over new_core.
        let w2 = reg.writer_for("g", &new_core);
        let (reply, rx) = oneshot::channel();
        w2.try_enqueue(WriteOp::AddNode {
            node_id: "x".into(),
            properties_msgpack: node_props(1),
            reply,
        })
        .ok();
        let _ = rx.await;
        assert!(
            new_core.has_node("x"),
            "write must route to the NEW core after delete+recreate"
        );
        assert!(
            !old_core.has_node("x"),
            "write must NOT land on the deleted incarnation's orphaned core"
        );
    }

    fn emb8(i: usize, n: usize) -> Vec<f32> {
        let mut v = vec![0.0f32; 8];
        v[i % 8] = 1.0;
        v[(i + 1) % 8] = (i as f32) / (n as f32);
        v
    }

    /// A RemoveNode batch drives the incremental index-maintenance seam
    /// (CONCEPT:EG-KG.storage.write-changeset / .incremental-ann): the removed node's embedding
    /// is tombstoned under the SAME topology lock, so a later kNN search never
    /// returns it. Uses a second op as a barrier — the single worker drains batches
    /// sequentially, so awaiting a later op guarantees the earlier batch's index
    /// maintenance completed (maintenance runs after the per-op reply, before the
    /// lock releases).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remove_node_batch_tombstones_embedding() {
        let core = Arc::new(GraphCore::new());
        core.add_node("keep".into(), node_props(1));
        core.add_node("drop".into(), node_props(2));
        {
            let mut s = core.semantic_store.write();
            s.add_embedding("keep".into(), vec![1.0, 0.0, 0.0]).unwrap();
            s.add_embedding("drop".into(), vec![0.0, 1.0, 0.0]).unwrap();
        }
        let cfg = CoalescerConfig {
            max_batch: 16,
            queue_capacity: 256,
            max_linger: Duration::from_millis(1),
        };
        let writer = GraphWriter::spawn("g".into(), core.clone(), cfg);

        {
            let (reply, rx) = oneshot::channel();
            let op = WriteOp::RemoveNode {
                node_id: "drop".into(),
                reply,
            };
            if let Err(op) = writer.try_enqueue(op) {
                writer.apply_one_inline(&core, "g", op);
            }
            assert!(matches!(rx.await.unwrap(), WriteOutcome::Ok));
        }
        // Barrier op — its completion guarantees the RemoveNode batch fully drained.
        let _ = submit_node(&writer, "barrier", 0).await;

        let s = core.semantic_store.read();
        assert!(
            s.get_embedding("drop").is_none(),
            "removed node's embedding must be tombstoned by the maintenance seam"
        );
        assert!(
            s.get_embedding("keep").is_some(),
            "survivor embedding must remain"
        );
    }

    /// Concurrency coherence (CONCEPT:EG-KG.storage.write-changeset): kNN reads running
    /// CONCURRENTLY with a stream of node removals NEVER observe a torn index — under
    /// one read guard, every hit's vector is still present in the store. Maintenance
    /// runs under the batch topology lock and mutates the vector store under its own
    /// write lock, so a reader sees the embedding set atomically, never mid-removal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reads_never_see_a_torn_index() {
        let core = Arc::new(GraphCore::new());
        const N: usize = 200;
        for i in 0..N {
            core.add_node(format!("n{i}"), node_props(i as i64));
            core.semantic_store
                .write()
                .add_embedding(format!("n{i}"), emb8(i, N))
                .unwrap();
        }
        let cfg = CoalescerConfig {
            max_batch: 8,
            queue_capacity: 512,
            max_linger: Duration::from_millis(1),
        };
        let writer = GraphWriter::spawn("g".into(), core.clone(), cfg);

        // Reader: hammer kNN while removals proceed. Under ONE read guard a search
        // result and the presence check are consistent — a torn read would surface an
        // id whose vector was concurrently removed.
        let reader_core = core.clone();
        let reader = tokio::spawn(async move {
            let q = vec![1.0f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
            for _ in 0..400 {
                {
                    let s = reader_core.semantic_store.read();
                    for (id, _) in s.semantic_search(&q, 10) {
                        assert!(
                            s.get_embedding(&id).is_some(),
                            "torn index: hit {id} absent from the store under one guard"
                        );
                    }
                }
                tokio::task::yield_now().await;
            }
        });

        // Writer: remove the first half, one node per op.
        for i in 0..N / 2 {
            let (reply, rx) = oneshot::channel();
            let op = WriteOp::RemoveNode {
                node_id: format!("n{i}"),
                reply,
            };
            if let Err(op) = writer.try_enqueue(op) {
                writer.apply_one_inline(&core, "g", op);
            }
            let _ = rx.await;
        }
        reader.await.unwrap();

        // Barrier, then the final state: first half gone, second half intact.
        let _ = submit_node(&writer, "barrier", 0).await;
        let s = core.semantic_store.read();
        for i in 0..N / 2 {
            assert!(
                s.get_embedding(&format!("n{i}")).is_none(),
                "n{i} should be removed"
            );
        }
        for i in N / 2..N {
            assert!(
                s.get_embedding(&format!("n{i}")).is_some(),
                "n{i} should remain"
            );
        }
    }
}
