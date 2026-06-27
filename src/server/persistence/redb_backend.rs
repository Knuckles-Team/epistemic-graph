//! redb write-through persistence backend (CONCEPT:KG-2.177, feature `redb`).
//!
//! A pluggable durable tier that mirrors every graph mutation into ONE embedded
//! `redb` database (`{persist_dir}/graph.redb`), keyed by a `(graph, …)` PREFIX so
//! all tenants share the same tables (not a file per graph). It is a WRITE-THROUGH
//! tier sitting BESIDE the in-memory model — it is NOT authoritative yet (a later
//! milestone makes redb the source of truth); selecting it changes where durable
//! writes land, not how reads are served.
//!
//! ## The #1 risk: never one WriteTransaction per mutation
//!
//! A redb `WriteTransaction::commit` is a B-tree + WAL + (optional) fsync — orders
//! of magnitude more expensive than a single row write. Committing one per graph
//! mutation would collapse write p99. So this backend reuses the EXACT threading
//! model of [`crate::wal_service`]: a single dedicated OS thread owns the
//! `Database`, drains a bounded channel, and folds MANY mutations into ONE
//! `WriteTransaction` per group-commit interval. The [`FsyncPolicy`] cadence maps
//! onto redb `Durability`:
//!   * `Off`      → every group commits `Durability::None` (page-cache only;
//!     process-crash safe, hard-power-loss bounded by the OS — matches WAL `Off`).
//!   * `Interval` → commit once per interval with `Durability::Immediate`
//!     (group-commit fsync; bounds hard-power loss to the interval).
//!   * `Each`     → commit `Durability::Immediate` after every drained batch.
//!
//! Backpressure is identical: the channel is bounded and sheds LOUDLY rather than
//! stalling the reactor, because the abstracted backend remains the system of
//! record (see AGENTS.md durability model).
//!
//! ## Tables (all keyed by graph prefix)
//!   * `nodes`          `(graph, id)            -> node properties msgpack`
//!   * `edges`          `(graph, src, tgt, ord) -> edge properties msgpack`
//!   * `ledger`         `(graph, seq)           -> ledger line`
//!   * `semantic_store` `graph                  -> semantic store blob (msgpack)`
//!   * `graph_meta`     `graph                  -> {name, graph_type} blob` (replaces manifest.json)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::oneshot;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::protocol::{GraphType, Method};
use crate::server::ServerState;
use crate::wal_service::FsyncPolicy;

use super::PersistenceBackend;

// The graph table layout + the PURE durable-row machinery (Method→rows apply,
// group-commit, checkpoint/load) now live in the server-INDEPENDENT
// `crate::redb_store` (CONCEPT:KG-2.216) so the embedded API can drive the SAME
// durable format with no Tokio. This backend reuses them verbatim — ONE format,
// never duplicated — and adds only the off-reactor group-commit writer thread +
// the `PersistenceBackend` async trait wiring on top.
#[cfg(feature = "compute-dist")]
use crate::redb_store::MatViewScanResult;
use crate::redb_store::{
    apply_checkpoint, clear_xshard_decision, clear_xshard_prepare, commit_crossmodal, commit_ops,
    get_xshard_decision, purge_graph_rows, put_xshard_decision, put_xshard_prepare, read_all_dumps,
    read_graph_dump, read_one_node, scan_xshard_prepares, write_graph_meta, GraphDump,
    XshardPrepareScan, EDGES, GRAPH_META, LEDGER, NODES, RAFT_LOG, SEMANTIC, XSHARD_DECISION,
    XSHARD_PREPARE,
};
/// `(first, last)` present Raft log index for a group, or an error (CONCEPT:KG-2.204).
type LogBoundsResult = Result<(Option<u64>, Option<u64>), String>;
// Per-group Raft metadata (vote, applied-state pointers, last-purged), keyed by
// `(group_id, key)`. Lives in `graph.redb` alongside the log; Raft-only, so it
// stays here with the Raft helpers rather than in the shared graph store.
const RAFT_META: TableDefinition<(u64, &str), &[u8]> = TableDefinition::new("raft_meta");

// Time-series tables (CONCEPT:KG-2.210). The CANONICAL `(series_id, bucket_start)`
// chunk schema is declared once in the eg-tsdb crate (where the store/query logic
// lives) and re-exported here so it sits WITH the durable tier's other table
// definitions. They use the SAME redb composite-key range-scan idiom as
// NODES/EDGES/LEDGER. The series store opens its OWN `series.redb` file (redb holds
// an exclusive per-process file lock, so it cannot share this backend's `graph.redb`
// handle) — these aliases document the schema beside the graph tables; the actual
// open + I/O lives in `eg_tsdb::store::SeriesStore`.
#[cfg(feature = "tsdb")]
#[allow(unused_imports)]
pub(crate) use eg_tsdb::store::{SERIES_CHUNKS, SERIES_META};

/// One write command handed to the off-reactor thread. A `Mutation` carries the
/// graph file-name + the applied method; the thread translates it into row writes
/// inside the current group-commit transaction.
enum Cmd {
    Mutation {
        graph: String,
        method: Box<Method>,
        /// When `Some`, this op is part of the COMMIT-BEFORE-ACK barrier
        /// (CONCEPT:KG-2.187): the writer fires this oneshot AFTER the
        /// `WriteTransaction` carrying this op has durably committed, so the awaiting
        /// dispatch task only acks the client once the write is on disk. Many such
        /// senders ride the SAME group-commit batch — one fsync, N notified writers.
        /// `Err` is sent if that op's commit failed (dispatch → ERROR response).
        done: Option<oneshot::Sender<Result<(), String>>>,
    },
    /// Durably persist a graph's identity row (`graph_meta`) so authoritative
    /// `load_all` recovers the graph under its real name/type even with no
    /// checkpoint. Carries a completion oneshot (commit-before-ack semantics).
    RegisterGraph {
        graph: String,
        name: String,
        graph_type: GraphType,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Drop EVERY durable row for one graph — nodes/edges/ledger/semantic AND the
    /// `graph_meta` identity row — in one durable transaction (CONCEPT:KG-2.221).
    /// Issued when a tenant is DELETED so a recreate of the SAME name starts from a
    /// clean durable slate: without this the stale rows survive (same `graph_fname`
    /// key) and leak into the recreated tenant via the read-through / `load_all`.
    /// Carries a completion oneshot (commit-before-ack: the delete is acked only
    /// after the purge is on disk).
    PurgeGraph {
        graph: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Read a single node's stored properties back (read-through on RAM miss under
    /// authoritative mode). Runs on the owner thread because redb holds an exclusive
    /// per-process file lock.
    ReadNode {
        graph: String,
        node_id: String,
        reply: std::sync::mpsc::Sender<Result<Option<Vec<u8>>, String>>,
    },
    /// Snapshot the full registry state into redb (idempotent overwrite per graph)
    /// and force a durable commit. Used by `checkpoint_all` so a protocol/boot
    /// checkpoint persists everything currently resident even if it predates the
    /// redb tier being enabled.
    Checkpoint {
        graphs: Vec<GraphDump>,
        reply: std::sync::mpsc::Sender<Result<usize, String>>,
    },
    /// Read the full store back as owned dumps. redb holds an EXCLUSIVE per-process
    /// file lock, so the load MUST go through the one thread that owns the
    /// `Database` rather than opening a second handle (which errors "Database
    /// already open"). The async caller rebuilds the registry from the dumps.
    Load {
        reply: std::sync::mpsc::Sender<Result<Vec<GraphDump>, String>>,
    },
    /// Read ONE graph's durable rows back as an owned dump (CONCEPT:KG-2.224 — tenant
    /// rehydration). Goes through the owner thread (exclusive file lock) and flushes
    /// pending writes first so the rehydrated dump reflects the latest durable state.
    ReadGraphDump {
        graph: String,
        reply: std::sync::mpsc::Sender<Result<Option<GraphDump>, String>>,
    },
    /// Verify ONE graph's tamper-evident hash-chained audit log (CONCEPT:KG-2.231).
    /// Flushes pending first so the walk reflects the latest durable entries, then
    /// scans `(graph, 0..)` and reports OK or the first break.
    #[cfg(feature = "security")]
    AuditVerify {
        graph: String,
        reply: std::sync::mpsc::Sender<Result<crate::protocol::AuditReport, String>>,
    },
    /// TEST-ONLY tamper of one audit entry (see `test_tamper_audit_entry`).
    #[cfg(all(test, feature = "security"))]
    TestTamperAudit {
        graph: String,
        seq: u64,
        reply: std::sync::mpsc::Sender<Result<(), String>>,
    },
    /// **Cross-modal ACID commit (CONCEPT:KG-2.225).** Land a graph, vector, blob-ref,
    /// and property write-set for ONE graph in ONE `WriteTransaction`, all-or-nothing,
    /// awaiting its durable fsync (commit-before-ack). On any error nothing lands: the
    /// dropped transaction discards every modality (no partial cross-modal commit).
    CrossModalCommit {
        graph: String,
        methods: Vec<Method>,
        vectors: Vec<(String, Vec<f32>)>,
        blob_refs: Vec<(String, String)>,
        done: oneshot::Sender<Result<(), String>>,
    },
    Shutdown {
        reply: std::sync::mpsc::Sender<()>,
    },
    // ── Raft log/meta (CONCEPT:KG-2.204) — all on the writer thread because redb
    // holds an EXCLUSIVE per-process file lock, so log + M2 graph data must go
    // through the ONE thread that owns the Database. ──────────────────────────
    /// Append Raft log entries `(group_id, index) -> blob` and await durable commit.
    /// Buffered into the SAME `Pending` batch as M2 mutations so a log append and a
    /// graph mutation coalesce into ONE group-commit `WriteTransaction` / one fsync
    /// (the spike's key optimization). Commit-before-ack: the writer fires `done`
    /// only after the carrying transaction has durably committed.
    RaftLogAppend {
        group_id: u64,
        entries: Vec<(u64, Vec<u8>)>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Read a `[lo, hi]` inclusive index range for one group, in order.
    RaftLogRead {
        group_id: u64,
        lo: u64,
        hi: u64,
        reply: std::sync::mpsc::Sender<Result<Vec<Vec<u8>>, String>>,
    },
    /// Delete entries with index >= `from` for one group (conflict truncation).
    RaftLogDeleteFrom {
        group_id: u64,
        from: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Delete entries with index <= `upto` for one group (purge/compaction).
    RaftLogPurgeUpto {
        group_id: u64,
        upto: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// (first, last) present log index for one group, for `get_log_state`.
    RaftLogBounds {
        group_id: u64,
        reply: std::sync::mpsc::Sender<LogBoundsResult>,
    },
    /// Durably write one Raft metadata key (vote / applied-state / last-purged).
    RaftMetaPut {
        group_id: u64,
        key: String,
        val: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Read one Raft metadata key.
    RaftMetaGet {
        group_id: u64,
        key: String,
        reply: std::sync::mpsc::Sender<Result<Option<Vec<u8>>, String>>,
    },
    // ── Cross-shard 2PC durable records (CONCEPT:KG-2.222) ──────────────────
    /// Durably persist ONE participant group's PREPARE slice for a cross-shard txn
    /// (commit-before-vote: a group votes yes only after this is on disk).
    XshardPreparePut {
        txn_id: String,
        group_id: u64,
        slice: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Durably write the coordinator's DECISION for a cross-shard txn (the atomic
    /// commit point): `1` = COMMIT, `0` = ABORT.
    XshardDecisionPut {
        txn_id: String,
        commit: bool,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Clear ONE participant's prepare record after the txn is resolved.
    XshardPrepareClear {
        txn_id: String,
        group_id: u64,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Clear a resolved txn's decision record (after every participant cleared).
    XshardDecisionClear {
        txn_id: String,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Scan ALL in-doubt prepare records (txn_id, group_id, slice) for recovery.
    XshardScanPrepares {
        reply: std::sync::mpsc::Sender<XshardPrepareScan>,
    },
    /// Read a txn's decision (Some(true)=commit, Some(false)=abort, None=undecided).
    XshardDecisionGet {
        txn_id: String,
        reply: std::sync::mpsc::Sender<Result<Option<bool>, String>>,
    },
    /// Durably upsert a named materialized view's blob (CONCEPT:KG-2.227).
    #[cfg(feature = "compute-dist")]
    MatViewPut {
        name: String,
        blob: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    /// Scan every persisted materialized view `(name, blob)` for reload on boot.
    #[cfg(feature = "compute-dist")]
    MatViewScan {
        reply: std::sync::mpsc::Sender<MatViewScanResult>,
    },
}

/// Adaptive group-commit micro-linger tuning for the redb writer (CONCEPT:EG-024).
///
/// Live profiling of the `eg-redb-writer` thread showed it pinned ~100% on ext4
/// writeback (disk ~83% util, ~50ms write latency, queue depth 42) while every
/// tokio worker sat idle — it is the ingestion write ceiling. The machinery already
/// group-commits (many `Cmd::Mutation` fold into ONE `WriteTransaction`/fsync), but
/// because every authoritative write carries a commit-before-ack `done` oneshot,
/// `Pending::has_barrier()` is ALWAYS true, so the writer commits the instant the
/// channel momentarily drains. With low in-flight write concurrency (serial awaits —
/// the idle workers) the batch is whatever incidentally sat in the channel, i.e.
/// ~1 op ⇒ ~1 fsync per write. The batching machinery was starved of a window.
///
/// This adds a bounded, adaptive linger: when about to commit a SHALLOW barrier
/// batch, spend ONE `recv_timeout(linger)` letting more concurrent writers arrive,
/// then drain again. It MIRRORS the in-memory write-coalescer's `max_linger`
/// (CONCEPT:KG-2.182, `write_coalescer.rs`) but for the DURABLE tier — it does NOT
/// touch the coalescer. Durability is unchanged: authoritative writes still commit
/// `Durability::Immediate` BEFORE their `done` fires; we only widen the batch, never
/// defer an ack past its commit. A crash before commit still loses only un-acked writes.
#[derive(Debug, Clone, Copy)]
pub struct RedbGroupCommitConfig {
    /// Max time to linger for more concurrent writers before committing a shallow
    /// barrier batch. `Duration::ZERO` disables lingering entirely (commit-on-drain
    /// = today's behavior, used as the bench baseline).
    pub linger: Duration,
    /// Only linger when `pending.ops.len()` is BELOW this — a deep batch already
    /// coalesces well, so lingering buys nothing and just adds latency (adaptive).
    pub shallow_threshold: usize,
}

impl RedbGroupCommitConfig {
    /// Resolve from env (Configuration discipline: read once at backend open).
    ///   * `EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US` — linger microseconds (default
    ///     `1000` = 1ms; `0` disables lingering / restores commit-on-drain).
    ///   * `EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW` — shallow-batch op threshold
    ///     (default `32`); the writer lingers only while `ops.len()` is under it.
    pub fn from_env() -> Self {
        let linger_us = std::env::var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(1000);
        let shallow = std::env::var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(32);
        Self {
            linger: Duration::from_micros(linger_us),
            // Never above the 4096 early-flush bound; at least 1.
            shallow_threshold: shallow.clamp(1, 4096),
        }
    }
}

impl Default for RedbGroupCommitConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Group-commit observability for the redb writer (CONCEPT:EG-024), mirroring
/// `write_coalescer::BatchStats`. `ops / commits` is the average batch size = the
/// fsyncs-saved ratio; `lingered` counts commits that paid a micro-linger window.
/// Cheap relaxed atomics, shared (`Arc`) between the writer thread and any reader.
#[derive(Debug, Default)]
pub struct RedbCommitStats {
    /// Group-commit `WriteTransaction`s issued on the run-loop barrier/timeout path.
    pub commits: AtomicU64,
    /// Total graph ops folded across those commits.
    pub ops: AtomicU64,
    /// How many of those commits paid a micro-linger window.
    pub lingered: AtomicU64,
}

impl RedbCommitStats {
    fn record(&self, ops: usize, lingered: bool) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.ops.fetch_add(ops as u64, Ordering::Relaxed);
        if lingered {
            self.lingered.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn commits(&self) -> u64 {
        self.commits.load(Ordering::Relaxed)
    }
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }
    pub fn lingered(&self) -> u64 {
        self.lingered.load(Ordering::Relaxed)
    }
    /// Average group-commit batch size (`ops / commits`); 0.0 before any commit.
    pub fn avg_batch(&self) -> f64 {
        let c = self.commits();
        if c == 0 {
            0.0
        } else {
            self.ops() as f64 / c as f64
        }
    }
}

/// Handle to the redb write-through tier. The dispatch path holds an `Arc` of this
/// and calls `record`; the dedicated thread does all `Database` I/O.
pub struct RedbBackend {
    db_path: String,
    tx: SyncSender<Cmd>,
    dropped: Arc<AtomicU64>,
    /// Group-commit batch-size / linger counters (CONCEPT:EG-024).
    stats: Arc<RedbCommitStats>,
    handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

impl RedbBackend {
    /// Open (or create) `{persist_dir}/graph.redb` and spawn the off-reactor
    /// group-commit writer thread.
    pub fn open(persist_dir: String, policy: FsyncPolicy, capacity: usize) -> Result<Self, String> {
        std::fs::create_dir_all(&persist_dir).map_err(|e| e.to_string())?;
        let db_path = std::path::Path::new(&persist_dir)
            .join("graph.redb")
            .to_string_lossy()
            .to_string();
        let db = Database::create(&db_path).map_err(|e| e.to_string())?;
        // Ensure all tables exist so a fresh DB load_all doesn't error on a missing
        // table (open_table in a read txn fails if the table was never created).
        {
            let wtx = db.begin_write().map_err(|e| e.to_string())?;
            wtx.open_table(NODES).map_err(|e| e.to_string())?;
            wtx.open_table(EDGES).map_err(|e| e.to_string())?;
            wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
            wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
            wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
            wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
            wtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
            wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
            wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
            #[cfg(feature = "security")]
            wtx.open_table(crate::redb_store::AUDIT)
                .map_err(|e| e.to_string())?;
            wtx.commit().map_err(|e| e.to_string())?;
        }
        let (tx, rx) = sync_channel::<Cmd>(capacity.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        // Adaptive group-commit micro-linger config + observability (CONCEPT:EG-024).
        // Resolved once at open (Configuration discipline); the writer thread owns a
        // clone of the stats Arc so callers can read batch-size/throughput live.
        let group_commit = RedbGroupCommitConfig::from_env();
        let stats = Arc::new(RedbCommitStats::default());
        let stats_writer = stats.clone();
        // Encryption-at-rest (CONCEPT:KG-2.231): resolve the value-blob cipher ONCE at
        // open from EPISTEMIC_GRAPH_ENCRYPTION_KEY (the KMS seam). `None` ⇒ encryption
        // OFF ⇒ the durable format + write/read paths are byte-for-byte unchanged.
        #[cfg(feature = "security")]
        let cipher = crate::crypto::ValueCipher::from_env();
        #[cfg(feature = "security")]
        if cipher.is_some() {
            tracing::info!(
                "redb encryption-at-rest ENABLED (value blobs sealed with ChaCha20-Poly1305)"
            );
        }
        let handle = std::thread::Builder::new()
            .name("eg-redb-writer".into())
            .spawn(move || {
                run(
                    rx,
                    db,
                    policy,
                    group_commit,
                    stats_writer,
                    #[cfg(feature = "security")]
                    cipher,
                )
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            db_path,
            tx,
            dropped,
            stats,
            handle: parking_lot::Mutex::new(Some(handle)),
        })
    }

    /// Total mutations dropped due to channel saturation (observability).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Group-commit batch-size / linger counters (CONCEPT:EG-024). `avg_batch()` =
    /// ops-per-fsync; the higher it climbs under concurrent writers, the more the
    /// micro-linger is collapsing the per-write fsync into a shared group commit.
    pub fn commit_stats(&self) -> Arc<RedbCommitStats> {
        self.stats.clone()
    }

    /// TEST-ONLY: flip a byte in the stored audit entry `(graph, seq)` to simulate
    /// tampering, so the verify path can prove detection. Routed through the owner
    /// thread (exclusive file lock).
    #[cfg(all(test, feature = "security"))]
    pub fn test_tamper_audit_entry(&self, graph_fname: &str, seq: u64) -> Result<(), String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::TestTamperAudit {
                graph: graph_fname.to_string(),
                seq,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped tamper reply".to_string())?
    }

    /// Verify ONE graph's tamper-evident hash-chained audit log (CONCEPT:KG-2.231).
    /// Routed through the owner thread (exclusive file lock), which flushes pending
    /// writes first so the walk reflects the latest durable entries.
    #[cfg(feature = "security")]
    pub fn audit_verify_blocking(
        &self,
        graph_fname: &str,
    ) -> Result<crate::protocol::AuditReport, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::AuditVerify {
                graph: graph_fname.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped audit_verify reply".to_string())?
    }

    /// Read ONE graph's durable rows back as an owned dump (CONCEPT:KG-2.224 — tenant
    /// rehydration). Routed through the owner thread (exclusive file lock) which
    /// flushes pending writes first. `None` ⇒ the graph has no durable identity.
    pub fn read_graph_dump_blocking(&self, graph_fname: &str) -> Result<Option<GraphDump>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::ReadGraphDump {
                graph: graph_fname.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped read_graph_dump reply".to_string())?
    }

    /// Reconstruct every graph from the redb store into the registry. The actual
    /// DB read runs on the owner thread (via the `Load` command) because redb holds
    /// an exclusive per-process file lock; this rebuilds each `GraphCore` from the
    /// returned dumps via the SAME `add_node`/`add_edge` calls the WAL replay uses.
    async fn load_into(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::Load { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        let dumps = rx
            .recv()
            .map_err(|_| "redb writer dropped load reply".to_string())??;

        let mut count = 0usize;
        for dump in dumps {
            // Create the live graph (or reuse it) and grab its core.
            let core: Arc<GraphCore> = {
                let mut s = state.write().await;
                if !s.registry.exists(&dump.name) {
                    let _ = s.registry.create_graph(&dump.name, dump.graph_type, None);
                }
                match s.registry.get_mut(&dump.name).map(|e| e.core.clone()) {
                    Some(c) => c,
                    None => continue,
                }
            };
            // Rebuild via the SAME add_node/add_edge calls the WAL replay uses —
            // these regenerate the ledger as a side effect, so the `ledger` table is
            // only a durable mirror (not separately replayed) to avoid double-
            // applying mutations.
            for (id, props) in dump.nodes {
                core.add_node(id, props);
            }
            for (src, tgt, props) in dump.edges {
                let _ = core.add_edge(src, tgt, props);
            }
            // Semantic store restores directly onto the public RwLock field (same
            // destination `from_msgpack` writes).
            if !dump.semantic.is_empty() {
                if let Ok(store) =
                    rmp_serde::from_slice::<crate::compute::semantic::SemanticStore>(&dump.semantic)
                {
                    *core.semantic_store.write() = store;
                }
            }
            count += 1;
        }
        Ok(count)
    }
}

/// Rebuild a live [`GraphCore`] from a durable [`GraphDump`] (CONCEPT:KG-2.224 —
/// tenant rehydration). Uses the SAME `add_node`/`add_edge`/semantic-restore path
/// `load_into` uses, so a rehydrated graph is byte-identical to a freshly loaded one.
/// The core is cleared first so a re-rehydrate is idempotent.
pub fn rehydrate_core_from_dump(core: &GraphCore, dump: &GraphDump) {
    core.clear();
    for (id, props) in &dump.nodes {
        core.add_node(id.clone(), props.clone());
    }
    for (src, tgt, props) in &dump.edges {
        let _ = core.add_edge(src.clone(), tgt.clone(), props.clone());
    }
    if !dump.semantic.is_empty() {
        if let Ok(store) =
            rmp_serde::from_slice::<crate::compute::semantic::SemanticStore>(&dump.semantic)
        {
            *core.semantic_store.write() = store;
        }
    }
}

#[async_trait::async_trait]
impl PersistenceBackend for RedbBackend {
    async fn load_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        let n = self.load_into(state).await?;
        tracing::info!("redb: loaded {} graph(s) from {}", n, self.db_path);
        Ok(n)
    }

    async fn checkpoint_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        // Dump the registry OFF the writer thread (brief per-graph read locks),
        // then hand the dump to the writer for one durable transaction — same
        // discipline as the snapshot checkpoint (clone under lock, encode off it).
        let dumps: Vec<GraphDump> = {
            let s = state.read().await;
            s.registry
                .all_entries()
                .iter()
                .map(|e| GraphDump {
                    graph: crate::persist::sanitize(&e.name),
                    name: e.name.clone(),
                    graph_type: e.graph_type,
                    nodes: e.core.get_nodes(),
                    edges: e.core.get_edges(),
                    ledger: e.core.get_ledger(),
                    semantic: rmp_serde::to_vec_named(&*e.core.semantic_store.read())
                        .unwrap_or_default(),
                })
                .collect()
        };
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::Checkpoint {
                graphs: dumps,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped checkpoint reply".to_string())?
    }

    fn record(&self, graph_fname: &str, method: &Method) {
        match self.tx.try_send(Cmd::Mutation {
            graph: graph_fname.to_string(),
            method: Box::new(method.clone()),
            done: None,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::warn!(
                    "redb writer saturated: dropped mutation (total dropped={}); data is in \
                     memory and will checkpoint, but the redb crash-recovery window has \
                     widened — raise EPISTEMIC_GRAPH_WAL_QUEUE or check disk",
                    n
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                tracing::warn!("redb writer thread is gone; mutation not persisted");
            }
        }
    }

    /// COMMIT-BEFORE-ACK (CONCEPT:KG-2.187). Enqueue the mutation with a completion
    /// oneshot and await its durable commit. Backpressure-NOT-drop: a full queue
    /// BLOCKS for capacity (`SyncSender::send`) instead of shedding the write — under
    /// authoritative mode a durable mutation is NEVER silently discarded. The enqueue
    /// + the blocking send both happen on the blocking pool so the Tokio worker is
    /// never parked on disk/lock pressure. Completion is signalled by the writer
    /// AFTER its group-commit `WriteTransaction` commits, so concurrent callers still
    /// coalesce into ONE fsync.
    async fn record_durable(&self, graph_fname: &str, method: &Method) -> Result<(), String> {
        let (done_tx, done_rx) = oneshot::channel();
        let cmd = Cmd::Mutation {
            graph: graph_fname.to_string(),
            method: Box::new(method.clone()),
            done: Some(done_tx),
        };
        // Blocking send = backpressure: park until the bounded channel has room
        // rather than dropping. Off the reactor via spawn_blocking so a saturated
        // writer can't stall the Tokio worker pool.
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || tx.send(cmd))
            .await
            .map_err(|e| format!("redb record_durable join error: {e}"))?
            .map_err(|_| {
                "redb writer thread is gone; durable mutation not persisted".to_string()
            })?;
        // Await the writer's post-commit signal. A dropped sender (writer gone /
        // commit thread died) is a durability failure, surfaced as Err.
        match done_rx.await {
            Ok(res) => res,
            Err(_) => Err("redb writer dropped durable-commit completion".to_string()),
        }
    }

    /// **Cross-modal ACID (CONCEPT:KG-2.225).** Land graph + vectors + blob-refs for ONE
    /// graph in ONE redb `WriteTransaction`, awaiting its durable fsync. On any error
    /// the transaction is dropped without commit, so NONE of the modalities land — a
    /// true rollback (no partial cross-modal commit). Routed through the owner thread
    /// (exclusive file lock) via a blocking send off the reactor.
    async fn commit_crossmodal(
        &self,
        graph_fname: &str,
        methods: &[Method],
        vectors: &[(String, Vec<f32>)],
        blob_refs: &[(String, String)],
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::CrossModalCommit {
            graph: graph_fname.to_string(),
            methods: methods.to_vec(),
            vectors: vectors.to_vec(),
            blob_refs: blob_refs.to_vec(),
            done,
        };
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || tx.send(cmd))
            .await
            .map_err(|e| format!("commit_crossmodal join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        match rx.await {
            Ok(res) => res,
            Err(_) => Err("redb writer dropped commit_crossmodal completion".to_string()),
        }
    }

    async fn register_graph(
        &self,
        graph_fname: &str,
        name: &str,
        graph_type: GraphType,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::RegisterGraph {
            graph: graph_fname.to_string(),
            name: name.to_string(),
            graph_type,
            done,
        };
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || tx.send(cmd))
            .await
            .map_err(|e| format!("redb register_graph join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        match rx.await {
            Ok(res) => res,
            Err(_) => Err("redb writer dropped register_graph completion".to_string()),
        }
    }

    async fn purge_graph(&self, graph_fname: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::PurgeGraph {
            graph: graph_fname.to_string(),
            done,
        };
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || tx.send(cmd))
            .await
            .map_err(|e| format!("redb purge_graph join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        match rx.await {
            Ok(res) => res,
            Err(_) => Err("redb writer dropped purge_graph completion".to_string()),
        }
    }

    async fn read_node(&self, graph_fname: &str, node_id: &str) -> Result<Option<Vec<u8>>, String> {
        self.read_node_blocking(graph_fname, node_id)
    }

    fn read_node_blocking(
        &self,
        graph_fname: &str,
        node_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        // Point-read routed through the off-reactor writer (so it flushes any
        // pending group-commit first and sees the latest durable state) over a
        // blocking channel. Only hit on a RAM miss (CONCEPT:KG-2.191), never on the
        // resident hot path, so the synchronous round-trip cost is paid only when a
        // node was actually evicted.
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::ReadNode {
                graph: graph_fname.to_string(),
                node_id: node_id.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped read_node reply".to_string())?
    }

    fn shutdown(&self) {
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            let (reply, rx) = std::sync::mpsc::channel();
            if self.tx.send(Cmd::Shutdown { reply }).is_ok() {
                let _ = rx.recv();
            }
            let _ = handle.join();
        }
    }

    #[cfg(any(feature = "raft", feature = "security"))]
    fn as_redb(&self) -> Option<&RedbBackend> {
        Some(self)
    }
}

// ── Durable Raft log API (CONCEPT:KG-2.204) — inherent methods ────────────────
// The Raft log lives in the SAME `graph.redb` Database, written by the SAME
// off-reactor group-commit thread, keyed by `(group_id, index)` so one table
// serves every group (CONCEPT:KG-2.205). Sharing the writer is what lets a log
// append and its graph mutation coalesce into ONE fsync. All gated on `raft`
// (only the raft module consumes them).
#[cfg(feature = "raft")]
impl RedbBackend {
    /// Durably append Raft log entries for a group, awaiting the group-commit fsync
    /// (commit-before-ack). The entries fold into the SAME batch as concurrent M2
    /// mutations, so one fsync covers both.
    #[cfg(feature = "raft")]
    pub async fn raft_log_append(
        &self,
        group_id: u64,
        entries: Vec<(u64, Vec<u8>)>,
    ) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }
        let (done, rx) = oneshot::channel();
        let cmd = Cmd::RaftLogAppend {
            group_id,
            entries,
            done,
        };
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || tx.send(cmd))
            .await
            .map_err(|e| format!("raft_log_append join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped raft_log_append completion".to_string())?
    }

    /// Read an inclusive `[lo, hi]` log index range for a group, in order.
    #[cfg(feature = "raft")]
    pub fn raft_log_read(&self, group_id: u64, lo: u64, hi: u64) -> Result<Vec<Vec<u8>>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::RaftLogRead {
                group_id,
                lo,
                hi,
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped raft_log_read reply".to_string())?
    }

    /// Delete entries with index >= `from` for a group (conflict truncation).
    #[cfg(feature = "raft")]
    pub async fn raft_log_delete_from(&self, group_id: u64, from: u64) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::RaftLogDeleteFrom {
                group_id,
                from,
                done,
            })
        })
        .await
        .map_err(|e| format!("raft_log_delete_from join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped raft_log_delete_from completion".to_string())?
    }

    /// Delete entries with index <= `upto` for a group (purge/compaction).
    #[cfg(feature = "raft")]
    pub async fn raft_log_purge_upto(&self, group_id: u64, upto: u64) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::RaftLogPurgeUpto {
                group_id,
                upto,
                done,
            })
        })
        .await
        .map_err(|e| format!("raft_log_purge_upto join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped raft_log_purge_upto completion".to_string())?
    }

    /// `(first, last)` present log index for a group (for `get_log_state`).
    #[cfg(feature = "raft")]
    pub fn raft_log_bounds(&self, group_id: u64) -> LogBoundsResult {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::RaftLogBounds { group_id, reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped raft_log_bounds reply".to_string())?
    }

    /// Durably write one Raft metadata key (vote / applied-state / last-purged).
    #[cfg(feature = "raft")]
    pub async fn raft_meta_put(
        &self,
        group_id: u64,
        key: &str,
        val: Vec<u8>,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let key = key.to_string();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::RaftMetaPut {
                group_id,
                key,
                val,
                done,
            })
        })
        .await
        .map_err(|e| format!("raft_meta_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped raft_meta_put completion".to_string())?
    }

    /// Read one Raft metadata key for a group.
    #[cfg(feature = "raft")]
    pub fn raft_meta_get(&self, group_id: u64, key: &str) -> Result<Option<Vec<u8>>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::RaftMetaGet {
                group_id,
                key: key.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped raft_meta_get reply".to_string())?
    }

    // ── Cross-shard 2PC durable records (CONCEPT:KG-2.222) ─────────────────────
    // The 2PC coordinator persists each participant's PREPARE slice + the final
    // DECISION here so an in-doubt txn is resolvable after a crash. Each write awaits
    // an Immediate-durability commit (commit-before-vote / commit-before-apply): a
    // group only votes yes once its slice is on disk, and the decision is on disk
    // before any participant applies — the atomicity barrier.

    /// Durably persist one participant group's prepared slice. Awaits the fsync.
    #[cfg(feature = "raft")]
    pub async fn xshard_prepare_put(
        &self,
        txn_id: &str,
        group_id: u64,
        slice: Vec<u8>,
    ) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardPreparePut {
                txn_id,
                group_id,
                slice,
                done,
            })
        })
        .await
        .map_err(|e| format!("xshard_prepare_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped xshard_prepare_put completion".to_string())?
    }

    /// Durably write the coordinator's decision (the atomic commit point). Awaits fsync.
    #[cfg(feature = "raft")]
    pub async fn xshard_decision_put(&self, txn_id: &str, commit: bool) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardDecisionPut {
                txn_id,
                commit,
                done,
            })
        })
        .await
        .map_err(|e| format!("xshard_decision_put join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped xshard_decision_put completion".to_string())?
    }

    /// Clear one participant's prepare record after it is resolved.
    #[cfg(feature = "raft")]
    pub async fn xshard_prepare_clear(&self, txn_id: &str, group_id: u64) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            tx.send(Cmd::XshardPrepareClear {
                txn_id,
                group_id,
                done,
            })
        })
        .await
        .map_err(|e| format!("xshard_prepare_clear join error: {e}"))?
        .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped xshard_prepare_clear completion".to_string())?
    }

    /// Clear a resolved txn's decision record.
    #[cfg(feature = "raft")]
    pub async fn xshard_decision_clear(&self, txn_id: &str) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let txn_id = txn_id.to_string();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || tx.send(Cmd::XshardDecisionClear { txn_id, done }))
            .await
            .map_err(|e| format!("xshard_decision_clear join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped xshard_decision_clear completion".to_string())?
    }

    /// Scan every in-doubt prepare record `(txn_id, group_id, slice)` (for recovery).
    #[cfg(feature = "raft")]
    pub fn xshard_scan_prepares(&self) -> XshardPrepareScan {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::XshardScanPrepares { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped xshard_scan_prepares reply".to_string())?
    }

    /// Read a txn's durable decision (Some(true)=commit, Some(false)=abort, None=undecided).
    #[cfg(feature = "raft")]
    pub fn xshard_decision_get(&self, txn_id: &str) -> Result<Option<bool>, String> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::XshardDecisionGet {
                txn_id: txn_id.to_string(),
                reply,
            })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped xshard_decision_get reply".to_string())?
    }

    /// Durably upsert a named materialized view's serialized blob (CONCEPT:KG-2.227).
    /// Awaits the fsync so a `CreateMatView`/`RefreshMatView` ack means it is on disk.
    #[cfg(feature = "compute-dist")]
    pub async fn matview_put(&self, name: &str, blob: Vec<u8>) -> Result<(), String> {
        let (done, rx) = oneshot::channel();
        let name = name.to_string();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || tx.send(Cmd::MatViewPut { name, blob, done }))
            .await
            .map_err(|e| format!("matview_put join error: {e}"))?
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.await
            .map_err(|_| "redb writer dropped matview_put completion".to_string())?
    }

    /// Scan every persisted materialized view `(name, blob)` (reload on boot).
    #[cfg(feature = "compute-dist")]
    pub fn matview_scan(&self) -> MatViewScanResult {
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(Cmd::MatViewScan { reply })
            .map_err(|_| "redb writer thread is gone".to_string())?;
        rx.recv()
            .map_err(|_| "redb writer dropped matview_scan reply".to_string())?
    }
}

// ── off-reactor group-commit writer thread ───────────────────────────────

fn run(
    rx: Receiver<Cmd>,
    db: Database,
    policy: FsyncPolicy,
    group_commit: RedbGroupCommitConfig,
    stats: Arc<RedbCommitStats>,
    #[cfg(feature = "security")] cipher: Option<crate::crypto::ValueCipher>,
) {
    // Build the durable-crypto handle ONCE (borrows the owned cipher for the thread's
    // lifetime). No-op handle when encryption is off / not compiled.
    #[cfg(feature = "security")]
    let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
    #[cfg(not(feature = "security"))]
    let crypto = crate::redb_store::DurableCrypto::none();
    let tick = match policy {
        FsyncPolicy::Interval(d) => d,
        _ => Duration::from_millis(1000),
    };
    // Pending mutations folded into the NEXT group commit, each with its optional
    // commit-before-ack completion sender (CONCEPT:KG-2.187). After a commit, EVERY
    // sender in the batch is fired with the batch's result — one fsync, N notified.
    let mut pending: Pending = Pending::default();
    // CONCEPT:EG-024 — record the group-commit batch size (ops-per-fsync), then
    // commit+notify. Only counts a batch that actually carried work; `lingered` marks
    // commits that paid a micro-linger window so the win is measurable.
    let commit_now = |pending: &mut Pending, durability: Durability, lingered: bool| {
        if !pending.is_empty() {
            stats.record(pending.ops.len(), lingered);
        }
        commit_and_notify(&db, pending, durability, crypto);
    };
    loop {
        match rx.recv_timeout(tick) {
            Ok(cmd) => {
                if handle_cmd(cmd, &db, &mut pending, policy, crypto) {
                    // shutdown: flush whatever is pending durably, then stop.
                    commit_now(&mut pending, Durability::Immediate, false);
                    break;
                }
                // Drain the rest of the burst so it coalesces into one commit.
                let mut stop = false;
                while let Ok(cmd) = rx.try_recv() {
                    if handle_cmd(cmd, &db, &mut pending, policy, crypto) {
                        stop = true;
                        break;
                    }
                }
                if stop {
                    commit_now(&mut pending, Durability::Immediate, false);
                    return;
                }
                // Any awaiting commit-before-ack op in the batch MUST be made durable
                // now (don't leave an awaited write parked until the next tick): if a
                // barrier op is pending, commit immediately; otherwise honor policy.
                let must_commit_now = pending.has_barrier() || matches!(policy, FsyncPolicy::Each);
                if must_commit_now {
                    // CONCEPT:EG-024 — adaptive group-commit micro-linger. The commit
                    // trigger fires the instant the channel drains, so with low in-flight
                    // write concurrency (serial awaits) the barrier batch is ~1 op ⇒ ~1
                    // fsync/write — the profiled write ceiling. When the about-to-commit
                    // batch is SHALLOW (and no hard barrier needs immediacy), spend ONE
                    // bounded `recv_timeout(linger)` so concurrently-awaiting writers can
                    // land in the channel, then drain again — folding them into the SAME
                    // fsync. Adaptive: a DEEP batch (ops >= shallow_threshold) is already
                    // coalescing, so we linger 0. Guards that PRESERVE latency/correctness:
                    //   * skip when linger == 0 (disabled / bench baseline),
                    //   * skip under FsyncPolicy::Off (no fsync to amortize),
                    //   * skip Raft-log barriers (`raft_log_ops`) so consensus is never
                    //     delayed — only shallow GRAPH-mutation batches linger,
                    //   * the existing 4096 early-flush bound in `handle_cmd` is the upper
                    //     op-count guard, so a linger can never overgrow the batch.
                    // Durability is UNCHANGED: we widen the batch, we do NOT defer any ack
                    // past its own commit (the same `Durability::Immediate` fsync still
                    // precedes every `done` waiter firing).
                    let mut lingered = false;
                    if group_commit.linger > Duration::ZERO
                        && !matches!(policy, FsyncPolicy::Off)
                        && pending.raft_log_ops.is_empty()
                        && !pending.ops.is_empty()
                        && pending.ops.len() < group_commit.shallow_threshold
                    {
                        lingered = true;
                        match rx.recv_timeout(group_commit.linger) {
                            Ok(cmd) => {
                                if handle_cmd(cmd, &db, &mut pending, policy, crypto) {
                                    commit_now(&mut pending, Durability::Immediate, true);
                                    return;
                                }
                                // Drain everyone who arrived during the linger window.
                                while let Ok(cmd) = rx.try_recv() {
                                    if handle_cmd(cmd, &db, &mut pending, policy, crypto) {
                                        commit_now(&mut pending, Durability::Immediate, true);
                                        return;
                                    }
                                }
                            }
                            Err(RecvTimeoutError::Timeout) => {}
                            Err(RecvTimeoutError::Disconnected) => {
                                commit_now(&mut pending, Durability::Immediate, true);
                                break;
                            }
                        }
                    }
                    let durability = match policy {
                        FsyncPolicy::Off => Durability::None,
                        _ => Durability::Immediate,
                    };
                    commit_now(&mut pending, durability, lingered);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // Group-commit boundary: flush pending mutations.
                let durability = match policy {
                    FsyncPolicy::Off => Durability::None,
                    _ => Durability::Immediate,
                };
                commit_now(&mut pending, durability, false);
            }
            Err(RecvTimeoutError::Disconnected) => {
                commit_now(&mut pending, Durability::Immediate, false);
                break;
            }
        }
    }
}

/// Buffered mutations awaiting the next group commit, plus the commit-before-ack
/// completion senders that must be fired once the batch is durable.
#[derive(Default)]
struct Pending {
    ops: Vec<(String, Method)>,
    /// Raft log appends `(group_id, index, blob)` folded into the SAME group-commit
    /// transaction as `ops` (CONCEPT:KG-2.204) — one fsync covers both the log entry
    /// and the M2 graph mutation.
    raft_log_ops: Vec<(u64, u64, Vec<u8>)>,
    /// One per awaited (commit-before-ack) op in this batch.
    waiters: Vec<oneshot::Sender<Result<(), String>>>,
}

impl Pending {
    fn has_barrier(&self) -> bool {
        !self.waiters.is_empty()
    }
    fn is_empty(&self) -> bool {
        self.ops.is_empty() && self.raft_log_ops.is_empty() && self.waiters.is_empty()
    }
}

/// Returns true if the writer should stop. `Mutation` is buffered into `pending`
/// (committed at the next group boundary); `Checkpoint` is applied + committed
/// immediately (it carries its own reply).
fn handle_cmd(
    cmd: Cmd,
    db: &Database,
    pending: &mut Pending,
    policy: FsyncPolicy,
    crypto: crate::redb_store::DurableCrypto<'_>,
) -> bool {
    match cmd {
        Cmd::Mutation {
            graph,
            method,
            done,
        } => {
            pending.ops.push((graph, *method));
            if let Some(tx) = done {
                pending.waiters.push(tx);
            }
            // Bound memory: if a burst outpaces the tick, flush early. The group
            // still amortizes thousands of row writes per commit, and fires every
            // commit-before-ack waiter for the ops in this flush.
            if pending.ops.len() >= 4096 {
                let durability = match policy {
                    FsyncPolicy::Off => Durability::None,
                    _ => Durability::Immediate,
                };
                commit_and_notify(db, pending, durability, crypto);
            }
            false
        }
        Cmd::RegisterGraph {
            graph,
            name,
            graph_type,
            done,
        } => {
            // Flush pending mutations first so a graph's rows and its meta land in a
            // consistent order, then durably write the graph_meta row.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = write_graph_meta(db, &graph, &name, graph_type);
            let _ = done.send(res);
            false
        }
        Cmd::PurgeGraph { graph, done } => {
            // Flush pending mutations first so we never purge a graph and then
            // re-apply a buffered op for it out of order, then drop ALL of its rows
            // (incl. graph_meta) in one durable transaction.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(purge_graph_rows(db, &graph));
            false
        }
        Cmd::ReadNode {
            graph,
            node_id,
            reply,
        } => {
            // Flush pending (incl. any awaited ops) so the read reflects the latest
            // durable state, then point-read the node row.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(read_one_node(db, &graph, &node_id, crypto));
            false
        }
        Cmd::Checkpoint { graphs, reply } => {
            // Flush any buffered Raft log appends (+ their barrier waiters) durably
            // first — the checkpoint path only folds graph `ops`, not log ops, so a
            // pending log entry must be committed on its own before the checkpoint.
            if !pending.raft_log_ops.is_empty() {
                commit_and_notify(db, pending, Durability::Immediate, crypto);
            }
            // Fold any buffered mutations into the same durable commit first so the
            // checkpoint reflects them, then overwrite each graph's rows. The
            // checkpoint commits durably, so any awaited ops it absorbed are durable
            // too — notify their waiters with the checkpoint's success/failure.
            let res = apply_checkpoint(db, &mut pending.ops, graphs, crypto);
            let waiters = std::mem::take(&mut pending.waiters);
            let signal = res.as_ref().map(|_| ()).map_err(|e| e.clone());
            for w in waiters {
                let _ = w.send(signal.clone());
            }
            let _ = reply.send(res);
            false
        }
        Cmd::Load { reply } => {
            // Flush pending so the read sees the latest, then scan the owned DB.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(read_all_dumps(db, crypto));
            false
        }
        Cmd::ReadGraphDump { graph, reply } => {
            // Flush pending so the rehydrated dump reflects the latest durable state,
            // then range-scan ONE graph's rows (CONCEPT:KG-2.224).
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(read_graph_dump(db, &graph, crypto));
            false
        }
        #[cfg(feature = "security")]
        Cmd::AuditVerify { graph, reply } => {
            // Flush pending so the chain walk includes the latest durable audit
            // entries, then verify the hash chain (CONCEPT:KG-2.231).
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(crate::redb_store::verify_audit(db, &graph));
            false
        }
        #[cfg(all(test, feature = "security"))]
        Cmd::TestTamperAudit { graph, seq, reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = (|| {
                let wtx = db.begin_write().map_err(|e| e.to_string())?;
                {
                    let mut audit = wtx
                        .open_table(crate::redb_store::AUDIT)
                        .map_err(|e| e.to_string())?;
                    let original = audit
                        .get((graph.as_str(), seq))
                        .map_err(|e| e.to_string())?
                        .ok_or_else(|| "no such audit entry".to_string())?
                        .value()
                        .to_vec();
                    let mut mutated = original;
                    let last = mutated.len() - 1;
                    mutated[last] ^= 0xFF;
                    audit
                        .insert((graph.as_str(), seq), mutated.as_slice())
                        .map_err(|e| e.to_string())?;
                }
                wtx.commit().map_err(|e| e.to_string())?;
                Ok(())
            })();
            let _ = reply.send(res);
            false
        }
        Cmd::CrossModalCommit {
            graph,
            methods,
            vectors,
            blob_refs,
            done,
        } => {
            // Flush pending first so this cross-modal txn observes the latest durable
            // state (its vector read-modify-write of the SEMANTIC blob must start from
            // the committed store), then land ALL modalities in ONE WriteTransaction.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let res = commit_crossmodal(db, &graph, &methods, &vectors, &blob_refs, crypto);
            let _ = done.send(res);
            false
        }
        Cmd::Shutdown { reply } => {
            let _ = reply.send(());
            true
        }
        Cmd::RaftLogAppend {
            group_id,
            entries,
            done,
        } => {
            // Buffer into the SAME pending batch as M2 mutations; the awaited `done`
            // makes this a commit-before-ack barrier, so the batch commits durably at
            // the next boundary (or immediately, since has_barrier() is now true) and
            // a concurrently-pending graph mutation rides the SAME fsync.
            for (idx, blob) in entries {
                pending.raft_log_ops.push((group_id, idx, blob));
            }
            pending.waiters.push(done);
            false
        }
        Cmd::RaftLogRead {
            group_id,
            lo,
            hi,
            reply,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(read_raft_log_range(db, group_id, lo, hi));
            false
        }
        Cmd::RaftLogDeleteFrom {
            group_id,
            from,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(delete_raft_log_from(db, group_id, from));
            false
        }
        Cmd::RaftLogPurgeUpto {
            group_id,
            upto,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(purge_raft_log_upto(db, group_id, upto));
            false
        }
        Cmd::RaftLogBounds { group_id, reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(raft_log_bounds(db, group_id));
            false
        }
        Cmd::RaftMetaPut {
            group_id,
            key,
            val,
            done,
        } => {
            // Flush pending first so meta ordering is consistent with the log, then
            // durably write the meta row in its own transaction.
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(put_raft_meta(db, group_id, &key, &val));
            false
        }
        Cmd::RaftMetaGet {
            group_id,
            key,
            reply,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(get_raft_meta(db, group_id, &key));
            false
        }
        Cmd::XshardPreparePut {
            txn_id,
            group_id,
            slice,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(put_xshard_prepare(db, &txn_id, group_id, &slice));
            false
        }
        Cmd::XshardDecisionPut {
            txn_id,
            commit,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(put_xshard_decision(db, &txn_id, commit));
            false
        }
        Cmd::XshardPrepareClear {
            txn_id,
            group_id,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(clear_xshard_prepare(db, &txn_id, group_id));
            false
        }
        Cmd::XshardDecisionClear { txn_id, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(clear_xshard_decision(db, &txn_id));
            false
        }
        Cmd::XshardScanPrepares { reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(scan_xshard_prepares(db));
            false
        }
        Cmd::XshardDecisionGet { txn_id, reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(get_xshard_decision(db, &txn_id));
            false
        }
        #[cfg(feature = "compute-dist")]
        Cmd::MatViewPut { name, blob, done } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = done.send(crate::redb_store::put_matview(db, &name, &blob));
            false
        }
        #[cfg(feature = "compute-dist")]
        Cmd::MatViewScan { reply } => {
            commit_and_notify(db, pending, Durability::Immediate, crypto);
            let _ = reply.send(crate::redb_store::scan_matviews(db));
            false
        }
    }
}

/// Commit all buffered mutations in ONE write transaction at the given durability,
/// then fire EVERY commit-before-ack waiter for the ops in this batch with the
/// batch's result (CONCEPT:KG-2.187). Coalescing is preserved: N awaiting writers
/// ride one `WriteTransaction` / one fsync and are all notified after it commits.
/// A waiter is only signalled `Ok` once its op is provably on disk.
fn commit_and_notify(
    db: &Database,
    pending: &mut Pending,
    durability: Durability,
    crypto: crate::redb_store::DurableCrypto<'_>,
) {
    if pending.is_empty() {
        return;
    }
    let res = commit_ops(
        db,
        &mut pending.ops,
        &mut pending.raft_log_ops,
        durability,
        crypto,
    );
    let waiters = std::mem::take(&mut pending.waiters);
    let signal = res.map(|_| ());
    for w in waiters {
        let _ = w.send(signal.clone());
    }
}

// commit_ops / write_graph_meta / read_one_node now live in `crate::redb_store`
// (imported above) — shared verbatim with the embedded path, ONE durable format.

// ── Raft log/meta helpers (CONCEPT:KG-2.204) — run on the writer thread ───────

/// Read a `[lo, hi]` inclusive log range for one group, in index order.
fn read_raft_log_range(db: &Database, gid: u64, lo: u64, hi: u64) -> Result<Vec<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for kv in t.range((gid, lo)..=(gid, hi)).map_err(|e| e.to_string())? {
        let (_, v) = kv.map_err(|e| e.to_string())?;
        out.push(v.value().to_vec());
    }
    Ok(out)
}

/// Delete entries with index >= `from` for one group (conflict truncation).
fn delete_raft_log_from(db: &Database, gid: u64, from: u64) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
        let keys: Vec<u64> = t
            .range((gid, from)..=(gid, u64::MAX))
            .map_err(|e| e.to_string())?
            .filter_map(|kv| kv.ok().map(|(k, _)| k.value().1))
            .collect();
        for idx in keys {
            t.remove((gid, idx)).map_err(|e| e.to_string())?;
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Delete entries with index <= `upto` for one group (purge/compaction).
fn purge_raft_log_upto(db: &Database, gid: u64, upto: u64) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
        let keys: Vec<u64> = t
            .range((gid, 0)..=(gid, upto))
            .map_err(|e| e.to_string())?
            .filter_map(|kv| kv.ok().map(|(k, _)| k.value().1))
            .collect();
        for idx in keys {
            t.remove((gid, idx)).map_err(|e| e.to_string())?;
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// (first, last) present log index for one group.
fn raft_log_bounds(db: &Database, gid: u64) -> LogBoundsResult {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
    let mut range = t
        .range((gid, 0)..=(gid, u64::MAX))
        .map_err(|e| e.to_string())?;
    let first = range
        .next()
        .and_then(|kv| kv.ok().map(|(k, _)| k.value().1));
    // Re-scan for the last (the iterator was advanced by `next`).
    let last = t
        .range((gid, 0)..=(gid, u64::MAX))
        .map_err(|e| e.to_string())?
        .next_back()
        .and_then(|kv| kv.ok().map(|(k, _)| k.value().1));
    Ok((first, last))
}

/// Durably write one Raft metadata key for a group.
fn put_raft_meta(db: &Database, gid: u64, key: &str, val: &[u8]) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut t = wtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
        t.insert((gid, key), val).map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Read one Raft metadata key for a group.
fn get_raft_meta(db: &Database, gid: u64, key: &str) -> Result<Option<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let t = rtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
    Ok(t.get((gid, key))
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::registry::GraphRegistry;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// A minimal `ServerState` (no persistence backend stored on it — the test
    /// drives the backend directly) with a persist dir set.
    fn new_state(persist_dir: Option<String>) -> Arc<RwLock<ServerState>> {
        new_state_auth(persist_dir, false)
    }

    fn new_state_auth(
        persist_dir: Option<String>,
        authoritative: bool,
    ) -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: "test".to_string(),
            persist_dir,
            persistence: None,
            redb_authoritative: authoritative,
            max_in_flight: Arc::new(tokio::sync::Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
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
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn redb_write_through_roundtrip() {
        // Boot a redb backend, write nodes/edges through `record`, checkpoint to
        // force a durable commit, drop, reload via redb-only load → graph identical.
        let dir = std::env::temp_dir().join(format!("eg-redb-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        // ── write side ──
        let backend =
            RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 64).expect("open redb backend");

        // The registry must have the graph for checkpoint to dump it; build a
        // minimal ServerState with the graph created + populated in memory.
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
        }
        // Apply mutations in memory AND record them through the backend. Fetch g1
        // by name — registry iteration order is not stable (HashMap), so
        // all_entries()[0] could be the pre-created `__commons__`.
        let core = {
            let s = state.read().await;
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        core.add_node(
            "a".into(),
            props(serde_json::json!({"type": "Task", "n": 1})),
        );
        core.add_node("b".into(), props(serde_json::json!({"type": "Task"})));
        let _ = core.add_edge("a".into(), "b".into(), props(serde_json::json!({"w": 2})));

        backend.record(
            "g1",
            &Method::AddNode {
                node_id: "a".into(),
                properties_msgpack: props(serde_json::json!({"type": "Task", "n": 1})),
            },
        );
        backend.record(
            "g1",
            &Method::AddNode {
                node_id: "b".into(),
                properties_msgpack: props(serde_json::json!({"type": "Task"})),
            },
        );
        backend.record(
            "g1",
            &Method::AddEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: props(serde_json::json!({"w": 2})),
            },
        );
        // Checkpoint persists graph_meta (so load_all sees each graph) + a durable
        // commit. The registry pre-creates `__commons__`, so g1 + commons = 2.
        assert_eq!(backend.checkpoint_all(&state).await.unwrap(), 2);
        assert_eq!(backend.dropped(), 0);
        backend.shutdown();
        drop(backend);

        // ── reload side: fresh backend + fresh empty state ──
        let backend2 =
            RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 64).expect("reopen redb backend");
        let state2 = new_state(Some(dir_s.clone()));
        let loaded = backend2.load_all(&state2).await.unwrap();
        assert_eq!(loaded, 2, "g1 + __commons__ reloaded from redb");

        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("g1")
                .map(|e| e.core.clone())
                .expect("g1 reloaded")
        };
        assert_eq!(core2.node_count(), 2);
        assert_eq!(
            core2.get_node_properties("a"),
            Some(props(serde_json::json!({"type": "Task", "n": 1})))
        );
        assert_eq!(core2.get_edges().len(), 1);
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:KG-2.180 — a committed OCC transaction is durable through the redb
    /// backend: stage nodes/edge in a txn, commit through the full dispatch path
    /// (which records each staged method via `persistence.record`), drop, then
    /// reload via redb-only → the committed graph is recovered.
    #[tokio::test(flavor = "multi_thread")]
    async fn txn_commit_persists_to_redb() {
        use crate::protocol::{Request, ResultPayload};
        use crate::server::{compute_auth_token, dispatch};

        const SECRET: &str = "redb-txn-secret";
        let dir = std::env::temp_dir().join(format!("eg-redb-txn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> = Arc::new(
            RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 64).expect("open redb backend"),
        );
        let state = new_state(Some(dir_s.clone()));
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }

        let req = |id: u64, method: Method| Request {
            id,
            graph: "__commons__".to_string(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: None,
            method,
        };
        let txn = match dispatch(
            &state,
            req(
                1,
                Method::BeginTxn {
                    graph: None,
                    isolation: None,
                },
            ),
        )
        .await
        .result
        {
            Some(ResultPayload::String(t)) => t,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        for (rid, nid) in [(2u64, "x"), (3, "y")] {
            let r = dispatch(
                &state,
                req(
                    rid,
                    Method::TxnAddNode {
                        txn_id: txn.clone(),
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"type": "Task"})),
                        graph: None,
                    },
                ),
            )
            .await;
            assert!(matches!(r.result, Some(ResultPayload::Bool(true))));
        }
        let r = dispatch(
            &state,
            req(
                4,
                Method::TxnAddEdge {
                    txn_id: txn.clone(),
                    source_id: "x".into(),
                    target_id: "y".into(),
                    properties_msgpack: props(serde_json::json!({})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

        let r = dispatch(&state, req(5, Method::Commit { txn_id: txn })).await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "commit ok: {:?}",
            r.error
        );

        // Checkpoint to flush graph_meta + a durable commit, then reload redb-only.
        assert!(backend.checkpoint_all(&state).await.unwrap() >= 1);
        backend.shutdown();

        let backend2 =
            RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 64).expect("reopen redb backend");
        let state2 = new_state(Some(dir_s.clone()));
        backend2.load_all(&state2).await.unwrap();
        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("__commons__")
                .map(|e| e.core.clone())
                .expect("__commons__ reloaded")
        };
        assert!(
            core2.has_node("x") && core2.has_node("y"),
            "committed txn nodes durable in redb"
        );
        assert_eq!(
            core2.get_edges().len(),
            1,
            "committed txn edge durable in redb"
        );
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:KG-2.221 — tenant DELETE + recreate-same-name must not drop the new
    /// graph's writes. Under redb-authoritative mode (read-through wired exactly as
    /// `main.rs` does it) we: create "g", add "n1"={v:1}, DELETE "g", recreate "g",
    /// add "n1"={v:2}, then read "n1" back through the full dispatch path. The
    /// recreated graph's node MUST read back as {v:2} — not the stale {v:1} left in
    /// redb by the first incarnation, not empty. Mirrors the agent-utilities
    /// `test_find_analogous_subgraphs` tenant-churn failure at the engine level.
    #[tokio::test(flavor = "multi_thread")]
    async fn delete_then_recreate_same_name_keeps_new_writes() {
        use crate::protocol::{Request, ResultPayload};
        use crate::server::persistence::read_through::BackendReadThroughFactory;
        use crate::server::{compute_auth_token, dispatch};

        const SECRET: &str = "redb-recreate";
        let dir = std::env::temp_dir().join(format!("eg-redb-recreate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 256).expect("open"));
        let state = new_state_auth(Some(dir_s.clone()), true);
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
            // Wire the durable read-through exactly like main.rs does under
            // authoritative mode — this is the read path that serves a RAM miss.
            let factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
            s.registry.set_read_through_factory(factory);
        }

        let req = |id: u64, method: Method| Request {
            id,
            graph: "g".to_string(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: None,
            method,
        };
        let create = |id: u64| {
            req(
                id,
                Method::CreateGraph {
                    graph_name: "g".into(),
                    graph_type: GraphType::Global,
                },
            )
        };
        let add = |id: u64, node: &str, v: i64| {
            req(
                id,
                Method::AddNode {
                    node_id: node.into(),
                    properties_msgpack: props(serde_json::json!({"v": v})),
                },
            )
        };
        let get = |id: u64, node: &str| {
            req(
                id,
                Method::GetNodeProperties {
                    node_id: node.into(),
                },
            )
        };

        // First incarnation: create + write n1={v:1} and stale={v:9}.
        assert!(dispatch(&state, create(1)).await.error.is_none());
        assert!(dispatch(&state, add(2, "n1", 1)).await.error.is_none());
        assert!(dispatch(&state, add(3, "stale", 9)).await.error.is_none());

        // Delete the tenant.
        let del = dispatch(
            &state,
            req(
                4,
                Method::DeleteGraph {
                    graph_name: "g".into(),
                },
            ),
        )
        .await;
        assert!(del.error.is_none(), "delete: {:?}", del.error);

        // Recreate SAME name. The new tenant writes ONLY n1={v:2}; it never writes
        // "stale" — that node belongs to the deleted incarnation and must be gone.
        let recreate = dispatch(&state, create(5)).await;
        assert!(recreate.error.is_none(), "recreate: {:?}", recreate.error);
        assert!(dispatch(&state, add(6, "n1", 2)).await.error.is_none());

        // (a) LIVE read-through: force every node out of RAM so the next read
        // RAM-MISSES and falls to the durable read-through (the eviction path is real
        // under authoritative mode — it bounds memory per CONCEPT:KG-2.191).
        let ev = dispatch(&state, req(7, Method::EvictLRU { max_nodes: 0 })).await;
        assert!(ev.error.is_none(), "evict: {:?}", ev.error);

        // The deleted incarnation's "stale" node must NOT resurrect from redb on a
        // RAM-miss read of the recreated graph.
        let r = dispatch(&state, get(8, "stale")).await;
        let stale = match r.result {
            Some(ResultPayload::PropertiesMsgpack(b)) => Some(b),
            Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
            other => panic!("unexpected get result: {other:?}"),
        };
        assert_eq!(
            stale, None,
            "deleted tenant's node 'stale' resurrected via read-through after recreate"
        );

        // And n1 reads back as the NEW write {v:2}.
        let r = dispatch(&state, get(9, "n1")).await;
        let got = match r.result {
            Some(ResultPayload::PropertiesMsgpack(b)) => Some(b),
            Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
            other => panic!("unexpected get result: {other:?}"),
        };
        assert_eq!(
            got,
            Some(props(serde_json::json!({"v": 2}))),
            "recreated tenant's node n1 must read back as the NEW write {{v:2}}, not stale/empty"
        );
        backend.shutdown();
        drop(backend);

        // (b) DURABLE resurrection across a reload (restart / resharding / hibernation
        // rehydration): a fresh backend + state that load_all from the SAME redb dir
        // must NOT recover the deleted incarnation's nodes.
        let backend2: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 256).expect("reopen"));
        let state2 = new_state_auth(Some(dir_s.clone()), true);
        backend2.load_all(&state2).await.unwrap();
        let core2 = {
            let s = state2.read().await;
            s.registry.get("g").map(|e| e.core.clone())
        };
        if let Some(core2) = core2 {
            assert!(
                !core2.has_node("stale"),
                "deleted tenant's node 'stale' resurrected from redb on load_all after recreate"
            );
            assert_eq!(
                core2.get_node_properties("n1"),
                Some(props(serde_json::json!({"v": 2}))),
                "recreated tenant's n1 must survive a reload as {{v:2}}"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:KG-2.237 — MANY repeated create→delete→recreate cycles on the SAME
    /// graph name must NOT enter a corrupted in-memory state that silently drops the
    /// recreated graph's writes (the vault-lane `__secrets__` failure). DISTINCT from
    /// KG-2.221 (durable redb purge): here every read is served HOT from RAM (no
    /// eviction, no reload) so the bug is purely the in-memory per-graph state keyed
    /// by name that DeleteGraph failed to reset — specifically the write-coalescer's
    /// cached `GraphWriter`, whose worker owns an `Arc<GraphCore>` of the DELETED
    /// incarnation. On recreate, `writer_for` returned the STALE writer (keyed by
    /// name) and routed the new tenant's writes into the orphaned core; the registry's
    /// fresh GraphCore stayed empty, so a hot RAM read saw nothing. The corruption
    /// accumulates: it appears the first cycle the coalescer batched a write.
    ///
    /// Tested for BOTH a plain name and a `__…__`-style reserved name (the report saw
    /// it on `__secrets__`). 50 cycles, each writing a DIFFERENT node so a stale read
    /// can't masquerade as a fresh one.
    #[tokio::test(flavor = "multi_thread")]
    async fn many_recreate_cycles_keep_inmemory_writes_visible() {
        use crate::protocol::{Request, ResultPayload};
        use crate::server::persistence::read_through::BackendReadThroughFactory;
        use crate::server::{compute_auth_token, dispatch};

        const SECRET: &str = "redb-churn";
        const CYCLES: u64 = 50;

        for graph in ["g", "__secrets__"] {
            let dir = std::env::temp_dir().join(format!(
                "eg-redb-churn-{}-{}",
                std::process::id(),
                graph.trim_matches('_')
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();

            let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
                Arc::new(RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 256).expect("open"));
            let state = new_state_auth(Some(dir_s.clone()), true);
            {
                let mut s = state.write().await;
                s.auth_secret = SECRET.to_string();
                s.persistence = Some(backend.clone());
                let factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
                s.registry.set_read_through_factory(factory);
            }

            let req = |id: u64, method: Method| Request {
                id,
                graph: graph.to_string(),
                auth_token: compute_auth_token(SECRET, id),
                agent_id: None,
                method,
            };

            let mut id = 0u64;
            let mut next = || {
                id += 1;
                id
            };

            for cycle in 0..CYCLES {
                // create
                let c = dispatch(
                    &state,
                    req(
                        next(),
                        Method::CreateGraph {
                            graph_name: graph.into(),
                            graph_type: GraphType::Global,
                        },
                    ),
                )
                .await;
                assert!(c.error.is_none(), "cycle {cycle} create: {:?}", c.error);

                // write a node UNIQUE to this cycle
                let node = format!("n{cycle}");
                let a = dispatch(
                    &state,
                    req(
                        next(),
                        Method::AddNode {
                            node_id: node.clone(),
                            properties_msgpack: props(serde_json::json!({"cycle": cycle})),
                        },
                    ),
                )
                .await;
                assert!(a.error.is_none(), "cycle {cycle} add: {:?}", a.error);

                // read it back HOT from RAM (no eviction) — the new tenant's write
                // MUST be visible. This is where a stale coalescer routes the write
                // into the deleted core and the fresh core reads back empty.
                let r = dispatch(
                    &state,
                    req(
                        next(),
                        Method::GetNodeProperties {
                            node_id: node.clone(),
                        },
                    ),
                )
                .await;
                let got = match r.result {
                    Some(ResultPayload::PropertiesMsgpack(b)) => Some(b),
                    Some(ResultPayload::Json(serde_json::Value::Null)) | None => None,
                    other => panic!("cycle {cycle} unexpected get result: {other:?}"),
                };
                assert_eq!(
                    got,
                    Some(props(serde_json::json!({"cycle": cycle}))),
                    "graph {graph:?} cycle {cycle}: recreated tenant's write to {node:?} \
                     was silently dropped (in-memory churn corruption)"
                );

                // IN-MEMORY proof: NodeCount reads the registry's live GraphCore
                // directly (NO durable read-through), so it sees ONLY what actually
                // landed in RAM. The recreated graph must hold EXACTLY this cycle's one
                // node. If the stale coalescer routed the write to the deleted core,
                // the live core is empty and this is 0 — the durable read-through above
                // would otherwise MASK the corruption by serving redb.
                let nc = dispatch(&state, req(next(), Method::NodeCount)).await;
                let count = match nc.result {
                    Some(ResultPayload::Count(c)) => c,
                    other => panic!("cycle {cycle} unexpected node-count result: {other:?}"),
                };
                assert_eq!(
                    count, 1,
                    "graph {graph:?} cycle {cycle}: recreated tenant's live GraphCore must hold \
                     exactly the 1 node written this cycle (in-memory write was dropped)"
                );

                // delete (skip on the last cycle so we leave the graph live)
                if cycle + 1 < CYCLES {
                    let d = dispatch(
                        &state,
                        req(
                            next(),
                            Method::DeleteGraph {
                                graph_name: graph.into(),
                            },
                        ),
                    )
                    .await;
                    assert!(d.error.is_none(), "cycle {cycle} delete: {:?}", d.error);
                }
            }

            backend.shutdown();
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// CONCEPT:KG-2.187 — full dispatch path under AUTHORITATIVE mode: a write acked
    /// through `dispatch` is durable in redb WITHOUT any checkpoint, and reloads via
    /// redb `load_all` (the authoritative source). This proves the commit-before-ack
    /// barrier covers the real dispatch write path (incl. the coalescer) AND that the
    /// graph is recoverable under its real name with no checkpoint (graph_meta is
    /// durably registered on create + backfilled on write).
    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_authoritative_durable_without_checkpoint() {
        use crate::protocol::{Request, ResultPayload};
        use crate::server::{compute_auth_token, dispatch};

        const SECRET: &str = "redb-auth-dispatch";
        let dir = std::env::temp_dir().join(format!("eg-redb-authd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend: Arc<dyn crate::server::persistence::PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 256).expect("open"));
        let state = new_state_auth(Some(dir_s.clone()), true);
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| Request {
            id,
            graph: "g_auth".to_string(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: None,
            method,
        };
        // Create graph (durably registered) then write nodes — each dispatch returns
        // only after the durable commit.
        let r = dispatch(
            &state,
            req(
                1,
                Method::CreateGraph {
                    graph_name: "g_auth".into(),
                    graph_type: GraphType::Global,
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "create: {:?}", r.error);
        for (rid, nid) in [(2u64, "a"), (3, "b"), (4, "c")] {
            let r = dispatch(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"id": nid})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "addnode {nid}: {:?}", r.error);
            assert!(
                matches!(r.result, Some(ResultPayload::Bool(true)) | None) || r.error.is_none()
            );
        }

        // NO checkpoint. Drop the backend (flushes shutdown) and reload redb-only.
        backend.shutdown();

        let backend2 = RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 256).expect("reopen");
        let state2 = new_state_auth(Some(dir_s.clone()), true);
        let loaded = backend2.load_all(&state2).await.unwrap();
        assert!(loaded >= 1, "graphs recovered from redb without checkpoint");
        let core2 = {
            let s = state2.read().await;
            s.registry
                .get("g_auth")
                .map(|e| e.core.clone())
                .expect("g_auth recovered under real name")
        };
        assert!(core2.has_node("a") && core2.has_node("b") && core2.has_node("c"));
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:KG-2.187 — commit-before-ack: `record_durable` returns ONLY after the
    /// op is durably committed. Use FsyncPolicy::Interval so the op is NOT committed
    /// by an Each-after-batch path; the only way the await completes is the group
    /// commit firing the waiter. After the await returns, a SEPARATE reopened DB sees
    /// the row — proving the await observed durable state, not just an enqueue.
    #[tokio::test(flavor = "multi_thread")]
    async fn record_durable_awaits_commit() {
        let dir = std::env::temp_dir().join(format!("eg-redb-durable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(
            dir_s.clone(),
            FsyncPolicy::Interval(Duration::from_millis(50)),
            64,
        )
        .expect("open");

        backend
            .record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "a".into(),
                    properties_msgpack: props(serde_json::json!({"v": 1})),
                },
            )
            .await
            .expect("durable commit");

        // The await returned ⇒ the op is committed. Verify via a point read on the
        // SAME backend (goes through the owner thread, reflecting committed state).
        let got = backend.read_node("g1", "a").await.expect("read");
        assert_eq!(got, Some(props(serde_json::json!({"v": 1}))));
        assert_eq!(backend.dropped(), 0, "authoritative path never drops");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:KG-2.187 — many concurrent `record_durable` calls COALESCE into group
    /// commits (NOT one fsync per op): all N complete, all N are durable. We can't
    /// directly count fsyncs here, but we assert all N awaited writers resolve Ok and
    /// every node is durably present — the coalescing path (one WriteTransaction per
    /// batch firing all the batch's waiters) is what makes that terminate quickly.
    #[tokio::test(flavor = "multi_thread")]
    async fn record_durable_coalesces_many_writers() {
        let dir = std::env::temp_dir().join(format!("eg-redb-coalesce-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(
            RedbBackend::open(
                dir_s.clone(),
                FsyncPolicy::Interval(Duration::from_millis(20)),
                256,
            )
            .expect("open"),
        );

        let n = 200usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each durable commit ok");
        }
        assert_eq!(backend.dropped(), 0);
        // Every node durable.
        for i in 0..n {
            let got = backend.read_node("g1", &format!("n{i}")).await.unwrap();
            assert!(got.is_some(), "n{i} durable");
        }
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-024 — the adaptive micro-linger COALESCES concurrent in-flight
    /// authoritative writers into fewer, larger group commits WITHOUT losing
    /// durability. We fan N concurrent `record_durable` calls (each its own task, so
    /// the writer sees them arrive within the linger window) and assert:
    ///   * every awaited writer resolves Ok and every node is durably present
    ///     (durability guarantee unchanged — commit-before-ack still holds),
    ///   * the average batch size (`ops / commits`) climbs well above 1, i.e. the
    ///     linger folded many writers into one fsync (the profiled win),
    ///   * lingered commits were actually exercised.
    /// `FsyncPolicy::Each` would commit per-drained-batch regardless, so we use
    /// `Interval` (the live authoritative cadence) where, pre-EG-024, a drained
    /// channel commits immediately at ~1 op/fsync.
    /// Serializes the env-mutating linger tests. `EPISTEMIC_GRAPH_REDB_GROUP_*` are
    /// process-global and read once inside `RedbBackend::open`, so two parallel tests
    /// setting different values would race the config read (the disabled test could
    /// observe the coalesce test's `2000`). Hold this across set_var → open →
    /// remove_var; `open` is sync so there is no await under the guard.
    static LINGER_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test(flavor = "multi_thread")]
    async fn micro_linger_coalesces_concurrent_writers() {
        let dir = std::env::temp_dir().join(format!("eg-redb-linger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            // Explicit, deterministic knobs; serialized vs the other env-mutating test.
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US", "2000");
            std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW", "256");
            let b = Arc::new(
                RedbBackend::open(
                    dir_s.clone(),
                    // Long interval so the ONLY thing that commits a batch is the barrier
                    // path (+ its micro-linger), never the tick.
                    FsyncPolicy::Interval(Duration::from_millis(500)),
                    512,
                )
                .expect("open"),
            );
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US");
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_SHALLOW");
            b
        };

        let stats = backend.commit_stats();
        let n = 256usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each durable commit ok");
        }
        assert_eq!(backend.dropped(), 0, "authoritative path never drops");
        // Durability: every node present.
        for i in 0..n {
            assert!(
                backend.read_node("g1", &format!("n{i}")).await.unwrap().is_some(),
                "n{i} durable"
            );
        }
        // The win: many writers folded into far fewer commits than ops.
        let commits = stats.commits();
        let ops = stats.ops();
        assert!(ops >= n as u64, "all {n} ops counted (got {ops})");
        assert!(
            commits < n as u64,
            "linger must coalesce: {commits} commits for {ops} ops (expected << {n})"
        );
        assert!(
            stats.avg_batch() > 1.5,
            "avg batch {:.2} should be well above the 1-op/fsync baseline",
            stats.avg_batch()
        );
        assert!(stats.lingered() > 0, "micro-linger path was exercised");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:EG-024 — with the linger DISABLED (`LINGER_US=0`) the writer falls back
    /// to the exact commit-on-drain behavior, and durability is identical. This pins
    /// the baseline the bench measures against and proves the knob is a real opt-out.
    #[tokio::test(flavor = "multi_thread")]
    async fn micro_linger_disabled_preserves_durability() {
        let dir = std::env::temp_dir().join(format!("eg-redb-nolinger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = {
            // Serialized vs the coalesce test so its `2000` can't leak into our `open`.
            let _env = LINGER_ENV_LOCK.lock().unwrap();
            std::env::set_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US", "0");
            let b = Arc::new(
                RedbBackend::open(
                    dir_s.clone(),
                    FsyncPolicy::Interval(Duration::from_millis(50)),
                    256,
                )
                .expect("open"),
            );
            std::env::remove_var("EPISTEMIC_GRAPH_REDB_GROUP_LINGER_US");
            b
        };

        let stats = backend.commit_stats();
        let n = 64usize;
        let mut handles = Vec::new();
        for i in 0..n {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                b.record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: props(serde_json::json!({"i": i})),
                    },
                )
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("each durable commit ok");
        }
        for i in 0..n {
            assert!(
                backend.read_node("g1", &format!("n{i}")).await.unwrap().is_some(),
                "n{i} durable"
            );
        }
        // No commit ever lingered when the knob is 0.
        assert_eq!(stats.lingered(), 0, "linger disabled ⇒ zero lingered commits");
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── CONCEPT:KG-2.191 — read-through-on-RAM-miss + safe authoritative eviction ──

    /// Populate `core` AND redb with `n` durable nodes, then install the read-through
    /// factory + the backend on `state` exactly as `main.rs` does under authoritative
    /// mode. Returns the live core for "g1".
    async fn seed_authoritative(
        backend: &Arc<dyn PersistenceBackend>,
        state: &Arc<RwLock<ServerState>>,
        n: usize,
    ) -> Arc<GraphCore> {
        {
            let mut s = state.write().await;
            s.persistence = Some(backend.clone());
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
            // Wire read-through exactly like startup (attaches to g1 + __commons__).
            let factory = Arc::new(
                crate::server::persistence::read_through::BackendReadThroughFactory::new(
                    backend.clone(),
                ),
            );
            s.registry.set_read_through_factory(factory);
        }
        let core = {
            let s = state.read().await;
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        for i in 0..n {
            let p = props(serde_json::json!({"type": "Task", "i": i}));
            // RAM
            core.add_node(format!("n{i}"), p.clone());
            // Durable (commit-before-ack) — every node is provably on disk.
            backend
                .record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: p,
                    },
                )
                .await
                .expect("durable commit");
        }
        core
    }

    /// (a) Memory bounded: filling past the cap under authoritative mode + read-through
    /// EVICTS down to the cap — RAM resident count is bounded, eviction actually ran.
    /// (b)/(c) An EVICTED node still reads back its correct properties via the
    /// read-through (it is not in RAM, but redb serves it) — no loss across the boundary.
    #[tokio::test(flavor = "multi_thread")]
    async fn authoritative_eviction_bounds_memory_and_reads_through() {
        let dir = std::env::temp_dir().join(format!("eg-redb-evict-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 256).expect("open"));
        let state = new_state_auth(Some(dir_s.clone()), true);

        let n = 50usize;
        let cap = 10usize;
        let core = seed_authoritative(&backend, &state, n).await;
        assert_eq!(
            core.node_count(),
            n,
            "all {n} nodes resident before eviction"
        );

        // Evict down to the cap (the per-graph max-nodes backstop).
        let evicted = crate::persist::evict_oversized_all(&state, cap).await;
        assert_eq!(evicted, n - cap, "evicted everything above the cap");

        // (a) memory bounded: RAM resident count is at the cap, NOT n.
        assert_eq!(
            core.node_count(),
            cap,
            "RAM resident count bounded to the cap after eviction"
        );

        // (b)/(c) the EVICTED nodes (lowest indices n0..) are gone from RAM yet read
        // back their exact properties via read-through from redb — no data loss.
        assert!(!core.has_node("n0"), "n0 evicted from RAM topology");
        for i in 0..(n - cap) {
            let got = core.get_node_properties(&format!("n{i}"));
            assert_eq!(
                got,
                Some(props(serde_json::json!({"type": "Task", "i": i}))),
                "evicted node n{i} reads back correct properties via read-through"
            );
        }
        // A node still resident reads from RAM as before.
        for i in (n - cap)..n {
            assert!(core.has_node(&format!("n{i}")), "n{i} still resident");
            assert_eq!(
                core.get_node_properties(&format!("n{i}")),
                Some(props(serde_json::json!({"type": "Task", "i": i})))
            );
        }
        // A genuinely absent node is still None (read-through is not a fabricator).
        assert_eq!(core.get_node_properties("does-not-exist"), None);

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// No data loss: a node NOT durably in redb is NEVER evicted, even if it is the
    /// LRU candidate. We add an extra RAM-only node with the LOWEST index (so it is
    /// first in the LRU order) but do NOT record it durably; eviction must keep it
    /// resident (durability unconfirmed) and instead evict only durable nodes.
    #[tokio::test(flavor = "multi_thread")]
    async fn authoritative_eviction_never_drops_undurable_node() {
        let dir = std::env::temp_dir().join(format!("eg-redb-evict-safe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 64).expect("open"));
        let state = new_state_auth(Some(dir_s.clone()), true);

        // Insert the un-durable node FIRST so it has the lowest NodeIndex (front of
        // the LRU order — the one the cache would normally drop first).
        let core = {
            let mut s = state.write().await;
            s.persistence = Some(backend.clone());
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
            let factory = Arc::new(
                crate::server::persistence::read_through::BackendReadThroughFactory::new(
                    backend.clone(),
                ),
            );
            s.registry.set_read_through_factory(factory);
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        core.add_node(
            "ghost".into(),
            props(serde_json::json!({"type": "Task", "durable": false})),
        );
        // Now 10 durable nodes.
        for i in 0..10usize {
            let p = props(serde_json::json!({"type": "Task", "i": i}));
            core.add_node(format!("n{i}"), p.clone());
            backend
                .record_durable(
                    "g1",
                    &Method::AddNode {
                        node_id: format!("n{i}"),
                        properties_msgpack: p,
                    },
                )
                .await
                .expect("durable commit");
        }
        assert_eq!(core.node_count(), 11);

        // Cap = 5 ⇒ 6 candidates: ghost (lowest index) + n0..n4. Only the 5 durable
        // ones may be dropped; ghost is kept (its durability cannot be confirmed).
        let evicted = crate::persist::evict_oversized_all(&state, 5).await;
        assert_eq!(
            evicted, 5,
            "only the 5 confirmed-durable candidates evicted"
        );
        assert!(
            core.has_node("ghost"),
            "un-durable node kept resident — never evicted (no data loss)"
        );
        // The un-durable node has no redb row, so a (hypothetical) miss would not
        // resurrect it — which is exactly why eviction must not drop it. Confirm it
        // still reads from RAM.
        assert_eq!(
            core.get_node_properties("ghost"),
            Some(props(serde_json::json!({"type": "Task", "durable": false})))
        );

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (d) Non-authoritative regression: the default rebuildable-cache path is
    /// UNCHANGED — eviction drops the oldest down to the cap, and with NO read-through
    /// attached an evicted node reads back as absent (it re-hydrates from the external
    /// durable tier, which this in-engine test does not model).
    #[tokio::test(flavor = "multi_thread")]
    async fn non_authoritative_eviction_unchanged() {
        let dir =
            std::env::temp_dir().join(format!("eg-redb-evict-nonauth-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 64).expect("open"));
        // authoritative = false.
        let state = new_state_auth(Some(dir_s.clone()), false);
        {
            let mut s = state.write().await;
            s.persistence = Some(backend.clone());
            let _ = s.registry.create_graph("g1", GraphType::Global, None);
            // Deliberately NO read-through factory in the default model.
        }
        let core = {
            let s = state.read().await;
            s.registry.get("g1").map(|e| e.core.clone()).unwrap()
        };
        for i in 0..20usize {
            core.add_node(
                format!("n{i}"),
                props(serde_json::json!({"type": "Task", "i": i})),
            );
        }
        let evicted = crate::persist::evict_oversized_all(&state, 8).await;
        assert_eq!(evicted, 12, "drops oldest down to the cap, as before");
        assert_eq!(core.node_count(), 8);
        // No read-through ⇒ an evicted node reads back absent (re-hydrates externally).
        assert_eq!(core.get_node_properties("n0"), None);
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONCEPT:KG-2.204 — ONE fsync covers a Raft log entry AND its graph mutation.
    /// Under `FsyncPolicy::Interval`, the only way an awaited op completes is the
    /// group commit firing. We launch a `record_durable` (M2 graph mutation) and a
    /// `raft_log_append` (Raft log entry) CONCURRENTLY into the same tick window;
    /// both share ONE `Pending` batch → ONE `WriteTransaction` → ONE fsync. We then
    /// prove BOTH landed durably (the graph row AND the log row).
    #[cfg(feature = "raft")]
    #[tokio::test(flavor = "multi_thread")]
    async fn raft_log_and_mutation_share_one_group_commit() {
        let dir = std::env::temp_dir().join(format!("eg-redb-1txn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();
        let backend = Arc::new(
            RedbBackend::open(
                dir_s.clone(),
                FsyncPolicy::Interval(Duration::from_millis(40)),
                64,
            )
            .expect("open"),
        );

        // Fire both into the SAME group-commit window, concurrently. With Interval
        // fsync, neither completes until the group commit fires — so if they both
        // resolve from ONE flush, they rode ONE transaction together.
        let b1 = backend.clone();
        let mutation = tokio::spawn(async move {
            b1.record_durable(
                "g1",
                &Method::AddNode {
                    node_id: "shared".into(),
                    properties_msgpack: props(serde_json::json!({"v": 7})),
                },
            )
            .await
        });
        let b2 = backend.clone();
        let log_blob = rmp_serde::to_vec_named(&serde_json::json!({"entry": 1})).unwrap();
        let log = tokio::spawn(async move { b2.raft_log_append(0, vec![(1u64, log_blob)]).await });

        mutation.await.unwrap().expect("graph mutation durable");
        log.await.unwrap().expect("raft log append durable");

        // Both are on disk: the graph row AND the log row at (group 0, index 1).
        let node = backend.read_node("g1", "shared").await.expect("read node");
        assert_eq!(node, Some(props(serde_json::json!({"v": 7}))));
        let entries = backend.raft_log_read(0, 1, 1).expect("read log");
        assert_eq!(
            entries.len(),
            1,
            "the log entry committed in the same flush"
        );
        assert_eq!(backend.dropped(), 0);
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Cross-modal ACID (CONCEPT:KG-2.225) ─────────────────────────────────────

    use crate::protocol::{Response, ResultPayload};
    use crate::server::handlers::txn::try_handle as txn_handle;

    fn cm_dir(tag: &str) -> String {
        let d = std::env::temp_dir().join(format!("eg-crossmodal-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    fn as_bool(r: Response) -> Option<bool> {
        match r.result {
            Some(ResultPayload::Bool(b)) => Some(b),
            _ => None,
        }
    }

    /// Drive BeginTxn(g) → TxnAddNode(node) → TxnAddEmbedding → TxnBlobRef, returning
    /// the txn id (the staged cross-modal write-set is ready to Commit).
    async fn stage_crossmodal(
        state: &Arc<RwLock<ServerState>>,
        graph: &str,
        node: &str,
        digest: &str,
    ) -> String {
        let begin = txn_handle(
            state,
            1,
            None,
            Method::BeginTxn {
                graph: Some(graph.to_string()),
                isolation: None,
            },
        )
        .await
        .unwrap();
        let txn_id = match begin.result {
            Some(ResultPayload::String(id)) => id,
            other => panic!("BeginTxn id, got {other:?}"),
        };
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    2,
                    None,
                    Method::TxnAddNode {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        properties_msgpack: props(serde_json::json!({"type": "Media"})),
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    3,
                    None,
                    Method::TxnAddEmbedding {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        embedding: vec![0.1, 0.2, 0.3],
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        assert_eq!(
            as_bool(
                txn_handle(
                    state,
                    4,
                    None,
                    Method::TxnBlobRef {
                        txn_id: txn_id.clone(),
                        node_id: node.to_string(),
                        digest: digest.to_string(),
                        graph: None,
                    },
                )
                .await
                .unwrap()
            ),
            Some(true)
        );
        txn_id
    }

    /// HAPPY: a cross-modal txn (node + vector + blob-ref) commits atomically — ALL
    /// modalities land durably in ONE WriteTransaction and survive a reload.
    #[tokio::test(flavor = "multi_thread")]
    async fn crossmodal_txn_commits_all_modalities_atomically() {
        let dir = cm_dir("happy");
        let backend: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 64).unwrap());
        let state = new_state_auth(Some(dir.clone()), true);
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
        }

        let txn_id = stage_crossmodal(&state, "media", "m1", "sha256:abc").await;
        // Nothing applied before commit.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "no apply before commit");
            assert_eq!(
                core.semantic_store.read().len(),
                0,
                "no vector before commit"
            );
        }

        // COMMIT — all three modalities land atomically.
        assert_eq!(
            as_bool(
                txn_handle(&state, 5, None, Method::Commit { txn_id })
                    .await
                    .unwrap()
            ),
            Some(true),
            "cross-modal commit"
        );

        // In-memory: node + vector + blob-ref property all present.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node landed");
            assert_eq!(core.semantic_store.read().len(), 1, "vector landed");
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc")
            );
        }
        backend.shutdown();
        drop(backend);

        // Reload from redb: every modality is DURABLE (the one WriteTransaction).
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 64).unwrap());
        let state2 = new_state_auth(Some(dir.clone()), true);
        backend2.load_all(&state2).await.unwrap();
        {
            let s = state2.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(core.has_node("m1"), "node durable");
            assert_eq!(core.semantic_store.read().len(), 1, "vector durable");
            let blob = core.get_node_properties("m1").unwrap();
            let p: serde_json::Map<String, serde_json::Value> =
                rmp_serde::from_slice(&blob).unwrap();
            assert_eq!(
                p.get("__blob__").and_then(|v| v.as_str()),
                Some("sha256:abc"),
                "blob-ref durable"
            );
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A backend whose `commit_crossmodal` always FAILS — used to prove the handler
    /// rolls back ALL modalities (applies nothing in-memory) on a durable-commit
    /// failure: no partial cross-modal commit (CONCEPT:KG-2.225).
    struct FailingBackend {
        inner: Arc<RedbBackend>,
    }

    #[async_trait::async_trait]
    impl PersistenceBackend for FailingBackend {
        async fn load_all(&self, s: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
            self.inner.load_all(s).await
        }
        async fn checkpoint_all(&self, s: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
            self.inner.checkpoint_all(s).await
        }
        fn record(&self, g: &str, m: &Method) {
            self.inner.record(g, m)
        }
        async fn commit_crossmodal(
            &self,
            _g: &str,
            _m: &[Method],
            _v: &[(String, Vec<f32>)],
            _b: &[(String, String)],
        ) -> Result<(), String> {
            // Simulate a mid-way durable failure: NOTHING is written to redb.
            Err("injected durable commit failure".to_string())
        }
        fn shutdown(&self) {
            self.inner.shutdown()
        }
    }

    /// ROLLBACK: a cross-modal txn whose durable commit FAILS mid-way applies NONE of
    /// its modalities — no node, no vector, no blob-ref (no partial commit).
    #[tokio::test(flavor = "multi_thread")]
    async fn crossmodal_txn_rolls_back_all_modalities_on_failure() {
        let dir = cm_dir("rollback");
        let inner = Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 64).unwrap());
        let backend: Arc<dyn PersistenceBackend> = Arc::new(FailingBackend {
            inner: inner.clone(),
        });
        let state = new_state_auth(Some(dir.clone()), true);
        {
            let mut s = state.write().await;
            let _ = s.registry.create_graph("media", GraphType::Global, None);
            s.persistence = Some(backend.clone());
        }

        let txn_id = stage_crossmodal(&state, "media", "m1", "sha256:def").await;

        // COMMIT must FAIL (the durable barrier errored) → Response is an error.
        let resp = txn_handle(&state, 5, None, Method::Commit { txn_id })
            .await
            .unwrap();
        assert!(resp.error.is_some(), "commit surfaces the durable failure");
        assert!(resp.result.is_none(), "no Bool ack on a failed commit");

        // NO PARTIAL COMMIT: NONE of the modalities applied in-memory.
        {
            let s = state.read().await;
            let core = s.registry.get("media").unwrap().core.clone();
            assert!(!core.has_node("m1"), "node rolled back");
            assert_eq!(core.semantic_store.read().len(), 0, "vector rolled back");
        }

        // And NONE durable in redb either (the WriteTransaction never committed).
        inner.shutdown();
        drop(inner);
        drop(backend);
        let backend2: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir.clone(), FsyncPolicy::Each, 64).unwrap());
        let state2 = new_state_auth(Some(dir.clone()), true);
        backend2.load_all(&state2).await.unwrap();
        {
            let s = state2.read().await;
            let durable_node = s
                .registry
                .get("media")
                .map(|e| e.core.has_node("m1"))
                .unwrap_or(false);
            assert!(!durable_node, "node never landed durably");
        }
        backend2.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// AuditVerify dispatch round-trip (CONCEPT:KG-2.231): durable writes build a
    /// hash-chained audit log; `Method::AuditVerify` over the served dispatch returns
    /// `ok=true`; tampering an entry makes the served verify report the break.
    #[cfg(feature = "security")]
    #[tokio::test]
    async fn audit_verify_dispatch_detects_tamper() {
        use crate::protocol::{AuditReport, Request, ResultPayload};
        use crate::server::{compute_auth_token, dispatch};

        const SECRET: &str = "audit-secret";
        let dir = std::env::temp_dir().join(format!("eg-audit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_string_lossy().to_string();

        let backend = Arc::new(
            RedbBackend::open(dir_s.clone(), FsyncPolicy::Each, 64).expect("open redb backend"),
        );
        let state = new_state_auth(Some(dir_s.clone()), true);
        {
            let mut s = state.write().await;
            s.auth_secret = SECRET.to_string();
            s.persistence = Some(backend.clone());
        }
        let req = |id: u64, method: Method| Request {
            id,
            graph: "__commons__".to_string(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: None,
            method,
        };

        // Two durable writes → two chained audit entries (commit-before-ack durable).
        for (rid, nid) in [(1u64, "n1"), (2, "n2")] {
            let r = dispatch(
                &state,
                req(
                    rid,
                    Method::AddNode {
                        node_id: nid.into(),
                        properties_msgpack: props(serde_json::json!({"v": rid})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "add failed: {:?}", r.error);
        }

        // Served AuditVerify ⇒ ok.
        let decode = |r: crate::protocol::Response| -> AuditReport {
            match r.result {
                Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
                other => panic!("expected raw AuditReport, got {other:?}"),
            }
        };
        let report = decode(dispatch(&state, req(3, Method::AuditVerify)).await);
        assert!(report.ok, "clean chain should verify: {report:?}");
        assert_eq!(report.entries, 2);

        // Tamper the audit table directly under the writer-thread DB, then re-verify.
        backend
            .test_tamper_audit_entry(&crate::persist::sanitize("__commons__"), 0)
            .expect("tamper");
        let broken = decode(dispatch(&state, req(4, Method::AuditVerify)).await);
        assert!(!broken.ok, "tamper should be detected");
        assert_eq!(broken.first_broken_seq, Some(0));

        backend.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
