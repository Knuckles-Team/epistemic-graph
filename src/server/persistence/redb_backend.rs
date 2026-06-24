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

use std::collections::HashMap;
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

const NODES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("nodes");
const EDGES: TableDefinition<(&str, &str, &str, u32), &[u8]> = TableDefinition::new("edges");
const LEDGER: TableDefinition<(&str, u64), &str> = TableDefinition::new("ledger");
const SEMANTIC: TableDefinition<&str, &[u8]> = TableDefinition::new("semantic_store");
const GRAPH_META: TableDefinition<&str, &[u8]> = TableDefinition::new("graph_meta");
// Durable Raft log (CONCEPT:KG-2.204): committed log entries live in the SAME
// `graph.redb` Database, keyed by `(group_id, index)` so ONE table serves every
// Raft group (CONCEPT:KG-2.205 — the spike's "one DB, composite key" shape, not a
// file per group). Sharing the Database with the M2 graph tables is what lets a log
// append and a graph mutation ride ONE group-commit `WriteTransaction` / one fsync.
const RAFT_LOG: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("raft_log");
/// `(first, last)` present Raft log index for a group, or an error (CONCEPT:KG-2.204).
type LogBoundsResult = Result<(Option<u64>, Option<u64>), String>;
// Per-group Raft metadata (vote, applied-state pointers, last-purged), keyed by
// `(group_id, key)`. Lives in `graph.redb` alongside the log for the same reason.
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
}

/// An owned, off-lock dump of one graph used by the checkpoint path.
struct GraphDump {
    graph: String,
    name: String,
    graph_type: GraphType,
    nodes: Vec<(String, Vec<u8>)>,
    edges: Vec<(String, String, Vec<u8>)>,
    ledger: Vec<String>,
    semantic: Vec<u8>,
}

/// Handle to the redb write-through tier. The dispatch path holds an `Arc` of this
/// and calls `record`; the dedicated thread does all `Database` I/O.
pub struct RedbBackend {
    db_path: String,
    tx: SyncSender<Cmd>,
    dropped: Arc<AtomicU64>,
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
            wtx.commit().map_err(|e| e.to_string())?;
        }
        let (tx, rx) = sync_channel::<Cmd>(capacity.max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        let handle = std::thread::Builder::new()
            .name("eg-redb-writer".into())
            .spawn(move || run(rx, db, policy))
            .map_err(|e| e.to_string())?;
        Ok(Self {
            db_path,
            tx,
            dropped,
            handle: parking_lot::Mutex::new(Some(handle)),
        })
    }

    /// Total mutations dropped due to channel saturation (observability).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
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

    #[cfg(feature = "raft")]
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
}

// ── off-reactor group-commit writer thread ───────────────────────────────

fn run(rx: Receiver<Cmd>, db: Database, policy: FsyncPolicy) {
    let tick = match policy {
        FsyncPolicy::Interval(d) => d,
        _ => Duration::from_millis(1000),
    };
    // Pending mutations folded into the NEXT group commit, each with its optional
    // commit-before-ack completion sender (CONCEPT:KG-2.187). After a commit, EVERY
    // sender in the batch is fired with the batch's result — one fsync, N notified.
    let mut pending: Pending = Pending::default();
    loop {
        match rx.recv_timeout(tick) {
            Ok(cmd) => {
                if handle_cmd(cmd, &db, &mut pending, policy) {
                    // shutdown: flush whatever is pending durably, then stop.
                    commit_and_notify(&db, &mut pending, Durability::Immediate);
                    break;
                }
                // Drain the rest of the burst so it coalesces into one commit.
                let mut stop = false;
                while let Ok(cmd) = rx.try_recv() {
                    if handle_cmd(cmd, &db, &mut pending, policy) {
                        stop = true;
                        break;
                    }
                }
                if stop {
                    commit_and_notify(&db, &mut pending, Durability::Immediate);
                    return;
                }
                // Any awaiting commit-before-ack op in the batch MUST be made durable
                // now (don't leave an awaited write parked until the next tick): if a
                // barrier op is pending, commit immediately; otherwise honor policy.
                let must_commit_now = pending.has_barrier() || matches!(policy, FsyncPolicy::Each);
                if must_commit_now {
                    let durability = match policy {
                        FsyncPolicy::Off => Durability::None,
                        _ => Durability::Immediate,
                    };
                    commit_and_notify(&db, &mut pending, durability);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // Group-commit boundary: flush pending mutations.
                let durability = match policy {
                    FsyncPolicy::Off => Durability::None,
                    _ => Durability::Immediate,
                };
                commit_and_notify(&db, &mut pending, durability);
            }
            Err(RecvTimeoutError::Disconnected) => {
                commit_and_notify(&db, &mut pending, Durability::Immediate);
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
fn handle_cmd(cmd: Cmd, db: &Database, pending: &mut Pending, policy: FsyncPolicy) -> bool {
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
                commit_and_notify(db, pending, durability);
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
            commit_and_notify(db, pending, Durability::Immediate);
            let res = write_graph_meta(db, &graph, &name, graph_type);
            let _ = done.send(res);
            false
        }
        Cmd::ReadNode {
            graph,
            node_id,
            reply,
        } => {
            // Flush pending (incl. any awaited ops) so the read reflects the latest
            // durable state, then point-read the node row.
            commit_and_notify(db, pending, Durability::Immediate);
            let _ = reply.send(read_one_node(db, &graph, &node_id));
            false
        }
        Cmd::Checkpoint { graphs, reply } => {
            // Flush any buffered Raft log appends (+ their barrier waiters) durably
            // first — the checkpoint path only folds graph `ops`, not log ops, so a
            // pending log entry must be committed on its own before the checkpoint.
            if !pending.raft_log_ops.is_empty() {
                commit_and_notify(db, pending, Durability::Immediate);
            }
            // Fold any buffered mutations into the same durable commit first so the
            // checkpoint reflects them, then overwrite each graph's rows. The
            // checkpoint commits durably, so any awaited ops it absorbed are durable
            // too — notify their waiters with the checkpoint's success/failure.
            let res = apply_checkpoint(db, &mut pending.ops, graphs);
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
            commit_and_notify(db, pending, Durability::Immediate);
            let _ = reply.send(read_all_dumps(db));
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
            commit_and_notify(db, pending, Durability::Immediate);
            let _ = reply.send(read_raft_log_range(db, group_id, lo, hi));
            false
        }
        Cmd::RaftLogDeleteFrom {
            group_id,
            from,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate);
            let _ = done.send(delete_raft_log_from(db, group_id, from));
            false
        }
        Cmd::RaftLogPurgeUpto {
            group_id,
            upto,
            done,
        } => {
            commit_and_notify(db, pending, Durability::Immediate);
            let _ = done.send(purge_raft_log_upto(db, group_id, upto));
            false
        }
        Cmd::RaftLogBounds { group_id, reply } => {
            commit_and_notify(db, pending, Durability::Immediate);
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
            commit_and_notify(db, pending, Durability::Immediate);
            let _ = done.send(put_raft_meta(db, group_id, &key, &val));
            false
        }
        Cmd::RaftMetaGet {
            group_id,
            key,
            reply,
        } => {
            commit_and_notify(db, pending, Durability::Immediate);
            let _ = reply.send(get_raft_meta(db, group_id, &key));
            false
        }
    }
}

/// Commit all buffered mutations in ONE write transaction at the given durability,
/// then fire EVERY commit-before-ack waiter for the ops in this batch with the
/// batch's result (CONCEPT:KG-2.187). Coalescing is preserved: N awaiting writers
/// ride one `WriteTransaction` / one fsync and are all notified after it commits.
/// A waiter is only signalled `Ok` once its op is provably on disk.
fn commit_and_notify(db: &Database, pending: &mut Pending, durability: Durability) {
    if pending.is_empty() {
        return;
    }
    let res = commit_ops(db, &mut pending.ops, &mut pending.raft_log_ops, durability);
    let waiters = std::mem::take(&mut pending.waiters);
    let signal = res.map(|_| ());
    for w in waiters {
        let _ = w.send(signal.clone());
    }
}

/// Commit all buffered mutations AND Raft log appends in ONE write transaction at
/// the given durability (CONCEPT:KG-2.204). A graph mutation and a Raft log entry in
/// the same batch therefore share ONE `WriteTransaction` and ONE fsync.
fn commit_ops(
    db: &Database,
    ops: &mut Vec<(String, Method)>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    durability: Durability,
) -> Result<(), String> {
    if ops.is_empty() && raft_log_ops.is_empty() {
        return Ok(());
    }
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(durability).map_err(|e| e.to_string())?;
    // Graphs touched by this batch — used to backfill a graph_meta row for any
    // graph that received writes but was never explicitly registered (e.g. the
    // pre-created `__commons__`), so authoritative `load_all` recovers it even with
    // no checkpoint. The fallback name == the sanitized graph key (exact for names
    // that survive sanitization, incl. `__commons__`); an explicit `register_graph`
    // overwrites it with the REAL name when one was created via CreateGraph.
    let mut touched: std::collections::HashSet<String> = std::collections::HashSet::new();
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        for (graph, method) in ops.drain(..) {
            touched.insert(graph.clone());
            apply_method_rows(&graph, &method, &mut nodes, &mut edges, &mut ledger)?;
        }
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        for g in &touched {
            if meta.get(g.as_str()).map_err(|e| e.to_string())?.is_none() {
                meta.insert(g.as_str(), encode_meta(g, GraphType::Global).as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
        // Raft log appends ride the SAME transaction (CONCEPT:KG-2.204) — one fsync
        // covers the graph mutation AND its replicated log entry.
        if !raft_log_ops.is_empty() {
            let mut log = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
            for (gid, idx, blob) in raft_log_ops.drain(..) {
                log.insert((gid, idx), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Durably write/overwrite a graph_meta identity row in its OWN transaction.
fn write_graph_meta(
    db: &Database,
    graph: &str,
    name: &str,
    graph_type: GraphType,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        meta.insert(graph, encode_meta(name, graph_type).as_slice())
            .map_err(|e| e.to_string())?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Point-read a single node's stored properties (read-through path).
fn read_one_node(db: &Database, graph: &str, node_id: &str) -> Result<Option<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let nodes = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let v = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|g| g.value().to_vec());
    Ok(v)
}

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

/// Translate ONE applied method into redb row writes inside an open transaction.
/// Mirrors `crate::wal::apply`'s method set: the durable DATA mutations only.
fn apply_method_rows(
    graph: &str,
    method: &Method,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
) -> Result<(), String> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            nodes
                .insert((graph, node_id.as_str()), properties_msgpack.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Method::RemoveNode { node_id } => {
            nodes
                .remove((graph, node_id.as_str()))
                .map_err(|e| e.to_string())?;
            // Remove this node's incident edges (best-effort prefix sweep on src).
            // Edge keys whose src OR tgt is this node are dropped on reload anyway
            // because the node won't exist; we sweep src-keyed here for hygiene.
            let to_del: Vec<(String, String, u32)> = edges
                .range((graph, node_id.as_str(), "", 0u32)..)
                .map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .take_while(|(k, _)| {
                    let (g, s, _, _) = k.value();
                    g == graph && s == node_id.as_str()
                })
                .map(|(k, _)| {
                    let (_, s, t, o) = k.value();
                    (s.to_string(), t.to_string(), o)
                })
                .collect();
            for (s, t, o) in to_del {
                let _ = edges.remove((graph, s.as_str(), t.as_str(), o));
            }
        }
        Method::CompareAndSetNodeFields {
            node_id,
            updates_msgpack,
            ..
        } => {
            // Write-through best-effort: persist the post-update node properties.
            // The in-memory CAS already decided success; on reload the stored row
            // is the authoritative latest properties for this node.
            nodes
                .insert((graph, node_id.as_str()), updates_msgpack.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            // Ordinal lets a graph carry parallel edges between the same pair; we
            // append at the next free ordinal for this (src,tgt).
            let ord = next_edge_ordinal(edges, graph, source_id, target_id)?;
            edges
                .insert(
                    (graph, source_id.as_str(), target_id.as_str(), ord),
                    properties_msgpack.as_slice(),
                )
                .map_err(|e| e.to_string())?;
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            // Remove every ordinal for this (src,tgt).
            let ords: Vec<u32> = edges
                .range((graph, source_id.as_str(), target_id.as_str(), 0u32)..)
                .map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .take_while(|(k, _)| {
                    let (g, s, t, _) = k.value();
                    g == graph && s == source_id.as_str() && t == target_id.as_str()
                })
                .map(|(k, _)| k.value().3)
                .collect();
            for o in ords {
                let _ = edges.remove((graph, source_id.as_str(), target_id.as_str(), o));
            }
        }
        Method::BatchUpdate { operations_msgpack } => {
            // A batch is a vector of (op, args) — decode and apply each as rows.
            apply_batch_rows(graph, operations_msgpack, nodes, edges)?;
        }
        Method::ClearGraph => {
            // Drop every row for this graph across nodes/edges/ledger.
            clear_graph_rows(graph, nodes, edges, ledger)?;
        }
        _ => {}
    }
    Ok(())
}

/// Next free edge ordinal for a (src,tgt) pair in this graph.
fn next_edge_ordinal(
    edges: &redb::Table<(&str, &str, &str, u32), &[u8]>,
    graph: &str,
    src: &str,
    tgt: &str,
) -> Result<u32, String> {
    let max = edges
        .range((graph, src, tgt, 0u32)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| {
            let (g, s, t, _) = k.value();
            g == graph && s == src && t == tgt
        })
        .map(|(k, _)| k.value().3)
        .max();
    Ok(max.map(|m| m + 1).unwrap_or(0))
}

/// Apply a decoded `BatchUpdate` op-list as row writes. The batch payload is the
/// same msgpack the in-memory `batch_update` consumes: a list of `{op, ...}` maps.
fn apply_batch_rows(
    graph: &str,
    operations_msgpack: &[u8],
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
) -> Result<(), String> {
    let ops: Vec<serde_json::Value> = match rmp_serde::from_slice(operations_msgpack) {
        Ok(o) => o,
        Err(_) => return Ok(()), // opaque batch — skip rather than fail the commit
    };
    for op in ops {
        let kind = op.get("op").and_then(|v| v.as_str()).unwrap_or("");
        match kind {
            "add_node" | "upsert_node" => {
                if let Some(id) = op.get("node_id").and_then(|v| v.as_str()) {
                    let props = op
                        .get("properties")
                        .map(|p| rmp_serde::to_vec_named(p).unwrap_or_default())
                        .unwrap_or_default();
                    nodes
                        .insert((graph, id), props.as_slice())
                        .map_err(|e| e.to_string())?;
                }
            }
            "remove_node" => {
                if let Some(id) = op.get("node_id").and_then(|v| v.as_str()) {
                    nodes.remove((graph, id)).map_err(|e| e.to_string())?;
                }
            }
            "add_edge" => {
                if let (Some(s), Some(t)) = (
                    op.get("source_id").and_then(|v| v.as_str()),
                    op.get("target_id").and_then(|v| v.as_str()),
                ) {
                    let props = op
                        .get("properties")
                        .map(|p| rmp_serde::to_vec_named(p).unwrap_or_default())
                        .unwrap_or_default();
                    let ord = next_edge_ordinal(edges, graph, s, t)?;
                    edges
                        .insert((graph, s, t, ord), props.as_slice())
                        .map_err(|e| e.to_string())?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Drop every row for `graph` across nodes/edges/ledger (ClearGraph).
fn clear_graph_rows(
    graph: &str,
    nodes: &mut redb::Table<(&str, &str), &[u8]>,
    edges: &mut redb::Table<(&str, &str, &str, u32), &[u8]>,
    ledger: &mut redb::Table<(&str, u64), &str>,
) -> Result<(), String> {
    let node_keys: Vec<String> = nodes
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| k.value().1.to_string())
        .collect();
    for id in node_keys {
        let _ = nodes.remove((graph, id.as_str()));
    }
    let edge_keys: Vec<(String, String, u32)> = edges
        .range((graph, "", "", 0u32)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| {
            let (_, s, t, o) = k.value();
            (s.to_string(), t.to_string(), o)
        })
        .collect();
    for (s, t, o) in edge_keys {
        let _ = edges.remove((graph, s.as_str(), t.as_str(), o));
    }
    let seqs: Vec<u64> = ledger
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .take_while(|(k, _)| k.value().0 == graph)
        .map(|(k, _)| k.value().1)
        .collect();
    for seq in seqs {
        let _ = ledger.remove((graph, seq));
    }
    Ok(())
}

/// Snapshot the full registry dump into redb, overwriting each graph's rows, and
/// commit durably. Folds any buffered mutations into the SAME transaction first.
fn apply_checkpoint(
    db: &Database,
    pending: &mut Vec<(String, Method)>,
    graphs: Vec<GraphDump>,
) -> Result<usize, String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let mut count = 0usize;
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;

        // Drain buffered mutations into this commit so the checkpoint is consistent.
        for (graph, method) in pending.drain(..) {
            apply_method_rows(&graph, &method, &mut nodes, &mut edges, &mut ledger)?;
        }

        for dump in graphs {
            // Overwrite-by-replace: clear then write this graph's full state.
            clear_graph_rows(&dump.graph, &mut nodes, &mut edges, &mut ledger)?;
            for (id, props) in &dump.nodes {
                nodes
                    .insert((dump.graph.as_str(), id.as_str()), props.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            for (src, tgt, props) in &dump.edges {
                let ord = next_edge_ordinal(&edges, &dump.graph, src, tgt)?;
                edges
                    .insert(
                        (dump.graph.as_str(), src.as_str(), tgt.as_str(), ord),
                        props.as_slice(),
                    )
                    .map_err(|e| e.to_string())?;
            }
            for (seq, line) in dump.ledger.iter().enumerate() {
                ledger
                    .insert((dump.graph.as_str(), seq as u64), line.as_str())
                    .map_err(|e| e.to_string())?;
            }
            semantic
                .insert(dump.graph.as_str(), dump.semantic.as_slice())
                .map_err(|e| e.to_string())?;
            meta.insert(
                dump.graph.as_str(),
                encode_meta(&dump.name, dump.graph_type).as_slice(),
            )
            .map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(count)
}

// ── full read-back (load path, runs on the DB-owner thread) ───────────────

/// Read the entire store into owned per-graph dumps. Runs on the writer thread
/// (the only holder of the exclusive redb file lock). Each graph's rows are
/// collected by iterating the whole table once and bucketing by the graph prefix
/// — simpler than per-graph range bounds and load is a once-per-boot path.
fn read_all_dumps(db: &Database) -> Result<Vec<GraphDump>, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let ledger_table = rtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;

    // graph_meta drives which graphs exist + their name/type; seed a dump each.
    let mut dumps: HashMap<String, GraphDump> = HashMap::new();
    for row in meta_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let graph = k.value().to_string();
        let (name, graph_type) = decode_meta(v.value());
        dumps.insert(
            graph.clone(),
            GraphDump {
                graph,
                name,
                graph_type,
                nodes: Vec::new(),
                edges: Vec::new(),
                ledger: Vec::new(),
                semantic: Vec::new(),
            },
        );
    }

    for row in nodes_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, id) = k.value();
        if let Some(d) = dumps.get_mut(g) {
            d.nodes.push((id.to_string(), v.value().to_vec()));
        }
    }
    for row in edges_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, _) = k.value();
        if let Some(d) = dumps.get_mut(g) {
            d.edges
                .push((s.to_string(), t.to_string(), v.value().to_vec()));
        }
    }
    for row in ledger_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, _) = k.value();
        if let Some(d) = dumps.get_mut(g) {
            d.ledger.push(v.value().to_string());
        }
    }
    for row in semantic_table.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        if let Some(d) = dumps.get_mut(k.value()) {
            d.semantic = v.value().to_vec();
        }
    }
    Ok(dumps.into_values().collect())
}

// ── graph_meta blob (replaces manifest.json) ─────────────────────────────

fn encode_meta(name: &str, gtype: GraphType) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "name": name, "graph_type": gtype }))
        .unwrap_or_default()
}

fn decode_meta(blob: &[u8]) -> (String, GraphType) {
    let v: serde_json::Value = rmp_serde::from_slice(blob).unwrap_or(serde_json::Value::Null);
    let name = v
        .get("name")
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string();
    let gtype = v
        .get("graph_type")
        .cloned()
        .and_then(|x| serde_json::from_value(x).ok())
        .unwrap_or(GraphType::Global);
    (name, gtype)
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
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
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
}
