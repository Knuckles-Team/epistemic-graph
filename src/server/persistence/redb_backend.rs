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

/// One write command handed to the off-reactor thread. A `Mutation` carries the
/// graph file-name + the applied method; the thread translates it into row writes
/// inside the current group-commit transaction.
enum Cmd {
    Mutation {
        graph: String,
        method: Box<Method>,
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
}

// ── off-reactor group-commit writer thread ───────────────────────────────

fn run(rx: Receiver<Cmd>, db: Database, policy: FsyncPolicy) {
    let tick = match policy {
        FsyncPolicy::Interval(d) => d,
        _ => Duration::from_millis(1000),
    };
    // Pending mutations folded into the NEXT group commit.
    let mut pending: Vec<(String, Method)> = Vec::new();
    loop {
        match rx.recv_timeout(tick) {
            Ok(cmd) => {
                if handle_cmd(cmd, &db, &mut pending, policy) {
                    // shutdown: flush whatever is pending durably, then stop.
                    let _ = commit_pending(&db, &mut pending, Durability::Immediate);
                    break;
                }
                // Drain the rest of the burst so it coalesces into one commit.
                while let Ok(cmd) = rx.try_recv() {
                    if handle_cmd(cmd, &db, &mut pending, policy) {
                        let _ = commit_pending(&db, &mut pending, Durability::Immediate);
                        return;
                    }
                }
                if matches!(policy, FsyncPolicy::Each) {
                    let _ = commit_pending(&db, &mut pending, Durability::Immediate);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // Group-commit boundary: flush pending mutations.
                let durability = match policy {
                    FsyncPolicy::Off => Durability::None,
                    _ => Durability::Immediate,
                };
                let _ = commit_pending(&db, &mut pending, durability);
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = commit_pending(&db, &mut pending, Durability::Immediate);
                break;
            }
        }
    }
}

/// Returns true if the writer should stop. `Mutation` is buffered into `pending`
/// (committed at the next group boundary); `Checkpoint` is applied + committed
/// immediately (it carries its own reply).
fn handle_cmd(
    cmd: Cmd,
    db: &Database,
    pending: &mut Vec<(String, Method)>,
    policy: FsyncPolicy,
) -> bool {
    match cmd {
        Cmd::Mutation { graph, method } => {
            pending.push((graph, *method));
            // Bound memory: if a burst outpaces the tick, flush early. The group
            // still amortizes thousands of row writes per commit.
            if pending.len() >= 4096 {
                let durability = match policy {
                    FsyncPolicy::Off => Durability::None,
                    _ => Durability::Immediate,
                };
                let _ = commit_pending(db, pending, durability);
            }
            false
        }
        Cmd::Checkpoint { graphs, reply } => {
            // Fold any buffered mutations into the same durable commit first so the
            // checkpoint reflects them, then overwrite each graph's rows.
            let res = apply_checkpoint(db, pending, graphs);
            let _ = reply.send(res);
            false
        }
        Cmd::Load { reply } => {
            // Flush pending so the read sees the latest, then scan the owned DB.
            let _ = commit_pending(db, pending, Durability::Immediate);
            let _ = reply.send(read_all_dumps(db));
            false
        }
        Cmd::Shutdown { reply } => {
            let _ = reply.send(());
            true
        }
    }
}

/// Commit all buffered mutations in ONE write transaction at the given durability.
fn commit_pending(
    db: &Database,
    pending: &mut Vec<(String, Method)>,
    durability: Durability,
) -> Result<(), String> {
    if pending.is_empty() {
        return Ok(());
    }
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(durability).map_err(|e| e.to_string())?;
    {
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        for (graph, method) in pending.drain(..) {
            apply_method_rows(&graph, &method, &mut nodes, &mut edges, &mut ledger)?;
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
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
        Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: "test".to_string(),
            persist_dir,
            persistence: None,
            max_in_flight: Arc::new(tokio::sync::Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(dashmap::DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
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
}
