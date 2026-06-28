//! Online single-node per-tenant resharding execution (CONCEPT:EG-032, M3 keystone).
//!
//! ## What it solves
//!
//! [`super::shard_migrate`] (CONCEPT:EG-030) moves shards OFFLINE — the engine must be
//! stopped because it rewrites the whole store to a new uniform K. This module moves ONE
//! graph between shards while the engine RUNS, with no data loss and no stop: the graph's
//! rows are copied verbatim to the destination shard, the [`super::tenant_catalog`]
//! (CONCEPT:EG-031) route is flipped so reads/writes follow the graph to its new shard,
//! and the source rows are GC'd — all without touching any OTHER graph's writers.
//!
//! ## Correctness — the same verbatim row copy as EG-030
//!
//! Like the offline tool, the per-graph copy is **verbatim** (stored bytes are NOT
//! decoded / unsealed / re-derived):
//!
//! * Per-graph data (`NODES`/`EDGES`/`LEDGER`/`SEMANTIC`/`GRAPH_META`) is moved row for
//!   row, value blob unchanged — so encryption-at-rest blobs survive WITHOUT the key.
//! * The tamper-evident hash-chained `AUDIT` log (CONCEPT:KG-2.231) is copied verbatim
//!   `(graph, seq) -> blob`, so the chain stays verifiable (re-deriving would break it).
//!
//! ## Correctness — the quiesce / flip window
//!
//! The move is driven by [`RedbBackend::reshard_graph`](super::redb_backend::RedbBackend),
//! which holds the backend's `routing_epoch` WRITE guard for the duration of the move.
//! Every catalog-attached durable write (`record_durable` / `commit_crossmodal`) resolves
//! its shard and enqueues its op while holding a SHARED `routing_epoch` READ guard, so the
//! exclusive flip cannot interleave: when the flip holds the write guard, no write is
//! mid-resolve, and once it releases, every subsequent write resolves the catalog AFTER
//! the route flip — so a write is never lost or routed to the stale shard. The ordering
//! inside the move — `import(dst) durably committed -> catalog flip durable -> purge(src)`
//! — is crash-consistent: a crash before the flip leaves the data in BOTH shards with the
//! route still on `src` (reads find it on `src`); a crash after the flip leaves the route
//! on `dst` where `import` already landed the data; the `src` GC is the last, idempotent
//! step. See `RedbBackend::reshard_graph` for the quiesce wiring and the round-trip tests.

use std::sync::mpsc::SyncSender;

use redb::{Database, Durability, ReadableDatabase};

use super::redb_backend::Cmd;
use super::tenant_catalog::TenantCatalog;
#[cfg(feature = "security")]
use crate::redb_store::AUDIT;
use crate::redb_store::{EDGES, GRAPH_META, LEDGER, NODES, SEMANTIC};

/// One graph's durable rows captured VERBATIM for an online shard move (CONCEPT:EG-032).
/// Value blobs are the raw on-disk bytes (encrypted if encryption-at-rest is on, the
/// audit chain untouched) so re-inserting them on the destination shard preserves both
/// encryption and audit-chain verifiability — exactly as EG-030's offline copy does.
#[derive(Default)]
pub(crate) struct RawGraphRows {
    /// `graph_meta` identity blob (`{name, graph_type}`), or `None` if the graph has no
    /// durable identity (nothing to move — the reshard becomes a pure route flip).
    pub meta: Option<Vec<u8>>,
    /// `(node_id, raw_value_blob)`.
    pub nodes: Vec<(String, Vec<u8>)>,
    /// `(src_id, tgt_id, ordinal, raw_value_blob)`.
    pub edges: Vec<(String, String, u32, Vec<u8>)>,
    /// `(seq, ledger_line)`.
    pub ledger: Vec<(u64, String)>,
    /// The semantic-store blob, if any.
    pub semantic: Option<Vec<u8>>,
    /// `(seq, chained_audit_blob)` — copied verbatim to keep the hash chain valid.
    #[cfg(feature = "security")]
    pub audit: Vec<(u64, Vec<u8>)>,
}

/// Per-table counts of a completed online reshard (CONCEPT:EG-032).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReshardReport {
    pub graph: String,
    pub from_shard: usize,
    pub to_shard: usize,
    pub nodes: u64,
    pub edges: u64,
    pub ledger: u64,
    pub semantic: u64,
    pub audit: u64,
    /// `true` when the graph already routed to the target shard (nothing moved).
    pub no_op: bool,
}

impl ReshardReport {
    /// The graph already lives on the requested shard — nothing to move.
    pub fn no_op(graph: &str, shard: usize) -> Self {
        ReshardReport {
            graph: graph.to_string(),
            from_shard: shard,
            to_shard: shard,
            no_op: true,
            ..Default::default()
        }
    }

    fn counts(graph: &str, from: usize, to: usize, rows: &RawGraphRows) -> Self {
        ReshardReport {
            graph: graph.to_string(),
            from_shard: from,
            to_shard: to,
            nodes: rows.nodes.len() as u64,
            edges: rows.edges.len() as u64,
            ledger: rows.ledger.len() as u64,
            semantic: rows.semantic.is_some() as u64,
            #[cfg(feature = "security")]
            audit: rows.audit.len() as u64,
            #[cfg(not(feature = "security"))]
            audit: 0,
            no_op: false,
        }
    }
}

/// Scan ONE graph's durable rows VERBATIM off a `Database` (CONCEPT:EG-032). Runs on the
/// owning shard's writer thread (via [`Cmd::ExportGraphRaw`]) AFTER it has flushed pending
/// writes, so the snapshot reflects every committed mutation. Value blobs are copied raw
/// (no `crypto.unseal`) so encryption-at-rest and the audit chain survive the move.
pub(crate) fn export_graph_raw(db: &Database, graph: &str) -> Result<RawGraphRows, String> {
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let mut out = RawGraphRows::default();

    let meta_table = rtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    out.meta = meta_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec());

    let nodes_table = rtx.open_table(NODES).map_err(|e| e.to_string())?;
    for row in nodes_table
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, id) = k.value();
        if g != graph {
            break;
        }
        out.nodes.push((id.to_string(), v.value().to_vec()));
    }

    let edges_table = rtx.open_table(EDGES).map_err(|e| e.to_string())?;
    for row in edges_table
        .range((graph, "", "", 0u32)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, s, t, o) = k.value();
        if g != graph {
            break;
        }
        out.edges
            .push((s.to_string(), t.to_string(), o, v.value().to_vec()));
    }

    let ledger_table = rtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    for row in ledger_table
        .range((graph, 0u64)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let (g, seq) = k.value();
        if g != graph {
            break;
        }
        out.ledger.push((seq, v.value().to_string()));
    }

    let semantic_table = rtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    out.semantic = semantic_table
        .get(graph)
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec());

    #[cfg(feature = "security")]
    {
        let audit_table = rtx.open_table(AUDIT).map_err(|e| e.to_string())?;
        for row in audit_table
            .range((graph, 0u64)..)
            .map_err(|e| e.to_string())?
        {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let (g, seq) = k.value();
            if g != graph {
                break;
            }
            out.audit.push((seq, v.value().to_vec()));
        }
    }

    Ok(out)
}

/// Insert ONE graph's verbatim rows into a destination `Database` in ONE durable
/// transaction (CONCEPT:EG-032). Runs on the destination shard's writer thread (via
/// [`Cmd::ImportGraphRaw`]); the single `Durability::Immediate` commit is the
/// commit-before-ack point of the move. Idempotent (keyed upserts), so a re-run after an
/// interrupted move overwrites rather than duplicates.
pub(crate) fn import_graph_raw(
    db: &Database,
    graph: &str,
    rows: &RawGraphRows,
) -> Result<(), String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    {
        let mut meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
        if let Some(blob) = &rows.meta {
            meta.insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        for (id, blob) in &rows.nodes {
            nodes
                .insert((graph, id.as_str()), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
        for (s, t, o, blob) in &rows.edges {
            edges
                .insert((graph, s.as_str(), t.as_str(), *o), blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        let mut ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
        for (seq, line) in &rows.ledger {
            ledger
                .insert((graph, *seq), line.as_str())
                .map_err(|e| e.to_string())?;
        }
        let mut semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
        if let Some(blob) = &rows.semantic {
            semantic
                .insert(graph, blob.as_slice())
                .map_err(|e| e.to_string())?;
        }
        #[cfg(feature = "security")]
        {
            let mut audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;
            for (seq, blob) in &rows.audit {
                audit
                    .insert((graph, *seq), blob.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

/// Drive the four-step online move once the routing quiesce (the `routing_epoch` WRITE
/// guard) is held (CONCEPT:EG-032). Runs on a blocking thread: it talks to BOTH shard
/// writer threads over their channels, so it never touches the redb file handles directly
/// (each shard holds an exclusive per-file lock). Ordering is crash-consistent — see the
/// module docs.
pub(crate) fn execute_online_reshard(
    src_tx: &SyncSender<Cmd>,
    dst_tx: &SyncSender<Cmd>,
    catalog: &TenantCatalog,
    graph: &str,
    src_idx: usize,
    dst_idx: usize,
) -> Result<ReshardReport, String> {
    let gone = || "redb writer thread is gone".to_string();

    // 1. Export the graph's rows VERBATIM from the source shard (flush-then-scan).
    let (rtx, rrx) = std::sync::mpsc::channel();
    src_tx
        .send(Cmd::ExportGraphRaw {
            graph: graph.to_string(),
            reply: rtx,
        })
        .map_err(|_| gone())?;
    let rows = rrx
        .recv()
        .map_err(|_| "redb writer dropped export reply".to_string())??;
    let report = ReshardReport::counts(graph, src_idx, dst_idx, &rows);

    // 2. Import them into the destination shard, awaiting the durable commit.
    let (itx, irx) = std::sync::mpsc::channel();
    dst_tx
        .send(Cmd::ImportGraphRaw {
            graph: graph.to_string(),
            rows: Box::new(rows),
            reply: itx,
        })
        .map_err(|_| gone())?;
    irx.recv()
        .map_err(|_| "redb writer dropped import reply".to_string())??;

    // 3. Flip the durable route — reads/writes now follow the graph to `dst` (preserving
    //    any cluster-node placement). This is the atomic commit point of the move.
    let node = catalog.lookup(graph).and_then(|a| a.node);
    catalog.assign(graph, dst_idx as u32, node)?;

    // 4. GC the now-orphaned source rows (idempotent purge). After the flip no writer can
    //    route to `src` for this graph, so the purge can never race a live write.
    let (done, done_rx) = tokio::sync::oneshot::channel();
    src_tx
        .send(Cmd::PurgeGraph {
            graph: graph.to_string(),
            done,
        })
        .map_err(|_| gone())?;
    done_rx
        .blocking_recv()
        .map_err(|_| "redb writer dropped purge reply".to_string())??;

    Ok(report)
}
