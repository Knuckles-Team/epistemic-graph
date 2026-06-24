//! Pluggable durable persistence backend (CONCEPT:KG-2.177).
//!
//! The engine's durability has been a single hard-wired pair of paths: the
//! snapshot RDB (`persist.rs`) + the off-reactor WAL (`wal_service.rs`). This
//! module lifts that behind a [`PersistenceBackend`] trait so a different durable
//! tier can be selected at boot without touching the dispatch or main wiring —
//! the two centralized seams ([`ServerState::persistence`] and the dispatch
//! write-side-effect block) now talk to a trait object instead of a `WalService`.
//!
//! Two implementations ship:
//!
//! * [`snapshot_wal::SnapshotWalBackend`] — the DEFAULT. It wraps today's logic
//!   verbatim (delegates `load_all`/`checkpoint_all` to the existing `persist.rs`
//!   functions and owns the [`WalService`](crate::wal_service::WalService)
//!   internally for `record`). Zero behavior change — the shipped engine is byte
//!   identical in behavior to before this trait existed.
//! * [`redb_backend::RedbBackend`] — a feature-gated (`redb`) WRITE-THROUGH tier
//!   that mirrors every mutation into an embedded `redb` database, reusing the
//!   off-reactor + group-commit threading model so write p99 doesn't collapse.
//!   It is NOT authoritative yet — it writes beside the existing model, selected
//!   by `EPISTEMIC_GRAPH_PERSIST_BACKEND=redb`.
//!
//! Backend selection (parsed once in `main.rs`):
//! `EPISTEMIC_GRAPH_PERSIST_BACKEND=snapshot|redb` (default `snapshot`).

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Method;
use crate::server::ServerState;

pub mod read_through;
pub mod snapshot_wal;

#[cfg(feature = "redb")]
pub mod redb_backend;

/// A durable persistence tier for the graph registry.
///
/// `load_all`/`checkpoint_all` are async to match the existing `persist.rs`
/// functions (they take the global `RwLock<ServerState>`); `record`/`shutdown`
/// are sync — `record` is on the write hot path and must be cheap (it hands the
/// mutation to an off-reactor writer), and `shutdown` flushes at process exit.
#[async_trait::async_trait]
pub trait PersistenceBackend: Send + Sync {
    /// Reconstruct the registry from durable storage at boot. Returns the number
    /// of graphs loaded. No-op (Ok(0)) when nothing is configured.
    async fn load_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String>;

    /// Persist the current registry state. Returns the number of graphs written.
    async fn checkpoint_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String>;

    /// Record a single applied DATA mutation (already succeeded in memory). Called
    /// from the dispatch write-side-effect block for every durable mutation, so it
    /// must be non-blocking — it enqueues to an off-reactor writer, never doing
    /// file I/O on the calling Tokio worker. `graph_fname` is already sanitized.
    fn record(&self, graph_fname: &str, method: &Method);

    /// Record a single applied DATA mutation AND await its durable commit
    /// (CONCEPT:KG-2.187 commit-before-ack barrier). Used ONLY when redb is
    /// authoritative: dispatch awaits this before acking the write to the client,
    /// so a write is never acknowledged unless it is on disk. The enqueue must
    /// still fold into the SAME group-commit batch as every other concurrent
    /// awaiting writer (one fsync per batch, NOT one fsync per op) — it returns
    /// only after the `WriteTransaction` carrying this op has committed. On a
    /// durable-commit failure it returns `Err(_)`, and dispatch turns that into an
    /// ERROR response (the write did NOT land).
    ///
    /// Backpressure (NOT drop): under authoritative mode a full writer queue must
    /// block/await capacity or fail loudly — never silently discard the mutation.
    ///
    /// The default implementation falls back to the fire-and-forget `record`
    /// (returning Ok immediately): backends with no durable-commit handshake
    /// (e.g. snapshot+WAL) keep today's semantics. Only [`redb_backend`] overrides
    /// it with a real awaited commit, and only redb may be made authoritative.
    async fn record_durable(&self, graph_fname: &str, method: &Method) -> Result<(), String> {
        self.record(graph_fname, method);
        Ok(())
    }

    /// Durably register a graph's identity (CONCEPT:KG-2.187). Under authoritative
    /// mode the per-mutation write path persists node/edge ROWS but not the graph's
    /// name/type, yet `load_all` rebuilds the registry from a durable graph manifest
    /// (redb `graph_meta`). So dispatch calls this when a graph is created (and it
    /// must commit durably) so a `kill -9` between creation and the next checkpoint
    /// still recovers the graph under its REAL name/type. Idempotent. Default no-op
    /// for non-authoritative / non-redb backends.
    async fn register_graph(
        &self,
        _graph_fname: &str,
        _name: &str,
        _graph_type: crate::protocol::GraphType,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Durably remove ALL state for a graph when its tenant is DELETED
    /// (CONCEPT:KG-2.212). The default per-mutation write path persists rows keyed by
    /// the graph's sanitized name but has NO row-removal on `DeleteGraph`; that left
    /// a deleted tenant's nodes/edges/meta resident in the durable tier, so a
    /// recreate of the SAME name inherited them via the read-through-on-RAM-miss path
    /// and via `load_all` — silently corrupting/dropping the new tenant's writes.
    /// Dispatch awaits this on `DeleteGraph` under authoritative mode so the purge is
    /// COMMITTED before the delete is acked (commit-before-ack), making a same-name
    /// recreate start from a clean durable slate with no race. Idempotent. Default
    /// no-op for non-authoritative / non-redb backends (those keep no authoritative
    /// per-graph durable rows that could leak across a recreate).
    async fn purge_graph(&self, _graph_fname: &str) -> Result<(), String> {
        Ok(())
    }

    /// Read a single node's stored properties back from the durable tier
    /// (CONCEPT:KG-2.187 read-through). Returns `Ok(None)` when the node is not
    /// present durably, `Ok(Some(props))` when it is. The default is `Ok(None)`
    /// — only an authoritative backend that can serve a RAM-miss read implements
    /// it. Provided so a future read-through-on-eviction path has a backend seam
    /// without another trait revision; see the eviction note in `persist.rs`.
    async fn read_node(
        &self,
        _graph_fname: &str,
        _node_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    /// SYNC read-through of a single node's stored properties (CONCEPT:KG-2.191).
    /// The eg-core read path (`GraphCore::get_node_properties`) is synchronous, so
    /// the read-through it consults on a RAM miss must be sync too. The redb backend
    /// already performs its point-read over a blocking channel to its off-reactor
    /// writer thread, so a sync variant is natural and adds no async runtime to
    /// eg-core. Returns `Ok(None)` for a non-authoritative / non-redb backend (no
    /// read-through is ever attached to a graph in that case, so this is never hit
    /// in the default model). Same return contract as [`read_node`].
    fn read_node_blocking(
        &self,
        _graph_fname: &str,
        _node_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    /// Flush and stop any background writer threads at graceful shutdown.
    /// Idempotent.
    fn shutdown(&self);

    /// Downcast hook (CONCEPT:KG-2.204). The Raft store needs the CONCRETE
    /// [`redb_backend::RedbBackend`] to reach its durable-log API (which is not part
    /// of this trait — the log is a Raft concern, not a general persistence one).
    /// Raft requires the redb backend, so this lets the raft module recover the
    /// concrete type from the `Arc<dyn PersistenceBackend>` in `ServerState`. The
    /// default returns `None`; only the redb backend overrides it.
    #[cfg(feature = "raft")]
    fn as_redb(&self) -> Option<&redb_backend::RedbBackend> {
        None
    }
}
