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

    /// Flush and stop any background writer threads at graceful shutdown.
    /// Idempotent.
    fn shutdown(&self);
}
