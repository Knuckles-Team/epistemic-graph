//! Default persistence backend: snapshot RDB + off-reactor WAL (CONCEPT:EG-KG.storage.kg-kg).
//!
//! This wraps TODAY's durability logic verbatim — there is no behavior change.
//! `load_all`/`checkpoint_all` delegate to the existing `crate::persist`
//! functions; `record` is the existing WAL append (serialize the method, hand it
//! to the off-reactor [`WalService`]). The `WalService` is owned HERE rather than
//! on `ServerState`, so the WAL type no longer leaks out of the shared state — the
//! whole point of routing durability through the trait.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Method;
use crate::server::ServerState;
use crate::wal_service::WalService;

use super::PersistenceBackend;

/// Snapshot + WAL durability — the engine's shipped default.
pub struct SnapshotWalBackend {
    /// `None` when no persist dir is configured (in-memory only); then `record`
    /// is a no-op and load/checkpoint return Ok(0). `Some` mirrors the previous
    /// `ServerState::wal_service` exactly.
    wal_service: Option<Arc<WalService>>,
}

impl SnapshotWalBackend {
    /// Build the default backend, taking ownership of the off-reactor WAL writer
    /// (or `None` for an in-memory engine with no persist dir).
    pub fn new(wal_service: Option<Arc<WalService>>) -> Self {
        Self { wal_service }
    }

    /// Borrow the owned WAL writer (used by `checkpoint_all`/`load_all` for the
    /// position-based truncation + tail replay).
    pub(crate) fn wal_service(&self) -> Option<&Arc<WalService>> {
        self.wal_service.as_ref()
    }
}

#[async_trait::async_trait]
impl PersistenceBackend for SnapshotWalBackend {
    async fn load_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        crate::persist::load_all(state, self.wal_service.as_deref()).await
    }

    async fn checkpoint_all(&self, state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        crate::persist::checkpoint_all(state, self.wal_service.as_deref()).await
    }

    fn record(&self, graph_fname: &str, method: &Method) {
        // Serialize on the calling (reactor) thread — cheap CPU — and hand the
        // bytes to the off-reactor writer thread; no file I/O runs here.
        if let Some(svc) = &self.wal_service {
            match rmp_serde::to_vec_named(method) {
                Ok(bytes) => svc.append(graph_fname.to_string(), bytes),
                Err(e) => tracing::warn!("WAL serialize failed for '{}': {}", graph_fname, e),
            }
        }
    }

    fn shutdown(&self) {
        if let Some(svc) = &self.wal_service {
            svc.shutdown();
        }
    }
}
