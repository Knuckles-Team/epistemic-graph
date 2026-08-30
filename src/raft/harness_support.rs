//! Shared setup for in-process Raft harnesses.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::channels::ChannelManager;
use crate::isolation::IsolationLayer;
use crate::registry::GraphRegistry;
use crate::server::persistence::PersistenceBackend;
use crate::server::ServerState;

/// Build a harness `ServerState` over an already-open persistence backend.
pub(crate) async fn make_state(
    dir: &str,
    backend: Arc<dyn PersistenceBackend>,
    isolation: IsolationLayer,
    auth_secret: &str,
) -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            crate::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation,
        channels: ChannelManager::new(),
        #[cfg(feature = "viz-static-export")]
        viz_engine: None,
        auth_secret: auth_secret.to_string(),
        persist_dir: Some(dir.to_string()),
        persistence: Some(backend),
        max_in_flight: Arc::new(tokio::sync::Semaphore::new(64)),
        read_admission: Arc::new(tokio::sync::Semaphore::new(64)),
        per_graph_inflight: Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit: 16,
        write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(
            crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
        ),
        open_txns: Arc::new(dashmap::DashMap::new()),
        txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "streaming")]
        cdc: None,
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
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
    }))
}
