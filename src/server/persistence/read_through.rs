//! Facade adapter wiring the durable [`PersistenceBackend`] to eg-core's
//! read-through seam (CONCEPT:KG-2.191).
//!
//! eg-core defines `ReadThrough` / `ReadThroughFactory` but must not know about
//! `PersistenceBackend` (that would invert the crate DAG). This module — in the
//! FACADE, ABOVE eg-core — implements those eg-core traits over a backend trait
//! object, so the dependency points the correct way (facade → eg-core, facade →
//! backend) and no facade type leaks down into eg-core.
//!
//! Installed once at startup, only under redb-authoritative mode, via
//! `GraphRegistry::set_read_through_factory`. On a RAM miss, `GraphCore` calls
//! `ReadThrough::read_node_blob`, which routes to the backend's SYNC point-read.

use std::sync::Arc;

use eg_core::read_through::{ReadThrough, ReadThroughFactory};

use super::PersistenceBackend;
use crate::persist::sanitize;

/// A per-graph read-through bound to one graph's sanitized durable key.
struct BackendReadThrough {
    backend: Arc<dyn PersistenceBackend>,
    /// Sanitized graph file name — the key the redb backend stores rows under.
    graph_fname: String,
}

// `eg_core::ReadThrough` requires `Debug`, but the backend trait object is not
// Debug; print only the graph key (the backend pointer carries no useful debug).
impl std::fmt::Debug for BackendReadThrough {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendReadThrough")
            .field("graph_fname", &self.graph_fname)
            .finish_non_exhaustive()
    }
}

impl ReadThrough for BackendReadThrough {
    fn read_node_blob(&self, node_id: &str) -> Option<Vec<u8>> {
        // A durable read error (writer gone, etc.) is treated as "not found": the
        // read path returns None rather than surfacing an I/O error type into
        // eg-core. The miss is logged so a real durability fault is observable.
        match self.backend.read_node_blocking(&self.graph_fname, node_id) {
            Ok(blob) => blob,
            Err(e) => {
                tracing::warn!(
                    "read-through miss for node '{}' in graph '{}': {}",
                    node_id,
                    self.graph_fname,
                    e
                );
                None
            }
        }
    }
}

/// Builds per-graph read-throughs over a shared backend. The factory sanitizes the
/// logical graph name to the durable key, so eg-core stays unaware of the
/// filename-sanitization rule.
pub struct BackendReadThroughFactory {
    backend: Arc<dyn PersistenceBackend>,
}

impl BackendReadThroughFactory {
    pub fn new(backend: Arc<dyn PersistenceBackend>) -> Self {
        Self { backend }
    }
}

impl ReadThroughFactory for BackendReadThroughFactory {
    fn for_graph(&self, graph_name: &str) -> Arc<dyn ReadThrough> {
        Arc::new(BackendReadThrough {
            backend: self.backend.clone(),
            graph_fname: sanitize(graph_name),
        })
    }
}
