//! Facade adapter wiring the durable [`PersistenceBackend`] to eg-core's
//! read-through seam (CONCEPT:EG-KG.storage.read-through-seam-exercised).
//!
//! eg-core defines `ReadThrough` / `ReadThroughFactory` but must not know about
//! `PersistenceBackend` (that would invert the crate DAG). This module — in the
//! FACADE, ABOVE eg-core — implements those eg-core traits over a backend trait
//! object, so the dependency points the correct way (facade → eg-core, facade →
//! backend) and no facade type leaks down into eg-core.
//!
//! Installed once at startup via
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

/// A [`eg_core::registry::GraphMaterializer`] over a [`PersistenceBackend`]
/// (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3): fetches ONE graph's durable material
/// (nodes/edges/semantic store) on lazy first-open by calling
/// [`PersistenceBackend::read_graph_material_blocking`]. Installed once at
/// startup when lazy startup is enabled, mirroring
/// [`BackendReadThroughFactory`] above.
pub struct BackendGraphMaterializer {
    backend: Arc<dyn PersistenceBackend>,
}

impl BackendGraphMaterializer {
    pub fn new(backend: Arc<dyn PersistenceBackend>) -> Self {
        Self { backend }
    }
}

impl eg_core::registry::GraphMaterializer for BackendGraphMaterializer {
    fn materialize(&self, graph_name: &str) -> Option<eg_core::registry::GraphMaterial> {
        let fname = sanitize(graph_name);
        match self.backend.read_graph_material_blocking(&fname) {
            Ok(material) => material,
            Err(e) => {
                tracing::warn!(
                    "lazy-open materialize failed for graph '{}': {}",
                    graph_name,
                    e
                );
                None
            }
        }
    }

    /// Bounded-page override (CONCEPT:EG-KG.sharding.paged-lazy-open, L38 "paged adjacency") — routes to
    /// [`PersistenceBackend::read_graph_material_page_blocking`] instead of
    /// inheriting the trait's own default (which would call [`materialize`](Self::materialize)
    /// above and slice in memory). Only the redb backend gives this a genuinely
    /// bounded SOURCE fetch today; any other backend still gets a correct — just
    /// not SOURCE-bounded — page via that method's own default fallback.
    fn materialize_page(
        &self,
        graph_name: &str,
        cursor: Option<eg_core::registry::MaterializeCursor>,
        page_size: usize,
    ) -> Option<eg_core::registry::MaterialPage> {
        let fname = sanitize(graph_name);
        match self
            .backend
            .read_graph_material_page_blocking(&fname, cursor, page_size)
        {
            Ok(page) => page,
            Err(e) => {
                tracing::warn!(
                    "paged lazy-open materialize_page failed for graph '{}': {}",
                    graph_name,
                    e
                );
                None
            }
        }
    }
}
