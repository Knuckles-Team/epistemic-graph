//! The edge index as a server index of the graph's `IndexManager` (EH-352):
//! every committed batch's edge delta reaches it under the batch's topology
//! lock, the registry answers its lifecycle status, and a drop retires it.

use std::sync::Arc;

use eg_core::graph::GraphCore;
use eg_core::index::{
    ChangeSet, IndexColumns, IndexDescriptor, IndexError, IndexKind, IndexManifest,
    ManagedIndexStatus, Predicate, SecondaryIndex,
};

use super::{EdgeIndex, EdgeRefreshOutcome, POISONED};

/// The registry's handle on one edge index; searches and builds clone the
/// `Arc` out of the registry lock.
pub(super) struct RegisteredEdgeIndex(pub(super) Arc<EdgeIndex>);

impl SecondaryIndex for RegisteredEdgeIndex {
    fn kind(&self) -> IndexKind {
        IndexKind::EdgeSearch
    }

    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::EdgeSearch,
            columns: IndexColumns::NonColumnar,
            serves_lookup: false,
        }
    }

    fn covers(&self, _predicate: &Predicate) -> bool {
        false
    }

    fn lookup(&self, _core: &GraphCore, _predicate: &Predicate) -> Option<Vec<String>> {
        None
    }

    fn manifest(&self) -> IndexManifest {
        *self.0.lock_manifest()
    }

    fn publish_manifest(&self, manifest: IndexManifest) {
        *self.0.lock_manifest() = manifest;
    }

    fn maintains_manifest(&self) -> bool {
        true
    }

    /// Record every endpoint pair the batch added or removed an edge of,
    /// stamped with the graph version the batch applies over. Never reads the
    /// graph: the batch holds its topology lock.
    fn apply_delta(&self, core: &GraphCore, change: &ChangeSet) -> Result<(), IndexError> {
        let stamp = core.version();
        let mut touched = self.0.touched.lock().expect(POISONED);
        for edge in change.added_edges.iter().chain(&change.removed_edges) {
            touched.insert((edge.source.clone(), edge.target.clone()), stamp);
        }
        Ok(())
    }

    fn full_rebuild(&self, core: &GraphCore) -> Result<(), IndexError> {
        match self.0.refresh(core) {
            EdgeRefreshOutcome::Failed(block) => Err(IndexError::Failed(block.detail)),
            EdgeRefreshOutcome::Activated { .. }
            | EdgeRefreshOutcome::InFlight
            | EdgeRefreshOutcome::Superseded
            | EdgeRefreshOutcome::Retired => Ok(()),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn managed_status(&self) -> Option<ManagedIndexStatus> {
        Some(self.0.status())
    }

    fn retire(&self) {
        self.0.lock_maintenance().retired = true;
    }
}
