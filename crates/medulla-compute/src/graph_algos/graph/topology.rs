use std::hash::Hash;
use std::ops::Deref;

use super::{storage::GraphStorage, GraphViews};

/// Stable node and directed-edge access over an adjacency graph.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct GraphTopology<N> {
    pub(super) views: GraphViews<N>,
}

impl<N> GraphTopology<N>
where
    N: Clone + Eq + Hash + Ord,
{
    /// Number of nodes.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.views.storage.nodes.len()
    }

    /// Number of directed edges (after parallel-edge merge).
    #[inline]
    pub fn edge_count(&self) -> usize {
        self.views.storage.out.iter().map(Vec::len).sum()
    }

    /// The nodes in compact-index order (i.e. sorted `N`).
    #[inline]
    pub fn nodes(&self) -> &[N] {
        &self.views.storage.nodes
    }

    /// Compact index of a node id, if present.
    #[inline]
    pub fn index_of(&self, node: &N) -> Option<usize> {
        self.views.storage.index.get(node).copied()
    }

    /// Node id at a compact index.
    #[inline]
    pub fn node_at(&self, idx: usize) -> &N {
        &self.views.storage.nodes[idx]
    }

    /// Outgoing `(neighbor_idx, weight)` edges, sorted by neighbor index.
    #[inline]
    pub fn out_edges(&self, idx: usize) -> &[(usize, f64)] {
        &self.views.storage.out[idx]
    }

    /// Incoming `(source_idx, weight)` edges, sorted by source index.
    #[inline]
    pub fn in_edges(&self, idx: usize) -> &[(usize, f64)] {
        &self.views.storage.inc[idx]
    }

    pub(super) fn from_storage(storage: GraphStorage<N>) -> Self {
        Self {
            views: GraphViews { storage },
        }
    }
}

impl<N> Deref for GraphTopology<N> {
    type Target = GraphViews<N>;

    fn deref(&self) -> &Self::Target {
        &self.views
    }
}
