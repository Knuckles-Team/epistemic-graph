//! A graph's stored vectors as ONE caller may see them (EH-396, EH-397): the
//! graph ACL is checked when the view is opened, and every row is re-checked
//! against the caller's row-level security before its vector is read or it is
//! reported by a probe. A learned artefact is therefore fitted and evaluated
//! only on rows the caller could already read.

use std::sync::Arc;

use eg_core::graph::GraphCore;
use eg_types::decision::statistical::StatisticalErrorCode;

use super::stat_support::readable_graph;
use crate::isolation::IsolationLayer;

/// Over-fetch factor of a visibility-filtered probe.
const PROBE_OVERFETCH: usize = 4;

/// The stored vectors of one graph, as the caller may see them.
pub(super) struct GraphVectors {
    core: Arc<GraphCore>,
    isolation: IsolationLayer,
    caller: String,
}

impl GraphVectors {
    /// Whether the store may hold vectors of `space_digest`: a store that
    /// declares a space must declare exactly that one.
    pub(super) fn admits_space(&self, space_digest: &str) -> bool {
        self.core
            .semantic_store
            .read()
            .space()
            .is_none_or(|space| space.digest == space_digest)
    }

    /// The store's vector width.
    pub(super) fn dimensions(&self) -> usize {
        self.core.semantic_store.read().dim()
    }

    /// How many rows the store embeds.
    pub(super) fn embedded(&self) -> u64 {
        self.core.semantic_store.read().len() as u64
    }

    #[cfg(feature = "security")]
    fn visible(&self, id: &str) -> bool {
        self.core.get_node_properties(id).is_some_and(|blob| {
            self.isolation
                .can_see_row(&self.caller, &crate::isolation::row_visibility(&blob))
        })
    }

    #[cfg(not(feature = "security"))]
    fn visible(&self, id: &str) -> bool {
        let _ = (&self.isolation, &self.caller);
        self.core.get_node_properties(id).is_some()
    }

    /// A visible unit's stored vector; `None` when invisible or unembedded.
    fn vector(&self, id: &str) -> Option<Vec<f64>> {
        if !self.visible(id) {
            return None;
        }
        let vector = self.core.semantic_store.read().get_embedding(id)?;
        Some(vector.into_iter().map(f64::from).collect())
    }

    /// The visible stored vectors of `ids`, in order, skipping the rest.
    pub(super) fn vectors(&self, ids: impl Iterator<Item = String>) -> Vec<Vec<f64>> {
        ids.filter_map(|id| self.vector(&id)).collect()
    }

    /// The top `k` VISIBLE rows for `query`; empty for a query of another width.
    pub(super) fn probe(&self, query: &[f32], k: usize) -> Vec<(String, f32)> {
        if query.len() != self.dimensions() {
            return Vec::new();
        }
        let hits = self
            .core
            .semantic_store
            .read()
            .semantic_search(query, k.saturating_mul(PROBE_OVERFETCH));
        hits.into_iter()
            .filter(|(id, _)| self.visible(id))
            .take(k)
            .collect()
    }
}

/// `graph`, ACL-checked for `agent_id`, as that caller may see it.
pub(super) async fn graph_vectors(
    state: &Arc<tokio::sync::RwLock<crate::server::state::ServerState>>,
    agent_id: &str,
    graph: &str,
) -> Result<GraphVectors, String> {
    let (core, isolation) = readable_graph(
        state,
        agent_id,
        graph,
        StatisticalErrorCode::ParameterInvalid,
    )
    .await?;
    Ok(GraphVectors {
        core,
        isolation,
        caller: agent_id.to_string(),
    })
}
