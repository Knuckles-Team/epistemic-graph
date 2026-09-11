use std::collections::BTreeSet;

use eg_core::graph::GraphView;
use eg_types::protocol::Method;

use super::{
    opaque_identity, IncrementalDelta, IncrementalReasoningIndex, ProjectionInvalidationKind,
    ProjectionPosition,
};

impl IncrementalReasoningIndex {
    /// One-time bootstrap when no projection snapshot exists. Unsupported or corrupt
    /// persisted snapshots are rejected by the current reader. Steady-state updates
    /// are exclusively change-driven through [`Self::apply_batch`].
    pub fn from_graph_view(view: &GraphView) -> Self {
        let mut index = Self::default();
        for node_id in view.node_properties.keys() {
            index.register_from_graph_view(view, node_id);
        }
        // A `(source, target)` pair may carry several parallel property entries —
        // e.g. a provenance `GENERATED_BY` edge AND a separate epistemic `SUPPORTS`
        // edge to the same node (see the read/write model note on
        // `GraphCore::edge_properties`). Registering only `versions.last()` silently
        // dropped whichever relationship was NOT the most recently written for that
        // pair. `add_edge` already decodes and dispatches by each entry's own
        // `relationship`, so visiting every version is the correct, non-lossy read.
        for ((source, target), versions) in &view.edge_properties {
            for properties in versions {
                index.add_edge(source, target, properties);
            }
        }
        index
    }

    /// Apply one committed batch exactly once. Replaying the same outbox lease is
    /// harmless; cursor acknowledgement happens only after this state is durable.
    pub fn apply_batch(
        &mut self,
        position: ProjectionPosition,
        methods: &[Method],
    ) -> Result<IncrementalDelta, String> {
        let Some(position) = self.prepare_position(position)? else {
            return Ok(IncrementalDelta::default());
        };
        let mut delta = IncrementalDelta::default();
        for method in methods {
            apply_method(self, method, &mut delta);
        }
        self.position = Some(position);
        Ok(delta)
    }

    pub fn causes_of(&self, source: &str) -> BTreeSet<String> {
        self.causal_out
            .get(&opaque_identity(source))
            .cloned()
            .unwrap_or_default()
    }

    pub fn conflicts_with(&self, node: &str) -> BTreeSet<String> {
        self.conflicts
            .get(&opaque_identity(node))
            .cloned()
            .unwrap_or_default()
    }

    pub fn stale_materializations(&self) -> &BTreeSet<String> {
        &self.stale_materializations
    }
}

fn apply_method(
    index: &mut IncrementalReasoningIndex,
    method: &Method,
    delta: &mut IncrementalDelta,
) {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => apply_add_node(index, node_id, properties_msgpack, delta),
        Method::RemoveNode { node_id } => apply_remove_node(index, node_id, delta),
        Method::CompareAndSetNodeFields { node_id, .. } => {
            index.stale_dependents(node_id, delta);
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => apply_add_edge(index, source_id, target_id, properties_msgpack, delta),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => apply_remove_edge(index, source_id, target_id, delta),
        Method::ApplyMutation { event_type, query } => {
            apply_invalidation(index, event_type, query, delta);
        }
        _ => {}
    }
}

fn apply_add_node(
    index: &mut IncrementalReasoningIndex,
    node_id: &str,
    properties: &[u8],
    delta: &mut IncrementalDelta,
) {
    index.stale_dependents(node_id, delta);
    index.register_materialization(node_id, properties);
}

fn apply_remove_node(
    index: &mut IncrementalReasoningIndex,
    node_id: &str,
    delta: &mut IncrementalDelta,
) {
    let node_ref = opaque_identity(node_id);
    let was_materialization = index.materializations.contains(&node_ref);
    index.remove_incident(node_id, delta);
    index.materialization_deps.remove(&node_ref);
    index.remove_materialization_generator(&node_ref);
    index.stale_materializations.remove(&node_ref);
    if was_materialization {
        index.retracted_materializations.insert(node_ref);
    } else {
        index.materializations.remove(&node_ref);
        index.retracted_materializations.remove(&node_ref);
    }
    index.stale_dependents(node_id, delta);
}

fn apply_add_edge(
    index: &mut IncrementalReasoningIndex,
    source_id: &str,
    target_id: &str,
    properties: &[u8],
    delta: &mut IncrementalDelta,
) {
    if index.add_edge(source_id, target_id, properties) {
        delta.edge_changes += 1;
        index.stale_dependents(&format!("{source_id}->{target_id}"), delta);
        index.stale_dependents(source_id, delta);
        index.stale_dependents(target_id, delta);
    }
}

fn apply_remove_edge(
    index: &mut IncrementalReasoningIndex,
    source_id: &str,
    target_id: &str,
    delta: &mut IncrementalDelta,
) {
    if index.remove_edge(source_id, target_id) {
        delta.edge_changes += 1;
    }
    index.stale_dependents(&format!("{source_id}->{target_id}"), delta);
    index.stale_dependents(source_id, delta);
    index.stale_dependents(target_id, delta);
}

fn apply_invalidation(
    index: &mut IncrementalReasoningIndex,
    event_type: &str,
    query: &str,
    delta: &mut IncrementalDelta,
) {
    let kind = match event_type {
        "policy_changed" => Some(ProjectionInvalidationKind::PolicyChanged),
        "model_retired" => Some(ProjectionInvalidationKind::ModelRetired),
        "ontology_evolved" => Some(ProjectionInvalidationKind::OntologyEvolved),
        _ => None,
    };
    if let Some(kind) = kind {
        delta.newly_stale.extend(index.invalidate(kind, query));
    }
}
