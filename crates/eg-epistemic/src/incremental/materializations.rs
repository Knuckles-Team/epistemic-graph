use std::collections::BTreeSet;

use eg_core::graph::GraphView;

use super::{decode, opaque_identity, remove_value, IncrementalReasoningIndex};

impl IncrementalReasoningIndex {
    /// Refresh one materialization from its authoritative current properties.
    /// This is the targeted CAS/update path; it avoids a graph-wide rebuild while
    /// replacing (rather than merging) a prior dependency set.
    pub fn refresh_materialization(&mut self, node_id: &str, properties: Option<&[u8]>) {
        let node_ref = opaque_identity(node_id);
        self.clear_materialization_state(&node_ref);
        if let Some(properties) = properties {
            self.register_materialization(node_id, properties);
        }
    }

    /// Refresh one materialization from the complete authoritative graph post-image,
    /// including edge-carried `DERIVED_FROM`/`GENERATED_BY` provenance.
    pub fn refresh_from_graph_view(&mut self, view: &GraphView, node_id: &str) {
        let node_ref = opaque_identity(node_id);
        self.clear_materialization_state(&node_ref);
        if view.node_properties.contains_key(node_id) {
            self.register_from_graph_view(view, node_id);
        }
    }

    pub(super) fn register_from_graph_view(&mut self, view: &GraphView, node_id: &str) {
        let node_properties = view
            .node_properties
            .get(node_id)
            .map(|value| value.as_slice());
        // Yield EVERY parallel entry for each outgoing pair, not just `versions.last()`
        // — `resolve_provenance` below already scans its whole `outgoing_edges`
        // iterator filtering by each entry's own `relationship`, so narrowing to one
        // blob per neighbor here only drops information (a `DERIVED_FROM`/
        // `GENERATED_BY` edge shadowed by a later, different-relationship edge to the
        // same target). Mirrors `recompute::register_from_provenance`'s `flat_map`.
        let outgoing = view
            .edge_properties
            .iter()
            .filter(|((source, _), _)| source == node_id)
            .flat_map(|((_, target), versions)| {
                versions
                    .iter()
                    .map(move |properties| (target.clone(), properties.as_slice()))
            });
        let (dependencies, generator) = crate::resolve_provenance(node_properties, outgoing);
        if dependencies.is_empty() && generator.is_none() {
            return;
        }
        let node_ref = opaque_identity(node_id);
        self.register_materialization_refs(&node_ref, dependencies, generator);
    }

    pub(super) fn register_materialization(&mut self, node_id: &str, properties: &[u8]) {
        let node_ref = opaque_identity(node_id);
        let Some(value) = decode(properties) else {
            self.clear_materialization_state(&node_ref);
            return;
        };
        let deps = value
            .get("invalidation_deps")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(opaque_identity)
            .collect::<BTreeSet<_>>();
        let generator = value
            .get("generating_activity")
            .and_then(serde_json::Value::as_str)
            .map(opaque_identity);
        if deps.is_empty() && generator.is_none() {
            self.clear_materialization_state(&node_ref);
            return;
        }
        self.register_materialization_refs(&node_ref, deps, generator);
    }

    pub(super) fn clear_materialization_state(&mut self, node_ref: &str) {
        self.materializations.remove(node_ref);
        self.materialization_deps.remove(node_ref);
        self.remove_materialization_generator(node_ref);
        self.stale_materializations.remove(node_ref);
        self.retracted_materializations.remove(node_ref);
    }

    pub(super) fn register_materialization_refs(
        &mut self,
        node_ref: &str,
        dependencies: BTreeSet<String>,
        generator: Option<String>,
    ) {
        let dependencies: BTreeSet<String> = dependencies
            .into_iter()
            .map(|value| opaque_identity(&value))
            .collect();
        self.materializations.insert(node_ref.to_string());
        if dependencies.is_empty() {
            self.materialization_deps.remove(node_ref);
        } else {
            self.materialization_deps
                .insert(node_ref.to_string(), dependencies);
        }
        self.replace_materialization_generator(node_ref, generator);
        self.stale_materializations.remove(node_ref);
        self.retracted_materializations.remove(node_ref);
    }

    pub(super) fn replace_materialization_generator(
        &mut self,
        materialization: &str,
        generator: Option<String>,
    ) {
        self.remove_materialization_generator(materialization);
        if let Some(generator) = generator {
            let generator = opaque_identity(&generator);
            let materialization = materialization.to_string();
            self.materialization_generators
                .insert(materialization.clone(), generator.clone());
            self.generator_materializations
                .entry(generator)
                .or_default()
                .insert(materialization);
        }
    }

    pub(super) fn remove_materialization_generator(
        &mut self,
        materialization: &str,
    ) -> Option<String> {
        let generator = self.materialization_generators.remove(materialization)?;
        remove_value(
            &mut self.generator_materializations,
            &generator,
            materialization,
        );
        Some(generator)
    }
}
