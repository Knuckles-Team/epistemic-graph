use std::collections::{BTreeMap, BTreeSet};

use super::{decode, opaque_identity, remove_value, IncrementalDelta, IncrementalReasoningIndex};

impl IncrementalReasoningIndex {
    pub(super) fn canonical_edge_generators(&self) -> BTreeMap<String, String> {
        let mut generators = BTreeMap::new();
        for ((materialization, generator), relationship) in &self.provenance_edges {
            if relationship == "GENERATED_BY" {
                generators
                    .entry(materialization.clone())
                    .or_insert_with(|| generator.clone());
            }
        }
        generators
    }

    pub(super) fn reconcile_edge_generator(&mut self, materialization: &str) {
        // Tuple ordering groups every target for one source contiguously. Seek to
        // that prefix in O(log P), then inspect only this materialization's P_m
        // provenance rows. The first GENERATED_BY target is therefore the same
        // canonical opaque target selected by a complete ordered scan.
        let lower_bound = (materialization.to_string(), String::new());
        let generator = self
            .provenance_edges
            .range(lower_bound..)
            .take_while(|((source, _), _)| source == materialization)
            .find_map(|((_, target), relationship)| {
                (relationship == "GENERATED_BY").then(|| target.clone())
            });
        self.replace_materialization_generator(materialization, generator);
    }

    pub(super) fn add_edge(&mut self, source: &str, target: &str, properties: &[u8]) -> bool {
        let Some(relationship) = decode(properties).and_then(|value| {
            value
                .get("relationship")
                .and_then(serde_json::Value::as_str)
                .map(str::to_ascii_uppercase)
        }) else {
            return false;
        };
        if !matches!(
            relationship.as_str(),
            "SUPPORTS"
                | "CONTRADICTS"
                | "ATTACKS"
                | "CAUSES"
                | "ENABLES"
                | "DERIVED_FROM"
                | "GENERATED_BY"
        ) {
            return false;
        }
        self.add_edge_relationship(source, target, &relationship)
    }

    pub(super) fn add_edge_relationship(
        &mut self,
        source: &str,
        target: &str,
        relationship: &str,
    ) -> bool {
        let source = opaque_identity(source);
        let target = opaque_identity(target);
        if matches!(relationship, "DERIVED_FROM" | "GENERATED_BY") {
            self.provenance_edges
                .insert((source.clone(), target.clone()), relationship.to_string());
            self.materializations.insert(source.clone());
            // Both provenance relationships are invalidation dependencies. The
            // canonical generator is the lexicographically first GENERATED_BY
            // target, matching GraphView's BTreeMap bootstrap order regardless of
            // mutation arrival order.
            self.materialization_deps
                .entry(source.clone())
                .or_default()
                .insert(target);
            self.reconcile_edge_generator(&source);
            self.stale_materializations.remove(&source);
            self.retracted_materializations.remove(&source);
            return true;
        }
        self.remove_edge_refs(&source, &target);
        self.epistemic_edges
            .insert((source.clone(), target.clone()), relationship.to_string());
        match relationship {
            "CAUSES" | "ENABLES" => {
                self.causal_out.entry(source).or_default().insert(target);
            }
            "CONTRADICTS" | "ATTACKS" => {
                self.conflicts
                    .entry(source.clone())
                    .or_default()
                    .insert(target.clone());
                self.conflicts.entry(target).or_default().insert(source);
            }
            _ => {}
        }
        true
    }

    pub(super) fn remove_edge(&mut self, source: &str, target: &str) -> bool {
        self.remove_edge_refs(&opaque_identity(source), &opaque_identity(target))
    }

    pub(super) fn remove_edge_refs(&mut self, source: &str, target: &str) -> bool {
        if let Some(relationship) = self
            .provenance_edges
            .remove(&(source.to_string(), target.to_string()))
        {
            if matches!(relationship.as_str(), "DERIVED_FROM" | "GENERATED_BY") {
                remove_value(&mut self.materialization_deps, source, target);
            }
            if relationship == "GENERATED_BY" {
                self.reconcile_edge_generator(source);
            }
            self.stale_materializations.insert(source.to_string());
            return true;
        }
        let removed = self
            .epistemic_edges
            .remove(&(source.to_string(), target.to_string()));
        let Some(relationship) = removed else {
            return false;
        };
        if matches!(relationship.as_str(), "CAUSES" | "ENABLES") {
            remove_value(&mut self.causal_out, source, target);
        }
        if matches!(relationship.as_str(), "CONTRADICTS" | "ATTACKS") {
            remove_value(&mut self.conflicts, source, target);
            remove_value(&mut self.conflicts, target, source);
        }
        true
    }

    pub(super) fn remove_incident(&mut self, node_id: &str, delta: &mut IncrementalDelta) {
        let node_id = opaque_identity(node_id);
        let edges = self
            .epistemic_edges
            .keys()
            .chain(self.provenance_edges.keys())
            .filter(|(source, target)| source == &node_id || target == &node_id)
            .cloned()
            .collect::<Vec<_>>();
        for (source, target) in edges {
            if self.remove_edge_refs(&source, &target) {
                delta.edge_changes += 1;
            }
        }
    }

    pub(super) fn stale_dependents(&mut self, changed: &str, delta: &mut IncrementalDelta) {
        let mut frontier = vec![opaque_identity(changed)];
        let mut visited = BTreeSet::new();
        while let Some(subject) = frontier.pop() {
            if !visited.insert(subject.clone()) {
                continue;
            }
            let dependents = self
                .materialization_deps
                .iter()
                .filter(|(_, deps)| deps.contains(&subject))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            for dependent in dependents {
                if self.stale_materializations.insert(dependent.clone()) {
                    delta.newly_stale.insert(dependent.clone());
                }
                frontier.push(dependent);
            }
        }
    }
}
