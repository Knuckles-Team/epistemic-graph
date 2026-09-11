use std::collections::BTreeSet;

use eg_core::graph::GraphView;

use super::{
    opaque_identity, IncrementalDelta, IncrementalReasoningIndex, ProjectedMaterialization,
    ProjectedMaterializationStatus, ProjectionInvalidationKind, ProjectionPosition,
};

impl IncrementalReasoningIndex {
    pub(super) fn prepare_position(
        &self,
        mut position: ProjectionPosition,
    ) -> Result<Option<ProjectionPosition>, String> {
        self.validate()?;
        position.validate()?;
        position.batch_id = opaque_identity(&position.batch_id);
        if let Some(current) = self.position.as_ref() {
            if current == &position {
                return Ok(None);
            }
            if position.source_graph_version < current.source_graph_version
                || (position.source_graph_version == current.source_graph_version
                    && (position.batch_id != current.batch_id
                        || position.ordinal <= current.ordinal))
            {
                return Err(
                    "STALE_PROJECTION_POSITION: event does not advance watermark".to_string(),
                );
            }
        }
        Ok(Some(position))
    }

    pub fn status_of(&self, node_id: &str) -> Option<ProjectedMaterializationStatus> {
        let node_ref = opaque_identity(node_id);
        if self.retracted_materializations.contains(&node_ref) {
            Some(ProjectedMaterializationStatus::Retracted)
        } else if self.stale_materializations.contains(&node_ref) {
            Some(ProjectedMaterializationStatus::Stale)
        } else if self.materializations.contains(&node_ref) {
            Some(ProjectedMaterializationStatus::Fresh)
        } else {
            None
        }
    }

    pub fn materialization(&self, node_id: &str) -> Option<ProjectedMaterialization> {
        let materialization_ref = opaque_identity(node_id);
        let status = self.status_of(node_id)?;
        Some(ProjectedMaterialization {
            dependency_refs: self
                .materialization_deps
                .get(&materialization_ref)
                .into_iter()
                .flatten()
                .cloned()
                .collect(),
            generator_ref: self
                .materialization_generators
                .get(&materialization_ref)
                .cloned(),
            materialization_ref,
            status,
            source_graph_version: self
                .position
                .as_ref()
                .map_or(0, |position| position.source_graph_version),
        })
    }

    /// Claim a stale/retracted projection row under the exact source watermark.
    /// Completion must present this monotonically increasing epoch, preventing a
    /// late recompute from overwriting a newer invalidation.
    pub fn claim_recompute(
        &mut self,
        node_id: &str,
        expected_source_graph_version: u64,
    ) -> Result<u64, String> {
        let current = self
            .position
            .as_ref()
            .ok_or_else(|| "reasoning projection has no committed watermark".to_string())?;
        if current.source_graph_version != expected_source_graph_version {
            return Err("STALE_RECOMPUTE_FENCE: projection watermark changed".to_string());
        }
        let node_ref = opaque_identity(node_id);
        if !self.stale_materializations.contains(&node_ref)
            && !self.retracted_materializations.contains(&node_ref)
        {
            return Err("recompute requires a stale or retracted materialization".to_string());
        }
        self.recompute_epoch = self
            .recompute_epoch
            .checked_add(1)
            .ok_or_else(|| "reasoning recompute epoch exhausted".to_string())?;
        self.recompute_fences.insert(node_ref, self.recompute_epoch);
        Ok(self.recompute_epoch)
    }

    /// Complete a fenced recompute with provenance resolved from the authoritative
    /// graph post-image. `None` means the materialization no longer exists and must
    /// remain retracted; it is never silently recreated from stale projection data.
    pub fn complete_recompute(
        &mut self,
        node_id: &str,
        expected_source_graph_version: u64,
        fence_epoch: u64,
        provenance: Option<(BTreeSet<String>, Option<String>)>,
    ) -> Result<ProjectedMaterialization, String> {
        let current = self
            .position
            .as_ref()
            .ok_or_else(|| "reasoning projection has no committed watermark".to_string())?;
        if current.source_graph_version != expected_source_graph_version {
            return Err("STALE_RECOMPUTE_FENCE: projection watermark changed".to_string());
        }
        let node_ref = opaque_identity(node_id);
        if self.recompute_fences.get(&node_ref) != Some(&fence_epoch) {
            return Err("STALE_RECOMPUTE_FENCE: lease epoch changed".to_string());
        }
        match provenance {
            Some((dependencies, generator)) => {
                self.register_materialization_refs(&node_ref, dependencies, generator);
            }
            None => {
                self.materializations.insert(node_ref.clone());
                self.materialization_deps.remove(&node_ref);
                self.remove_materialization_generator(&node_ref);
                self.stale_materializations.remove(&node_ref);
                self.retracted_materializations.insert(node_ref.clone());
            }
        }
        self.recompute_fences.remove(&node_ref);
        self.materialization(node_id)
            .ok_or_else(|| "recomputed materialization is unavailable".to_string())
    }

    /// Consume a committed recompute intent using only its privacy-safe identity.
    /// The empty-row-delta graph commit advances the authoritative version by one;
    /// depending on which same-batch outbox ordinal was last acknowledged, this
    /// index may still be at the caller's observed version or already at the commit
    /// target. No other watermark is accepted.
    pub fn recompute_from_ref(
        &mut self,
        materialization_ref: &str,
        expected_source_graph_version: u64,
        committed_source_graph_version: u64,
        view: &GraphView,
    ) -> Result<ProjectedMaterialization, String> {
        if opaque_identity(materialization_ref) != materialization_ref {
            return Err("reasoning recompute identity is not opaque".to_string());
        }
        let expected_target = expected_source_graph_version
            .checked_add(1)
            .ok_or_else(|| "reasoning recompute graph version exhausted".to_string())?;
        if committed_source_graph_version != expected_target {
            return Err(
                "STALE_RECOMPUTE_FENCE: committed graph version does not follow observation"
                    .to_string(),
            );
        }
        let current_version = self
            .position
            .as_ref()
            .ok_or_else(|| "reasoning projection has no committed watermark".to_string())?
            .source_graph_version;
        if current_version != expected_source_graph_version
            && current_version != committed_source_graph_version
        {
            return Err("STALE_RECOMPUTE_FENCE: projection watermark changed".to_string());
        }

        let provenance = provenance_for_ref(view, materialization_ref)?;
        let fence_epoch = self.claim_recompute(materialization_ref, current_version)?;
        self.complete_recompute(
            materialization_ref,
            current_version,
            fence_epoch,
            provenance,
        )
    }

    pub fn invalidate(
        &mut self,
        kind: ProjectionInvalidationKind,
        subject: &str,
    ) -> BTreeSet<String> {
        let mut delta = IncrementalDelta::default();
        match kind {
            ProjectionInvalidationKind::PolicyChanged => {
                self.stale_dependents(subject, &mut delta);
            }
            ProjectionInvalidationKind::ModelRetired
            | ProjectionInvalidationKind::OntologyEvolved => {
                let generator_ref = opaque_identity(subject);
                let generated = self
                    .generator_materializations
                    .get(&generator_ref)
                    .cloned()
                    .unwrap_or_default();
                for materialization in generated {
                    if self.stale_materializations.insert(materialization.clone()) {
                        delta.newly_stale.insert(materialization.clone());
                    }
                    self.stale_dependents(&materialization, &mut delta);
                }
            }
        }
        delta.newly_stale
    }
}

fn provenance_for_ref(
    view: &GraphView,
    materialization_ref: &str,
) -> Result<Option<(BTreeSet<String>, Option<String>)>, String> {
    let mut matching_ids = view
        .node_properties
        .keys()
        .filter(|node_id| opaque_identity(node_id) == materialization_ref);
    let Some(source_id) = matching_ids.next() else {
        return Ok(None);
    };
    if matching_ids.next().is_some() {
        return Err("reasoning recompute identity is ambiguous".to_string());
    }
    let Some(properties) = view.node_properties.get(source_id) else {
        return Ok(None);
    };
    let mut outgoing = Vec::new();
    for ((source, target), versions) in &view.edge_properties {
        if source == source_id {
            if let Some(properties) = versions.last() {
                outgoing.push((target.clone(), properties.as_slice()));
            }
        }
    }
    Ok(Some(crate::resolve_provenance(
        Some(properties.as_slice()),
        outgoing,
    )))
}
