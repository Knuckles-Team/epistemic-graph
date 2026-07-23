//! Change-driven epistemic projection used by the durable reasoning worker.
//!
//! This is intentionally a compact index, not a second graph.  It records only
//! support/conflict/causal edges and derived-materialization dependencies, and is
//! updated from committed `MutationOperation`s in outbox order.  Contradictions
//! remain explicit edges (paraconsistent, never exploded into arbitrary facts).

use std::collections::{BTreeMap, BTreeSet};

use eg_core::graph::GraphView;
use eg_types::protocol::Method;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Current durable schema for the compact reasoning projection snapshot.
pub const REASONING_PROJECTION_VERSION: u16 = 4;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionPosition {
    pub batch_id: String,
    pub ordinal: u32,
    pub source_graph_version: u64,
}

impl ProjectionPosition {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_id.trim().is_empty() {
            return Err("projection position requires a batch identity".to_string());
        }
        if self.source_graph_version == 0 {
            return Err(
                "reasoning projection requires a non-zero source graph version".to_string(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncrementalReasoningIndex {
    pub schema_version: u16,
    pub position: Option<ProjectionPosition>,
    /// `(source, target) -> normalized relationship`.
    epistemic_edges: BTreeMap<(String, String), String>,
    causal_out: BTreeMap<String, BTreeSet<String>>,
    conflicts: BTreeMap<String, BTreeSet<String>>,
    provenance_edges: BTreeMap<(String, String), String>,
    materializations: BTreeSet<String>,
    materialization_deps: BTreeMap<String, BTreeSet<String>>,
    materialization_generators: BTreeMap<String, String>,
    /// Durable reverse of `materialization_generators`, maintained in the same
    /// snapshot mutation so generator invalidation never scans all materializations.
    generator_materializations: BTreeMap<String, BTreeSet<String>>,
    stale_materializations: BTreeSet<String>,
    retracted_materializations: BTreeSet<String>,
    recompute_epoch: u64,
    recompute_fences: BTreeMap<String, u64>,
}

impl Default for IncrementalReasoningIndex {
    fn default() -> Self {
        Self {
            schema_version: REASONING_PROJECTION_VERSION,
            position: None,
            epistemic_edges: BTreeMap::new(),
            causal_out: BTreeMap::new(),
            conflicts: BTreeMap::new(),
            provenance_edges: BTreeMap::new(),
            materializations: BTreeSet::new(),
            materialization_deps: BTreeMap::new(),
            materialization_generators: BTreeMap::new(),
            generator_materializations: BTreeMap::new(),
            stale_materializations: BTreeSet::new(),
            retracted_materializations: BTreeSet::new(),
            recompute_epoch: 0,
            recompute_fences: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IncrementalDelta {
    pub edge_changes: usize,
    pub newly_stale: BTreeSet<String>,
}

/// Durable status served from the incremental projection. Source graph identifiers
/// never enter this value; callers address a materialization by its graph id and the
/// projection resolves it to the same domain-separated opaque reference it persists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectedMaterializationStatus {
    Fresh,
    Stale,
    Retracted,
}

/// Privacy-safe materialization metadata returned by projection reads/recompute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectedMaterialization {
    pub materialization_ref: String,
    pub dependency_refs: Vec<String>,
    pub generator_ref: Option<String>,
    pub status: ProjectedMaterializationStatus,
    pub source_graph_version: u64,
}

/// Explicit non-row invalidations accepted from the committed mutation stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionInvalidationKind {
    PolicyChanged,
    ModelRetired,
    OntologyEvolved,
}

/// Privacy-safe change currency stored in the MutationBatch projection wake-up.
/// Every identity is already domain-separated; relationship and invalidation values
/// are closed enums, so this sidecar never duplicates source labels or properties.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum IncrementalReasoningEvent {
    NodeUpserted {
        node_ref: String,
        dependency_refs: BTreeSet<String>,
        generator_ref: Option<String>,
        is_materialization: bool,
    },
    NodeRemoved {
        node_ref: String,
    },
    NodeChanged {
        node_ref: String,
    },
    EdgeUpserted {
        source_ref: String,
        target_ref: String,
        relationship: Option<String>,
    },
    EdgeRemoved {
        source_ref: String,
        target_ref: String,
    },
    Invalidate {
        invalidation: String,
        subject_ref: String,
    },
    /// Recompute one stale materialization after the authoritative graph accepted
    /// an empty-row-delta fence commit. The source identity is already opaque;
    /// consumers resolve provenance from the committed graph image.
    Recompute {
        materialization_ref: String,
        expected_source_graph_version: u64,
    },
    InvalidateAll,
}

/// Current required payload for `engine.projection.rebuild` outbox intents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasoningProjectionWakeup {
    pub schema_version: u16,
    pub operation_count: u32,
    pub operations_sha256: String,
    pub events: Vec<IncrementalReasoningEvent>,
}

impl ReasoningProjectionWakeup {
    pub const SCHEMA_VERSION: u16 = 1;

    pub fn new(
        operation_count: usize,
        operations_sha256: String,
        events: Vec<IncrementalReasoningEvent>,
    ) -> Result<Self, String> {
        let operation_count = u32::try_from(operation_count)
            .map_err(|_| "reasoning projection wake-up has too many operations".to_string())?;
        let wakeup = Self {
            schema_version: Self::SCHEMA_VERSION,
            operation_count,
            operations_sha256,
            events,
        };
        wakeup.validate()?;
        Ok(wakeup)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err("unsupported reasoning projection wake-up version".to_string());
        }
        if self.operations_sha256.len() != 64
            || !self
                .operations_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("reasoning projection wake-up digest is invalid".to_string());
        }
        if self.events.len() != self.operation_count as usize {
            return Err("reasoning projection wake-up event count is invalid".to_string());
        }
        for event in &self.events {
            event.validate()?;
        }
        Ok(())
    }

    /// Compile fixed-schema, privacy-safe events before state-backed operations are
    /// lowered to opaque receipts. Unknown mutation surfaces invalidate all tracked
    /// materializations rather than returning a false `Fresh` answer.
    pub fn events_for_methods(methods: &[Method]) -> Vec<IncrementalReasoningEvent> {
        methods.iter().map(event_for_method).collect()
    }
}

impl IncrementalReasoningEvent {
    fn validate(&self) -> Result<(), String> {
        let valid_ref = |value: &str| opaque_identity(value) == value;
        let valid_relationship = |value: &str| {
            matches!(
                value,
                "SUPPORTS"
                    | "CONTRADICTS"
                    | "ATTACKS"
                    | "CAUSES"
                    | "ENABLES"
                    | "DERIVED_FROM"
                    | "GENERATED_BY"
            )
        };
        match self {
            Self::NodeUpserted {
                node_ref,
                dependency_refs,
                generator_ref,
                is_materialization,
            } => {
                if !valid_ref(node_ref)
                    || dependency_refs.iter().any(|value| !valid_ref(value))
                    || generator_ref
                        .as_deref()
                        .is_some_and(|value| !valid_ref(value))
                    || (*is_materialization
                        != (!dependency_refs.is_empty() || generator_ref.is_some()))
                {
                    return Err("reasoning node event is invalid".to_string());
                }
            }
            Self::NodeRemoved { node_ref } | Self::NodeChanged { node_ref } => {
                if !valid_ref(node_ref) {
                    return Err("reasoning node event is invalid".to_string());
                }
            }
            Self::EdgeUpserted {
                source_ref,
                target_ref,
                relationship,
            } => {
                if !valid_ref(source_ref)
                    || !valid_ref(target_ref)
                    || relationship
                        .as_deref()
                        .is_some_and(|value| !valid_relationship(value))
                {
                    return Err("reasoning edge event is invalid".to_string());
                }
            }
            Self::EdgeRemoved {
                source_ref,
                target_ref,
            } => {
                if !valid_ref(source_ref) || !valid_ref(target_ref) {
                    return Err("reasoning edge event is invalid".to_string());
                }
            }
            Self::Invalidate {
                invalidation,
                subject_ref,
            } => {
                if !matches!(
                    invalidation.as_str(),
                    "policy_changed" | "model_retired" | "ontology_evolved"
                ) || !valid_ref(subject_ref)
                {
                    return Err("reasoning invalidation event is invalid".to_string());
                }
            }
            Self::Recompute {
                materialization_ref,
                expected_source_graph_version,
            } => {
                if !valid_ref(materialization_ref)
                    || *expected_source_graph_version == 0
                    || *expected_source_graph_version == u64::MAX
                {
                    return Err("reasoning recompute event is invalid".to_string());
                }
            }
            Self::InvalidateAll => {}
        }
        Ok(())
    }
}

impl IncrementalReasoningIndex {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != REASONING_PROJECTION_VERSION {
            return Err(format!(
                "unsupported reasoning projection version {} (expected {})",
                self.schema_version, REASONING_PROJECTION_VERSION
            ));
        }
        if let Some(position) = &self.position {
            position.validate()?;
            if opaque_identity(&position.batch_id) != position.batch_id {
                return Err("reasoning projection position is not opaque".to_string());
            }
        }
        if !self
            .materialization_deps
            .keys()
            .chain(self.materialization_generators.keys())
            .chain(
                self.generator_materializations
                    .values()
                    .flat_map(|values| values.iter()),
            )
            .chain(self.stale_materializations.iter())
            .chain(self.retracted_materializations.iter())
            .all(|materialization| self.materializations.contains(materialization))
        {
            return Err(
                "reasoning projection contains state for an unknown materialization".to_string(),
            );
        }
        if self
            .stale_materializations
            .iter()
            .any(|materialization| self.retracted_materializations.contains(materialization))
        {
            return Err(
                "reasoning projection materialization cannot be stale and retracted".to_string(),
            );
        }
        let valid_ref = |value: &str| opaque_identity(value) == value;
        let all_refs_valid = self.materializations.iter().all(|value| valid_ref(value))
            && self
                .materialization_deps
                .iter()
                .all(|(materialization, dependencies)| {
                    valid_ref(materialization) && dependencies.iter().all(|value| valid_ref(value))
                })
            && self
                .materialization_generators
                .iter()
                .all(|(materialization, generator)| {
                    valid_ref(materialization) && valid_ref(generator)
                })
            && self
                .generator_materializations
                .iter()
                .all(|(generator, materializations)| {
                    valid_ref(generator) && materializations.iter().all(|value| valid_ref(value))
                })
            && self
                .epistemic_edges
                .keys()
                .chain(self.provenance_edges.keys())
                .all(|(source, target)| valid_ref(source) && valid_ref(target))
            && self
                .causal_out
                .iter()
                .chain(self.conflicts.iter())
                .all(|(source, targets)| {
                    valid_ref(source) && targets.iter().all(|target| valid_ref(target))
                })
            && self
                .stale_materializations
                .iter()
                .chain(self.retracted_materializations.iter())
                .chain(self.recompute_fences.keys())
                .all(|value| valid_ref(value));
        if !all_refs_valid {
            return Err("reasoning projection contains a non-opaque identity".to_string());
        }
        if self
            .generator_materializations
            .values()
            .any(BTreeSet::is_empty)
            || self
                .materialization_generators
                .iter()
                .any(|(materialization, generator)| {
                    !self
                        .generator_materializations
                        .get(generator)
                        .is_some_and(|values| values.contains(materialization))
                })
            || self
                .generator_materializations
                .iter()
                .any(|(generator, materializations)| {
                    materializations.iter().any(|materialization| {
                        self.materialization_generators.get(materialization) != Some(generator)
                    })
                })
        {
            return Err("reasoning projection generator reverse index is inconsistent".to_string());
        }
        for (materialization, expected_generator) in self.canonical_edge_generators() {
            if self.materialization_generators.get(&materialization) != Some(&expected_generator) {
                return Err(
                    "reasoning projection canonical generator edge is inconsistent".to_string(),
                );
            }
        }
        if self
            .provenance_edges
            .iter()
            .filter(|(_, relationship)| {
                matches!(relationship.as_str(), "DERIVED_FROM" | "GENERATED_BY")
            })
            .any(|((materialization, dependency), _)| {
                !self
                    .materialization_deps
                    .get(materialization)
                    .is_some_and(|dependencies| dependencies.contains(dependency))
            })
        {
            return Err(
                "reasoning projection provenance dependency index is inconsistent".to_string(),
            );
        }
        if self
            .epistemic_edges
            .values()
            .chain(self.provenance_edges.values())
            .any(|relationship| {
                !matches!(
                    relationship.as_str(),
                    "SUPPORTS"
                        | "CONTRADICTS"
                        | "ATTACKS"
                        | "CAUSES"
                        | "ENABLES"
                        | "DERIVED_FROM"
                        | "GENERATED_BY"
                )
            })
        {
            return Err("reasoning projection contains an invalid relationship".to_string());
        }
        if self
            .recompute_fences
            .iter()
            .any(|(materialization, epoch)| {
                !self.materializations.contains(materialization)
                    || *epoch == 0
                    || *epoch > self.recompute_epoch
            })
        {
            return Err("reasoning projection contains an invalid recompute fence".to_string());
        }
        Ok(())
    }

    /// One-time bootstrap when no projection snapshot exists. Unsupported or corrupt
    /// persisted snapshots are rejected by the current reader. Steady-state updates
    /// are exclusively change-driven through [`Self::apply_batch`].
    pub fn from_graph_view(view: &GraphView) -> Self {
        let mut index = Self::default();
        for node_id in view.node_properties.keys() {
            index.register_from_graph_view(view, node_id);
        }
        for ((source, target), versions) in &view.edge_properties {
            if let Some(properties) = versions.last() {
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
            self.apply(method, &mut delta);
        }
        self.position = Some(position);
        Ok(delta)
    }

    /// Apply a privacy-safe projection wake-up and reconcile touched rows/edges to
    /// the current authoritative graph image. This is the restart path for
    /// state-backed mutations whose durable batch deliberately contains only an
    /// opaque operation receipt.
    pub fn apply_wakeup(
        &mut self,
        position: ProjectionPosition,
        wakeup: &ReasoningProjectionWakeup,
        view: &GraphView,
    ) -> Result<IncrementalDelta, String> {
        wakeup.validate()?;
        let Some(position) = self.prepare_position(position)? else {
            return Ok(IncrementalDelta::default());
        };
        let mut delta = IncrementalDelta::default();
        let mut touched_nodes = BTreeSet::new();
        let mut touched_edges = BTreeSet::new();
        for event in &wakeup.events {
            self.apply_wakeup_event(
                event,
                position.source_graph_version,
                view,
                &mut touched_nodes,
                &mut touched_edges,
                &mut delta,
            )?;
        }
        let source_nodes = view
            .node_properties
            .keys()
            .map(|node_id| (opaque_identity(node_id), node_id.as_str()))
            .collect::<BTreeMap<_, _>>();
        for node_ref in touched_nodes {
            if let Some(node_id) = source_nodes.get(&node_ref) {
                self.refresh_from_graph_view(view, node_id);
            }
        }
        for (source_ref, target_ref) in touched_edges {
            self.remove_edge_refs(&source_ref, &target_ref);
            let Some(source_id) = source_nodes.get(&source_ref) else {
                continue;
            };
            let Some(target_id) = source_nodes.get(&target_ref) else {
                continue;
            };
            if let Some(properties) = view
                .edge_properties
                .get(&(source_id.to_string(), target_id.to_string()))
                .and_then(|versions| versions.last())
            {
                self.add_edge(source_id, target_id, properties);
            }
        }
        self.position = Some(position);
        Ok(delta)
    }

    fn prepare_position(
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

        let mut matching_ids = view
            .node_properties
            .keys()
            .filter(|node_id| opaque_identity(node_id) == materialization_ref);
        let source_id = matching_ids.next().map(String::as_str);
        if matching_ids.next().is_some() {
            return Err("reasoning recompute identity is ambiguous".to_string());
        }
        let provenance = source_id.and_then(|node_id| {
            view.node_properties.get(node_id).map(|properties| {
                let outgoing =
                    view.edge_properties
                        .iter()
                        .filter_map(|((source, target), versions)| {
                            (source == node_id)
                                .then(|| {
                                    versions
                                        .last()
                                        .map(|properties| (target.clone(), properties.as_slice()))
                                })
                                .flatten()
                        });
                crate::resolve_provenance(Some(properties.as_slice()), outgoing)
            })
        });
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

    /// Refresh one materialization from its authoritative current properties.
    /// This is the targeted CAS/update path; it avoids a graph-wide rebuild while
    /// replacing (rather than merging) a prior dependency set.
    pub fn refresh_materialization(&mut self, node_id: &str, properties: Option<&[u8]>) {
        let node_ref = opaque_identity(node_id);
        self.materializations.remove(&node_ref);
        self.materialization_deps.remove(&node_ref);
        self.remove_materialization_generator(&node_ref);
        self.stale_materializations.remove(&node_ref);
        self.retracted_materializations.remove(&node_ref);
        if let Some(properties) = properties {
            self.register_materialization(node_id, properties);
        }
    }

    /// Refresh one materialization from the complete authoritative graph post-image,
    /// including edge-carried `DERIVED_FROM`/`GENERATED_BY` provenance.
    pub fn refresh_from_graph_view(&mut self, view: &GraphView, node_id: &str) {
        let node_ref = opaque_identity(node_id);
        self.materializations.remove(&node_ref);
        self.materialization_deps.remove(&node_ref);
        self.remove_materialization_generator(&node_ref);
        self.stale_materializations.remove(&node_ref);
        self.retracted_materializations.remove(&node_ref);
        if view.node_properties.contains_key(node_id) {
            self.register_from_graph_view(view, node_id);
        }
    }

    fn apply_wakeup_event(
        &mut self,
        event: &IncrementalReasoningEvent,
        committed_source_graph_version: u64,
        view: &GraphView,
        touched_nodes: &mut BTreeSet<String>,
        touched_edges: &mut BTreeSet<(String, String)>,
        delta: &mut IncrementalDelta,
    ) -> Result<(), String> {
        match event {
            IncrementalReasoningEvent::NodeUpserted {
                node_ref,
                dependency_refs,
                generator_ref,
                is_materialization,
            } => {
                self.stale_dependents(node_ref, delta);
                if *is_materialization {
                    self.register_materialization_refs(
                        node_ref,
                        dependency_refs.clone(),
                        generator_ref.clone(),
                    );
                } else {
                    self.materializations.remove(node_ref);
                    self.materialization_deps.remove(node_ref);
                    self.remove_materialization_generator(node_ref);
                    self.stale_materializations.remove(node_ref);
                    self.retracted_materializations.remove(node_ref);
                }
                touched_nodes.insert(node_ref.clone());
            }
            IncrementalReasoningEvent::NodeRemoved { node_ref } => {
                let was_materialization = self.materializations.contains(node_ref);
                self.remove_incident(node_ref, delta);
                self.materialization_deps.remove(node_ref);
                self.remove_materialization_generator(node_ref);
                self.stale_materializations.remove(node_ref);
                if was_materialization {
                    self.retracted_materializations.insert(node_ref.clone());
                } else {
                    self.materializations.remove(node_ref);
                    self.retracted_materializations.remove(node_ref);
                }
                self.stale_dependents(node_ref, delta);
                touched_nodes.insert(node_ref.clone());
            }
            IncrementalReasoningEvent::NodeChanged { node_ref } => {
                self.stale_dependents(node_ref, delta);
                touched_nodes.insert(node_ref.clone());
            }
            IncrementalReasoningEvent::EdgeUpserted {
                source_ref,
                target_ref,
                relationship,
            } => {
                if let Some(relationship) = relationship {
                    self.add_edge_relationship(source_ref, target_ref, relationship);
                }
                self.stale_dependents(source_ref, delta);
                self.stale_dependents(target_ref, delta);
                touched_edges.insert((source_ref.clone(), target_ref.clone()));
            }
            IncrementalReasoningEvent::EdgeRemoved {
                source_ref,
                target_ref,
            } => {
                if self.remove_edge_refs(source_ref, target_ref) {
                    delta.edge_changes += 1;
                }
                self.stale_dependents(source_ref, delta);
                self.stale_dependents(target_ref, delta);
                touched_edges.insert((source_ref.clone(), target_ref.clone()));
            }
            IncrementalReasoningEvent::Invalidate {
                invalidation,
                subject_ref,
            } => {
                let kind = match invalidation.as_str() {
                    "policy_changed" => ProjectionInvalidationKind::PolicyChanged,
                    "model_retired" => ProjectionInvalidationKind::ModelRetired,
                    "ontology_evolved" => ProjectionInvalidationKind::OntologyEvolved,
                    _ => return Err("reasoning invalidation event is invalid".to_string()),
                };
                delta.newly_stale.extend(self.invalidate(kind, subject_ref));
            }
            IncrementalReasoningEvent::Recompute {
                materialization_ref,
                expected_source_graph_version,
            } => {
                self.recompute_from_ref(
                    materialization_ref,
                    *expected_source_graph_version,
                    committed_source_graph_version,
                    view,
                )?;
            }
            IncrementalReasoningEvent::InvalidateAll => {
                let prior_epoch = self.recompute_epoch;
                let mut rebuilt = Self::from_graph_view(view);
                rebuilt.recompute_epoch = prior_epoch;
                for materialization in &rebuilt.materializations {
                    if rebuilt
                        .stale_materializations
                        .insert(materialization.clone())
                    {
                        delta.newly_stale.insert(materialization.clone());
                    }
                }
                *self = rebuilt;
                touched_nodes.clear();
                touched_edges.clear();
            }
        }
        Ok(())
    }

    fn apply(&mut self, method: &Method, delta: &mut IncrementalDelta) {
        match method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => {
                self.stale_dependents(node_id, delta);
                self.register_materialization(node_id, properties_msgpack);
            }
            Method::RemoveNode { node_id } => {
                let node_ref = opaque_identity(node_id);
                let was_materialization = self.materializations.contains(&node_ref);
                self.remove_incident(node_id, delta);
                self.materialization_deps.remove(&node_ref);
                self.remove_materialization_generator(&node_ref);
                self.stale_materializations.remove(&node_ref);
                if was_materialization {
                    self.retracted_materializations.insert(node_ref);
                } else {
                    self.materializations.remove(&node_ref);
                    self.retracted_materializations.remove(&node_ref);
                }
                self.stale_dependents(node_id, delta);
            }
            Method::CompareAndSetNodeFields { node_id, .. } => {
                self.stale_dependents(node_id, delta);
            }
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            } => {
                if self.add_edge(source_id, target_id, properties_msgpack) {
                    delta.edge_changes += 1;
                    self.stale_dependents(&format!("{source_id}->{target_id}"), delta);
                    self.stale_dependents(source_id, delta);
                    self.stale_dependents(target_id, delta);
                }
            }
            Method::RemoveEdge {
                source_id,
                target_id,
            } => {
                if self.remove_edge(source_id, target_id) {
                    delta.edge_changes += 1;
                }
                self.stale_dependents(&format!("{source_id}->{target_id}"), delta);
                self.stale_dependents(source_id, delta);
                self.stale_dependents(target_id, delta);
            }
            Method::ApplyMutation { event_type, query } => {
                let kind = match event_type.as_str() {
                    "policy_changed" => Some(ProjectionInvalidationKind::PolicyChanged),
                    "model_retired" => Some(ProjectionInvalidationKind::ModelRetired),
                    "ontology_evolved" => Some(ProjectionInvalidationKind::OntologyEvolved),
                    _ => None,
                };
                if let Some(kind) = kind {
                    delta.newly_stale.extend(self.invalidate(kind, query));
                }
            }
            _ => {}
        }
    }

    fn register_from_graph_view(&mut self, view: &GraphView, node_id: &str) {
        let node_properties = view
            .node_properties
            .get(node_id)
            .map(|value| value.as_slice());
        let outgoing = view
            .edge_properties
            .iter()
            .filter_map(|((source, target), versions)| {
                (source == node_id)
                    .then(|| {
                        versions
                            .last()
                            .map(|properties| (target.clone(), properties.as_slice()))
                    })
                    .flatten()
            });
        let (dependencies, generator) = crate::resolve_provenance(node_properties, outgoing);
        if dependencies.is_empty() && generator.is_none() {
            return;
        }
        let node_ref = opaque_identity(node_id);
        self.register_materialization_refs(&node_ref, dependencies, generator);
    }

    fn register_materialization(&mut self, node_id: &str, properties: &[u8]) {
        let node_ref = opaque_identity(node_id);
        let Some(value) = decode(properties) else {
            self.materializations.remove(&node_ref);
            self.materialization_deps.remove(&node_ref);
            self.remove_materialization_generator(&node_ref);
            self.stale_materializations.remove(&node_ref);
            self.retracted_materializations.remove(&node_ref);
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
            self.materializations.remove(&node_ref);
            self.materialization_deps.remove(&node_ref);
            self.remove_materialization_generator(&node_ref);
            self.stale_materializations.remove(&node_ref);
            self.retracted_materializations.remove(&node_ref);
            return;
        }
        self.register_materialization_refs(&node_ref, deps, generator);
    }

    fn register_materialization_refs(
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

    fn replace_materialization_generator(
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

    fn remove_materialization_generator(&mut self, materialization: &str) -> Option<String> {
        let generator = self.materialization_generators.remove(materialization)?;
        remove_value(
            &mut self.generator_materializations,
            &generator,
            materialization,
        );
        Some(generator)
    }

    fn canonical_edge_generators(&self) -> BTreeMap<String, String> {
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

    fn reconcile_edge_generator(&mut self, materialization: &str) {
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

    fn add_edge(&mut self, source: &str, target: &str, properties: &[u8]) -> bool {
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

    fn add_edge_relationship(&mut self, source: &str, target: &str, relationship: &str) -> bool {
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

    fn remove_edge(&mut self, source: &str, target: &str) -> bool {
        self.remove_edge_refs(&opaque_identity(source), &opaque_identity(target))
    }

    fn remove_edge_refs(&mut self, source: &str, target: &str) -> bool {
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

    fn remove_incident(&mut self, node_id: &str, delta: &mut IncrementalDelta) {
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

    fn stale_dependents(&mut self, changed: &str, delta: &mut IncrementalDelta) {
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

/// Stable domain-separated projection identity. Calling it with an identity that
/// is already in this namespace is idempotent, which lets transitive dependency
/// traversal operate entirely on opaque values.
fn opaque_identity(value: &str) -> String {
    if value.strip_prefix("eg:reasoning:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    }) {
        return value.to_ascii_lowercase();
    }
    format!(
        "eg:reasoning:{}",
        hex::encode(Sha256::digest(value.as_bytes()))
    )
}

/// Return the stable privacy-safe identity used by the durable reasoning
/// projection. The operation is idempotent for already-projected references.
pub fn projection_identity(value: &str) -> String {
    opaque_identity(value)
}

fn decode(properties: &[u8]) -> Option<serde_json::Value> {
    eg_types::msgpack::decode_property_value(properties).ok()
}

fn remove_value(index: &mut BTreeMap<String, BTreeSet<String>>, key: &str, value: &str) {
    let remove_key = if let Some(values) = index.get_mut(key) {
        values.remove(value);
        values.is_empty()
    } else {
        false
    };
    if remove_key {
        index.remove(key);
    }
}

fn event_for_method(method: &Method) -> IncrementalReasoningEvent {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            let value = decode(properties_msgpack);
            let dependency_refs = value
                .as_ref()
                .and_then(|value| value.get("invalidation_deps"))
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(opaque_identity)
                .collect::<BTreeSet<_>>();
            let generator_ref = value
                .as_ref()
                .and_then(|value| value.get("generating_activity"))
                .and_then(serde_json::Value::as_str)
                .map(opaque_identity);
            IncrementalReasoningEvent::NodeUpserted {
                node_ref: opaque_identity(node_id),
                is_materialization: !dependency_refs.is_empty() || generator_ref.is_some(),
                dependency_refs,
                generator_ref,
            }
        }
        Method::RemoveNode { node_id } => IncrementalReasoningEvent::NodeRemoved {
            node_ref: opaque_identity(node_id),
        },
        Method::CompareAndSetNodeFields { node_id, .. } => IncrementalReasoningEvent::NodeChanged {
            node_ref: opaque_identity(node_id),
        },
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => IncrementalReasoningEvent::EdgeUpserted {
            source_ref: opaque_identity(source_id),
            target_ref: opaque_identity(target_id),
            relationship: decode(properties_msgpack)
                .and_then(|value| {
                    value
                        .get("relationship")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_ascii_uppercase)
                })
                .filter(|relationship| {
                    matches!(
                        relationship.as_str(),
                        "SUPPORTS"
                            | "CONTRADICTS"
                            | "ATTACKS"
                            | "CAUSES"
                            | "ENABLES"
                            | "DERIVED_FROM"
                            | "GENERATED_BY"
                    )
                }),
        },
        Method::RemoveEdge {
            source_id,
            target_id,
        } => IncrementalReasoningEvent::EdgeRemoved {
            source_ref: opaque_identity(source_id),
            target_ref: opaque_identity(target_id),
        },
        Method::ApplyMutation { event_type, query }
            if matches!(
                event_type.as_str(),
                "policy_changed" | "model_retired" | "ontology_evolved"
            ) =>
        {
            IncrementalReasoningEvent::Invalidate {
                invalidation: event_type.clone(),
                subject_ref: opaque_identity(query),
            }
        }
        Method::RecomputeMaterialization {
            derived_id,
            expected_source_graph_version,
        } => IncrementalReasoningEvent::Recompute {
            materialization_ref: opaque_identity(derived_id),
            expected_source_graph_version: *expected_source_graph_version,
        },
        Method::AddEmbedding { node_id, .. } => IncrementalReasoningEvent::NodeChanged {
            node_ref: opaque_identity(node_id),
        },
        _ => IncrementalReasoningEvent::InvalidateAll,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(source: &str, target: &str, relationship: &str) -> Method {
        Method::AddEdge {
            source_id: source.to_string(),
            target_id: target.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(
                &serde_json::json!({"relationship": relationship}),
            )
            .unwrap(),
        }
    }

    #[test]
    fn contradictions_coexist_without_explosion() {
        let mut index = IncrementalReasoningIndex::default();
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "b1".into(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[edge("p", "not-p", "CONTRADICTS")],
            )
            .unwrap();
        assert_eq!(
            index.conflicts_with("p"),
            BTreeSet::from([opaque_identity("not-p")])
        );
        assert!(index.causes_of("p").is_empty());
    }

    #[test]
    fn duplicate_position_is_idempotent() {
        let mut index = IncrementalReasoningIndex::default();
        let position = ProjectionPosition {
            batch_id: "b1".into(),
            ordinal: 0,
            source_graph_version: 1,
        };
        assert_eq!(
            index
                .apply_batch(position.clone(), &[edge("a", "b", "CAUSES")])
                .unwrap()
                .edge_changes,
            1
        );
        assert_eq!(
            index
                .apply_batch(position, &[edge("a", "c", "CAUSES")])
                .unwrap()
                .edge_changes,
            0
        );
        assert_eq!(index.causes_of("a"), BTreeSet::from([opaque_identity("b")]));
    }

    #[test]
    fn serialized_projection_does_not_copy_source_identifiers() {
        let mut index = IncrementalReasoningIndex::default();
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "caller-visible-batch-label".into(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[edge("source-node-label", "target-node-label", "CAUSES")],
            )
            .unwrap();
        let bytes = rmp_serde::to_vec_named(&index).unwrap();
        let rendered = String::from_utf8_lossy(&bytes);
        assert!(!rendered.contains("caller-visible-batch-label"));
        assert!(!rendered.contains("source-node-label"));
        assert!(!rendered.contains("target-node-label"));
    }

    #[test]
    fn projection_wakeup_contains_no_source_identifiers_or_properties() {
        let method = Method::AddNode {
            node_id: "derived-source-label".into(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                "invalidation_deps": ["base-source-label"],
                "generating_activity": "model-source-label",
                "free_text": "must-not-be-copied",
            }))
            .unwrap(),
        };
        let wakeup = ReasoningProjectionWakeup::new(
            1,
            "a".repeat(64),
            ReasoningProjectionWakeup::events_for_methods(&[method]),
        )
        .unwrap();
        let bytes = rmp_serde::to_vec_named(&wakeup).unwrap();
        let rendered = String::from_utf8_lossy(&bytes);
        for forbidden in [
            "derived-source-label",
            "base-source-label",
            "model-source-label",
            "must-not-be-copied",
        ] {
            assert!(!rendered.contains(forbidden));
        }
    }

    #[test]
    fn privacy_safe_wakeup_invalidates_from_authoritative_post_image() {
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "derived".into(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "invalidation_deps": ["base"],
            }))
            .unwrap(),
        );
        let view = core.analysis_snapshot();
        let mut index = IncrementalReasoningIndex::from_graph_view(&view);
        let wakeup = ReasoningProjectionWakeup::new(
            1,
            "b".repeat(64),
            ReasoningProjectionWakeup::events_for_methods(&[Method::CompareAndSetNodeFields {
                node_id: "base".into(),
                conditions_msgpack: Vec::new(),
                updates_msgpack: Vec::new(),
            }]),
        )
        .unwrap();
        index
            .apply_wakeup(
                ProjectionPosition {
                    batch_id: "state-backed".into(),
                    ordinal: 1,
                    source_graph_version: 1,
                },
                &wakeup,
                &view,
            )
            .unwrap();
        assert_eq!(
            index.status_of("derived"),
            Some(ProjectedMaterializationStatus::Stale)
        );
    }

    #[test]
    fn committed_recompute_wakeup_survives_same_batch_operation_ordinal() {
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "base".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"type": "fact"})).unwrap(),
        );
        core.add_node(
            "derived".into(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "invalidation_deps": ["base"],
                "generating_activity": "model",
            }))
            .unwrap(),
        );
        let view = core.analysis_snapshot();
        let mut index = IncrementalReasoningIndex::from_graph_view(&view);
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "prior".into(),
                    ordinal: 0,
                    source_graph_version: 2,
                },
                &[],
            )
            .unwrap();
        index.invalidate(ProjectionInvalidationKind::PolicyChanged, "base");
        assert_eq!(
            index.status_of("derived"),
            Some(ProjectedMaterializationStatus::Stale)
        );

        // The generic committed-operation outbox row precedes the projection
        // wakeup and can advance the watermark to the no-op commit target first.
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "recompute".into(),
                    ordinal: 0,
                    source_graph_version: 3,
                },
                &[],
            )
            .unwrap();
        let wakeup = ReasoningProjectionWakeup::new(
            1,
            "c".repeat(64),
            vec![IncrementalReasoningEvent::Recompute {
                materialization_ref: projection_identity("derived"),
                expected_source_graph_version: 2,
            }],
        )
        .unwrap();
        index
            .apply_wakeup(
                ProjectionPosition {
                    batch_id: "recompute".into(),
                    ordinal: 1,
                    source_graph_version: 3,
                },
                &wakeup,
                &view,
            )
            .unwrap();

        assert_eq!(
            index.status_of("derived"),
            Some(ProjectedMaterializationStatus::Fresh)
        );
        assert_eq!(
            index
                .position
                .as_ref()
                .map(|position| position.source_graph_version),
            Some(3)
        );
    }

    #[test]
    fn projection_watermark_is_required_and_strictly_monotonic() {
        let mut index = IncrementalReasoningIndex::default();
        assert!(index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "zero".into(),
                    ordinal: 0,
                    source_graph_version: 0,
                },
                &[],
            )
            .unwrap_err()
            .contains("non-zero"));

        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "current".into(),
                    ordinal: 0,
                    source_graph_version: 3,
                },
                &[],
            )
            .unwrap();
        assert!(index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "older".into(),
                    ordinal: 0,
                    source_graph_version: 2,
                },
                &[],
            )
            .unwrap_err()
            .contains("STALE_PROJECTION_POSITION"));
        assert!(index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "different-batch-same-version".into(),
                    ordinal: 1,
                    source_graph_version: 3,
                },
                &[],
            )
            .unwrap_err()
            .contains("STALE_PROJECTION_POSITION"));
    }

    #[test]
    fn projection_snapshot_requires_current_schema() {
        let index = IncrementalReasoningIndex::default();
        index.validate().unwrap();
        let encoded = serde_json::to_value(index).unwrap();
        assert_eq!(
            encoded["schema_version"].as_u64(),
            Some(u64::from(REASONING_PROJECTION_VERSION))
        );

        let mut without_version = encoded.clone();
        without_version
            .as_object_mut()
            .unwrap()
            .remove("schema_version");
        assert!(serde_json::from_value::<IncrementalReasoningIndex>(without_version).is_err());

        let mut without_reverse_index = encoded;
        without_reverse_index
            .as_object_mut()
            .unwrap()
            .remove("generator_materializations");
        assert!(
            serde_json::from_value::<IncrementalReasoningIndex>(without_reverse_index).is_err()
        );
    }

    #[test]
    fn generator_reverse_index_roundtrips_and_drives_targeted_invalidation() {
        let generated = |node_id: &str, generator: &str| Method::AddNode {
            node_id: node_id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                "generating_activity": generator,
            }))
            .unwrap(),
        };
        let mut index = IncrementalReasoningIndex::default();
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "create".into(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[
                    generated("derived-a", "model-v1"),
                    generated("derived-b", "model-v1"),
                    generated("derived-c", "model-v2"),
                ],
            )
            .unwrap();
        index.validate().unwrap();

        let model_v1 = opaque_identity("model-v1");
        let model_v1_outputs =
            BTreeSet::from([opaque_identity("derived-a"), opaque_identity("derived-b")]);
        assert_eq!(
            index.generator_materializations.get(&model_v1),
            Some(&model_v1_outputs)
        );

        let bytes = rmp_serde::to_vec_named(&index).unwrap();
        let mut restored: IncrementalReasoningIndex = rmp_serde::from_slice(&bytes).unwrap();
        restored.validate().unwrap();
        assert_eq!(
            restored.invalidate(ProjectionInvalidationKind::ModelRetired, "model-v1"),
            BTreeSet::from([opaque_identity("derived-a"), opaque_identity("derived-b"),])
        );
        assert_eq!(
            restored.status_of("derived-c"),
            Some(ProjectedMaterializationStatus::Fresh)
        );

        let derived_a = opaque_identity("derived-a");
        restored.register_materialization_refs(
            &derived_a,
            BTreeSet::new(),
            Some("model-v2".to_string()),
        );
        let remaining_model_v1_output = BTreeSet::from([opaque_identity("derived-b")]);
        assert_eq!(
            restored.generator_materializations.get(&model_v1),
            Some(&remaining_model_v1_output)
        );
        restored.refresh_materialization("derived-b", None);
        assert!(!restored.generator_materializations.contains_key(&model_v1));
        restored.validate().unwrap();
    }

    #[test]
    fn generated_by_edges_have_one_order_independent_canonical_generator() {
        let mut index = IncrementalReasoningIndex::default();
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "generators".into(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[
                    edge("derived", "model-z", "GENERATED_BY"),
                    edge("derived", "model-a", "GENERATED_BY"),
                ],
            )
            .unwrap();

        let derived = opaque_identity("derived");
        let model_a = opaque_identity("model-a");
        let model_z = opaque_identity("model-z");
        assert_eq!(
            index.materialization_generators.get(&derived),
            Some(&model_a)
        );
        assert_eq!(
            index.materialization_deps.get(&derived),
            Some(&BTreeSet::from([model_a.clone(), model_z.clone()]))
        );

        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "remove-canonical".into(),
                    ordinal: 0,
                    source_graph_version: 2,
                },
                &[Method::RemoveEdge {
                    source_id: "derived".into(),
                    target_id: "model-a".into(),
                }],
            )
            .unwrap();
        assert_eq!(
            index.materialization_generators.get(&derived),
            Some(&model_z)
        );
        assert_eq!(
            index.generator_materializations.get(&model_z),
            Some(&BTreeSet::from([derived.clone()]))
        );
        assert!(!index.generator_materializations.contains_key(&model_a));
        assert_eq!(
            index.materialization_deps.get(&derived),
            Some(&BTreeSet::from([model_z]))
        );
        index.validate().unwrap();
    }

    #[test]
    fn generated_by_dependency_semantics_match_graph_bootstrap() {
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "derived".into(),
            rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
        );
        core.add_node(
            "model".into(),
            rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
        );
        core.add_edge(
            "derived".into(),
            "model".into(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "relationship": "GENERATED_BY"
            }))
            .unwrap(),
        )
        .unwrap();
        let bootstrapped = IncrementalReasoningIndex::from_graph_view(&core.analysis_snapshot());

        let mut incremental = IncrementalReasoningIndex::default();
        incremental
            .apply_batch(
                ProjectionPosition {
                    batch_id: "incremental".into(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[edge("derived", "model", "GENERATED_BY")],
            )
            .unwrap();

        assert_eq!(incremental.materializations, bootstrapped.materializations);
        assert_eq!(
            incremental.materialization_deps,
            bootstrapped.materialization_deps
        );
        assert_eq!(
            incremental.materialization_generators,
            bootstrapped.materialization_generators
        );
        assert_eq!(
            incremental.generator_materializations,
            bootstrapped.generator_materializations
        );
    }

    #[test]
    fn multiple_generated_by_edges_match_bootstrap_and_incremental_projection() {
        let core = eg_core::graph::GraphCore::new();
        for node_id in ["derived", "model-a", "model-z"] {
            core.add_node(
                node_id.into(),
                rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            );
        }
        for generator in ["model-a", "model-z"] {
            core.add_edge(
                "derived".into(),
                generator.into(),
                rmp_serde::to_vec_named(&serde_json::json!({
                    "relationship": "GENERATED_BY"
                }))
                .unwrap(),
            )
            .unwrap();
        }
        let bootstrapped = IncrementalReasoningIndex::from_graph_view(&core.analysis_snapshot());

        // Deliberately reverse source-graph order. Incremental arrival order must
        // not alter the canonical generator or either reverse dependency index.
        let mut incremental = IncrementalReasoningIndex::default();
        incremental
            .apply_batch(
                ProjectionPosition {
                    batch_id: "incremental-multiple-generators".into(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[
                    edge("derived", "model-z", "GENERATED_BY"),
                    edge("derived", "model-a", "GENERATED_BY"),
                ],
            )
            .unwrap();

        let derived = opaque_identity("derived");
        let expected_generator = [opaque_identity("model-a"), opaque_identity("model-z")]
            .into_iter()
            .min()
            .unwrap();
        assert_eq!(
            incremental.materialization_generators.get(&derived),
            Some(&expected_generator)
        );
        assert_eq!(incremental.materializations, bootstrapped.materializations);
        assert_eq!(incremental.provenance_edges, bootstrapped.provenance_edges);
        assert_eq!(
            incremental.materialization_deps,
            bootstrapped.materialization_deps
        );
        assert_eq!(
            incremental.materialization_generators,
            bootstrapped.materialization_generators
        );
        assert_eq!(
            incremental.generator_materializations,
            bootstrapped.generator_materializations
        );
        incremental.validate().unwrap();
        bootstrapped.validate().unwrap();
    }

    #[test]
    fn validation_rejects_a_mismatched_generator_reverse_index() {
        let mut index = IncrementalReasoningIndex::default();
        let materialization = opaque_identity("derived");
        index.register_materialization_refs(
            &materialization,
            BTreeSet::new(),
            Some("model".to_string()),
        );
        index.generator_materializations.clear();
        assert!(index
            .validate()
            .unwrap_err()
            .contains("reverse index is inconsistent"));
    }

    #[test]
    fn validation_rejects_generator_state_that_disagrees_with_provenance() {
        let mut index = IncrementalReasoningIndex::default();
        index.add_edge_relationship(
            &opaque_identity("derived"),
            &opaque_identity("model"),
            "GENERATED_BY",
        );
        index.remove_materialization_generator(&opaque_identity("derived"));
        assert!(index
            .validate()
            .unwrap_err()
            .contains("canonical generator edge is inconsistent"));
    }

    #[test]
    fn policy_model_and_ontology_events_invalidate_the_durable_index() {
        let mut index = IncrementalReasoningIndex::default();
        let materialization = Method::AddNode {
            node_id: "derived".into(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                "invalidation_deps": ["policy-scope"],
                "generating_activity": "model-v1",
            }))
            .unwrap(),
        };
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "create".into(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[materialization],
            )
            .unwrap();
        assert_eq!(
            index.status_of("derived"),
            Some(ProjectedMaterializationStatus::Fresh)
        );

        let changed = index.invalidate(ProjectionInvalidationKind::PolicyChanged, "policy-scope");
        assert_eq!(changed, BTreeSet::from([opaque_identity("derived")]));
        assert_eq!(
            index.status_of("derived"),
            Some(ProjectedMaterializationStatus::Stale)
        );

        let fence = index.claim_recompute("derived", 1).unwrap();
        index
            .complete_recompute(
                "derived",
                1,
                fence,
                Some((
                    BTreeSet::from(["policy-scope".to_string()]),
                    Some("model-v1".to_string()),
                )),
            )
            .unwrap();
        assert_eq!(
            index.status_of("derived"),
            Some(ProjectedMaterializationStatus::Fresh)
        );

        assert_eq!(
            index.invalidate(ProjectionInvalidationKind::ModelRetired, "model-v1"),
            BTreeSet::from([opaque_identity("derived")])
        );
        let fence = index.claim_recompute("derived", 1).unwrap();
        index
            .complete_recompute(
                "derived",
                1,
                fence,
                Some((BTreeSet::new(), Some("ontology-v2".to_string()))),
            )
            .unwrap();
        assert_eq!(
            index.invalidate(ProjectionInvalidationKind::OntologyEvolved, "ontology-v2",),
            BTreeSet::from([opaque_identity("derived")])
        );
    }

    #[test]
    fn recompute_fence_rejects_a_late_writeback() {
        let mut index = IncrementalReasoningIndex::default();
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "create".into(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[Method::AddNode {
                    node_id: "derived".into(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "invalidation_deps": ["base"],
                    }))
                    .unwrap(),
                }],
            )
            .unwrap();
        index.invalidate(ProjectionInvalidationKind::PolicyChanged, "base");
        let fence = index.claim_recompute("derived", 1).unwrap();
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "newer".into(),
                    ordinal: 0,
                    source_graph_version: 2,
                },
                &[],
            )
            .unwrap();
        assert!(index
            .complete_recompute(
                "derived",
                1,
                fence,
                Some((BTreeSet::from(["base".to_string()]), None)),
            )
            .unwrap_err()
            .contains("STALE_RECOMPUTE_FENCE"));
    }
}
