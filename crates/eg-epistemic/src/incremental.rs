//! Change-driven epistemic projection used by the durable reasoning worker.
//!
//! This is intentionally a compact index, not a second graph.  It records only
//! support/conflict/causal edges and derived-materialization dependencies, and is
//! updated from committed `MutationOperation`s in outbox order.  Contradictions
//! remain explicit edges (paraconsistent, never exploded into arbitrary facts).

use std::collections::{BTreeMap, BTreeSet};

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

mod events;
mod materializations;
mod mutation;
mod recompute;
mod relations;
mod validation;
mod wakeup;

pub use events::{IncrementalReasoningEvent, ReasoningProjectionWakeup};

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

/// Remove `value` from the set stored under `key` in a reverse index, and
/// drop `key` entirely once its set is left empty so the index never
/// accumulates stale keys mapped to nothing. A missing `key` is a no-op.
/// Shared by the incremental reasoning index's own relation/materialization
/// bookkeeping and by [`crate::recompute`]'s in-crate truth-maintenance
/// fixture, which keeps the same `BTreeMap<String, BTreeSet<String>>`
/// dependents/generators shape (there under the name `remove_reverse_index_entry`).
pub(crate) fn remove_value(index: &mut BTreeMap<String, BTreeSet<String>>, key: &str, value: &str) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::protocol::Method;

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
