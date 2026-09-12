use std::collections::BTreeSet;

use eg_types::protocol::Method;
use serde::{Deserialize, Serialize};

use super::{decode, opaque_identity};

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
    pub(super) fn validate(&self) -> Result<(), String> {
        match self {
            Self::NodeUpserted {
                node_ref,
                dependency_refs,
                generator_ref,
                is_materialization,
            } => validate_node_upserted(
                node_ref,
                dependency_refs,
                generator_ref,
                *is_materialization,
            ),
            Self::NodeRemoved { node_ref } | Self::NodeChanged { node_ref } => {
                validate_node_ref(node_ref)
            }
            Self::EdgeUpserted {
                source_ref,
                target_ref,
                relationship,
            } => validate_edge_upserted(source_ref, target_ref, relationship.as_deref()),
            Self::EdgeRemoved {
                source_ref,
                target_ref,
            } => validate_edge_refs(source_ref, target_ref),
            Self::Invalidate {
                invalidation,
                subject_ref,
            } => validate_invalidation(invalidation, subject_ref),
            Self::Recompute {
                materialization_ref,
                expected_source_graph_version,
            } => validate_recompute(materialization_ref, *expected_source_graph_version),
            Self::InvalidateAll => Ok(()),
        }
    }
}

fn validate_node_upserted(
    node_ref: &str,
    dependency_refs: &BTreeSet<String>,
    generator_ref: &Option<String>,
    is_materialization: bool,
) -> Result<(), String> {
    let refs_are_opaque = valid_ref(node_ref)
        && dependency_refs.iter().all(|value| valid_ref(value))
        && generator_ref.as_deref().is_none_or(valid_ref);
    if !refs_are_opaque
        || is_materialization != (!dependency_refs.is_empty() || generator_ref.is_some())
    {
        return Err("reasoning node event is invalid".to_string());
    }
    Ok(())
}

fn validate_node_ref(node_ref: &str) -> Result<(), String> {
    if valid_ref(node_ref) {
        Ok(())
    } else {
        Err("reasoning node event is invalid".to_string())
    }
}

fn validate_edge_upserted(
    source_ref: &str,
    target_ref: &str,
    relationship: Option<&str>,
) -> Result<(), String> {
    if valid_ref(source_ref) && valid_ref(target_ref) && relationship.is_none_or(valid_relationship)
    {
        Ok(())
    } else {
        Err("reasoning edge event is invalid".to_string())
    }
}

fn validate_edge_refs(source_ref: &str, target_ref: &str) -> Result<(), String> {
    if valid_ref(source_ref) && valid_ref(target_ref) {
        Ok(())
    } else {
        Err("reasoning edge event is invalid".to_string())
    }
}

fn validate_invalidation(invalidation: &str, subject_ref: &str) -> Result<(), String> {
    if matches!(
        invalidation,
        "policy_changed" | "model_retired" | "ontology_evolved"
    ) && valid_ref(subject_ref)
    {
        Ok(())
    } else {
        Err("reasoning invalidation event is invalid".to_string())
    }
}

fn validate_recompute(
    materialization_ref: &str,
    expected_source_graph_version: u64,
) -> Result<(), String> {
    if valid_ref(materialization_ref)
        && expected_source_graph_version != 0
        && expected_source_graph_version != u64::MAX
    {
        Ok(())
    } else {
        Err("reasoning recompute event is invalid".to_string())
    }
}

fn valid_ref(value: &str) -> bool {
    opaque_identity(value) == value
}

fn valid_relationship(value: &str) -> bool {
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
