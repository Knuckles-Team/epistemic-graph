use std::collections::{BTreeMap, BTreeSet};

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

/// Most events a wake-up carries inline. Past it the notice is a bounded
/// summary and the events are paged from the committed batch it names.
///
/// The outbox payload is screened as bounded inline material (8 MiB / 200 000
/// MessagePack items). One event is at most 11 items, so an O(operations)
/// notice refused every repository-sized batch (20 000+ operations); the cap
/// keeps any notice to a few thousand items whatever the batch size.
pub const MAX_INLINE_WAKEUP_EVENTS: usize = 256;

/// Where a wake-up's per-operation events live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeupEventSource {
    /// `events` holds exactly one event per committed operation.
    Inline,
    /// `events` is empty: the consumer derives them from the committed batch
    /// the outbox row names (its key and `batch_id`), bound by
    /// `operations_sha256` and checked against `kind_counts`.
    CommittedBatch,
}

/// Current required payload for `engine.projection.rebuild` outbox intents: a
/// BOUNDED notice. It always carries the operation count, the digest of the
/// committed operations and per-event-kind counts; the events themselves only
/// when there are at most [`MAX_INLINE_WAKEUP_EVENTS`] of them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasoningProjectionWakeup {
    pub schema_version: u16,
    pub operation_count: u32,
    pub operations_sha256: String,
    pub kind_counts: BTreeMap<String, u32>,
    pub event_source: WakeupEventSource,
    pub events: Vec<IncrementalReasoningEvent>,
}

impl ReasoningProjectionWakeup {
    pub const SCHEMA_VERSION: u16 = 2;

    /// A notice whose events are always inline. Only for events that can NOT
    /// be re-derived from the committed operations (state-backed batches,
    /// whose operations are opaque receipts); every other producer uses
    /// [`Self::bounded`].
    pub fn new(
        operation_count: usize,
        operations_sha256: String,
        events: Vec<IncrementalReasoningEvent>,
    ) -> Result<Self, String> {
        Self::build(
            operation_count,
            operations_sha256,
            events,
            WakeupEventSource::Inline,
        )
    }

    /// The bounded notice for events that are exactly
    /// `events_for_methods(committed operations)`: inline when small, else a
    /// summary that references the committed batch.
    pub fn bounded(
        operation_count: usize,
        operations_sha256: String,
        events: Vec<IncrementalReasoningEvent>,
    ) -> Result<Self, String> {
        let source = if events.len() <= MAX_INLINE_WAKEUP_EVENTS {
            WakeupEventSource::Inline
        } else {
            WakeupEventSource::CommittedBatch
        };
        Self::build(operation_count, operations_sha256, events, source)
    }

    fn build(
        operation_count: usize,
        operations_sha256: String,
        events: Vec<IncrementalReasoningEvent>,
        event_source: WakeupEventSource,
    ) -> Result<Self, String> {
        let operation_count = u32::try_from(operation_count)
            .map_err(|_| "reasoning projection wake-up has too many operations".to_string())?;
        let kind_counts = kind_counts(&events);
        let events = match event_source {
            WakeupEventSource::Inline => events,
            WakeupEventSource::CommittedBatch => Vec::new(),
        };
        let wakeup = Self {
            schema_version: Self::SCHEMA_VERSION,
            operation_count,
            operations_sha256,
            kind_counts,
            event_source,
            events,
        };
        wakeup.validate()?;
        Ok(wakeup)
    }

    /// Materialize the events of a [`WakeupEventSource::CommittedBatch`]
    /// notice from the committed operations the caller has already bound to
    /// `operations_sha256`. The derived events must reproduce the notice's
    /// per-kind counts. An inline notice is returned unchanged.
    pub fn with_committed_events(self, committed: &[Method]) -> Result<Self, String> {
        if self.event_source == WakeupEventSource::Inline {
            return Ok(self);
        }
        let events = Self::events_for_methods(committed);
        if kind_counts(&events) != self.kind_counts {
            return Err("reasoning projection wake-up does not match its batch".to_string());
        }
        let wakeup = Self {
            event_source: WakeupEventSource::Inline,
            events,
            ..self
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
        if !self.counts_are_consistent() {
            return Err("reasoning projection wake-up event count is invalid".to_string());
        }
        for event in &self.events {
            event.validate()?;
        }
        Ok(())
    }

    /// The per-kind counts sum to the operation count, and inline events are
    /// exactly one per operation with those counts; a batch-referencing
    /// notice carries no events.
    fn counts_are_consistent(&self) -> bool {
        let counted: u64 = self
            .kind_counts
            .values()
            .map(|count| u64::from(*count))
            .sum();
        if counted != u64::from(self.operation_count) {
            return false;
        }
        match self.event_source {
            WakeupEventSource::Inline => {
                self.events.len() == self.operation_count as usize
                    && kind_counts(&self.events) == self.kind_counts
            }
            WakeupEventSource::CommittedBatch => self.events.is_empty(),
        }
    }

    /// Compile fixed-schema, privacy-safe events before state-backed operations are
    /// lowered to opaque receipts. Unknown mutation surfaces invalidate all tracked
    /// materializations rather than returning a false `Fresh` answer.
    pub fn events_for_methods(methods: &[Method]) -> Vec<IncrementalReasoningEvent> {
        methods.iter().map(event_for_method).collect()
    }
}

/// Count of events per kind tag (the serde `kind`), the notice's summary.
fn kind_counts(events: &[IncrementalReasoningEvent]) -> BTreeMap<String, u32> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.kind().to_string()).or_insert(0u32) += 1;
    }
    counts
}

impl IncrementalReasoningEvent {
    /// The event's wire kind tag.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NodeUpserted { .. } => "node_upserted",
            Self::NodeRemoved { .. } => "node_removed",
            Self::NodeChanged { .. } => "node_changed",
            Self::EdgeUpserted { .. } => "edge_upserted",
            Self::EdgeRemoved { .. } => "edge_removed",
            Self::Invalidate { .. } => "invalidate",
            Self::Recompute { .. } => "recompute",
            Self::InvalidateAll => "invalidate_all",
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn repository_methods(count: usize) -> Vec<Method> {
        (0..count)
            .map(|index| Method::AddNode {
                node_id: format!("repository-node-{index}"),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"type": "Blob"}))
                    .unwrap(),
            })
            .collect()
    }

    #[test]
    fn a_small_batch_keeps_its_events_inline() {
        let methods = repository_methods(MAX_INLINE_WAKEUP_EVENTS);
        let events = ReasoningProjectionWakeup::events_for_methods(&methods);
        let wakeup =
            ReasoningProjectionWakeup::bounded(methods.len(), "a".repeat(64), events.clone())
                .unwrap();
        assert_eq!(wakeup.event_source, WakeupEventSource::Inline);
        assert_eq!(wakeup.events, events);
    }

    #[test]
    fn a_large_batch_notice_is_a_bounded_summary_resolved_from_its_batch() {
        let methods = repository_methods(24_000);
        let events = ReasoningProjectionWakeup::events_for_methods(&methods);
        let notice =
            ReasoningProjectionWakeup::bounded(methods.len(), "b".repeat(64), events.clone())
                .unwrap();
        assert_eq!(notice.event_source, WakeupEventSource::CommittedBatch);
        assert!(notice.events.is_empty());
        assert_eq!(notice.kind_counts.get("node_upserted"), Some(&24_000));
        let bytes = rmp_serde::to_vec_named(&notice).unwrap();
        assert!(bytes.len() < 1024, "the notice is {} bytes", bytes.len());

        let resolved = notice.clone().with_committed_events(&methods).unwrap();
        assert_eq!(resolved.event_source, WakeupEventSource::Inline);
        assert_eq!(resolved.events, events, "every change reaches the consumer");
        assert!(notice.with_committed_events(&methods[1..]).is_err());
    }

    #[test]
    fn a_notice_that_claims_inline_events_it_lacks_is_invalid() {
        let mut wakeup = ReasoningProjectionWakeup::bounded(
            1,
            "c".repeat(64),
            ReasoningProjectionWakeup::events_for_methods(&repository_methods(1)),
        )
        .unwrap();
        wakeup.events.clear();
        assert!(wakeup.validate().is_err());
    }
}
