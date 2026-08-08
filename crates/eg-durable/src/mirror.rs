//! The mirrored-property DTO (CONCEPT:AU-KG.storage.durable-execution-unit).
//!
//! Matches, field for field, the `:DurableExecutionUnit` OWL schema reserved in the
//! sibling `agent-utilities` change this lane makes
//! (`agent_utilities/knowledge_graph/ontology_orchestration.ttl`): `:backendRef`,
//! `:durableStatus`, `:checkpointRef`, `:definitionVersion`, `:leaseEpoch`,
//! `:idempotencyKey`. DE0 defines the shape only — no code here reads or writes a
//! KG node; DE1 is responsible for actually populating and committing this DTO
//! mirror-on-next-write, the same pattern the ontology's own `RunTrace` checkpoint
//! linkage already uses (never a bulk migration).

use serde::{Deserialize, Serialize};

use crate::route::DurableBackendKind;

/// One durable-execution unit's mirrored KG properties, in the source of truth's OWN
/// vocabulary (never re-normalized — see the ontology's `:durableStatus` doc).
///
/// The struct is intentionally "wide": most fields are `Option` because they are
/// meaningful only for a subset of backends (`lease_epoch` is Jobs-only,
/// `idempotency_key` is unset for a fresh `StatechartInstance`, `definition_version`
/// stays empty everywhere until DE5 lands enforcement). A `None` field mirrors the
/// backend's own absence of that concept; it is never a sentinel for "not yet
/// mirrored".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DurableExecutionUnitMirror {
    /// Opaque foreign-key-shaped pointer back to the authoritative row in the owning
    /// backend (`:backendRef`) — e.g. an `eg-statechart` `InstanceId`, an `eg-jobs`
    /// `JobId`, an `eg-mutation-store` `batch_id`, or a
    /// `(session_id, node_id)` pair rendered as one opaque string for
    /// `DurableExecutionManager`.
    pub backend_ref: String,

    /// Which backend owns `backend_ref` — resolves which `:DurableExecutionUnit`
    /// subclass this mirror instantiates (`:StatechartInstance`/`:AnalyticsJob`/
    /// `:SagaCoordination`/`:DurableRun`).
    pub kind: DurableBackendKind,

    /// The backend's own lifecycle status label (`:durableStatus`) — deliberately a
    /// free-form string, not a shared enum, so the mirror can never drift from what
    /// the backend itself reports.
    pub durable_status: String,

    /// Pointer to the most recent durable checkpoint (`:checkpointRef`), never the
    /// checkpoint payload itself.
    pub checkpoint_ref: Option<String>,

    /// The definition/deployment version this unit started under (`:definitionVersion`).
    /// Schema-only at DE0 — always `None` until DE5 adds resume-time enforcement.
    pub definition_version: Option<String>,

    /// Monotonic fencing epoch (`:leaseEpoch`) — `Some` only for
    /// [`DurableBackendKind::Jobs`] units; every other backend has no lease concept.
    pub lease_epoch: Option<u64>,

    /// The backend's real idempotency key (`:idempotencyKey`), where one exists.
    pub idempotency_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json_with_all_optionals_absent() {
        let mirror = DurableExecutionUnitMirror {
            backend_ref: "instance-42".to_string(),
            kind: DurableBackendKind::Statechart,
            durable_status: "active".to_string(),
            checkpoint_ref: None,
            definition_version: None,
            lease_epoch: None,
            idempotency_key: None,
        };
        let json = serde_json::to_string(&mirror).unwrap();
        let back: DurableExecutionUnitMirror = serde_json::from_str(&json).unwrap();
        assert_eq!(mirror, back);
    }

    #[test]
    fn round_trips_through_json_with_every_optional_present() {
        let mirror = DurableExecutionUnitMirror {
            backend_ref: "job-7".to_string(),
            kind: DurableBackendKind::Jobs,
            durable_status: "running".to_string(),
            checkpoint_ref: Some("stage=fetch;progress=0.5".to_string()),
            definition_version: Some("2026.08.07-1".to_string()),
            lease_epoch: Some(3),
            idempotency_key: Some("idem-abc".to_string()),
        };
        let json = serde_json::to_string(&mirror).unwrap();
        let back: DurableExecutionUnitMirror = serde_json::from_str(&json).unwrap();
        assert_eq!(mirror, back);
    }
}
