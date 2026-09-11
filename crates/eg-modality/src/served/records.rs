use serde::{Deserialize, Serialize};

use crate::artifact::{ArtifactBundle, Classification, Occurrence, OccurrenceId, OpaqueRef};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Active,
    Cold,
    Tombstoned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServedEventKind {
    Ingested,
    Updated,
    Deleted,
    MovedToCold,
    Restored,
    Reindexed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServedEvent {
    pub sequence: u64,
    pub occurrence_id: OccurrenceId,
    pub observation_version: u64,
    pub kind: ServedEventKind,
    pub tenant_ref: OpaqueRef,
    pub access_policy_ref: OpaqueRef,
}

/// Verified query authority. It is built from already opaque policy identities and
/// must exactly match the stored envelope; the runtime never trusts caller display
/// fields or performs a permissive fallback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServedPolicyScope {
    pub tenant_ref: OpaqueRef,
    pub access_policy_ref: OpaqueRef,
    pub purpose_ref: OpaqueRef,
    pub maximum_classification: Classification,
}

impl ServedPolicyScope {
    /// Verify one occurrence against authority derived by the serving boundary.
    /// Ingest adapters must call this before accepting a caller-provided bundle;
    /// queries and lifecycle methods call the same predicate internally.
    pub fn authorizes_occurrence(&self, occurrence: &Occurrence) -> bool {
        occurrence.policy.tenant_ref == self.tenant_ref
            && occurrence.policy.access_policy_ref == self.access_policy_ref
            && occurrence
                .policy
                .purpose_refs
                .iter()
                .any(|purpose| purpose == &self.purpose_ref)
            && occurrence.policy.classification <= self.maximum_classification
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServedRecord<T> {
    pub occurrence_id: OccurrenceId,
    pub observation_version: u64,
    pub lifecycle: LifecycleState,
    pub bundle: ArtifactBundle,
    /// `None` after a governed delete. The opaque semantic/audit envelope remains,
    /// while normalized payload and all external raw content are erased.
    pub value: Option<T>,
}

impl<T> ServedRecord<T> {
    pub fn occurrence(&self) -> Option<&Occurrence> {
        self.bundle
            .occurrences
            .iter()
            .find(|o| o.id == self.occurrence_id)
    }
}
