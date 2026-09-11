use serde::{Deserialize, Serialize};

use crate::artifact::{ArtifactBundle, OccurrenceId, OpaqueRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServedIngest<T> {
    pub idempotency_ref: OpaqueRef,
    pub target_occurrence_id: OccurrenceId,
    /// `None` creates a new occurrence; `Some(v)` is an OCC compare-and-swap update.
    pub expected_version: Option<u64>,
    pub bundle: ArtifactBundle,
    pub value: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServedDelete {
    pub idempotency_ref: OpaqueRef,
    pub occurrence_id: OccurrenceId,
    pub expected_version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyDisposition {
    Applied,
    IdempotentReplay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyOutcome {
    pub disposition: ApplyDisposition,
    pub observation_version: u64,
    pub event_sequence: u64,
}

/// Public so a delta-scoped persistence adapter (`delta_store`) can read/write
/// exactly one idempotency row instead of the whole `idempotency` map (BUG-017).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyEntry {
    pub fingerprint: [u8; 32],
    pub outcome: ApplyOutcome,
}
