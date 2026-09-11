//! Dependency-light governed serving runtime shared by first-class modalities.
//!
//! The runtime is deliberately storage-adapter neutral: it provides the complete
//! ingest/update/delete/replay/query/paging/lifecycle/restart semantics in memory,
//! while [`snapshot`](ServedModalityRuntime::snapshot) is the deterministic payload
//! an engine adapter commits to its authoritative transaction. No source location,
//! endpoint, user name, or raw blob is accepted; those can only be represented by
//! opaque references inside [`ArtifactBundle`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::artifact::{ModalityKind, OccurrenceId, OpaqueRef, SegmentKind};
use crate::native::NativeIndexKey;
use crate::GovernedModality;

mod error;
mod indexes;
mod mutation;
mod protocol;
mod query;
mod records;
mod recovery;

struct IngestUndo<T> {
    occurrence_id: OccurrenceId,
    previous_record: Option<ServedRecord<T>>,
    idempotency_ref: OpaqueRef,
    previous_idempotency: Option<IdempotencyEntry>,
    events_len: usize,
    next_sequence: u64,
    active_count: Option<usize>,
}

pub use error::ServedError;
pub use indexes::record_index_memberships;
pub use protocol::{ApplyDisposition, ApplyOutcome, IdempotencyEntry, ServedDelete, ServedIngest};
pub use query::{ServedNativeQuery, ServedPage, ServedQuery, ServedRuntimeStats};
pub use records::{LifecycleState, ServedEvent, ServedEventKind, ServedPolicyScope, ServedRecord};

/// A complete served modality state machine. Every mutation emits a monotonic event
/// only after the new authoritative state is installed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServedModalityRuntime<T> {
    records: BTreeMap<OccurrenceId, ServedRecord<T>>,
    modality_index: BTreeMap<ModalityKind, BTreeSet<OccurrenceId>>,
    segment_index: BTreeMap<SegmentKind, BTreeSet<OccurrenceId>>,
    /// Derived from normalized values during ingest/recovery. It is deliberately
    /// omitted from the snapshot so recovery must prove that it can rebuild a
    /// complete native index before serving a query.
    #[serde(skip)]
    native_index: BTreeMap<NativeIndexKey, BTreeSet<OccurrenceId>>,
    events: Vec<ServedEvent>,
    next_sequence: u64,
    idempotency: BTreeMap<OpaqueRef, IdempotencyEntry>,
    /// Derived cache only and intentionally absent from authoritative snapshots.
    /// Official recovery rebuilds it; direct deserialization computes an exact count
    /// until the next mutation materializes the cache.
    #[serde(skip)]
    active_count: Option<usize>,
}

impl<T: PartialEq> PartialEq for ServedModalityRuntime<T> {
    fn eq(&self, other: &Self) -> bool {
        self.records == other.records
            && self.modality_index == other.modality_index
            && self.segment_index == other.segment_index
            && self.events == other.events
            && self.next_sequence == other.next_sequence
            && self.idempotency == other.idempotency
    }
}

impl<T> Default for ServedModalityRuntime<T> {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
            modality_index: BTreeMap::new(),
            segment_index: BTreeMap::new(),
            native_index: BTreeMap::new(),
            events: Vec::new(),
            next_sequence: 1,
            idempotency: BTreeMap::new(),
            active_count: Some(0),
        }
    }
}

impl<T> ServedModalityRuntime<T>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.active_count.unwrap_or_else(|| {
            self.records
                .values()
                .filter(|record| record.lifecycle != LifecycleState::Tombstoned)
                .count()
        })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn native_index_key_count(&self) -> usize {
        self.native_index.len()
    }

    pub fn next_sequence_value(&self) -> u64 {
        self.next_sequence
    }
}
