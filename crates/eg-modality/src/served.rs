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
use std::ops::Bound::{Excluded, Unbounded};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::artifact::{
    ArtifactBundle, Classification, ModalityKind, Occurrence, OccurrenceId, OpaqueRef, SegmentKind,
};
use crate::native::{NativeIndexKey, NativePredicate, NativePredicateError, NativeQueryStats};
use crate::GovernedModality;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServedQuery {
    pub scope: ServedPolicyScope,
    pub modality: Option<ModalityKind>,
    pub segment_kind: Option<SegmentKind>,
    pub after: Option<OccurrenceId>,
    pub limit: usize,
    pub include_cold: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ServedNativeQuery {
    pub scope: ServedPolicyScope,
    pub predicate: NativePredicate,
    pub after: Option<OccurrenceId>,
    pub limit: usize,
    pub include_cold: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServedPage<T> {
    pub records: Vec<ServedRecord<T>>,
    pub next: Option<OccurrenceId>,
}

/// Privacy-safe live cardinalities for exact storage/index/erasure certification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServedRuntimeStats {
    pub active_records: usize,
    pub total_records: usize,
    pub tombstoned_records: usize,
    pub modality_index_postings: usize,
    pub segment_index_postings: usize,
    pub native_index_keys: usize,
    pub native_index_postings: usize,
    pub events: usize,
    pub snapshot_bytes: usize,
}

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

    /// Apply one atomic governed ingest/update. Idempotency is content-sensitive:
    /// reusing a key for different bytes fails closed.
    pub fn ingest(&mut self, command: ServedIngest<T>) -> Result<ApplyOutcome, ServedError> {
        command
            .bundle
            .validate_certified()
            .map_err(|_| ServedError::InvalidBundle)?;
        if !command.value.validate_governed_payload() {
            return Err(ServedError::UnsafePayload);
        }
        let occurrence = command
            .bundle
            .occurrences
            .iter()
            .find(|o| o.id == command.target_occurrence_id)
            .ok_or(ServedError::InvalidBundle)?;
        if occurrence.observation_version == 0
            || !bundle_matches_modality(&command.bundle, occurrence, command.value.storage_kind())
        {
            return Err(ServedError::InvalidBundle);
        }

        let fingerprint = stable_fingerprint(&command).map_err(|_| ServedError::Codec)?;
        if let Some(entry) = self.idempotency.get(&command.idempotency_ref) {
            return if entry.fingerprint == fingerprint {
                Ok(ApplyOutcome {
                    disposition: ApplyDisposition::IdempotentReplay,
                    ..entry.outcome
                })
            } else {
                Err(ServedError::IdempotencyConflict)
            };
        }

        let kind = match self.records.get(&command.target_occurrence_id) {
            None => {
                if command.expected_version.is_some() {
                    return Err(ServedError::VersionConflict);
                }
                ServedEventKind::Ingested
            }
            Some(current) => {
                if command.expected_version != Some(current.observation_version)
                    || occurrence.observation_version <= current.observation_version
                {
                    return Err(ServedError::VersionConflict);
                }
                ServedEventKind::Updated
            }
        };

        let occurrence_id = command.target_occurrence_id.clone();
        let observation_version = occurrence.observation_version;
        let tenant_ref = occurrence.policy.tenant_ref.clone();
        let access_policy_ref = occurrence.policy.access_policy_ref.clone();
        self.ensure_active_count();
        let activates_record = self
            .records
            .get(&occurrence_id)
            .is_none_or(|record| record.lifecycle == LifecycleState::Tombstoned);
        let record = ServedRecord {
            occurrence_id: occurrence_id.clone(),
            observation_version,
            lifecycle: LifecycleState::Active,
            bundle: command.bundle,
            value: Some(command.value),
        };
        self.remove_from_indexes(&occurrence_id);
        self.records.insert(occurrence_id.clone(), record);
        self.add_to_indexes(&occurrence_id);
        if activates_record {
            *self
                .active_count
                .as_mut()
                .expect("active count was initialized") += 1;
        }
        let outcome = self.push_event(
            occurrence_id,
            observation_version,
            kind,
            tenant_ref,
            access_policy_ref,
        );
        self.idempotency.insert(
            command.idempotency_ref,
            IdempotencyEntry {
                fingerprint,
                outcome,
            },
        );
        Ok(outcome)
    }

    /// Apply a sequence atomically. This is both the batch-ingest path and the
    /// bounded streaming-ingest primitive: the caller may feed any iterator, while a
    /// failure leaves the original runtime untouched. Atomicity uses a touched-row
    /// undo journal rather than cloning total runtime state, so staging/rollback is
    /// proportional to the batch and each touched bundle's index fan-out.
    pub fn ingest_stream<I>(&mut self, commands: I) -> Result<Vec<ApplyOutcome>, ServedError>
    where
        I: IntoIterator<Item = ServedIngest<T>>,
    {
        let mut outcomes = Vec::new();
        let mut undo = Vec::new();
        for command in commands {
            let occurrence_id = command.target_occurrence_id.clone();
            let idempotency_ref = command.idempotency_ref.clone();
            let checkpoint = IngestUndo {
                occurrence_id,
                previous_record: self.records.get(&command.target_occurrence_id).cloned(),
                idempotency_ref: idempotency_ref.clone(),
                previous_idempotency: self.idempotency.get(&idempotency_ref).cloned(),
                events_len: self.events.len(),
                next_sequence: self.next_sequence,
                active_count: self.active_count,
            };
            match self.ingest(command) {
                Ok(outcome) => {
                    if outcome.disposition == ApplyDisposition::Applied {
                        undo.push(checkpoint);
                    }
                    outcomes.push(outcome);
                }
                Err(error) => {
                    for entry in undo.into_iter().rev() {
                        self.rollback_ingest(entry);
                    }
                    return Err(error);
                }
            }
        }
        Ok(outcomes)
    }

    pub fn delete(
        &mut self,
        scope: &ServedPolicyScope,
        command: ServedDelete,
    ) -> Result<ApplyOutcome, ServedError> {
        let fingerprint = stable_fingerprint(&command).map_err(|_| ServedError::Codec)?;
        if let Some(entry) = self.idempotency.get(&command.idempotency_ref) {
            return if entry.fingerprint == fingerprint {
                Ok(ApplyOutcome {
                    disposition: ApplyDisposition::IdempotentReplay,
                    ..entry.outcome
                })
            } else {
                Err(ServedError::IdempotencyConflict)
            };
        }
        self.ensure_active_count();
        let record = self
            .records
            .get(&command.occurrence_id)
            .ok_or(ServedError::NotFound)?;
        let occurrence = record.occurrence().ok_or(ServedError::InvalidBundle)?;
        if !scope.authorizes_occurrence(occurrence) {
            return Err(ServedError::Forbidden);
        }
        if occurrence.policy.legal_hold_ref.is_some() {
            return Err(ServedError::LegalHold);
        }
        if command.expected_version != record.observation_version
            || record.lifecycle == LifecycleState::Tombstoned
        {
            return Err(ServedError::VersionConflict);
        }
        let tenant_ref = occurrence.policy.tenant_ref.clone();
        let access_policy_ref = occurrence.policy.access_policy_ref.clone();
        self.remove_from_indexes(&command.occurrence_id);
        let record = self
            .records
            .get_mut(&command.occurrence_id)
            .ok_or(ServedError::NotFound)?;
        record.observation_version = record.observation_version.saturating_add(1);
        record.lifecycle = LifecycleState::Tombstoned;
        record.value = None;
        let version = record.observation_version;
        *self
            .active_count
            .as_mut()
            .expect("active count was initialized") -= 1;
        let outcome = self.push_event(
            command.occurrence_id,
            version,
            ServedEventKind::Deleted,
            tenant_ref,
            access_policy_ref,
        );
        self.idempotency.insert(
            command.idempotency_ref,
            IdempotencyEntry {
                fingerprint,
                outcome,
            },
        );
        Ok(outcome)
    }

    pub fn move_to_cold(
        &mut self,
        scope: &ServedPolicyScope,
        occurrence_id: &OccurrenceId,
    ) -> Result<ApplyOutcome, ServedError> {
        self.transition_lifecycle(
            scope,
            occurrence_id,
            LifecycleState::Cold,
            ServedEventKind::MovedToCold,
        )
    }

    pub fn restore(
        &mut self,
        scope: &ServedPolicyScope,
        occurrence_id: &OccurrenceId,
    ) -> Result<ApplyOutcome, ServedError> {
        self.transition_lifecycle(
            scope,
            occurrence_id,
            LifecycleState::Active,
            ServedEventKind::Restored,
        )
    }

    pub fn query(&self, query: &ServedQuery) -> Result<ServedPage<T>, ServedError> {
        let limit = query.limit.clamp(1, 1_000);
        let mut records = Vec::with_capacity(limit);
        let mut eligible: Box<dyn Iterator<Item = &OccurrenceId> + '_> =
            match (query.modality, query.segment_kind) {
                (Some(modality), _) => match (self.modality_index.get(&modality), &query.after) {
                    (Some(ids), Some(after)) => Box::new(ids.range((Excluded(after), Unbounded))),
                    (Some(ids), None) => Box::new(ids.iter()),
                    (None, _) => Box::new(std::iter::empty()),
                },
                (None, Some(kind)) => match (self.segment_index.get(&kind), &query.after) {
                    (Some(ids), Some(after)) => Box::new(ids.range((Excluded(after), Unbounded))),
                    (Some(ids), None) => Box::new(ids.iter()),
                    (None, _) => Box::new(std::iter::empty()),
                },
                (None, None) => match &query.after {
                    Some(after) => Box::new(
                        self.records
                            .range((Excluded(after), Unbounded))
                            .map(|(id, _)| id),
                    ),
                    None => Box::new(self.records.keys()),
                },
            };
        for id in eligible.by_ref() {
            let Some(record) = self.records.get(id) else {
                continue;
            };
            if record.lifecycle == LifecycleState::Tombstoned
                || (!query.include_cold && record.lifecycle == LifecycleState::Cold)
            {
                continue;
            }
            let occurrence = record.occurrence().ok_or(ServedError::InvalidBundle)?;
            if !query.scope.authorizes_occurrence(occurrence) {
                continue;
            }
            if let Some(kind) = query.segment_kind {
                if !record
                    .bundle
                    .segments
                    .iter()
                    .any(|segment| segment.kind == kind)
                {
                    continue;
                }
            }
            records.push(record.clone());
            if records.len() == limit {
                break;
            }
        }
        let next = records.last().map(|record| record.occurrence_id.clone());
        Ok(ServedPage { records, next })
    }

    /// Execute a typed native predicate through rebuilt posting lists, then apply
    /// exact predicate and policy checks to the bounded candidate set.
    pub fn query_native(
        &self,
        query: &ServedNativeQuery,
    ) -> Result<(ServedPage<T>, NativeQueryStats), ServedError> {
        let keys = query
            .predicate
            .candidate_keys()
            .map_err(|error| match error {
                NativePredicateError::Invalid => ServedError::InvalidQuery,
                NativePredicateError::TooBroad => ServedError::QueryTooBroad,
            })?;
        let mut candidates = BTreeSet::new();
        for key in &keys {
            if let Some(ids) = self.native_index.get(key) {
                candidates.extend(ids.iter().cloned());
            }
        }
        let mut stats = NativeQueryStats {
            index_lookups: keys.len(),
            candidates: candidates.len(),
            examined: 0,
        };
        let limit = query.limit.clamp(1, 1_000);
        let eligible: Box<dyn Iterator<Item = &OccurrenceId> + '_> = match &query.after {
            Some(after) => Box::new(candidates.range((Excluded(after), Unbounded))),
            None => Box::new(candidates.iter()),
        };
        let mut records = Vec::with_capacity(limit);
        for id in eligible {
            stats.examined += 1;
            let Some(record) = self.records.get(id) else {
                return Err(ServedError::CorruptIndex);
            };
            if record.lifecycle == LifecycleState::Tombstoned
                || (!query.include_cold && record.lifecycle == LifecycleState::Cold)
            {
                continue;
            }
            let occurrence = record.occurrence().ok_or(ServedError::InvalidBundle)?;
            if !query.scope.authorizes_occurrence(occurrence) {
                continue;
            }
            let value = record.value.as_ref().ok_or(ServedError::CorruptIndex)?;
            if !value.matches_native_predicate(&query.predicate) {
                continue;
            }
            records.push(record.clone());
            if records.len() == limit {
                break;
            }
        }
        let next = records.last().map(|record| record.occurrence_id.clone());
        Ok((ServedPage { records, next }, stats))
    }

    pub fn native_index_key_count(&self) -> usize {
        self.native_index.len()
    }

    pub fn stats(&self) -> Result<ServedRuntimeStats, ServedError> {
        let snapshot_bytes = self.snapshot()?.len();
        Ok(ServedRuntimeStats {
            active_records: self.len(),
            total_records: self.records.len(),
            tombstoned_records: self
                .records
                .values()
                .filter(|record| record.lifecycle == LifecycleState::Tombstoned)
                .count(),
            modality_index_postings: self.modality_index.values().map(BTreeSet::len).sum(),
            segment_index_postings: self.segment_index.values().map(BTreeSet::len).sum(),
            native_index_keys: self.native_index.len(),
            native_index_postings: self.native_index.values().map(BTreeSet::len).sum(),
            events: self.events.len(),
            snapshot_bytes,
        })
    }

    pub fn collect_tombstones(
        &mut self,
        scope: &ServedPolicyScope,
        through_event_sequence: u64,
    ) -> usize {
        let eligible: BTreeSet<OccurrenceId> = self
            .events
            .iter()
            .take_while(|event| event.sequence <= through_event_sequence)
            .filter(|event| event.kind == ServedEventKind::Deleted)
            .map(|event| event.occurrence_id.clone())
            .collect();
        let collected: Vec<OccurrenceId> = self
            .records
            .iter()
            .filter(|(id, record)| {
                record.lifecycle == LifecycleState::Tombstoned
                    && eligible.contains(*id)
                    && record
                        .occurrence()
                        .is_some_and(|occurrence| scope.authorizes_occurrence(occurrence))
            })
            .map(|(id, _record)| id.clone())
            .collect();
        for id in &collected {
            self.remove_from_indexes(id);
            self.records.remove(id);
        }
        collected.len()
    }

    /// Monotonic CDC/replay stream. Cursors are scalar sequence numbers and contain
    /// no source or host details.
    pub fn events_after(&self, sequence: u64, limit: usize) -> Vec<ServedEvent> {
        let start = usize::try_from(sequence)
            .unwrap_or(usize::MAX)
            .min(self.events.len());
        self.events[start..]
            .iter()
            .take(limit.clamp(1, 10_000))
            .cloned()
            .collect()
    }

    /// Policy-filtered replay stream used by the live serving boundary. Runtime
    /// partitions already isolate tenant/policy, but classification and purpose
    /// remain per-occurrence and therefore must be checked for every event too.
    pub fn events_after_authorized(
        &self,
        scope: &ServedPolicyScope,
        sequence: u64,
        limit: usize,
    ) -> Vec<ServedEvent> {
        let start = usize::try_from(sequence)
            .unwrap_or(usize::MAX)
            .min(self.events.len());
        self.events[start..]
            .iter()
            .filter(|event| {
                self.records
                    .get(&event.occurrence_id)
                    .and_then(ServedRecord::occurrence)
                    .is_some_and(|occurrence| scope.authorizes_occurrence(occurrence))
            })
            .take(limit.clamp(1, 10_000))
            .cloned()
            .collect()
    }

    /// Deterministic backup/checkpoint bytes, including the idempotency ledger so an
    /// exact replay remains an exact replay across restart.
    pub fn snapshot(&self) -> Result<Vec<u8>, ServedError> {
        serde_json::to_vec(self).map_err(|_| ServedError::Codec)
    }

    pub fn recover(snapshot: &[u8]) -> Result<Self, ServedError> {
        let mut runtime: Self = serde_json::from_slice(snapshot).map_err(|_| ServedError::Codec)?;
        runtime.rebuild_indexes()?;
        runtime.validate_recovered()?;
        Ok(runtime)
    }

    /// Assemble a runtime directly from durably-stored, individually-addressable
    /// rows (BUG-017 frozen format — `delta_store`) instead of one whole-blob
    /// snapshot. Runs the exact same contiguity/idempotency/index validation
    /// [`recover`] does, so a row-backed reconstruction is held to the identical
    /// correctness bar as the legacy whole-snapshot reader.
    pub fn from_rows(
        records: BTreeMap<OccurrenceId, ServedRecord<T>>,
        events: Vec<ServedEvent>,
        next_sequence: u64,
        idempotency: BTreeMap<OpaqueRef, IdempotencyEntry>,
    ) -> Result<Self, ServedError> {
        let mut runtime = Self {
            records,
            modality_index: BTreeMap::new(),
            segment_index: BTreeMap::new(),
            native_index: BTreeMap::new(),
            events,
            next_sequence,
            idempotency,
            active_count: None,
        };
        runtime.rebuild_indexes()?;
        runtime.validate_recovered()?;
        Ok(runtime)
    }

    fn validate_recovered(&self) -> Result<(), ServedError> {
        let contiguous = self
            .events
            .iter()
            .enumerate()
            .all(|(index, event)| event.sequence == index as u64 + 1);
        let expected_next = self
            .events
            .last()
            .map_or(1, |event| event.sequence.saturating_add(1));
        let idempotency_valid = self.idempotency.values().all(|entry| {
            entry
                .outcome
                .event_sequence
                .checked_sub(1)
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| self.events.get(index))
                .is_some_and(|event| {
                    event.sequence == entry.outcome.event_sequence
                        && event.observation_version == entry.outcome.observation_version
                })
        });
        if !contiguous || !idempotency_valid || self.next_sequence != expected_next {
            return Err(ServedError::CorruptSnapshot);
        }
        Ok(())
    }

    /// One record, by occurrence id — the delta-scoped read a row-based adapter
    /// needs instead of the whole `records` map (BUG-017).
    pub fn record(&self, occurrence_id: &OccurrenceId) -> Option<&ServedRecord<T>> {
        self.records.get(occurrence_id)
    }

    /// All records, for a caller that must export the full corpus once (e.g. the
    /// snapshot-to-rows migration backfill). NOT for the per-mutation hot path.
    pub fn records(&self) -> &BTreeMap<OccurrenceId, ServedRecord<T>> {
        &self.records
    }

    /// All events, for migration export. NOT for the per-mutation hot path.
    pub fn all_events(&self) -> &[ServedEvent] {
        &self.events
    }

    /// All idempotency entries, for migration export. NOT for the per-mutation
    /// hot path.
    pub fn idempotency_entries(&self) -> &BTreeMap<OpaqueRef, IdempotencyEntry> {
        &self.idempotency
    }

    /// The event most recently appended by the mutation that just ran (`ingest`/
    /// `delete`/lifecycle transition), i.e. the ONE new event a delta write must
    /// persist.
    pub fn last_event(&self) -> Option<&ServedEvent> {
        self.events.last()
    }

    /// One idempotency entry, by key — the delta-scoped read/write a row-based
    /// adapter needs instead of the whole `idempotency` map (BUG-017).
    pub fn idempotency_entry(&self, key: &OpaqueRef) -> Option<&IdempotencyEntry> {
        self.idempotency.get(key)
    }

    pub fn next_sequence_value(&self) -> u64 {
        self.next_sequence
    }

    /// The modality/segment/native-index buckets exactly one occurrence belongs
    /// to right now — bounded by that occurrence's own artifact/rendition/
    /// segment/feature count, never by corpus size. A delta write persists index
    /// membership as edges/postings derived from this, never the whole
    /// `BTreeSet` bucket.
    pub fn index_memberships(
        &self,
        occurrence_id: &OccurrenceId,
    ) -> (BTreeSet<ModalityKind>, BTreeSet<SegmentKind>, Vec<NativeIndexKey>) {
        match self.records.get(occurrence_id) {
            Some(record) => record_index_memberships(record),
            None => (BTreeSet::new(), BTreeSet::new(), Vec::new()),
        }
    }

    /// Rebuild all native indexes from the authoritative records and validate every
    /// bundle. A recovered runtime never advertises an index before this succeeds.
    pub fn rebuild_indexes(&mut self) -> Result<(), ServedError> {
        self.modality_index.clear();
        self.segment_index.clear();
        self.native_index.clear();
        let mut active_count = 0usize;
        let ids: Vec<OccurrenceId> = self.records.keys().cloned().collect();
        for id in ids {
            let record = self.records.get(&id).ok_or(ServedError::CorruptSnapshot)?;
            record
                .bundle
                .validate_certified()
                .map_err(|_| ServedError::CorruptSnapshot)?;
            let occurrence = record.occurrence().ok_or(ServedError::CorruptSnapshot)?;
            if record.observation_version == 0 {
                return Err(ServedError::CorruptSnapshot);
            }
            match (record.lifecycle, record.value.as_ref()) {
                (LifecycleState::Tombstoned, None) => {}
                (LifecycleState::Active | LifecycleState::Cold, Some(value))
                    if value.validate_governed_payload()
                        && bundle_matches_modality(
                            &record.bundle,
                            occurrence,
                            value.storage_kind(),
                        ) => {}
                _ => return Err(ServedError::CorruptSnapshot),
            }
            if record.lifecycle != LifecycleState::Tombstoned {
                self.add_to_indexes(&id);
                active_count += 1;
            }
        }
        self.active_count = Some(active_count);
        Ok(())
    }

    fn transition_lifecycle(
        &mut self,
        scope: &ServedPolicyScope,
        occurrence_id: &OccurrenceId,
        target: LifecycleState,
        event_kind: ServedEventKind,
    ) -> Result<ApplyOutcome, ServedError> {
        let record = self
            .records
            .get_mut(occurrence_id)
            .ok_or(ServedError::NotFound)?;
        let occurrence = record.occurrence().ok_or(ServedError::InvalidBundle)?;
        if !scope.authorizes_occurrence(occurrence) {
            return Err(ServedError::Forbidden);
        }
        if record.lifecycle == LifecycleState::Tombstoned {
            return Err(ServedError::InvalidLifecycle);
        }
        let allowed = matches!(
            (record.lifecycle, target),
            (LifecycleState::Active, LifecycleState::Cold)
                | (LifecycleState::Cold, LifecycleState::Active)
        );
        if !allowed {
            return Err(ServedError::InvalidLifecycle);
        }
        let tenant_ref = occurrence.policy.tenant_ref.clone();
        let access_policy_ref = occurrence.policy.access_policy_ref.clone();
        record.lifecycle = target;
        record.observation_version = record.observation_version.saturating_add(1);
        let version = record.observation_version;
        Ok(self.push_event(
            occurrence_id.clone(),
            version,
            event_kind,
            tenant_ref,
            access_policy_ref,
        ))
    }

    fn push_event(
        &mut self,
        occurrence_id: OccurrenceId,
        observation_version: u64,
        kind: ServedEventKind,
        tenant_ref: OpaqueRef,
        access_policy_ref: OpaqueRef,
    ) -> ApplyOutcome {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push(ServedEvent {
            sequence,
            occurrence_id,
            observation_version,
            kind,
            tenant_ref,
            access_policy_ref,
        });
        ApplyOutcome {
            disposition: ApplyDisposition::Applied,
            observation_version,
            event_sequence: sequence,
        }
    }

    fn add_to_indexes(&mut self, occurrence_id: &OccurrenceId) {
        let Some(record) = self.records.get(occurrence_id) else {
            return;
        };
        if record.lifecycle == LifecycleState::Tombstoned || record.value.is_none() {
            return;
        }
        let modalities: BTreeSet<ModalityKind> = record
            .bundle
            .artifacts
            .iter()
            .map(|artifact| artifact.modality)
            .chain(
                record
                    .bundle
                    .renditions
                    .iter()
                    .map(|rendition| rendition.modality),
            )
            .collect();
        let segment_kinds: BTreeSet<SegmentKind> = record
            .bundle
            .segments
            .iter()
            .map(|segment| segment.kind)
            .collect();
        let native_keys = record
            .value
            .as_ref()
            .map(GovernedModality::native_index_keys)
            .unwrap_or_default();
        for modality in modalities {
            self.modality_index
                .entry(modality)
                .or_default()
                .insert(occurrence_id.clone());
        }
        for kind in segment_kinds {
            self.segment_index
                .entry(kind)
                .or_default()
                .insert(occurrence_id.clone());
        }
        for key in native_keys {
            self.native_index
                .entry(key)
                .or_default()
                .insert(occurrence_id.clone());
        }
    }

    fn remove_from_indexes(&mut self, occurrence_id: &OccurrenceId) {
        let Some(record) = self.records.get(occurrence_id) else {
            return;
        };
        let (modalities, segment_kinds) = index_keys(record);
        let native_keys = record
            .value
            .as_ref()
            .map(GovernedModality::native_index_keys)
            .unwrap_or_default();
        for modality in modalities {
            let remove_key = if let Some(ids) = self.modality_index.get_mut(&modality) {
                ids.remove(occurrence_id);
                ids.is_empty()
            } else {
                false
            };
            if remove_key {
                self.modality_index.remove(&modality);
            }
        }
        for kind in segment_kinds {
            let remove_key = if let Some(ids) = self.segment_index.get_mut(&kind) {
                ids.remove(occurrence_id);
                ids.is_empty()
            } else {
                false
            };
            if remove_key {
                self.segment_index.remove(&kind);
            }
        }
        for key in native_keys {
            let remove_key = if let Some(ids) = self.native_index.get_mut(&key) {
                ids.remove(occurrence_id);
                ids.is_empty()
            } else {
                false
            };
            if remove_key {
                self.native_index.remove(&key);
            }
        }
    }

    fn ensure_active_count(&mut self) {
        if self.active_count.is_none() {
            self.active_count = Some(
                self.records
                    .values()
                    .filter(|record| record.lifecycle != LifecycleState::Tombstoned)
                    .count(),
            );
        }
    }

    fn rollback_ingest(&mut self, undo: IngestUndo<T>) {
        self.remove_from_indexes(&undo.occurrence_id);
        match undo.previous_record {
            Some(record) => {
                self.records.insert(undo.occurrence_id.clone(), record);
                self.add_to_indexes(&undo.occurrence_id);
            }
            None => {
                self.records.remove(&undo.occurrence_id);
            }
        }
        match undo.previous_idempotency {
            Some(entry) => {
                self.idempotency.insert(undo.idempotency_ref, entry);
            }
            None => {
                self.idempotency.remove(&undo.idempotency_ref);
            }
        }
        self.events.truncate(undo.events_len);
        self.next_sequence = undo.next_sequence;
        self.active_count = undo.active_count;
    }
}

struct IngestUndo<T> {
    occurrence_id: OccurrenceId,
    previous_record: Option<ServedRecord<T>>,
    idempotency_ref: OpaqueRef,
    previous_idempotency: Option<IdempotencyEntry>,
    events_len: usize,
    next_sequence: u64,
    active_count: Option<usize>,
}

/// The modality/segment/native-index buckets a standalone record clone belongs
/// to (a `Tombstoned`/valueless record belongs to none). Used both by
/// [`ServedModalityRuntime::index_memberships`] (looked up live) and by
/// `delta_store` when it must diff a "before" record clone taken prior to a
/// mutation against the "after" state (BUG-017 delta-scoped index writes).
pub fn record_index_memberships<T>(
    record: &ServedRecord<T>,
) -> (BTreeSet<ModalityKind>, BTreeSet<SegmentKind>, Vec<NativeIndexKey>)
where
    T: GovernedModality,
{
    if record.lifecycle == LifecycleState::Tombstoned || record.value.is_none() {
        return (BTreeSet::new(), BTreeSet::new(), Vec::new());
    }
    let (modalities, segment_kinds) = index_keys(record);
    let native_keys = record
        .value
        .as_ref()
        .map(GovernedModality::native_index_keys)
        .unwrap_or_default();
    (modalities, segment_kinds, native_keys)
}

fn index_keys<T>(record: &ServedRecord<T>) -> (BTreeSet<ModalityKind>, BTreeSet<SegmentKind>) {
    let modalities = record
        .bundle
        .artifacts
        .iter()
        .map(|artifact| artifact.modality)
        .chain(
            record
                .bundle
                .renditions
                .iter()
                .map(|rendition| rendition.modality),
        )
        .collect();
    let segment_kinds = record
        .bundle
        .segments
        .iter()
        .map(|segment| segment.kind)
        .collect();
    (modalities, segment_kinds)
}

fn bundle_matches_modality(
    bundle: &ArtifactBundle,
    occurrence: &Occurrence,
    storage_kind: &str,
) -> bool {
    let expected = match storage_kind {
        "document" => ModalityKind::Document,
        "image" => ModalityKind::Image,
        "audio" => ModalityKind::Audio,
        "video" => ModalityKind::Video,
        _ => return false,
    };
    bundle
        .artifacts
        .iter()
        .any(|artifact| artifact.id == occurrence.artifact_id && artifact.modality == expected)
}

fn stable_fingerprint<T: Serialize>(value: &T) -> Result<[u8; 32], ()> {
    let bytes = serde_json::to_vec(value).map_err(|_| ())?;
    Ok(Sha256::digest(bytes).into())
}

/// Served errors never include rejected data, ids, paths, endpoints, or payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServedError {
    InvalidBundle,
    UnsafePayload,
    Codec,
    IdempotencyConflict,
    VersionConflict,
    Forbidden,
    LegalHold,
    NotFound,
    InvalidLifecycle,
    CorruptSnapshot,
    InvalidQuery,
    QueryTooBroad,
    CorruptIndex,
}

impl fmt::Display for ServedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidBundle => "invalid governed modality bundle",
            Self::UnsafePayload => "unsafe governed modality payload",
            Self::Codec => "modality codec failure",
            Self::IdempotencyConflict => "idempotency conflict",
            Self::VersionConflict => "observation version conflict",
            Self::Forbidden => "modality operation forbidden",
            Self::LegalHold => "modality occurrence is under legal hold",
            Self::NotFound => "modality occurrence not found",
            Self::InvalidLifecycle => "invalid modality lifecycle transition",
            Self::CorruptSnapshot => "corrupt modality snapshot",
            Self::InvalidQuery => "invalid native modality query",
            Self::QueryTooBroad => "native modality query exceeds resource limits",
            Self::CorruptIndex => "corrupt modality index",
        })
    }
}

impl std::error::Error for ServedError {}
