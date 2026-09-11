use std::collections::BTreeMap;

use super::{
    IdempotencyEntry, OccurrenceId, ServedError, ServedEvent, ServedModalityRuntime, ServedRecord,
};
use crate::artifact::OpaqueRef;

impl<T> ServedModalityRuntime<T>
where
    T: crate::GovernedModality
        + Clone
        + PartialEq
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
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
}
