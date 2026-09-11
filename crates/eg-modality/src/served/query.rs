use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use serde::{Deserialize, Serialize};

use super::{
    LifecycleState, ServedError, ServedEvent, ServedEventKind, ServedModalityRuntime,
    ServedPolicyScope, ServedRecord,
};
use crate::artifact::{ModalityKind, OccurrenceId, SegmentKind};
use crate::native::{NativeIndexKey, NativePredicate, NativePredicateError, NativeQueryStats};

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

impl<T> ServedModalityRuntime<T>
where
    T: crate::GovernedModality
        + Clone
        + PartialEq
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
    pub fn query(&self, query: &ServedQuery) -> Result<ServedPage<T>, ServedError> {
        let limit = query.limit.clamp(1, 1_000);
        let mut records = Vec::with_capacity(limit);
        for id in self.query_candidates(query) {
            let Some(record) = self.records.get(id) else {
                continue;
            };
            if !Self::query_record_matches(record, query)? {
                continue;
            }
            records.push(record.clone());
            if records.len() == limit {
                break;
            }
        }
        Ok(page(records))
    }

    /// Execute a typed native predicate through rebuilt posting lists, then apply
    /// exact predicate and policy checks to the bounded candidate set.
    pub fn query_native(
        &self,
        query: &ServedNativeQuery,
    ) -> Result<(ServedPage<T>, NativeQueryStats), ServedError> {
        let (keys, candidates) = self.native_candidates(&query.predicate)?;
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
            if !Self::native_record_matches(record, query)? {
                continue;
            }
            records.push(record.clone());
            if records.len() == limit {
                break;
            }
        }
        Ok((page(records), stats))
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

    fn query_candidates<'a>(
        &'a self,
        query: &'a ServedQuery,
    ) -> Box<dyn Iterator<Item = &'a OccurrenceId> + 'a> {
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
        }
    }

    fn query_record_matches(
        record: &ServedRecord<T>,
        query: &ServedQuery,
    ) -> Result<bool, ServedError> {
        if !record_is_visible(record, query.include_cold) {
            return Ok(false);
        }
        if !record_is_authorized(record, &query.scope)? {
            return Ok(false);
        }
        if let Some(kind) = query.segment_kind {
            if !record
                .bundle
                .segments
                .iter()
                .any(|segment| segment.kind == kind)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn native_candidates(
        &self,
        predicate: &NativePredicate,
    ) -> Result<(Vec<NativeIndexKey>, BTreeSet<OccurrenceId>), ServedError> {
        let keys = predicate.candidate_keys().map_err(|error| match error {
            NativePredicateError::Invalid => ServedError::InvalidQuery,
            NativePredicateError::TooBroad => ServedError::QueryTooBroad,
        })?;
        let mut candidates = BTreeSet::new();
        for key in &keys {
            if let Some(ids) = self.native_index.get(key) {
                candidates.extend(ids.iter().cloned());
            }
        }
        Ok((keys, candidates))
    }

    fn native_record_matches(
        record: &ServedRecord<T>,
        query: &ServedNativeQuery,
    ) -> Result<bool, ServedError> {
        if !record_is_visible(record, query.include_cold) {
            return Ok(false);
        }
        if !record_is_authorized(record, &query.scope)? {
            return Ok(false);
        }
        let value = record.value.as_ref().ok_or(ServedError::CorruptIndex)?;
        Ok(value.matches_native_predicate(&query.predicate))
    }
}

fn page<T>(records: Vec<ServedRecord<T>>) -> ServedPage<T> {
    let next = records.last().map(|record| record.occurrence_id.clone());
    ServedPage { records, next }
}

fn record_is_visible<T>(record: &ServedRecord<T>, include_cold: bool) -> bool {
    record.lifecycle != LifecycleState::Tombstoned
        && (include_cold || record.lifecycle != LifecycleState::Cold)
}

fn record_is_authorized<T>(
    record: &ServedRecord<T>,
    scope: &ServedPolicyScope,
) -> Result<bool, ServedError> {
    let occurrence = record.occurrence().ok_or(ServedError::InvalidBundle)?;
    Ok(scope.authorizes_occurrence(occurrence))
}
