use std::collections::{BTreeMap, BTreeSet};

use super::{LifecycleState, NativeIndexKey, ServedError, ServedModalityRuntime, ServedRecord};
use crate::artifact::{ArtifactBundle, ModalityKind, Occurrence, OccurrenceId, SegmentKind};
use crate::GovernedModality;

impl<T> ServedModalityRuntime<T>
where
    T: GovernedModality
        + Clone
        + PartialEq
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
    pub fn index_memberships(
        &self,
        occurrence_id: &OccurrenceId,
    ) -> (
        BTreeSet<ModalityKind>,
        BTreeSet<SegmentKind>,
        Vec<NativeIndexKey>,
    ) {
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

    pub(super) fn add_to_indexes(&mut self, occurrence_id: &OccurrenceId) {
        let Some(record) = self.records.get(occurrence_id) else {
            return;
        };
        if record.lifecycle == LifecycleState::Tombstoned || record.value.is_none() {
            return;
        }
        let (modalities, segment_kinds) = index_keys(record);
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

    pub(super) fn remove_from_indexes(&mut self, occurrence_id: &OccurrenceId) {
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
            remove_index_entry(&mut self.modality_index, &modality, occurrence_id);
        }
        for kind in segment_kinds {
            remove_index_entry(&mut self.segment_index, &kind, occurrence_id);
        }
        for key in native_keys {
            remove_index_entry(&mut self.native_index, &key, occurrence_id);
        }
    }

    pub(super) fn ensure_active_count(&mut self) {
        if self.active_count.is_none() {
            self.active_count = Some(
                self.records
                    .values()
                    .filter(|record| record.lifecycle != LifecycleState::Tombstoned)
                    .count(),
            );
        }
    }
}

fn remove_index_entry<K: Ord>(
    index: &mut BTreeMap<K, BTreeSet<OccurrenceId>>,
    key: &K,
    occurrence_id: &OccurrenceId,
) {
    let remove_key = match index.get_mut(key) {
        Some(ids) => {
            ids.remove(occurrence_id);
            ids.is_empty()
        }
        None => false,
    };
    if remove_key {
        index.remove(key);
    }
}

/// The modality/segment/native-index buckets a standalone record clone belongs
/// to (a `Tombstoned`/valueless record belongs to none). Used both by
/// [`ServedModalityRuntime::index_memberships`] (looked up live) and by
/// `delta_store` when it must diff a "before" record clone taken prior to a
/// mutation against the "after" state (BUG-017 delta-scoped index writes).
pub fn record_index_memberships<T>(
    record: &ServedRecord<T>,
) -> (
    BTreeSet<ModalityKind>,
    BTreeSet<SegmentKind>,
    Vec<NativeIndexKey>,
)
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

pub(super) fn bundle_matches_modality(
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
