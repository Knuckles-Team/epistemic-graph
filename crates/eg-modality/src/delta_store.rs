//! BUG-017 frozen row/chunk format — delta capture and row-backed
//! reconstruction.
//!
//! `served.rs`'s `ServedModalityRuntime` already does O(log corpus) per-key
//! work for every mutation (`records`/`idempotency` are `BTreeMap`s keyed by
//! occurrence id / idempotency ref; `add_to_indexes`/`remove_from_indexes`
//! touch only the ONE occurrence's own modality/segment/native-key set). The
//! measured defect (`served_runtime.rs::bug_017_write_bytes_scale_with_corpus_not_delta`,
//! 16,426x amplification at corpus=16384) is entirely in the PERSISTENCE
//! boundary: `src/server/handlers/modality.rs::store_runtime` calls
//! `runtime.snapshot()` — a `serde_json` encode of the WHOLE struct — after
//! every single-occurrence mutation.
//!
//! This module is the storage-adapter-neutral half of the fix: given the
//! runtime immediately after one mutation (still fully in RAM, unchanged),
//! [`MutationDelta::capture`] extracts ONLY what that mutation touched — the
//! one record, the one newly appended event, the one idempotency entry, and
//! the bounded index-membership delta (diffed against a "before" record
//! clone the caller took prior to calling `ingest`/`delete`/lifecycle
//! transition) — so a persistence adapter (`src/server/handlers/modality.rs`)
//! can write bounded bytes per mutation instead of `runtime.snapshot()`.
//!
//! [`ServedModalityRuntime::from_rows`] is the read-side counterpart: it
//! reconstructs a runtime from durably-stored rows (whatever key/value store
//! an adapter used to persist each `MutationDelta`) and enforces the exact
//! same contiguity/idempotency/index validation `recover()` does — a
//! row-backed reconstruction is held to the identical correctness bar as the
//! legacy whole-snapshot reader. [`export_all`] and [`verify_round_trip`]
//! are the migration/shadow-write tools: export every row a full snapshot
//! implies, and prove a `from_rows` reconstruction from those rows is
//! byte-for-byte the original runtime — the mandatory proof before an
//! existing whole-snapshot partition may cut over.
//!
//! Nothing here depends on GOC-03's cross-domain commit descriptor. Every row
//! commits through the SAME durable path (the caller's existing
//! `GraphCore::add_node`/`add_edge`, already commit-before-ack per
//! `AGENTS.md`) every other graph write already uses today; `next_sequence`
//! plays the role of a local per-partition monotonic counter, not a
//! cross-domain authority sequence. See `rows.rs`'s module doc for why the
//! FULL GOC-04 lane (barrier-gated reads, commit-descriptor-authenticated
//! chunk manifests) is a separate, larger, and genuinely GOC-03-dependent
//! scope this module does not attempt.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{de::DeserializeOwned, Serialize};

use crate::artifact::{ModalityKind, OccurrenceId, OpaqueRef, SegmentKind};
use crate::native::NativeIndexKey;
use crate::served::{
    record_index_memberships, IdempotencyEntry, ServedError, ServedEvent, ServedModalityRuntime,
    ServedRecord,
};
use crate::GovernedModality;

/// The modality/segment/native-index buckets one occurrence belongs to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexMembership {
    pub modality: BTreeSet<ModalityKind>,
    pub segment: BTreeSet<SegmentKind>,
    pub native: Vec<NativeIndexKey>,
}

/// Everything ONE mutation (`ingest`/`delete`/`move_to_cold`/`restore`)
/// changed, and nothing else. A persistence adapter writes exactly these
/// pieces — never `runtime.snapshot()` — to fix BUG-017.
#[derive(Clone, Debug)]
pub struct MutationDelta<T> {
    pub occurrence_id: OccurrenceId,
    /// The occurrence's record AFTER the mutation. Always `Some` — even a
    /// governed delete leaves a `Tombstoned` record (erased `value`, per
    /// `served.rs`'s documented erasure contract), it never removes the row.
    pub record: ServedRecord<T>,
    /// The ONE event this mutation appended.
    pub event: ServedEvent,
    /// `Some` for an idempotency-keyed mutation (`ingest`/`delete`, which
    /// carry an `idempotency_ref` on the wire and record a replay-guard
    /// entry). `None` for `move_to_cold`/`restore` — `ServedModalityOp::
    /// MoveToCold`/`Restore` carry no `idempotency_ref` at all
    /// (`src/server/handlers/modality.rs`'s `lifecycle<T>` takes no such
    /// parameter), so there is no entry to capture. `store_delta` skips
    /// writing an "idem" row when this is `None`; `from_rows`' reconstruction
    /// never required one per mutation — it derives the idempotency map from
    /// whatever rows exist, so omitting it here is a strict subset, not a
    /// format change.
    pub idempotency: Option<(OpaqueRef, IdempotencyEntry)>,
    /// `runtime.next_sequence_value()` immediately after the mutation — the
    /// tiny per-partition counter a delta write persists alongside the row
    /// (bounded, O(1) bytes; see `served.rs` module docs on why this is NOT
    /// GOC-03's commit sequence).
    pub next_sequence: u64,
    pub index_adds: IndexMembership,
    pub index_removes: IndexMembership,
}

fn diff_indexes<T>(
    before: Option<&ServedRecord<T>>,
    record: &ServedRecord<T>,
) -> (IndexMembership, IndexMembership)
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let (after_modality, after_segment, after_native) = record_index_memberships(record);
    let (before_modality, before_segment, before_native) = match before {
        Some(record) => record_index_memberships(record),
        None => (BTreeSet::new(), BTreeSet::new(), Vec::new()),
    };
    let before_native_set: BTreeSet<NativeIndexKey> = before_native.into_iter().collect();
    let after_native_set: BTreeSet<NativeIndexKey> = after_native.into_iter().collect();
    (
        IndexMembership {
            modality: after_modality
                .difference(&before_modality)
                .cloned()
                .collect(),
            segment: after_segment.difference(&before_segment).cloned().collect(),
            native: after_native_set
                .difference(&before_native_set)
                .cloned()
                .collect(),
        },
        IndexMembership {
            modality: before_modality
                .difference(&after_modality)
                .cloned()
                .collect(),
            segment: before_segment.difference(&after_segment).cloned().collect(),
            native: before_native_set
                .difference(&after_native_set)
                .cloned()
                .collect(),
        },
    )
}

impl<T> MutationDelta<T>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    /// Capture the delta for one idempotency-keyed mutation (`ingest`/
    /// `delete`). `before` is a clone of the occurrence's record taken by the
    /// caller BEFORE calling `ingest`/`delete` (`None` for a fresh ingest
    /// with no prior occurrence) — needed only to diff index membership,
    /// since `remove_from_indexes`/`add_to_indexes` mutate the live
    /// `BTreeSet` buckets in place and leave no trace of what was removed.
    ///
    /// Returns `None` if the mutation was an idempotent replay (disposition
    /// `IdempotentReplay`) — a replay touches no new durable state, so there
    /// is nothing to persist beyond the outcome already durable from the
    /// first application.
    pub fn capture(
        runtime: &ServedModalityRuntime<T>,
        before: Option<&ServedRecord<T>>,
        occurrence_id: &OccurrenceId,
        idempotency_key: &OpaqueRef,
        is_idempotent_replay: bool,
    ) -> Option<Self> {
        if is_idempotent_replay {
            return None;
        }
        let record = runtime.record(occurrence_id)?.clone();
        let event = runtime.last_event()?.clone();
        let idempotency_entry = runtime.idempotency_entry(idempotency_key)?.clone();
        let (index_adds, index_removes) = diff_indexes(before, &record);
        Some(Self {
            occurrence_id: occurrence_id.clone(),
            record,
            event,
            idempotency: Some((idempotency_key.clone(), idempotency_entry)),
            next_sequence: runtime.next_sequence_value(),
            index_adds,
            index_removes,
        })
    }

    /// Capture the delta for one `move_to_cold`/`restore` lifecycle
    /// transition (BUG-017 coverage gap: `transition_lifecycle` carries no
    /// `idempotency_ref` on the wire — `ServedModalityOp::MoveToCold`/
    /// `Restore` have no such field — so [`capture`] cannot be used; it
    /// requires a `runtime.idempotency_entry` lookup that would always miss).
    /// Before this constructor existed, a lifecycle transition was persisted
    /// ONLY via the legacy whole-snapshot `store_runtime` — the shadow row
    /// store silently fell out of sync with production state on every
    /// `move_to_cold`/`restore`, which would have failed any future
    /// digest-compare cutover check the first time a partition saw one.
    ///
    /// Returns `None` only if `runtime.record`/`runtime.last_event` are
    /// unexpectedly absent immediately after a transition that itself
    /// succeeded — defensive, should not occur in practice.
    pub fn capture_lifecycle(
        runtime: &ServedModalityRuntime<T>,
        before: Option<&ServedRecord<T>>,
        occurrence_id: &OccurrenceId,
    ) -> Option<Self> {
        let record = runtime.record(occurrence_id)?.clone();
        let event = runtime.last_event()?.clone();
        let (index_adds, index_removes) = diff_indexes(before, &record);
        Some(Self {
            occurrence_id: occurrence_id.clone(),
            record,
            event,
            idempotency: None,
            next_sequence: runtime.next_sequence_value(),
            index_adds,
            index_removes,
        })
    }
}

/// Every row a full snapshot implies, for the one-time snapshot-to-rows
/// migration backfill and for shadow-write digest comparison. NOT for the
/// per-mutation hot path (this is O(corpus), same cost class as
/// `snapshot()`/`recover()` — it exists to run ONCE per partition during
/// migration).
pub struct ExportedRows<T> {
    pub records: BTreeMap<OccurrenceId, ServedRecord<T>>,
    pub events: Vec<ServedEvent>,
    pub next_sequence: u64,
    pub idempotency: BTreeMap<OpaqueRef, IdempotencyEntry>,
}

pub fn export_all<T>(runtime: &ServedModalityRuntime<T>) -> ExportedRows<T>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    ExportedRows {
        records: runtime.records().clone(),
        events: runtime.all_events().to_vec(),
        next_sequence: runtime.next_sequence_value(),
        idempotency: runtime.idempotency_entries().clone(),
    }
}

/// The migration proof: reconstruct a runtime purely from `rows` (as a
/// row-backed adapter would read them back from durable storage) and assert
/// it is identical to `original` (the runtime `recover()`ed from the legacy
/// whole snapshot). Returns the reconstructed runtime on success so a caller
/// can also digest-compare the two `snapshot()` byte strings for a
/// belt-and-suspenders equality check before flipping the per-partition
/// cutover.
pub fn verify_round_trip<T>(
    original: &ServedModalityRuntime<T>,
    rows: ExportedRows<T>,
) -> Result<ServedModalityRuntime<T>, ServedError>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let reconstructed = ServedModalityRuntime::from_rows(
        rows.records,
        rows.events,
        rows.next_sequence,
        rows.idempotency,
    )?;
    if &reconstructed != original {
        return Err(ServedError::CorruptSnapshot);
    }
    Ok(reconstructed)
}
