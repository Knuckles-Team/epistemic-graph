//! Capability DTOs for the four `ModalityContract` hooks added in EG-P1-1 to close
//! the TCK's previously-structural gaps (ingest/streaming, storage+index+stats,
//! backup/restore/migrate/recover, single-node failure/recovery).
//!
//! Before this increment those four TCK points had NO corresponding trait method, so
//! the TCK could only ever report `NotImplemented("no hook exists yet")` for EVERY
//! modality — the point was structurally unmeasurable. These types + the four new
//! default methods on `ModalityContract` (see `contract.rs`) give each point a real
//! hook. A modality that does not override a hook still reports `NotImplemented` for
//! ITSELF (the default returns "unsupported"), but the POINT is now genuinely
//! measurable — a modality CAN implement it and reach `Pass` (see `eg-tensor`/`eg-geo`
//! for two that do, and `eg-modality`'s own `SmokeValue` self-test for an all-green
//! in-crate proof).
//!
//! ## Honest three-way outcome
//!
//! [`ModalitySelfTest`] is the shape a hook returns when it actually EXERCISES a real
//! operation (a codec round-trip, a crash-recovery replay). It has four states, all
//! honest: `Passed` (a real self-check ran and succeeded), `Failed` (a real
//! self-check ran and FAILED — honest red, never hidden), `Unsupported` (the default:
//! this modality has not wired this capability), and `NotApplicable` (the capability
//! genuinely does not apply to this modality's nature, WITH a reason — a distinct,
//! honest status from "not implemented yet", per the EG-P1-1 directive).

use serde::{Deserialize, Serialize};

/// The honest outcome of a hook that runs a real self-check. See module docs — there
/// is deliberately no "assumed pass" state; `Passed` is only ever returned by an impl
/// that actually performed the operation and observed it succeed.
///
/// Not `Serialize`/`Deserialize`: the `NotApplicable(&'static str)` reason is a
/// compile-time string literal (borrowed-`'static`, not deserializable from arbitrary
/// input), and these are runtime capability results, never persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModalitySelfTest {
    /// A real self-check ran and succeeded (e.g. a durable-codec round-trip whose
    /// output equalled the input).
    Passed,
    /// A real self-check ran and FAILED — surfaced honestly, never swallowed.
    Failed,
    /// The default: this modality has not wired this capability at all.
    Unsupported,
    /// The capability genuinely does not apply to this modality's nature, with a
    /// concrete reason (distinct from `Unsupported`/"not yet").
    NotApplicable(&'static str),
}

impl ModalitySelfTest {
    /// `true` only for `Passed`.
    pub fn is_passed(&self) -> bool {
        matches!(self, ModalitySelfTest::Passed)
    }
}

/// A modality's ingest capability report: batch ingest (the base requirement) plus
/// streaming ingest "where applicable" (a modality that is a whole-value literal, not
/// a stream, legitimately reports `NotApplicable` for streaming — see `eg-tensor`/
/// `eg-geo`). The TCK evaluates both fields: production certification requires both
/// to pass; a documented streaming N/A can only produce first-class/non-production
/// coverage. Not serde (see [`ModalitySelfTest`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestReport {
    pub batch: ModalitySelfTest,
    pub streaming: ModalitySelfTest,
}

impl IngestReport {
    /// The default every non-overriding modality gets: neither batch nor streaming
    /// ingest wired.
    pub fn unsupported() -> Self {
        Self {
            batch: ModalitySelfTest::Unsupported,
            streaming: ModalitySelfTest::Unsupported,
        }
    }

    /// A batch-only ingest report (batch `Passed`, streaming `NotApplicable` with the
    /// caller's reason) — the common case for a whole-value modality (tensor/geo).
    pub fn batch_only(streaming_na_reason: &'static str) -> Self {
        Self {
            batch: ModalitySelfTest::Passed,
            streaming: ModalitySelfTest::NotApplicable(streaming_na_reason),
        }
    }
}

impl Default for IngestReport {
    fn default() -> Self {
        Self::unsupported()
    }
}

/// Storage + secondary-index + stats presence for one modality value. Reporting a
/// `StorageStats` at all is what the TCK's storage point checks — a modality that can
/// state its own logical size / element count / whether it participates in a secondary
/// index has a real storage story to attest to. `has_secondary_index` is a genuine
/// sub-capability: `eg-geo` sets it `true` (it maintains a Hilbert/STR R-tree over
/// geometry bboxes), `eg-tensor` sets it `false` (a dense array is not secondary-
/// indexed) — both still report stats, both still `Pass` the point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageStats {
    /// Logical on-the-wire/on-disk size of this value in bytes (e.g. its durable
    /// codec length).
    pub logical_bytes: u64,
    /// Number of stored elements this value comprises (a tensor's element count; `1`
    /// for a modality whose storage unit is the whole value, e.g. one indexed
    /// geometry).
    pub element_count: u64,
    /// Whether this modality participates in a secondary index (an R-tree, an ANN
    /// index, …) beyond primary-key/id lookup.
    pub has_secondary_index: bool,
}

/// Executed native-runtime evidence required in addition to the generic 12-point
/// contract before a leaf can be advertised by the production server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeProductionProbe {
    pub codec: bool,
    pub normalized_payload: bool,
    pub secondary_index: bool,
    pub typed_query: bool,
    pub malformed_and_resource_bounds: bool,
}

impl NativeProductionProbe {
    pub fn passed(self) -> bool {
        self.codec
            && self.normalized_payload
            && self.secondary_index
            && self.typed_query
            && self.malformed_and_resource_bounds
    }
}
