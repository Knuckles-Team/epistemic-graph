//! `ModalityContract` retrofit for [`NodeChange`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale.
//!
//! [`NodeChange`] is the engine's own change-coalescing CDC-shaped record ("One
//! node touched by a committed write batch" — see the doc comment on
//! [`crate::index::NodeChange`]), so declaring a real `cdc_topic()` here is honest,
//! not invented.

use eg_modality::{encode_staged, ConformanceTestable, ModalityContract, RowSetShape, StagedWrite};

use crate::index::NodeChange;

impl ModalityContract for NodeChange {
    fn storage_kind(&self) -> &'static str {
        "core"
    }

    /// A change record has no ranking score of its own.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// `NodeChange` IS literally the engine's own change-coalescing CDC-shaped
    /// record (see the type's module doc: "One node touched by a committed write
    /// batch") — a real, natural CDC topic, not an invented one.
    fn cdc_topic(&self) -> Option<&'static str> {
        Some("core.node.change")
    }

    // provenance/evidence/policy_labels/analytics_ops: left at the trait default —
    // a bare change record carries none of those; it doesn't even know if it was
    // an add or an update, so no policy label can be honestly derived from it
    // alone.
}

impl ConformanceTestable for NodeChange {
    fn conformance_sample() -> Self {
        NodeChange::with_properties("node-1".to_string(), vec![1, 2, 3])
    }
}

eg_modality::modality_conformance_tests!(NodeChange);
