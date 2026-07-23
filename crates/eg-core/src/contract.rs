//! `ModalityContract` retrofit for [`NodeChange`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale.
//!
//! [`NodeChange`] is the engine's own change-coalescing CDC-shaped record ("One
//! node touched by a committed write batch" — see the doc comment on
//! [`crate::index::NodeChange`]), so declaring a real `cdc_topic()` here is honest,
//! not invented.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, IngestReport, ModalityContract,
    ModalitySelfTest, RowSetShape, StagedWrite, StorageStats, TckPoint,
};

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

    // ── EG-P1-1 hooks — minimal TCK implementations. ──

    /// Batch ingest = round-trip through serialization.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<NodeChange>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable("a node change record is not a stream"),
        }
    }

    /// Real storage stats.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        Some(StorageStats {
            logical_bytes: encode_staged(self).len() as u64,
            element_count: 1,
            has_secondary_index: false,
        })
    }

    /// Simulated crash-and-recover.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<NodeChange>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// N/A: change records are CDC events, not durable values.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "a node change is a CDC event coalescing record; backup/restore is not applicable",
        )
    }

    /// No policy.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::TenantRowRegionPolicy => Some("policy is on the node itself, not the change"),
            _ => None,
        }
    }
}

impl ConformanceTestable for NodeChange {
    fn conformance_sample() -> Self {
        NodeChange::with_properties("node-1".to_string(), vec![1, 2, 3])
    }
}

eg_modality::modality_conformance_tests!(NodeChange);
