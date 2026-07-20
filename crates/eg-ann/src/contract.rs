//! `ModalityContract` retrofit for [`FlatIndex`] (CONCEPT:E4) — the lowest-friction
//! step of the "rest of the retrofit" order in `eg-modality`'s README (a pure-serde
//! leaf, similar shape to `eg-tensor`).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceAddress, IngestReport,
    ModalityContract, ModalitySelfTest, Provenance, RowSetShape, StagedWrite, StorageStats,
    TckPoint,
};

use crate::flat::FlatIndex;

impl ModalityContract for FlatIndex {
    fn storage_kind(&self) -> &'static str {
        "ann"
    }

    /// A whole `FlatIndex` (many vectors, no query of its own) has no single
    /// query-relative score — exactly like `eg-geo::Geometry`, this is unranked,
    /// awaiting a downstream RANK op (`FlatIndex::search`/`rerank` against a
    /// caller-supplied query vector) rather than an intrinsic property of the
    /// stored index itself.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A raw vector index is not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// A raw vector index has no derivation history of its own — default `None` is
    /// correct as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// No located-evidence concept applies to a bare vector index — default `None`.
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        None
    }

    /// The crate's real methods on `FlatIndex`, listed exactly like
    /// `eg-tensor`/`eg-geo` list their real ops.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["search", "rerank", "refine_ann", "add"]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over the vector index's
    // txn staging path. FlatIndex derives Serde so serialization works; there is no
    // specialized durable codec beyond serde (unlike eg-tensor's blob or eg-geo's WKB). ──

    /// Batch ingest = parse a `FlatIndex` back from its serialized form via txn
    /// staging. Streaming is genuinely N/A: a vector index is a whole structure,
    /// not an append stream.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<FlatIndex>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a vector index is a whole structure, not an append stream",
            ),
        }
    }

    /// Real storage stats from the serialized index: logical size from the encoded
    /// length; element count is the number of vectors in the index. A flat index
    /// does NOT have a secondary index (it is a brute-force scan store), so
    /// `has_secondary_index` is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        let element_count = self.len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count,
            has_secondary_index: false,
        })
    }

    /// N/A: FlatIndex has no specialized durable codec beyond serde (unlike
    /// eg-tensor's to_blob/from_blob or eg-geo's WKB). Recovery testing is covered
    /// by recovery_selfcheck; backup-to-durable-storage is a store-layer concern.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "no specialized durable codec — serde is transient; durability is a store-layer concern",
        )
    }

    /// Simulated single-node crash-and-recover through the txn staging path.
    /// Stage the index as an in-txn write; the staged payload IS the WAL record;
    /// on "restart" replay-decode it and confirm the recovered index is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<FlatIndex>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// A vector index has no CDC, policy, or provenance of its own (parallel to
    /// `eg-tensor`): CDC is a store-layer concern for immutable values; policy is
    /// enforced at the graph-node layer; provenance/evidence are recorded by the
    /// producing operator, not embedded in the index bytes.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "a vector index is an immutable structure — change-capture/delete/GC is a store-layer concern, not a modality-value capability",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the index",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "a vector index has no derivation history and no located-evidence artifact; lineage, where it exists, is recorded by the producing operator",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for FlatIndex {
    fn conformance_sample() -> Self {
        let mut idx = FlatIndex::new(2);
        idx.add(&[
            (10, vec![0.0, 0.0]),
            (20, vec![1.0, 0.0]),
            (30, vec![0.0, 2.0]),
        ]);
        idx
    }
}

eg_modality::modality_conformance_tests!(FlatIndex);
