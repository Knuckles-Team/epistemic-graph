//! `ModalityContract` retrofit for [`KMeansResult`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale. `contract`
//! ALSO turns on `ndarray/serde` (see `Cargo.toml`), which is what lets
//! `KMeansResult`'s `Array2<f64>` field derive `Serialize`/`Deserialize` (see
//! `src/cluster.rs`'s `cfg_attr`) — a plain, non-`contract` build never needs
//! `Array2<f64>: Serialize` at all.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, IngestReport, ModalityContract,
    ModalitySelfTest, RowSetShape, StagedWrite, StorageStats, TckPoint,
};

use crate::cluster::KMeansResult;

impl ModalityContract for KMeansResult {
    fn storage_kind(&self) -> &'static str {
        "numeric"
    }

    /// The fit's own inertia (within-cluster sum of squared distances) is a real,
    /// natural scalar signal — mirrors `eg-tensor::Tensor`'s L2-norm score.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::scored(id, self.inertia as f32)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A KMeans fit result is not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// The crate's real clustering entry point that produces a `KMeansResult`.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["kmeans"]
    }

    // provenance/evidence/policy_labels: left at the trait default — a KMeans fit
    // result has no derivation history, no located evidence, and no policy tags of
    // its own.

    // ── EG-P1-1 hooks — real, minimal implementations over KMeansResult's
    // serialization and txn staging. KMeansResult is a computed fit output. ──

    /// Batch ingest = parse a `KMeansResult` back from its serialized form. Streaming
    /// is N/A: a clustering result is a static snapshot, not an append stream.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<KMeansResult>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a clustering result is a static snapshot, not an append stream",
            ),
        }
    }

    /// Real storage stats from the serialized KMeansResult: logical size from
    /// encoded length; element count is the number of clusters (centroids). Clustering
    /// results do NOT have a secondary index, so `has_secondary_index` is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        let element_count = self.centroids.nrows() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count,
            has_secondary_index: false,
        })
    }

    /// N/A: KMeansResult is a computed clustering output, not a durable persisted
    /// value. Backup/restore/migrate is a workflow-layer concern, not a modality capability.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "a KMeans result is a computed clustering output; backup/restore/migrate is a workflow-layer concern, not a modality-value capability",
        )
    }

    /// Simulated single-node crash-and-recover through the txn staging path.
    /// Stage the result as an in-txn write; the staged payload IS the WAL record;
    /// on "restart" replay-decode it and confirm the recovered result is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<KMeansResult>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Clustering results have no CDC, policy, or provenance of their own (parallel
    /// to other computed outputs): CDC would require materialized cluster assignments;
    /// policy is at the graph-node layer; provenance is implicit in the fit parameters.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "a KMeans result is a computed output; CDC would require materializing cluster assignments as persistent nodes/edges",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the result",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "provenance is implicit in the fit parameters (data, k, seed); a KMeans result itself has no derivation history to store",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for KMeansResult {
    fn conformance_sample() -> Self {
        KMeansResult {
            labels: vec![0, 1, 0],
            centroids: ndarray::arr2(&[[0.0, 0.0], [1.0, 1.0]]),
            inertia: 0.5,
            n_iter: 3,
        }
    }
}

eg_modality::modality_conformance_tests!(KMeansResult);
