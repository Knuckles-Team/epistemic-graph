//! `ModalityContract` retrofit for [`KMeansResult`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale. `contract`
//! ALSO turns on `ndarray/serde` (see `Cargo.toml`), which is what lets
//! `KMeansResult`'s `Array2<f64>` field derive `Serialize`/`Deserialize` (see
//! `src/cluster.rs`'s `cfg_attr`) — a plain, non-`contract` build never needs
//! `Array2<f64>: Serialize` at all.

use eg_modality::{encode_staged, ConformanceTestable, ModalityContract, RowSetShape, StagedWrite};

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
