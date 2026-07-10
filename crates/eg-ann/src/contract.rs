//! `ModalityContract` retrofit for [`FlatIndex`] (CONCEPT:E4) — the lowest-friction
//! step of the "rest of the retrofit" order in `eg-modality`'s README (a pure-serde
//! leaf, similar shape to `eg-tensor`).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale.

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
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
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    /// The crate's real methods on `FlatIndex`, listed exactly like
    /// `eg-tensor`/`eg-geo` list their real ops.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["search", "rerank", "refine_ann", "add"]
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
