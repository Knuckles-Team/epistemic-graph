//! `ModalityContract` retrofit for [`TextHit`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s README retrofit order (step 4: eg-text — BM25/Tantivy —
//! `to_rowset`'s score is the natural BM25 rank).
//!
//! `TextHit` itself is dep-free (no `tantivy` needed), so `contract` here does NOT
//! need to imply `tantivy` — it stays minimal (`contract = ["dep:eg-modality"]`).

use eg_modality::{encode_staged, ConformanceTestable, ModalityContract, RowSetShape, StagedWrite};

use crate::TextHit;

impl ModalityContract for TextHit {
    fn storage_kind(&self) -> &'static str {
        "text"
    }

    /// A BM25 hit IS an intrinsically ranked value (`self.score`) — but the id
    /// projected into the row is the caller-supplied `id` PARAMETER, not `self.id`:
    /// the two may differ (e.g. a caller re-keying a hit under a different id space)
    /// and the parameter wins, per `ModalityContract::to_rowset`'s own contract that
    /// "the modality value itself does not know its own id" — mirroring every other
    /// pilot (`eg-tensor`/`eg-geo`/`eg-rdf`'s `to_rowset(&self, id: &str)`).
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::scored(id, self.score)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A BM25 hit is a query-time result row, not a live write path — not (yet) on
    /// the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// The crate's real dep-free scoring/fusion functions (`crate::bm25::{bm25_score,
    /// bm25_snippet}` + `crate::rrf_fuse`) — the ones a `TextHit` is actually
    /// produced/explained/fused by, always compiled regardless of the `tantivy`
    /// feature.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["bm25_score", "bm25_snippet", "rrf_fuse"]
    }
}

impl ConformanceTestable for TextHit {
    fn conformance_sample() -> Self {
        TextHit {
            id: "doc-1".to_string(),
            score: 4.2,
        }
    }
}

eg_modality::modality_conformance_tests!(TextHit);
