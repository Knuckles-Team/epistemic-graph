//! `ModalityContract` retrofit for [`TextHit`] (CONCEPT:E4/X1).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s README retrofit order (step 4: eg-text — BM25/Tantivy —
//! `to_rowset`'s score is the natural BM25 rank).
//!
//! `TextHit` itself is dep-free (no `tantivy` needed), so `contract` here does NOT
//! need to imply `tantivy` — it stays minimal (`contract = ["dep:eg-modality"]`).
//!
//! `evidence()` is the X1 (multimodal-evidence) case where the modality's OWN value
//! shape limits how precise the located span can honestly be — see the method's own
//! doc comment for why this is a whole-document `DocumentSpan`, not a fabricated
//! character range.

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, RowSetShape, StagedWrite,
};

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

    /// X1 (multimodal-evidence): the ONLY located information a `TextHit` itself
    /// carries is WHICH document matched — it does not track character offsets. The
    /// underlying [`crate::index::TextIndex`] indexes with `STORED = false` (see its
    /// module docs: "we never read the body back"), so no passage-level span can be
    /// reconstructed from a `TextHit` alone; only [`crate::bm25::bm25_snippet`]
    /// (given the ORIGINAL doc text, which a bare `TextHit` does not carry) computes
    /// a real byte-offset match window.
    ///
    /// Rather than fabricate a byte range this modality does not know, this reports
    /// the one thing that IS real and exact — `id` names the precise document this
    /// hit resolved to — as a whole-document `DocumentSpan`: `start: 0`, `end:
    /// usize::MAX` as an explicit "rest of document, exact length not tracked at
    /// this granularity" sentinel, never a specific fabricated sub-range. A future
    /// passage-level resolver — e.g. a caller pairing a `TextHit` with the query and
    /// the doc's own text to run `bm25_snippet`'s window-finding logic and keep its
    /// `(lo_byte, hi_byte)` — is the documented follow-up once that pairing exists.
    fn evidence(&self, id: &str) -> Option<EvidenceSpan> {
        Some(EvidenceSpan::DocumentSpan {
            document_id: id.to_string(),
            start: 0,
            end: usize::MAX,
        })
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

// A direct test of the `evidence()` mapping itself, beyond the generic "never
// panics" conformance check — proves the whole-document `DocumentSpan` uses the
// caller-supplied `id` (NOT `self.id`, consistent with `to_rowset`'s own id
// contract) and the documented `usize::MAX` "rest of document" sentinel.
#[cfg(test)]
mod evidence_mapping {
    use super::*;

    #[test]
    fn whole_document_span_uses_the_caller_supplied_id() {
        let hit = TextHit {
            id: "doc-1".to_string(),
            score: 4.2,
        };
        let span = hit
            .evidence("doc-7")
            .expect("a TextHit always reports document-granularity evidence");
        assert_eq!(
            span,
            EvidenceSpan::DocumentSpan {
                document_id: "doc-7".to_string(),
                start: 0,
                end: usize::MAX,
            }
        );
    }
}
