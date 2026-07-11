//! `ModalityContract` retrofit for [`DocumentData`] (CONCEPT:EG-P1-3, mirroring
//! `eg-image`/`eg-audio`/`eg-video`'s CONCEPT:E4 follow-up).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF).

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::document::{DocumentData, LayoutBlock, Page, Span};

impl ModalityContract for DocumentData {
    fn storage_kind(&self) -> &'static str {
        "document"
    }

    /// A document is a FILTER/SOURCE candidate, not an intrinsically ranked value —
    /// unranked until a RANK op imposes a score (mirrors `eg_image::ImageData`).
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// Bare document values are not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// A document has no derivation history of its own — default `None` is
    /// correct as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// The X1 evidence resolver: the FIRST span of the first block of the first
    /// page, as a real, located `EvidenceSpan::DocumentSpan` under the given row
    /// `id` as `document_id`. `None` when there is no page/block/span structure
    /// yet — this NEVER fabricates a whole-document fallback span (mirrors
    /// `eg_image::ImageData::evidence`'s "never fabricated" contract).
    fn evidence(&self, id: &str) -> Option<EvidenceSpan> {
        let span = self.first_span()?;
        Some(EvidenceSpan::DocumentSpan {
            document_id: id.to_string(),
            start: span.start,
            end: span.end,
        })
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "page_index",
            "layout_blocks",
            "table_extract",
            "chunk_lineage",
        ]
    }
}

impl ConformanceTestable for DocumentData {
    fn conformance_sample() -> Self {
        DocumentData::new("deadbeefcafefeed00000000000000000")
            .with_language("en")
            .with_pages(vec![Page::new(
                1,
                vec![LayoutBlock::paragraph(vec![Span::labeled("intro", 0, 42)])],
            )])
    }
}

// Exercise the no-structure branch (evidence() -> None) through the SAME battery,
// beyond the one sample the macro drives.
#[cfg(test)]
mod extra_coverage {
    use super::*;

    #[test]
    fn evidence_is_none_without_any_page() {
        let doc = DocumentData::new("h");
        assert_eq!(ModalityContract::evidence(&doc, "doc-1"), None);
    }

    #[test]
    fn evidence_returns_the_first_span_as_a_real_located_span() {
        let doc = DocumentData::new("h").with_pages(vec![Page::new(
            1,
            vec![LayoutBlock::paragraph(vec![
                Span::labeled("a", 1, 10),
                Span::labeled("b", 10, 20),
            ])],
        )]);
        assert_eq!(
            ModalityContract::evidence(&doc, "doc-1"),
            Some(EvidenceSpan::DocumentSpan {
                document_id: "doc-1".to_string(),
                start: 1,
                end: 10,
            })
        );
    }

    #[test]
    fn to_rowset_stays_unranked() {
        let doc = DocumentData::new("h");
        assert_eq!(ModalityContract::to_rowset(&doc, "doc-1").score, None);
    }
}

eg_modality::modality_conformance_tests!(DocumentData);
