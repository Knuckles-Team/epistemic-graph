//! `ModalityContract` retrofit for [`TextHit`] (CONCEPT:E4/X1).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s README retrofit order (step 4: eg-text — BM25/Tantivy —
//! `to_rowset`'s score is the natural BM25 rank).
//!
//! `TextHit` itself is dep-free (no `tantivy` needed), so `contract` here does NOT
//! need to imply `tantivy` — it stays minimal (`contract = ["dep:eg-modality"]`).
//!
//! `evidence_address()` is the X1 (multimodal-evidence) case where the modality's own
//! value lacks enough information for an honest location. A bare hit therefore
//! returns `None`; a caller with the original text can bind a real snippet range.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceAddress, IngestReport,
    ModalityContract, ModalitySelfTest, RowSetShape, StagedWrite, StorageStats, TckPoint,
};

use crate::{CitationSpan, TableSpan, TextHit};

/// A [`TableSpan`] (from `src/layout.rs`'s heuristic table extractor) IS
/// field-for-field an `EvidenceAddress::TableCellRange` — this conversion only
/// exists under `contract` since that's the only place `eg-modality` is linked
/// (the extractor itself stays dependency-free; see `src/layout.rs`'s module docs).
impl From<&TableSpan> for EvidenceAddress {
    fn from(span: &TableSpan) -> Self {
        EvidenceAddress::TableCellRange {
            row_start: span.row_start as u64,
            row_end: span.row_end as u64,
            col_start: span.col_start as u64,
            col_end: span.col_end as u64,
        }
    }
}

/// A [`CitationSpan`] is a located byte range inside SOME document; the caller
/// binds the resulting address to a governed `EvidenceLocus` subject.
impl CitationSpan {
    pub fn to_evidence_address(&self) -> EvidenceAddress {
        EvidenceAddress::CharacterRange {
            start: self.start as u64,
            end: self.end as u64,
        }
    }
}

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
    /// Rather than fabricate a byte range this modality does not know, this returns
    /// `None`. The ingestion boundary must bind a real snippet range and governed
    /// subject before it can construct an `EvidenceLocus`.
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        None
    }

    // ── EG-P1-1 hooks — real, minimal implementations over TextHit's serialization
    // and txn staging. TextHit is a query-time result, not a durable store value. ──

    /// Batch ingest = parse a `TextHit` back from its serialized form. Streaming
    /// is genuinely N/A: a BM25 hit is a scalar query result, not an append stream.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<TextHit>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a BM25 hit is a scalar query result, not an append stream",
            ),
        }
    }

    /// Real storage stats from the serialized TextHit: logical size from encoded
    /// length; a TextHit is a single unit so element count is 1. Text search results
    /// do NOT have a secondary index (the index is maintained separately by the
    /// Tantivy backend), so `has_secondary_index` is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: 1,
            has_secondary_index: false,
        })
    }

    /// N/A: TextHit is a query-time result, not a durable store value. There is no
    /// backup codec — durability is a concern of the underlying Tantivy index, not
    /// the result value itself.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "a BM25 hit is a query-time result, not a durable store value — durability is maintained by the Tantivy index backend",
        )
    }

    /// Simulated single-node crash-and-recover through the txn staging path.
    /// Stage the hit as an in-txn write; the staged payload IS the WAL record;
    /// on "restart" replay-decode it and confirm the recovered hit is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<TextHit>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// A BM25 hit has no CDC, policy, or provenance of its own (parallel to query
    /// results from any search backend): CDC is the index's concern; policy is at
    /// the graph-node layer; provenance/evidence are per-document, not per-hit.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "a BM25 hit is a query-time result, not a stored value — change-capture/delete/GC is maintained by the Tantivy index backend",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the result",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "a BM25 hit has no derivation history; provenance/evidence are document-level concerns, not per-hit properties",
            ),
            _ => None,
        }
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

// A direct test beyond the generic "never panics" conformance check: a scored hit
// without an exact snippet must never fabricate a location.
#[cfg(test)]
mod evidence_mapping {
    use super::*;

    #[test]
    fn scored_hit_without_snippet_has_no_evidence_address() {
        let hit = TextHit {
            id: "doc-1".to_string(),
            score: 4.2,
        };
        assert_eq!(hit.evidence_address(), None);
    }
}

// Direct tests of the layout-extractor → `EvidenceAddress` conversions.
#[cfg(test)]
mod layout_evidence_mapping {
    use super::*;
    use crate::{citation_spans, extract_tables};

    #[test]
    fn table_span_converts_losslessly_to_table_cell_range() {
        let text = "| A | B |\n| --- | --- |\n| 1 | 2 |\n";
        let tables = extract_tables(text);
        let cell = tables[0].cell_span(1, 0).unwrap();
        let evidence: EvidenceAddress = (&cell).into();
        assert_eq!(
            evidence,
            EvidenceAddress::TableCellRange {
                row_start: 1,
                row_end: 1,
                col_start: 0,
                col_end: 0,
            }
        );
    }

    #[test]
    fn citation_span_converts_to_document_span_with_caller_supplied_id() {
        let text = "supported by prior work [12].";
        let spans = citation_spans(text);
        let evidence = spans[0].to_evidence_address();
        assert_eq!(
            evidence,
            EvidenceAddress::CharacterRange {
                start: spans[0].start as u64,
                end: spans[0].end as u64,
            }
        );
    }
}
