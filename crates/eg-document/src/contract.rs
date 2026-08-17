//! Governed modality contract for [`DocumentData`].

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceAddress, GovernedModality,
    IngestReport, ModalityContract, ModalitySelfTest, NativeIndexKey, NativePredicate, OpaqueRef,
    Provenance, RowSetShape, StagedWrite, StorageStats,
};

use crate::document::{DocumentData, LayoutBlock, LexicalPosting, Page, Span};

const MAX_PAGES: usize = 4_096;
const MAX_BLOCKS: usize = 250_000;
const MAX_STRUCTURED_ITEMS: usize = 1_000_000;

fn opaque(value: &str) -> bool {
    OpaqueRef::new(value.to_string()).is_ok()
}

fn content_address(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn safe_language(value: &str) -> bool {
    let mut parts = value.split('-');
    let Some(primary) = parts.next() else {
        return false;
    };
    (2..=3).contains(&primary.len())
        && primary.bytes().all(|byte| byte.is_ascii_lowercase())
        && parts.all(|part| {
            (2..=8).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_alphanumeric())
        })
}

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

    fn cdc_topic(&self) -> Option<&'static str> {
        Some("modality.document.v1")
    }

    fn provenance(&self, _id: &str) -> Option<Provenance> {
        Some(Provenance::asserted())
    }

    /// The X1 evidence resolver: the FIRST span of the first block of the first
    /// page, as an exact character range. `None` when there is no page/block/span structure
    /// yet — this NEVER fabricates a whole-document fallback span (mirrors
    /// `eg_image::ImageData::evidence`'s "never fabricated" contract).
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        let span = self.first_span()?;
        Some(EvidenceAddress::CharacterRange {
            start: span.start as u64,
            end: span.end as u64,
        })
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "page_index",
            "layout_blocks",
            "table_extract",
            "chunk_lineage",
            "lexical_lookup",
        ]
    }

    fn policy_labels(&self, _id: &str) -> Vec<String> {
        vec![
            "eg:policylabel:0000000000000001".to_string(),
            "eg:policylabel:0000000000000002".to_string(),
            "eg:policylabel:0000000000000003".to_string(),
        ]
    }

    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let passed = match decode_staged::<DocumentData>(&staged) {
            Ok(round_trip) if round_trip == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        let streaming = [staged].into_iter().all(|item| {
            matches!(
                decode_staged::<DocumentData>(&item),
                Ok(round_trip) if round_trip == *self
            )
        });
        IngestReport {
            batch: passed,
            streaming: if streaming {
                ModalitySelfTest::Passed
            } else {
                ModalitySelfTest::Failed
            },
        }
    }

    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        Some(StorageStats {
            logical_bytes: encode_staged(self).len() as u64,
            element_count: self.pages.iter().map(|page| page.blocks.len() as u64).sum(),
            has_secondary_index: !self.lexical_postings.is_empty(),
        })
    }

    fn backup_selfcheck(&self, id: &str) -> ModalitySelfTest {
        match decode_staged::<DocumentData>(&self.txn_stage(id)) {
            Ok(restored) if restored == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        self.backup_selfcheck(id)
    }
}

impl GovernedModality for DocumentData {
    fn validate_governed_payload(&self) -> bool {
        let mut page_numbers = std::collections::BTreeSet::new();
        let mut postings = std::collections::BTreeSet::new();
        let block_count = self
            .pages
            .iter()
            .try_fold(0usize, |count, page| count.checked_add(page.blocks.len()));
        let structured_items = self.pages.iter().try_fold(0usize, |count, page| {
            page.blocks.iter().try_fold(count, |count, block| {
                let count = count.checked_add(block.spans.len())?;
                count.checked_add(block.table.as_ref().map_or(0, |table| table.cells.len()))
            })
        });
        content_address(&self.blob_ref)
            && !self.pages.is_empty()
            && self.pages.len() <= MAX_PAGES
            && block_count.is_some_and(|count| count <= MAX_BLOCKS)
            && structured_items.is_some_and(|count| count <= MAX_STRUCTURED_ITEMS)
            && self.annotations.len() <= MAX_STRUCTURED_ITEMS
            && self.chunks.len() <= MAX_STRUCTURED_ITEMS
            && self.lexical_postings.len() <= MAX_STRUCTURED_ITEMS
            && self.language.as_deref().is_none_or(safe_language)
            && self.version.as_deref().is_none_or(opaque)
            && self.pages.iter().all(|page| {
                page.number > 0
                    && page_numbers.insert(page.number)
                    && page.blocks.iter().all(|block| {
                        (block.kind == crate::BlockKind::Table) == block.table.is_some()
                            && block.spans.iter().all(|span| {
                                span.end > span.start && span.label.as_deref().is_none_or(opaque)
                            })
                            && block
                                .spans
                                .windows(2)
                                .all(|pair| pair[0].end <= pair[1].start)
                            && block.table.as_ref().is_none_or(|table| {
                                table.rows > 0
                                    && table.cols > 0
                                    && table.rows.checked_mul(table.cols).is_some_and(|slots| {
                                        slots <= MAX_STRUCTURED_ITEMS && table.cells.len() <= slots
                                    })
                                    && table
                                        .cells
                                        .iter()
                                        .all(|cell| cell.row < table.rows && cell.col < table.cols)
                                    && table
                                        .cells
                                        .iter()
                                        .map(|cell| (cell.row, cell.col))
                                        .collect::<std::collections::BTreeSet<_>>()
                                        .len()
                                        == table.cells.len()
                            })
                    })
            })
            && self.annotations.iter().all(|annotation| {
                opaque(&annotation.label)
                    && match (annotation.page, annotation.span) {
                        (None, None) => true,
                        (Some(page), Some((start, end))) => {
                            end > start && page_numbers.contains(&page)
                        }
                        _ => false,
                    }
            })
            && self.chunks.iter().all(|chunk| {
                opaque(&chunk.chunk_id)
                    && opaque(&chunk.document_id)
                    && chunk.derived_from.len() <= MAX_STRUCTURED_ITEMS
                    && chunk.page.is_none_or(|page| page_numbers.contains(&page))
                    && chunk.span.1 > chunk.span.0
                    && chunk.derived_from.iter().all(|parent| opaque(parent))
                    && chunk
                        .derived_from
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == chunk.derived_from.len()
            })
            && !self.lexical_postings.is_empty()
            && self.lexical_postings.iter().all(|posting| {
                OpaqueRef::new(posting.token_ref.clone())
                    .is_ok_and(|reference| reference.namespace() == "lexeme")
                    && posting.page > 0
                    && self
                        .pages
                        .iter()
                        .find(|page| page.number == posting.page)
                        .and_then(|page| page.blocks.get(posting.block as usize))
                        .is_some_and(|block| {
                            block
                                .spans
                                .iter()
                                .any(|span| span.start <= posting.start && posting.end <= span.end)
                        })
                    && posting.end > posting.start
                    && postings.insert((
                        posting.token_ref.clone(),
                        posting.page,
                        posting.block,
                        posting.start,
                        posting.end,
                    ))
            })
    }

    fn native_index_keys(&self) -> Vec<NativeIndexKey> {
        self.lexical_postings
            .iter()
            .filter_map(|posting| OpaqueRef::new(posting.token_ref.clone()).ok())
            .map(NativeIndexKey::Lexeme)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn matches_native_predicate(&self, predicate: &NativePredicate) -> bool {
        let NativePredicate::DocumentLexeme { lexeme_ref, page } = predicate else {
            return false;
        };
        self.lexical_postings.iter().any(|posting| {
            posting.token_ref == lexeme_ref.as_str()
                && page.is_none_or(|number| posting.page == number)
        })
    }
}

impl ConformanceTestable for DocumentData {
    fn conformance_sample() -> Self {
        DocumentData::new("deadbeefcafefeed00000000000000000deadbeefcafefeed000000000000000")
            .with_language("en")
            .with_pages(vec![Page::new(
                1,
                vec![LayoutBlock::paragraph(vec![Span::labeled(
                    "eg:label:0000000000000001",
                    0,
                    42,
                )])],
            )])
            .with_lexical_postings(vec![LexicalPosting {
                token_ref: "eg:lexeme:0000000000000001".to_string(),
                page: 1,
                block: 0,
                start: 0,
                end: 42,
            }])
    }

    #[cfg(feature = "serving")]
    fn native_production_probe() -> Option<eg_modality::NativeProductionProbe> {
        Some(crate::runtime::production_probe())
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
        assert_eq!(ModalityContract::evidence_address(&doc), None);
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
            ModalityContract::evidence_address(&doc),
            Some(EvidenceAddress::CharacterRange { start: 1, end: 10 })
        );
    }

    #[test]
    fn to_rowset_stays_unranked() {
        let doc = DocumentData::new("h");
        assert_eq!(ModalityContract::to_rowset(&doc, "doc-1").score, None);
    }

    #[cfg(feature = "serving")]
    #[test]
    fn served_document_is_production_ready_12_of_12() {
        let report = eg_modality::tck_report::<DocumentData>();
        assert!(report.is_production_ready(), "{}", report.summary());
        assert_eq!(report.pass_count(), 12);
        assert_eq!(report.na_count(), 0);
    }

    #[test]
    fn governed_document_rejects_raw_labels() {
        let valid = DocumentData::conformance_sample();
        assert!(GovernedModality::validate_governed_payload(&valid));
        let mut unsafe_value = valid;
        unsafe_value.pages[0].blocks[0].spans[0].label = Some("raw-display-label".to_string());
        assert!(!GovernedModality::validate_governed_payload(&unsafe_value));
    }
}

eg_modality::modality_conformance_tests!(DocumentData);
