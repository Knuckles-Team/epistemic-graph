//! The document modality's stored value model (CONCEPT:EG-P1-3 — first-class
//! document/media runtime modalities) — a typed raw-bytes -> pages -> layout-blocks
//! -> tables -> spans hierarchy, plus language/version/annotation fields and a
//! chunk-lineage concept, deliberately NOT a rendered/OCR'd view of the source
//! bytes (see the crate docs' Pi-contract rationale: real layout/OCR extraction is
//! a plugin, see `crate::decoder`).
//!
//! [`DocumentData`] mirrors `eg_image::ImageData`/`eg_audio::AudioData`/
//! `eg_video::VideoData`'s shape: a small, serde-serializable value that persists
//! as a typed property in the engine's redb per-graph store, with the source
//! bytes' content address (`blob_ref`) resolvable through the engine's blob CAS.
//! Unlike the metadata-only image/audio/video siblings (which read ONE scalar
//! header field), a document's structure — `pages` -> `blocks` -> `spans`/`table`
//! — is itself the typed artifact: this crate defines that hierarchy and a
//! `chunk_id`-addressable lineage trail (`ChunkLineage`) back to the exact
//! page/block/byte-range a downstream chunk (e.g. an embedding-pipeline chunk) was
//! derived from. Every level is externally supplied (by `crate::decoder`'s
//! `DocumentDecoder` plugin, or built directly via the constructors below) — this
//! crate never invents pages/blocks/spans out of nothing.

use serde::{Deserialize, Serialize};

/// The kind of layout block a [`LayoutBlock`] represents. Purely descriptive —
/// never gates behavior in this crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockKind {
    Paragraph,
    Heading,
    ListItem,
    Table,
    Caption,
    /// A block whose kind the decoder that produced it didn't classify further.
    Other,
}

/// A character range `[start, end)` of text inside a [`LayoutBlock`], with an
/// optional caller-supplied label (a named entity mention, a redaction tag, a
/// citation marker, …). Mirrors `eg_image::ImageRegion`/`eg_audio::AudioSegment`'s
/// "one named located sub-range, externally supplied" shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub start: usize,
    pub end: usize,
    #[serde(default)]
    pub label: Option<String>,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self {
            start,
            end,
            label: None,
        }
    }

    pub fn labeled(label: impl Into<String>, start: usize, end: usize) -> Self {
        Self {
            start,
            end,
            label: Some(label.into()),
        }
    }
}

/// One cell of a [`Table`], addressed by zero-based `(row, col)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableCell {
    pub row: usize,
    pub col: usize,
    pub text: String,
}

/// A table extracted from a document page — row/column extent plus a sparse cell
/// list (a decoder may omit empty cells rather than emit them).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub rows: usize,
    pub cols: usize,
    #[serde(default)]
    pub cells: Vec<TableCell>,
}

impl Table {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            cells: Vec::new(),
        }
    }

    pub fn with_cells(mut self, cells: Vec<TableCell>) -> Self {
        self.cells = cells;
        self
    }
}

/// One layout block on a [`Page`] — a paragraph/heading/list-item's text spans, or
/// a table. `spans` and `table` are independent (a `Table`-kind block carries its
/// structure in `table`, not `spans`) rather than an enum-with-payload, so serde
/// stays a flat struct (mirrors `eg_image::ImageRegion`'s plain-struct-not-enum
/// choice) and a decoder can still attach caption/footnote spans to a table block.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LayoutBlock {
    pub kind: BlockKind,
    #[serde(default)]
    pub spans: Vec<Span>,
    #[serde(default)]
    pub table: Option<Table>,
}

impl LayoutBlock {
    pub fn paragraph(spans: Vec<Span>) -> Self {
        Self {
            kind: BlockKind::Paragraph,
            spans,
            table: None,
        }
    }

    pub fn table(table: Table) -> Self {
        Self {
            kind: BlockKind::Table,
            spans: Vec::new(),
            table: Some(table),
        }
    }

    pub fn with_kind(mut self, kind: BlockKind) -> Self {
        self.kind = kind;
        self
    }
}

/// One page of a [`DocumentData`] — a 1-based page number plus its layout blocks in
/// reading order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Page {
    pub number: u32,
    #[serde(default)]
    pub blocks: Vec<LayoutBlock>,
}

impl Page {
    pub fn new(number: u32, blocks: Vec<LayoutBlock>) -> Self {
        Self { number, blocks }
    }
}

/// A document-level annotation (a redaction tag, a reviewer comment, a
/// classification label, …) independent of any single span — e.g. attached to a
/// whole page or to the document as a whole. Optionally located at a page +
/// character range when the annotation IS span-scoped.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Annotation {
    pub label: String,
    #[serde(default)]
    pub page: Option<u32>,
    #[serde(default)]
    pub span: Option<(usize, usize)>,
}

impl Annotation {
    pub fn document_level(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            page: None,
            span: None,
        }
    }

    pub fn located(label: impl Into<String>, page: u32, start: usize, end: usize) -> Self {
        Self {
            label: label.into(),
            page: Some(page),
            span: Some((start, end)),
        }
    }
}

/// A chunk-lineage record: traces one downstream-derived chunk (e.g. an
/// embedding-pipeline chunk, a retrieval passage, a redaction unit) back to the
/// exact page + byte range of `document_id` it was derived from, plus the ids of
/// any chunks it was itself derived FROM (`derived_from` — e.g. a summary chunk
/// derived from several source chunks). This is the seam a downstream chunking/
/// embedding pipeline attaches to, not something this crate computes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChunkLineage {
    pub chunk_id: String,
    pub document_id: String,
    #[serde(default)]
    pub page: Option<u32>,
    pub span: (usize, usize),
    #[serde(default)]
    pub derived_from: Vec<String>,
}

impl ChunkLineage {
    pub fn new(
        chunk_id: impl Into<String>,
        document_id: impl Into<String>,
        span: (usize, usize),
    ) -> Self {
        Self {
            chunk_id: chunk_id.into(),
            document_id: document_id.into(),
            page: None,
            span,
            derived_from: Vec::new(),
        }
    }

    pub fn on_page(mut self, page: u32) -> Self {
        self.page = Some(page);
        self
    }

    pub fn derived_from(mut self, parents: Vec<String>) -> Self {
        self.derived_from = parents;
        self
    }
}

/// The document modality's stored value: pages of layout blocks (spans/tables) +
/// language/version + a content-addressed blob reference + optional annotations
/// and chunk-lineage records. No rendered/OCR'd view of the bytes — see module
/// docs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentData {
    /// Content address of the ORIGINAL document bytes, resolvable through the
    /// engine's blob CAS. Opaque to this crate.
    pub blob_ref: String,
    /// BCP-47-ish language tag, when known (e.g. `"en"`, `"en-US"`). `None` when
    /// not supplied/detected.
    #[serde(default)]
    pub language: Option<String>,
    /// A caller-defined version/revision marker (a source doc's revision id, an
    /// extraction-pipeline version, …). Opaque string, never interpreted here.
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub pages: Vec<Page>,
    #[serde(default)]
    pub annotations: Vec<Annotation>,
    #[serde(default)]
    pub chunks: Vec<ChunkLineage>,
}

impl DocumentData {
    pub fn new(blob_ref: impl Into<String>) -> Self {
        Self {
            blob_ref: blob_ref.into(),
            language: None,
            version: None,
            pages: Vec::new(),
            annotations: Vec::new(),
            chunks: Vec::new(),
        }
    }

    pub fn with_pages(mut self, pages: Vec<Page>) -> Self {
        self.pages = pages;
        self
    }

    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn with_annotations(mut self, annotations: Vec<Annotation>) -> Self {
        self.annotations = annotations;
        self
    }

    pub fn with_chunks(mut self, chunks: Vec<ChunkLineage>) -> Self {
        self.chunks = chunks;
        self
    }

    /// The first span of the first block of the first page, if any — the
    /// "primary" located range a `ModalityContract::evidence()` resolver reports.
    /// `None` for a document with no page/block/span structure yet (never
    /// fabricated).
    pub fn first_span(&self) -> Option<&Span> {
        self.pages.first()?.blocks.first()?.spans.first()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_defaults_to_empty_structure() {
        let doc = DocumentData::new("abc");
        assert!(doc.pages.is_empty());
        assert!(doc.annotations.is_empty());
        assert!(doc.chunks.is_empty());
        assert_eq!(doc.language, None);
        assert_eq!(doc.version, None);
    }

    #[test]
    fn serde_round_trips_the_full_hierarchy() {
        let doc = DocumentData::new("hash1")
            .with_language("en")
            .with_version("rev-3")
            .with_pages(vec![Page::new(
                1,
                vec![
                    LayoutBlock::paragraph(vec![Span::labeled("intro", 0, 10)]),
                    LayoutBlock::table(Table::new(2, 2).with_cells(vec![TableCell {
                        row: 0,
                        col: 0,
                        text: "a".to_string(),
                    }])),
                ],
            )])
            .with_annotations(vec![Annotation::located("redacted", 1, 0, 10)])
            .with_chunks(vec![
                ChunkLineage::new("chunk-1", "doc-1", (0, 10)).on_page(1)
            ]);
        let json = serde_json::to_string(&doc).unwrap();
        let back: DocumentData = serde_json::from_str(&json).unwrap();
        assert_eq!(doc, back);
    }

    #[test]
    fn first_span_is_none_for_an_empty_document() {
        let doc = DocumentData::new("abc");
        assert_eq!(doc.first_span(), None);
    }

    #[test]
    fn first_span_finds_the_first_span_of_the_first_block() {
        let doc = DocumentData::new("abc").with_pages(vec![Page::new(
            1,
            vec![LayoutBlock::paragraph(vec![
                Span::new(0, 5),
                Span::new(5, 10),
            ])],
        )]);
        assert_eq!(doc.first_span(), Some(&Span::new(0, 5)));
    }
}
