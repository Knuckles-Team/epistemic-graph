//! `DocumentDecoder` — the plugin seam real extractors implement (CONCEPT:EG-P1-3).
//!
//! Turning raw bytes into a real `DocumentData` (PDF layout analysis, OCR over
//! scanned pages, HTML/DOCX structure extraction, table detection, …) needs heavy,
//! often non-Rust dependencies (`pdfium`/`lopdf`, `tesseract` bindings, …) that must
//! NEVER land in this crate's default build (the Pi contract every sibling
//! modality crate holds). So decoding is a trait, not a function: any crate/service
//! (in-process plugin, or a remote extraction job called over the wire) can
//! implement [`DocumentDecoder`] and hand back a typed [`crate::DocumentData`]
//! without this crate ever depending on the extractor.
//!
//! The ONE decoder this crate ships, [`PlainTextDecoder`], is deliberately trivial:
//! it treats the whole byte payload as UTF-8 text and produces a single page with a
//! single paragraph block containing a single span covering the entire text. This
//! is enough to exercise the full bytes -> pages -> blocks -> spans pipeline
//! end-to-end (see the crate-level tests) without decoding anything beyond a UTF-8
//! validity check. Real layout/OCR extraction stays an explicitly out-of-scope,
//! documented follow-up.

use crate::document::{DocumentData, LayoutBlock, Page, Span};

/// A pluggable document decoder: raw bytes in, a typed [`DocumentData`] out (or
/// `None` if these bytes aren't a format this decoder recognizes). Implement this
/// in a separate crate/service for a real extractor (PDF/DOCX/HTML layout, OCR,
/// table detection, …) — this crate's own default build ships only the trivial
/// [`PlainTextDecoder`].
pub trait DocumentDecoder {
    fn decode(&self, bytes: &[u8]) -> Option<DocumentData>;
}

/// The trivial, dependency-free default decoder: treats `bytes` as UTF-8 text and
/// wraps the WHOLE text as one page / one paragraph block / one span (`0..len`).
/// Returns `None` for non-UTF-8 bytes — it never fabricates structure over bytes
/// it cannot interpret as text. A real PDF/OCR/HTML decoder is a separate,
/// out-of-scope plugin (see module docs); this is the "one trivial end-to-end
/// path" every document-consuming caller can rely on with zero extra dependencies.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlainTextDecoder;

impl DocumentDecoder for PlainTextDecoder {
    fn decode(&self, bytes: &[u8]) -> Option<DocumentData> {
        let text = std::str::from_utf8(bytes).ok()?;
        let span = Span::new(0, text.len());
        let block = LayoutBlock::paragraph(vec![span]);
        let page = Page::new(1, vec![block]);
        Some(DocumentData::new(crate::header::content_hash(bytes)).with_pages(vec![page]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::BlockKind;

    #[test]
    fn plain_text_decoder_wraps_the_whole_text_as_one_span() {
        let decoder = PlainTextDecoder;
        let doc = decoder.decode(b"hello world").expect("valid utf8 decodes");
        assert_eq!(doc.pages.len(), 1);
        assert_eq!(doc.pages[0].number, 1);
        assert_eq!(doc.pages[0].blocks.len(), 1);
        assert_eq!(doc.pages[0].blocks[0].kind, BlockKind::Paragraph);
        let span = &doc.pages[0].blocks[0].spans[0];
        assert_eq!((span.start, span.end), (0, "hello world".len()));
    }

    #[test]
    fn plain_text_decoder_rejects_invalid_utf8() {
        let decoder = PlainTextDecoder;
        assert!(decoder.decode(&[0xFF, 0xFE, 0x00, 0x00]).is_none());
    }

    #[test]
    fn plain_text_decoder_content_addresses_the_source_bytes() {
        let decoder = PlainTextDecoder;
        let a = decoder.decode(b"same bytes").unwrap();
        let b = decoder.decode(b"same bytes").unwrap();
        let c = decoder.decode(b"different bytes").unwrap();
        assert_eq!(a.blob_ref, b.blob_ref);
        assert_ne!(a.blob_ref, c.blob_ref);
    }
}
