//! # eg-document — the document modality (layout-level, CONCEPT:EG-P1-3)
//!
//! A pure-Rust leaf crate (a sibling of `eg-image` / `eg-audio` / `eg-video`) giving
//! the engine a document data type with **NO heavy PDF/HTML/OCR dependency** (no
//! `pdfium`/`lopdf`/`tesseract` bindings) — the Raspberry-Pi contract. It is
//! deliberately **layout-level**, not a rendering/OCR engine:
//!
//! * [`DocumentData`] — a typed `blob_ref` (content-addressed original bytes) +
//!   `language`/`version` + a `pages` hierarchy (`Page` -> `LayoutBlock` ->
//!   `Span`/`Table`) + `annotations` + `chunks` (a [`ChunkLineage`] trail back to
//!   the exact page/byte-range a downstream chunk was derived from). This crate
//!   never invents structure — every page/block/span/table is externally
//!   supplied (by a [`decoder::DocumentDecoder`] plugin, or built directly via the
//!   constructors).
//! * [`decoder::DocumentDecoder`] — the plugin trait real extractors (PDF layout
//!   analysis, OCR, HTML/DOCX structure extraction, table detection, …)
//!   implement. The ONE decoder shipped here, [`decoder::PlainTextDecoder`], is
//!   trivial: it treats raw bytes as UTF-8 text and wraps the whole thing as one
//!   page/one paragraph/one span — enough to exercise the full bytes -> pages ->
//!   blocks -> spans pipeline end-to-end with zero extra dependencies.
//! * [`header::content_hash`] — the SAME FNV-1a-128 construction
//!   `eg_image`/`eg_audio`/`eg_video`'s `header::content_hash` use (duplicated,
//!   not shared — `eg-document` and its siblings are DAG-parallel leaf crates).
//!
//! ## What this crate does NOT do
//!
//! Real PDF/DOCX/HTML layout parsing, table detection, and OCR are explicitly OUT
//! of scope for v1 — see the crate's reserved, dependency-free `extract` feature
//! (currently a no-op) for where a real extractor would be gated in a follow-up,
//! kept OUT of the default build so the Pi contract holds (no heavy
//! extraction/OCR dependency ever lands in a default/pi/node/full build unless a
//! caller explicitly opts in). A real extractor is a PLUGIN: implement
//! [`decoder::DocumentDecoder`] in a separate crate/service (in-process, or a
//! remote extraction job called over the wire) and hand this crate the resulting
//! typed [`DocumentData`] — this crate never needs to depend on the extractor to
//! accept its output.
//!
//! ## `ModalityContract` (CONCEPT:EG-P1-3)
//!
//! Behind the crate's own opt-in `contract` feature (default OFF, mirroring
//! `eg-image`/`eg-audio`/`eg-video`): `DocumentData::evidence(id)` returns a REAL,
//! located `eg_modality::EvidenceSpan::DocumentSpan` from the first stored span
//! (or `None` when the document carries no page/block/span structure yet — never
//! fabricated). See `src/contract.rs`.

mod decoder;
mod document;
mod header;

// ModalityContract retrofit (CONCEPT:EG-P1-3): `impl ModalityContract for
// DocumentData`, behind the crate's own opt-in `contract` feature (default OFF).
#[cfg(feature = "contract")]
mod contract;

pub use decoder::{DocumentDecoder, PlainTextDecoder};
pub use document::{
    Annotation, BlockKind, ChunkLineage, DocumentData, LayoutBlock, Page, Span, Table, TableCell,
};
pub use header::content_hash;

// CONCEPT:EG-P1-3 — the small-footprint default is sacred: neither `contract` nor
// the reserved `extract` seam (a real PDF/OCR/HTML extractor) is ever implied by
// `default`, so a plain `cargo build -p eg-document` links no heavy extraction/
// OCR dependency. Asserted here as an executable regression check (mirrors
// `eg-image`/`eg-audio`/`eg-video`'s identical guardrail).
#[cfg(all(test, not(any(feature = "contract", feature = "extract"))))]
mod default_build_guardrail {
    #[test]
    fn default_build_has_neither_contract_nor_extract_on() {
        assert!(
            !cfg!(feature = "contract"),
            "the `contract` feature must stay opt-in, never part of `default`"
        );
        assert!(
            !cfg!(feature = "extract"),
            "the `extract` feature must stay opt-in, never part of `default`"
        );
    }
}
