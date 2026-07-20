//! # eg-document — native governed document serving
//!
//! The current decoder accepts bounded UTF-8, derives form-feed pages, line-level
//! heading/list/paragraph/table structure, exact Unicode-scalar character spans, and authority-keyed
//! lexical postings. Only SHA-256 content identity, structure, spans, and opaque
//! lexeme references are durable; source text stays request-local. Rich binary
//! document decoders can implement [`DocumentDecoder`] and must emit the same
//! governed, source-free [`DocumentData`] contract.
//!
//! ## `ModalityContract` (CONCEPT:EG-P1-3)
//!
//! Behind the crate's `contract` feature, `DocumentData::evidence_address()` returns a real,
//! located `eg_modality::EvidenceAddress::CharacterRange` from the first stored span
//! (or `None` when the document carries no page/block/span structure yet — never
//! fabricated). See `src/contract.rs`.
//!
//! ## `serving`
//!
//! `serving` exposes [`runtime::NativeDocumentRuntime`] and
//! [`DocumentServingRuntime`]. Commits use the universal governed artifact protocol,
//! maintain the lexical posting index atomically, and never retain source text.

mod decoder;
mod document;
mod header;

// ModalityContract implementation, behind the crate's opt-in contract feature.
#[cfg(feature = "contract")]
mod contract;

#[cfg(feature = "serving")]
pub mod runtime;

pub use decoder::{DocumentDecoder, LexemeEncoder, NativeTextDecoder};
pub use document::{
    Annotation, BlockKind, ChunkLineage, DocumentData, LayoutBlock, LexicalPosting, Page, Span,
    Table, TableCell,
};
pub use header::content_hash;

/// Governed, restartable served document runtime. Available under `serving` and
/// backed by the universal Artifact/Occurrence/Rendition/Segment/Feature/Locus
/// protocol rather than source-specific configuration.
#[cfg(feature = "serving")]
pub type DocumentServingRuntime = eg_modality::ServedModalityRuntime<DocumentData>;

// The leaf default stays value-only; the main facade enables governed serving.
#[cfg(all(test, not(feature = "contract")))]
mod default_build_guardrail {
    #[test]
    fn default_build_has_no_contract() {
        assert!(
            !cfg!(feature = "contract"),
            "the `contract` feature must stay opt-in, never part of `default`"
        );
    }
}
