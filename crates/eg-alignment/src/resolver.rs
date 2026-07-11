//! The evidence-resolver seam (CONCEPT:EG-P1-3, closing the X1 residue this crate
//! completes): a way to resolve an `eg_modality::EvidenceSpan` back to its located
//! artifact — re-extract the text at a `DocumentSpan`, crop the pixels at an
//! `ImageRegion`, slice the samples at an `AudioSegment`, seek the frame at a
//! `VideoShot`, … `eg-modality`'s own `evidence.rs` module docs call this
//! "explicitly out of scope" for that crate; this is where the seam actually
//! lives.
//!
//! [`EvidenceResolver`] is a trait, not a concrete implementation, for the same
//! reason `eg_document::DocumentDecoder` is a trait: resolving a span into REAL
//! rendered content needs the same heavy, format-specific machinery
//! (PDF/image/audio/video codecs) this workspace keeps out of the default build.
//! A real resolver — one backed by the engine's blob CAS plus the actual
//! artifact's stored value — is a follow-up a caller wires against its own
//! storage layer. [`InMemoryResolver`] is the trivial, dependency-free
//! implementation this crate ships: a caller-populated `artifact_id -> content`
//! map, useful for tests and for small in-process alignments where the located
//! content is already at hand.

use eg_modality::EvidenceSpan;
use std::collections::HashMap;

/// The artifact-level content an [`EvidenceResolver`] resolved an `EvidenceSpan`
/// to. Intentionally coarse (text excerpt vs. an opaque blob reference) — a real
/// resolver backed by a real codec could return a richer, format-specific type;
/// this is the minimal common shape the seam needs to prove resolution occurred.
#[derive(Clone, Debug, PartialEq)]
pub enum ResolvedArtifact {
    /// A resolved text excerpt (e.g. the characters at a `DocumentSpan`, or a
    /// transcript slice at an `AudioSegment`).
    Text {
        artifact_id: String,
        excerpt: String,
    },
    /// A resolved reference into the blob CAS (e.g. a cropped image region's
    /// re-encoded bytes, or a video shot's keyframe) — the blob itself is opaque
    /// here; `note` records what this reference represents.
    Blob {
        artifact_id: String,
        blob_ref: String,
        note: String,
    },
}

/// Resolves a located [`EvidenceSpan`] to its [`ResolvedArtifact`]. `None` means
/// "this resolver has nothing for that artifact" — never a fabricated result.
pub trait EvidenceResolver {
    fn resolve(&self, span: &EvidenceSpan) -> Option<ResolvedArtifact>;
}

/// The stable artifact-id an `EvidenceSpan` locates content within — the SAME id
/// a caller would have used as the `id` argument to
/// `eg_modality::ModalityContract::evidence(id)` when the span was produced.
/// Every variant carries exactly one such id; this just projects it out
/// uniformly so a resolver can key off it without a match per call site.
pub fn artifact_id(span: &EvidenceSpan) -> &str {
    match span {
        EvidenceSpan::DocumentSpan { document_id, .. } => document_id,
        EvidenceSpan::TableCellRange { table_id, .. } => table_id,
        EvidenceSpan::ImageRegion { image_id, .. } => image_id,
        EvidenceSpan::PageBox { document_id, .. } => document_id,
        EvidenceSpan::AudioSegment { audio_id, .. } => audio_id,
        EvidenceSpan::VideoShot { video_id, .. } => video_id,
        EvidenceSpan::CodeSymbol { file_path, .. } => file_path,
        EvidenceSpan::TraceSpan { trace_id, .. } => trace_id,
    }
}

/// A trivial, dependency-free [`EvidenceResolver`]: a caller-populated
/// `artifact_id -> content` registry. Not a real codec/extractor — see module
/// docs — but enough to prove the resolver seam end-to-end (the crate's
/// `tests/integration.rs` uses exactly this).
#[derive(Clone, Debug, Default)]
pub struct InMemoryResolver {
    texts: HashMap<String, String>,
    blobs: HashMap<String, String>,
}

impl InMemoryResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a resolvable text excerpt under `artifact_id`.
    pub fn with_text(mut self, artifact_id: impl Into<String>, excerpt: impl Into<String>) -> Self {
        self.texts.insert(artifact_id.into(), excerpt.into());
        self
    }

    /// Register a resolvable blob reference under `artifact_id`.
    pub fn with_blob(
        mut self,
        artifact_id: impl Into<String>,
        blob_ref: impl Into<String>,
    ) -> Self {
        self.blobs.insert(artifact_id.into(), blob_ref.into());
        self
    }
}

impl EvidenceResolver for InMemoryResolver {
    fn resolve(&self, span: &EvidenceSpan) -> Option<ResolvedArtifact> {
        let id = artifact_id(span);
        if let Some(excerpt) = self.texts.get(id) {
            return Some(ResolvedArtifact::Text {
                artifact_id: id.to_string(),
                excerpt: excerpt.clone(),
            });
        }
        if let Some(blob_ref) = self.blobs.get(id) {
            return Some(ResolvedArtifact::Blob {
                artifact_id: id.to_string(),
                blob_ref: blob_ref.clone(),
                note: "resolved via InMemoryResolver registry".to_string(),
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_id_projects_the_right_field_per_variant() {
        assert_eq!(
            artifact_id(&EvidenceSpan::DocumentSpan {
                document_id: "d1".to_string(),
                start: 0,
                end: 1
            }),
            "d1"
        );
        assert_eq!(
            artifact_id(&EvidenceSpan::ImageRegion {
                image_id: "i1".to_string(),
                x: 0.0,
                y: 0.0,
                width: 1.0,
                height: 1.0
            }),
            "i1"
        );
    }

    #[test]
    fn in_memory_resolver_resolves_a_registered_text_artifact() {
        let resolver = InMemoryResolver::new().with_text("doc-1", "hello world");
        let span = EvidenceSpan::DocumentSpan {
            document_id: "doc-1".to_string(),
            start: 0,
            end: 5,
        };
        assert_eq!(
            resolver.resolve(&span),
            Some(ResolvedArtifact::Text {
                artifact_id: "doc-1".to_string(),
                excerpt: "hello world".to_string(),
            })
        );
    }

    #[test]
    fn in_memory_resolver_resolves_a_registered_blob_artifact() {
        let resolver = InMemoryResolver::new().with_blob("img-1", "blobref123");
        let span = EvidenceSpan::ImageRegion {
            image_id: "img-1".to_string(),
            x: 1.0,
            y: 2.0,
            width: 3.0,
            height: 4.0,
        };
        let resolved = resolver.resolve(&span);
        assert!(matches!(
            resolved,
            Some(ResolvedArtifact::Blob { ref blob_ref, .. }) if blob_ref == "blobref123"
        ));
    }

    /// CONCEPT:EG-X1 — `artifact_id` projects `PageBox`'s `document_id`, the SAME
    /// field `DocumentSpan` projects (both locate content within a document).
    #[test]
    fn artifact_id_projects_page_box_document_id() {
        assert_eq!(
            artifact_id(&EvidenceSpan::PageBox {
                document_id: "doc-pdf-1".to_string(),
                page: 3,
                x: 10.0,
                y: 20.0,
                width: 100.0,
                height: 40.0,
            }),
            "doc-pdf-1"
        );
    }

    /// CONCEPT:EG-X1 — a registered `PageBox` artifact resolves through the SAME
    /// `InMemoryResolver` text/blob registry every other `EvidenceSpan` variant
    /// does (it keys purely off `artifact_id`, not the variant).
    #[test]
    fn in_memory_resolver_resolves_a_registered_page_box_artifact() {
        let resolver = InMemoryResolver::new().with_blob("doc-pdf-1", "blobref-pdf");
        let span = EvidenceSpan::PageBox {
            document_id: "doc-pdf-1".to_string(),
            page: 3,
            x: 10.0,
            y: 20.0,
            width: 100.0,
            height: 40.0,
        };
        let resolved = resolver.resolve(&span);
        assert!(matches!(
            resolved,
            Some(ResolvedArtifact::Blob { ref blob_ref, .. }) if blob_ref == "blobref-pdf"
        ));
    }

    #[test]
    fn in_memory_resolver_returns_none_for_an_unregistered_artifact() {
        let resolver = InMemoryResolver::new();
        let span = EvidenceSpan::AudioSegment {
            audio_id: "unknown".to_string(),
            start_ms: 0,
            end_ms: 1,
        };
        assert_eq!(resolver.resolve(&span), None);
    }
}
