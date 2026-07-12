//! CAS-backed `EvidenceResolver` (L21, CONCEPT:EG-P1-3 follow-up) — see the
//! `alignment` cargo feature's own `Cargo.toml` comment for why this lives HERE
//! rather than inside `eg-alignment` itself: `eg-alignment` is a leaf crate BELOW
//! the server (deps = `eg-modality` ONLY, no CAS/graph-storage dependency, by
//! design — see its own crate docs), so a resolver that needs to read REAL bytes
//! back out of the engine's blob CAS must live at the ONE layer that already links
//! both `eg-alignment` (the `EvidenceResolver` trait) and the blob CAS
//! ([`super::store::ChunkStore`] + [`super::stream::stream_blob_get`]) — this
//! crate's server tier.
//!
//! [`CasEvidenceResolver`] resolves an [`eg_modality::EvidenceSpan`] in two steps:
//!
//!  1. Project the span's artifact id ([`eg_alignment::artifact_id`]) and look up
//!     that id's own stored node properties on a [`GraphView`] snapshot — the SAME
//!     `node_row_object` decode `eg_plan::KnowledgeSet::from_rowset` already uses
//!     (see `crates/eg-plan/src/knowledge.rs`) — reading its `blob_ref` field: the
//!     content address every modality value type (`eg_document::DocumentData`/
//!     `eg_image::ImageData`/`eg_audio::AudioData`/`eg_video::VideoData`) stores;
//!     see each crate's own docs ("resolvable through the engine's blob CAS").
//!  2. Stream the blob's bytes back out of the [`ChunkStore`] via
//!     [`stream_blob_get`].
//!
//! A [`EvidenceSpan::DocumentSpan`] gets a REAL `ResolvedArtifact::Text` excerpt:
//! the blob's bytes decoded as UTF-8 (lossy — a non-UTF-8 document still resolves,
//! just with replacement characters rather than an error) and sliced at the span's
//! CHARACTER range, clamped to the decoded length so an out-of-range span never
//! panics (it resolves whatever the blob actually contains — never fabricated
//! content beyond that). Every other span kind (`TableCellRange`/`ImageRegion`/
//! `AudioSegment`/`VideoShot`/`CodeSymbol`/`TraceSpan`) needs a real codec/crop/
//! table-model to produce anything more specific than the raw bytes — explicitly
//! out of scope here, exactly as `eg-alignment`'s own module docs describe for a
//! "real resolver" follow-up — so it resolves to `ResolvedArtifact::Blob` naming
//! the real CAS digest, never a fabricated excerpt.
//!
//! `None` (never a fabricated result) whenever the artifact id has no node on the
//! snapshot, that node carries no `blob_ref` string property, or the digest is not
//! present in the CAS — mirroring [`eg_alignment::InMemoryResolver`]'s "nothing
//! registered for that artifact" contract.

use std::sync::Arc;

use eg_alignment::{artifact_id, EvidenceResolver, ResolvedArtifact};
use eg_core::graph::GraphView;
use eg_modality::EvidenceSpan;

use super::store::ChunkStore;
use super::stream::stream_blob_get;

/// A real, engine-backed [`EvidenceResolver`]: resolves a located [`EvidenceSpan`]
/// through a [`GraphView`] snapshot's own stored `blob_ref` property, then reads
/// the actual bytes back out of the blob [`ChunkStore`] — see the module docs.
pub struct CasEvidenceResolver<'a> {
    view: &'a GraphView,
    store: Arc<dyn ChunkStore>,
}

impl<'a> CasEvidenceResolver<'a> {
    /// Build a resolver over `view` (the graph snapshot the evidence spans were
    /// resolved from — e.g. the same one `KnowledgeSet::from_rowset` ran over) and
    /// `store` (the server's blob CAS, [`super::BlobCursors::store`]).
    pub fn new(view: &'a GraphView, store: Arc<dyn ChunkStore>) -> Self {
        Self { view, store }
    }

    /// The artifact node's own `blob_ref` string property, or `None` if the node is
    /// absent from this snapshot or carries no such property.
    fn blob_ref_for(&self, artifact_id: &str) -> Option<String> {
        self.view
            .node_row_object(artifact_id)?
            .get("blob_ref")?
            .as_str()
            .map(|s| s.to_string())
    }

    /// Stream the full blob back out of the CAS into memory. `None` if the digest
    /// is unknown to the CAS (a never-uploaded or already-GC'd blob) — never
    /// fabricated.
    fn fetch_bytes(&self, blob_digest: &str) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        stream_blob_get(self.store.as_ref(), blob_digest, &mut buf).ok()?;
        Some(buf)
    }
}

impl EvidenceResolver for CasEvidenceResolver<'_> {
    fn resolve(&self, span: &EvidenceSpan) -> Option<ResolvedArtifact> {
        let id = artifact_id(span).to_string();
        let blob_ref = self.blob_ref_for(&id)?;

        match span {
            EvidenceSpan::DocumentSpan { start, end, .. } => {
                let bytes = self.fetch_bytes(&blob_ref)?;
                let text = String::from_utf8_lossy(&bytes);
                let char_len = text.chars().count();
                let s = (*start).min(char_len);
                let e = (*end).max(s).min(char_len);
                let excerpt: String = text.chars().skip(s).take(e - s).collect();
                Some(ResolvedArtifact::Text {
                    artifact_id: id,
                    excerpt,
                })
            }
            // TableCellRange/ImageRegion/AudioSegment/VideoShot/CodeSymbol/TraceSpan
            // — no in-tree table-model/codec to crop/slice a real excerpt with (see
            // the module docs), so this reports the REAL CAS digest, not a
            // fabricated excerpt of it.
            _ => Some(ResolvedArtifact::Blob {
                artifact_id: id,
                blob_ref,
                note: "resolved via the engine's blob CAS (CasEvidenceResolver)".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::blob::store::RedbChunkStore;
    use crate::server::blob::stream::stream_blob_put;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    fn store() -> Arc<dyn ChunkStore> {
        Arc::new(RedbChunkStore::open_temp().unwrap())
    }

    /// L21: a `DocumentSpan` resolves to the REAL text excerpt read back out of the
    /// blob CAS — not a fabricated string, not just the node id.
    #[test]
    fn resolves_document_span_to_a_real_excerpt_from_the_blob_cas() {
        let cas = store();
        let committed = stream_blob_put(cas.as_ref(), "hello world".as_bytes(), 0).unwrap();

        let core = GraphCore::new();
        core.add_node(
            "doc1".into(),
            blob(json!({
                "node_type": "Document",
                "blob_ref": committed.digest,
            })),
        );
        let view = core.analysis_snapshot();

        let resolver = CasEvidenceResolver::new(&view, cas.clone());
        let span = EvidenceSpan::DocumentSpan {
            document_id: "doc1".to_string(),
            start: 0,
            end: 5,
        };

        assert_eq!(
            resolver.resolve(&span),
            Some(ResolvedArtifact::Text {
                artifact_id: "doc1".to_string(),
                excerpt: "hello".to_string(),
            })
        );
    }

    /// L21: a span whose range runs past the blob's actual length is clamped, not
    /// fabricated — it resolves the real tail of the content, never garbage past it.
    #[test]
    fn document_span_out_of_range_clamps_to_the_real_content() {
        let cas = store();
        let committed = stream_blob_put(cas.as_ref(), "hi".as_bytes(), 0).unwrap();

        let core = GraphCore::new();
        core.add_node(
            "doc1".into(),
            blob(json!({ "node_type": "Document", "blob_ref": committed.digest })),
        );
        let view = core.analysis_snapshot();

        let resolver = CasEvidenceResolver::new(&view, cas.clone());
        let span = EvidenceSpan::DocumentSpan {
            document_id: "doc1".to_string(),
            start: 0,
            end: 500,
        };

        assert_eq!(
            resolver.resolve(&span),
            Some(ResolvedArtifact::Text {
                artifact_id: "doc1".to_string(),
                excerpt: "hi".to_string(),
            })
        );
    }

    /// L21: a non-text span (e.g. an `ImageRegion`) resolves to the REAL CAS digest
    /// as a `Blob` reference — no codec to crop pixels with, so this never
    /// fabricates a cropped excerpt, only names the real stored content address.
    #[test]
    fn resolves_image_region_to_the_real_blob_digest() {
        let cas = store();
        let committed = stream_blob_put(cas.as_ref(), &[0xFFu8; 64][..], 0).unwrap();

        let core = GraphCore::new();
        core.add_node(
            "img1".into(),
            blob(json!({ "node_type": "Image", "blob_ref": committed.digest })),
        );
        let view = core.analysis_snapshot();

        let resolver = CasEvidenceResolver::new(&view, cas.clone());
        let span = EvidenceSpan::ImageRegion {
            image_id: "img1".to_string(),
            x: 0.0,
            y: 0.0,
            width: 10.0,
            height: 10.0,
        };

        let resolved = resolver.resolve(&span);
        assert_eq!(
            resolved,
            Some(ResolvedArtifact::Blob {
                artifact_id: "img1".to_string(),
                blob_ref: committed.digest,
                note: "resolved via the engine's blob CAS (CasEvidenceResolver)".to_string(),
            })
        );
    }

    /// L21: an artifact id with no node at all on the snapshot resolves to `None`
    /// — never fabricated.
    #[test]
    fn unknown_artifact_returns_none() {
        let cas = store();
        let core = GraphCore::new();
        let view = core.analysis_snapshot();

        let resolver = CasEvidenceResolver::new(&view, cas);
        let span = EvidenceSpan::DocumentSpan {
            document_id: "ghost".to_string(),
            start: 0,
            end: 1,
        };

        assert_eq!(resolver.resolve(&span), None);
    }

    /// L21: a node that exists but carries no `blob_ref` property resolves to
    /// `None` — never fabricated.
    #[test]
    fn node_without_blob_ref_returns_none() {
        let cas = store();
        let core = GraphCore::new();
        core.add_node("doc1".into(), blob(json!({ "node_type": "Document" })));
        let view = core.analysis_snapshot();

        let resolver = CasEvidenceResolver::new(&view, cas);
        let span = EvidenceSpan::DocumentSpan {
            document_id: "doc1".to_string(),
            start: 0,
            end: 1,
        };

        assert_eq!(resolver.resolve(&span), None);
    }

    /// L21: a `blob_ref` that names a digest never actually stored in the CAS
    /// resolves to `None` — never fabricated content for a dangling reference.
    #[test]
    fn unknown_blob_digest_returns_none() {
        let cas = store();
        let core = GraphCore::new();
        core.add_node(
            "doc1".into(),
            blob(json!({ "node_type": "Document", "blob_ref": "does-not-exist" })),
        );
        let view = core.analysis_snapshot();

        let resolver = CasEvidenceResolver::new(&view, cas);
        let span = EvidenceSpan::DocumentSpan {
            document_id: "doc1".to_string(),
            start: 0,
            end: 1,
        };

        assert_eq!(resolver.resolve(&span), None);
    }
}
