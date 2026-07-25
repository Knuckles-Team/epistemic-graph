//! CAS-backed resolver for governed evidence loci.

use std::sync::Arc;

use eg_alignment::{subject_ref, EvidenceResolver, ResolvedArtifact};
use eg_core::graph::GraphView;
use eg_modality::{EvidenceAddress, EvidenceLocus};

use super::store::ChunkStore;
use super::stream::stream_blob_get;

/// Resolves a locus through its opaque subject node and that node's CAS digest.
pub struct CasEvidenceResolver<'a> {
    view: &'a GraphView,
    store: Arc<dyn ChunkStore>,
}

impl<'a> CasEvidenceResolver<'a> {
    pub fn new(view: &'a GraphView, store: Arc<dyn ChunkStore>) -> Self {
        Self { view, store }
    }

    fn blob_ref_for(&self, subject: &str) -> Option<String> {
        self.view
            .node_row_object(subject)?
            .get("blob_ref")?
            .as_str()
            .map(str::to_string)
    }

    fn fetch_bytes(&self, digest: &str) -> Option<Vec<u8>> {
        let mut bytes = Vec::new();
        stream_blob_get(self.store.as_ref(), digest, &mut bytes).ok()?;
        Some(bytes)
    }
}

impl EvidenceResolver for CasEvidenceResolver<'_> {
    fn resolve(&self, locus: &EvidenceLocus) -> Option<ResolvedArtifact> {
        locus.validate().ok()?;
        let subject = subject_ref(locus).as_str();
        let blob_ref = self.blob_ref_for(subject)?;

        match &locus.address {
            EvidenceAddress::CharacterRange { start, end } => {
                let bytes = self.fetch_bytes(&blob_ref)?;
                let text = String::from_utf8_lossy(&bytes);
                let char_len = text.chars().count();
                let start = usize::try_from(*start).ok()?.min(char_len);
                let end = usize::try_from(*end).ok()?.max(start).min(char_len);
                Some(ResolvedArtifact::Text {
                    subject_ref: subject.to_string(),
                    excerpt: text.chars().skip(start).take(end - start).collect(),
                })
            }
            EvidenceAddress::CodeSymbol {
                start_line,
                end_line,
                ..
            } => {
                let bytes = self.fetch_bytes(&blob_ref)?;
                let text = String::from_utf8_lossy(&bytes);
                let lines: Vec<&str> = text.lines().collect();
                let start = (*start_line as usize).min(lines.len());
                let end = (*end_line as usize).max(start).min(lines.len());
                Some(ResolvedArtifact::Text {
                    subject_ref: subject.to_string(),
                    excerpt: lines[start..end].join("\n"),
                })
            }
            _ => Some(ResolvedArtifact::Blob {
                subject_ref: subject.to_string(),
                blob_ref,
                note: "resolved from the engine content-addressed store".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::blob::store::RedbChunkStore;
    use crate::server::blob::stream::stream_blob_put;
    use eg_alignment::{AlignmentGraph, AlignmentNode, AlignmentRelation};
    use eg_core::graph::GraphCore;
    use eg_modality::{ArtifactId, DerivationId, EvidenceLocusId, OpaqueRef, ResourceId};
    use serde_json::json;

    fn blob(value: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&value).unwrap()
    }

    fn store() -> Arc<dyn ChunkStore> {
        Arc::new(RedbChunkStore::open_temp().unwrap())
    }

    fn r(namespace: &str, suffix: u8) -> OpaqueRef {
        OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
    }

    fn locus(address: EvidenceAddress) -> EvidenceLocus {
        EvidenceLocus {
            id: EvidenceLocusId::from_token("0000000000000001").unwrap(),
            subject: ResourceId::Artifact(ArtifactId::from_token("0000000000000002").unwrap()),
            address,
            policy_ref: r("policy", 3),
            derivation_ref: DerivationId::from_token("0000000000000004").unwrap(),
        }
    }

    fn resolver_fixture(content: &[u8]) -> (CasEvidenceResolver<'static>, EvidenceLocus) {
        let cas = store();
        let committed = stream_blob_put(cas.as_ref(), content, 0).unwrap();
        let evidence = locus(EvidenceAddress::CharacterRange { start: 0, end: 5 });
        let subject = subject_ref(&evidence).to_string();
        let core = Box::leak(Box::new(GraphCore::new()));
        core.add_node(
            subject,
            blob(json!({ "node_type": "Document", "blob_ref": committed.digest })),
        );
        let view = Box::leak(Box::new(core.analysis_snapshot()));
        (CasEvidenceResolver::new(view, cas), evidence)
    }

    #[test]
    fn resolves_character_range_from_real_cas_bytes() {
        let (resolver, evidence) = resolver_fixture(b"hello world");
        assert_eq!(
            resolver.resolve(&evidence),
            Some(ResolvedArtifact::Text {
                subject_ref: subject_ref(&evidence).to_string(),
                excerpt: "hello".to_string(),
            })
        );
    }

    #[test]
    fn resolves_code_lines_from_real_cas_bytes() {
        let (resolver, mut evidence) = resolver_fixture(b"line0\nline1\nline2");
        evidence.address = EvidenceAddress::CodeSymbol {
            revision_ref: r("revision", 5),
            symbol_ref: r("symbol", 6),
            start_line: 1,
            end_line: 3,
        };
        assert!(matches!(
            resolver.resolve(&evidence),
            Some(ResolvedArtifact::Text { excerpt, .. }) if excerpt == "line1\nline2"
        ));
    }

    #[test]
    fn non_text_address_returns_the_real_digest_reference() {
        let (resolver, mut evidence) = resolver_fixture(&[0xff; 64]);
        evidence.address = EvidenceAddress::ImageRegion {
            x: 0.0,
            y: 0.0,
            width: 10.0,
            height: 10.0,
        };
        assert!(matches!(
            resolver.resolve(&evidence),
            Some(ResolvedArtifact::Blob { .. })
        ));
    }

    #[test]
    fn unknown_subject_returns_none() {
        let cas = store();
        let core = GraphCore::new();
        let view = core.analysis_snapshot();
        let resolver = CasEvidenceResolver::new(&view, cas);
        let evidence = locus(EvidenceAddress::CharacterRange { start: 0, end: 1 });
        assert_eq!(resolver.resolve(&evidence), None);
    }

    /// W4.7 (M3) — the cross-modal join proof: an `eg_alignment::AlignmentGraph`
    /// links a document span to an image region (`CoOccursWith`) which supports a
    /// claim (`SupportsClaim`), exactly the shape `eg-alignment` exists for. Both
    /// evidence hops are backed by DISTINCT governed subjects whose `blob_ref`
    /// points at REAL bytes committed to a fixture CAS (`RedbChunkStore::open_temp`,
    /// no live services) — resolution reads those bytes back through the SAME
    /// `CasEvidenceResolver` production code path `ExplainEvidence` uses, not a
    /// fabricated excerpt/digest. This is the one place `eg-alignment`'s graph and
    /// the real CAS resolver are exercised together.
    #[test]
    fn alignment_graph_cross_modal_join_resolves_through_cas() {
        let cas = store();
        let doc_committed =
            stream_blob_put(cas.as_ref(), b"a fox jumps over the fence".as_slice(), 0).unwrap();
        let image_committed = stream_blob_put(cas.as_ref(), [0xabu8; 128].as_slice(), 0).unwrap();

        let doc_locus = locus(EvidenceAddress::CharacterRange { start: 0, end: 5 });
        let mut image_locus = locus(EvidenceAddress::ImageRegion {
            x: 10.0,
            y: 10.0,
            width: 40.0,
            height: 30.0,
        });
        // A distinct governed subject from the doc locus's (`locus()` fixes
        // ArtifactId "...02") so the two evidence hops resolve independent CAS
        // content — a real cross-modal join, not one node twice.
        image_locus.subject =
            ResourceId::Artifact(ArtifactId::from_token("0000000000000003").unwrap());

        let doc_subject = subject_ref(&doc_locus).to_string();
        let image_subject = subject_ref(&image_locus).to_string();

        let core = GraphCore::new();
        core.add_node(
            doc_subject.clone(),
            blob(json!({ "node_type": "Document", "blob_ref": doc_committed.digest })),
        );
        core.add_node(
            image_subject.clone(),
            blob(json!({ "node_type": "Image", "blob_ref": image_committed.digest })),
        );
        let view = core.analysis_snapshot();
        let resolver = CasEvidenceResolver::new(&view, cas);

        let mut graph = AlignmentGraph::new();
        let doc_node = graph.add_node(AlignmentNode::Evidence(doc_locus));
        let image_node = graph.add_node(AlignmentNode::Evidence(image_locus));
        let claim_node = graph.add_node(AlignmentNode::Claim {
            claim_id: "claim-fox-in-fence".to_string(),
        });
        graph.add_edge(doc_node, image_node, AlignmentRelation::CoOccursWith);
        graph.add_edge(image_node, claim_node, AlignmentRelation::SupportsClaim);

        // The cross-modal query: a doc span reaches a claim through an image
        // region hop (directed — the reverse does not hold).
        assert!(graph.path_exists(doc_node, claim_node));
        assert!(
            !graph.path_exists(claim_node, doc_node),
            "edges are directed"
        );

        // Both hops resolve through the REAL CAS resolver: real bytes read back
        // off the fixture store, not a fabricated excerpt/digest.
        assert_eq!(
            graph.resolve_evidence(doc_node, &resolver),
            Some(ResolvedArtifact::Text {
                subject_ref: doc_subject,
                excerpt: "a fox".to_string(),
            }),
            "the doc span resolves to the real CAS excerpt"
        );
        assert_eq!(
            graph.resolve_evidence(image_node, &resolver),
            Some(ResolvedArtifact::Blob {
                subject_ref: image_subject,
                blob_ref: image_committed.digest,
                note: "resolved from the engine content-addressed store".to_string(),
            }),
            "the image region resolves to the real CAS digest"
        );

        // The claim node has no located span — never resolved.
        assert_eq!(graph.resolve_evidence(claim_node, &resolver), None);
    }
}
