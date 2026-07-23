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
}
