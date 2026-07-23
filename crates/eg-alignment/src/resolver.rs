//! Resolution seam for governed evidence loci.
//!
//! The locus subject is the only lookup key. Address coordinates select content
//! within that subject; raw paths, endpoints, names, and upstream identifiers are
//! never accepted by this layer.

use std::collections::HashMap;

use eg_modality::{EvidenceLocus, OpaqueRef};

#[derive(Clone, Debug, PartialEq)]
pub enum ResolvedArtifact {
    Text {
        subject_ref: String,
        excerpt: String,
    },
    Blob {
        subject_ref: String,
        blob_ref: String,
        note: String,
    },
}

pub trait EvidenceResolver {
    fn resolve(&self, locus: &EvidenceLocus) -> Option<ResolvedArtifact>;
}

/// The opaque subject a locus addresses.
pub fn subject_ref(locus: &EvidenceLocus) -> &OpaqueRef {
    locus.subject.opaque()
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryResolver {
    texts: HashMap<OpaqueRef, String>,
    blobs: HashMap<OpaqueRef, String>,
}

impl InMemoryResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_text(mut self, subject: OpaqueRef, excerpt: impl Into<String>) -> Self {
        self.texts.insert(subject, excerpt.into());
        self
    }

    pub fn with_blob(mut self, subject: OpaqueRef, blob_ref: impl Into<String>) -> Self {
        self.blobs.insert(subject, blob_ref.into());
        self
    }
}

impl EvidenceResolver for InMemoryResolver {
    fn resolve(&self, locus: &EvidenceLocus) -> Option<ResolvedArtifact> {
        let subject = subject_ref(locus);
        if let Some(excerpt) = self.texts.get(subject) {
            return Some(ResolvedArtifact::Text {
                subject_ref: subject.to_string(),
                excerpt: excerpt.clone(),
            });
        }
        self.blobs
            .get(subject)
            .map(|blob_ref| ResolvedArtifact::Blob {
                subject_ref: subject.to_string(),
                blob_ref: blob_ref.clone(),
                note: "resolved from the registered governed subject".to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_modality::{ArtifactId, DerivationId, EvidenceAddress, EvidenceLocusId, ResourceId};

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

    #[test]
    fn subject_projection_is_uniform_across_addresses() {
        let text = locus(EvidenceAddress::CharacterRange { start: 0, end: 1 });
        let image = locus(EvidenceAddress::ImageRegion {
            x: 0.0,
            y: 0.0,
            width: 1.0,
            height: 1.0,
        });
        assert_eq!(subject_ref(&text), subject_ref(&image));
    }

    #[test]
    fn resolver_uses_only_the_opaque_subject() {
        let evidence = locus(EvidenceAddress::CharacterRange { start: 0, end: 5 });
        let resolver =
            InMemoryResolver::new().with_text(subject_ref(&evidence).clone(), "hello world");
        assert_eq!(
            resolver.resolve(&evidence),
            Some(ResolvedArtifact::Text {
                subject_ref: subject_ref(&evidence).to_string(),
                excerpt: "hello world".to_string(),
            })
        );
    }

    #[test]
    fn resolver_returns_none_for_an_unregistered_subject() {
        let evidence = locus(EvidenceAddress::AudioRange {
            start_ms: 0,
            end_ms: 1,
        });
        assert_eq!(InMemoryResolver::new().resolve(&evidence), None);
    }
}
