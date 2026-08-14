//! Resolution seam for governed evidence loci.
//!
//! The locus subject is the only lookup key. Address coordinates select content
//! within that subject; raw paths, endpoints, names, and upstream identifiers are
//! never accepted by this layer.

use eg_modality::{EvidenceLocus, OpaqueRef};

/// Why a locus's exact bounded value could not be produced (GOC-05 gate 3: an
/// address that promises a region/interval must not silently degrade to a
/// blob-only "success"). Names match the lane doc's resolver reason-code
/// catalog (`GOC-05-universal-artifact-evidence-ontology.md` "Address and
/// resolver contract").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnresolvedReason {
    /// No decoded/normalized rendition exists yet for this occurrence.
    MissingRendition,
    /// The address kind has a defined exact-resolution contract, but no
    /// provider capable of decoding it is registered in this build (e.g. no
    /// image/audio/video/table region decoder — GOC-06/GOC-07's job).
    CodecUnavailable,
    /// Policy denied resolving this locus for the calling scope.
    PolicyDenied,
    /// The referenced bytes could not be read back intact.
    CorruptBytes,
    /// The address's numeric coordinates fall outside the resolved content.
    OutOfRange,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResolvedArtifact {
    Text {
        subject_ref: String,
        excerpt: String,
    },
    /// Exact resolution success only when the address itself is an
    /// intentionally opaque reference (e.g. `RowVersion`/`TraceSpan` — a
    /// versioned pointer a downstream store resolves by key, not a byte range
    /// this resolver decodes). An address that names a byte range/region/
    /// interval must resolve to `Text`/a typed exact result or `Unresolved` —
    /// never silently degrade to this variant (GOC-05 gate 3).
    Blob {
        subject_ref: String,
        blob_ref: String,
        note: String,
    },
    /// The typed, honest "could not produce an exact result" outcome. Never
    /// reported as evidence success by a caller — see `UnresolvedReason`.
    Unresolved {
        subject_ref: String,
        reason: UnresolvedReason,
    },
}

pub trait EvidenceResolver {
    fn resolve(&self, locus: &EvidenceLocus) -> Option<ResolvedArtifact>;
}

/// The opaque subject a locus addresses.
pub fn subject_ref(locus: &EvidenceLocus) -> &OpaqueRef {
    locus.subject.opaque()
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_modality::{ArtifactId, DerivationId, EvidenceAddress, EvidenceLocusId, ResourceId};
    use std::collections::HashMap;

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

    /// Test-only `EvidenceResolver`: exercises the trait/subject-projection
    /// mechanics this module owns, without a resolution backend. The real
    /// backend is `CasEvidenceResolver` in the facade
    /// (`src/server/blob/cas_resolver.rs`), which this leaf crate cannot link
    /// (see the crate docs on why) — its own tests prove real CAS-backed
    /// resolution, including a cross-modal `AlignmentGraph` join.
    struct FixtureResolver(HashMap<OpaqueRef, ResolvedArtifact>);

    impl FixtureResolver {
        fn new() -> Self {
            Self(HashMap::new())
        }

        fn with_text(mut self, subject: OpaqueRef, excerpt: impl Into<String>) -> Self {
            self.0.insert(
                subject.clone(),
                ResolvedArtifact::Text {
                    subject_ref: subject.to_string(),
                    excerpt: excerpt.into(),
                },
            );
            self
        }
    }

    impl EvidenceResolver for FixtureResolver {
        fn resolve(&self, locus: &EvidenceLocus) -> Option<ResolvedArtifact> {
            self.0.get(subject_ref(locus)).cloned()
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
            FixtureResolver::new().with_text(subject_ref(&evidence).clone(), "hello world");
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
        assert_eq!(FixtureResolver::new().resolve(&evidence), None);
    }
}
