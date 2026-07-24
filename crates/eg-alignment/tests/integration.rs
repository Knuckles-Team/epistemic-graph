//! End-to-end integration test (CONCEPT:EG-P1-3): construct a document
//! (bytes -> pages -> spans) via `eg_document`'s trivial decoder, an image
//! artifact via `eg_image`'s `ModalityContract`, and an audio + video artifact
//! the same way; then build an `eg_alignment::AlignmentGraph` linking a document
//! span to an image region to a claim, and assert the alignment resolves.
//!
//! DEV-dependency only (see `Cargo.toml` docs) — none of this affects
//! `eg-alignment`'s own published dependency graph or the workspace's
//! default-build footprint.

use eg_alignment::{
    AlignmentGraph, AlignmentNode, AlignmentRelation, EvidenceResolver, ResolvedArtifact,
};
use eg_audio::AudioData;
use eg_document::{content_hash, DocumentDecoder, NativeTextDecoder};
use std::collections::HashMap;

/// A minimal `LexemeEncoder` for the fixture decodes below — the alignment test
/// only needs a document to decode successfully, not any particular lexeme scheme.
fn lexeme(value: &str) -> Option<String> {
    Some(format!("eg:lexeme:{}", content_hash(value.as_bytes())))
}
use eg_modality::{
    ArtifactId, DerivationId, EvidenceAddress, EvidenceLocus, EvidenceLocusId, ModalityContract,
    OpaqueRef, ResourceId,
};
use eg_video::VideoData;

fn r(namespace: &str, suffix: u8) -> OpaqueRef {
    OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
}

fn locus(address: EvidenceAddress, suffix: u8) -> EvidenceLocus {
    EvidenceLocus {
        id: EvidenceLocusId::from_token(&format!("00000000000000{suffix:02x}")).unwrap(),
        subject: ResourceId::Artifact(
            ArtifactId::from_token(&format!("00000000000001{suffix:02x}")).unwrap(),
        ),
        address,
        policy_ref: r("policy", suffix),
        derivation_ref: DerivationId::from_token(&format!("00000000000002{suffix:02x}")).unwrap(),
    }
}

/// Test-only `EvidenceResolver` fixture: this crate ships the `EvidenceResolver`
/// trait/`AlignmentGraph` seam only (no resolver implementation of its own — see
/// the crate docs), so an integration test outside the crate needs its own
/// minimal double to exercise `resolve_evidence`. The real, production resolver
/// is `CasEvidenceResolver` in the facade (`src/server/blob/cas_resolver.rs`),
/// which its own tests prove against real CAS-backed fixture content, including
/// this exact doc-span/image-region/claim cross-modal join.
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

    fn with_blob(mut self, subject: OpaqueRef, blob_ref: impl Into<String>) -> Self {
        self.0.insert(
            subject.clone(),
            ResolvedArtifact::Blob {
                subject_ref: subject.to_string(),
                blob_ref: blob_ref.into(),
                note: "resolved from the registered governed subject".to_string(),
            },
        );
        self
    }
}

impl EvidenceResolver for FixtureResolver {
    fn resolve(&self, locus: &EvidenceLocus) -> Option<ResolvedArtifact> {
        self.0.get(eg_alignment::subject_ref(locus)).cloned()
    }
}

#[test]
fn document_bytes_to_pages_to_spans() {
    let decoder = NativeTextDecoder;
    let doc = decoder
        .decode(b"the quick brown fox", &lexeme)
        .expect("plain text decodes");
    assert_eq!(doc.pages.len(), 1);
    let span = doc.first_span().expect("a decoded document has a span");
    assert_eq!((span.start, span.end), (0, "the quick brown fox".len()));
}

#[test]
fn image_artifact_via_the_modality_contract_trait() {
    let image = eg_image::ImageData::new(64, 48, "img-blob-1").with_regions(vec![
        eg_image::ImageRegion::labeled("subject", 4.0, 4.0, 20.0, 20.0),
    ]);
    let address = ModalityContract::evidence_address(&image).expect("image has a region");
    assert!(matches!(address, EvidenceAddress::ImageRegion { .. }));
}

#[test]
fn audio_artifact_via_the_modality_contract_trait() {
    let audio = AudioData::new(16_000, 3000, "audio-blob-1")
        .with_segments(vec![eg_audio::AudioSegment::labeled("speech", 0, 1500)]);
    let address = ModalityContract::evidence_address(&audio).expect("audio has a segment");
    assert!(matches!(address, EvidenceAddress::AudioRange { .. }));
}

#[test]
fn video_artifact_via_the_modality_contract_trait() {
    let video = VideoData::new(9000, "video-blob-1")
        .with_shots(vec![eg_video::VideoShot::labeled("scene-1", 0, 3000)]);
    let address = ModalityContract::evidence_address(&video).expect("video has a shot");
    assert!(matches!(address, EvidenceAddress::VideoTimeRange { .. }));
}

/// The load-bearing test: a document span aligns to an image region, which
/// supports a claim. Both evidence nodes resolve through an `EvidenceResolver`,
/// and the alignment graph proves the doc-span -> claim connectivity through the
/// image region hop.
#[test]
fn alignment_links_a_document_span_to_an_image_region_to_a_claim_and_resolves() {
    let decoder = NativeTextDecoder;
    let doc = decoder
        .decode(b"a fox jumps over the fence", &lexeme)
        .expect("plain text decodes");
    let doc_locus = locus(
        ModalityContract::evidence_address(&doc).expect("document has a span"),
        1,
    );

    let image = eg_image::ImageData::new(100, 100, "img-blob-1").with_regions(vec![
        eg_image::ImageRegion::labeled("fox", 10.0, 10.0, 40.0, 30.0),
    ]);
    let image_locus = locus(
        ModalityContract::evidence_address(&image).expect("image has a region"),
        2,
    );

    let mut graph = AlignmentGraph::new();
    let doc_subject = doc_locus.subject.opaque().clone();
    let image_subject = image_locus.subject.opaque().clone();
    let doc_node = graph.add_node(AlignmentNode::Evidence(doc_locus));
    let image_node = graph.add_node(AlignmentNode::Evidence(image_locus));
    let claim_node = graph.add_node(AlignmentNode::Claim {
        claim_id: "claim-fox-in-fence".to_string(),
    });

    graph.add_edge(doc_node, image_node, AlignmentRelation::CoOccursWith);
    graph.add_edge(image_node, claim_node, AlignmentRelation::SupportsClaim);

    // Connectivity: the doc span aligns (transitively) to the claim through the
    // image region, but not the reverse (edges are directed).
    assert!(graph.path_exists(doc_node, claim_node));
    assert!(!graph.path_exists(claim_node, doc_node));

    // Resolution: both evidence nodes resolve to a real artifact through the
    // evidence-resolver seam; the claim node (not an `Evidence` node) does not.
    let resolver = FixtureResolver::new()
        .with_text(doc_subject.clone(), "a fox jumps over the fence")
        .with_blob(image_subject.clone(), "img-blob-1");

    let resolved_doc = graph
        .resolve_evidence(doc_node, &resolver)
        .expect("the document span resolves");
    assert!(matches!(
        resolved_doc,
        eg_alignment::ResolvedArtifact::Text { ref subject_ref, .. }
            if subject_ref == doc_subject.as_str()
    ));

    let resolved_image = graph
        .resolve_evidence(image_node, &resolver)
        .expect("the image region resolves");
    assert!(matches!(
        resolved_image,
        eg_alignment::ResolvedArtifact::Blob { ref subject_ref, .. }
            if subject_ref == image_subject.as_str()
    ));

    assert_eq!(graph.resolve_evidence(claim_node, &resolver), None);
}
