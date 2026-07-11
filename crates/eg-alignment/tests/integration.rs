//! End-to-end integration test (CONCEPT:EG-P1-3): construct a document
//! (bytes -> pages -> spans) via `eg_document`'s trivial decoder, an image
//! artifact via `eg_image`'s `ModalityContract`, and an audio + video artifact
//! the same way; then build an `eg_alignment::AlignmentGraph` linking a document
//! span to an image region to a claim, and assert the alignment resolves.
//!
//! DEV-dependency only (see `Cargo.toml` docs) — none of this affects
//! `eg-alignment`'s own published dependency graph or the workspace's
//! default-build footprint.

use eg_alignment::{AlignmentGraph, AlignmentNode, AlignmentRelation, InMemoryResolver};
use eg_audio::AudioData;
use eg_document::{DocumentDecoder, PlainTextDecoder};
use eg_modality::ModalityContract;
use eg_video::VideoData;

#[test]
fn document_bytes_to_pages_to_spans() {
    let decoder = PlainTextDecoder;
    let doc = decoder
        .decode(b"the quick brown fox")
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
    let evidence = ModalityContract::evidence(&image, "img-1").expect("image has a region");
    assert!(matches!(
        evidence,
        eg_modality::EvidenceSpan::ImageRegion { ref image_id, .. } if image_id == "img-1"
    ));
}

#[test]
fn audio_artifact_via_the_modality_contract_trait() {
    let audio = AudioData::new(16_000, 3000, "audio-blob-1")
        .with_segments(vec![eg_audio::AudioSegment::labeled("speech", 0, 1500)]);
    let evidence = ModalityContract::evidence(&audio, "audio-1").expect("audio has a segment");
    assert!(matches!(
        evidence,
        eg_modality::EvidenceSpan::AudioSegment { ref audio_id, .. } if audio_id == "audio-1"
    ));
}

#[test]
fn video_artifact_via_the_modality_contract_trait() {
    let video = VideoData::new(9000, "video-blob-1")
        .with_shots(vec![eg_video::VideoShot::labeled("scene-1", 0, 3000)]);
    let evidence = ModalityContract::evidence(&video, "video-1").expect("video has a shot");
    assert!(matches!(
        evidence,
        eg_modality::EvidenceSpan::VideoShot { ref video_id, .. } if video_id == "video-1"
    ));
}

/// The load-bearing test: a document span aligns to an image region, which
/// supports a claim. Both evidence nodes resolve through an `EvidenceResolver`,
/// and the alignment graph proves the doc-span -> claim connectivity through the
/// image region hop.
#[test]
fn alignment_links_a_document_span_to_an_image_region_to_a_claim_and_resolves() {
    let decoder = PlainTextDecoder;
    let doc = decoder
        .decode(b"a fox jumps over the fence")
        .expect("plain text decodes");
    let doc_span = ModalityContract::evidence(&doc, "doc-1").expect("document has a span");

    let image = eg_image::ImageData::new(100, 100, "img-blob-1").with_regions(vec![
        eg_image::ImageRegion::labeled("fox", 10.0, 10.0, 40.0, 30.0),
    ]);
    let image_region = ModalityContract::evidence(&image, "img-1").expect("image has a region");

    let mut graph = AlignmentGraph::new();
    let doc_node = graph.add_node(AlignmentNode::Evidence(doc_span));
    let image_node = graph.add_node(AlignmentNode::Evidence(image_region));
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
    let resolver = InMemoryResolver::new()
        .with_text("doc-1", "a fox jumps over the fence")
        .with_blob("img-1", "img-blob-1");

    let resolved_doc = graph
        .resolve_evidence(doc_node, &resolver)
        .expect("the document span resolves");
    assert!(matches!(
        resolved_doc,
        eg_alignment::ResolvedArtifact::Text { ref artifact_id, .. } if artifact_id == "doc-1"
    ));

    let resolved_image = graph
        .resolve_evidence(image_node, &resolver)
        .expect("the image region resolves");
    assert!(matches!(
        resolved_image,
        eg_alignment::ResolvedArtifact::Blob { ref artifact_id, .. } if artifact_id == "img-1"
    ));

    assert_eq!(graph.resolve_evidence(claim_node, &resolver), None);
}
