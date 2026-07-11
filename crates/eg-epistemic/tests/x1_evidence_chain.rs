#![cfg(feature = "evidence-graph")]
//! X-1 end-to-end integration test (CONCEPT:EG-X1) — the task's acceptance test.
//!
//! Builds a REAL six-level evidence chain over an actual `GraphView` snapshot
//! (real msgpack-encoded property blobs, decoded through the exact
//! `BeliefGraph::from_graph_view` path a live engine uses — not a `from_parts`
//! fixture):
//!
//! ```text
//! SourceObject("src-pdf-1")
//!   -> AssetOccurrence("occ-page-4")
//!     -> Blob("blob-sha256-abc")
//!       -> Span (the locus itself: EvidenceSpan::PageBox{page:4,x,y,w,h})
//!         -> Observation("obs-1", an :Evidence-role node carrying that Span
//!            as its `evidence_span` property, plus `occurrence_id`/`blob_ref`)
//!           --SUPPORTS--> Claim("claim-revenue-1")
//! ```
//!
//! Then resolves `claim-revenue-1`'s evidence to its citation list and asserts
//! the EXACT PDF page+box locus, occurrence id, and blob ref come back —
//! nothing fabricated, nothing lost in the decode round-trip.

use std::sync::Arc;

use eg_core::graph::GraphView;
use eg_epistemic::{evidence_citations, resolve_locus, BeliefGraph, EdgeKind, EvidenceCitation};
use eg_modality::EvidenceSpan;
use serde_json::json;

/// Encode a node/edge property object the same way the live engine does
/// (`rmp_serde::to_vec_named` over a `serde_json::Value`) — the exact format
/// `BeliefGraph::from_graph_view` decodes.
fn blob(v: serde_json::Value) -> Arc<Vec<u8>> {
    Arc::new(rmp_serde::to_vec_named(&v).expect("encodes"))
}

#[test]
fn source_object_to_asset_occurrence_to_blob_to_span_to_observation_to_claim_resolves_the_exact_locus(
) {
    let mut view = GraphView::default();

    // Level 1: SourceObject — the uploaded PDF file itself.
    view.node_properties.insert(
        "src-pdf-1".to_string(),
        blob(json!({ "type": "SourceObject", "kind": "pdf", "filename": "quarterly-report.pdf" })),
    );

    // Level 2: AssetOccurrence — "page 4 of this PDF's current revision".
    view.node_properties.insert(
        "occ-page-4".to_string(),
        blob(json!({
            "type": "AssetOccurrence",
            "source_object_id": "src-pdf-1",
            "page": 4,
        })),
    );

    // Level 3: Blob — the content-addressed rendition that occurrence resolves to.
    view.node_properties.insert(
        "blob-sha256-abc".to_string(),
        blob(json!({ "type": "Blob", "digest": "sha256:abc123" })),
    );

    // Level 4/5: Observation — the `:Evidence`-role node. Carries the located
    // `Span` (a `PageBox` locus: "page 4, box (x=72.0, y=120.5, w=400.0, h=18.0)")
    // AND the identity chain it was extracted from (`occurrence_id`/`blob_ref`).
    let locus = EvidenceSpan::PageBox {
        document_id: "src-pdf-1".to_string(),
        page: 4,
        x: 72.0,
        y: 120.5,
        width: 400.0,
        height: 18.0,
    };
    view.node_properties.insert(
        "obs-1".to_string(),
        blob(json!({
            "type": "Observation",
            "confidence": 0.93,
            "evidence_span": locus,
            "occurrence_id": "occ-page-4",
            "blob_ref": "blob-sha256-abc",
        })),
    );

    // Level 6: Claim — "Q3 revenue grew 12% YoY", asserted from the observation.
    view.node_properties.insert(
        "claim-revenue-1".to_string(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );

    // The one epistemic edge: obs-1 --SUPPORTS--> claim-revenue-1. The
    // SourceObject/AssetOccurrence/Blob/Span links above are identity/extraction
    // references (plain properties), not epistemic support/contradiction edges —
    // exactly like `about`/`family`/`provenance` on the existing `:Claim`/
    // `:Evidence` convention (`src/server/handlers/mining.rs::materialize_claim`).
    view.edge_properties.insert(
        ("obs-1".to_string(), "claim-revenue-1".to_string()),
        vec![blob(json!({ "relationship_type": "SUPPORTS" }))],
    );

    // Decode through the REAL engine path.
    let bg = BeliefGraph::from_graph_view(&view);

    // The claim's own prior was read correctly (proves the SAME decode pass that
    // reads `confidence` also ran for this graph).
    assert_eq!(bg.priors.get("claim-revenue-1"), Some(&0.5));

    // Direct locus lookup on the observation node itself.
    let direct = resolve_locus(&bg, "obs-1").expect("obs-1 is in the view");
    assert_eq!(direct.locus, Some(locus.clone()));
    assert_eq!(direct.occurrence_id.as_deref(), Some("occ-page-4"));
    assert_eq!(direct.blob_ref.as_deref(), Some("blob-sha256-abc"));

    // The claim's resolved citation list: exactly one citation, naming obs-1,
    // a SUPPORTS relation, and the EXACT PageBox locus + identity chain.
    let citations = evidence_citations(&bg, "claim-revenue-1");
    assert_eq!(
        citations,
        vec![EvidenceCitation {
            evidence_id: "obs-1".to_string(),
            kind: EdgeKind::Supports,
            locus: Some(locus),
            occurrence_id: Some("occ-page-4".to_string()),
            blob_ref: Some("blob-sha256-abc".to_string()),
        }]
    );

    // A locus for an unrelated/unknown claim is genuinely empty, never fabricated.
    assert!(evidence_citations(&bg, "no-such-claim").is_empty());
}
