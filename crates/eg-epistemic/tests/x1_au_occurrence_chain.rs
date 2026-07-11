#![cfg(feature = "evidence-graph")]
//! Seam 2 (cross-repo: agent-utilities `AssetOccurrence` <-> epistemic-graph
//! evidence-graph, CONCEPT:AU-KG.identity.evidence-spine-convergence / EG-X1) —
//! proves the EG half of the round trip: the EXACT node/edge property shape
//! `agent_utilities.knowledge_graph.memory.media_store.MediaStore
//! .store_document_page_evidence` writes (see that method's docstring and
//! `tests/unit/knowledge_graph/test_media_store_evidence_spine.py` on the AU side)
//! decodes and resolves through the REAL `BeliefGraph::from_graph_view` +
//! `evidence_citations` path — the SAME engine-side resolver
//! `x1_evidence_chain.rs`'s hand-built fixture already exercises, this time keyed
//! off AU's ACTUAL property/edge keys and values (mirrored here 1:1, not
//! re-derived), so a drift between the two sides would show up as a failing
//! assertion in ONE of these two test suites.
//!
//! No new engine write endpoint exists or was added for this: AU writes these
//! nodes/edges over the plain `AddNode`/`AddEdge` RPCs (`client.nodes.add`/
//! `client.edges.add`) the rest of `MediaStore` already uses — the resolver is
//! entirely engine-side (`Method::ExplainEvidence`) and is reused UNCHANGED.
//!
//! ```text
//! sourceobject:doc-quarterly-report  --hasOccurrence-->  occurrence:<uuid>
//!                                                            |
//!                                                        hasBlob
//!                                                            v
//!                                                     blob:<digest>
//!
//! evidence:<uuid>  --extractedFrom-->  occurrence:<uuid>
//! evidence:<uuid>  --SUPPORTS-->       claim:revenue-q3
//! ```

use std::sync::Arc;

use eg_core::graph::GraphView;
use eg_epistemic::{evidence_citations, resolve_locus, BeliefGraph, EdgeKind, EvidenceCitation};
use eg_modality::EvidenceSpan;
use serde_json::json;

/// Encode a node/edge property object the same way the live engine does
/// (`rmp_serde::to_vec_named` over a `serde_json::Value`) — the exact format
/// `BeliefGraph::from_graph_view` decodes, and the exact wire encoding AU's
/// `client.nodes.add`/`client.edges.add` (`AddNode`/`AddEdge` — see
/// `epistemic_graph/client.py`'s `NodeClient.add`/`EdgeClient.add`) produce over
/// msgpack.
fn blob(v: serde_json::Value) -> Arc<Vec<u8>> {
    Arc::new(rmp_serde::to_vec_named(&v).expect("encodes"))
}

#[test]
fn au_produced_document_page_occurrence_resolves_through_the_one_eg_evidence_spine() {
    let document_id = "doc-quarterly-report";
    let occurrence_id = "occurrence:11111111111111111111111111111111";
    let blob_id = "blob:sha256:deadbeefcafe";
    let source_object_id = "sourceobject:doc-quarterly-report";
    let evidence_id = "evidence:22222222222222222222222222222222";
    let claim_id = "claim:revenue-q3";

    let mut view = GraphView::default();

    // 1) `:SourceObject` — `MediaStore.store_document_page_evidence`'s
    // `source_object_id = f"sourceobject:{document_id}"` write.
    view.node_properties.insert(
        source_object_id.to_string(),
        blob(json!({
            "type": "SourceObject",
            "document_id": document_id,
            "mime_type": "application/pdf",
            "created_at": "2026-07-11T00:00:00Z",
        })),
    );

    // 2) `:AssetOccurrence` — `MediaStore.store_media`'s existing write (AU-P1-4),
    // unchanged by Seam 2.
    view.node_properties.insert(
        occurrence_id.to_string(),
        blob(json!({
            "type": "AssetOccurrence",
            "name": "media document_page",
            "content_digest": "sha256:deadbeefcafe",
            "blob_id": blob_id,
            "media_type": "document_page",
            "mime_type": "application/pdf",
            "file_size_bytes": 4096,
            "source": "ingest-pipeline",
            "tenant": "",
            "owner": "",
            "event_time": "2026-07-11T00:00:00Z",
            "retention": "",
            "legal_hold": false,
            "created_at": "2026-07-11T00:00:00Z",
        })),
    );

    // 3) `:Blob` — `MediaStore.store_media`'s existing write, unchanged.
    view.node_properties.insert(
        blob_id.to_string(),
        blob(json!({
            "type": "Blob",
            "content_digest": "sha256:deadbeefcafe",
            "file_size_bytes": 4096,
            "created_at": "2026-07-11T00:00:00Z",
        })),
    );

    // 4) `:Evidence` — the Seam 2 write. `evidence_span` is the externally-tagged
    // `EvidenceSpan::PageBox` shape `MediaStore.store_document_page_evidence`
    // builds verbatim as a nested dict — encoded here as the SAME
    // `serde_json::json!` shape, not via the `EvidenceSpan` type, to prove the
    // AU-authored dict (not a Rust-side re-derivation of it) decodes correctly.
    let expected_locus = EvidenceSpan::PageBox {
        document_id: document_id.to_string(),
        page: 4,
        x: 72.0,
        y: 120.5,
        width: 400.0,
        height: 18.0,
    };
    view.node_properties.insert(
        evidence_id.to_string(),
        blob(json!({
            "type": "Evidence",
            "about": document_id,
            "confidence": 1.0,
            "evidence_span": {
                "PageBox": {
                    "document_id": document_id,
                    "page": 4,
                    "x": 72.0,
                    "y": 120.5,
                    "width": 400.0,
                    "height": 18.0,
                }
            },
            "occurrence_id": occurrence_id,
            "blob_ref": blob_id,
            "created_at": "2026-07-11T00:00:00Z",
        })),
    );

    // 5) The claim this evidence is cited FOR — minted by whatever epistemic
    // writer owns claim materialization (out of `MediaStore`'s scope; the test
    // stands one up directly, mirroring how `x1_evidence_chain.rs` does).
    view.node_properties.insert(
        claim_id.to_string(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );

    // Structural (non-epistemic) edges — `{"type": ...}`, NOT `relationship_type`,
    // so `BeliefGraph` correctly ignores them as identity/navigation links, exactly
    // like `hasBlob`/`attachedToMessage` elsewhere in `MediaStore`.
    view.edge_properties.insert(
        (source_object_id.to_string(), occurrence_id.to_string()),
        vec![blob(json!({ "type": "hasOccurrence" }))],
    );
    view.edge_properties.insert(
        (occurrence_id.to_string(), blob_id.to_string()),
        vec![blob(json!({ "type": "hasBlob" }))],
    );
    view.edge_properties.insert(
        (evidence_id.to_string(), occurrence_id.to_string()),
        vec![blob(json!({ "type": "extractedFrom" }))],
    );

    // The ONE epistemic edge: evidence --SUPPORTS--> claim, the SAME
    // `relationship_type: "SUPPORTS"` convention
    // `src/server/handlers/mining.rs::materialize_claim`'s `supports_edge` writes —
    // `MediaStore.store_document_page_evidence`'s `claim_id`-linking branch writes
    // this identical shape, no engine-side change needed to recognize it.
    view.edge_properties.insert(
        (evidence_id.to_string(), claim_id.to_string()),
        vec![blob(json!({ "relationship_type": "SUPPORTS" }))],
    );

    // Decode through the REAL engine path — the SAME `BeliefGraph::from_graph_view`
    // a live server's `Method::ExplainEvidence` handler
    // (`explain_evidence_wire` in `src/server/handlers/query.rs`) runs.
    let bg = BeliefGraph::from_graph_view(&view);

    // Direct locus lookup on the AU-produced evidence node.
    let direct = resolve_locus(&bg, evidence_id).expect("evidence node is in the view");
    assert_eq!(direct.locus, Some(expected_locus.clone()));
    assert_eq!(direct.occurrence_id.as_deref(), Some(occurrence_id));
    assert_eq!(direct.blob_ref.as_deref(), Some(blob_id));

    // The claim's resolved citation list: exactly one citation, naming the
    // AU-produced evidence node, a SUPPORTS relation, and the EXACT PageBox locus
    // + occurrence/blob identity — "AU's occurrence is now citable through the ONE
    // EG evidence spine."
    let citations = evidence_citations(&bg, claim_id);
    assert_eq!(
        citations,
        vec![EvidenceCitation {
            evidence_id: evidence_id.to_string(),
            kind: EdgeKind::Supports,
            locus: Some(expected_locus),
            occurrence_id: Some(occurrence_id.to_string()),
            blob_ref: Some(blob_id.to_string()),
        }]
    );

    // The structural identity-chain nodes (`SourceObject`/`AssetOccurrence`/`Blob`)
    // carry no locus of their own — only the `:Evidence` node does. Never
    // fabricated as citations themselves.
    assert!(resolve_locus(&bg, source_object_id)
        .map(|r| r.is_empty())
        .unwrap_or(true));
    assert!(resolve_locus(&bg, occurrence_id)
        .map(|r| r.is_empty())
        .unwrap_or(true));
}
