#![cfg(feature = "evidence-graph")]
//! An externally authored current locus shape resolves through the engine without
//! environment-specific ontology or connection data.

use std::sync::Arc;

use eg_core::graph::GraphView;
use eg_epistemic::{evidence_citations, resolve_locus, BeliefGraph, EdgeKind};
use eg_modality::{EvidenceAddress, ResourceId};
use serde_json::json;

fn blob(value: serde_json::Value) -> Arc<Vec<u8>> {
    Arc::new(rmp_serde::to_vec_named(&value).expect("encodes"))
}

#[test]
fn externally_authored_locus_uses_the_universal_contract() {
    let evidence_id = "evidence-1";
    let claim_id = "claim-1";
    let mut view = GraphView::default();
    view.node_properties.insert(
        evidence_id.to_string(),
        blob(json!({
            "type": "Evidence",
            "confidence": 1.0,
            "evidence_locus": {
                "id": "eg:locus:0000000000000001",
                "subject": {
                    "kind": "occurrence",
                    "id": "eg:occurrence:0000000000000002"
                },
                "address": {
                    "kind": "page_region",
                    "page": 4,
                    "x": 72.0,
                    "y": 120.5,
                    "width": 400.0,
                    "height": 18.0
                },
                "policy_ref": "eg:policy:0000000000000003",
                "derivation_ref": "eg:derivation:0000000000000004"
            }
        })),
    );
    view.node_properties.insert(
        claim_id.to_string(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );
    view.edge_properties.insert(
        (evidence_id.to_string(), claim_id.to_string()),
        vec![blob(json!({ "relationship": "SUPPORTS" }))],
    );

    let graph = BeliefGraph::from_graph_view(&view);
    let locus = resolve_locus(&graph, evidence_id).expect("valid locus");
    assert!(matches!(locus.subject, ResourceId::Occurrence(_)));
    assert!(matches!(
        locus.address,
        EvidenceAddress::PageRegion { page: 4, .. }
    ));
    let citations = evidence_citations(&graph, claim_id);
    assert_eq!(citations.len(), 1);
    assert_eq!(citations[0].kind, EdgeKind::Supports);
    assert_eq!(citations[0].locus, locus);
}
