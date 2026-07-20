#![cfg(feature = "evidence-graph")]
//! End-to-end governed evidence-locus decoding and citation resolution.

use std::sync::Arc;

use eg_core::graph::GraphView;
use eg_epistemic::{evidence_citations, resolve_locus, BeliefGraph, EdgeKind, EvidenceCitation};
use eg_modality::{
    DerivationId, EvidenceAddress, EvidenceLocus, EvidenceLocusId, OccurrenceId, OpaqueRef,
    ResourceId,
};
use serde_json::json;

fn blob(value: serde_json::Value) -> Arc<Vec<u8>> {
    Arc::new(rmp_serde::to_vec_named(&value).expect("encodes"))
}

fn r(namespace: &str, suffix: u8) -> OpaqueRef {
    OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
}

fn locus() -> EvidenceLocus {
    EvidenceLocus {
        id: EvidenceLocusId::from_token("0000000000000001").unwrap(),
        subject: ResourceId::Occurrence(OccurrenceId::from_token("0000000000000002").unwrap()),
        address: EvidenceAddress::PageRegion {
            page: 4,
            x: 72.0,
            y: 120.5,
            width: 400.0,
            height: 18.0,
        },
        policy_ref: r("policy", 3),
        derivation_ref: DerivationId::from_token("0000000000000004").unwrap(),
    }
}

#[test]
fn graph_view_resolves_the_exact_governed_locus() {
    let expected = locus();
    let mut view = GraphView::default();
    view.node_properties.insert(
        "evidence-1".to_string(),
        blob(json!({
            "type": "Evidence",
            "confidence": 0.93,
            "evidence_locus": serde_json::to_value(&expected).unwrap()
        })),
    );
    view.node_properties.insert(
        "claim-1".to_string(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );
    view.edge_properties.insert(
        ("evidence-1".to_string(), "claim-1".to_string()),
        vec![blob(json!({ "relationship": "SUPPORTS" }))],
    );

    let graph = BeliefGraph::from_graph_view(&view);
    assert_eq!(resolve_locus(&graph, "evidence-1"), Some(expected.clone()));
    assert_eq!(
        evidence_citations(&graph, "claim-1"),
        vec![EvidenceCitation {
            evidence_id: "evidence-1".to_string(),
            kind: EdgeKind::Supports,
            locus: expected,
        }]
    );
    assert!(evidence_citations(&graph, "absent").is_empty());
}
