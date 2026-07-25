#![cfg(feature = "evidence-graph")]
//! Extends `x1_au_occurrence_chain.rs`'s "externally authored locus resolves
//! through the engine" proof from `PageRegion` to the five NON-media-decode
//! `EvidenceAddress` kinds W3.3 wired real AU producers for: `TableCellRange`
//! (table-cell), `CodeSymbol` (code-symbol-span), `RowVersion` (row-level),
//! `MetricWindow` and `TraceSpan` (metric/trace-span — the task's own
//! vocabulary). `ImageRegion`/`AudioRange`/`VideoTimeRange`/`FrameRange`
//! (OCR/whisper/video-decode-dependent) are W4.6 sidecar territory and stay
//! out of this file's scope.
//!
//! Each fixture below is the EXACT shape `MediaStore._governed_locus`
//! (agent-utilities `knowledge_graph/memory/media_store.py`) now produces for
//! its corresponding `store_<kind>_evidence` producer — an opaque-hex `id`/
//! `subject`/`policy_ref`/`derivation_ref` envelope plus an internally-tagged
//! `address`, decoded by the SAME engine-side code `x1_au_occurrence_chain.rs`
//! exercises (`BeliefGraph::from_graph_view`, `resolve_locus`,
//! `evidence_citations` — the function `Method::ExplainEvidence`'s handler
//! calls).

use std::collections::BTreeSet;
use std::sync::Arc;

use eg_core::graph::GraphView;
use eg_epistemic::{evidence_citations, resolve_locus, BeliefGraph, EdgeKind};
use eg_modality::EvidenceAddress;
use serde_json::{json, Value};

fn blob(value: Value) -> Arc<Vec<u8>> {
    Arc::new(rmp_serde::to_vec_named(&value).expect("encodes"))
}

/// A 16-lowercase-hex-char token — the same fixed-width convention
/// `x1_evidence_chain.rs`/`cas_resolver.rs`'s own test helpers use, so every
/// `id`/`subject`/`policy_ref`/`derivation_ref`/opaque-address-field below is
/// a valid `eg_modality::OpaqueRef` token.
fn hex(n: u8) -> String {
    format!("00000000000000{n:02x}")
}

/// The exact `eg_modality::EvidenceLocus` wire shape `MediaStore._governed_locus`
/// builds: `id`/`subject`/`policy_ref`/`derivation_ref` derived from one
/// evidence-node token, `subject` from one occurrence token, `address`
/// internally tagged with `kind`.
fn au_locus(evidence_token: u8, occurrence_token: u8, address: Value) -> Value {
    let ev = hex(evidence_token);
    let occ = hex(occurrence_token);
    json!({
        "type": "Evidence",
        "confidence": 0.9,
        "evidence_locus": {
            "id": format!("eg:locus:{ev}"),
            "subject": { "kind": "occurrence", "id": format!("eg:occurrence:{occ}") },
            "address": address,
            "policy_ref": format!("eg:policy:{ev}"),
            "derivation_ref": format!("eg:derivation:{ev}"),
        }
    })
}

#[test]
fn au_authored_non_media_locus_kinds_resolve_and_are_cited() {
    let claim_id = "claim-1";
    let mut view = GraphView::default();
    view.node_properties.insert(
        claim_id.to_string(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );

    let table = au_locus(
        0x11,
        0x12,
        json!({
            "kind": "table_cell_range",
            "row_start": 1, "row_end": 3, "col_start": 0, "col_end": 2
        }),
    );
    let code = au_locus(
        0x21,
        0x22,
        json!({
            "kind": "code_symbol",
            "revision_ref": format!("eg:revision:{}", hex(0x23)),
            "symbol_ref": format!("eg:symbol:{}", hex(0x24)),
            "start_line": 210, "end_line": 245
        }),
    );
    let row = au_locus(
        0x31,
        0x32,
        json!({
            "kind": "row_version",
            "row_ref": format!("eg:row:{}", hex(0x33)),
            "version": 7
        }),
    );
    let metric = au_locus(
        0x41,
        0x42,
        json!({ "kind": "metric_window", "start_ms": 0, "end_ms": 60000 }),
    );
    let trace = au_locus(
        0x51,
        0x52,
        json!({
            "kind": "trace_span",
            "trace_ref": format!("eg:trace:{}", hex(0x53)),
            "span_ref": format!("eg:span:{}", hex(0x54))
        }),
    );

    for (id, node) in [
        ("evidence-table", &table),
        ("evidence-code", &code),
        ("evidence-row", &row),
        ("evidence-metric", &metric),
        ("evidence-trace", &trace),
    ] {
        view.node_properties
            .insert(id.to_string(), blob(node.clone()));
        view.edge_properties.insert(
            (id.to_string(), claim_id.to_string()),
            vec![blob(json!({ "relationship": "SUPPORTS" }))],
        );
    }

    let graph = BeliefGraph::from_graph_view(&view);

    // Each locus decodes and round-trips its own exact address kind + fields.
    let resolved = resolve_locus(&graph, "evidence-table").expect("table_cell_range locus");
    assert!(matches!(
        resolved.address,
        EvidenceAddress::TableCellRange {
            row_start: 1,
            row_end: 3,
            col_start: 0,
            col_end: 2,
        }
    ));

    let resolved = resolve_locus(&graph, "evidence-code").expect("code_symbol locus");
    assert!(matches!(
        resolved.address,
        EvidenceAddress::CodeSymbol {
            start_line: 210,
            end_line: 245,
            ..
        }
    ));

    let resolved = resolve_locus(&graph, "evidence-row").expect("row_version locus");
    assert!(matches!(
        resolved.address,
        EvidenceAddress::RowVersion { version: 7, .. }
    ));

    let resolved = resolve_locus(&graph, "evidence-metric").expect("metric_window locus");
    assert!(matches!(
        resolved.address,
        EvidenceAddress::MetricWindow {
            start_ms: 0,
            end_ms: 60000,
        }
    ));

    let resolved = resolve_locus(&graph, "evidence-trace").expect("trace_span locus");
    assert!(matches!(
        resolved.address,
        EvidenceAddress::TraceSpan { .. }
    ));

    // And ALL FIVE are cited together as this claim's evidence — the exact
    // walk `Method::ExplainEvidence`'s handler drives.
    let citations = evidence_citations(&graph, claim_id);
    assert_eq!(citations.len(), 5);
    assert!(citations.iter().all(|c| c.kind == EdgeKind::Supports));
    let cited_ids: BTreeSet<&str> = citations.iter().map(|c| c.evidence_id.as_str()).collect();
    assert_eq!(
        cited_ids,
        BTreeSet::from([
            "evidence-code",
            "evidence-metric",
            "evidence-row",
            "evidence-table",
            "evidence-trace",
        ])
    );
}

#[test]
fn a_malformed_au_authored_locus_is_dropped_not_fabricated() {
    // The governed-identity contract is enforced, not merely hoped for: a
    // locus missing the required envelope (no `policy_ref`) never resolves
    // and is silently absent from citations -- exactly `evidence.rs`'s own
    // "citations_do_not_fabricate_missing_loci" contract, now proven against
    // an AU-shaped (not hand-built) fixture.
    let claim_id = "claim-1";
    let mut view = GraphView::default();
    view.node_properties.insert(
        claim_id.to_string(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );
    view.node_properties.insert(
        "evidence-bad".to_string(),
        blob(json!({
            "type": "Evidence",
            "confidence": 0.9,
            "evidence_locus": {
                "id": format!("eg:locus:{}", hex(0x61)),
                "subject": { "kind": "occurrence", "id": format!("eg:occurrence:{}", hex(0x62)) },
                "address": { "kind": "row_version", "row_ref": format!("eg:row:{}", hex(0x63)), "version": 1 },
                // policy_ref/derivation_ref deliberately omitted.
            }
        })),
    );
    view.edge_properties.insert(
        ("evidence-bad".to_string(), claim_id.to_string()),
        vec![blob(json!({ "relationship": "SUPPORTS" }))],
    );

    let graph = BeliefGraph::from_graph_view(&view);
    assert_eq!(resolve_locus(&graph, "evidence-bad"), None);
    assert!(evidence_citations(&graph, claim_id).is_empty());
}
