// CONCEPT:EH-280 — the lowered write-set is deterministic and tombstones remove.

use std::collections::HashMap;

use eg_types::change_envelope::{ChangeEnvelope, MaterialClass};
use eg_types::ingestion_wire::{ExtractedEdge, ExtractedNode, IndexResult};

use super::{lower, seal, IndexWriteSet};
use crate::mutation_batch::MutationSurface;
use crate::protocol::Method;
use crate::server::mutation_batch::{compile_methods, CompileBatch};

fn node(id: &str, kind: &str, properties: &[(&str, &str)]) -> ExtractedNode {
    ExtractedNode {
        node_id: id.to_string(),
        node_type: kind.to_string(),
        properties: properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    }
}

fn edge(source: &str, target: &str, kind: &str) -> ExtractedEdge {
    ExtractedEdge {
        source: source.to_string(),
        target: target.to_string(),
        edge_type: kind.to_string(),
        properties: HashMap::new(),
    }
}

fn result() -> IndexResult {
    IndexResult {
        nodes: vec![
            node(
                "branch:b",
                "Branch",
                &[("ref_name", "main"), ("status", "live")],
            ),
            node(
                "fileversion:f",
                "FileVersion",
                &[("path", "a.py"), ("content_digest", "d")],
            ),
        ],
        edges: vec![
            edge("branch:b", "fileversion:f", "hasFileVersion"),
            edge("branch:b", "fileversion:old", "removesFileVersion"),
            edge("symbol:a", "symbol:b", "calls"),
            edge("symbol:a", "symbol:b", "similar_to"),
        ],
        ..Default::default()
    }
}

#[test]
fn write_set_is_deterministic_across_property_order() {
    let first = lower(&result()).expect("lowered");
    for _ in 0..8 {
        // A fresh HashMap may iterate in another order; the id must not move.
        assert_eq!(
            lower(&result()).expect("lowered").envelope_id,
            first.envelope_id
        );
    }
    assert!(first.envelope_id.starts_with("repository-index:"));
}

#[test]
fn tombstones_lower_to_membership_removal() {
    let lowered = lower(&result()).expect("lowered");
    assert!(lowered.methods.iter().any(|method| matches!(
        method,
        Method::RemoveEdge { source_id, target_id }
            if source_id == "branch:b" && target_id == "fileversion:old"
    )));
}

#[test]
fn one_edge_per_endpoint_pair_keeps_the_structural_one() {
    let lowered = lower(&result()).expect("lowered");
    let symbol_edges: Vec<&Method> = lowered
        .methods
        .iter()
        .filter(
            |method| matches!(method, Method::AddEdge { source_id, .. } if source_id == "symbol:a"),
        )
        .collect();
    assert_eq!(symbol_edges.len(), 1);
    let Method::AddEdge {
        properties_msgpack, ..
    } = symbol_edges[0]
    else {
        unreachable!("filtered to AddEdge");
    };
    let properties: std::collections::BTreeMap<String, String> =
        rmp_serde::from_slice(properties_msgpack).expect("edge properties");
    assert_eq!(properties["relationship"], "calls");
}

/// A repository shaped like real code: a decorated class in `app/home/views.py`.
fn code_result(extra: &[(&str, &str)]) -> IndexResult {
    let mut symbol = vec![("name", "Settings"), ("decorators", "@dataclass")];
    symbol.extend_from_slice(extra);
    IndexResult {
        nodes: vec![
            node(
                "fileversion:v",
                "FileVersion",
                &[("path", "app/home/views.py")],
            ),
            node("symbol:s", "SYMBOL", &symbol),
        ],
        edges: vec![edge("fileversion:v", "symbol:s", "IMPLEMENTS")],
        ..Default::default()
    }
}

/// The repository-snapshot envelope the durable route commits for `result`.
pub(crate) fn repository_envelope(result: &IndexResult) -> Result<ChangeEnvelope, String> {
    let IndexWriteSet {
        envelope_id,
        digest,
        methods,
    } = lower(result)?;
    let mutation = compile_methods(
        CompileBatch {
            batch_id: &envelope_id,
            request_id: 1,
            attempt_nonce: None,
            principal: Some("test-principal"),
            tenant: "tenant-a",
            graph: "graph-a",
            placement_epoch: 0,
            idempotency_key: "repository-index-test",
            expected_graph_version: Some(0),
            fencing_token: None,
            created_at_ms: 1,
            default_surface: MutationSurface::Graph,
            authoritative_state: None,
        },
        methods,
    )?;
    seal(mutation, envelope_id, &digest, "tenant-a")
}

#[test]
fn repository_code_passes_the_repository_content_rule() {
    let envelope = repository_envelope(&code_result(&[])).expect("real code is repository content");
    assert_eq!(envelope.material_class, MaterialClass::RepositorySnapshot);
}

#[test]
fn the_same_code_is_refused_under_the_strict_attested_rule() {
    let mut envelope = repository_envelope(&code_result(&[])).expect("repository content");
    envelope.material_class = MaterialClass::Attested;
    let error = envelope
        .validate()
        .expect_err("strict rule refuses @ and /home/");
    assert!(error.contains("persistence privacy policy"), "{error}");
}

#[test]
fn host_identity_inside_repository_content_is_still_refused() {
    for leak in ["/home/alice/project/app.py", "maintainer dev@example.com"] {
        let error = repository_envelope(&code_result(&[("doc", leak)]))
            .expect_err("host identity is refused in every class");
        assert!(error.contains("host identity"), "{error}");
    }
}
