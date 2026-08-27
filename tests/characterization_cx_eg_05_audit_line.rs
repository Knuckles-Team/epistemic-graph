//! CX-EG-05 characterization test for `audit::audit_line` (`src/audit.rs`,
//! CCN 11 as measured by the repo's lizard-based complexity gate before this
//! lane's refactor -- one over the `--cap 10` ceiling).
//!
//! `audit_line` is `pub fn`, so unlike the private `redb_store.rs` functions
//! this lane also owns, it can be exercised directly (no served-dispatch
//! harness needed) -- a true black-box call of the public function.
#![cfg(feature = "graph")]

use epistemic_graph::audit::audit_line;
use epistemic_graph::protocol::Method;

/// Plain CRUD arm: no guard, no branching -- a sanity baseline.
#[test]
fn t01_add_node_produces_add_node_line() {
    let line = audit_line(&Method::AddNode {
        node_id: "n1".to_string(),
        properties_msgpack: vec![],
    });
    assert_eq!(line, Some("ADD_NODE|n1".to_string()));
}

/// Unit-variant arm.
#[test]
fn t02_clear_graph_produces_clear_graph_line() {
    let line = audit_line(&Method::ClearGraph);
    assert_eq!(line, Some("CLEAR_GRAPH".to_string()));
}

/// The one arm with a 3-way `&&`-chained guard this lane plans to extract:
/// `ApplyMutation` whose `event_type`/`query` shape is the opaque
/// authoritative-state digest receipt (`event_type ==
/// "authoritative_state_operation" && query.len() == 71 &&
/// query.starts_with("sha256:") && <hex digits>`). Pins the exact
/// "AUTHORITATIVE_STATE_MUTATION|<digest>" line the guard produces when it
/// matches.
#[test]
fn t03_apply_mutation_authoritative_state_receipt_is_recognized() {
    let digest = format!("sha256:{}", "a".repeat(64));
    assert_eq!(digest.len(), 71);
    let line = audit_line(&Method::ApplyMutation {
        event_type: "authoritative_state_operation".to_string(),
        query: digest.clone(),
    });
    assert_eq!(line, Some(format!("AUTHORITATIVE_STATE_MUTATION|{digest}")));
}

/// Same guard, each of its four conditions individually broken -- pins that
/// EVERY condition is load-bearing (the guard is not vacuously true), so the
/// characterization survives the "make it fail on purpose" discipline against
/// the SOURCE behaviour, not just the test's own assertions.
#[test]
fn t04_apply_mutation_guard_requires_every_condition() {
    // wrong event_type
    let line = audit_line(&Method::ApplyMutation {
        event_type: "sparql_update".to_string(),
        query: format!("sha256:{}", "a".repeat(64)),
    });
    assert!(matches!(line, Some(ref s) if s.starts_with("APPLY_MUTATION|sparql_update|sha256:")));

    // wrong length (63 hex chars instead of 64)
    let line = audit_line(&Method::ApplyMutation {
        event_type: "authoritative_state_operation".to_string(),
        query: format!("sha256:{}", "a".repeat(63)),
    });
    assert!(
        matches!(line, Some(ref s) if s.starts_with("APPLY_MUTATION|authoritative_state_operation|sha256:")),
        "got {line:?}"
    );

    // wrong prefix
    let line = audit_line(&Method::ApplyMutation {
        event_type: "authoritative_state_operation".to_string(),
        query: format!("sha512:{}", "a".repeat(64)),
    });
    assert!(
        matches!(line, Some(ref s) if s.starts_with("APPLY_MUTATION|authoritative_state_operation|sha256:")),
        "got {line:?}"
    );

    // non-hex character
    let line = audit_line(&Method::ApplyMutation {
        event_type: "authoritative_state_operation".to_string(),
        query: format!("sha256:{}z", "a".repeat(63)),
    });
    assert!(
        matches!(line, Some(ref s) if s.starts_with("APPLY_MUTATION|authoritative_state_operation|sha256:")),
        "got {line:?}"
    );
}

/// A non-durable method (`DurabilityDomain::None`) falls through to
/// `_ => return None`.
#[test]
fn t05_non_durable_method_returns_none() {
    let line = audit_line(&Method::ListGraphs);
    assert_eq!(line, None);
}
