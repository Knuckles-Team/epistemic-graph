//! Lifecycle trace walk (MEDULLA-SDLC-R006.2).

use eg_core::lifecycle::derive::DerivedEdge;
use eg_core::lifecycle::store::LifecycleStore;
use eg_core::lifecycle::trace::lifecycle_trace;
use eg_types::lifecycle::{
    LifecycleEdgeKind as E, LifecycleKind as K, LifecycleTraceRequest, TRACE_NODE_BUDGET,
};

fn edge(kind: E, from: &str, to: &str) -> DerivedEdge {
    DerivedEdge {
        kind,
        from: from.into(),
        to: to.into(),
        rule: "changes.span_overlap".into(),
        rule_version: 1,
        input_digests: vec![],
    }
}

fn request(anchor: &str, depth: u8, kinds: Vec<K>) -> LifecycleTraceRequest {
    LifecycleTraceRequest::new(anchor.into(), depth, kinds).unwrap()
}

// spec: MEDULLA-SDLC-R006.2
#[test]
fn trace_respects_depth_and_budget() {
    // r -- c1 -- s1 -- t1 -- r2 -- c2 -- s2 : a chain of alternating kinds.
    let mut store = LifecycleStore::default();
    let nodes = [
        ("r", K::Requirement),
        ("c1", K::Changeset),
        ("s1", K::CodeSymbol),
        ("t1", K::TestCase),
        ("r2", K::Requirement),
        ("c2", K::Changeset),
        ("s2", K::CodeSymbol),
    ];
    for (id, kind) in nodes {
        store.add_node(id, kind);
    }
    store
        .apply_derived(vec![
            edge(E::CitesRequirement, "c1", "r"),
            edge(E::Changes, "c1", "s1"),
            edge(E::Exercises, "t1", "s1"),
            edge(E::VerifiedBy, "r2", "t1"),
            edge(E::CitesRequirement, "c2", "r2"),
            edge(E::Changes, "c2", "s2"),
        ])
        .unwrap();

    let ids = |depth: u8, kinds: Vec<K>| -> Vec<String> {
        lifecycle_trace(&store, &request("r", depth, kinds))
            .records
            .into_iter()
            .map(|r| r.id)
            .collect()
    };
    assert_eq!(ids(1, vec![]), ["r", "c1"]);
    assert_eq!(ids(3, vec![]), ["r", "c1", "s1", "t1"]);
    assert_eq!(ids(8, vec![]).len(), 7);
    // The walk passes through filtered kinds but returns only the asked kinds.
    assert_eq!(ids(8, vec![K::TestCase]), ["t1"]);
    let answer = lifecycle_trace(&store, &request("r", 2, vec![]));
    assert_eq!(answer.edges.len(), 2);
    assert!(!answer.truncated);
    assert!(answer
        .edges
        .iter()
        .all(|e| e.rule == "changes.span_overlap" && e.rule_version == 1));
    // An unknown anchor is an empty answer, not an error.
    assert!(lifecycle_trace(&store, &request("ghost", 4, vec![]))
        .records
        .is_empty());

    // The node budget stops a wide walk and reports it.
    let mut wide = LifecycleStore::default();
    wide.add_node("hub", K::Requirement);
    let mut edges = Vec::new();
    for i in 0..TRACE_NODE_BUDGET + 500 {
        let id = format!("leaf{i:05}");
        wide.add_node(&id, K::Changeset);
        edges.push(edge(E::CitesRequirement, &id, "hub"));
    }
    wide.apply_derived(edges).unwrap();
    let answer = lifecycle_trace(&wide, &request("hub", 8, vec![]));
    assert_eq!(answer.records.len(), TRACE_NODE_BUDGET);
    assert!(answer.truncated);
    let small = lifecycle_trace(&store, &request("r", 8, vec![]));
    assert!(!small.truncated);
}
