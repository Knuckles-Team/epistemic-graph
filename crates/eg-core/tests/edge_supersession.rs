use eg_core::graph::GraphCore;
use serde_json::json;

fn graph_with_prior() -> GraphCore {
    let core = GraphCore::new();
    for id in ["a", "b"] {
        core.add_node(id.into(), rmp_serde::to_vec_named(&json!({})).unwrap());
    }
    core.add_edge(
        "a".into(),
        "b".into(),
        rmp_serde::to_vec_named(&json!({
            "relationship": "LIKES", "valid_from": 100, "tx_from": 100
        }))
        .unwrap(),
    )
    .unwrap();
    core
}

// spec: EG-DURABLE-KERNEL-R080.1
#[test]
fn supersession_missing_endpoint_preserves_prior_and_allows_retry() {
    for (source, target, role) in [("missing", "b", "Source"), ("a", "missing", "Target")] {
        let core = graph_with_prior();
        let before = core.get_edge_properties("a", "b");
        let ledger_before = core.get_ledger();
        let edges_before = core.edge_count();
        let successor = rmp_serde::to_vec_named(&json!({
            "relationship": "DISLIKES", "valid_from": 200, "tx_from": 250,
            "supersedes": "a:b:LIKES"
        }))
        .unwrap();
        let error = core
            .supersede_edge(
                source.into(),
                target.into(),
                successor.clone(),
                "a",
                "b",
                "LIKES",
                200,
                250,
            )
            .unwrap_err();
        assert!(error.contains(role), "{error}");
        assert_eq!(core.get_edge_properties("a", "b"), before);
        assert_eq!(core.get_ledger(), ledger_before);
        assert_eq!(core.edge_count(), edges_before);
        assert!(core.get_edge_properties(source, target).is_empty());

        core.add_node(
            "missing".into(),
            rmp_serde::to_vec_named(&json!({})).unwrap(),
        );
        core.supersede_edge(
            source.into(),
            target.into(),
            successor,
            "a",
            "b",
            "LIKES",
            200,
            250,
        )
        .unwrap();
        let prior = core.get_edge_properties("a", "b");
        assert_eq!(prior.len(), 1);
        let prior: serde_json::Value = rmp_serde::from_slice(&prior[0]).unwrap();
        assert_eq!(prior["valid_until"], 200);
        assert_eq!(prior["tx_to"], 250);
        assert_eq!(core.get_edge_properties(source, target).len(), 1);
    }
}

#[test]
fn repeated_supersession_preserves_closed_history_at_every_timestamp() {
    let core = graph_with_prior();
    let successor = rmp_serde::to_vec_named(&json!({
        "relationship": "DISLIKES", "valid_from": 200, "tx_from": 250,
        "supersedes": "a:b:LIKES"
    }))
    .unwrap();
    core.supersede_edge(
        "a".into(),
        "b".into(),
        successor.clone(),
        "a",
        "b",
        "LIKES",
        200,
        250,
    )
    .unwrap();
    let history = core.get_edge_properties("a", "b");
    let ledger = core.get_ledger();
    let edge_count = core.edge_count();
    for valid_at in [150, 200, 300] {
        let error = core
            .supersede_edge(
                "a".into(),
                "b".into(),
                successor.clone(),
                "a",
                "b",
                "LIKES",
                valid_at,
                400,
            )
            .unwrap_err();
        assert!(error.contains("already closed"), "{error}");
        assert_eq!(core.get_edge_properties("a", "b"), history);
        assert_eq!(core.get_ledger(), ledger);
        assert_eq!(core.edge_count(), edge_count);
        assert_eq!(core.invalidate_edge("a", "b", "LIKES", valid_at, 500), 0);
        assert_eq!(core.get_edge_properties("a", "b"), history);
        assert_eq!(core.get_ledger(), ledger);
    }
}

#[test]
fn supersession_requires_a_matching_open_prior() {
    let core = graph_with_prior();
    let before = core.get_edge_properties("a", "b");
    let ledger = core.get_ledger();
    let error = core
        .supersede_edge(
            "a".into(),
            "b".into(),
            rmp_serde::to_vec_named(&json!({"relationship": "DISLIKES"})).unwrap(),
            "a",
            "b",
            "MISSING",
            200,
            250,
        )
        .unwrap_err();
    assert!(error.contains("prior edge"), "{error}");
    assert_eq!(core.get_edge_properties("a", "b"), before);
    assert_eq!(core.get_ledger(), ledger);
    assert_eq!(core.edge_count(), 1);
}

#[test]
fn same_relationship_supersession_preserves_older_closed_versions() {
    let core = graph_with_prior();
    for valid_at in [200, 300] {
        let before = core.get_edge_properties("a", "b");
        let successor = rmp_serde::to_vec_named(&json!({
            "relationship": "LIKES", "valid_from": valid_at, "tx_from": valid_at,
            "supersedes": "a:b:LIKES"
        }))
        .unwrap();
        core.supersede_edge(
            "a".into(),
            "b".into(),
            successor,
            "a",
            "b",
            "LIKES",
            valid_at,
            valid_at,
        )
        .unwrap();
        let after = core.get_edge_properties("a", "b");
        assert_eq!(after.len(), before.len() + 1);
        assert_eq!(&after[..before.len() - 1], &before[..before.len() - 1]);
        let closed: serde_json::Value = rmp_serde::from_slice(&after[before.len() - 1]).unwrap();
        assert_eq!(closed["valid_until"], valid_at);
        assert_eq!(closed["tx_to"], valid_at);
    }
}
