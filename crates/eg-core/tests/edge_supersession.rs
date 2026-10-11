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
