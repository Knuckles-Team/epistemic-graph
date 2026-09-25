//! `PROPAGATE` (EH-526): impact probabilities over the resident graph, served by the
//! same kernel as `MineRiskPropagation`.

use super::blob;
use eg_compute::graph_algos::impact::{noisy_or, ImpactGraph, Seed};
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use eg_types::wire::UqlResult;
use serde_json::json;

use crate::exec::PlanCtx;
use crate::uql::serve::run_statement;
use crate::uql::{parse_statement, Params};

/// api, cache and web depend (transitively) on db; web's edge carries no transmission.
fn fixture(db_p_seed: Option<f64>) -> (eg_core::graph::GraphView, SemanticStore) {
    let core = GraphCore::new();
    for id in ["db", "api", "web", "cache", "other"] {
        let mut props = json!({ "type": "Service", "name": id });
        if let (Some(p), "db") = (db_p_seed, id) {
            props["p_seed"] = json!(p);
        }
        core.add_node(id.into(), blob(props));
    }
    let edges = [
        (
            "api",
            "db",
            json!({ "relationship": "dependsOn", "transmission": 0.8 }),
        ),
        ("web", "api", json!({ "relationship": "dependsOn" })),
        (
            "cache",
            "db",
            json!({ "relationship": "dependsOn", "transmission": 0.3, "tier": 2 }),
        ),
        (
            "other",
            "db",
            json!({ "relationship": "monitors", "transmission": 1.0 }),
        ),
    ];
    for (s, t, props) in edges {
        core.add_edge(s.into(), t.into(), blob(props)).unwrap();
    }
    (core.analysis_snapshot(), SemanticStore::new())
}

fn impact(src: &str, db_p_seed: Option<f64>) -> Vec<(String, f64)> {
    let (view, semantic) = fixture(db_p_seed);
    let ctx = PlanCtx::new(&view, &semantic);
    let stmt = parse_statement(src, &Params::new())
        .map_err(|e| e.render(src))
        .unwrap();
    let UqlResult::Rows { rows, columns, .. } = run_statement(&stmt, &ctx).unwrap() else {
        panic!("rows expected")
    };
    assert_eq!(columns, vec!["impact"]);
    rows.into_iter()
        .map(|r| (r.id, r.channels[0].expect("impact channel")))
        .collect()
}

const QUERY: &str = "MATCH (:Service) |> WHERE name = 'db' \
                     |> PROPAGATE NOISY_OR <-[:dependsOn]- HOPS 4 DEFAULT 0.5 |> RETURN impact";

#[test]
fn noisy_or_flows_against_dependencies_with_declared_and_default_transmission() {
    let rows = impact(QUERY, None);
    let expected = [("db", 1.0f64), ("api", 0.8), ("web", 0.4), ("cache", 0.3)];
    assert_eq!(rows.len(), expected.len(), "{rows:?}");
    for ((id, p), (want_id, want)) in rows.iter().zip(expected) {
        assert_eq!(id, want_id);
        assert!((p - want).abs() < 1e-6, "{id}: {p}");
    }
}

#[test]
fn the_query_surface_and_the_kernel_agree() {
    // The same cone, handed to the kernel directly: 0=db, 1=api, 2=cache, 3=web.
    let graph = ImpactGraph::new(4, &[(0, 1, 0.8), (0, 2, 0.3), (1, 3, 0.5)]);
    let kernel = noisy_or(
        &graph,
        &[Seed {
            node: 0,
            probability: 0.5,
        }],
        4,
    )
    .probability;
    let rows = impact(QUERY, Some(0.5));
    let by_id = |id: &str| rows.iter().find(|(r, _)| r == id).map(|(_, p)| *p);
    for (i, id) in ["db", "api", "cache", "web"].into_iter().enumerate() {
        assert_eq!(by_id(id), Some(f64::from(kernel[i] as f32)), "{id}");
    }
}

#[test]
fn hops_edge_predicates_and_the_cascade() {
    let one_hop = impact(
        "MATCH (:Service) |> WHERE name = 'db' |> PROPAGATE NOISY_OR <-[:dependsOn]- HOPS 1 \
         |> RETURN impact",
        None,
    );
    assert!(one_hop.iter().all(|(id, _)| id != "web"));
    let filtered = impact(
        "MATCH (:Service) |> WHERE name = 'db' \
         |> PROPAGATE NOISY_OR <-[:dependsOn WHERE tier IS NULL]- |> RETURN impact",
        None,
    );
    assert!(filtered.iter().all(|(id, _)| id != "cache"));
    let cascade = "MATCH (:Service) |> WHERE name = 'db' \
                   |> PROPAGATE CASCADE SAMPLES 4000 SEED 3 <-[:dependsOn]- DEFAULT 0.5 \
                   |> RETURN impact";
    let first = impact(cascade, None);
    assert_eq!(first, impact(cascade, None), "seeded");
    let api = first.iter().find(|(id, _)| id == "api").unwrap().1;
    assert!((api - 0.8).abs() < 0.03, "api {api}");
}
