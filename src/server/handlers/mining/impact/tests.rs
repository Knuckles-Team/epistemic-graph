//! The served impact models: `MineRiskPropagation` with `noisy_or` /
//! `independent_cascade`, their `:ImpactAssessment` writeback, and the WAL
//! replay reproducing the same facts.

use std::sync::Arc;

use eg_types::compute_result::mining::{ImpactOptions, RiskModel};

use super::super::{dispatch_for_test, replay};
use crate::graph::GraphCore;
use crate::protocol::{Method, ResultPayload};

fn method(model: RiskModel, writeback: bool) -> Method {
    // svc-a <- db, svc-b <- db, api <- svc-a, api <- svc-b: an incident on db.
    Method::MineRiskPropagation {
        nodes: ["db", "svc-a", "svc-b", "api"].map(String::from).to_vec(),
        seed: vec![1.0, 0.0, 0.0, 0.0],
        edges: vec![
            ("db".into(), "svc-a".into(), 0.8),
            ("db".into(), "svc-b".into(), 0.5),
            ("svc-a".into(), "api".into(), 0.9),
            ("svc-b".into(), "api".into(), 0.9),
        ],
        damping: 0.85,
        tolerance: 1e-9,
        max_iterations: 100,
        model,
        writeback,
        #[cfg(feature = "epistemic")]
        as_claim: false,
    }
}

fn options(scope: &str) -> ImpactOptions {
    ImpactOptions {
        scope: scope.into(),
        as_of_ms: 1_700_000_000_000,
        attribute_seeds: true,
        ..ImpactOptions::default()
    }
}

fn json(core: &Arc<GraphCore>, method: Method) -> serde_json::Value {
    let resp = dispatch_for_test(7, Arc::clone(core), method).expect("a mining method");
    let Some(ResultPayload::Json(v)) = resp.result else {
        panic!("expected a JSON result: {:?}", resp.error);
    };
    v
}

fn assessments(core: &GraphCore) -> Vec<(String, serde_json::Value)> {
    core.mark_dirty();
    let mut rows: Vec<(String, serde_json::Value)> = core
        .get_nodes_by_label("ImpactAssessment", 0)
        .into_iter()
        .map(|(id, blob)| (id, rmp_serde::from_slice(&blob).expect("assessment props")))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

#[test]
fn noisy_or_reports_probabilities_semantics_paths_and_attribution() {
    let core = Arc::new(GraphCore::new());
    let v = json(&core, method(RiskModel::NoisyOr(options("w1")), false));
    let score = |i: usize| v["scores"][i]["score"].as_f64().unwrap();
    assert_eq!(score(0), 1.0);
    assert!((score(1) - 0.8).abs() < 1e-12);
    // Shared ancestry (db) makes the noisy-OR an upper bound at api.
    let api = 1.0 - (1.0 - 0.9 * 0.8) * (1.0 - 0.9 * 0.5);
    assert!((score(3) - api).abs() < 1e-12);
    assert_eq!(v["impact"]["semantics"], "upper_bound");
    assert!(v["impact"]["digest"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    assert_eq!(v["scores"][3]["hops"], 2);
    let paths = v["impact"]["paths"].as_array().unwrap();
    assert_eq!(paths[0]["node"], "svc-a");
    assert_eq!(
        v["impact"]["paths"][1]["path"],
        serde_json::json!(["db", "svc-a", "api"])
    );
    let attributed: f64 = v["impact"]["attribution"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["node"] == "api")
        .map(|row| row["shapley"].as_f64().unwrap())
        .sum();
    assert!((attributed - api).abs() < 1e-12, "one seed carries it all");
}

#[test]
fn share_output_is_unchanged_mass_share() {
    let core = Arc::new(GraphCore::new());
    let v = json(&core, method(RiskModel::Share, false));
    let total: f64 = (0..4)
        .map(|i| v["scores"][i]["score"].as_f64().unwrap())
        .sum();
    assert!((total - 1.0).abs() < 1e-6, "PPR shares sum to one");
    assert!(v.get("impact").is_none());
    assert!(v["scores"][0].get("hops").is_none());
}

#[test]
fn writeback_is_reproduced_by_replay() {
    let model = RiskModel::IndependentCascade(ImpactOptions {
        samples: 500,
        rng_seed: 5,
        ..options("watch-1")
    });
    let served = Arc::new(GraphCore::new());
    let v = json(&served, method(model.clone(), true));
    assert_eq!(v["written_back"], 4);
    assert_eq!(v["impact"]["semantics"], "monte_carlo");
    let written = assessments(&served);
    assert_eq!(written.len(), 4);
    let db = written.iter().find(|(_, p)| p["of"] == "db").unwrap();
    assert_eq!(db.1["scope"], "watch-1");
    assert_eq!(db.1["as_of_ms"], 1_700_000_000_000u64);
    assert_eq!(db.1["seeds"], serde_json::json!(["db"]));
    let replayed = Arc::new(GraphCore::new());
    replay(&replayed, &method(model, true));
    assert_eq!(assessments(&replayed), written);
}

#[test]
fn assess_limits_writeback_and_records_a_cleared_node() {
    let core = Arc::new(GraphCore::new());
    let mut cleared = method(
        RiskModel::NoisyOr(ImpactOptions {
            assess: vec!["api".into()],
            ..options("w2")
        }),
        true,
    );
    if let Method::MineRiskPropagation { seed, .. } = &mut cleared {
        *seed = vec![0.0; 4];
    }
    let v = json(&core, cleared);
    assert_eq!(v["written_back"], 1);
    let written = assessments(&core);
    assert_eq!(written[0].1["of"], "api");
    assert_eq!(written[0].1["probability"], 0.0);
}
