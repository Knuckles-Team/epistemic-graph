//! Surface-B numeric analytics UDFs/UDAFs (CONCEPT:EG-329) — the `eg-numeric`-backed
//! `cosine_sim`/`l2_normalize`/`zscore` scalar UDFs + `covariance` UDAF, asserted in-engine
//! (compute-near-data) against hand-computed values. Only compiles under `numeric`.
#![cfg(feature = "numeric")]

use eg_core::graph::GraphCore;
use eg_query::{exec_sql, QueryResult};
use serde_json::json;

/// A 5-node graph whose nodes carry `x` (1..5) and `y = 2x` (2,4,6,8,10) numeric props,
/// so covariance/zscore have exact hand-computed answers.
fn graph() -> GraphCore {
    let core = GraphCore::new();
    for i in 1..=5 {
        let val = json!({ "x": i as f64, "y": (2 * i) as f64 });
        core.add_node(format!("n{i}"), rmp_serde::to_vec_named(&val).unwrap());
    }
    core
}

fn rows(r: &QueryResult) -> Vec<Vec<serde_json::Value>> {
    r.rows
        .iter()
        .map(|b| rmp_serde::from_slice::<Vec<serde_json::Value>>(b).unwrap())
        .collect()
}

#[test]
fn cosine_sim_text_literals() {
    // Identical vectors → 1.0; orthogonal → 0.0; opposite → -1.0.
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT cosine_sim('[1,2,3]', '[1,2,3]') AS same, \
                cosine_sim('[1,0]', '[0,1]') AS orth, \
                cosine_sim('[1,0]', '[-1,0]') AS opp LIMIT 1",
    )
    .unwrap();
    let v = rows(&r);
    assert!(
        (v[0][0].as_f64().unwrap() - 1.0).abs() < 1e-9,
        "same: {:?}",
        v[0][0]
    );
    assert!(
        v[0][1].as_f64().unwrap().abs() < 1e-9,
        "orth: {:?}",
        v[0][1]
    );
    assert!(
        (v[0][2].as_f64().unwrap() + 1.0).abs() < 1e-9,
        "opp: {:?}",
        v[0][2]
    );
}

#[test]
fn cosine_sim_dim_mismatch_is_null() {
    let snap = graph().analysis_snapshot();
    let r = exec_sql(&snap, "SELECT cosine_sim('[1,2,3]', '[1,2]') AS m LIMIT 1").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], json!(null));
}

#[test]
fn l2_normalize_unit_vector() {
    // [3,4] → [0.6, 0.8] (‖[3,4]‖ = 5). Decodes as a JSON array (List<Float32>).
    let snap = graph().analysis_snapshot();
    let r = exec_sql(&snap, "SELECT l2_normalize('[3,4]') AS u LIMIT 1").unwrap();
    let v = rows(&r);
    let arr = v[0][0].as_array().expect("l2_normalize → JSON array");
    assert_eq!(arr.len(), 2);
    assert!((arr[0].as_f64().unwrap() - 0.6).abs() < 1e-6, "{arr:?}");
    assert!((arr[1].as_f64().unwrap() - 0.8).abs() < 1e-6, "{arr:?}");
}

#[test]
fn l2_normalize_feeds_cosine_sim() {
    // Normalizing preserves direction, so cosine_sim(l2_normalize(v), v) == 1.
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT cosine_sim(l2_normalize('[3,4]'), '[3,4]') AS s LIMIT 1",
    )
    .unwrap();
    let v = rows(&r);
    assert!(
        (v[0][0].as_f64().unwrap() - 1.0).abs() < 1e-6,
        "{:?}",
        v[0][0]
    );
}

#[test]
fn zscore_standardizes_column() {
    // x = [1,2,3,4,5]: mean 3, population std sqrt(2). zscore(1) = -2/sqrt(2) = -sqrt(2).
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT json_get_f64(props, 'x') AS x, zscore(json_get_f64(props, 'x')) AS z \
         FROM nodes ORDER BY x",
    )
    .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 5);
    let sqrt2 = std::f64::consts::SQRT_2;
    assert!(
        (v[0][1].as_f64().unwrap() + sqrt2).abs() < 1e-9,
        "z[0]: {:?}",
        v[0][1]
    );
    // Middle value equals the mean → z == 0.
    assert!(
        v[2][1].as_f64().unwrap().abs() < 1e-9,
        "z[2]: {:?}",
        v[2][1]
    );
    // Standardized column has (population) mean 0, std 1 by construction.
    let zs: Vec<f64> = v.iter().map(|row| row[1].as_f64().unwrap()).collect();
    let mean: f64 = zs.iter().sum::<f64>() / zs.len() as f64;
    assert!(mean.abs() < 1e-9, "mean of z: {mean}");
}

#[test]
fn covariance_udaf() {
    // y = 2x, so cov(x,y) (sample, ddof=1) = 2 * var_sample(x) = 2 * 2.5 = 5.0.
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT covariance(json_get_f64(props, 'x'), json_get_f64(props, 'y')) AS c FROM nodes",
    )
    .unwrap();
    let v = rows(&r);
    assert!(
        (v[0][0].as_f64().unwrap() - 5.0).abs() < 1e-9,
        "cov: {:?}",
        v[0][0]
    );
}
