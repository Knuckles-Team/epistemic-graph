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

// ── svd (CONCEPT:EG-336) / pca (CONCEPT:EG-335) — column→Array2 marshalling UDAFs ──
//
// Both aggregate a COLUMN OF VECTORS (text-literal operand form, as `cosine_sim` accepts)
// into a matrix and run a faer kernel, so a `VALUES` list of vectors is the exact matrix.

#[test]
fn svd_singular_values_of_known_matrix() {
    // Rows [3,0] and [0,-2] form the diagonal matrix diag(3,-2); its singular values are
    // the absolute eigenvalues, descending → [3, 2].
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT svd(v) AS s FROM (VALUES ('[3,0]'), ('[0,-2]')) AS t(v)",
    )
    .unwrap();
    let v = rows(&r);
    let s = v[0][0]
        .as_array()
        .expect("svd → JSON array of singular values");
    assert_eq!(s.len(), 2, "{s:?}");
    assert!((s[0].as_f64().unwrap() - 3.0).abs() < 1e-9, "s0: {s:?}");
    assert!((s[1].as_f64().unwrap() - 2.0).abs() < 1e-9, "s1: {s:?}");
}

#[test]
fn svd_rectangular_matrix() {
    // 3×2 matrix rows [2,0],[0,3],[0,0] → singular values [3, 2] (min(n,d)=2 of them).
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT svd(v) AS s FROM (VALUES ('[2,0]'), ('[0,3]'), ('[0,0]')) AS t(v)",
    )
    .unwrap();
    let v = rows(&r);
    let s = v[0][0].as_array().unwrap();
    assert_eq!(s.len(), 2, "{s:?}");
    assert!((s[0].as_f64().unwrap() - 3.0).abs() < 1e-9, "{s:?}");
    assert!((s[1].as_f64().unwrap() - 2.0).abs() < 1e-9, "{s:?}");
}

#[test]
fn pca_first_component_direction() {
    // Points along the [1,1] diagonal (mean-centered at origin): the first principal
    // component must be ±[1/√2, 1/√2] (sign is arbitrary for an eigenvector).
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT pca(v, 1) AS pcs \
         FROM (VALUES ('[2,2]'), ('[1,1]'), ('[-1,-1]'), ('[-2,-2]')) AS t(v)",
    )
    .unwrap();
    let v = rows(&r);
    // List<List<Float64>>: one component vector of length 2.
    let pcs = v[0][0].as_array().expect("pca → list of components");
    assert_eq!(pcs.len(), 1, "{pcs:?}");
    let pc0 = pcs[0].as_array().expect("component → vector");
    assert_eq!(pc0.len(), 2, "{pc0:?}");
    let inv_sqrt2 = 1.0 / std::f64::consts::SQRT_2;
    let a = pc0[0].as_f64().unwrap();
    let b = pc0[1].as_f64().unwrap();
    assert!((a.abs() - inv_sqrt2).abs() < 1e-9, "a: {pc0:?}");
    assert!((b.abs() - inv_sqrt2).abs() < 1e-9, "b: {pc0:?}");
    // Same sign (direction [1,1], not [1,-1]).
    assert!(a * b > 0.0, "components should share sign: {pc0:?}");
}

#[test]
fn pca_two_components_are_orthonormal_and_ordered() {
    // A dataset with more spread along x than y: PC1 ≈ ±[1,0], PC2 ≈ ±[0,1].
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT pca(v, 2) AS pcs \
         FROM (VALUES ('[4,1]'), ('[2,0]'), ('[-2,0]'), ('[-4,-1]')) AS t(v)",
    )
    .unwrap();
    let v = rows(&r);
    let pcs = v[0][0].as_array().unwrap();
    assert_eq!(pcs.len(), 2, "{pcs:?}");
    let pc1 = pcs[0].as_array().unwrap();
    let pc2 = pcs[1].as_array().unwrap();
    // Each component is a unit vector.
    let norm = |p: &[serde_json::Value]| -> f64 {
        p.iter()
            .map(|x| x.as_f64().unwrap().powi(2))
            .sum::<f64>()
            .sqrt()
    };
    assert!((norm(pc1) - 1.0).abs() < 1e-9, "pc1 norm: {pc1:?}");
    assert!((norm(pc2) - 1.0).abs() < 1e-9, "pc2 norm: {pc2:?}");
    // Orthogonal (dot ≈ 0).
    let dot: f64 = pc1
        .iter()
        .zip(pc2.iter())
        .map(|(x, y)| x.as_f64().unwrap() * y.as_f64().unwrap())
        .sum();
    assert!(dot.abs() < 1e-9, "pc1·pc2: {dot}");
    // PC1 (largest variance) aligns with the x-axis: |x| > |y|.
    assert!(
        pc1[0].as_f64().unwrap().abs() > pc1[1].as_f64().unwrap().abs(),
        "pc1 should be x-dominant: {pc1:?}"
    );
}

#[test]
fn svd_empty_is_null() {
    // No rows → NULL list (never an error).
    let snap = graph().analysis_snapshot();
    let r = exec_sql(
        &snap,
        "SELECT svd(json_get_f64(props, 'missing_vec')) AS s FROM nodes WHERE 1 = 0",
    )
    .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], json!(null));
}
