use serde_json::Value;

pub fn kmeans_labels(value: &Value) -> Vec<i64> {
    let labels: Vec<i64> = value
        .as_array()
        .expect("kmeans → JSON array of Int64 labels")
        .iter()
        .map(|value| value.as_i64().unwrap())
        .collect();
    assert_eq!(labels.len(), 6, "{labels:?}");
    labels
}

pub fn pca_diagonal_component(value: &Value) -> (f64, f64) {
    let pcs = value.as_array().expect("pca → list of components");
    assert_eq!(pcs.len(), 1, "{pcs:?}");
    let pc0 = pcs[0].as_array().expect("component → vector");
    assert_eq!(pc0.len(), 2, "{pc0:?}");
    let (a, b) = (pc0[0].as_f64().unwrap(), pc0[1].as_f64().unwrap());
    let inv_sqrt2 = 1.0 / std::f64::consts::SQRT_2;
    assert!((a.abs() - inv_sqrt2).abs() < 1e-9, "pc0[0]: {pc0:?}");
    assert!((b.abs() - inv_sqrt2).abs() < 1e-9, "pc0[1]: {pc0:?}");
    assert!(a * b > 0.0, "PC1 should lie on y=x: {pc0:?}");
    (a, b)
}
