//! Small numerical helpers shared by the mining classifiers and clusterers.

/// Log N(x | mean, diag(var)) for a diagonal-covariance Gaussian.
pub(super) fn log_gaussian_diag(x: &[f64], mean: &[f64], var: &[f64]) -> f64 {
    const LOG_2PI: f64 = 1.837_877_066_409_345_6; // ln(2π)
    let mut acc = 0.0;
    for d in 0..x.len() {
        let v = var[d].max(1e-12);
        let diff = x[d] - mean[d];
        acc += -0.5 * (LOG_2PI + v.ln() + diff * diff / v);
    }
    acc
}

/// Return the first index carrying the greatest value.
pub(super) fn argmax(v: &[f64]) -> usize {
    let mut best = 0;
    for i in 1..v.len() {
        if v[i] > v[best] {
            best = i;
        }
    }
    best
}
