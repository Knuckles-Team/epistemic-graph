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

/// Squared Euclidean distance shared by the vector mining algorithms.
pub(super) fn sq_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Deterministic splitmix64 (+ Box-Muller Gaussian) used by seeded mining
/// algorithms. Keeping the generator here avoids each algorithm carrying a
/// subtly different implementation while preserving reproducible seeds.
pub(super) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub(super) fn new(seed: u64) -> Self {
        SplitMix64 {
            state: seed.wrapping_add(0x9E37_79B9_7F4A_7C15),
        }
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        crate::splitmix64_next(&mut self.state)
    }

    pub(super) fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub(super) fn next_gauss(&mut self) -> f64 {
        // Box-Muller.
        let u1 = self.next_f64().max(1e-12);
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}
