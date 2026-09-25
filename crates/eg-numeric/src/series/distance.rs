//! z-normalised Euclidean distance between subsequences (EH-529, ANALYTICS-HARVEST §4
//! AH-09) — the one distance every motif / discord kernel reports: MASS
//! query-by-example, the SCRIMP++ matrix profile and the streaming (STAMPI) left profile.
//!
//! From the dot product `QT` of two length-`m` subsequences and their means `μ` and
//! population deviations `σ`: `d = √(2m(1 − ρ))`, `ρ = (QT − m·μa·μb) / (m·σa·σb)`, with `ρ`
//! clamped to `[−1, 1]`. The subsequence statistics are the exact rolling moments
//! (EH-562), so a flat subsequence has `σ = 0` EXACTLY and is handled by definition: two
//! flat subsequences are at distance `0`, a flat and a non-flat one at `√m`.

use super::exact::MomentWindow;

/// The shortest subsequence a z-normalised distance is defined for.
pub const MIN_LENGTH: usize = 2;

/// The mean and population deviation of every length-`m` subsequence of a series.
#[derive(Clone, Debug, PartialEq)]
pub struct SubsequenceStats {
    pub mean: Vec<f64>,
    pub std: Vec<f64>,
}

impl SubsequenceStats {
    /// Statistics of the `n − m + 1` subsequences of `xs` (empty when `m > n`).
    pub fn of(xs: &[f64], m: usize) -> Self {
        let mut window = MomentWindow::new(m);
        let (mut mean, mut std) = (Vec::new(), Vec::new());
        for &x in xs {
            if let Some(moments) = window.step(x) {
                mean.push(moments.mean);
                std.push(moments.population_std());
            }
        }
        Self { mean, std }
    }

    /// `(μ, σ)` of subsequence `i`.
    pub fn at(&self, i: usize) -> (f64, f64) {
        (self.mean[i], self.std[i])
    }
}

/// The z-normalised distance of two length-`m` subsequences from their dot product and
/// their `(μ, σ)`.
pub fn znorm_distance(qt: f64, m: usize, a: (f64, f64), b: (f64, f64)) -> f64 {
    let m = m as f64;
    match (a.1 == 0.0, b.1 == 0.0) {
        (true, true) => 0.0,
        (true, false) | (false, true) => m.sqrt(),
        (false, false) => {
            let rho = ((qt - m * a.0 * b.0) / (m * a.1 * b.1)).clamp(-1.0, 1.0);
            (2.0 * m * (1.0 - rho)).max(0.0).sqrt()
        }
    }
}

/// The plain dot product (the direct `QT` a diagonal walk re-anchors on).
pub fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).fold(0.0, |acc, (x, y)| acc + x * y)
}

/// The trivial-match exclusion half-width for subsequence length `m`: a subsequence
/// never matches one that starts within `⌈m/4⌉` of it.
pub fn exclusion_zone(m: usize) -> usize {
    m.div_ceil(4)
}

/// The series shifted by its mean — z-normalised distances are shift-invariant, and a
/// centred series keeps `QT − m·μa·μb` away from catastrophic cancellation.
pub fn centred(xs: &[f64]) -> Vec<f64> {
    let origin = if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    };
    xs.iter().map(|x| x - origin).collect()
}
