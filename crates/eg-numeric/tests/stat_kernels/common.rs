//! Shared fixtures for the statistics tests: seeded generators whose ground
//! truth is known by construction, and comparison helpers. Evidence produced
//! from these generators is synthetic.

use eg_numeric::detkernel::kernels::softmax;
use eg_numeric::detkernel::Level;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// A deterministic generator for `seed`.
pub fn rng(seed: u64) -> ChaCha8Rng {
    ChaCha8Rng::seed_from_u64(seed)
}

/// A uniform draw in `[0, 1)` (53 random bits; no float transcendental).
pub fn uniform(rng: &mut ChaCha8Rng) -> f64 {
    rng.gen::<f64>()
}

/// A uniform draw in `[low, high)`.
pub fn uniform_in(rng: &mut ChaCha8Rng, low: f64, high: f64) -> f64 {
    low + (high - low) * uniform(rng)
}

/// A draw from a categorical distribution by inverse CDF.
pub fn categorical(rng: &mut ChaCha8Rng, probabilities: &[f64]) -> usize {
    let u = uniform(rng);
    let mut cumulative = 0.0;
    for (index, p) in probabilities.iter().enumerate() {
        cumulative += p;
        if u < cumulative {
            return index;
        }
    }
    probabilities.len() - 1
}

/// `|actual - expected| <= tolerance`, naming the quantity on failure.
pub fn assert_close(actual: f64, expected: f64, tolerance: f64, what: &str) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{what}: got {actual:e}, expected {expected:e} +/- {tolerance:e}"
    );
}

/// Element-wise [`assert_close`] over equal-length slices.
pub fn assert_all_close(actual: &[f64], expected: &[f64], tolerance: f64, what: &str) {
    assert_eq!(actual.len(), expected.len(), "{what}: length");
    for (got, want) in actual.iter().zip(expected) {
        assert_close(*got, *want, tolerance, what);
    }
}

/// A level from a fraction; panics on an invalid fixture.
pub fn level(numerator: u64, denominator: u64) -> Level {
    Level::new(numerator, denominator).expect("fixture level is valid")
}

/// Binary scores with a planted calibration curve: `score ~ U(0, 1)` and
/// `P(outcome | score) = curve(score)`.
pub fn planted_binary(seed: u64, n: usize, curve: fn(f64) -> f64) -> (Vec<f64>, Vec<bool>) {
    let mut generator = rng(seed);
    (0..n)
        .map(|_| {
            let score = uniform(&mut generator);
            let outcome = uniform(&mut generator) < curve(score);
            (score, outcome)
        })
        .unzip()
}

/// Multi-class rows with planted true logits `z ~ U(-3, 3)^classes`, labels
/// drawn from `softmax(z)`, and presented scores `z * overconfidence + shift`.
/// Returns (presented rows, labels, true probability rows).
pub fn planted_logits(
    seed: u64,
    n: usize,
    classes: usize,
    overconfidence: f64,
    shift: &[f64],
) -> (Vec<Vec<f64>>, Vec<usize>, Vec<Vec<f64>>) {
    let mut generator = rng(seed);
    let mut presented = Vec::with_capacity(n);
    let mut labels = Vec::with_capacity(n);
    let mut truth = Vec::with_capacity(n);
    for _ in 0..n {
        let z: Vec<f64> = (0..classes)
            .map(|_| uniform_in(&mut generator, -3.0, 3.0))
            .collect();
        let p = softmax(&z).expect("finite logits");
        labels.push(categorical(&mut generator, &p));
        presented.push(
            z.iter()
                .zip(shift)
                .map(|(v, s)| v * overconfidence + s)
                .collect(),
        );
        truth.push(p);
    }
    (presented, labels, truth)
}

/// Mean over rows of the largest absolute probability difference.
pub fn mean_max_abs_difference(left: &[Vec<f64>], right: &[Vec<f64>]) -> f64 {
    let total: f64 = left
        .iter()
        .zip(right)
        .map(|(a, b)| {
            a.iter()
                .zip(b)
                .map(|(x, y)| (x - y).abs())
                .fold(0.0, f64::max)
        })
        .sum();
    total / left.len() as f64
}

/// `mean >= target - z * sd / sqrt(repeats)`: a one-sided binomial tolerance
/// for a mean of per-repeat rates whose per-repeat standard deviation is `sd`.
pub fn assert_at_least_within(mean: f64, target: f64, sd: f64, repeats: usize, what: &str) {
    let slack = 4.0 * sd / (repeats as f64).sqrt();
    assert!(
        mean >= target - slack,
        "{what}: mean {mean} below {target} - {slack}"
    );
}

/// FNV-1a 64 over bytes: a stable fingerprint for golden vectors.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
