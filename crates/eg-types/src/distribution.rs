// CONCEPT:EG-086 — Probabilistic / uncertainty values.
//
// A first-class, serde-serializable `Distribution` VALUE. It generalizes the
// scalar `eg:confidence` / noisy-OR belief model (CONCEPT:KG-2.236) and the
// Ebbinghaus recency decay (CONCEPT:KG-2.211) into full probability
// distributions, so a node/edge property can carry *uncertainty* — a Gaussian
// sensor reading, a Beta belief over a Bernoulli rate, a Categorical over
// discrete labels, or an Empirical bag of samples — for medical / robotics /
// decision uncertainty.
//
// This is a stored VALUE (it lives as JSON inside the property map), NOT a wire
// `Op` — propagating uncertainty through the planner (`Op::Probabilistic`) is a
// deliberately deferred follow-up so this pass does not touch `wire.rs` /
// `eg-plan`. Pure Rust + serde only: all the special-function math (Lanczos
// `ln_gamma`, the regularized incomplete beta, Acklam's inverse-normal CDF) and
// the deterministic sampler (SplitMix64 → Box–Muller / Marsaglia–Tsang) are
// implemented inline, so eg-types keeps its serde-only dependency footprint.

use serde::{Deserialize, Serialize};

/// A probability distribution stored as a node/edge property value (CONCEPT:EG-086).
///
/// Serializes as a tagged JSON object, e.g. `{"kind":"gaussian","mean":1.0,"std":0.5}`,
/// so it round-trips through the arbitrary-JSON property map with no schema change.
///
/// The numeric moment/interval/sample methods treat `Categorical` over its
/// zero-based category *index* (a `Categorical` is a distribution over labels,
/// not over the reals — `mean`/`variance`/`sample` operate on the index, while
/// `pmf` operates on the label). All other variants are over the reals.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Distribution {
    /// Normal distribution `N(mean, std²)`.
    Gaussian { mean: f64, std: f64 },
    /// Beta distribution `Beta(alpha, beta)` on `(0, 1)` — the conjugate belief
    /// over a Bernoulli rate (generalizes scalar `eg:confidence`).
    Beta { alpha: f64, beta: f64 },
    /// Discrete distribution over labelled categories with their probabilities.
    Categorical { probs: Vec<(String, f64)> },
    /// Empirical distribution: equal mass `1/n` on each observed sample.
    Empirical { samples: Vec<f64> },
}

impl Distribution {
    /// Expected value. For `Categorical` this is the mean category *index*
    /// (`Σ i·pᵢ`, weights renormalized); for `Empirical` the sample mean.
    pub fn mean(&self) -> f64 {
        match self {
            Distribution::Gaussian { mean, .. } => *mean,
            Distribution::Beta { alpha, beta } => {
                let s = alpha + beta;
                if s <= 0.0 {
                    return 0.0;
                }
                alpha / s
            }
            Distribution::Categorical { probs } => {
                let total: f64 = probs.iter().map(|(_, p)| p).sum();
                if total <= 0.0 {
                    return 0.0;
                }
                probs
                    .iter()
                    .enumerate()
                    .map(|(i, (_, p))| i as f64 * (p / total))
                    .sum()
            }
            Distribution::Empirical { samples } => {
                if samples.is_empty() {
                    return 0.0;
                }
                samples.iter().sum::<f64>() / samples.len() as f64
            }
        }
    }

    /// Variance. For `Categorical` the variance of the category *index*; for
    /// `Empirical` the population variance (`÷n`), i.e. the variance of the
    /// empirical distribution that puts mass `1/n` on each sample.
    pub fn variance(&self) -> f64 {
        match self {
            Distribution::Gaussian { std, .. } => std * std,
            Distribution::Beta { alpha, beta } => {
                let s = alpha + beta;
                if s <= 0.0 {
                    return 0.0;
                }
                (alpha * beta) / (s * s * (s + 1.0))
            }
            Distribution::Categorical { probs } => {
                let total: f64 = probs.iter().map(|(_, p)| p).sum();
                if total <= 0.0 {
                    return 0.0;
                }
                let m = self.mean();
                probs
                    .iter()
                    .enumerate()
                    .map(|(i, (_, p))| {
                        let d = i as f64 - m;
                        (p / total) * d * d
                    })
                    .sum()
            }
            Distribution::Empirical { samples } => {
                if samples.is_empty() {
                    return 0.0;
                }
                let m = self.mean();
                samples.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / samples.len() as f64
            }
        }
    }

    /// Probability density at `x` for the continuous variants (`Gaussian`,
    /// `Beta`) and a Gaussian-KDE estimate for `Empirical`. `Categorical` is
    /// discrete — its density is `0.0`; use [`Distribution::pmf`] instead.
    pub fn pdf(&self, x: f64) -> f64 {
        match self {
            Distribution::Gaussian { mean, std } => {
                if *std <= 0.0 {
                    return 0.0;
                }
                let z = (x - mean) / std;
                (-0.5 * z * z).exp() / (std * (2.0 * std::f64::consts::PI).sqrt())
            }
            Distribution::Beta { alpha, beta } => {
                if x <= 0.0 || x >= 1.0 || *alpha <= 0.0 || *beta <= 0.0 {
                    return 0.0;
                }
                let ln_b = ln_gamma(*alpha) + ln_gamma(*beta) - ln_gamma(alpha + beta);
                ((alpha - 1.0) * x.ln() + (beta - 1.0) * (1.0 - x).ln() - ln_b).exp()
            }
            Distribution::Categorical { .. } => 0.0,
            Distribution::Empirical { samples } => {
                let n = samples.len();
                if n == 0 {
                    return 0.0;
                }
                // Silverman's rule-of-thumb bandwidth over a Gaussian kernel.
                let sd = self.variance().sqrt();
                let h = (1.06 * sd * (n as f64).powf(-0.2)).max(1e-9);
                let norm = 1.0 / (h * (2.0 * std::f64::consts::PI).sqrt());
                samples
                    .iter()
                    .map(|xi| {
                        let z = (x - xi) / h;
                        norm * (-0.5 * z * z).exp()
                    })
                    .sum::<f64>()
                    / n as f64
            }
        }
    }

    /// Probability mass of a labelled outcome. Non-zero only for `Categorical`
    /// (the summed, renormalized probability of `label`); `0.0` otherwise.
    pub fn pmf(&self, label: &str) -> f64 {
        match self {
            Distribution::Categorical { probs } => {
                let total: f64 = probs.iter().map(|(_, p)| p).sum();
                if total <= 0.0 {
                    return 0.0;
                }
                probs
                    .iter()
                    .filter(|(l, _)| l == label)
                    .map(|(_, p)| p / total)
                    .sum()
            }
            _ => 0.0,
        }
    }

    /// Draw one deterministic sample given `seed` (same seed ⇒ same draw). For
    /// `Categorical` returns the sampled category *index* as `f64`; for
    /// `Empirical` returns one of the observed samples.
    pub fn sample(&self, seed: u64) -> f64 {
        let mut rng = SplitMix64::new(seed);
        match self {
            Distribution::Gaussian { mean, std } => mean + std * std_normal(&mut rng),
            Distribution::Beta { alpha, beta } => {
                let x = sample_gamma(&mut rng, *alpha);
                let y = sample_gamma(&mut rng, *beta);
                let s = x + y;
                if s <= 0.0 {
                    0.0
                } else {
                    x / s
                }
            }
            Distribution::Categorical { probs } => {
                let total: f64 = probs.iter().map(|(_, p)| p).sum();
                if total <= 0.0 || probs.is_empty() {
                    return 0.0;
                }
                let u = rng.next_f64() * total;
                let mut acc = 0.0;
                for (i, (_, p)) in probs.iter().enumerate() {
                    acc += p;
                    if u < acc {
                        return i as f64;
                    }
                }
                (probs.len() - 1) as f64
            }
            Distribution::Empirical { samples } => {
                if samples.is_empty() {
                    return 0.0;
                }
                let idx = (rng.next_f64() * samples.len() as f64) as usize;
                samples[idx.min(samples.len() - 1)]
            }
        }
    }

    /// Central credible interval covering probability mass `p` (e.g. `0.95`):
    /// the `(1−p)/2` and `(1+p)/2` quantiles. `p` is clamped to `(0, 1)`. For
    /// `Categorical` the bounds are category *indices*.
    pub fn credible_interval(&self, p: f64) -> (f64, f64) {
        let p = p.clamp(1e-9, 1.0 - 1e-9);
        let lo_q = (1.0 - p) / 2.0;
        let hi_q = (1.0 + p) / 2.0;
        (self.quantile(lo_q), self.quantile(hi_q))
    }

    /// The `q`-quantile (inverse CDF), `q ∈ (0, 1)`.
    pub fn quantile(&self, q: f64) -> f64 {
        let q = q.clamp(1e-12, 1.0 - 1e-12);
        match self {
            Distribution::Gaussian { mean, std } => mean + std * inv_norm_cdf(q),
            Distribution::Beta { alpha, beta } => beta_quantile(q, *alpha, *beta),
            Distribution::Categorical { probs } => {
                let total: f64 = probs.iter().map(|(_, p)| p).sum();
                if total <= 0.0 || probs.is_empty() {
                    return 0.0;
                }
                let target = q * total;
                let mut acc = 0.0;
                for (i, (_, p)) in probs.iter().enumerate() {
                    acc += p;
                    if acc >= target {
                        return i as f64;
                    }
                }
                (probs.len() - 1) as f64
            }
            Distribution::Empirical { samples } => {
                if samples.is_empty() {
                    return 0.0;
                }
                let mut sorted = samples.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                // Type-7 linear-interpolated empirical quantile.
                let n = sorted.len();
                if n == 1 {
                    return sorted[0];
                }
                let h = (n as f64 - 1.0) * q;
                let lo = h.floor() as usize;
                let hi = (lo + 1).min(n - 1);
                let frac = h - lo as f64;
                sorted[lo] + frac * (sorted[hi] - sorted[lo])
            }
        }
    }
}

// ── Deterministic PRNG + samplers (pure Rust, no `rand` dep) ─────────────────

/// SplitMix64 — a tiny, fast, deterministic PRNG. Seeded reproducibly so
/// `sample(seed)` is a pure function of `(distribution, seed)`.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)` using the top 53 bits (f64 mantissa).
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// One standard-normal draw via the Box–Muller transform.
fn std_normal(rng: &mut SplitMix64) -> f64 {
    let u1 = rng.next_f64().max(1e-300);
    let u2 = rng.next_f64();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

/// One `Gamma(shape, 1)` draw via Marsaglia–Tsang (shape > 0).
fn sample_gamma(rng: &mut SplitMix64, shape: f64) -> f64 {
    if shape <= 0.0 {
        return 0.0;
    }
    if shape < 1.0 {
        // Boost: Gamma(a) = Gamma(a+1) · U^(1/a).
        let u = rng.next_f64().max(1e-300);
        return sample_gamma(rng, shape + 1.0) * u.powf(1.0 / shape);
    }
    let d = shape - 1.0 / 3.0;
    let c = 1.0 / (9.0 * d).sqrt();
    loop {
        let x = std_normal(rng);
        let v = (1.0 + c * x).powi(3);
        if v <= 0.0 {
            continue;
        }
        let u = rng.next_f64();
        if u < 1.0 - 0.0331 * x.powi(4) {
            return d * v;
        }
        if u.ln() < 0.5 * x * x + d * (1.0 - v + v.ln()) {
            return d * v;
        }
    }
}

// ── Special functions (pure Rust) ────────────────────────────────────────────

/// Natural log of the Gamma function via the Lanczos approximation (g = 7).
fn ln_gamma(x: f64) -> f64 {
    const COEF: [f64; 8] = [
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        // Reflection: Γ(x)Γ(1−x) = π / sin(πx).
        let pi = std::f64::consts::PI;
        (pi / (pi * x).sin()).ln() - ln_gamma(1.0 - x)
    } else {
        let x = x - 1.0;
        let mut a = 0.999_999_999_999_809_9;
        let t = x + 7.5;
        for (i, &c) in COEF.iter().enumerate() {
            a += c / (x + i as f64 + 1.0);
        }
        // 0.5·ln(2π)
        const LN_SQRT_2PI: f64 = 0.918_938_533_204_672_7;
        LN_SQRT_2PI + (x + 0.5) * t.ln() - t + a.ln()
    }
}

/// Regularized incomplete beta `Iₓ(a, b)` — the Beta CDF. Numerical-Recipes
/// continued fraction (`betacf`) with the standard symmetry pivot.
fn reg_inc_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let ln_beta = ln_gamma(a) + ln_gamma(b) - ln_gamma(a + b);
    let front = (a * x.ln() + b * (1.0 - x).ln() - ln_beta).exp();
    if x < (a + 1.0) / (a + b + 2.0) {
        front * betacf(a, b, x) / a
    } else {
        1.0 - front * betacf(b, a, 1.0 - x) / b
    }
}

/// Lentz's continued fraction for the incomplete beta.
fn betacf(a: f64, b: f64, x: f64) -> f64 {
    const MAXIT: usize = 200;
    const EPS: f64 = 3.0e-14;
    const FPMIN: f64 = 1.0e-300;
    let qab = a + b;
    let qap = a + 1.0;
    let qam = a - 1.0;
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < FPMIN {
        d = FPMIN;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..=MAXIT {
        let m = m as f64;
        let m2 = 2.0 * m;
        let mut aa = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < FPMIN {
            d = FPMIN;
        }
        c = 1.0 + aa / c;
        if c.abs() < FPMIN {
            c = FPMIN;
        }
        d = 1.0 / d;
        h *= d * c;
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < FPMIN {
            d = FPMIN;
        }
        c = 1.0 + aa / c;
        if c.abs() < FPMIN {
            c = FPMIN;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;
        if (del - 1.0).abs() < EPS {
            break;
        }
    }
    h
}

/// Beta `q`-quantile by bisection on the monotone CDF `reg_inc_beta`.
fn beta_quantile(q: f64, a: f64, b: f64) -> f64 {
    if a <= 0.0 || b <= 0.0 {
        return 0.0;
    }
    let (mut lo, mut hi) = (0.0_f64, 1.0_f64);
    for _ in 0..100 {
        let mid = 0.5 * (lo + hi);
        if reg_inc_beta(mid, a, b) < q {
            lo = mid;
        } else {
            hi = mid;
        }
        if hi - lo < 1e-12 {
            break;
        }
    }
    0.5 * (lo + hi)
}

/// Inverse standard-normal CDF (probit) via Acklam's rational approximation.
fn inv_norm_cdf(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239e0,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838e0,
        -2.549_732_539_343_734e0,
        4.374_664_141_464_968e0,
        2.938_163_982_698_783e0,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996e0,
        3.754_408_661_907_416e0,
    ];
    const PLOW: f64 = 0.02425;
    const PHIGH: f64 = 1.0 - PLOW;
    if p < PLOW {
        let qv = (-2.0 * p.ln()).sqrt();
        (((((C[0] * qv + C[1]) * qv + C[2]) * qv + C[3]) * qv + C[4]) * qv + C[5])
            / ((((D[0] * qv + D[1]) * qv + D[2]) * qv + D[3]) * qv + 1.0)
    } else if p <= PHIGH {
        let qv = p - 0.5;
        let r = qv * qv;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * qv
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let qv = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * qv + C[1]) * qv + C[2]) * qv + C[3]) * qv + C[4]) * qv + C[5])
            / ((((D[0] * qv + D[1]) * qv + D[2]) * qv + D[3]) * qv + 1.0)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-9;

    #[test]
    fn gaussian_moments_pdf_and_interval() {
        let d = Distribution::Gaussian {
            mean: 2.0,
            std: 3.0,
        };
        assert!((d.mean() - 2.0).abs() < EPS);
        assert!((d.variance() - 9.0).abs() < EPS);
        // Peak density = 1/(σ√(2π)).
        let want = 1.0 / (3.0 * (2.0 * std::f64::consts::PI).sqrt());
        assert!((d.pdf(2.0) - want).abs() < EPS);
        // 95% central interval ≈ mean ± 1.959964·σ.
        let (lo, hi) = d.credible_interval(0.95);
        assert!((lo - (2.0 - 1.959_963_984_540_054 * 3.0)).abs() < 1e-6);
        assert!((hi - (2.0 + 1.959_963_984_540_054 * 3.0)).abs() < 1e-6);
    }

    #[test]
    fn beta_moments_and_pdf() {
        let d = Distribution::Beta {
            alpha: 2.0,
            beta: 3.0,
        };
        // mean = a/(a+b) = 0.4; var = ab/((a+b)²(a+b+1)) = 6/150 = 0.04.
        assert!((d.mean() - 0.4).abs() < EPS);
        assert!((d.variance() - 0.04).abs() < EPS);
        // Beta(2,3) pdf(x) = 12 x (1-x)²; at x=0.5 → 12·0.5·0.25 = 1.5.
        assert!((d.pdf(0.5) - 1.5).abs() < 1e-6);
        // Interval is inside the support and ordered.
        let (lo, hi) = d.credible_interval(0.9);
        assert!(lo > 0.0 && hi < 1.0 && lo < hi);
        // Median ≈ CDF⁻¹(0.5) ~ 0.3857 for Beta(2,3).
        assert!((d.quantile(0.5) - 0.385_727).abs() < 1e-3);
    }

    #[test]
    fn categorical_index_moments_and_pmf() {
        let d = Distribution::Categorical {
            probs: vec![("a".into(), 0.2), ("b".into(), 0.3), ("c".into(), 0.5)],
        };
        // mean index = 0·0.2 + 1·0.3 + 2·0.5 = 1.3.
        assert!((d.mean() - 1.3).abs() < EPS);
        // var = Σ p(i-1.3)² = 0.2·1.69 + 0.3·0.09 + 0.5·0.49 = 0.61.
        assert!((d.variance() - 0.61).abs() < EPS);
        assert!((d.pmf("b") - 0.3).abs() < EPS);
        assert!((d.pmf("z") - 0.0).abs() < EPS);
        assert_eq!(d.pdf(1.0), 0.0);
    }

    #[test]
    fn empirical_moments_and_quantile() {
        let d = Distribution::Empirical {
            samples: vec![1.0, 2.0, 3.0, 4.0],
        };
        assert!((d.mean() - 2.5).abs() < EPS);
        // Population variance = (1.5²+0.5²+0.5²+1.5²)/4 = 5/4 = 1.25.
        assert!((d.variance() - 1.25).abs() < EPS);
        // Type-7 median of {1,2,3,4} = 2.5.
        assert!((d.quantile(0.5) - 2.5).abs() < EPS);
        let (lo, hi) = d.credible_interval(0.5);
        assert!(lo >= 1.0 && hi <= 4.0 && lo < hi);
    }

    #[test]
    fn sample_is_deterministic_given_seed() {
        for d in [
            Distribution::Gaussian {
                mean: 0.0,
                std: 1.0,
            },
            Distribution::Beta {
                alpha: 2.0,
                beta: 5.0,
            },
            Distribution::Categorical {
                probs: vec![("a".into(), 0.5), ("b".into(), 0.5)],
            },
            Distribution::Empirical {
                samples: vec![10.0, 20.0, 30.0],
            },
        ] {
            let a = d.sample(42);
            let b = d.sample(42);
            assert_eq!(a, b, "same seed must reproduce the same draw");
        }
        // A Beta sample stays inside its (0,1) support.
        let beta = Distribution::Beta {
            alpha: 2.0,
            beta: 5.0,
        };
        let s = beta.sample(7);
        assert!(s > 0.0 && s < 1.0);
        // A large Gaussian sample mean tracks the true mean deterministically.
        let g = Distribution::Gaussian {
            mean: 5.0,
            std: 2.0,
        };
        let n = 20_000;
        let sum: f64 = (0..n).map(|i| g.sample(i as u64)).sum();
        assert!((sum / n as f64 - 5.0).abs() < 0.1);
    }

    #[test]
    fn json_roundtrip_tagged() {
        let d = Distribution::Gaussian {
            mean: 1.5,
            std: 0.25,
        };
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains("\"kind\":\"gaussian\""));
        let back: Distribution = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }
}
