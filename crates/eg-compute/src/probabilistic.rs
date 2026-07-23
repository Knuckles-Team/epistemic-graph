// CONCEPT:EG-KG.compute.probabilistic-reasoning — Probabilistic reasoning over distribution-valued properties.
//
// Bayesian-update / marginal / mixture / fusion helpers that operate on the
// `eg_types::Distribution` VALUE (CONCEPT:EG-KG.compute.probabilistic-reasoning). These generalize the scalar
// `eg:confidence` / noisy-OR belief model (CONCEPT:EG-KG.ontology.concept-13) into full
// distributions: instead of combining point confidences you update, marginalize,
// mix, and fuse whole distributions.
//
// This ships a deliberately TRACTABLE representative set — the closed-form
// conjugate posteriors plus moment-matched mixture / Gaussian fusion — all
// closed-form (no sampling), so it rides the pure `reasoning` feature with no
// heavy dependency. Follow-up (NOT here): a wire `Op::Probabilistic` to
// propagate uncertainty through the planner (deferred to avoid the concurrent
// wire.rs / eg-plan wave).

use eg_types::Distribution;

/// Observed evidence for a conjugate Bayesian update (CONCEPT:EG-KG.compute.probabilistic-reasoning).
#[derive(Clone, Debug, PartialEq)]
pub enum Evidence {
    /// Bernoulli trial counts — the sufficient statistic for a Beta prior.
    Bernoulli { successes: f64, failures: f64 },
    /// I.i.d. Gaussian observations with a KNOWN likelihood variance — the
    /// sufficient statistic for a Gaussian prior over the unknown mean.
    Gaussian {
        observations: Vec<f64>,
        known_variance: f64,
    },
}

/// Conjugate Bayesian update `posterior ∝ prior · likelihood` (CONCEPT:EG-KG.compute.probabilistic-reasoning).
///
/// Supported conjugate pairs:
/// * **Beta–Bernoulli:** `Beta(α, β)` + `Bernoulli{s, f}` ⇒ `Beta(α+s, β+f)`.
/// * **Gaussian–Gaussian (known variance):** `N(μ₀, σ₀²)` + `n` observations
///   with known likelihood variance `σ²` ⇒ posterior `N(μ₁, σ₁²)` where the
///   posterior precision `1/σ₁² = 1/σ₀² + n/σ²` and `μ₁ = σ₁²·(μ₀/σ₀² + Σx/σ²)`.
///
/// Any other `(prior, evidence)` shape is an `Err` (unsupported conjugate pair).
pub fn bayesian_update(prior: &Distribution, evidence: &Evidence) -> Result<Distribution, String> {
    match (prior, evidence) {
        (
            Distribution::Beta { alpha, beta },
            Evidence::Bernoulli {
                successes,
                failures,
            },
        ) => {
            if *successes < 0.0 || *failures < 0.0 {
                return Err("Bernoulli counts must be non-negative".into());
            }
            Ok(Distribution::Beta {
                alpha: alpha + successes,
                beta: beta + failures,
            })
        }
        (
            Distribution::Gaussian { mean, std },
            Evidence::Gaussian {
                observations,
                known_variance,
            },
        ) => {
            if *known_variance <= 0.0 {
                return Err("known likelihood variance must be positive".into());
            }
            if *std <= 0.0 {
                return Err("prior std must be positive".into());
            }
            let n = observations.len() as f64;
            let sum: f64 = observations.iter().sum();
            let prior_prec = 1.0 / (std * std);
            let like_prec = n / known_variance;
            let post_prec = prior_prec + like_prec;
            let post_var = 1.0 / post_prec;
            let post_mean = post_var * (mean * prior_prec + sum / known_variance);
            Ok(Distribution::Gaussian {
                mean: post_mean,
                std: post_var.sqrt(),
            })
        }
        _ => Err(format!(
            "unsupported conjugate pair: {:?} with the given evidence",
            std::mem::discriminant(prior)
        )),
    }
}

/// Marginalize the latent component out of a weighted mixture (CONCEPT:EG-KG.compute.probabilistic-reasoning):
/// `p(x) = Σᵢ wᵢ·pᵢ(x)`, returned moment-matched to a `Gaussian` via the law of
/// total expectation/variance
/// (`E = Σ wᵢμᵢ`, `Var = Σ wᵢ(σᵢ² + μᵢ²) − E²`). Weights are renormalized;
/// an empty mixture yields `N(0, 0)`.
///
/// This is the marginal of the joint `p(x, z)` where `z` is the component index
/// — i.e. it realizes both the `marginal` and the `mixture` primitive.
pub fn marginal(components: &[(f64, Distribution)]) -> Distribution {
    let total_w: f64 = components.iter().map(|(w, _)| w.max(0.0)).sum();
    if total_w <= 0.0 {
        return Distribution::Gaussian {
            mean: 0.0,
            std: 0.0,
        };
    }
    let mut mean = 0.0;
    let mut second = 0.0; // Σ wᵢ(σᵢ² + μᵢ²)
    for (w, d) in components {
        let w = w.max(0.0) / total_w;
        let mu = d.mean();
        let var = d.variance();
        mean += w * mu;
        second += w * (var + mu * mu);
    }
    let var = (second - mean * mean).max(0.0);
    Distribution::Gaussian {
        mean,
        std: var.sqrt(),
    }
}

/// Alias for [`marginal`]: collapse a weighted mixture to its moment-matched
/// marginal `Gaussian` (CONCEPT:EG-KG.compute.probabilistic-reasoning).
pub fn mixture(components: &[(f64, Distribution)]) -> Distribution {
    marginal(components)
}

/// Fuse two INDEPENDENT beliefs about the same quantity (CONCEPT:EG-KG.compute.probabilistic-reasoning) — the
/// distributional generalization of noisy-OR / confidence combination
/// (CONCEPT:EG-KG.ontology.concept-13):
/// * **Gaussian × Gaussian:** precision-weighted product (sensor fusion) —
///   `1/σ² = 1/σ₁² + 1/σ₂²`, `μ = σ²(μ₁/σ₁² + μ₂/σ₂²)`.
/// * **Beta × Beta:** pseudo-count pooling — `Beta(α₁+α₂−1, β₁+β₂−1)` (the
///   product of two Beta densities, clamped so parameters stay `≥ ε`).
///
/// Other combinations are an `Err`.
pub fn combine(a: &Distribution, b: &Distribution) -> Result<Distribution, String> {
    match (a, b) {
        (
            Distribution::Gaussian { mean: m1, std: s1 },
            Distribution::Gaussian { mean: m2, std: s2 },
        ) => {
            if *s1 <= 0.0 || *s2 <= 0.0 {
                return Err("Gaussian std must be positive to fuse".into());
            }
            let p1 = 1.0 / (s1 * s1);
            let p2 = 1.0 / (s2 * s2);
            let var = 1.0 / (p1 + p2);
            Ok(Distribution::Gaussian {
                mean: var * (m1 * p1 + m2 * p2),
                std: var.sqrt(),
            })
        }
        (
            Distribution::Beta {
                alpha: a1,
                beta: b1,
            },
            Distribution::Beta {
                alpha: a2,
                beta: b2,
            },
        ) => Ok(Distribution::Beta {
            alpha: (a1 + a2 - 1.0).max(1e-9),
            beta: (b1 + b2 - 1.0).max(1e-9),
        }),
        _ => Err("combine supports Gaussian×Gaussian or Beta×Beta only".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-9;

    #[test]
    fn beta_bernoulli_update_matches_posterior() {
        // Beta(2,2) prior + 3 successes, 1 failure ⇒ Beta(5,3).
        let prior = Distribution::Beta {
            alpha: 2.0,
            beta: 2.0,
        };
        let post = bayesian_update(
            &prior,
            &Evidence::Bernoulli {
                successes: 3.0,
                failures: 1.0,
            },
        )
        .unwrap();
        match post {
            Distribution::Beta { alpha, beta } => {
                assert!((alpha - 5.0).abs() < EPS);
                assert!((beta - 3.0).abs() < EPS);
            }
            _ => panic!("expected Beta posterior"),
        }
    }

    #[test]
    fn gaussian_gaussian_update_matches_posterior() {
        // Prior N(0, 1²); one observation x=2 with known variance 1.
        // precision = 1/1 + 1/1 = 2 ⇒ var = 0.5; mean = 0.5·(0 + 2) = 1.
        let prior = Distribution::Gaussian {
            mean: 0.0,
            std: 1.0,
        };
        let post = bayesian_update(
            &prior,
            &Evidence::Gaussian {
                observations: vec![2.0],
                known_variance: 1.0,
            },
        )
        .unwrap();
        assert!((post.mean() - 1.0).abs() < EPS);
        assert!((post.variance() - 0.5).abs() < EPS);
    }

    #[test]
    fn gaussian_update_two_observations() {
        // Prior N(0,1); observe {1,3}, known var 1 ⇒ precision 1+2=3,
        // var=1/3, mean = (1/3)·(0 + 4) = 4/3.
        let prior = Distribution::Gaussian {
            mean: 0.0,
            std: 1.0,
        };
        let post = bayesian_update(
            &prior,
            &Evidence::Gaussian {
                observations: vec![1.0, 3.0],
                known_variance: 1.0,
            },
        )
        .unwrap();
        assert!((post.mean() - 4.0 / 3.0).abs() < 1e-9);
        assert!((post.variance() - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn mixture_moment_matches() {
        // Equal mix of N(0,1) and N(10,1): mean 5, var = 1 + 25 = 26.
        let comps = vec![
            (
                0.5,
                Distribution::Gaussian {
                    mean: 0.0,
                    std: 1.0,
                },
            ),
            (
                0.5,
                Distribution::Gaussian {
                    mean: 10.0,
                    std: 1.0,
                },
            ),
        ];
        let m = mixture(&comps);
        assert!((m.mean() - 5.0).abs() < EPS);
        assert!((m.variance() - 26.0).abs() < EPS);
    }

    #[test]
    fn combine_fuses_gaussians() {
        // Fuse N(0,1) and N(2,1): precision 1+1=2 ⇒ var 0.5, mean 1.
        let fused = combine(
            &Distribution::Gaussian {
                mean: 0.0,
                std: 1.0,
            },
            &Distribution::Gaussian {
                mean: 2.0,
                std: 1.0,
            },
        )
        .unwrap();
        assert!((fused.mean() - 1.0).abs() < EPS);
        assert!((fused.variance() - 0.5).abs() < EPS);
    }

    #[test]
    fn unsupported_pair_errors() {
        let prior = Distribution::Gaussian {
            mean: 0.0,
            std: 1.0,
        };
        let err = bayesian_update(
            &prior,
            &Evidence::Bernoulli {
                successes: 1.0,
                failures: 0.0,
            },
        );
        assert!(err.is_err());
    }
}
