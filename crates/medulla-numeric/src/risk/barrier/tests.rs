//! Oracles for the barrier kernels: seeded Monte Carlo with the exact
//! Brownian-bridge crossing correction, the `sigma -> 0` linear limit, symmetry,
//! quantile inversion and the MLE on simulated paths.

use super::*;

fn fit(mu: f64, sigma: f64) -> DriftDiffusion {
    DriftDiffusion {
        mu,
        sigma,
        n_increments: 0,
    }
}

/// Unbiased Monte Carlo of `P(T <= horizon)`: Euler paths plus, per step, the
/// exact probability that the Brownian bridge between the two endpoints
/// touched the level (so discrete monitoring does not bias the estimate).
fn monte_carlo_hit(
    barrier: Barrier,
    drift: DriftDiffusion,
    horizon: f64,
    paths: u32,
) -> (f64, f64) {
    let steps = 200u32;
    let dt = horizon / f64::from(steps);
    let spacing = vec![dt; steps as usize];
    let mut rng = ChaCha20Rng::seed_from_u64(7);
    let mut total = 0.0;
    for _ in 0..paths {
        let mut x = barrier.x0;
        let mut survive = 1.0;
        for (_, dx) in simulate_steps(&spacing, drift, &mut rng) {
            let next = x + dx;
            survive *= 1.0 - bridge_touch(x, next, barrier.level, drift.sigma, dt);
            x = next;
        }
        total += 1.0 - survive;
    }
    let p = total / f64::from(paths);
    (p, (p * (1.0 - p) / f64::from(paths)).sqrt())
}

fn bridge_touch(x1: f64, x2: f64, level: f64, sigma: f64, dt: f64) -> f64 {
    let (d1, d2) = (level - x1, level - x2);
    if d1 <= 0.0 || d2 <= 0.0 {
        return 1.0;
    }
    math::exp(-2.0 * d1 * d2 / (sigma * sigma * dt))
}

#[test]
fn closed_form_matches_bridge_corrected_monte_carlo() {
    let barrier = Barrier {
        x0: 0.0,
        level: 1.0,
    };
    for (mu, sigma) in [(0.4, 0.8), (-0.2, 1.1), (0.0, 0.5)] {
        let drift = fit(mu, sigma);
        let exact = first_passage_cdf(barrier, drift, 2.0);
        let (mc, se) = monte_carlo_hit(barrier, drift, 2.0, 20_000);
        assert!(
            (exact - mc).abs() < 4.0 * se + 1e-3,
            "mu={mu} sigma={sigma}: closed form {exact} vs MC {mc} (se {se})"
        );
    }
}

#[test]
fn vanishing_volatility_is_the_linear_crossing() {
    // Exactly linear data: the fit has sigma = 0 and the time to the level is
    // the straight-line extrapolation predict_linear would give.
    let times = [0.0, 10.0, 20.0, 30.0];
    let values = [100.0, 80.0, 60.0, 40.0];
    let drift = fit_drift_diffusion(&times, &values).unwrap();
    assert_eq!(drift.mu, -2.0);
    assert_eq!(drift.sigma, 0.0);
    let barrier = Barrier {
        x0: 40.0,
        level: 0.0,
    };
    let crossing = time_to_barrier_quantile(barrier, drift, 0.5)
        .unwrap()
        .unwrap();
    assert_eq!(crossing, 20.0);
    assert_eq!(values[3] + drift.mu * crossing, barrier.level);
    assert_eq!(first_passage_cdf(barrier, drift, 19.999), 0.0);
    assert_eq!(first_passage_cdf(barrier, drift, 20.0), 1.0);
    // A vanishing sigma converges to the same crossing time.
    let median = time_to_barrier_quantile(barrier, fit(-2.0, 1e-4), 0.5)
        .unwrap()
        .unwrap();
    assert!((median - 20.0).abs() < 1e-3, "median {median}");
}

#[test]
fn a_level_below_mirrors_a_level_above() {
    let up = first_passage_cdf(
        Barrier {
            x0: 0.0,
            level: 2.0,
        },
        fit(0.3, 0.9),
        5.0,
    );
    let down = first_passage_cdf(
        Barrier {
            x0: 0.0,
            level: -2.0,
        },
        fit(-0.3, 0.9),
        5.0,
    );
    assert!((up - down).abs() < 1e-15);
}

#[test]
fn breached_and_receding_edges() {
    let breached = Barrier {
        x0: 5.0,
        level: 5.0,
    };
    assert_eq!(first_passage_cdf(breached, fit(-1.0, 1.0), 0.0), 1.0);
    assert_eq!(
        time_to_barrier_quantile(breached, fit(-1.0, 1.0), 0.9).unwrap(),
        Some(0.0)
    );
    let receding = Barrier {
        x0: 0.0,
        level: 1.0,
    };
    let away = fit(-0.5, 1.0);
    let eventual = eventual_hit(receding, away);
    assert!((eventual - math::exp(-1.0)).abs() < 1e-15);
    assert!(first_passage_cdf(receding, away, 1e6) <= eventual + 1e-12);
    assert_eq!(time_to_barrier_quantile(receding, away, 0.5).unwrap(), None);
    assert!(time_to_barrier_quantile(receding, away, 0.2)
        .unwrap()
        .is_some());
}

#[test]
fn first_passage_dominates_terminal_and_grows_with_time() {
    let barrier = Barrier {
        x0: 0.0,
        level: 1.5,
    };
    let drift = fit(0.1, 0.7);
    let mut previous = 0.0;
    for step in 1..50 {
        let t = f64::from(step) * 0.2;
        let hit = first_passage_cdf(barrier, drift, t);
        assert!(hit >= previous - 1e-15);
        assert!(hit + 1e-15 >= terminal_crossing(barrier, drift, t));
        previous = hit;
    }
}

#[test]
fn quantile_inverts_the_cdf() {
    let barrier = Barrier {
        x0: 3.0,
        level: 0.0,
    };
    let drift = fit(-0.25, 0.6);
    for q in [0.05, 0.5, 0.95] {
        let t = time_to_barrier_quantile(barrier, drift, q)
            .unwrap()
            .unwrap();
        assert!(
            (first_passage_cdf(barrier, drift, t) - q).abs() < 1e-9,
            "q={q}"
        );
    }
}

#[test]
fn mle_recovers_simulated_parameters() {
    let spacing: Vec<f64> = (0..4_000).map(|i| 0.5 + f64::from(i % 3) * 0.25).collect();
    let mut rng = ChaCha20Rng::seed_from_u64(11);
    let steps = simulate_steps(&spacing, fit(0.3, 1.2), &mut rng);
    let fitted = fit_steps(&steps);
    let span: f64 = spacing.iter().sum();
    assert!(
        (fitted.mu - 0.3).abs() < 4.0 * 1.2 / span.sqrt(),
        "mu {}",
        fitted.mu
    );
    assert!((fitted.sigma - 1.2).abs() < 0.05, "sigma {}", fitted.sigma);
}

#[test]
fn geometric_is_arithmetic_on_logs_and_the_interval_brackets() {
    let times: Vec<f64> = (0..40).map(f64::from).collect();
    let values: Vec<f64> = times
        .iter()
        .map(|t| 100.0 * math::exp(-0.01 * t + 0.02 * math::cos(*t)))
        .collect();
    let spec = BarrierSpec {
        dynamics: Dynamics::Geometric,
        level: 50.0,
        horizon: 60.0,
        replicates: 400,
        seed: 3,
        confidence: 0.9,
    };
    let geometric = estimate_barrier_hit(&times, &values, &spec).unwrap();
    let logs: Vec<f64> = values.iter().map(|v| math::ln(*v)).collect();
    let arithmetic = estimate_barrier_hit(
        &times,
        &logs,
        &BarrierSpec {
            dynamics: Dynamics::Arithmetic,
            level: math::ln(50.0),
            ..spec
        },
    )
    .unwrap();
    assert_eq!(geometric, arithmetic);
    assert!(geometric.hit_lower <= geometric.hit_probability);
    assert!(geometric.hit_probability <= geometric.hit_upper);
    assert!(geometric.hit_probability >= geometric.terminal_probability);
    let again = estimate_barrier_hit(&times, &values, &spec).unwrap();
    assert_eq!(geometric, again, "the bootstrap is seeded");
}

#[test]
fn refuses_bad_input() {
    assert!(fit_drift_diffusion(&[0.0, 1.0], &[1.0, 2.0]).is_err());
    assert!(fit_drift_diffusion(&[0.0, 1.0, 1.0], &[1.0, 2.0, 3.0]).is_err());
    let spec = BarrierSpec {
        dynamics: Dynamics::Geometric,
        level: 1.0,
        horizon: 1.0,
        replicates: 0,
        seed: 0,
        confidence: 0.95,
    };
    assert!(estimate_barrier_hit(&[0.0, 1.0, 2.0], &[1.0, -1.0, 2.0], &spec).is_err());
    assert!(time_to_barrier_quantile(
        Barrier {
            x0: 0.0,
            level: 1.0
        },
        fit(1.0, 1.0),
        1.0
    )
    .is_err());
}
