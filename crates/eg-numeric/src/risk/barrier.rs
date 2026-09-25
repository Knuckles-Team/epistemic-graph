//! Barrier-hit / time-to-exhaustion (EH-527, ANALYTICS-HARVEST AH-07).
//!
//! The probability that a drifting, diffusing quantity crosses a level within a
//! horizon — disk or quota exhaustion, error-budget burn, budget depletion, a
//! queue overflowing — as a first-passage distribution rather than the
//! deterministic straight-line crossing `predict_linear` gives.
//!
//! * **Arithmetic Brownian motion** `X_t = x0 + mu t + sigma W_t`. With `a` the
//!   distance to the level and `nu` the drift *toward* it, the first-passage time
//!   is inverse-Gaussian:
//!   `P(T <= t) = Phi((nu t - a) / (sigma sqrt t)) + exp(2 nu a / sigma^2) Phi((-a - nu t) / (sigma sqrt t))`.
//!   The second term is evaluated in log space, so a strong drift toward the level
//!   cannot overflow the exponential.
//! * **Geometric Brownian motion** is the same law on `ln X` (the log barrier):
//!   [`Dynamics::Geometric`] fits and evaluates on logarithms.
//! * **Terminal crossing** `P(X_t beyond level)` is reported beside it as the
//!   comparison a point forecast would give; it never exceeds the first passage.
//! * `sigma = 0` is the deterministic crossing at `a / nu` — exactly the time
//!   `predict_linear` extrapolates to — so the kernel degrades to the linear
//!   forecast instead of dividing by zero.
//!
//! Estimation is by maximum likelihood from increments over irregular spacing
//! (`mu = sum dx / sum dt`, `sigma^2 = mean((dx - mu dt)^2 / dt)`), with a
//! seeded parametric bootstrap for an interval on the hit probability. Every
//! transcendental is the pinned libm, and the bootstrap stream is ChaCha20, so a
//! result replays bit-identically on every release target.

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;

use crate::detkernel::{math, validate, StatError, StatResult};
use crate::stats::{norm_cdf, norm_log_cdf};

/// Bisection steps of the time-to-barrier quantile (well past f64 resolution).
const QUANTILE_STEPS: u32 = 200;
/// Horizon doublings tried while bracketing a quantile.
const BRACKET_DOUBLINGS: u32 = 200;

/// Whether the level is approached additively or multiplicatively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dynamics {
    /// Arithmetic Brownian motion on the values (bytes, counts, budget units).
    Arithmetic,
    /// Geometric Brownian motion: arithmetic on `ln value` (the log barrier).
    Geometric,
}

/// A fitted drift-diffusion, per unit of the time axis it was fitted on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DriftDiffusion {
    pub mu: f64,
    pub sigma: f64,
    /// Increments the fit used.
    pub n_increments: usize,
}

/// One barrier problem on the working (possibly logged) scale.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Barrier {
    /// The current value.
    pub x0: f64,
    /// The level whose crossing counts, on either side of `x0`.
    pub level: f64,
}

impl Barrier {
    /// Distance to the level and the drift toward it: `(a, nu)`.
    fn toward(self, mu: f64) -> (f64, f64) {
        if self.level >= self.x0 {
            (self.level - self.x0, mu)
        } else {
            (self.x0 - self.level, -mu)
        }
    }
}

/// `P(T <= t)`: the first-passage CDF of arithmetic Brownian motion.
pub fn first_passage_cdf(barrier: Barrier, fit: DriftDiffusion, t: f64) -> f64 {
    let (a, nu, scale) = match crossing_inputs(barrier, fit, t) {
        Ok(inputs) => inputs,
        Err(probability) => return probability,
    };
    let direct = norm_cdf((nu * t - a) / scale);
    let log_reflected =
        2.0 * nu * a / (fit.sigma * fit.sigma) + norm_log_cdf((-a - nu * t) / scale);
    (direct + math::exp(log_reflected)).clamp(0.0, 1.0)
}

/// The `sigma = 0` limit: the level is reached at `a / nu` or never.
fn deterministic_cdf(a: f64, nu: f64, t: f64) -> f64 {
    if nu > 0.0 && a / nu <= t {
        1.0
    } else {
        0.0
    }
}

/// `P(X_t >= level)` (or `<=` for a level below): the terminal-crossing comparison.
pub fn terminal_crossing(barrier: Barrier, fit: DriftDiffusion, t: f64) -> f64 {
    crossing_inputs(barrier, fit, t)
        .map(|(a, nu, scale)| norm_cdf((nu * t - a) / scale))
        .unwrap_or_else(|probability| probability)
}

/// Shared boundary cases and scale for both crossing probabilities.
fn crossing_inputs(barrier: Barrier, fit: DriftDiffusion, t: f64) -> Result<(f64, f64, f64), f64> {
    let (a, nu) = barrier.toward(fit.mu);
    if a <= 0.0 {
        return Err(1.0);
    }
    if t <= 0.0 {
        return Err(0.0);
    }
    if fit.sigma <= 0.0 {
        return Err(deterministic_cdf(a, nu, t));
    }
    Ok((a, nu, fit.sigma * t.sqrt()))
}

/// `P(T < infinity)`: one when drifting toward the level, `exp(2 nu a / sigma^2)`
/// when drifting away.
pub fn eventual_hit(barrier: Barrier, fit: DriftDiffusion) -> f64 {
    let (a, nu) = barrier.toward(fit.mu);
    let recurrent = nu > 0.0 || (nu == 0.0 && fit.sigma > 0.0);
    if a <= 0.0 || recurrent {
        return 1.0;
    }
    if fit.sigma <= 0.0 {
        return 0.0;
    }
    math::exp(2.0 * nu * a / (fit.sigma * fit.sigma)).min(1.0)
}

/// The `q`-quantile of the time to reach the level; `None` when the level is
/// reached with probability below `q` (drift away), so the quantile is infinite.
pub fn time_to_barrier_quantile(
    barrier: Barrier,
    fit: DriftDiffusion,
    q: f64,
) -> StatResult<Option<f64>> {
    validate::parameter(q > 0.0 && q < 1.0, "q", "0 < q < 1")?;
    if eventual_hit(barrier, fit) < q {
        return Ok(None);
    }
    let (a, nu) = barrier.toward(fit.mu);
    if a <= 0.0 {
        return Ok(Some(0.0));
    }
    if fit.sigma <= 0.0 {
        return Ok(Some(a / nu));
    }
    let Some(hi) = bracket(barrier, fit, q, initial_guess(a, nu, fit.sigma)) else {
        return Ok(None);
    };
    Ok(Some(bisect(barrier, fit, q, hi)))
}

/// A starting horizon on the time scale of the problem.
fn initial_guess(a: f64, nu: f64, sigma: f64) -> f64 {
    let drift_time = if nu > 0.0 { a / nu } else { f64::INFINITY };
    let diffusion_time = (a / sigma) * (a / sigma);
    drift_time.min(diffusion_time).max(f64::MIN_POSITIVE)
}

/// Double `t` until `P(T <= t) >= q`.
fn bracket(barrier: Barrier, fit: DriftDiffusion, q: f64, start: f64) -> Option<f64> {
    let mut t = start;
    for _ in 0..BRACKET_DOUBLINGS {
        if first_passage_cdf(barrier, fit, t) >= q {
            return Some(t);
        }
        t *= 2.0;
    }
    None
}

/// Bisect `(0, hi]` for the smallest `t` with `P(T <= t) >= q`.
fn bisect(barrier: Barrier, fit: DriftDiffusion, q: f64, hi: f64) -> f64 {
    let (mut lo, mut hi) = (0.0, hi);
    for _ in 0..QUANTILE_STEPS {
        let mid = 0.5 * (lo + hi);
        if mid <= lo || mid >= hi {
            break;
        }
        if first_passage_cdf(barrier, fit, mid) >= q {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    hi
}

/// Maximum-likelihood drift and volatility from `(time, value)` observations
/// in strictly increasing time order (at least three, so two increments).
pub fn fit_drift_diffusion(times: &[f64], values: &[f64]) -> StatResult<DriftDiffusion> {
    validate::same_len(times.len(), values.len(), "values")?;
    validate::all_finite(times, "times")?;
    validate::all_finite(values, "values")?;
    validate::parameter(times.len() >= 3, "observations", "at least 3")?;
    let steps: Vec<(f64, f64)> = times
        .windows(2)
        .zip(values.windows(2))
        .map(|(t, v)| (t[1] - t[0], v[1] - v[0]))
        .collect();
    if let Some(index) = steps.iter().position(|(dt, _)| *dt <= 0.0) {
        return Err(StatError::OutOfDomain {
            what: "times",
            index: index + 1,
            domain: "strictly increasing",
        });
    }
    Ok(fit_steps(&steps))
}

/// The MLE over `(dt, dx)` increments.
fn fit_steps(steps: &[(f64, f64)]) -> DriftDiffusion {
    let span: f64 = steps.iter().map(|(dt, _)| dt).sum();
    let moved: f64 = steps.iter().map(|(_, dx)| dx).sum();
    let mu = moved / span;
    let n = steps.len() as f64;
    let variance = steps
        .iter()
        .map(|(dt, dx)| (dx - mu * dt) * (dx - mu * dt) / dt)
        .sum::<f64>()
        / n;
    DriftDiffusion {
        mu,
        sigma: variance.max(0.0).sqrt(),
        n_increments: steps.len(),
    }
}

/// What a barrier estimate is asked for.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BarrierSpec {
    pub dynamics: Dynamics,
    pub level: f64,
    pub horizon: f64,
    /// Parametric-bootstrap replicates for the interval (0 = no interval).
    pub replicates: u32,
    pub seed: u64,
    /// Two-sided interval mass, e.g. `0.95`.
    pub confidence: f64,
}

/// A fitted barrier-hit estimate with its bootstrap interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BarrierEstimate {
    pub fit: DriftDiffusion,
    /// The problem on the working scale (logged under `Geometric`).
    pub barrier: Barrier,
    pub hit_probability: f64,
    pub hit_lower: f64,
    pub hit_upper: f64,
    pub terminal_probability: f64,
    pub eventual_probability: f64,
    pub already_breached: bool,
}

/// Fit `(times, values)` and estimate the probability of reaching `spec.level`
/// within `spec.horizon` after the last observation.
pub fn estimate_barrier_hit(
    times: &[f64],
    values: &[f64],
    spec: &BarrierSpec,
) -> StatResult<BarrierEstimate> {
    check_spec(spec)?;
    let working = working_values(values, spec.dynamics)?;
    let fit = fit_drift_diffusion(times, &working)?;
    let barrier = Barrier {
        x0: working[working.len() - 1],
        level: working_level(spec)?,
    };
    let hit = first_passage_cdf(barrier, fit, spec.horizon);
    let (hit_lower, hit_upper) =
        bootstrap_interval(times, barrier, fit, spec).unwrap_or((hit, hit));
    Ok(BarrierEstimate {
        fit,
        barrier,
        hit_probability: hit,
        hit_lower: hit_lower.min(hit),
        hit_upper: hit_upper.max(hit),
        terminal_probability: terminal_crossing(barrier, fit, spec.horizon),
        eventual_probability: eventual_hit(barrier, fit),
        already_breached: barrier.toward(fit.mu).0 <= 0.0,
    })
}

fn check_spec(spec: &BarrierSpec) -> StatResult<()> {
    validate::parameter(
        spec.horizon.is_finite() && spec.horizon >= 0.0,
        "horizon",
        "finite and >= 0",
    )?;
    validate::parameter(spec.level.is_finite(), "level", "finite")?;
    validate::parameter(
        spec.confidence > 0.0 && spec.confidence < 1.0,
        "confidence",
        "0 < confidence < 1",
    )
}

/// The values on the scale the dynamics are arithmetic on.
fn working_values(values: &[f64], dynamics: Dynamics) -> StatResult<Vec<f64>> {
    match dynamics {
        Dynamics::Arithmetic => Ok(values.to_vec()),
        Dynamics::Geometric => {
            if let Some(index) = values.iter().position(|v| *v <= 0.0) {
                return Err(StatError::OutOfDomain {
                    what: "values",
                    index,
                    domain: "positive (geometric dynamics)",
                });
            }
            Ok(values.iter().map(|v| math::ln(*v)).collect())
        }
    }
}

fn working_level(spec: &BarrierSpec) -> StatResult<f64> {
    match spec.dynamics {
        Dynamics::Arithmetic => Ok(spec.level),
        Dynamics::Geometric => {
            validate::parameter(spec.level > 0.0, "level", "positive (geometric dynamics)")?;
            Ok(math::ln(spec.level))
        }
    }
}

/// Percentile interval of the hit probability over parametric-bootstrap refits
/// (the same spacing, fitted drift and volatility, seeded ChaCha20 normals).
fn bootstrap_interval(
    times: &[f64],
    barrier: Barrier,
    fit: DriftDiffusion,
    spec: &BarrierSpec,
) -> Option<(f64, f64)> {
    if spec.replicates == 0 {
        return None;
    }
    let spacing: Vec<f64> = times.windows(2).map(|t| t[1] - t[0]).collect();
    let mut rng = ChaCha20Rng::seed_from_u64(spec.seed);
    let mut draws: Vec<f64> = (0..spec.replicates)
        .map(|_| {
            let steps = simulate_steps(&spacing, fit, &mut rng);
            first_passage_cdf(barrier, fit_steps(&steps), spec.horizon)
        })
        .collect();
    draws.sort_by(f64::total_cmp);
    let tail = 0.5 * (1.0 - spec.confidence);
    Some((percentile(&draws, tail), percentile(&draws, 1.0 - tail)))
}

/// One bootstrap path of increments.
fn simulate_steps(spacing: &[f64], fit: DriftDiffusion, rng: &mut ChaCha20Rng) -> Vec<(f64, f64)> {
    spacing
        .iter()
        .map(|dt| {
            (
                *dt,
                fit.mu * dt + fit.sigma * dt.sqrt() * standard_normal(rng),
            )
        })
        .collect()
}

/// A uniform on `(0, 1]` from 53 random bits.
fn unit_open(rng: &mut ChaCha20Rng) -> f64 {
    ((rng.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64
}

/// Box-Muller over the pinned libm.
fn standard_normal(rng: &mut ChaCha20Rng) -> f64 {
    let radius = (-2.0 * math::ln(unit_open(rng))).sqrt();
    radius * math::cos(std::f64::consts::TAU * unit_open(rng))
}

/// Nearest-rank percentile of sorted `values` at `p` in `[0, 1]`.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let last = sorted.len() - 1;
    let rank = (p * last as f64).round() as usize;
    sorted[rank.min(last)]
}

#[cfg(test)]
mod tests;
