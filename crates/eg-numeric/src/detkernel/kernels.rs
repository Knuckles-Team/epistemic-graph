//! Deterministic probability kernels: log-sum-exp, softmax, log-softmax,
//! entropy, sigmoid, softplus, logit, and the standard normal CDF and quantile.
//!
//! All of them use only IEEE basic operations, correctly rounded `abs`, the
//! pinned transcendentals in [`super::math`], and serial left-to-right sums, so
//! their bits are identical on every release target.

use super::error::{StatError, StatResult};
use super::math;
use super::reduce::serial_sum;
use super::validate;

/// `ln Σ exp(v_i)`, shifted by the maximum for stability.
pub fn log_sum_exp(values: &[f64]) -> StatResult<f64> {
    validate::non_empty(values, "log_sum_exp input")?;
    validate::all_finite(values, "log_sum_exp input")?;
    Ok(log_sum_exp_finite(values))
}

/// [`log_sum_exp`] for an input already known to be non-empty and finite.
pub(crate) fn log_sum_exp_finite(values: &[f64]) -> f64 {
    let top = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let mut total = 0.0;
    for value in values {
        total += math::exp(value - top);
    }
    top + math::ln(total)
}

/// `softmax(v)_i = exp(v_i - lse(v))`.
pub fn softmax(values: &[f64]) -> StatResult<Vec<f64>> {
    let lse = log_sum_exp(values)?;
    Ok(values.iter().map(|value| math::exp(value - lse)).collect())
}

/// `log_softmax(v)_i = v_i - lse(v)`.
pub fn log_softmax(values: &[f64]) -> StatResult<Vec<f64>> {
    let lse = log_sum_exp(values)?;
    Ok(values.iter().map(|value| value - lse).collect())
}

/// Shannon entropy in nats of a probability vector, with `0 ln 0 = 0`.
pub fn entropy(probabilities: &[f64]) -> StatResult<f64> {
    validate::probability_vector(probabilities, "entropy input")?;
    let terms: Vec<f64> = probabilities
        .iter()
        .map(|&p| if p > 0.0 { -p * math::ln(p) } else { 0.0 })
        .collect();
    Ok(serial_sum(&terms))
}

/// Logistic sigmoid `1 / (1 + e^-x)`, evaluated on the side that does not
/// overflow. NaN propagates.
pub fn sigmoid(x: f64) -> f64 {
    if x >= 0.0 {
        1.0 / (1.0 + math::exp(-x))
    } else {
        let e = math::exp(x);
        e / (1.0 + e)
    }
}

/// `softplus(x) = ln(1 + e^x)` without overflow.
pub fn softplus(x: f64) -> f64 {
    if x > 0.0 {
        x + math::ln_1p(math::exp(-x))
    } else {
        math::ln_1p(math::exp(x))
    }
}

/// `ln sigmoid(x) = -softplus(-x)`.
pub fn log_sigmoid(x: f64) -> f64 {
    -softplus(-x)
}

/// `logit(p) = ln(p / (1 - p))` for `p` strictly inside `(0, 1)`.
pub fn logit(p: f64) -> StatResult<f64> {
    if !(p > 0.0 && p < 1.0) {
        return Err(StatError::OutOfDomain {
            what: "logit input",
            index: 0,
            domain: "(0, 1)",
        });
    }
    Ok(math::ln(p) - math::ln_1p(-p))
}

/// The standard normal CDF `Φ(x) = erfc(-x / √2) / 2`.
pub fn normal_cdf(x: f64) -> f64 {
    0.5 * math::erfc(-x * std::f64::consts::FRAC_1_SQRT_2)
}

/// Enough halvings of `[-40, 40]` to reach adjacent floats anywhere in range.
const NORMAL_QUANTILE_BISECTIONS: u32 = 1100;

/// The standard normal quantile `Φ⁻¹(p)` for `p` strictly inside `(0, 1)`: a bisection
/// on `[-40, 40]` that stops only when the midpoint reaches an adjacent float, so its
/// bits are a pure function of `p` (the same scheme as the beta quantile).
pub fn normal_quantile(p: f64) -> StatResult<f64> {
    if !(p > 0.0 && p < 1.0) {
        return Err(StatError::OutOfDomain {
            what: "normal quantile probability",
            index: 0,
            domain: "(0, 1)",
        });
    }
    let (mut lower, mut upper) = (-40.0f64, 40.0f64);
    for _ in 0..NORMAL_QUANTILE_BISECTIONS {
        let mid = lower + (upper - lower) * 0.5;
        if mid <= lower || mid >= upper {
            break;
        }
        if normal_cdf(mid) < p {
            lower = mid;
        } else {
            upper = mid;
        }
    }
    Ok(lower + (upper - lower) * 0.5)
}
