//! The regularised incomplete beta function and its inverse, deterministically.
//!
//! `I_x(a, b)` uses the classical continued fraction (modified Lentz) on the
//! side of `x = (a + 1) / (a + b + 2)` where it converges fast, with the
//! symmetry `I_x(a, b) = 1 - I_{1-x}(b, a)` on the other side. The quantile is a
//! bisection on `[0, 1]` that stops only when the midpoint reaches an adjacent
//! float, so its bits are a pure function of the inputs.

use crate::detkernel::math;
use crate::detkernel::{validate, Level, StatError, StatResult};

const CONTINUED_FRACTION_ITERATIONS: u32 = 100_000;
const CONTINUED_FRACTION_EPSILON: f64 = 1e-15;
const LENTZ_FLOOR: f64 = 1e-300;
/// Enough halvings of `[0, 1]` to reach adjacent floats anywhere in range.
const QUANTILE_BISECTIONS: u32 = 1100;

fn beta_shape(a: f64, b: f64) -> StatResult<()> {
    validate::parameter(a.is_finite() && a > 0.0, "a", "finite and > 0")?;
    validate::parameter(b.is_finite() && b > 0.0, "b", "finite and > 0")
}

/// `ln B(a, b)`.
pub fn ln_beta(a: f64, b: f64) -> f64 {
    math::ln_gamma(a) + math::ln_gamma(b) - math::ln_gamma(a + b)
}

fn floor_magnitude(value: f64) -> f64 {
    if value.abs() < LENTZ_FLOOR {
        LENTZ_FLOOR
    } else {
        value
    }
}

fn lentz_step(coefficient: f64, c: f64, d: f64) -> (f64, f64) {
    let d = 1.0 / floor_magnitude(1.0 + coefficient * d);
    let c = floor_magnitude(1.0 + coefficient / c);
    (c, d)
}

fn continued_fraction(a: f64, b: f64, x: f64) -> StatResult<f64> {
    let (qab, qap, qam) = (a + b, a + 1.0, a - 1.0);
    let mut c = 1.0;
    let mut d = 1.0 / floor_magnitude(1.0 - qab * x / qap);
    let mut h = d;
    for m in 1..=CONTINUED_FRACTION_ITERATIONS {
        let m = f64::from(m);
        let m2 = 2.0 * m;
        let even = m * (b - m) * x / ((qam + m2) * (a + m2));
        (c, d) = lentz_step(even, c, d);
        h *= d * c;
        let odd = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        (c, d) = lentz_step(odd, c, d);
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() < CONTINUED_FRACTION_EPSILON {
            return Ok(h);
        }
    }
    Err(StatError::NoConvergence {
        what: "incomplete beta continued fraction",
        iterations: CONTINUED_FRACTION_ITERATIONS,
    })
}

/// The regularised incomplete beta function `I_x(a, b)` for `x` in `[0, 1]`.
pub fn regularized_incomplete_beta(x: f64, a: f64, b: f64) -> StatResult<f64> {
    beta_shape(a, b)?;
    validate::all_unit_interval(&[x], "incomplete beta x")?;
    incomplete_beta_checked(x, a, b)
}

fn incomplete_beta_checked(x: f64, a: f64, b: f64) -> StatResult<f64> {
    if x == 0.0 || x == 1.0 {
        return Ok(x);
    }
    let front = math::exp(a * math::ln(x) + b * math::ln_1p(-x) - ln_beta(a, b));
    if x < (a + 1.0) / (a + b + 2.0) {
        Ok(front * continued_fraction(a, b, x)? / a)
    } else {
        Ok(1.0 - front * continued_fraction(b, a, 1.0 - x)? / b)
    }
}

/// The `p`-quantile of `Beta(a, b)`: the `x` with `I_x(a, b) = p`.
pub fn beta_quantile(p: f64, a: f64, b: f64) -> StatResult<f64> {
    beta_shape(a, b)?;
    validate::all_unit_interval(&[p], "beta quantile probability")?;
    if p == 0.0 || p == 1.0 {
        return Ok(p);
    }
    let (mut lower, mut upper) = (0.0f64, 1.0f64);
    for _ in 0..QUANTILE_BISECTIONS {
        let mid = lower + (upper - lower) * 0.5;
        if mid <= lower || mid >= upper {
            break;
        }
        if incomplete_beta_checked(mid, a, b)? < p {
            lower = mid;
        } else {
            upper = mid;
        }
    }
    Ok(lower + (upper - lower) * 0.5)
}

/// Equal-tailed `Beta(a, b)` interval with total tail mass `delta`.
pub fn beta_interval(a: f64, b: f64, delta: Level) -> StatResult<(f64, f64)> {
    let tail = delta.to_f64() * 0.5;
    Ok((beta_quantile(tail, a, b)?, beta_quantile(1.0 - tail, a, b)?))
}
