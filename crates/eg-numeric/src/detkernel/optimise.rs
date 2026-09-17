//! Deterministic one-dimensional convex minimisation.
//!
//! A differentiable convex function has a nondecreasing derivative, so its
//! minimiser is where the derivative changes sign. The routines bisect on the
//! derivative's sign with a fixed step budget and stop early only when the
//! midpoint can no longer move (adjacent floats). There is no tolerance test on
//! a float difference, so the iterate sequence, and therefore the result bits,
//! are a pure function of the inputs.

use super::error::{StatError, StatResult};
use super::validate;

/// Bisection steps used by the calibration fits: enough to reach adjacent
/// floats from any bracket narrower than `2^60`.
pub const DEFAULT_BISECTION_STEPS: u32 = 128;

/// Doublings allowed while searching for a sign change.
pub const DEFAULT_MAX_EXPANSIONS: u32 = 60;

fn derivative_at(derivative: &impl Fn(f64) -> f64, x: f64) -> StatResult<f64> {
    let value = derivative(x);
    if value.is_nan() {
        return Err(StatError::NonFinite {
            what: "convex derivative",
            index: 0,
        });
    }
    Ok(value)
}

fn bisect_sign_change(
    derivative: &impl Fn(f64) -> f64,
    mut lower: f64,
    mut upper: f64,
    steps: u32,
) -> StatResult<f64> {
    for _ in 0..steps {
        let mid = lower + (upper - lower) * 0.5;
        if mid <= lower || mid >= upper {
            break;
        }
        if derivative_at(derivative, mid)? > 0.0 {
            upper = mid;
        } else {
            lower = mid;
        }
    }
    Ok(lower + (upper - lower) * 0.5)
}

/// Minimise a convex function on `[lower, upper]` given its derivative.
pub fn minimise_convex_bounded(
    derivative: impl Fn(f64) -> f64,
    lower: f64,
    upper: f64,
    steps: u32,
) -> StatResult<f64> {
    validate::all_finite(&[lower, upper], "convex bracket")?;
    validate::parameter(lower < upper, "convex bracket", "lower < upper")?;
    if derivative_at(&derivative, lower)? >= 0.0 {
        return Ok(lower);
    }
    if derivative_at(&derivative, upper)? <= 0.0 {
        return Ok(upper);
    }
    bisect_sign_change(&derivative, lower, upper, steps)
}

/// Minimise an unconstrained convex function from `start`: double a step in the
/// descent direction until the derivative changes sign, then bisect.
pub fn minimise_convex_unbounded(
    derivative: impl Fn(f64) -> f64,
    start: f64,
    initial_step: f64,
    steps: u32,
) -> StatResult<f64> {
    validate::all_finite(&[start, initial_step], "convex start")?;
    validate::parameter(initial_step > 0.0, "initial_step", "initial_step > 0")?;
    let slope = derivative_at(&derivative, start)?;
    if slope == 0.0 {
        return Ok(start);
    }
    let direction = if slope < 0.0 { 1.0 } else { -1.0 };
    let mut step = initial_step;
    for _ in 0..DEFAULT_MAX_EXPANSIONS {
        let far = start + direction * step;
        let far_slope = derivative_at(&derivative, far)?;
        if far_slope * direction >= 0.0 {
            let (lower, upper) = if direction > 0.0 {
                (start, far)
            } else {
                (far, start)
            };
            return bisect_sign_change(&derivative, lower, upper, steps);
        }
        step *= 2.0;
    }
    Err(StatError::NoConvergence {
        what: "convex bracket expansion",
        iterations: DEFAULT_MAX_EXPANSIONS,
    })
}
