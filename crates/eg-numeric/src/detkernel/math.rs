//! The only sanctioned transcendental functions for the statistics modules.
//!
//! Each wrapper calls the exactly pinned pure-Rust `libm` (a port of musl libm,
//! built with `force-soft-floats`), so the result bits are the same on every
//! release target. The std `f64` methods are banned for this crate through its
//! `clippy.toml`.

/// `e^x`.
#[inline]
pub fn exp(x: f64) -> f64 {
    libm::exp(x)
}

/// `e^x - 1`, accurate for small `x`.
#[inline]
pub fn exp_m1(x: f64) -> f64 {
    libm::expm1(x)
}

/// Natural logarithm.
#[inline]
pub fn ln(x: f64) -> f64 {
    libm::log(x)
}

/// `ln(1 + x)`, accurate for small `x`.
#[inline]
pub fn ln_1p(x: f64) -> f64 {
    libm::log1p(x)
}

/// `x^y`.
#[inline]
pub fn pow(x: f64, y: f64) -> f64 {
    libm::pow(x, y)
}

/// Hyperbolic tangent.
#[inline]
pub fn tanh(x: f64) -> f64 {
    libm::tanh(x)
}

/// `ln |Γ(x)|`.
#[inline]
pub fn ln_gamma(x: f64) -> f64 {
    libm::lgamma(x)
}
