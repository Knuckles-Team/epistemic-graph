//! Q32.32 fixed-point kernels for the resident scorer (EH-300).
//!
//! A value is an `i64` read as `value / 2^32`. Products and dot products
//! accumulate in `i128` and are shifted back once, rounding half up; a result
//! outside `i64` saturates. There is no float anywhere in this module, so every
//! function is a pure function of its integer inputs on every host.
//!
//! `exp` is range-reduced to base 2: `e^x = 2^(x log2 e) = 2^k 2^f` with `k`
//! an integer and `f` in `[0, 1)`; `2^f` is the degree-10 Taylor polynomial of
//! `e^(f ln 2)` evaluated by Horner over integer coefficients, and `2^k` is a
//! shift. Its error is below `2^-28` on `[0, 1)`, far under the calibration's
//! resolution, and -- the point -- identical everywhere.

use eg_types::decision::{QuantScaleTag, QuantisedValue};

/// Fractional bits.
pub const FRAC_BITS: u32 = 32;
/// `1.0`.
pub const ONE: i64 = 1 << FRAC_BITS;
/// `log2(e)` on Q32.
pub const LOG2_E: i64 = 6_196_328_019;
/// `(ln 2)^n / n!` on Q32, `n = 0..=10`.
const EXP2_COEFFICIENTS: [i64; 11] = [
    4_294_967_296,
    2_977_044_472,
    1_031_764_991,
    238_388_332,
    41_309_550,
    5_726_720,
    661_577,
    65_510,
    5_676,
    437,
    30,
];
/// Past this many halvings `2^f` is below one unit in the last place.
const SHIFT_LIMIT: i64 = 62;
/// `10^12`: the `Pico` scale's denominator.
const PICO: i128 = 1_000_000_000_000;

/// Clamp an `i128` into `i64`.
pub fn saturate(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Shift an accumulated `Q64` product back onto `Q32`, rounding half up.
fn rescale(product: i128) -> i64 {
    saturate(product.saturating_add(1_i128 << (FRAC_BITS - 1)) >> FRAC_BITS)
}

/// `a * b`.
pub fn mul(a: i64, b: i64) -> i64 {
    rescale(i128::from(a) * i128::from(b))
}

/// `sum a_i b_i`, accumulated exactly and rounded once.
pub fn dot(a: &[i64], b: &[i64]) -> i64 {
    let mut total: i128 = 0;
    for (x, y) in a.iter().zip(b) {
        total = total.saturating_add(i128::from(*x) * i128::from(*y));
    }
    rescale(total)
}

/// `a + b`, saturating.
pub fn add(a: i64, b: i64) -> i64 {
    a.saturating_add(b)
}

/// `2^f` for `f` in `[0, 1)`.
fn exp2_fraction(fraction: i64) -> i64 {
    let mut acc = EXP2_COEFFICIENTS[EXP2_COEFFICIENTS.len() - 1];
    for coefficient in EXP2_COEFFICIENTS[..EXP2_COEFFICIENTS.len() - 1]
        .iter()
        .rev()
    {
        acc = mul(acc, fraction) + coefficient;
    }
    acc
}

/// `e^x` for `x <= 0`; a positive `x` is read as `0`.
pub fn exp_non_positive(x: i64) -> i64 {
    let y = mul(x.min(0), LOG2_E);
    let whole = y >> FRAC_BITS;
    let fraction = y & (ONE - 1);
    let halvings = -whole;
    if halvings > SHIFT_LIMIT {
        return 0;
    }
    exp2_fraction(fraction) >> halvings
}

/// The softmax of `logits`: `(e_i << 32) / sum e` by integer division, with
/// every argument shifted by the maximum first. Empty in, empty out.
pub fn softmax(logits: &[i64]) -> Vec<i64> {
    let Some(&max) = logits.iter().max() else {
        return Vec::new();
    };
    let exps: Vec<i64> = logits
        .iter()
        .map(|&z| exp_non_positive(z.saturating_sub(max)))
        .collect();
    let total: i128 = exps.iter().map(|&e| i128::from(e)).sum::<i128>().max(1);
    exps.iter()
        .map(|&e| saturate((i128::from(e) << FRAC_BITS) / total))
        .collect()
}

/// A wire fixed-point value on Q32 (a `Pico` value is converted by exact
/// integer division, rounding toward negative infinity).
pub fn to_q32(value: QuantisedValue) -> i64 {
    match value.scale {
        QuantScaleTag::Q32 => value.value,
        QuantScaleTag::Pico => saturate((i128::from(value.value) << FRAC_BITS).div_euclid(PICO)),
    }
}

/// A Q32 value as the exact `f64` it denotes (every Q32 value with magnitude
/// below `2^21` is exactly representable, and the scorer's outputs are).
pub fn to_f64(value: i64) -> f64 {
    value as f64 / ONE as f64
}
