//! Exact integer arithmetic for the market kernels.
//!
//! Every division rounds half away from zero, computed in `i128`, so a result
//! never depends on the target's floating-point unit. `MILLI` converts ticks to
//! the milli-tick unit indicators are expressed in.

use super::{MarketError, MarketResult, OVERFLOW};

/// Milli-ticks per tick.
pub const MILLI: i64 = eg_types::compute_result::market::MILLI_TICKS_PER_TICK;

/// `numerator / denominator` rounded half away from zero; `denominator > 0`.
pub fn div_round(numerator: i128, denominator: i128) -> i128 {
    debug_assert!(denominator > 0);
    let doubled = numerator.abs() * 2 + denominator;
    let magnitude = doubled / (denominator * 2);
    if numerator < 0 {
        -magnitude
    } else {
        magnitude
    }
}

/// Narrow an intermediate back to `i64`, refusing overflow.
pub fn narrow(value: i128, what: &str) -> MarketResult<i64> {
    i64::try_from(value).map_err(|_| MarketError::new(OVERFLOW, format!("{what} overflows i64")))
}

/// Ticks to milli-ticks.
pub fn to_milli(ticks: i64) -> MarketResult<i64> {
    ticks
        .checked_mul(MILLI)
        .ok_or_else(|| MarketError::new(OVERFLOW, "price overflows milli-ticks"))
}

/// `a + b` with overflow refused.
pub fn add(a: i64, b: i64, what: &str) -> MarketResult<i64> {
    a.checked_add(b)
        .ok_or_else(|| MarketError::new(OVERFLOW, format!("{what} overflows i64")))
}

/// The largest integer an `f64` holds exactly: store fields stay within it.
pub const EXACT_F64: i64 = 1 << 53;

/// `value` as an exactly representable `f64`.
pub fn exact_f64(value: i64, what: &str) -> MarketResult<f64> {
    if value.unsigned_abs() > EXACT_F64 as u64 {
        return Err(MarketError::new(
            OVERFLOW,
            format!("{what} exceeds the exact f64 range"),
        ));
    }
    Ok(value as f64)
}

/// An `f64` store field back to its exact integer.
pub fn exact_i64(value: f64, what: &str) -> MarketResult<i64> {
    let in_range = value.is_finite() && value.abs() <= EXACT_F64 as f64;
    if !in_range || value.fract() != 0.0 {
        return Err(MarketError::new(
            OVERFLOW,
            format!("{what} is not an exact integer"),
        ));
    }
    Ok(value as i64)
}

/// Basis points of `delta` relative to `base` (`base != 0`), rounded.
pub fn basis_points(delta: i64, base: i64) -> i64 {
    let scaled = div_round(i128::from(delta) * 10_000, i128::from(base.unsigned_abs()));
    i64::try_from(scaled).unwrap_or(if scaled < 0 { i64::MIN } else { i64::MAX })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn division_rounds_half_away_from_zero() {
        assert_eq!(div_round(5, 2), 3);
        assert_eq!(div_round(-5, 2), -3);
        assert_eq!(div_round(4, 3), 1);
        assert_eq!(div_round(-4, 3), -1);
        assert_eq!(div_round(7, 7), 1);
        assert_eq!(div_round(0, 9), 0);
    }

    #[test]
    fn store_values_round_trip_only_when_exact() {
        assert_eq!(
            exact_i64(exact_f64(EXACT_F64, "v").unwrap(), "v"),
            Ok(EXACT_F64)
        );
        assert!(exact_f64(EXACT_F64 + 1, "v").is_err());
        assert!(exact_i64(1.5, "v").is_err());
        assert!(exact_i64(f64::NAN, "v").is_err());
        assert_eq!(basis_points(-50, 1_000), -500);
    }
}
