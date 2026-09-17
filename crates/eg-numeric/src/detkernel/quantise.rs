//! Fixed-point quantisation for digests.
//!
//! Committed digests are never taken over raw `f64` bytes. A value is scaled by
//! a power (`1e12` or `2^32`), rounded half-to-even and stored as `i64`; the
//! canonical encoding is a scale tag, the big-endian element count and the
//! big-endian `i64` values. Scaling and rounding are correctly rounded IEEE
//! operations, so the integers are the same on every target.

use super::error::{StatError, StatResult};

/// The fixed-point scale of a quantised value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QuantScale {
    /// `round(x * 1e12)`: twelve decimal places.
    Pico,
    /// `round(x * 2^32)`: Q32.32.
    Q32,
}

impl QuantScale {
    /// The multiplier applied before rounding.
    pub fn factor(self) -> f64 {
        match self {
            QuantScale::Pico => 1e12,
            QuantScale::Q32 => 4_294_967_296.0,
        }
    }

    /// The tag byte in the canonical encoding.
    pub fn tag(self) -> u8 {
        match self {
            QuantScale::Pico => 1,
            QuantScale::Q32 => 2,
        }
    }
}

/// `2^63` as an `f64`: the first magnitude that does not fit `i64`.
const I64_LIMIT: f64 = 9_223_372_036_854_775_808.0;

/// Quantise one finite value. `index` names it in a refusal.
pub fn quantise_at(value: f64, scale: QuantScale, index: usize) -> StatResult<i64> {
    if !value.is_finite() {
        return Err(StatError::NonFinite {
            what: "quantise input",
            index,
        });
    }
    let scaled = (value * scale.factor()).round_ties_even();
    if !(-I64_LIMIT..I64_LIMIT).contains(&scaled) {
        return Err(StatError::QuantiseOverflow { index });
    }
    Ok(scaled as i64)
}

/// Quantise one finite value.
pub fn quantise(value: f64, scale: QuantScale) -> StatResult<i64> {
    quantise_at(value, scale, 0)
}

/// The nearest `f64` to a quantised value.
pub fn dequantise(value: i64, scale: QuantScale) -> f64 {
    value as f64 / scale.factor()
}

/// A vector of quantised values with its scale.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuantisedVector {
    scale: QuantScale,
    values: Vec<i64>,
}

impl QuantisedVector {
    /// Quantise every value; the first refusal names its index.
    pub fn from_f64s(values: &[f64], scale: QuantScale) -> StatResult<Self> {
        let values = values
            .iter()
            .enumerate()
            .map(|(index, &value)| quantise_at(value, scale, index))
            .collect::<StatResult<Vec<i64>>>()?;
        Ok(Self { scale, values })
    }

    /// The scale.
    pub fn scale(&self) -> QuantScale {
        self.scale
    }

    /// The quantised integers.
    pub fn values(&self) -> &[i64] {
        &self.values
    }

    /// Canonical bytes: tag, big-endian `u64` length, big-endian values.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9 + 8 * self.values.len());
        bytes.push(self.scale.tag());
        bytes.extend_from_slice(&(self.values.len() as u64).to_be_bytes());
        for value in &self.values {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes
    }
}
