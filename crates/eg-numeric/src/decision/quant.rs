//! Bridges between the exact wire numbers and the kernel's `f64` working values.
//!
//! A value crosses into `f64` only to be computed on, and crosses back only
//! through [`q32`] or [`unit_wire`], which quantise with ties-to-even. Every
//! record digest is therefore over integers.

use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::{QuantScaleTag, QuantisedValue, UnitRationalWire};

use super::refusal::{Refusal, RefusalResult};
use crate::detkernel::quantise::{dequantise, quantise, QuantScale};
use crate::detkernel::{Level, StatError, UnitRational};

/// The denominator probabilities are rounded onto when recorded.
pub const PROBABILITY_DENOMINATOR: u64 = 1_000_000_000_000;

fn scale_of(tag: QuantScaleTag) -> QuantScale {
    match tag {
        QuantScaleTag::Pico => QuantScale::Pico,
        QuantScaleTag::Q32 => QuantScale::Q32,
    }
}

/// The working value of a fixed-point number.
pub fn value_of(value: QuantisedValue) -> f64 {
    dequantise(value.value, scale_of(value.scale))
}

/// The working value of an integer read on `tag`.
pub fn raw_value(raw: i64, tag: QuantScaleTag) -> f64 {
    dequantise(raw, scale_of(tag))
}

/// Quantise onto `Q32`, the scale every decision matrix and head uses.
pub fn q32(value: f64) -> RefusalResult<QuantisedValue> {
    Ok(QuantisedValue {
        scale: QuantScaleTag::Q32,
        value: quantise(value, QuantScale::Q32)?,
    })
}

/// Quantise an integer count onto `Q32` exactly.
pub fn q32_integer(value: i64) -> RefusalResult<i64> {
    value
        .checked_mul(1_i64 << 32)
        .ok_or_else(|| Refusal::from(StatError::QuantiseOverflow { index: 0 }))
}

/// A wire rational as the kernel's exact rational.
pub fn rational(value: UnitRationalWire) -> RefusalResult<UnitRational> {
    Ok(UnitRational::new(value.numerator(), value.denominator())?)
}

/// A wire rational as a strict level in `(0, 1)`.
pub fn level(value: UnitRationalWire) -> RefusalResult<Level> {
    Ok(Level::new(value.numerator(), value.denominator())?)
}

/// The working value of a wire rational.
pub fn rational_value(value: UnitRationalWire) -> f64 {
    value.numerator() as f64 / value.denominator() as f64
}

/// An exact rational as its wire form.
pub fn exact_wire(numerator: u64, denominator: u64) -> RefusalResult<UnitRationalWire> {
    let reduced = UnitRational::new(numerator, denominator)?;
    UnitRationalWire::new(reduced.numerator(), reduced.denominator())
        .map_err(|detail| Refusal::new(StatisticalErrorCode::NumericRefused, detail))
}

/// A probability in `[0, 1]` recorded on the fixed denominator.
pub fn unit_wire(value: f64) -> RefusalResult<UnitRationalWire> {
    let clamped = value.clamp(0.0, 1.0);
    let scaled = quantise(clamped, QuantScale::Pico)?;
    exact_wire(scaled.unsigned_abs(), PROBABILITY_DENOMINATOR)
}
