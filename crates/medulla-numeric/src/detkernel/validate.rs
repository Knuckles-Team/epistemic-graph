//! Shared input checks. Every public entry point of the statistics modules
//! validates through these helpers, so a refusal names the same thing the same
//! way everywhere.

use super::error::{StatError, StatResult};

/// Tolerance for "sums to one" on caller-supplied probability vectors.
pub const SIMPLEX_TOLERANCE: f64 = 1e-9;

/// Refuse an empty slice.
pub fn non_empty<T>(values: &[T], what: &'static str) -> StatResult<()> {
    if values.is_empty() {
        return Err(StatError::Empty { what });
    }
    Ok(())
}

/// Refuse a length other than `expected`.
pub fn same_len(expected: usize, actual: usize, what: &'static str) -> StatResult<()> {
    if expected != actual {
        return Err(StatError::LengthMismatch {
            what,
            expected,
            actual,
        });
    }
    Ok(())
}

/// Refuse any NaN or infinity.
pub fn all_finite(values: &[f64], what: &'static str) -> StatResult<()> {
    match values.iter().position(|v| !v.is_finite()) {
        Some(index) => Err(StatError::NonFinite { what, index }),
        None => Ok(()),
    }
}

/// Refuse anything outside `[0, 1]` (NaN included).
pub fn all_unit_interval(values: &[f64], what: &'static str) -> StatResult<()> {
    all_finite(values, what)?;
    match values.iter().position(|v| !(0.0..=1.0).contains(v)) {
        Some(index) => Err(StatError::OutOfDomain {
            what,
            index,
            domain: "[0, 1]",
        }),
        None => Ok(()),
    }
}

/// Refuse a vector that is not a probability distribution: entries in
/// `[0, 1]` summing to one within [`SIMPLEX_TOLERANCE`] (serial sum).
pub fn probability_vector(values: &[f64], what: &'static str) -> StatResult<()> {
    non_empty(values, what)?;
    all_unit_interval(values, what)?;
    let total = super::reduce::serial_sum(values);
    if (total - 1.0).abs() > SIMPLEX_TOLERANCE {
        return Err(StatError::OutOfDomain {
            what,
            index: values.len(),
            domain: "the probability simplex (sum 1)",
        });
    }
    Ok(())
}

/// Refuse a label that is not below `classes`.
pub fn labels_below(labels: &[usize], classes: usize) -> StatResult<()> {
    match labels.iter().position(|&label| label >= classes) {
        Some(index) => Err(StatError::ClassOutOfRange {
            index,
            label: labels[index],
            classes,
        }),
        None => Ok(()),
    }
}

/// Refuse a scalar parameter for which `ok` is false.
pub fn parameter(ok: bool, name: &'static str, requirement: &'static str) -> StatResult<()> {
    if ok {
        Ok(())
    } else {
        Err(StatError::InvalidParameter { name, requirement })
    }
}
