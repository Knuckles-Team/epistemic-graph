//! Serial, fixed-order reductions and deterministic orderings.
//!
//! Floating-point addition is not associative, so a reduction's bits depend on
//! its order. Everything here folds left to right over a slice (or over a
//! `BTreeMap` in key order at the call site): no rayon, no SIMD re-association,
//! no hash-map iteration. Orderings use `f64::total_cmp` with the index as the
//! tie-break, so equal values always come out in index order.

use super::error::StatResult;
use super::validate;
use std::cmp::Ordering;

/// Left-to-right sum.
pub fn serial_sum(values: &[f64]) -> f64 {
    let mut total = 0.0;
    for value in values {
        total += value;
    }
    total
}

/// Left-to-right Neumaier compensated sum: the same order every time, with the
/// rounding error of each step carried forward.
pub fn compensated_sum(values: &[f64]) -> f64 {
    let mut total = 0.0;
    let mut compensation = 0.0;
    for &value in values {
        let next = total + value;
        compensation += if total.abs() >= value.abs() {
            (total - next) + value
        } else {
            (value - next) + total
        };
        total = next;
    }
    total + compensation
}

/// Left-to-right mean of a non-empty slice.
pub fn serial_mean(values: &[f64], what: &'static str) -> StatResult<f64> {
    validate::non_empty(values, what)?;
    Ok(serial_sum(values) / values.len() as f64)
}

fn ascending_then_index(values: &[f64], a: usize, b: usize) -> Ordering {
    values[a].total_cmp(&values[b]).then(a.cmp(&b))
}

/// Indices that sort `values` ascending; equal values keep index order.
pub fn order_ascending(values: &[f64]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&a, &b| ascending_then_index(values, a, b));
    order
}

/// Indices that sort `values` descending; equal values keep index order.
pub fn order_descending(values: &[f64]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&a, &b| values[b].total_cmp(&values[a]).then(a.cmp(&b)));
    order
}

/// A sorted copy of `values` (ascending, `total_cmp`).
pub fn sorted_ascending(values: &[f64]) -> Vec<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted
}

/// Index of the largest value; the lowest index wins a tie.
pub fn argmax_first(values: &[f64], what: &'static str) -> StatResult<usize> {
    validate::non_empty(values, what)?;
    Ok(order_descending(values)[0])
}
