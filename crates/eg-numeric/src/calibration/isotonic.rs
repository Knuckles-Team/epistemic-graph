//! Binary isotonic calibration by pool-adjacent-violators (PAVA).
//!
//! Scores are ordered ascending (index tie-break), equal scores are pooled
//! first so they always map to one value, and adjacent blocks are merged left
//! to right while their weighted means decrease. The fitted map is a
//! non-decreasing step function: a score takes the value of the last block that
//! starts at or below it (the first block below the smallest score).

use crate::detkernel::reduce::order_ascending;
use crate::detkernel::{validate, StatError, StatResult};

#[derive(Debug, Clone, Copy)]
struct Block {
    lower: f64,
    upper: f64,
    weighted_sum: f64,
    weight: f64,
}

impl Block {
    fn mean(&self) -> f64 {
        self.weighted_sum / self.weight
    }

    fn absorb(&mut self, other: Block) {
        self.upper = other.upper;
        self.weighted_sum += other.weighted_sum;
        self.weight += other.weight;
    }
}

/// A fitted non-decreasing calibration map.
#[derive(Debug, Clone, PartialEq)]
pub struct IsotonicFit {
    lowers: Vec<f64>,
    uppers: Vec<f64>,
    values: Vec<f64>,
    n_calibration: u64,
}

impl IsotonicFit {
    /// Calibrated probability for a finite score.
    pub fn predict(&self, score: f64) -> StatResult<f64> {
        if !score.is_finite() {
            return Err(StatError::NonFinite {
                what: "isotonic score",
                index: 0,
            });
        }
        let starts_at_or_below = self.lowers.partition_point(|&lower| lower <= score);
        Ok(self.values[starts_at_or_below.saturating_sub(1)])
    }

    /// Block `(lowest score, highest score, value)` triples in score order.
    pub fn blocks(&self) -> Vec<(f64, f64, f64)> {
        (0..self.values.len())
            .map(|i| (self.lowers[i], self.uppers[i], self.values[i]))
            .collect()
    }

    /// Calibration-set size.
    pub fn n_calibration(&self) -> u64 {
        self.n_calibration
    }
}

/// Fit on scores and binary outcomes with unit weights.
pub fn fit_isotonic(scores: &[f64], outcomes: &[bool]) -> StatResult<IsotonicFit> {
    let targets: Vec<f64> = outcomes.iter().map(|&o| if o { 1.0 } else { 0.0 }).collect();
    let weights = vec![1.0; outcomes.len()];
    fit_isotonic_weighted(scores, &targets, &weights)
}

/// Fit on scores, targets in `[0, 1]` and positive finite weights.
pub fn fit_isotonic_weighted(
    scores: &[f64],
    targets: &[f64],
    weights: &[f64],
) -> StatResult<IsotonicFit> {
    validate::non_empty(scores, "isotonic scores")?;
    validate::same_len(scores.len(), targets.len(), "isotonic targets")?;
    validate::same_len(scores.len(), weights.len(), "isotonic weights")?;
    validate::all_finite(scores, "isotonic scores")?;
    validate::all_unit_interval(targets, "isotonic targets")?;
    validate::all_finite(weights, "isotonic weights")?;
    if let Some(index) = weights.iter().position(|&w| w <= 0.0) {
        return Err(StatError::OutOfDomain {
            what: "isotonic weights",
            index,
            domain: "(0, inf)",
        });
    }
    let blocks = pool_violators(tie_blocks(scores, targets, weights));
    Ok(IsotonicFit {
        lowers: blocks.iter().map(|b| b.lower).collect(),
        uppers: blocks.iter().map(|b| b.upper).collect(),
        values: blocks.iter().map(Block::mean).collect(),
        n_calibration: scores.len() as u64,
    })
}

fn tie_blocks(scores: &[f64], targets: &[f64], weights: &[f64]) -> Vec<Block> {
    let mut blocks: Vec<Block> = Vec::new();
    for index in order_ascending(scores) {
        let point = Block {
            lower: scores[index],
            upper: scores[index],
            weighted_sum: weights[index] * targets[index],
            weight: weights[index],
        };
        match blocks.last_mut() {
            Some(last) if last.upper == point.lower => last.absorb(point),
            Some(_) | None => blocks.push(point),
        }
    }
    blocks
}

fn pool_violators(blocks: Vec<Block>) -> Vec<Block> {
    let mut stack: Vec<Block> = Vec::with_capacity(blocks.len());
    for block in blocks {
        stack.push(block);
        while stack.len() >= 2 && stack[stack.len() - 2].mean() > stack[stack.len() - 1].mean() {
            let top = stack.pop().expect("stack holds at least two blocks");
            let below = stack.len() - 1;
            stack[below].absorb(top);
        }
    }
    stack
}
