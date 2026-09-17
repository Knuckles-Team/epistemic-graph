//! Multi-class scaling calibration: temperature, vector and matrix (Dirichlet
//! when fed log-probabilities) scaling, fitted by deterministic coordinate
//! descent on the mean negative log-likelihood.
//!
//! The calibrated logits are `W z + b`. Temperature scaling ties `W = beta I`
//! (temperature `1 / beta`) with `b = 0`; vector scaling fits a diagonal `W`
//! and `b`; matrix scaling fits all of `W` and `b` with an optional L2 penalty
//! on off-diagonal weights and biases (the ODIR regulariser). The NLL is convex
//! in `(W, b)`, so every coordinate is a convex 1-D problem, solved exactly by
//! [`crate::detkernel::optimise`]. The sweep count is fixed; coordinates are
//! visited in a fixed order, so the fit is bit-reproducible.

use super::scores::{LabelledScores, ScoreMatrix};
use crate::detkernel::kernels::{log_sum_exp_finite, softmax};
use crate::detkernel::optimise::{minimise_convex_bounded, minimise_convex_unbounded};
use crate::detkernel::{validate, StatResult};

/// Bounds on `beta = 1 / temperature` for temperature scaling.
pub const INVERSE_TEMPERATURE_BOUNDS: (f64, f64) = (1e-3, 1e3);

/// Which parameters are fitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalingFamily {
    /// One shared inverse temperature, no bias.
    Temperature,
    /// A per-class scale and a per-class bias.
    Vector,
    /// A full weight matrix and a bias vector.
    Matrix,
}

/// Fit budget and regularisation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScalingOptions {
    sweeps: u32,
    bisection_steps: u32,
    off_diagonal_l2: f64,
}

impl ScalingOptions {
    /// `sweeps` coordinate passes (1..=1000), `bisection_steps` per line search
    /// (1..=1100), and a non-negative finite L2 weight used by matrix scaling.
    pub fn new(sweeps: u32, bisection_steps: u32, off_diagonal_l2: f64) -> StatResult<Self> {
        validate::parameter((1..=1000).contains(&sweeps), "sweeps", "1..=1000")?;
        validate::parameter(
            (1..=1100).contains(&bisection_steps),
            "bisection_steps",
            "1..=1100",
        )?;
        validate::parameter(
            off_diagonal_l2.is_finite() && off_diagonal_l2 >= 0.0,
            "off_diagonal_l2",
            "finite and >= 0",
        )?;
        Ok(Self {
            sweeps,
            bisection_steps,
            off_diagonal_l2,
        })
    }
}

impl Default for ScalingOptions {
    fn default() -> Self {
        Self {
            sweeps: 25,
            bisection_steps: crate::detkernel::optimise::DEFAULT_BISECTION_STEPS,
            off_diagonal_l2: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Coordinate {
    Scale,
    Weight { row: usize, col: usize },
    Bias(usize),
}

/// A fitted affine scaling map.
#[derive(Debug, Clone, PartialEq)]
pub struct FittedScaling {
    family: ScalingFamily,
    classes: usize,
    weights: Vec<f64>,
    biases: Vec<f64>,
    n_calibration: u64,
    nll_before: f64,
    nll_after: f64,
}

impl FittedScaling {
    /// The family that was fitted.
    pub fn family(&self) -> ScalingFamily {
        self.family
    }

    /// `1 / beta` for temperature scaling; `None` for the other families.
    pub fn temperature(&self) -> Option<f64> {
        match self.family {
            ScalingFamily::Temperature => Some(1.0 / self.weights[0]),
            ScalingFamily::Vector | ScalingFamily::Matrix => None,
        }
    }

    /// Row-major `classes x classes` weights.
    pub fn weights(&self) -> &[f64] {
        &self.weights
    }

    /// Per-class biases.
    pub fn biases(&self) -> &[f64] {
        &self.biases
    }

    /// Calibration-set size.
    pub fn n_calibration(&self) -> u64 {
        self.n_calibration
    }

    /// Mean NLL of the identity map on the calibration set.
    pub fn nll_before(&self) -> f64 {
        self.nll_before
    }

    /// Mean NLL of the fitted map on the calibration set.
    pub fn nll_after(&self) -> f64 {
        self.nll_after
    }

    /// Calibrated probabilities for one row of raw scores.
    pub fn probabilities(&self, row: &[f64]) -> StatResult<Vec<f64>> {
        validate::same_len(self.classes, row.len(), "score row")?;
        validate::all_finite(row, "score row")?;
        softmax(&affine(&self.weights, &self.biases, row))
    }
}

fn affine(weights: &[f64], biases: &[f64], row: &[f64]) -> Vec<f64> {
    let classes = row.len();
    (0..classes)
        .map(|j| {
            let mut total = biases[j];
            for (k, z) in row.iter().enumerate() {
                total += weights[j * classes + k] * z;
            }
            total
        })
        .collect()
}

fn coordinates(family: ScalingFamily, classes: usize) -> Vec<Coordinate> {
    match family {
        ScalingFamily::Temperature => vec![Coordinate::Scale],
        ScalingFamily::Vector => (0..classes)
            .map(|k| Coordinate::Weight { row: k, col: k })
            .chain((0..classes).map(Coordinate::Bias))
            .collect(),
        ScalingFamily::Matrix => (0..classes)
            .flat_map(|row| (0..classes).map(move |col| Coordinate::Weight { row, col }))
            .chain((0..classes).map(Coordinate::Bias))
            .collect(),
    }
}

/// Direction of the calibrated logits when `coordinate` grows by one.
fn direction(coordinate: Coordinate, row: &[f64]) -> Vec<f64> {
    match coordinate {
        Coordinate::Scale => row.to_vec(),
        Coordinate::Weight { row: j, col } => unit(row.len(), j, row[col]),
        Coordinate::Bias(j) => unit(row.len(), j, 1.0),
    }
}

fn unit(len: usize, index: usize, value: f64) -> Vec<f64> {
    let mut out = vec![0.0; len];
    out[index] = value;
    out
}

fn value_of(coordinate: Coordinate, weights: &[f64], biases: &[f64], classes: usize) -> f64 {
    match coordinate {
        Coordinate::Scale => weights[0],
        Coordinate::Weight { row, col } => weights[row * classes + col],
        Coordinate::Bias(j) => biases[j],
    }
}

fn shift(coordinate: Coordinate, weights: &mut [f64], biases: &mut [f64], step: f64) {
    let classes = biases.len();
    match coordinate {
        Coordinate::Scale => (0..classes).for_each(|k| weights[k * classes + k] += step),
        Coordinate::Weight { row, col } => weights[row * classes + col] += step,
        Coordinate::Bias(j) => biases[j] += step,
    }
}

fn penalised(coordinate: Coordinate, family: ScalingFamily) -> bool {
    let off_diagonal_or_bias = match coordinate {
        Coordinate::Scale => false,
        Coordinate::Weight { row, col } => row != col,
        Coordinate::Bias(_) => true,
    };
    match family {
        ScalingFamily::Matrix => off_diagonal_or_bias,
        ScalingFamily::Temperature | ScalingFamily::Vector => false,
    }
}

struct LineProblem<'a> {
    labels: &'a [usize],
    classes: usize,
    base: Vec<f64>,
    directions: Vec<f64>,
    penalty: f64,
    start: f64,
}

impl LineProblem<'_> {
    fn derivative(&self, step: f64) -> f64 {
        let mut total = 0.0;
        let rows = self.base.chunks_exact(self.classes);
        let dirs = self.directions.chunks_exact(self.classes);
        for ((base, dir), &label) in rows.zip(dirs).zip(self.labels) {
            total += row_gradient(base, dir, label, step);
        }
        total / self.labels.len() as f64 + 2.0 * self.penalty * (self.start + step)
    }
}

fn row_gradient(base: &[f64], dir: &[f64], label: usize, step: f64) -> f64 {
    let logits: Vec<f64> = base.iter().zip(dir).map(|(b, d)| b + step * d).collect();
    let lse = log_sum_exp_finite(&logits);
    let mut total = 0.0;
    for (k, (logit, d)) in logits.iter().zip(dir).enumerate() {
        let p = crate::detkernel::math::exp(logit - lse);
        let y = if k == label { 1.0 } else { 0.0 };
        total += (p - y) * d;
    }
    total
}

struct FitState {
    family: ScalingFamily,
    classes: usize,
    weights: Vec<f64>,
    biases: Vec<f64>,
}

impl FitState {
    fn identity(family: ScalingFamily, classes: usize) -> Self {
        let mut weights = vec![0.0; classes * classes];
        (0..classes).for_each(|k| weights[k * classes + k] = 1.0);
        Self {
            family,
            classes,
            weights,
            biases: vec![0.0; classes],
        }
    }

    fn line_problem<'a>(
        &self,
        data: &'a LabelledScores,
        coordinate: Coordinate,
        l2: f64,
    ) -> LineProblem<'a> {
        let scores = data.scores();
        let mut base = Vec::with_capacity(scores.row_count() * self.classes);
        let mut directions = Vec::with_capacity(base.capacity());
        for row in scores.rows() {
            base.extend(affine(&self.weights, &self.biases, row));
            directions.extend(direction(coordinate, row));
        }
        let penalty = if penalised(coordinate, self.family) {
            l2
        } else {
            0.0
        };
        LineProblem {
            labels: data.labels(),
            classes: self.classes,
            base,
            directions,
            penalty,
            start: value_of(coordinate, &self.weights, &self.biases, self.classes),
        }
    }

    fn update(
        &mut self,
        data: &LabelledScores,
        coordinate: Coordinate,
        options: &ScalingOptions,
    ) -> StatResult<()> {
        let problem = self.line_problem(data, coordinate, options.off_diagonal_l2);
        let steps = options.bisection_steps;
        let step = match coordinate {
            Coordinate::Scale => {
                let (lower, upper) = INVERSE_TEMPERATURE_BOUNDS;
                let start = problem.start;
                minimise_convex_bounded(
                    |t| problem.derivative(t),
                    lower - start,
                    upper - start,
                    steps,
                )?
            }
            Coordinate::Weight { .. } | Coordinate::Bias(_) => {
                minimise_convex_unbounded(|t| problem.derivative(t), 0.0, 1.0, steps)?
            }
        };
        shift(coordinate, &mut self.weights, &mut self.biases, step);
        Ok(())
    }
}

/// Mean negative log-likelihood of the map `(weights, biases)` on `data`.
fn mean_nll(weights: &[f64], biases: &[f64], data: &LabelledScores) -> f64 {
    let mut total = 0.0;
    for (row, &label) in data.scores().rows().zip(data.labels()) {
        let logits = affine(weights, biases, row);
        total += log_sum_exp_finite(&logits) - logits[label];
    }
    total / data.labels().len() as f64
}

/// Mean negative log-likelihood of the raw scores (softmax of each row).
pub fn negative_log_likelihood(data: &LabelledScores) -> f64 {
    let identity = FitState::identity(ScalingFamily::Vector, data.scores().classes());
    mean_nll(&identity.weights, &identity.biases, data)
}

/// Fit a scaling map on a calibration set.
pub fn fit_scaling(
    data: &LabelledScores,
    family: ScalingFamily,
    options: ScalingOptions,
) -> StatResult<FittedScaling> {
    let classes = data.scores().classes();
    let mut state = FitState::identity(family, classes);
    let nll_before = mean_nll(&state.weights, &state.biases, data);
    let order = coordinates(family, classes);
    // One coordinate has nothing to alternate with: a single exact line search
    // is the optimum.
    let sweeps = match family {
        ScalingFamily::Temperature => 1,
        ScalingFamily::Vector | ScalingFamily::Matrix => options.sweeps,
    };
    for _ in 0..sweeps {
        for &coordinate in &order {
            state.update(data, coordinate, &options)?;
        }
    }
    Ok(FittedScaling {
        family,
        classes,
        nll_after: mean_nll(&state.weights, &state.biases, data),
        weights: state.weights,
        biases: state.biases,
        n_calibration: data.labels().len() as u64,
        nll_before,
    })
}

/// Apply a fitted map to every row of a matrix.
pub fn calibrate_rows(fitted: &FittedScaling, scores: &ScoreMatrix) -> StatResult<Vec<Vec<f64>>> {
    scores.rows().map(|row| fitted.probabilities(row)).collect()
}
