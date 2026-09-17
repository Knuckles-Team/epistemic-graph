//! Probability calibration: multi-class scaling (temperature, vector, matrix),
//! binary isotonic regression, and calibration metrics.
//!
//! Every fit records its calibration-set size. Multi-class heads use
//! [`scaling`]; isotonic regression is for binary scores only (per-class
//! isotonic followed by renormalisation is deliberately not offered).

pub mod isotonic;
pub mod metrics;
pub mod scaling;
pub mod scores;

pub use isotonic::{fit_isotonic, fit_isotonic_weighted, IsotonicFit};
pub use metrics::{BinCount, CalibrationReport, ProbabilityFloor, ReliabilityBin};
pub use scaling::{fit_scaling, FittedScaling, ScalingFamily, ScalingOptions};
pub use scores::{LabelledScores, ProbabilityMatrix, ScoreMatrix};
