//! Conformal prediction: split and weighted split conformal, APS/RAPS and LAC
//! scores, acceptability-set scores, Mondrian (class-conditional) and binary
//! conformal, and adaptive conformal inference for drift.
//!
//! Every calibrated type carries its level `alpha` (an exact rational) and its
//! calibration-set size, so a record states exactly what it was built from.

pub mod adaptive;
pub mod mondrian;
pub mod quantile;
pub mod sets;

pub use adaptive::{AdaptiveConformal, AdaptiveReport};
pub use mondrian::{binary_conformal, mondrian_conformal, BinaryConformal, BinarySet, ClassThreshold, MondrianConformal};
pub use quantile::{realised_coverage_interval, split_conformal, weighted_split_conformal, ConformalQuantile, Threshold};
pub use sets::{acceptability_score, aps_label_scores, aps_scores, lac_scores, PredictionSet, RapsPenalty};
