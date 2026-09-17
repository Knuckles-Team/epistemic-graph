//! Risk control: exact binomial tails and Clopper–Pearson intervals over a
//! deterministic incomplete beta, selective risk control (Learn-then-Test with
//! fixed-sequence testing), minimum-sample gates and beta–binomial pooling.

pub mod beta;
pub mod binomial;
mod counts;
pub mod pooling;
pub mod sample_gate;
pub mod selective;

pub use beta::{beta_quantile, regularized_incomplete_beta};
pub use binomial::{binomial_cdf, clopper_pearson, BinomialCounts, BinomialInterval, IntervalSide};
pub use pooling::{pool_hierarchy, BetaDistribution, ConcentrationBounds, GroupCounts, PoolTree, PooledNode};
pub use sample_gate::{SampleAssessment, SampleGate};
pub use selective::{calibrate_selective_risk, RiskTarget, SelectiveRiskCertificate, TestOutcome};
