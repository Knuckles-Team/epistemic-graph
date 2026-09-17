//! Off-policy evaluation of decision policies from logged bandit feedback:
//! exact rational logging propensities, support checks, IPS, clipped IPS,
//! SNIPS, SWITCH and doubly robust estimators, and effective-sample-size gates.
//!
//! Propensities are the executed policy's probabilities, never a model's score
//! mass; a deterministic logger records 1 for the chosen action and 0 for the
//! rest, and such records cannot evaluate any other policy (they are refused as
//! outside support).

pub mod estimators;
pub mod logged;

pub use estimators::{clipped_ips, doubly_robust, effective_sample_size, ips, snips, switch, EssGate, Estimator, OpeEstimate};
pub use logged::{require_support, support_report, LoggedDecision, SupportReport};
