//! Consistency checks are split into snapshots, documented divergence, and executable checks.

#[path = "consistency/checks.rs"]
mod checks;
#[path = "consistency/divergence.rs"]
mod divergence;
#[path = "consistency/snapshots.rs"]
mod snapshots;
