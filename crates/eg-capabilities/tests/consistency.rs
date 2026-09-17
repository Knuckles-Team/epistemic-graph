//! Consistency checks are split into snapshots, documented divergence, and executable checks.

#[path = "consistency/checks.rs"]
mod checks;
#[path = "consistency/divergence.rs"]
mod divergence;
#[path = "consistency/row_count.rs"]
mod row_count;
#[path = "consistency/snapshots.rs"]
mod snapshots;
