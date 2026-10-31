//! Entry point for the statistics-kernel integration tests: calibration,
//! conformal prediction, risk control, off-policy evaluation and the shared
//! deterministic kernels, plus the crate's clippy policy self-check. Split by
//! topic with fixtures shared through [`common`] so scenarios are not
//! duplicated per file (jscpd/dupehound would flag copies).

#[path = "stat_kernels/calibration.rs"]
mod calibration;
#[path = "stat_kernels/clippy_policy.rs"]
mod clippy_policy;
#[path = "stat_kernels/common.rs"]
mod common;
#[path = "stat_kernels/conformal.rs"]
mod conformal;
#[path = "stat_kernels/kernels.rs"]
mod kernels;
#[path = "stat_kernels/ope.rs"]
mod ope;
#[path = "stat_kernels/risk.rs"]
mod risk;
