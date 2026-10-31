//! Deterministic numeric kernels for replayable statistical decisions.
//!
//! Everything a decision record's outcome bits depend on is computed here or on
//! top of here: pinned software transcendentals ([`math`]), stable probability
//! kernels ([`kernels`]), serial fixed-order reductions and orderings
//! ([`reduce`]), exact rational levels and propensities ([`rational`]), a
//! deterministic convex line minimiser ([`optimise`]) and fixed-point
//! quantisation for digests ([`quantise`]).
//!
//! Rules this module and its users follow:
//! * no std float transcendentals (enforced by the crate `clippy.toml`);
//! * no parallel or hash-ordered reduction on a committed path;
//! * ties break by index (or by key order in a `BTreeMap`);
//! * digests are taken over [`quantise::QuantisedVector`] bytes, never raw
//!   `f64` bytes.

pub mod error;
pub mod kernels;
pub mod math;
pub mod optimise;
pub mod quantise;
pub mod rational;
pub mod reduce;
pub mod validate;

pub use error::{StatError, StatResult};
pub use rational::{Level, Propensity, UnitRational};
