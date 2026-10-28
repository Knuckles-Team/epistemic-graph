//! Deterministic candidate selection kernels for recursive-run policies.

pub mod front;

pub use front::{FrontError, ParetoFront, PolicyCandidate};
