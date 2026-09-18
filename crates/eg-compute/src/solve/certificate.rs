//! Re-export of the certificate wire types this module's search and verifier
//! use. The definitions (and their config/certificate methods) live in
//! `eg_types::solve::certificate` -- see that module's own doc for why there
//! is exactly one copy.

pub use eg_types::solve::certificate::{
    Algorithm, BoundProof, Certificate, ConfigError, DualEntry, Incumbent, LagrangeDual, LeafProof,
    ProofNode, SolveStatus, SolverConfig, SolverConfigSpec, DEFAULT_CERTIFICATE_LEAVES,
    DEFAULT_NODE_BUDGET, MAX_BOUND_DENOMINATOR, MAX_CERTIFICATE_LEAVES, MAX_MULTIPLIER,
    MAX_NODE_BUDGET,
};
