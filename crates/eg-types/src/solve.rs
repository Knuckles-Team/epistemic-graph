//! Wire contract of the general bounded 0-1 integer programme (`Method::Solve`).
//!
//! These are data transfer types only. The search, the exact objective
//! scalarisation and the certificate verifier are algorithms and live in
//! `eg_compute::solve`, which re-exports this module so there is exactly one
//! definition of every value that crosses the wire.
//!
//! Everything here is integer arithmetic: no clock, float or hash order
//! influences a value, so equal inputs give byte-identical certificates and
//! digests on every target.

pub mod certificate;
pub mod objective;
pub mod request;
pub mod scalar;
pub mod spec;

pub use certificate::{
    Algorithm, BoundProof, Certificate, ConfigError, DualEntry, Incumbent, LagrangeDual, LeafProof,
    ProofNode, SolveStatus, SolverConfig, SolverConfigSpec, DEFAULT_CERTIFICATE_LEAVES,
    DEFAULT_NODE_BUDGET, MAX_BOUND_DENOMINATOR, MAX_CERTIFICATE_LEAVES, MAX_MULTIPLIER,
    MAX_NODE_BUDGET,
};
pub use objective::{LevelValue, ObjectiveValue};
pub use request::{SolveErrorCode, SolveRequest, SolveResult, SOLVE_RESULT_SCHEMA_VERSION};
pub use scalar::{Scalar, ScalarParseError, Sha256Digest};
pub use spec::{
    Coefficient, ConstraintBody, ConstraintSpec, ModelSpec, ObjectiveLevelSpec, ObjectiveTerm,
    Relation, RowId, Term, VarId,
};
