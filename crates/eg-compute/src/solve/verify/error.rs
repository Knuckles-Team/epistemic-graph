//! Verdicts and typed rejections of the certificate verifier.

use std::fmt;

use crate::solve::model::{RowId, VarId};
use crate::solve::scalar::Scalar;

/// What a certificate was verified to establish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The incumbent is feasible and no feasible assignment is better.
    ProvenOptimal { objective: Scalar },
    /// No feasible assignment exists; the listed rows alone are infeasible.
    ProvenInfeasible { core: Vec<RowId> },
    /// The incumbent is feasible and within `incumbent − lower_bound` of optimal.
    ProvenGap {
        incumbent: Scalar,
        lower_bound: Scalar,
    },
    /// The incumbent and the lower bound are verified; optimality rests on
    /// replaying the deterministic search.
    OptimalityRequiresReplay {
        objective: Scalar,
        lower_bound: Scalar,
    },
    /// Nothing contradicts infeasibility; it rests on replaying the search.
    InfeasibilityRequiresReplay,
    /// The budget ran out; whatever incumbent and bound exist are verified.
    Unresolved {
        incumbent: Option<Scalar>,
        lower_bound: Option<Scalar>,
    },
}

/// Why a claimed status does not follow from the verified evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusDefect {
    MissingIncumbent,
    UnexpectedIncumbent,
    WrongProofKind,
    NoBoundLeaf,
    BoundBelowIncumbent,
    LowerBoundMismatch,
    LowerBoundAboveProof,
    GapMismatch,
    GapNotAccepted,
    GapAccepted,
    CoreMismatch,
    NodeCountMismatch,
}

/// Why a certificate was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyError {
    ModelDigestMismatch,
    AssignmentLength { expected: usize, found: usize },
    RowViolated { row: RowId },
    ObjectiveMismatch,
    VariableOutOfRange { node: usize, var: VarId },
    VariableAlreadyFixed { node: usize, var: VarId },
    RowOutOfRange { node: usize, row: RowId },
    ForcedUnjustified { node: usize },
    DualMalformed { node: usize },
    ArithmeticOverflow { node: usize },
    LeafNotInfeasible { node: usize },
    TreeIncomplete,
    TrailingNodes { node: usize },
    Status(StatusDefect),
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "certificate rejected: {self:?}")
    }
}

impl std::error::Error for VerifyError {}
