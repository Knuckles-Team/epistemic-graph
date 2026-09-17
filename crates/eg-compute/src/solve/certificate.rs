//! The solver's output: status, incumbent and a checkable bound proof.
//!
//! # Lagrangian bound (the only proof primitive)
//!
//! For row multipliers `λ_r` whose signs match the rows (`≥`: `λ > 0`,
//! `≤`: `λ < 0`, `=`: any non-zero), every feasible `x` satisfies
//! `λ_r·(a_r·x − b_r) ≥ 0`, so for every feasible `x` consistent with a partial
//! assignment `P`:
//!
//! ```text
//! w·x ≥ L(λ, P) = Σ_r λ_r·b_r + Σ_{j fixed by P} ρ_j·P_j + Σ_{j free} min(0, ρ_j),
//!       ρ_j = w_j − Σ_r λ_r·a_rj
//! ```
//!
//! Multipliers are rational: integers `μ` over one positive `denominator` `D`
//! (`λ = μ / D`). Objective values are integers, so the bound is
//! `⌈D·L / D⌉`. With the objective replaced by zero, `D·L > 0` proves that no
//! feasible `x` is consistent with `P` at all. A root dual whose bound reaches
//! the incumbent value proves optimality; a covering dual (`y ≥ 0` on covering
//! rows with `Σ y ≤ cost` per column) is the special case with no negative
//! reduced cost.
//!
//! # Search tree
//!
//! A [`BoundProof::Tree`] lists proof nodes in pre-order. `Branch` fixes `var`
//! to `first` for its first subtree and to `!first` for its second; `Forced`
//! fixes `var` to `value` because `row` has no solution with `!value` under the
//! current path; `Leaf` closes the current path with a bound or an
//! infeasibility dual. Well-formed trees partition `{0,1}^n`, so the minimum
//! leaf bound is a global lower bound.

use serde::{Deserialize, Serialize};

use super::model::{ObjectiveValue, RowId, VarId};
use super::scalar::{Scalar, Sha256Digest};

/// Default deterministic node-expansion budget.
pub const DEFAULT_NODE_BUDGET: u64 = 100_000;
/// Largest configurable node budget.
pub const MAX_NODE_BUDGET: u64 = 1_000_000_000;
/// Default number of leaves a certificate tree may carry.
pub const DEFAULT_CERTIFICATE_LEAVES: u32 = 64;
/// Largest configurable certificate leaf count.
pub const MAX_CERTIFICATE_LEAVES: u32 = 1 << 20;
/// Largest configurable bound denominator.
pub const MAX_BOUND_DENOMINATOR: u64 = 1 << 16;
/// Largest absolute multiplier numerator a certificate may carry.
///
/// With the model limits (≤ 2^16 rows, ≤ 2^12 variables, |a| ≤ 2^40,
/// |b| ≤ 2^52, Σ|w| ≤ 2^100) and a denominator ≤ 2^16, every intermediate of
/// `L(λ, P)` stays below 2^126 in any evaluation order: Σ|μ·b| ≤ 2^124,
/// Σ|μ·a| ≤ 2^124 and Σ|D·w| ≤ 2^116.
pub const MAX_MULTIPLIER: i128 = 1 << 56;

/// The solver algorithm that produced a certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    /// Propagation, greedy incumbent, depth-first branch-and-bound with a
    /// dual-ascent Lagrangian bound, pre-order leaf-bound certificate.
    DepthFirstDualAscent,
}

/// Why a [`SolverConfig`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    NodeBudgetOutOfRange { value: u64 },
    CertificateLeavesOutOfRange { value: u32 },
    DenominatorOutOfRange { value: u64 },
    NegativeAcceptedGap,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid solver config: {self:?}")
    }
}

impl std::error::Error for ConfigError {}

/// Wire form of a [`SolverConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SolverConfigSpec {
    pub node_budget: u64,
    pub max_certificate_leaves: u32,
    pub bound_denominator: u64,
    pub accepted_gap: Scalar,
}

/// Deterministic work limits and gap acceptance. No wall clock is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SolverConfigSpec", into = "SolverConfigSpec")]
pub struct SolverConfig {
    spec: SolverConfigSpec,
}

impl SolverConfig {
    pub fn node_budget(&self) -> u64 {
        self.spec.node_budget
    }

    pub fn max_certificate_leaves(&self) -> u32 {
        self.spec.max_certificate_leaves
    }

    pub fn bound_denominator(&self) -> u64 {
        self.spec.bound_denominator
    }

    /// Largest scalar gap reported as `FeasibleWithGap` instead of `BudgetExhausted`.
    pub fn accepted_gap(&self) -> i128 {
        self.spec.accepted_gap.get()
    }
}

impl Default for SolverConfig {
    fn default() -> Self {
        Self {
            spec: SolverConfigSpec {
                node_budget: DEFAULT_NODE_BUDGET,
                max_certificate_leaves: DEFAULT_CERTIFICATE_LEAVES,
                bound_denominator: 1,
                accepted_gap: Scalar::new(0),
            },
        }
    }
}

impl TryFrom<SolverConfigSpec> for SolverConfig {
    type Error = ConfigError;

    fn try_from(spec: SolverConfigSpec) -> Result<Self, Self::Error> {
        if !(1..=MAX_NODE_BUDGET).contains(&spec.node_budget) {
            return Err(ConfigError::NodeBudgetOutOfRange {
                value: spec.node_budget,
            });
        }
        if !(1..=MAX_CERTIFICATE_LEAVES).contains(&spec.max_certificate_leaves) {
            let value = spec.max_certificate_leaves;
            return Err(ConfigError::CertificateLeavesOutOfRange { value });
        }
        if !(1..=MAX_BOUND_DENOMINATOR).contains(&spec.bound_denominator) {
            return Err(ConfigError::DenominatorOutOfRange {
                value: spec.bound_denominator,
            });
        }
        if spec.accepted_gap.get() < 0 {
            return Err(ConfigError::NegativeAcceptedGap);
        }
        Ok(Self { spec })
    }
}

impl From<SolverConfig> for SolverConfigSpec {
    fn from(config: SolverConfig) -> Self {
        config.spec
    }
}

/// One non-zero rational multiplier numerator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DualEntry {
    pub row: RowId,
    pub numerator: Scalar,
}

/// Row multipliers `numerator / denominator`, entries strictly ordered by row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LagrangeDual {
    pub denominator: u64,
    pub entries: Vec<DualEntry>,
}

/// How a leaf of the search tree is closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LeafProof {
    /// `L(dual, path)` is a lower bound on every feasible completion.
    Bound { dual: LagrangeDual },
    /// `L(dual, path)` with a zero objective is positive: no feasible completion.
    Infeasible { dual: LagrangeDual },
}

/// One pre-order node of the search-tree proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProofNode {
    Branch { var: VarId, first: bool },
    Forced { var: VarId, value: bool, row: RowId },
    Leaf { proof: LeafProof },
}

/// The lower-bound evidence a certificate carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum BoundProof {
    /// A complete pre-order search tree.
    Tree { nodes: Vec<ProofNode> },
    /// Only the root dual (the tree exceeded the certificate leaf limit).
    Root { dual: LagrangeDual },
}

/// The outcome class of a solve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SolveStatus {
    /// The incumbent is optimal and the tree or root proof certifies it.
    Optimal,
    /// The search completed; its tree exceeded the certificate limit, so the
    /// proof is a reproducible deterministic derivation, not a certificate.
    OptimalByDeterministicSearch { nodes_expanded: u64 },
    /// The budget ran out with a certified gap no larger than the accepted gap.
    FeasibleWithGap { gap: Scalar },
    /// No feasible assignment exists; `core` lists the rows the proof uses.
    Infeasible { core: Vec<RowId> },
    /// The search completed without a feasible assignment and without a
    /// certificate-sized tree.
    InfeasibleByDeterministicSearch { nodes_expanded: u64 },
    /// The budget ran out with no incumbent or a gap above the accepted gap.
    BudgetExhausted,
}

/// A feasible assignment and its objective value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Incumbent {
    pub selected: Vec<bool>,
    pub objective: ObjectiveValue,
}

/// Everything a verifier needs, and nothing it must trust.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Certificate {
    pub model_digest: Sha256Digest,
    pub algorithm: Algorithm,
    pub config: SolverConfig,
    pub status: SolveStatus,
    pub incumbent: Option<Incumbent>,
    /// Certified scalar lower bound, when any bound leaf exists.
    pub lower_bound: Option<Scalar>,
    pub proof: BoundProof,
    pub nodes_expanded: u64,
}

/// Domain tag of the certificate digest.
const CERTIFICATE_DIGEST_DOMAIN: &str = "eg-solve/certificate/v1";

impl Certificate {
    /// Digest of the whole certificate; equal inputs give equal digests on
    /// every target.
    pub fn digest(&self) -> Sha256Digest {
        Sha256Digest::of_json(CERTIFICATE_DIGEST_DOMAIN, self)
    }
}
