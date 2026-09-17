//! Wire form of a 0-1 integer programme.
//!
//! These are plain data transfer types. They carry no invariants of their own:
//! a [`ModelSpec`] becomes a usable [`super::Model`] only through
//! `Model::try_from`, which validates every field and lowers every typed
//! constraint to one linear row.

use serde::{Deserialize, Serialize};

/// Index of a binary decision variable (its position in [`ModelSpec::variables`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VarId(pub u32);

impl VarId {
    /// The variable's position as a `usize` index.
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Index of a constraint (its position in [`ModelSpec::constraints`]). Every
/// constraint lowers to exactly one linear row, so this is also the row index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RowId(pub u32);

impl RowId {
    /// The row's position as a `usize` index.
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Relation of a linear row's left-hand side to its right-hand side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Relation {
    /// `Σ a·x ≤ rhs`.
    LessEqual,
    /// `Σ a·x ≥ rhs`.
    GreaterEqual,
    /// `Σ a·x = rhs`.
    Equal,
}

/// One `coefficient · variable` term of a linear row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Term {
    pub var: VarId,
    pub coefficient: i64,
}

/// The typed body of a constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ConstraintBody {
    /// A general linear row with integer coefficients.
    Linear {
        terms: Vec<Term>,
        relation: Relation,
        rhs: i64,
    },
    /// `antecedent ⇒ consequent` (`consequent − antecedent ≥ 0`).
    Implication {
        antecedent: VarId,
        consequent: VarId,
    },
    /// `antecedent ⇒ at least one consequent` (a conditional covering row).
    ImpliesAny {
        antecedent: VarId,
        consequents: Vec<VarId>,
    },
    /// Exactly one of `vars` is selected.
    ExactlyOne { vars: Vec<VarId> },
    /// At most `k` of `vars` are selected.
    AtMost { vars: Vec<VarId>, k: u32 },
    /// At least `k` of `vars` are selected (`k = 1` is a covering row).
    AtLeast { vars: Vec<VarId>, k: u32 },
    /// `var` is pinned to `value`.
    Fix { var: VarId, value: bool },
}

/// A labelled constraint. The label names the constraint in explanations
/// (for example an infeasibility core).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConstraintSpec {
    pub label: String,
    pub body: ConstraintBody,
}

/// An objective coefficient. `Unknown` is never read as zero: a level that
/// holds unknown coefficients ranks every selection of an unknown-cost
/// variable below every all-known selection (the "unknown tier").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Coefficient {
    Known(i64),
    Unknown,
}

/// One term of an objective level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveTerm {
    pub var: VarId,
    pub coefficient: Coefficient,
}

/// One level of the lexicographic objective (minimised).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveLevelSpec {
    pub label: String,
    pub terms: Vec<ObjectiveTerm>,
}

/// A complete 0-1 integer programme: binary variables, labelled constraints and
/// a lexicographic objective whose first level is the most significant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSpec {
    pub variables: Vec<String>,
    pub constraints: Vec<ConstraintSpec>,
    pub objective: Vec<ObjectiveLevelSpec>,
}
