//! Typed refusals raised while validating a [`super::ModelSpec`].

use std::fmt;

use super::spec::VarId;

/// Where in a model spec a defect was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Location {
    /// The variable at this position.
    Variable(usize),
    /// The constraint at this position.
    Constraint(usize),
    /// The objective level at this position.
    ObjectiveLevel(usize),
}

/// Why a model spec was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelError {
    NoVariables,
    TooManyVariables {
        count: usize,
        max: usize,
    },
    TooManyConstraints {
        count: usize,
        max: usize,
    },
    TooManyObjectiveLevels {
        count: usize,
        max: usize,
    },
    EmptyName {
        var: VarId,
    },
    DuplicateName {
        var: VarId,
    },
    LabelTooLong {
        location: Location,
        bytes: usize,
        max: usize,
    },
    UnknownVariable {
        location: Location,
        var: VarId,
    },
    DuplicateVariable {
        location: Location,
        var: VarId,
    },
    ZeroCoefficient {
        location: Location,
        var: VarId,
    },
    CoefficientOutOfRange {
        location: Location,
        var: VarId,
        value: i64,
    },
    RhsOutOfRange {
        constraint: usize,
        value: i64,
    },
    EmptyRow {
        constraint: usize,
    },
    CardinalityOutOfRange {
        constraint: usize,
        k: u32,
        len: usize,
    },
    SelfImplication {
        constraint: usize,
        var: VarId,
    },
    ObjectiveRangeOverflow,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, index) = match self {
            Location::Variable(index) => ("variable", index),
            Location::Constraint(index) => ("constraint", index),
            Location::ObjectiveLevel(index) => ("objective level", index),
        };
        write!(f, "{kind} {index}")
    }
}

impl fmt::Display for ModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid 0-1 model: {self:?}")
    }
}

impl std::error::Error for ModelError {}
