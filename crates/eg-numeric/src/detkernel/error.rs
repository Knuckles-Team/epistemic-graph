//! Typed errors for the deterministic statistics modules (`detkernel`,
//! `calibration`, `risk`, `conformal`, `ope`).
//!
//! Every variant carries integers and static names only, never an `f64`, so the
//! error is `Eq`, hashes deterministically and never prints a platform-formatted
//! float.

use std::fmt;

/// A refusal from the deterministic statistics kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatError {
    /// An input that must be non-empty was empty.
    Empty { what: &'static str },
    /// Two inputs that must have the same length did not.
    LengthMismatch {
        what: &'static str,
        expected: usize,
        actual: usize,
    },
    /// A NaN or an infinity where a finite value is required.
    NonFinite { what: &'static str, index: usize },
    /// A finite value outside its domain (for example a probability above 1).
    OutOfDomain {
        what: &'static str,
        index: usize,
        domain: &'static str,
    },
    /// A scalar parameter violates its requirement.
    InvalidParameter {
        name: &'static str,
        requirement: &'static str,
    },
    /// A class label is not below the number of classes.
    ClassOutOfRange {
        index: usize,
        label: usize,
        classes: usize,
    },
    /// Fewer samples than a guarantee needs; no claim may be made.
    InsufficientSamples {
        what: &'static str,
        required: u64,
        actual: u64,
    },
    /// A logged record's executed action has logging propensity zero, so no
    /// inverse-propensity weight exists for it.
    ZeroLoggingPropensity { action: usize },
    /// The evaluated policy puts mass on an action the logging policy never
    /// takes (logging propensity 0): the estimate would be outside support.
    UnsupportedAction { record: usize, action: usize },
    /// An iterative routine did not converge within its fixed budget.
    NoConvergence { what: &'static str, iterations: u32 },
    /// A value does not fit the fixed-point representation.
    QuantiseOverflow { index: usize },
    /// Exact integer arithmetic overflowed.
    ArithmeticOverflow { what: &'static str },
}

impl fmt::Display for StatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            StatError::Empty { what } => write!(f, "{what} must not be empty"),
            StatError::LengthMismatch {
                what,
                expected,
                actual,
            } => write!(f, "{what}: expected length {expected}, got {actual}"),
            StatError::NonFinite { what, index } => {
                write!(f, "{what}[{index}] is not finite")
            }
            StatError::OutOfDomain {
                what,
                index,
                domain,
            } => write!(f, "{what}[{index}] is outside {domain}"),
            StatError::InvalidParameter { name, requirement } => {
                write!(f, "parameter {name} must satisfy {requirement}")
            }
            StatError::ClassOutOfRange {
                index,
                label,
                classes,
            } => write!(f, "label[{index}] = {label} is not below {classes} classes"),
            StatError::InsufficientSamples {
                what,
                required,
                actual,
            } => write!(f, "{what}: {actual} samples, at least {required} required"),
            StatError::ZeroLoggingPropensity { action } => {
                write!(f, "executed action {action} has logging propensity 0")
            }
            StatError::UnsupportedAction { record, action } => write!(
                f,
                "record {record}: action {action} has target mass but logging propensity 0"
            ),
            StatError::NoConvergence { what, iterations } => {
                write!(f, "{what} did not converge in {iterations} iterations")
            }
            StatError::QuantiseOverflow { index } => {
                write!(f, "value {index} does not fit the fixed-point range")
            }
            StatError::ArithmeticOverflow { what } => {
                write!(f, "exact arithmetic overflowed in {what}")
            }
        }
    }
}

impl std::error::Error for StatError {}

impl From<StatError> for crate::NumericError {
    fn from(error: StatError) -> Self {
        crate::NumericError::bounds(error.to_string())
    }
}

/// Result alias for the statistics modules.
pub type StatResult<T> = std::result::Result<T, StatError>;
