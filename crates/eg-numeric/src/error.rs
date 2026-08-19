//! Kernel error type. `LinAlgError` mirrors `numpy.linalg.LinAlgError` so the
//! Python surface (Surface A) can raise the parity-equivalent exception, and the
//! engine (Surface B) can surface a typed analytics error.

use std::fmt;

/// Errors raised by the numeric kernel. The `LinAlg` variant is the parity twin
/// of `numpy.linalg.LinAlgError` (singular matrix, non-convergence, shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumericError {
    /// numpy `LinAlgError` equivalent (singular / non-PD / non-square / no-converge).
    LinAlg(String),
    /// Bad shape / dimension mismatch (numpy raises `ValueError`).
    Shape(String),
    /// A value has a type that the bounded native boundary does not support.
    Type(String),
    /// A parameter is outside the operation's valid domain.
    Bounds(String),
    /// An operation would exceed the kernel's explicit resource budget.
    Resource(String),
    /// Random-distribution parameters are invalid.
    Random(String),
}

impl NumericError {
    pub fn linalg(msg: impl Into<String>) -> Self {
        NumericError::LinAlg(msg.into())
    }
    pub fn shape(msg: impl Into<String>) -> Self {
        NumericError::Shape(msg.into())
    }

    pub fn type_error(msg: impl Into<String>) -> Self {
        NumericError::Type(msg.into())
    }

    pub fn bounds(msg: impl Into<String>) -> Self {
        NumericError::Bounds(msg.into())
    }

    pub fn resource(msg: impl Into<String>) -> Self {
        NumericError::Resource(msg.into())
    }

    pub fn random(msg: impl Into<String>) -> Self {
        NumericError::Random(msg.into())
    }
}

impl fmt::Display for NumericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NumericError::LinAlg(m) => write!(f, "LinAlgError: {m}"),
            NumericError::Shape(m) => write!(f, "ValueError: {m}"),
            NumericError::Type(m) => write!(f, "TypeError: {m}"),
            NumericError::Bounds(m) => write!(f, "ValueError: {m}"),
            NumericError::Resource(m) => write!(f, "ResourceError: {m}"),
            NumericError::Random(m) => write!(f, "ValueError: {m}"),
        }
    }
}

impl std::error::Error for NumericError {}

pub type Result<T> = std::result::Result<T, NumericError>;
