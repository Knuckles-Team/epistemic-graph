//! Typed error enum for the SQLite file-format layer: never `unwrap` on untrusted bytes.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// The bytes are not a well-formed SQLite database (bad magic, out-of-range field,
    /// truncated cell/varint, …).
    #[error("corrupt SQLite file: {0}")]
    Corrupt(String),
    /// A well-formed SQLite construct this crate deliberately does not support
    /// (WITHOUT ROWID, index b-trees, …).
    #[error("unsupported SQLite construct: {0}")]
    Unsupported(String),
    /// Underlying filesystem I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn corrupt(msg: impl Into<String>) -> Self {
        Error::Corrupt(msg.into())
    }
    pub(crate) fn unsupported(msg: impl Into<String>) -> Self {
        Error::Unsupported(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
