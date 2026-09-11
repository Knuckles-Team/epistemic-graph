use std::fmt;

/// A GraphQL parse error with the byte offset it occurred at.
#[derive(Clone, Debug, PartialEq)]
pub struct GqlError {
    pub msg: String,
    pub at: usize,
}

impl fmt::Display for GqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphQL parse error at byte {}: {}", self.at, self.msg)
    }
}

impl std::error::Error for GqlError {}
