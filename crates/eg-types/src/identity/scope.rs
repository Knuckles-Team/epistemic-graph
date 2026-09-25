//! Identity-store behavior for the canonical IDM-05 scope classes.
//!
//! The type and registry-facing trait live in `eg_types::scope`, below both
//! identity and eg-capabilities. Identity only adds its user-kind invariant.

pub use crate::scope::{ScopeClass, ScopeClassifier};

impl ScopeClass {
    /// Whether a principal of `kind` may hold this scope class.
    pub fn allows_kind(self, kind: super::UserKind) -> bool {
        match self {
            Self::User | Self::Domain => true,
            Self::ServiceOnly => kind == super::UserKind::Service,
            Self::Approver | Self::Admin => kind == super::UserKind::Human,
        }
    }
}
