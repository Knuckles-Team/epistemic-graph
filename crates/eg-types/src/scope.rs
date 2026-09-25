//! Canonical scope class currency shared by the capability registry and the
//! identity store. It lives below both to avoid a second class enum when the
//! identity store joins the engine.

use serde::{Deserialize, Serialize};

/// Who may hold a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ScopeClass {
    /// Ordinary data access; humans and services.
    User,
    /// A domain-app capability; humans and services.
    Domain,
    /// Infrastructure executed on a caller's behalf; services only.
    ServiceOnly,
    /// Two-person approvals; humans only through the built-in group.
    Approver,
    /// Administration; humans only.
    Admin,
}

impl ScopeClass {
    /// API keys cannot carry approvals or administration.
    pub fn allows_api_key(self) -> bool {
        matches!(self, Self::User | Self::Domain | Self::ServiceOnly)
    }
}

/// The registry as a future identity store consumes it.
pub trait ScopeClassifier {
    /// The class of `scope`, or `None` when it is not registered.
    fn class_of(&self, scope: &str) -> Option<ScopeClass>;

    /// The built-in group conferring an approver-class scope, if any.
    fn approver_group_of(&self, scope: &str) -> Option<&'static str>;
}
