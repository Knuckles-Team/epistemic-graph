//! Scope classes (IDM-05) and the store invariants they carry.
//!
//! The scope REGISTRY (every scope and its class) lives in `eg-capabilities`,
//! generated alongside the capability ledger; this crate sits below it, so
//! the store receives the registry through [`ScopeClassifier`].

use serde::{Deserialize, Serialize};

/// Who may hold a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ScopeClass {
    /// Ordinary data access; humans and services.
    User,
    /// A domain-app capability (`finance:alerts`, …); humans and services.
    Domain,
    /// Infrastructure executed on a caller's behalf (`broker:*`,
    /// `capacity:*`, `compute:*`, …): never reachable by a human.
    ServiceOnly,
    /// Two-person approvals (`rbac:approve-elevation`, …): humans only, and
    /// only through the scope's built-in approver group.
    Approver,
    /// Administration (`kg:admin`, `identity:admin`, …): humans only.
    Admin,
}

impl ScopeClass {
    /// Whether a principal of `kind` may hold a scope of this class at all.
    pub fn allows_kind(self, kind: super::UserKind) -> bool {
        match self {
            Self::User | Self::Domain => true,
            Self::ServiceOnly => kind == super::UserKind::Service,
            Self::Approver | Self::Admin => kind == super::UserKind::Human,
        }
    }

    /// Whether an API key may carry a scope of this class. API keys are
    /// bearer credentials, so neither approvals nor administration ride one.
    pub fn allows_api_key(self) -> bool {
        matches!(self, Self::User | Self::Domain | Self::ServiceOnly)
    }
}

/// The registry as the store sees it.
pub trait ScopeClassifier {
    /// The class of `scope`, or `None` when it is not registered.
    fn class_of(&self, scope: &str) -> Option<ScopeClass>;

    /// The one built-in group through which an approver-class scope may be
    /// held (`None` for every other class).
    fn approver_group_of(&self, scope: &str) -> Option<&'static str>;
}
