//! Request bodies of the principal, credential and session ops. Every
//! [`Secret`] here is hashed, verified or sealed at the request boundary and
//! cleared before the op is replicated.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::config::AuthMode;
use super::model::{TokenPurpose, UserKind, UserStatus};
use super::stamp::Secret;

/// `initialize`: seed the mode singleton and the bootstrap principal. Local
/// mode requires the first administrator's username and password (operator
/// ruling: every fresh instance forces creation of an admin user).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InitializeRequest {
    pub mode: AuthMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_username: Option<String>,
    #[serde(default)]
    pub admin_password: Secret,
}

/// `create_user`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CreateUserRequest {
    pub username: String,
    pub kind: UserKind,
    /// An explicit principal id (homelab migration: an existing identity
    /// provider subject becomes the principal id verbatim). Minted
    /// (`usr:<uuid>`) when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub groups: BTreeSet<String>,
    /// Optional initial password (humans only).
    #[serde(default)]
    pub password: Secret,
    #[serde(default)]
    pub must_change: bool,
}

/// `update_user`: each present field replaces the stored one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UserUpdate {
    pub principal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// `set_user_status`. Leaving `active` revokes every session and API key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UserStatusChange {
    pub principal_id: String,
    pub status: UserStatus,
}

/// `set_password` (administrator): the admin never learns the result; the
/// usual flow is an `admin_reset` one-time token instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PasswordSet {
    pub principal_id: String,
    pub password: Secret,
    #[serde(default)]
    pub must_change: bool,
}

/// `change_password` (self).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PasswordChange {
    pub current: Secret,
    pub new: Secret,
}

/// `authenticate`: verify a password and, on success, open a session under
/// the caller-generated `session_token` (only its hash is stored).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AuthenticateRequest {
    pub username: String,
    pub password: Secret,
    pub session_token: Secret,
    /// Truncated client address (/24 v4, /48 v6); keys the per-network
    /// throttle and the audit entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_prefix: Option<String>,
    /// A replacement password, applied only when the account must change its
    /// password (admin reset or `pending_reset`) and the current one verified.
    #[serde(default)]
    pub new_password: Secret,
}

/// A session-token-bearing op: `resolve_session`, `revoke_session`,
/// `bootstrap_session`, `consume_recovery_code`, `verify_totp`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SessionTouch {
    pub session_token: Secret,
    /// A TOTP or recovery code, for the second-factor ops.
    #[serde(default)]
    pub code: Secret,
}

/// `issue_one_time_token`: the broker generates the token for an
/// administrator's live session; only its hash is stored. `ttl_ms` is capped
/// at one day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OneTimeTokenIssue {
    /// The issuing administrator's live session (the broker acts for it).
    pub session_token: Secret,
    pub purpose: TokenPurpose,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    pub token: Secret,
    pub ttl_ms: u64,
}

/// `redeem_one_time_token`: spend a token for its purpose. A reset purpose
/// sets `new_password`; `link_claim` links `link`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TokenRedeem {
    pub purpose: TokenPurpose,
    pub token: Secret,
    #[serde(default)]
    pub new_password: Secret,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<LinkRequest>,
}

/// An identity-provider subject to link to a principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LinkRequest {
    pub idp_id: String,
    pub subject: String,
    /// Ignored for `link_claim` (the token names the principal).
    #[serde(default)]
    pub principal_id: String,
}

/// `issue_api_key`: `key_id` is public; the secret is caller-generated and
/// only its hash is stored. Scopes must be a subset of the owner's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ApiKeyIssue {
    /// The issuing administrator's live session (the broker acts for it).
    pub session_token: Secret,
    pub principal_id: String,
    pub key_id: String,
    pub secret: Secret,
    pub scopes: BTreeSet<String>,
    pub ttl_ms: u64,
}

/// `verify_api_key`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ApiKeyUse {
    pub key_id: String,
    pub secret: Secret,
}

/// `enroll_totp`: the broker generates the base32 secret for the principal
/// of `session_token`, shows it to the user once, and sends it here to be
/// sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TotpEnroll {
    pub session_token: Secret,
    pub secret_base32: Secret,
}

/// `set_recovery_codes`: the broker generates a new set for the principal
/// of `session_token`; it replaces the whole set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecoveryCodesSet {
    pub session_token: Secret,
    pub codes: Vec<Secret>,
}

/// `external_login`: graph-os verified an identity-provider assertion and
/// forwards the subject and the claim values the mapping rules read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExternalLogin {
    pub idp_id: String,
    pub subject: String,
    /// `claim_path → values` (e.g. `groups → [...]`).
    #[serde(default)]
    pub claims: std::collections::BTreeMap<String, Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username_hint: Option<String>,
    pub session_token: Secret,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_prefix: Option<String>,
}
