//! Principal and credential records (§3.2). Hash and sealed-secret fields are
//! never serialized into a view; see `views.rs` for what a reader sees.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Whether a principal is a person or a workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum UserKind {
    Human,
    Service,
}

/// A principal's lifecycle state. Nothing is ever deleted: a deprovisioned
/// principal keeps its id, links and owned data, and holds no authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum UserStatus {
    Active,
    Disabled,
    /// Must set a new password (admin reset, or local mode entered without a
    /// credential) before any session is issued.
    PendingReset,
    PendingVerification,
    Deprovisioned,
}

impl UserStatus {
    /// Only an active principal holds authority or may sign in.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// One principal. `principal_id` is opaque, stable and never renamed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserRecord {
    pub principal_id: String,
    /// Normalized (see `normalize_username`), unique, mutable.
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    pub kind: UserKind,
    pub status: UserStatus,
    /// The built-in demo/bootstrap principal (`usr:bootstrap`).
    #[serde(default)]
    pub is_bootstrap: bool,
    /// `local`, `scim:<idp>`, `ldap:<idp>` or `jit:<idp>`.
    pub source: String,
    /// Roles granted directly (group roles are resolved through groups).
    #[serde(default)]
    pub roles: BTreeSet<String>,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_login_at_ms: Option<u64>,
}

/// A password credential: an argon2id PHC string computed by the engine at
/// the request boundary. The candidate never reaches the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordCredential {
    pub hash: String,
    pub changed_at_ms: u64,
    #[serde(default)]
    pub must_change: bool,
    /// Previous hashes, newest first, for the reuse check.
    #[serde(default)]
    pub history: Vec<String>,
}

/// A server-side session. Only the SHA-256 of the session id is stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRecord {
    pub session_hash: String,
    pub principal_id: String,
    pub created_at_ms: u64,
    pub last_seen_at_ms: u64,
    pub idle_expires_at_ms: u64,
    pub absolute_expires_at_ms: u64,
    /// `pwd`, `totp`, `recovery`, `idp:<id>`.
    pub auth_methods: Vec<String>,
    /// The session still owes a second factor; it authorizes nothing until
    /// `verify_totp` / `consume_recovery_code` completes it.
    #[serde(default)]
    pub mfa_pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mfa_at_ms: Option<u64>,
    /// Truncated client address (/24 v4, /48 v6) for audit only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoke_reason: Option<String>,
}

impl SessionRecord {
    /// Live = not revoked, inside both expiry bounds, and second factor done.
    pub fn is_live(&self, now_ms: u64) -> bool {
        self.revoked_at_ms.is_none()
            && now_ms < self.idle_expires_at_ms
            && now_ms < self.absolute_expires_at_ms
    }
}

/// What a one-time token is for. A token redeems only for its purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TokenPurpose {
    PasswordReset,
    EmailVerify,
    Invite,
    LinkClaim,
    AdminReset,
}

/// A single-use token. Only its hash is stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OneTimeToken {
    pub token_hash: String,
    pub purpose: TokenPurpose,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    pub expires_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_at_ms: Option<u64>,
    pub created_by: String,
}

/// An API key. `key_id` is public (`gok_<id>`); only the secret's hash is
/// stored. Its scopes are a subset of its owner's, re-checked at every use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyRecord {
    pub key_id: String,
    pub principal_id: String,
    pub secret_hash: String,
    pub scopes: BTreeSet<String>,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at_ms: Option<u64>,
}

/// A TOTP factor. The shared secret is sealed by the engine's data key at
/// the request boundary; the store never holds it in the clear.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TotpRecord {
    pub sealed_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_at_ms: Option<u64>,
    /// Highest accepted RFC 6238 time step (replay guard).
    #[serde(default)]
    pub last_step: u64,
}

/// A link from an identity-provider subject to a principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExternalIdentity {
    pub idp_id: String,
    pub subject: String,
    pub principal_id: String,
    pub linked_at_ms: u64,
    pub linked_by: String,
}
