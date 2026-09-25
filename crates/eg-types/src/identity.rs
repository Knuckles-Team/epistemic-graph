//! The engine-owned identity store (IDENTITY-AND-AUTH-MODES-DESIGN §3, rows
//! IDM-01..05).
//!
//! EG is the sole durable authority for who a principal is. This module holds
//! the pure data model and the pure transitions of that store: users and their
//! credentials, sessions, one-time tokens, API keys, TOTP and recovery codes,
//! roles, groups, identity providers and their links, the login throttle, the
//! auth-mode singleton, and a bounded hash-chained identity audit trail.
//!
//! Three properties shape it:
//!
//! * **Secrets never reach the store in the clear.** Every password, token and
//!   code is hashed (argon2id for low-entropy secrets, SHA-256 for high-entropy
//!   tokens) or sealed by the engine at the request boundary, before the
//!   request is replicated or logged. The store only compares what the boundary
//!   stamped ([`IdentityStamp`]). No read returns a hash or a sealed secret.
//! * **Pure and deterministic.** Transitions take the clock and every
//!   engine-derived value as arguments, so a replica applying the same command
//!   reaches the same state.
//! * **Class rules are store invariants.** A scope's class (user / domain /
//!   service-only / approver / admin, [`ScopeClass`]) decides who may reach it;
//!   a transition that would break a class rule is refused as a whole.
//!
//! The RBAC projection ([`IdentityStore::rbac_projection`]) is what the engine
//! writes into its RBAC image in the SAME durable write as the store change.

use serde::{Deserialize, Serialize};

mod access;
mod audit;
mod config;
mod denials;
mod model;
mod ops;
mod password_policy;
mod projection;
mod relations;
mod requests;
mod requests_admin;
mod requests_provision;
mod scope;
mod sql_dump;
mod stamp;
mod store;
mod text;
mod views;

pub use access::{GroupRecord, IdpConfig, IdpKind, JitPolicy, MappingRule, RoleRecord};
pub use audit::{
    AuditRecord, AuditTrail, IdentityAuditEntry, IdentityEvent, MAX_IDENTITY_AUDIT_ENTRIES,
};
pub use config::{
    AuthMode, IdentityConfig, IssuerRotation, LocalFallback, ModeTransition, RegistrationPolicy,
    DEFAULT_PASSWORD_MIN_CHARS, MAX_PASSWORD_CHARS, NONE_MODE_ACK,
};
pub use denials::{DenialSample, DenialSampler, MAX_DENIAL_SAMPLES};
pub use model::{
    ApiKeyRecord, ExternalIdentity, OneTimeToken, PasswordCredential, SessionRecord, TokenPurpose,
    TotpRecord, UserKind, UserRecord, UserStatus, WebauthnRecord,
};
pub use ops::{
    AccessOp, ConfigOp, CredentialOp, IdentityOp, IdpOp, MfaOp, OpAuthority, OpMeta, SessionOp,
    TokenOp, UserOp, IDENTITY_ADMIN_SCOPE, IDENTITY_AUTHENTICATE_SCOPE, IDENTITY_PROVISION_SCOPE,
    IDENTITY_READ_SCOPE, IDENTITY_SELF_SCOPE,
};
pub use password_policy::check_password;
pub use projection::{rbac_role_name, PrincipalResolution, RbacProjection};
pub use relations::{SqlRelation, SqlType};
pub use requests::{
    AdminResetIssue, ApiKeyIssue, ApiKeyUse, AuthenticateRequest, CreateUserRequest, ExternalLogin,
    InitializeRequest, LinkRequest, OneTimeTokenIssue, PasswordChange, PasswordResetIssue,
    PasswordSet, RecoveryCodesSet, SessionTouch, TokenRedeem, TotpEnroll, UserStatusChange,
    UserUpdate, WebauthnCredential, WebauthnUse,
};
pub use requests_admin::{
    BindingChange, GroupMembershipChange, GroupUpsert, ListQuery, ObjectRef, PolicyUpdate,
    PrincipalListQuery, RoleGraphGrant, RoleUpsert, ScimClientBinding, SqlDump, UserRoleChange,
    UserSearch, MAX_PAGE,
};
pub use requests_provision::{
    DirectoryGroup, DirectoryGroupQuery, DirectoryGroupRef, ProvisionSubject, ProvisionedQuery,
    ProvisionedUser,
};
pub use scope::{ScopeClass, ScopeClassifier};
pub use sql_dump::{parse_dump, render_dump, DumpRow, MAX_DUMP_BYTES};
pub use stamp::{IdentityActor, IdentityStamp, PasswordCheck, Secret};
pub use store::{ApplyContext, IdentityStore, RecoveryCode, ThrottleEntry, TOUCH_GRANULARITY_MS};
pub use text::{
    check_recovery_code, check_token, check_totp_secret, normalize_username, validate_principal_id,
    MIN_RECOVERY_CODE_CHARS, MIN_TOTP_SECRET_CHARS,
};
pub use views::{
    ApiKeyView, AuthenticateOutcome, AuthenticateResult, IdentityReply, MfaStatusView,
    ResetDelivery, ScimClientView, SessionView, UserView, WebauthnCredentialView,
};

/// The principal id of the built-in bootstrap user (§2.2.3).
pub const BOOTSTRAP_PRINCIPAL: &str = "usr:bootstrap";
/// Built-in group every administrator belongs to.
pub const ADMINISTRATORS_GROUP: &str = "administrators";
/// Built-in approver groups (the only way to hold an approver-class scope).
pub const ELEVATION_APPROVERS_GROUP: &str = "elevation-approvers";
pub const LIVE_ORDER_APPROVERS_GROUP: &str = "live-order-approvers";
/// EH-560: approvers of governed schema repairs.
pub const SCHEMA_APPROVERS_GROUP: &str = "schema-approvers";
/// Built-in roles seeded at initialize.
pub const ADMIN_ROLE: &str = "admin";
pub const USER_ROLE: &str = "user";
pub const ELEVATION_APPROVER_ROLE: &str = "elevation-approver";
pub const LIVE_ORDER_APPROVER_ROLE: &str = "live-order-approver";
pub const SCHEMA_APPROVER_ROLE: &str = "schema-approver";
/// Prefix of every RBAC role the store projects, so the projection owns
/// exactly its own roles and never touches one registered by another path.
pub const RBAC_ROLE_PREFIX: &str = "idm:";

/// Most users (all kinds, including deprovisioned) one store holds.
pub const MAX_USERS: usize = 10_000;
/// Most live sessions one principal holds; the oldest is evicted beyond it.
pub const MAX_SESSIONS_PER_USER: usize = 32;
/// Most WebAuthn credentials one principal holds.
pub const MAX_WEBAUTHN_PER_USER: usize = 16;
/// Most live API keys one principal holds.
pub const MAX_API_KEYS_PER_USER: usize = 16;
/// Most outstanding one-time tokens one store holds.
pub const MAX_ONE_TIME_TOKENS: usize = 4_096;
/// Most roles, groups and identity providers.
pub const MAX_ROLES: usize = 512;
pub const MAX_GROUPS: usize = 512;
pub const MAX_IDPS: usize = 32;
/// Most directory groups (all IdPs) one store holds.
pub const MAX_DIRECTORY_GROUPS: usize = 4_096;
/// Largest claim set (bytes of paths and values) one provisioned subject
/// carries.
pub const MAX_PROVISIONED_CLAIM_BYTES: usize = 16 * 1024;
/// Most mapping rules per identity provider.
pub const MAX_MAPPING_RULES: usize = 256;
/// Most throttle keys tracked (older windows are evicted first).
pub const MAX_THROTTLE_KEYS: usize = 8_192;
/// Recovery codes per set.
pub const RECOVERY_CODES_PER_SET: usize = 10;
/// Password history depth (reuse check).
pub const PASSWORD_HISTORY_DEPTH: usize = 5;
/// Longest API-key lifetime (one year).
pub const MAX_API_KEY_LIFETIME_MS: u64 = 365 * 24 * 60 * 60 * 1000;
/// Longest one-time token lifetime (one day); link-claim codes use 10 min.
pub const MAX_ONE_TIME_TOKEN_LIFETIME_MS: u64 = 24 * 60 * 60 * 1000;
/// Shortest accepted high-entropy token (caller-generated session ids,
/// one-time tokens, API-key secrets): 32 characters of a URL-safe alphabet.
pub const MIN_TOKEN_CHARS: usize = 32;

/// Why an identity operation was refused. Every refusal is typed; none is a
/// fail-open fallback. Authentication failures are deliberately NOT refusals:
/// they are an [`AuthenticateOutcome`], uniform for unknown and known users.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IdentityRefusal {
    /// A field is outside its native bounds.
    InvalidRequest,
    /// The actor does not hold the exact scope this op needs.
    NotAuthorized,
    /// The store has not been initialized, or was already initialized.
    NotInitialized,
    AlreadyInitialized,
    /// No such user / role / group / identity provider / token / session.
    NotFound,
    /// A unique name or id is already taken.
    Collision,
    /// A bound (users, sessions, tokens, …) is reached.
    Full,
    /// A class rule would be broken (service-only scope reachable by a
    /// human, approver scope outside its built-in group, …).
    ClassViolation,
    /// A scope is not in the scope registry.
    UnknownScope,
    /// The password policy refused the candidate.
    WeakPassword,
    /// The candidate matches one of the last passwords.
    PasswordReused,
    /// The current password did not verify.
    BadCredential,
    /// The expected epoch does not match (a concurrent change won).
    EpochConflict,
    /// A mode transition's precondition does not hold.
    PreconditionFailed,
    /// The transition is not in the state machine.
    IllegalTransition,
    /// A built-in object cannot be removed or renamed.
    BuiltIn,
    /// The boundary did not stamp what this op needs (defense in depth: a
    /// replicated command without its stamp, or a stamp of the wrong kind).
    Unstamped,
    /// The token was already used, has expired, or was revoked.
    TokenSpent,
    /// A TOTP step was already used (replay).
    Replay,
    /// The principal's kind does not allow this (e.g. an API key for a
    /// service-only scope held by a human).
    KindMismatch,
}

impl IdentityRefusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "IDENTITY_INVALID",
            Self::NotAuthorized => "IDENTITY_NOT_AUTHORIZED",
            Self::NotInitialized => "IDENTITY_NOT_INITIALIZED",
            Self::AlreadyInitialized => "IDENTITY_ALREADY_INITIALIZED",
            Self::NotFound => "IDENTITY_NOT_FOUND",
            Self::Collision => "IDENTITY_COLLISION",
            Self::Full => "IDENTITY_FULL",
            Self::ClassViolation => "IDENTITY_CLASS_VIOLATION",
            Self::UnknownScope => "IDENTITY_UNKNOWN_SCOPE",
            Self::WeakPassword => "IDENTITY_WEAK_PASSWORD",
            Self::PasswordReused => "IDENTITY_PASSWORD_REUSED",
            Self::BadCredential => "IDENTITY_BAD_CREDENTIAL",
            Self::EpochConflict => "IDENTITY_EPOCH_CONFLICT",
            Self::PreconditionFailed => "IDENTITY_PRECONDITION_FAILED",
            Self::IllegalTransition => "IDENTITY_ILLEGAL_TRANSITION",
            Self::BuiltIn => "IDENTITY_BUILT_IN",
            Self::Unstamped => "IDENTITY_UNSTAMPED",
            Self::TokenSpent => "IDENTITY_TOKEN_SPENT",
            Self::Replay => "IDENTITY_REPLAY",
            Self::KindMismatch => "IDENTITY_KIND_MISMATCH",
        }
    }
}

impl std::fmt::Display for IdentityRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: identity operation refused ({self:?})", self.code())
    }
}

impl std::error::Error for IdentityRefusal {}

#[cfg(test)]
mod tests;
