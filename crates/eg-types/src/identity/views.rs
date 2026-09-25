//! What a reader sees. Every view is REDACTED by construction: it is built
//! field by field from a record and has no field that could hold a password
//! hash, a token hash or a sealed secret.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::access::{GroupRecord, IdpConfig, RoleRecord};
use super::audit::IdentityAuditEntry;
use super::config::IdentityConfig;
use super::model::{ApiKeyRecord, SessionRecord, UserKind, UserRecord, UserStatus};
use super::projection::PrincipalResolution;
use super::requests_provision::{DirectoryGroup, ProvisionedUser};

/// A principal without credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UserView {
    pub principal_id: String,
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    pub kind: UserKind,
    pub status: UserStatus,
    pub is_bootstrap: bool,
    pub source: String,
    pub roles: BTreeSet<String>,
    pub has_password: bool,
    pub totp_enrolled: bool,
    pub recovery_codes_left: usize,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_login_at_ms: Option<u64>,
}

impl UserView {
    pub(crate) fn of(user: &UserRecord, factors: CredentialFacts) -> Self {
        Self {
            principal_id: user.principal_id.clone(),
            username: user.username.clone(),
            display_name: user.display_name.clone(),
            email: user.email.clone(),
            kind: user.kind,
            status: user.status,
            is_bootstrap: user.is_bootstrap,
            source: user.source.clone(),
            roles: user.roles.clone(),
            has_password: factors.has_password,
            totp_enrolled: factors.totp_enrolled,
            recovery_codes_left: factors.recovery_codes_left,
            created_at_ms: user.created_at_ms,
            last_login_at_ms: user.last_login_at_ms,
        }
    }
}

/// Which credentials a principal has, without any of their material.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CredentialFacts {
    pub(crate) has_password: bool,
    pub(crate) totp_enrolled: bool,
    pub(crate) recovery_codes_left: usize,
}

/// A session without its id or id hash. `handle` is a short, one-way
/// prefix an operator can use to tell sessions apart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SessionView {
    pub handle: String,
    pub principal_id: String,
    pub created_at_ms: u64,
    pub last_seen_at_ms: u64,
    pub idle_expires_at_ms: u64,
    pub absolute_expires_at_ms: u64,
    pub auth_methods: Vec<String>,
    pub mfa_pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_prefix: Option<String>,
    pub revoked: bool,
}

/// Length of a session handle (hex characters of the hash, re-hashed).
const HANDLE_CHARS: usize = 12;

impl SessionView {
    pub(crate) fn of(session: &SessionRecord) -> Self {
        let handle =
            super::store::digest_hex(b"eg/identity-session-handle/v1\0", &session.session_hash);
        Self {
            handle: handle[..HANDLE_CHARS].to_string(),
            principal_id: session.principal_id.clone(),
            created_at_ms: session.created_at_ms,
            last_seen_at_ms: session.last_seen_at_ms,
            idle_expires_at_ms: session.idle_expires_at_ms,
            absolute_expires_at_ms: session.absolute_expires_at_ms,
            auth_methods: session.auth_methods.clone(),
            mfa_pending: session.mfa_pending,
            ip_prefix: session.ip_prefix.clone(),
            revoked: session.revoked_at_ms.is_some(),
        }
    }
}

/// API-key metadata. The secret hash is intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ApiKeyView {
    pub key_id: String,
    pub principal_id: String,
    pub scopes: BTreeSet<String>,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub last_used_at_ms: Option<u64>,
    pub revoked_at_ms: Option<u64>,
}

/// The service principal permitted to provision one SCIM IdP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ScimClientView {
    pub idp_id: String,
    pub principal_id: String,
    pub enabled: bool,
}

impl ApiKeyView {
    pub(crate) fn of(key: &ApiKeyRecord) -> Self {
        Self {
            key_id: key.key_id.clone(),
            principal_id: key.principal_id.clone(),
            scopes: key.scopes.clone(),
            created_at_ms: key.created_at_ms,
            expires_at_ms: key.expires_at_ms,
            last_used_at_ms: key.last_used_at_ms,
            revoked_at_ms: key.revoked_at_ms,
        }
    }
}

/// How a sign-in resolved. Unknown user, wrong password and disabled
/// account all answer `bad`: a caller cannot enumerate accounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AuthenticateOutcome {
    /// A live session was opened.
    Ok,
    Bad,
    /// Too many recent failures; retry after `retry_after_ms`.
    Throttled,
    /// A second factor is owed; the session is open but `mfa_pending`.
    MfaRequired,
    /// A group requires MFA and none is enrolled; no session was opened.
    MfaEnrollmentRequired,
    /// The password must be changed first; no session was opened.
    PasswordChangeRequired,
}

/// The answer to `authenticate` / `external_login` / a second factor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AuthenticateResult {
    pub outcome: AuthenticateOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl AuthenticateResult {
    pub(crate) fn bad() -> Self {
        Self {
            outcome: AuthenticateOutcome::Bad,
            principal_id: None,
            retry_after_ms: None,
        }
    }
}

/// A WebAuthn credential as the broker needs it to verify an assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WebauthnCredentialView {
    pub credential_id: String,
    pub public_key_cose: String,
    pub sign_count: u32,
    pub transports: Vec<String>,
    pub name: String,
}

/// Where a self-service reset link goes (`None`: nowhere, uniformly).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResetDelivery {
    pub email: Option<String>,
}

/// Every identity op's answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IdentityReply {
    /// A write that answers nothing but whether it changed state.
    Done {
        changed: bool,
    },
    Config(IdentityConfig),
    Principal {
        principal_id: String,
    },
    User(UserView),
    Users(Vec<UserView>),
    Resolution(PrincipalResolution),
    Authenticate(AuthenticateResult),
    Sessions(Vec<SessionView>),
    ApiKeys(Vec<ApiKeyView>),
    ScimClient(ScimClientView),
    ScimClients(Vec<ScimClientView>),
    Roles(Vec<RoleRecord>),
    Groups(Vec<GroupRecord>),
    Idps(Vec<IdpConfig>),
    Audit(Vec<IdentityAuditEntry>),
    AuditVerification {
        valid: bool,
        first_broken_seq: Option<u64>,
    },
    /// `export_sql`: the dump text.
    Sql(String),
    /// `list_provisioned`.
    Provisioned(Vec<ProvisionedUser>),
    DirectoryGroup(DirectoryGroup),
    DirectoryGroups(Vec<DirectoryGroup>),
    /// `webauthn_credentials`.
    WebauthnCredentials(Vec<WebauthnCredentialView>),
    /// `issue_password_reset`.
    ResetDelivery(ResetDelivery),
}
