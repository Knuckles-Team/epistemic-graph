//! The `Method::Identity` operation family (IDM-01).
//!
//! Grouped into eight families so each dispatcher stays a small exhaustive
//! match. On the wire an op is one object carrying both tags:
//! `{"family": "user", "op": "create", "request": {...}}`.
//!
//! Authority is EXACT-scope only (`identity:*` is never implied by `kg:admin`
//! or `*`): see [`OpAuthority`].

use serde::{Deserialize, Serialize};

use super::access::IdpConfig;
use super::config::ModeTransition;
use super::requests::{
    ApiKeyIssue, ApiKeyUse, AuthenticateRequest, CreateUserRequest, ExternalLogin,
    InitializeRequest, LinkRequest, OneTimeTokenIssue, PasswordChange, PasswordSet,
    RecoveryCodesSet, SessionTouch, TokenRedeem, TotpEnroll, UserStatusChange, UserUpdate,
};
use super::requests_admin::{
    GroupMembershipChange, GroupUpsert, ListQuery, ObjectRef, PolicyUpdate, RoleUpsert, SqlDump,
    UserRoleChange,
};

/// Administer identity: users, roles, groups, IdPs, policy, mode.
pub const IDENTITY_ADMIN_SCOPE: &str = "identity:admin";
/// Read the redacted identity directory and audit trail.
pub const IDENTITY_READ_SCOPE: &str = "identity:read";
/// The identity broker (graph-os): sign-ins, sessions, token redemption.
pub const IDENTITY_AUTHENTICATE_SCOPE: &str = "identity:authenticate";
/// A principal managing its own credentials and second factors.
pub const IDENTITY_SELF_SCOPE: &str = "identity:self";
/// SCIM provisioning (registered for IDM-14; no op uses it yet).
pub const IDENTITY_PROVISION_SCOPE: &str = "identity:provision";

/// Which exact scope an op needs.
///
/// Every op that carries a caller-generated high-entropy secret (a session
/// id, a one-time token, an API-key secret, a TOTP secret, recovery codes)
/// is a BROKER op: only `identity:authenticate` (service-only) may submit
/// one, and the engine enforces the entropy floor on it. When such an op acts
/// for a person (issuing a reset token, enrolling a factor) the person is
/// proven by a live session in this store, not by a claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpAuthority {
    /// `identity:admin`, direct (undelegated) actor.
    Admin,
    /// `identity:read` or `identity:admin`.
    Read,
    /// `identity:authenticate` (the broker).
    Broker,
    /// `identity:self`, acting on the actor's own principal.
    SelfService,
    /// `identity:authenticate` or `identity:admin`, and only while the store
    /// is uninitialized.
    FirstRun,
}

impl OpAuthority {
    /// The scope the ledger names for this authority.
    pub fn scope(self) -> &'static str {
        match self {
            Self::Admin => IDENTITY_ADMIN_SCOPE,
            Self::Read => IDENTITY_READ_SCOPE,
            Self::Broker | Self::FirstRun => IDENTITY_AUTHENTICATE_SCOPE,
            Self::SelfService => IDENTITY_SELF_SCOPE,
        }
    }
}

/// An op's static facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpMeta {
    pub name: &'static str,
    pub mutates: bool,
    pub authority: OpAuthority,
}

const fn meta(name: &'static str, mutates: bool, authority: OpAuthority) -> OpMeta {
    OpMeta {
        name,
        mutates,
        authority,
    }
}

/// Mode singleton, policy and the audit trail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ConfigOp {
    Initialize { request: InitializeRequest },
    Transition { request: ModeTransition },
    UpdatePolicy { request: PolicyUpdate },
    Get,
    Audit { request: ListQuery },
    /// The redacted Postgres dump of the store (backup / migration).
    ExportSql,
    /// Merge a dump produced by `export_sql` (restore / migration).
    ImportSql { request: SqlDump },
}

/// Principals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum UserOp {
    Create {
        request: CreateUserRequest,
    },
    Update {
        request: UserUpdate,
    },
    SetStatus {
        request: UserStatusChange,
    },
    Unlock {
        request: ObjectRef,
    },
    Get {
        request: ObjectRef,
    },
    List {
        request: ListQuery,
    },
    /// Effective roles, groups and scopes of one principal: what the local
    /// issuer puts in a token.
    Resolve {
        request: ObjectRef,
    },
}

/// Passwords and sign-in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CredentialOp {
    SetPassword {
        request: PasswordSet,
    },
    ChangePassword {
        request: PasswordChange,
    },
    Authenticate {
        request: AuthenticateRequest,
    },
    ExternalLogin {
        request: ExternalLogin,
    },
    /// `none` mode only: a session for the bootstrap principal.
    BootstrapSession {
        request: SessionTouch,
    },
}

/// Server-side sessions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SessionOp {
    /// Touch a live session and answer its principal's resolution.
    Resolve {
        request: SessionTouch,
    },
    Revoke {
        request: SessionTouch,
    },
    RevokeAll {
        request: ObjectRef,
    },
    List {
        request: ObjectRef,
    },
}

/// One-time tokens and API keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TokenOp {
    IssueOneTime { request: OneTimeTokenIssue },
    RedeemOneTime { request: TokenRedeem },
    IssueApiKey { request: ApiKeyIssue },
    VerifyApiKey { request: ApiKeyUse },
    RevokeApiKey { request: ObjectRef },
}

/// Second factors (TOTP and recovery codes). WebAuthn is IDM-09.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MfaOp {
    EnrollTotp { request: TotpEnroll },
    ConfirmTotp { request: SessionTouch },
    VerifyTotp { request: SessionTouch },
    SetRecoveryCodes { request: RecoveryCodesSet },
    ConsumeRecoveryCode { request: SessionTouch },
}

/// Roles, groups and bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AccessOp {
    UpsertRole { request: RoleUpsert },
    RemoveRole { request: ObjectRef },
    UpsertGroup { request: GroupUpsert },
    RemoveGroup { request: ObjectRef },
    ChangeMembership { request: GroupMembershipChange },
    ChangeUserRole { request: UserRoleChange },
    ListRoles,
    ListGroups,
}

/// Identity providers and subject links.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IdpOp {
    Upsert { request: IdpConfig },
    Remove { request: ObjectRef },
    Link { request: LinkRequest },
    Unlink { request: LinkRequest },
    List,
}

/// Every identity operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IdentityOp {
    Config(ConfigOp),
    User(UserOp),
    Credential(CredentialOp),
    Session(SessionOp),
    Token(TokenOp),
    Mfa(MfaOp),
    Access(AccessOp),
    Idp(IdpOp),
}

impl IdentityOp {
    /// The op's static facts.
    pub fn meta(&self) -> OpMeta {
        match self {
            Self::Config(op) => op.meta(),
            Self::User(op) => op.meta(),
            Self::Credential(op) => op.meta(),
            Self::Session(op) => op.meta(),
            Self::Token(op) => op.meta(),
            Self::Mfa(op) => op.meta(),
            Self::Access(op) => op.meta(),
            Self::Idp(op) => op.meta(),
        }
    }

    pub fn is_mutation(&self) -> bool {
        self.meta().mutates
    }

    /// The capability-ledger action (the scope the op's authority names).
    pub fn authz_action(&self) -> &'static str {
        self.meta().authority.scope()
    }

    pub fn name(&self) -> &'static str {
        self.meta().name
    }
}

impl ConfigOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::Initialize { .. } => meta("initialize", true, OpAuthority::FirstRun),
            Self::Transition { .. } => meta("transition", true, OpAuthority::Admin),
            Self::UpdatePolicy { .. } => meta("update_policy", true, OpAuthority::Admin),
            Self::Get => meta("get_config", false, OpAuthority::Broker),
            Self::Audit { .. } => meta("audit", false, OpAuthority::Read),
            Self::ExportSql => meta("export_sql", false, OpAuthority::Read),
            Self::ImportSql { .. } => meta("import_sql", true, OpAuthority::Admin),
        }
    }
}

impl UserOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::Create { .. } => meta("create_user", true, OpAuthority::Admin),
            Self::Update { .. } => meta("update_user", true, OpAuthority::Admin),
            Self::SetStatus { .. } => meta("set_user_status", true, OpAuthority::Admin),
            Self::Unlock { .. } => meta("unlock_user", true, OpAuthority::Admin),
            Self::Get { .. } => meta("get_user", false, OpAuthority::Read),
            Self::List { .. } => meta("list_users", false, OpAuthority::Read),
            Self::Resolve { .. } => meta("resolve_principal", false, OpAuthority::Broker),
        }
    }
}

impl CredentialOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::SetPassword { .. } => meta("set_password", true, OpAuthority::Admin),
            Self::ChangePassword { .. } => meta("change_password", true, OpAuthority::SelfService),
            Self::Authenticate { .. } => meta("authenticate", true, OpAuthority::Broker),
            Self::ExternalLogin { .. } => meta("external_login", true, OpAuthority::Broker),
            Self::BootstrapSession { .. } => meta("bootstrap_session", true, OpAuthority::Broker),
        }
    }
}

impl SessionOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::Resolve { .. } => meta("resolve_session", true, OpAuthority::Broker),
            Self::Revoke { .. } => meta("revoke_session", true, OpAuthority::Broker),
            Self::RevokeAll { .. } => meta("revoke_user_sessions", true, OpAuthority::Admin),
            Self::List { .. } => meta("list_sessions", false, OpAuthority::Read),
        }
    }
}

impl TokenOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::IssueOneTime { .. } => meta("issue_one_time_token", true, OpAuthority::Broker),
            Self::RedeemOneTime { .. } => meta("redeem_one_time_token", true, OpAuthority::Broker),
            Self::IssueApiKey { .. } => meta("issue_api_key", true, OpAuthority::Broker),
            Self::VerifyApiKey { .. } => meta("verify_api_key", true, OpAuthority::Broker),
            Self::RevokeApiKey { .. } => meta("revoke_api_key", true, OpAuthority::Admin),
        }
    }
}

impl MfaOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::EnrollTotp { .. } => meta("enroll_totp", true, OpAuthority::Broker),
            Self::ConfirmTotp { .. } => meta("confirm_totp", true, OpAuthority::Broker),
            Self::VerifyTotp { .. } => meta("verify_totp", true, OpAuthority::Broker),
            Self::SetRecoveryCodes { .. } => meta("set_recovery_codes", true, OpAuthority::Broker),
            Self::ConsumeRecoveryCode { .. } => {
                meta("consume_recovery_code", true, OpAuthority::Broker)
            }
        }
    }
}

impl AccessOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::UpsertRole { .. } => meta("upsert_role", true, OpAuthority::Admin),
            Self::RemoveRole { .. } => meta("remove_role", true, OpAuthority::Admin),
            Self::UpsertGroup { .. } => meta("upsert_group", true, OpAuthority::Admin),
            Self::RemoveGroup { .. } => meta("remove_group", true, OpAuthority::Admin),
            Self::ChangeMembership { .. } => {
                meta("change_group_membership", true, OpAuthority::Admin)
            }
            Self::ChangeUserRole { .. } => meta("change_user_role", true, OpAuthority::Admin),
            Self::ListRoles => meta("list_roles", false, OpAuthority::Read),
            Self::ListGroups => meta("list_groups", false, OpAuthority::Read),
        }
    }
}

impl IdpOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::Upsert { .. } => meta("upsert_idp", true, OpAuthority::Admin),
            Self::Remove { .. } => meta("remove_idp", true, OpAuthority::Admin),
            Self::Link { .. } => meta("link_identity", true, OpAuthority::Admin),
            Self::Unlink { .. } => meta("unlink_identity", true, OpAuthority::Admin),
            Self::List => meta("list_idps", false, OpAuthority::Read),
        }
    }
}
