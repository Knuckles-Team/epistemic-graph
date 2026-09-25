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
use super::config::{IssuerRotation, ModeTransition};
use super::requests::{
    AdminResetIssue, ApiKeyIssue, ApiKeyUse, AuthenticateRequest, CreateUserRequest, ExternalLogin,
    InitializeRequest, LinkRequest, OneTimeTokenIssue, PasswordChange, PasswordResetIssue,
    PasswordSet, RecoveryCodesSet, SessionTouch, TokenRedeem, TotpEnroll, UserStatusChange,
    UserUpdate, WebauthnCredential, WebauthnUse,
};
use super::requests_admin::{
    GroupMembershipChange, GroupUpsert, ListQuery, ObjectRef, PolicyUpdate, PrincipalListQuery,
    RoleUpsert, ScimClientBinding, SqlDump, UserRoleChange, UserSearch,
};
use super::requests_provision::{
    DirectoryGroup, DirectoryGroupQuery, DirectoryGroupRef, ProvisionSubject, ProvisionedQuery,
};

/// Administer identity: users, roles, groups, IdPs, policy, mode.
pub const IDENTITY_ADMIN_SCOPE: &str = "identity:admin";
/// Read the redacted identity directory and audit trail.
pub const IDENTITY_READ_SCOPE: &str = "identity:read";
/// The identity broker (graph-os): sign-ins, sessions, token redemption.
pub const IDENTITY_AUTHENTICATE_SCOPE: &str = "identity:authenticate";
/// A principal managing its own credentials and second factors.
pub const IDENTITY_SELF_SCOPE: &str = "identity:self";
/// Directory provisioning (SCIM), bound to one `kind=scim` IdP.
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
    /// `identity:read` / `identity:admin`, or the identity broker.
    ReadOrBroker,
    /// `identity:authenticate` (the broker).
    Broker,
    /// `identity:self`, acting on the actor's own principal.
    SelfService,
    /// `identity:authenticate` or `identity:admin`, and only while the store
    /// is uninitialized.
    FirstRun,
    /// The IdP directory: `identity:read` / `identity:admin`, or the broker
    /// (the sign-in page lists the enabled IdPs; an IdP record holds no
    /// secret, only `secret_ref`).
    Directory,
    /// `identity:provision` (SCIM) or `identity:authenticate` (the broker is
    /// the LDAP client), direct actor. The op is further bound to one IdP:
    /// a provisioner only to the `kind=scim` IdP whose
    /// `config_json.provisioner` names it, the broker only to `kind=ldap`.
    Provision,
    /// `identity:self` acting on its own principal, or `identity:admin`
    /// (direct) acting on anyone's.
    SelfOrAdmin,
}

impl OpAuthority {
    /// The scope the ledger names for this authority (the first of
    /// [`OpAuthority::scopes`]).
    pub fn scope(self) -> &'static str {
        self.scopes()[0]
    }

    /// Every exact scope that satisfies this authority. The store's check
    /// and the request boundary's ledger check both read this one list.
    pub fn scopes(self) -> &'static [&'static str] {
        match self {
            Self::Admin => &[IDENTITY_ADMIN_SCOPE],
            Self::Read => &[IDENTITY_READ_SCOPE, IDENTITY_ADMIN_SCOPE],
            Self::ReadOrBroker => &[
                IDENTITY_READ_SCOPE,
                IDENTITY_ADMIN_SCOPE,
                IDENTITY_AUTHENTICATE_SCOPE,
            ],
            Self::Broker => &[IDENTITY_AUTHENTICATE_SCOPE],
            Self::FirstRun => &[IDENTITY_AUTHENTICATE_SCOPE, IDENTITY_ADMIN_SCOPE],
            Self::SelfService => &[IDENTITY_SELF_SCOPE],
            Self::Directory => &[
                IDENTITY_READ_SCOPE,
                IDENTITY_ADMIN_SCOPE,
                IDENTITY_AUTHENTICATE_SCOPE,
            ],
            Self::Provision => &[IDENTITY_PROVISION_SCOPE, IDENTITY_AUTHENTICATE_SCOPE],
            Self::SelfOrAdmin => &[IDENTITY_SELF_SCOPE, IDENTITY_ADMIN_SCOPE],
        }
    }

    /// Whether a delegated actor is refused outright.
    pub fn direct_only(self) -> bool {
        matches!(self, Self::Admin | Self::FirstRun | Self::Provision)
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
    Initialize {
        request: InitializeRequest,
    },
    Transition {
        request: ModeTransition,
    },
    RotateIssuer {
        request: IssuerRotation,
    },
    UpdatePolicy {
        request: PolicyUpdate,
    },
    Get,
    Audit {
        request: ListQuery,
    },
    /// A paged, chain-bearing audit export; never a whole-store SQL dump.
    ExportAudit {
        request: ListQuery,
    },
    VerifyAudit,
    /// The redacted Postgres dump of the store (backup / migration).
    ExportSql,
    /// Merge a dump produced by `export_sql` (restore / migration).
    ImportSql {
        request: SqlDump,
    },
    /// Register or repair the engine's System identity AFTER the one-time
    /// System bootstrap (recovery: the System identity lost or replaced).
    /// Exact direct `identity:admin`; the engine sets the named agent's role
    /// to System and keeps its teams and roles -- nothing else is replaced.
    RepairSystemIdentity {
        request: ObjectRef,
    },
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
    Search {
        request: UserSearch,
    },
    ListServiceAccounts {
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
    /// Revoke one session by the redacted handle returned from `list`.
    RevokeOne {
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
    IssueOneTime {
        request: OneTimeTokenIssue,
    },
    IssueAdminReset {
        request: AdminResetIssue,
    },
    RedeemOneTime {
        request: TokenRedeem,
    },
    IssueApiKey {
        request: ApiKeyIssue,
    },
    VerifyApiKey {
        request: ApiKeyUse,
    },
    RevokeApiKey {
        request: ObjectRef,
    },
    ListApiKeys {
        request: PrincipalListQuery,
    },
    /// A signed-out user's reset link (uniform answer, throttled).
    IssuePasswordReset {
        request: PasswordResetIssue,
    },
}

/// Second factors: TOTP, recovery codes and WebAuthn (IDM-09).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MfaOp {
    EnrollTotp {
        request: TotpEnroll,
    },
    ConfirmTotp {
        request: SessionTouch,
    },
    VerifyTotp {
        request: SessionTouch,
    },
    SetRecoveryCodes {
        request: RecoveryCodesSet,
    },
    ConsumeRecoveryCode {
        request: SessionTouch,
    },
    RegisterWebauthn {
        request: WebauthnCredential,
    },
    /// The credentials of the session's principal (a pending session too:
    /// the broker needs them to verify the assertion that completes it).
    WebauthnCredentials {
        request: SessionTouch,
    },
    VerifyWebauthn {
        request: WebauthnUse,
    },
    /// Remove one credential by id: its owner, or an administrator.
    RemoveWebauthn {
        request: ObjectRef,
    },
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
    Upsert {
        request: IdpConfig,
    },
    Remove {
        request: ObjectRef,
    },
    Link {
        request: LinkRequest,
    },
    Unlink {
        request: LinkRequest,
    },
    List,
    UpsertScimClient {
        request: ScimClientBinding,
    },
    GetScimClient {
        request: ObjectRef,
    },
    ListScimClients,
    RemoveScimClient {
        request: ObjectRef,
    },
    /// Directory provisioning (SCIM `Users`, LDAP sync).
    Provision {
        request: ProvisionSubject,
    },
    ListProvisioned {
        request: ProvisionedQuery,
    },
    /// Directory groups (SCIM `Groups`, LDAP groups).
    ProvisionGroup {
        request: DirectoryGroup,
    },
    RemoveDirectoryGroup {
        request: DirectoryGroupRef,
    },
    ListDirectoryGroups {
        request: DirectoryGroupQuery,
    },
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
            Self::RotateIssuer { .. } => meta("rotate_issuer", true, OpAuthority::Admin),
            Self::UpdatePolicy { .. } => meta("update_policy", true, OpAuthority::Admin),
            Self::Get => meta("get_config", false, OpAuthority::ReadOrBroker),
            Self::Audit { .. } => meta("audit", false, OpAuthority::Read),
            Self::ExportAudit { .. } => meta("export_audit", false, OpAuthority::Read),
            Self::VerifyAudit => meta("verify_audit", false, OpAuthority::Read),
            Self::ExportSql => meta("export_sql", false, OpAuthority::Read),
            Self::ImportSql { .. } => meta("import_sql", true, OpAuthority::Admin),
            Self::RepairSystemIdentity { .. } => {
                meta("repair_system_identity", true, OpAuthority::Admin)
            }
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
            Self::Search { .. } => meta("search_users", false, OpAuthority::Read),
            Self::ListServiceAccounts { .. } => {
                meta("list_service_accounts", false, OpAuthority::Read)
            }
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
            Self::RevokeOne { .. } => meta("revoke_one_session", true, OpAuthority::Admin),
            Self::List { .. } => meta("list_sessions", false, OpAuthority::Read),
        }
    }
}

impl TokenOp {
    fn meta(&self) -> OpMeta {
        match self {
            Self::IssueOneTime { .. } => meta("issue_one_time_token", true, OpAuthority::Broker),
            Self::IssueAdminReset { .. } => meta("issue_admin_reset", true, OpAuthority::Admin),
            Self::RedeemOneTime { .. } => meta("redeem_one_time_token", true, OpAuthority::Broker),
            Self::IssueApiKey { .. } => meta("issue_api_key", true, OpAuthority::Broker),
            Self::VerifyApiKey { .. } => meta("verify_api_key", true, OpAuthority::Broker),
            Self::RevokeApiKey { .. } => meta("revoke_api_key", true, OpAuthority::Admin),
            Self::ListApiKeys { .. } => meta("list_api_keys", false, OpAuthority::Read),
            Self::IssuePasswordReset { .. } => {
                meta("issue_password_reset", true, OpAuthority::Broker)
            }
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
            Self::RegisterWebauthn { .. } => meta("register_webauthn", true, OpAuthority::Broker),
            Self::WebauthnCredentials { .. } => {
                meta("webauthn_credentials", false, OpAuthority::Broker)
            }
            Self::VerifyWebauthn { .. } => meta("verify_webauthn", true, OpAuthority::Broker),
            Self::RemoveWebauthn { .. } => meta("remove_webauthn", true, OpAuthority::SelfOrAdmin),
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
            Self::List => meta("list_idps", false, OpAuthority::Directory),
            Self::UpsertScimClient { .. } => meta("upsert_scim_client", true, OpAuthority::Admin),
            Self::GetScimClient { .. } => meta("get_scim_client", false, OpAuthority::Read),
            Self::ListScimClients => meta("list_scim_clients", false, OpAuthority::Read),
            Self::RemoveScimClient { .. } => meta("remove_scim_client", true, OpAuthority::Admin),
            Self::Provision { .. } => meta("provision", true, OpAuthority::Provision),
            Self::ListProvisioned { .. } => meta("list_provisioned", false, OpAuthority::Provision),
            Self::ProvisionGroup { .. } => meta("provision_group", true, OpAuthority::Provision),
            Self::RemoveDirectoryGroup { .. } => {
                meta("remove_directory_group", true, OpAuthority::Provision)
            }
            Self::ListDirectoryGroups { .. } => {
                meta("list_directory_groups", false, OpAuthority::Provision)
            }
        }
    }
}
