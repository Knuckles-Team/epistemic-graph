//! The identity store: every record, and the one `apply` entry point that
//! authorizes an op against its stamped actor and runs its transition.
//!
//! `apply` mutates in place and may leave a partial change behind when it
//! refuses; the engine applies it to a CLONE and keeps the clone only on
//! success, so a refusal changes nothing (the same discipline as every RBAC
//! write). The store is serialized as part of the RBAC policy image, so an
//! identity change and its RBAC projection are one durable write.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::access::{GroupRecord, IdpConfig, RoleRecord};
use super::audit::{AuditRecord, AuditTrail, IdentityEvent};
use super::config::IdentityConfig;
use super::model::{
    ApiKeyRecord, ExternalIdentity, OneTimeToken, PasswordCredential, SessionRecord, TotpRecord,
};
use super::ops::{IdentityOp, OpAuthority, IDENTITY_ADMIN_SCOPE, IDENTITY_AUTHENTICATE_SCOPE};
use super::scope::ScopeClassifier;
use super::stamp::IdentityStamp;
use super::views::IdentityReply;
use super::IdentityRefusal;

mod access_ops;
mod auth;
mod external;
mod invariants;
mod mfa;
mod modes;
mod sessions;
mod throttle;
mod tokens;
mod users;

pub use throttle::ThrottleEntry;

/// Session and API-key "last used" stamps move at most once per minute, so a
/// hot read path does not rewrite the durable image on every call.
pub const TOUCH_GRANULARITY_MS: u64 = 60_000;

/// One stored recovery code: only its hash, and when it was spent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryCode {
    pub code_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_at_ms: Option<u64>,
}

/// Everything an apply needs besides the op and its stamp.
pub struct ApplyContext<'a> {
    pub now_ms: u64,
    pub classifier: &'a dyn ScopeClassifier,
}

/// The whole store. Every map is ordered, so the serialized image is
/// deterministic and a replica reaches byte-identical state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityStore {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    config: Option<IdentityConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    users: BTreeMap<String, super::model::UserRecord>,
    /// Normalized username → principal id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    usernames: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    passwords: BTreeMap<String, PasswordCredential>,
    /// Session-id hash → session.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    sessions: BTreeMap<String, SessionRecord>,
    /// Token hash → one-time token.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    one_time: BTreeMap<String, OneTimeToken>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    api_keys: BTreeMap<String, ApiKeyRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    totp: BTreeMap<String, TotpRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    recovery: BTreeMap<String, Vec<RecoveryCode>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    roles: BTreeMap<String, RoleRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    groups: BTreeMap<String, GroupRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    idps: BTreeMap<String, IdpConfig>,
    /// `<idp_id>\0<subject>` → link.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    links: BTreeMap<String, ExternalIdentity>,
    /// IdP-mapped roles per link key, recomputed at every external login.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    link_roles: BTreeMap<String, std::collections::BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    throttle: BTreeMap<String, ThrottleEntry>,
    #[serde(default, skip_serializing_if = "AuditTrail::is_empty")]
    audit: AuditTrail,
}

/// Whether an op can change who holds which scope, so the class invariants
/// must be re-checked over the whole store before it is kept.
fn reshapes_authority(op: &IdentityOp) -> bool {
    match op {
        IdentityOp::Config(_) | IdentityOp::User(_) | IdentityOp::Access(_) | IdentityOp::Idp(_) => {
            op.is_mutation()
        }
        IdentityOp::Credential(op) => {
            matches!(op, super::ops::CredentialOp::ExternalLogin { .. })
        }
        IdentityOp::Token(op) => matches!(
            op,
            super::ops::TokenOp::IssueApiKey { .. } | super::ops::TokenOp::RedeemOneTime { .. }
        ),
        IdentityOp::Session(_) | IdentityOp::Mfa(_) => false,
    }
}

/// A domain-separated SHA-256 hex digest.
pub(crate) fn digest_hex(domain: &[u8], body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(body.as_bytes());
    hex::encode(hasher.finalize())
}

/// The key of one IdP link.
pub(crate) fn link_key(idp_id: &str, subject: &str) -> String {
    format!("{idp_id}\0{subject}")
}

impl IdentityStore {
    /// Whether nothing was ever written (the store is then omitted from the
    /// RBAC image, so an engine that never uses it keeps its bytes).
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    pub fn config(&self) -> Option<&IdentityConfig> {
        self.config.as_ref()
    }

    /// Whether `principal_id` is a principal this store owns. The RBAC
    /// projection owns exactly these identities; `RegisterIdentity` for one
    /// of them is refused so the two paths never race.
    pub fn manages(&self, principal_id: &str) -> bool {
        self.users.contains_key(principal_id)
    }

    /// The principal a normalized username names and its stored password
    /// hash, for the boundary's verify. A known principal with no password
    /// still answers its id, so the verdict names it (and is `bad`).
    pub fn sign_in_target(&self, username: &str) -> (Option<&str>, Option<&str>) {
        let principal = self.usernames.get(username).map(String::as_str);
        let hash = principal
            .and_then(|principal| self.passwords.get(principal))
            .map(|credential| credential.hash.as_str());
        (principal, hash)
    }

    /// The stored password hash and history of one principal.
    pub fn credential_of(&self, principal_id: &str) -> Option<&PasswordCredential> {
        self.passwords.get(principal_id)
    }

    /// The sealed TOTP secret of the principal behind a session hash, or of
    /// one principal directly.
    pub fn sealed_totp_of(&self, principal_id: &str) -> Option<&str> {
        self.totp
            .get(principal_id)
            .map(|record| record.sealed_secret.as_str())
    }

    /// The principal a live session hash belongs to.
    pub fn session_principal(&self, session_hash: &str, now_ms: u64) -> Option<&str> {
        self.sessions
            .get(session_hash)
            .filter(|session| session.is_live(now_ms))
            .map(|session| session.principal_id.as_str())
    }

    /// The kind of a principal this store owns.
    pub fn kind_of(&self, principal_id: &str) -> Option<super::model::UserKind> {
        self.users.get(principal_id).map(|user| user.kind)
    }

    /// The identity audit trail (read-only).
    pub fn audit_trail(&self) -> &AuditTrail {
        &self.audit
    }

    /// Record an event that happened outside an identity op (RBAC writes,
    /// sampled denials) in the same trail.
    pub fn record(&mut self, record: AuditRecord) {
        self.audit.append(record);
    }

    /// Authorize `op` against its stamped actor and apply it.
    pub fn apply(
        &mut self,
        op: &IdentityOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        self.authorize(op, stamp)?;
        let reply = self.dispatch(op, stamp, ctx)?;
        if reshapes_authority(op) {
            self.validate(ctx.classifier)?;
        }
        Ok(reply)
    }

    fn authorize(&self, op: &IdentityOp, stamp: &IdentityStamp) -> Result<(), IdentityRefusal> {
        let actor = &stamp.actor;
        let allowed = match op.meta().authority {
            OpAuthority::Admin => actor.holds(IDENTITY_ADMIN_SCOPE) && !actor.delegated,
            OpAuthority::Read => {
                actor.holds(super::ops::IDENTITY_READ_SCOPE) || actor.holds(IDENTITY_ADMIN_SCOPE)
            }
            OpAuthority::Broker => actor.holds(IDENTITY_AUTHENTICATE_SCOPE),
            OpAuthority::SelfService => {
                actor.holds(super::ops::IDENTITY_SELF_SCOPE) && self.is_active(&actor.principal_id)
            }
            OpAuthority::FirstRun => {
                (actor.holds(IDENTITY_AUTHENTICATE_SCOPE) || actor.holds(IDENTITY_ADMIN_SCOPE))
                    && !actor.delegated
            }
        };
        if allowed {
            Ok(())
        } else {
            Err(IdentityRefusal::NotAuthorized)
        }
    }

    fn dispatch(
        &mut self,
        op: &IdentityOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        if !matches!(op, IdentityOp::Config(super::ops::ConfigOp::Initialize { .. })) {
            self.require_initialized()?;
        }
        match op {
            IdentityOp::Config(op) => self.apply_config(op, stamp, ctx),
            IdentityOp::User(op) => self.apply_user(op, stamp, ctx),
            IdentityOp::Credential(op) => self.apply_credential(op, stamp, ctx),
            IdentityOp::Session(op) => self.apply_session(op, stamp, ctx),
            IdentityOp::Token(op) => self.apply_token(op, stamp, ctx),
            IdentityOp::Mfa(op) => self.apply_mfa(op, stamp, ctx),
            IdentityOp::Access(op) => self.apply_access(op, stamp, ctx),
            IdentityOp::Idp(op) => self.apply_idp(op, stamp, ctx),
        }
    }

    fn require_initialized(&self) -> Result<&IdentityConfig, IdentityRefusal> {
        self.config.as_ref().ok_or(IdentityRefusal::NotInitialized)
    }

    pub(crate) fn is_active(&self, principal_id: &str) -> bool {
        self.users
            .get(principal_id)
            .is_some_and(|user| user.status.is_active())
    }

    /// Append one audit entry for `actor` at `now_ms`.
    pub(crate) fn audit_event(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
        event: IdentityEvent,
        target: Option<&str>,
    ) {
        self.audit.append(AuditRecord {
            at_ms: now_ms,
            actor: stamp.actor.principal_id.clone(),
            event,
            target: target.map(str::to_string),
            ip_prefix: None,
            detail: String::new(),
        });
    }
}
