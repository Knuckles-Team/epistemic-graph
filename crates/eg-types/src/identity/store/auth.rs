//! Passwords and local sign-in.
//!
//! The boundary verified the candidate (argon2id, a dummy hash for an
//! unknown user) and stamped only the verdict. Here the verdict meets the
//! throttle, the account state, the mode and the MFA policy -- serialized
//! under the engine's write lock, so concurrent guesses cannot race past a
//! lockout: a correct guess applied after the lock engaged is refused.

use super::super::audit::{AuditRecord, IdentityEvent};
use super::super::config::{AuthMode, LocalFallback};
use super::super::model::{PasswordCredential, UserKind, UserStatus};
use super::super::ops::CredentialOp;
use super::super::requests::{AuthenticateRequest, PasswordSet};
use super::super::stamp::IdentityStamp;
use super::super::views::{AuthenticateOutcome, AuthenticateResult, IdentityReply};
use super::super::{
    normalize_username, IdentityRefusal, ADMINISTRATORS_GROUP, BOOTSTRAP_PRINCIPAL,
    PASSWORD_HISTORY_DEPTH,
};
use super::sessions::SessionOpen;
use super::throttle::{account_key, network_key};
use super::{ApplyContext, IdentityStore};

impl IdentityStore {
    pub(super) fn apply_credential(
        &mut self,
        op: &CredentialOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        match op {
            CredentialOp::SetPassword { request } => {
                self.admin_set_password(request, stamp, ctx.now_ms)
            }
            CredentialOp::ChangePassword { .. } => self.change_own_password(stamp, ctx.now_ms),
            CredentialOp::Authenticate { request } => Ok(IdentityReply::Authenticate(
                self.authenticate(request, stamp, ctx)?,
            )),
            CredentialOp::ExternalLogin { request } => Ok(IdentityReply::Authenticate(
                self.external_login(request, stamp, ctx)?,
            )),
            CredentialOp::BootstrapSession { .. } => self.bootstrap_session(stamp, ctx),
        }
    }

    /// Replace a principal's password hash, keeping the previous ones for the
    /// reuse check.
    pub(crate) fn store_password(
        &mut self,
        principal_id: &str,
        hash: &str,
        must_change: bool,
        now_ms: u64,
    ) {
        let mut history = self
            .passwords
            .get(principal_id)
            .map(|credential| {
                let mut history = vec![credential.hash.clone()];
                history.extend(credential.history.iter().cloned());
                history
            })
            .unwrap_or_default();
        history.truncate(PASSWORD_HISTORY_DEPTH);
        self.passwords.insert(
            principal_id.to_string(),
            PasswordCredential {
                hash: hash.to_string(),
                changed_at_ms: now_ms,
                must_change,
                history,
            },
        );
        if let Some(user) = self.users.get_mut(principal_id) {
            if user.status == UserStatus::PendingReset && !must_change {
                user.status = UserStatus::Active;
            }
        }
    }

    fn admin_set_password(
        &mut self,
        request: &PasswordSet,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let user = self
            .users
            .get(&request.principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        if user.kind != UserKind::Human {
            return Err(IdentityRefusal::KindMismatch);
        }
        let hash = stamp.new_password_hash()?.to_string();
        self.store_password(&request.principal_id, &hash, request.must_change, now_ms);
        self.revoke_principal_sessions(&request.principal_id, now_ms, "password_reset");
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::PasswordSet,
            Some(&request.principal_id),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    fn change_own_password(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let principal = stamp.actor.principal_id.clone();
        let check = stamp.check()?;
        if check.principal_id.as_deref() != Some(principal.as_str()) || !check.matched {
            return Err(IdentityRefusal::BadCredential);
        }
        let hash = stamp.new_password_hash()?.to_string();
        self.store_password(&principal, &hash, false, now_ms);
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::PasswordChanged,
            Some(&principal),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    /// Whether the mode lets a local password sign in `principal_id`.
    pub(super) fn local_sign_in_allowed(&self, principal_id: &str) -> bool {
        let Some(config) = self.config.as_ref() else {
            return false;
        };
        let is_admin = self.is_administrator(principal_id);
        match (config.mode, config.local_fallback) {
            (AuthMode::None, _) => false,
            (AuthMode::Local, _) | (AuthMode::External, LocalFallback::Full) => true,
            (AuthMode::External, LocalFallback::BreakGlass) => is_admin,
            (AuthMode::External, LocalFallback::Off) => false,
        }
    }

    fn is_administrator(&self, principal_id: &str) -> bool {
        self.groups
            .get(ADMINISTRATORS_GROUP)
            .is_some_and(|group| group.members.contains_key(principal_id))
    }

    fn login_audit(
        &mut self,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
        event: IdentityEvent,
        target: Option<&str>,
        ip: Option<&str>,
    ) {
        self.audit.append(AuditRecord {
            at_ms: ctx.now_ms,
            actor: stamp.actor.principal_id.clone(),
            event,
            target: target.map(str::to_string),
            ip_prefix: ip.map(str::to_string),
            detail: String::new(),
        });
    }

    /// The throttle verdict for a sign-in attempt, before the password.
    fn throttle_verdict(
        &self,
        principal: Option<&str>,
        ip: Option<&str>,
        now_ms: u64,
    ) -> Option<u64> {
        let account = principal.and_then(|p| self.throttled_until(&account_key(p), now_ms));
        let network_exempt = principal.is_some_and(|p| self.is_administrator(p));
        let network = ip
            .filter(|_| !network_exempt)
            .and_then(|ip| self.throttled_until(&network_key(ip), now_ms));
        account.max(network)
    }

    fn count_failure(&mut self, principal: Option<&str>, ip: Option<&str>, now_ms: u64) {
        if let Some(principal) = principal {
            self.record_failure(account_key(principal), now_ms);
        }
        if let Some(ip) = ip {
            self.record_failure(network_key(ip), now_ms);
        }
    }

    fn authenticate(
        &mut self,
        request: &AuthenticateRequest,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        let username = normalize_username(&request.username).ok();
        let principal = username
            .as_deref()
            .and_then(|name| self.usernames.get(name))
            .cloned();
        let check = stamp.check()?;
        if check.principal_id != principal {
            return Err(IdentityRefusal::Unstamped);
        }
        let ip = request.ip_prefix.as_deref();
        if let Some(until) = self.throttle_verdict(principal.as_deref(), ip, ctx.now_ms) {
            self.login_audit(
                stamp,
                ctx,
                IdentityEvent::LoginThrottled,
                principal.as_deref(),
                ip,
            );
            return Ok(AuthenticateResult {
                outcome: AuthenticateOutcome::Throttled,
                principal_id: None,
                retry_after_ms: Some(until - ctx.now_ms),
            });
        }
        let eligible = principal.as_deref().filter(|p| {
            self.local_sign_in_allowed(p)
                && self.users.get(*p).is_some_and(|user| {
                    matches!(user.status, UserStatus::Active | UserStatus::PendingReset)
                })
        });
        let Some(principal) = eligible.filter(|_| check.matched).map(str::to_string) else {
            self.count_failure(principal.as_deref(), ip, ctx.now_ms);
            self.login_audit(stamp, ctx, IdentityEvent::LoginFailed, None, ip);
            return Ok(AuthenticateResult::bad());
        };
        self.clear_throttle(&account_key(&principal));
        self.complete_password_sign_in(&principal, request, stamp, ctx)
    }

    /// The password verified: apply a rehash or a forced change, then open
    /// the session the MFA policy allows.
    fn complete_password_sign_in(
        &mut self,
        principal: &str,
        request: &AuthenticateRequest,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        let check = stamp.check()?;
        if let Some(rehash) = &check.rehash {
            if let Some(credential) = self.passwords.get_mut(principal) {
                credential.hash = rehash.clone();
            }
        }
        let must_change = self.passwords.get(principal).is_some_and(|c| c.must_change)
            || self
                .users
                .get(principal)
                .is_some_and(|u| u.status == UserStatus::PendingReset);
        if must_change {
            let Some(hash) = stamp.password_hash.clone() else {
                return Ok(self.outcome(principal, AuthenticateOutcome::CredentialChangeRequired));
            };
            self.store_password(principal, &hash, false, ctx.now_ms);
        }
        if let Some(user) = self.users.get_mut(principal) {
            user.last_login_at_ms = Some(ctx.now_ms);
        }
        let ip = request.ip_prefix.clone();
        self.login_audit(
            stamp,
            ctx,
            IdentityEvent::LoginSucceeded,
            Some(principal),
            ip.as_deref(),
        );
        self.open_with_mfa_policy(principal, "pwd", ip, stamp, ctx)
    }

    fn outcome(&self, principal: &str, outcome: AuthenticateOutcome) -> AuthenticateResult {
        AuthenticateResult {
            outcome,
            principal_id: Some(principal.to_string()),
            retry_after_ms: None,
        }
    }

    /// Open the session a first factor earns: complete, pending a second
    /// factor (one is enrolled), or pending enrolment (a group requires one
    /// and none is enrolled -- the session then authorizes only enrolment).
    pub(crate) fn open_with_mfa_policy(
        &mut self,
        principal: &str,
        method: &str,
        ip_prefix: Option<String>,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        let resolution = self.resolve(principal, ctx.classifier)?;
        let outcome = match (resolution.mfa_enrolled, resolution.mfa_required) {
            (true, _) => AuthenticateOutcome::MfaRequired,
            (false, true) => AuthenticateOutcome::MfaEnrollmentRequired,
            (false, false) => AuthenticateOutcome::Ok,
        };
        self.open_session(
            SessionOpen {
                session_hash: stamp.token_hash(0)?,
                principal_id: principal,
                method: method.to_string(),
                mfa_pending: outcome != AuthenticateOutcome::Ok,
                ip_prefix,
            },
            ctx,
        )?;
        Ok(self.outcome(principal, outcome))
    }

    /// `none` mode: the authenticator answers the bootstrap principal.
    fn bootstrap_session(
        &mut self,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let mode = self.require_initialized()?.mode;
        if mode != AuthMode::None || !self.is_active(BOOTSTRAP_PRINCIPAL) {
            return Err(IdentityRefusal::PreconditionFailed);
        }
        self.open_session(
            SessionOpen {
                session_hash: stamp.token_hash(0)?,
                principal_id: BOOTSTRAP_PRINCIPAL,
                method: "none".to_string(),
                mfa_pending: false,
                ip_prefix: None,
            },
            ctx,
        )?;
        Ok(IdentityReply::Authenticate(
            self.outcome(BOOTSTRAP_PRINCIPAL, AuthenticateOutcome::Ok),
        ))
    }
}
