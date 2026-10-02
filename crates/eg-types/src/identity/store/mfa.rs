//! Second factors: TOTP (RFC 6238, replay-guarded) and recovery codes.
//! The boundary unsealed the secret and matched the code; the store sees
//! only the matched time step.

use super::super::audit::IdentityEvent;
use super::super::model::TotpRecord;
use super::super::ops::MfaOp;
use super::super::stamp::IdentityStamp;
use super::super::views::{AuthenticateOutcome, AuthenticateResult, IdentityReply};
use super::super::IdentityRefusal;
use super::sessions::PendingSession;
use super::throttle::account_key;
use super::{ApplyContext, IdentityStore};

impl IdentityStore {
    pub(super) fn apply_mfa(
        &mut self,
        op: &MfaOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        match op {
            MfaOp::RegisterWebauthn { .. }
            | MfaOp::WebauthnCredentials { .. }
            | MfaOp::VerifyWebauthn { .. }
            | MfaOp::RemoveWebauthn { .. } => self.apply_webauthn(op, stamp, ctx),
            MfaOp::EnrollTotp { .. } => self.enroll_totp(stamp, ctx.now_ms),
            MfaOp::ConfirmTotp { .. } => self.confirm_totp(stamp, ctx.now_ms),
            MfaOp::VerifyTotp { .. } => Ok(IdentityReply::Authenticate(
                self.verify_totp(stamp, ctx.now_ms)?,
            )),
            MfaOp::SetRecoveryCodes { .. } => self.set_recovery_codes(stamp, ctx.now_ms),
            MfaOp::ConsumeRecoveryCode { .. } => Ok(IdentityReply::Authenticate(
                self.consume_recovery(stamp, ctx.now_ms)?,
            )),
        }
    }

    /// Store a sealed secret, unconfirmed. A confirmed factor is never
    /// silently replaced.
    fn enroll_totp(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let principal = self.session_subject(stamp, now_ms, PendingSession::AllowEnrollment)?;
        let sealed = stamp
            .sealed_secret
            .clone()
            .ok_or(IdentityRefusal::Unstamped)?;
        if self.totp_confirmed(&principal) {
            return Err(IdentityRefusal::Collision);
        }
        self.totp.insert(
            principal.clone(),
            TotpRecord {
                sealed_secret: sealed,
                confirmed_at_ms: None,
                last_step: 0,
            },
        );
        self.audit_event(stamp, now_ms, IdentityEvent::MfaEnrolled, Some(&principal));
        Ok(IdentityReply::Done { changed: true })
    }

    /// Accept `step` for `principal` once: later than every accepted step.
    fn accept_step(&mut self, principal: &str, step: u64) -> Result<(), IdentityRefusal> {
        let record = self
            .totp
            .get_mut(principal)
            .ok_or(IdentityRefusal::NotFound)?;
        if step <= record.last_step {
            return Err(IdentityRefusal::Replay);
        }
        record.last_step = step;
        Ok(())
    }

    fn confirm_totp(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let principal = self.session_subject(stamp, now_ms, PendingSession::AllowEnrollment)?;
        let step = stamp.totp_step.ok_or(IdentityRefusal::BadCredential)?;
        self.accept_step(&principal, step)?;
        if let Some(record) = self.totp.get_mut(&principal) {
            record.confirmed_at_ms.get_or_insert(now_ms);
        }
        self.audit_event(stamp, now_ms, IdentityEvent::MfaVerified, Some(&principal));
        Ok(IdentityReply::Done { changed: true })
    }

    /// The principal of a live session still owing its second factor.
    pub(super) fn pending_session_principal(
        &self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<(String, String), IdentityRefusal> {
        let hash = stamp.token_hash(0)?;
        let session = self
            .sessions
            .get(hash)
            .filter(|session| session.is_live(now_ms) && session.mfa_pending)
            .ok_or(IdentityRefusal::NotFound)?;
        Ok((hash.to_string(), session.principal_id.clone()))
    }

    /// The common entry of every second-factor verification (TOTP,
    /// WebAuthn): resolve the pending session's `(hash, principal)`, answer
    /// the already-built throttled outcome if the account backoff is
    /// running, or else hand both to `verify` for the factor's own check.
    pub(super) fn with_second_factor_gate(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
        verify: impl FnOnce(&mut Self, String, String) -> Result<AuthenticateResult, IdentityRefusal>,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        let (hash, principal) = self.pending_session_principal(stamp, now_ms)?;
        if let Some(throttled) = self.second_factor_throttled(&principal, now_ms) {
            return Ok(throttled);
        }
        verify(self, hash, principal)
    }

    /// Mark a pending session complete by `method`.
    pub(super) fn complete_session(&mut self, hash: &str, method: &str, now_ms: u64) {
        if let Some(session) = self.sessions.get_mut(hash) {
            session.mfa_pending = false;
            session.mfa_at_ms = Some(now_ms);
            session.auth_methods.push(method.to_string());
        }
    }

    /// A wrong second factor answers `bad` (not a refusal), so the failure
    /// count it adds is kept.
    pub(super) fn second_factor_failed(
        &mut self,
        stamp: &IdentityStamp,
        principal: &str,
        now_ms: u64,
    ) -> AuthenticateResult {
        self.record_failure(account_key(principal), now_ms);
        self.audit_event(stamp, now_ms, IdentityEvent::MfaFailed, Some(principal));
        AuthenticateResult::bad()
    }

    /// `Throttled` while `principal`'s account backoff runs.
    pub(super) fn second_factor_throttled(
        &self,
        principal: &str,
        now_ms: u64,
    ) -> Option<AuthenticateResult> {
        let until = self.throttled_until(&account_key(principal), now_ms)?;
        Some(AuthenticateResult {
            outcome: AuthenticateOutcome::Throttled,
            principal_id: None,
            retry_after_ms: Some(until - now_ms),
        })
    }

    pub(super) fn second_factor_ok(&self, principal: String) -> AuthenticateResult {
        AuthenticateResult {
            outcome: AuthenticateOutcome::Ok,
            principal_id: Some(principal),
            retry_after_ms: None,
        }
    }

    fn verify_totp(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        self.with_second_factor_gate(stamp, now_ms, |store, hash, principal| {
            let confirmed = store.totp_confirmed(&principal);
            let Some(step) = stamp.totp_step.filter(|_| confirmed) else {
                return Ok(store.second_factor_failed(stamp, &principal, now_ms));
            };
            store.accept_step(&principal, step)?;
            store.complete_session(&hash, "totp", now_ms);
            store.clear_throttle(&account_key(&principal));
            store.audit_event(stamp, now_ms, IdentityEvent::MfaVerified, Some(&principal));
            Ok(store.second_factor_ok(principal))
        })
    }
}
