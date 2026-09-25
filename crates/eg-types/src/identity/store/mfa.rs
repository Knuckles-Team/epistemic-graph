//! Second factors: TOTP (RFC 6238, replay-guarded) and recovery codes.
//! The boundary unsealed the secret and matched the code; the store sees
//! only the matched time step.

use super::super::audit::IdentityEvent;
use super::super::model::TotpRecord;
use super::super::ops::MfaOp;
use super::super::stamp::IdentityStamp;
use super::super::views::{AuthenticateOutcome, AuthenticateResult, IdentityReply, MfaStatusView};
use super::super::{IdentityRefusal, RECOVERY_CODES_PER_SET};
use super::sessions::PendingSession;
use super::throttle::account_key;
use super::{ApplyContext, IdentityStore, RecoveryCode};

impl IdentityStore {
    pub(super) fn apply_mfa(
        &mut self,
        op: &MfaOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        match op {
            MfaOp::Status => Ok(IdentityReply::MfaStatus(
                self.mfa_status(&stamp.actor.principal_id),
            )),
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

    fn mfa_status(&self, principal_id: &str) -> MfaStatusView {
        MfaStatusView {
            totp_enrolled: self.totp_confirmed(principal_id),
            webauthn_credentials: self
                .webauthn
                .values()
                .filter(|credential| credential.principal_id == principal_id)
                .count(),
            recovery_codes_left: self.recovery.get(principal_id).map_or(0, |codes| {
                codes
                    .iter()
                    .filter(|code| code.used_at_ms.is_none())
                    .count()
            }),
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
        let (hash, principal) = self.pending_session_principal(stamp, now_ms)?;
        if let Some(throttled) = self.second_factor_throttled(&principal, now_ms) {
            return Ok(throttled);
        }
        let confirmed = self.totp_confirmed(&principal);
        let Some(step) = stamp.totp_step.filter(|_| confirmed) else {
            return Ok(self.second_factor_failed(stamp, &principal, now_ms));
        };
        self.accept_step(&principal, step)?;
        self.complete_session(&hash, "totp", now_ms);
        self.clear_throttle(&account_key(&principal));
        self.audit_event(stamp, now_ms, IdentityEvent::MfaVerified, Some(&principal));
        Ok(self.second_factor_ok(principal))
    }

    fn set_recovery_codes(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        if stamp.token_hashes.len() != RECOVERY_CODES_PER_SET + 1 {
            return Err(IdentityRefusal::InvalidRequest);
        }
        let principal = self.session_subject(stamp, now_ms, PendingSession::AllowEnrollment)?;
        let codes = stamp
            .token_hashes
            .iter()
            .skip(1)
            .map(|hash| RecoveryCode {
                code_hash: hash.clone(),
                used_at_ms: None,
            })
            .collect();
        self.recovery.insert(principal.clone(), codes);
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::RecoveryCodesSet,
            Some(&principal),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    fn consume_recovery(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        let (hash, principal) = self.pending_session_principal(stamp, now_ms)?;
        let code_hash = stamp.token_hash(1)?;
        let code = self.recovery.get_mut(&principal).and_then(|codes| {
            codes
                .iter_mut()
                .find(|code| code.code_hash == code_hash && code.used_at_ms.is_none())
        });
        let Some(code) = code else {
            return Ok(self.second_factor_failed(stamp, &principal, now_ms));
        };
        code.used_at_ms = Some(now_ms);
        self.complete_session(&hash, "recovery", now_ms);
        self.audit_event(stamp, now_ms, IdentityEvent::MfaVerified, Some(&principal));
        Ok(self.second_factor_ok(principal))
    }
}
