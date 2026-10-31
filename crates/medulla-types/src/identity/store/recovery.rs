//! Recovery codes: a stand-in for a second factor the principal has and
//! cannot present, never a factor of their own.

use super::super::audit::IdentityEvent;
use super::super::stamp::IdentityStamp;
use super::super::views::{AuthenticateResult, IdentityReply};
use super::super::{IdentityRefusal, RECOVERY_CODES_PER_SET};
use super::sessions::PendingSession;
use super::throttle::account_key;
use super::{IdentityStore, RecoveryCode};

impl IdentityStore {
    /// A recovery code stands in for a second factor the principal HAS and
    /// cannot present; it is never a factor of its own. Without a confirmed
    /// factor there is nothing to recover, so codes can neither be issued nor
    /// spent -- otherwise a session that owes its first enrolment could issue
    /// itself codes and complete with one, and a group's required second
    /// factor would be met by the password alone.
    fn require_second_factor(&self, principal: &str) -> Result<(), IdentityRefusal> {
        self.mfa_enrolled(principal)
            .then_some(())
            .ok_or(IdentityRefusal::PreconditionFailed)
    }

    /// The principal of a COMPLETED session who holds a confirmed second
    /// factor: the only subject recovery codes are issued to.
    fn recoverable_subject(
        &self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<String, IdentityRefusal> {
        let principal = self.session_subject(stamp, now_ms, PendingSession::Refuse)?;
        self.require_second_factor(&principal)?;
        Ok(principal)
    }

    pub(super) fn set_recovery_codes(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        if stamp.token_hashes.len() != RECOVERY_CODES_PER_SET + 1 {
            return Err(IdentityRefusal::InvalidRequest);
        }
        let principal = self.recoverable_subject(stamp, now_ms)?;
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

    pub(super) fn consume_recovery(
        &mut self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        self.with_second_factor_gate(stamp, now_ms, |store, hash, principal| {
            store.require_second_factor(&principal)?;
            let code_hash = stamp.token_hash(1)?;
            let code = store.recovery.get_mut(&principal).and_then(|codes| {
                codes
                    .iter_mut()
                    .find(|code| code.code_hash == code_hash && code.used_at_ms.is_none())
            });
            let Some(code) = code else {
                return Ok(store.second_factor_failed(stamp, &principal, now_ms));
            };
            code.used_at_ms = Some(now_ms);
            store.complete_session(&hash, "recovery", now_ms);
            store.clear_throttle(&account_key(&principal));
            store.audit_event(stamp, now_ms, IdentityEvent::MfaVerified, Some(&principal));
            Ok(store.second_factor_ok(principal))
        })
    }
}
