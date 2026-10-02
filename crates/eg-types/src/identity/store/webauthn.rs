//! WebAuthn second factors (IDM-09). graph-os verifies every attestation and
//! assertion signature with a vetted library; the store keeps only the
//! PUBLIC credential, binds it to the principal a live session proves, and
//! enforces the signature counter (a counter that does not move forward is
//! a cloned authenticator and is refused).

use super::super::audit::IdentityEvent;
use super::super::model::WebauthnRecord;
use super::super::ops::{MfaOp, IDENTITY_ADMIN_SCOPE};
use super::super::requests::{WebauthnCredential, WebauthnUse};
use super::super::requests_admin::ObjectRef;
use super::super::stamp::IdentityStamp;
use super::super::text::{bounded, bounded_opt, identifier, MAX_NAME_BYTES};
use super::super::views::{AuthenticateResult, IdentityReply, WebauthnCredentialView};
use super::super::{IdentityRefusal, MAX_WEBAUTHN_PER_USER};
use super::sessions::PendingSession;
use super::throttle::account_key;
use super::{ApplyContext, IdentityStore};

/// Longest credential id and COSE key (base64url characters).
const MAX_CREDENTIAL_ID_CHARS: usize = 1_024;
const MAX_PUBLIC_KEY_CHARS: usize = 4_096;
/// Most transports one credential lists.
const MAX_TRANSPORTS: usize = 8;

/// A non-empty base64url string of at most `max` characters.
fn base64url(value: &str, max: usize) -> Result<(), IdentityRefusal> {
    let allowed = |c: char| c.is_ascii_alphanumeric() || "-_=".contains(c);
    if value.is_empty() || value.len() > max || !value.chars().all(allowed) {
        return Err(IdentityRefusal::InvalidRequest);
    }
    Ok(())
}

fn checked_credential(request: &WebauthnCredential) -> Result<(), IdentityRefusal> {
    base64url(&request.credential_id, MAX_CREDENTIAL_ID_CHARS)?;
    base64url(&request.public_key_cose, MAX_PUBLIC_KEY_CHARS)?;
    bounded(&request.name, MAX_NAME_BYTES)?;
    bounded_opt(request.aaguid.as_deref(), MAX_NAME_BYTES)?;
    if request.transports.len() > MAX_TRANSPORTS {
        return Err(IdentityRefusal::InvalidRequest);
    }
    request
        .transports
        .iter()
        .try_for_each(|transport| identifier(transport))
}

/// The counter rule: strictly forward, or an authenticator that keeps no
/// counter at all (both zero).
fn counter_moves_forward(stored: u32, presented: u32) -> bool {
    presented > stored || (presented == 0 && stored == 0)
}

impl IdentityStore {
    /// The WebAuthn ops of the MFA family (routed here by `apply_mfa`).
    pub(super) fn apply_webauthn(
        &mut self,
        op: &MfaOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let now_ms = ctx.now_ms;
        match op {
            MfaOp::RegisterWebauthn { request } => self.register_webauthn(request, stamp, now_ms),
            MfaOp::WebauthnCredentials { .. } => self.webauthn_credentials(stamp, now_ms),
            MfaOp::VerifyWebauthn { request } => Ok(IdentityReply::Authenticate(
                self.verify_webauthn(request, stamp, now_ms)?,
            )),
            MfaOp::RemoveWebauthn { request } => self.remove_webauthn(request, stamp, now_ms),
            MfaOp::EnrollTotp { .. }
            | MfaOp::ConfirmTotp { .. }
            | MfaOp::VerifyTotp { .. }
            | MfaOp::SetRecoveryCodes { .. }
            | MfaOp::ConsumeRecoveryCode { .. } => Err(IdentityRefusal::InvalidRequest),
        }
    }

    /// Whether `principal_id` holds any WebAuthn credential.
    pub(crate) fn has_webauthn(&self, principal_id: &str) -> bool {
        self.webauthn
            .values()
            .any(|record| record.principal_id == principal_id)
    }

    fn register_webauthn(
        &mut self,
        request: &WebauthnCredential,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        checked_credential(request)?;
        let principal = self.session_subject(stamp, now_ms, PendingSession::AllowEnrollment)?;
        if self.webauthn.contains_key(&request.credential_id) {
            return Err(IdentityRefusal::Collision);
        }
        let held = self
            .webauthn
            .values()
            .filter(|record| record.principal_id == principal)
            .count();
        if held >= MAX_WEBAUTHN_PER_USER {
            return Err(IdentityRefusal::Full);
        }
        self.webauthn.insert(
            request.credential_id.clone(),
            WebauthnRecord {
                credential_id: request.credential_id.clone(),
                principal_id: principal.clone(),
                public_key_cose: request.public_key_cose.clone(),
                sign_count: request.sign_count,
                aaguid: request.aaguid.clone(),
                transports: request.transports.clone(),
                name: request.name.clone(),
                created_at_ms: now_ms,
                last_used_at_ms: None,
            },
        );
        self.audit_event(stamp, now_ms, IdentityEvent::MfaEnrolled, Some(&principal));
        Ok(IdentityReply::Done { changed: true })
    }

    /// The credentials of a live session's active principal. A session
    /// still owing its second factor qualifies: this is what the broker
    /// verifies the completing assertion against.
    fn webauthn_credentials(
        &self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let principal = self
            .sessions
            .get(stamp.token_hash(0)?)
            .filter(|session| session.is_live(now_ms))
            .map(|session| session.principal_id.as_str())
            .filter(|principal| self.is_active(principal))
            .ok_or(IdentityRefusal::NotAuthorized)?;
        let views = self
            .webauthn
            .values()
            .filter(|record| record.principal_id == principal)
            .map(|record| WebauthnCredentialView {
                credential_id: record.credential_id.clone(),
                public_key_cose: record.public_key_cose.clone(),
                sign_count: record.sign_count,
                transports: record.transports.clone(),
                name: record.name.clone(),
            })
            .collect();
        Ok(IdentityReply::WebauthnCredentials(views))
    }

    /// Complete a pending session with a verified assertion. An unknown or
    /// foreign credential answers `bad` (counted); a counter that does not
    /// move forward is refused as a replay.
    fn verify_webauthn(
        &mut self,
        request: &WebauthnUse,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        self.with_second_factor_gate(stamp, now_ms, |store, hash, principal| {
            let record = store
                .webauthn
                .get_mut(&request.credential_id)
                .filter(|record| record.principal_id == principal);
            let Some(record) = record else {
                return Ok(store.second_factor_failed(stamp, &principal, now_ms));
            };
            if !counter_moves_forward(record.sign_count, request.new_sign_count) {
                return Err(IdentityRefusal::Replay);
            }
            record.sign_count = request.new_sign_count;
            record.last_used_at_ms = Some(now_ms);
            store.complete_session(&hash, "webauthn", now_ms);
            store.clear_throttle(&account_key(&principal));
            store.audit_event(stamp, now_ms, IdentityEvent::MfaVerified, Some(&principal));
            Ok(store.second_factor_ok(principal))
        })
    }

    /// Remove a credential: an administrator any, a principal only its own
    /// (another principal's credential is `not_found`, never confirmed).
    fn remove_webauthn(
        &mut self,
        request: &ObjectRef,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let actor = &stamp.actor;
        let admin = actor.holds(IDENTITY_ADMIN_SCOPE) && !actor.delegated;
        let owner = self
            .webauthn
            .get(&request.id)
            .map(|record| record.principal_id.clone())
            .filter(|owner| admin || owner == &actor.principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        self.webauthn.remove(&request.id);
        self.audit_event(stamp, now_ms, IdentityEvent::MfaRemoved, Some(&owner));
        Ok(IdentityReply::Done { changed: true })
    }
}

#[cfg(test)]
mod tests {
    use super::counter_moves_forward;

    #[test]
    fn the_counter_must_move_forward_unless_the_authenticator_keeps_none() {
        assert!(counter_moves_forward(4, 5));
        assert!(counter_moves_forward(0, 0));
        assert!(!counter_moves_forward(5, 5));
        assert!(!counter_moves_forward(5, 3));
        assert!(!counter_moves_forward(5, 0));
    }
}
