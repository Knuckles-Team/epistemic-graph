//! Which credential a password verdict belongs to.
//!
//! The boundary verifies a candidate against a snapshot of the store, outside
//! the engine's write lock, so by the time the verdict is applied the
//! credential may have been replaced or the principal's sessions revoked. A
//! verdict names the generation of the credential it was computed against and
//! is honored only while that is still the principal's credential.

use super::super::stamp::{IdentityStamp, PasswordCheck};
use super::super::IdentityRefusal;
use super::IdentityStore;

impl IdentityStore {
    /// Void every verdict the boundary computed against `principal_id`'s
    /// credential before now: its generation moves without the password
    /// changing. Called wherever the principal's sessions are revoked, so a
    /// sign-in that was being verified across the revocation opens nothing.
    pub(crate) fn void_verdicts(&mut self, principal_id: &str) {
        if let Some(credential) = self.passwords.get_mut(principal_id) {
            credential.generation += 1;
        }
    }

    /// The boundary's verdict, if it was computed against `principal`'s
    /// CURRENT credential. The derivation runs outside the engine's write
    /// lock, so the credential it read may have been replaced (or the
    /// principal's sessions revoked) before this apply: a verdict naming
    /// another principal is `Unstamped`, one naming another credential
    /// generation is `StaleCredential`, and neither is ever acted on.
    pub(super) fn current_verdict<'stamp>(
        &self,
        stamp: &'stamp IdentityStamp,
        principal: Option<&str>,
    ) -> Result<&'stamp PasswordCheck, IdentityRefusal> {
        let check = stamp.check()?;
        if check.principal_id.as_deref() != principal {
            return Err(IdentityRefusal::Unstamped);
        }
        let generation = principal
            .and_then(|principal| self.passwords.get(principal))
            .map(|credential| credential.generation);
        if check.generation != generation {
            return Err(IdentityRefusal::StaleCredential);
        }
        Ok(check)
    }

    /// Void every verdict computed before now, for every principal.
    pub(crate) fn void_all_verdicts(&mut self) {
        for credential in self.passwords.values_mut() {
            credential.generation += 1;
        }
    }
}
