//! What an actor may do NOW: the one authority every identity path asks.
//!
//! A verified token says what its principal held when it was minted. For a
//! principal this store owns, the store says what it holds now, and both must
//! agree -- so disabling, deprovisioning or demoting a principal takes effect
//! on the tokens it already carries, not only on the ones it is issued next.

use super::super::ops::{IdentityOp, OpAuthority, IDENTITY_ADMIN_SCOPE, IDENTITY_SELF_SCOPE};
use super::super::scope::ScopeClassifier;
use super::super::stamp::{IdentityActor, IdentityStamp};
use super::super::IdentityRefusal;
use super::IdentityStore;

impl IdentityStore {
    /// Whether `actor` holds `scope` NOW -- the one authority every identity
    /// path asks (the op families, the administrator override on a single
    /// credential, the SQL relations).
    ///
    /// The verified token must carry the exact scope. For a principal this
    /// store owns that is necessary and NOT sufficient: the store must still
    /// back it -- the principal is active and its current roles still resolve
    /// to the scope. A token minted before the principal was disabled,
    /// deprovisioned or taken out of the role therefore holds nothing, however
    /// long it has left to live. A principal the store does not own (the
    /// broker, a provisioner, the operator before first-run) has no record
    /// here; its verified token is its whole authority.
    pub fn holds_now(
        &self,
        actor: &IdentityActor,
        scope: &str,
        classifier: &dyn ScopeClassifier,
    ) -> bool {
        if !actor.holds(scope) {
            return false;
        }
        let Some(user) = self.users.get(&actor.principal_id) else {
            return true;
        };
        let (roles, _) = self.effective_roles(&actor.principal_id);
        user.status.is_active() && self.scopes_of(&roles, classifier).contains(scope)
    }

    /// Whether `actor` may read the redacted identity directory now.
    pub fn reads_directory(&self, actor: &IdentityActor, classifier: &dyn ScopeClassifier) -> bool {
        OpAuthority::Read
            .scopes()
            .iter()
            .any(|scope| self.holds_now(actor, scope, classifier))
    }

    /// Whether the stamped actor holds the exact authority `op` needs. The
    /// boundary calls this BEFORE deriving (argon2id, unsealing), so an
    /// unauthorized caller never makes the engine do that work; `apply` asks
    /// again against the store the op is applied to, so authority revoked
    /// while the derivation ran is not honored.
    pub fn authorize(
        &self,
        op: &IdentityOp,
        stamp: &IdentityStamp,
        classifier: &dyn ScopeClassifier,
    ) -> Result<(), IdentityRefusal> {
        let actor = &stamp.actor;
        let authority = op.meta().authority;
        let holds_one = authority
            .scopes()
            .iter()
            .any(|scope| self.holds_now(actor, scope, classifier));
        let standing = match authority {
            OpAuthority::SelfService => self.is_active(&actor.principal_id),
            OpAuthority::SelfOrAdmin => self.self_or_admin(actor, classifier),
            OpAuthority::Admin
            | OpAuthority::Read
            | OpAuthority::Broker
            | OpAuthority::FirstRun
            | OpAuthority::Directory
            | OpAuthority::Provision => !(authority.direct_only() && actor.delegated),
        };
        let allowed = holds_one && standing;
        if allowed {
            Ok(())
        } else {
            Err(IdentityRefusal::NotAuthorized)
        }
    }

    /// Direct `identity:admin` held now: the administrator override.
    pub(crate) fn administers(
        &self,
        actor: &IdentityActor,
        classifier: &dyn ScopeClassifier,
    ) -> bool {
        self.holds_now(actor, IDENTITY_ADMIN_SCOPE, classifier) && !actor.delegated
    }

    /// `identity:admin` from a direct actor, or `identity:self` from an
    /// active principal (the op then acts only on the actor's own records).
    fn self_or_admin(&self, actor: &IdentityActor, classifier: &dyn ScopeClassifier) -> bool {
        self.administers(actor, classifier)
            || (self.holds_now(actor, IDENTITY_SELF_SCOPE, classifier)
                && self.is_active(&actor.principal_id))
    }
}
