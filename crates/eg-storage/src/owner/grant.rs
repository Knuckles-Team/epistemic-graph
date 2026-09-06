use crate::owner::domain::OwnerDomain;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use eg_types::MutationScopeIdentity;
use std::marker::PhantomData;

/// Composition-root proof authority. Proof bytes are interpreted only here.
pub trait ScopeGrantVerifier: Send + Sync {
    fn verify(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String>;
}

/// Opaque authenticated authorization for one exact logical serving identity.
pub struct AuthenticatedScopeGrant<D: OwnerDomain> {
    identity: MutationScopeIdentity,
    principal: String,
    authority_digest: [u8; 32],
    _domain: PhantomData<D>,
}

impl<D: OwnerDomain> AuthenticatedScopeGrant<D> {
    pub(crate) fn new(
        identity: MutationScopeIdentity,
        principal: String,
        authority_digest: [u8; 32],
    ) -> Self {
        Self {
            identity,
            principal,
            authority_digest,
            _domain: PhantomData,
        }
    }

    pub(crate) fn into_parts(self) -> (MutationScopeIdentity, String, [u8; 32]) {
        (self.identity, self.principal, self.authority_digest)
    }

    pub(crate) fn identity(&self) -> &MutationScopeIdentity {
        &self.identity
    }

    pub(crate) fn authority_digest(&self) -> &[u8; 32] {
        &self.authority_digest
    }
}
