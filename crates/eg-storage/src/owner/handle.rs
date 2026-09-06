use crate::owner::domain::OwnerDomain;
use eg_types::MutationScopeIdentity;
use std::marker::PhantomData;

/// Opaque capability for one authenticated, bound serving scope.
pub struct OwnedStoreHandle<D: OwnerDomain> {
    pub(crate) identity: MutationScopeIdentity,
    pub(crate) principal: String,
    pub(crate) authority_digest: [u8; 32],
    _domain: PhantomData<D>,
}

impl<D: OwnerDomain> OwnedStoreHandle<D> {
    pub fn identity(&self) -> &MutationScopeIdentity {
        &self.identity
    }
}

impl<D: OwnerDomain> OwnedStoreHandle<D> {
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

    pub fn principal(&self) -> &str {
        &self.principal
    }

    pub(crate) fn authority_digest(&self) -> &[u8; 32] {
        &self.authority_digest
    }
}
