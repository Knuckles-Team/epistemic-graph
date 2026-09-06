//! The one composition-root scope-grant authority for every kernel-owned
//! durable store this binary opens (RF-RULING-004).
//!
//! `eg_storage::StorageKernelV1` never interprets proof bytes: it asks a
//! [`ScopeGrantVerifier`] the composition root supplies whether a principal is
//! entitled to serve one exact logical scope on one exact physical file under
//! one exact layout. This binary IS that root, so this module is where that
//! decision is made, once, for every store it owns.
//!
//! The proof is a keyed digest over the four things the grant actually binds —
//! the physical store identity, the owner layout, the scope binding digest and
//! the principal — under a secret minted once per process. It is therefore not
//! a constant a caller could forge from a source read: a proof issued for one
//! store, layout or scope does not verify for another, and no proof issued by
//! an earlier process verifies here. The secret never leaves the process and is
//! never persisted; the principal is stable, because it is written into every
//! scope binding and every batch actor this engine records.
//!
//! There is exactly one authority per process. Handing a second one out would
//! be a second grant authority for the same files, which is the shape
//! RF-RULING-004 forbids.

use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
use eg_types::MutationScopeIdentity;
use sha2::{Digest, Sha256};
use std::sync::{Arc, OnceLock};

/// Domain separation for the grant proof. A digest computed under any other
/// tag is not a grant proof.
const GRANT_PROOF_DOMAIN: &[u8] = b"eg/root-scope-grant/v1\0";

/// The stable principal this engine serves every store-private scope as.
///
/// It is durable state — it lands in `mutation_scope_bindings_v1` and in the
/// actor of every batch this engine commits — so it may not be derived from
/// the per-process secret.
pub const ENGINE_PRINCIPAL: &str = "principal:epistemic-graph:engine";

/// The composition root's scope-grant authority.
pub struct EngineScopeAuthority {
    secret: [u8; 32],
}

/// The one authority for this process.
///
/// Every kernel-owned store this binary opens authenticates its serving scope
/// against THIS instance, whether it is opened from `main`, from a server
/// state build, or from a durable store constructed lazily on first use. A
/// second authority would mean two independent grant decisions over the same
/// files, which is the shape RF-RULING-004 forbids; a `OnceLock` makes "one"
/// a property of the type rather than of a threading discipline.
static PROCESS_AUTHORITY: OnceLock<Arc<EngineScopeAuthority>> = OnceLock::new();

/// The process-wide scope-grant authority, minted on first use.
pub fn process_authority() -> &'static Arc<EngineScopeAuthority> {
    PROCESS_AUTHORITY.get_or_init(EngineScopeAuthority::new)
}

impl std::fmt::Debug for EngineScopeAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineScopeAuthority").finish_non_exhaustive()
    }
}

impl EngineScopeAuthority {
    /// Mint the one authority for this process.
    ///
    /// The secret is fresh per process and never persisted: a grant is
    /// authenticated at open time and bound in memory, so nothing durable
    /// depends on it, and a proof captured from one run cannot be replayed
    /// into the next.
    pub fn new() -> Arc<Self> {
        // Two v4 UUIDs are 32 bytes from the platform CSPRNG; folding them
        // through the domain tag keeps the secret opaque to their layout.
        let mut hasher = Sha256::new();
        hasher.update(GRANT_PROOF_DOMAIN);
        hasher.update(uuid::Uuid::new_v4().as_bytes());
        hasher.update(uuid::Uuid::new_v4().as_bytes());
        Arc::new(Self {
            secret: hasher.finalize().into(),
        })
    }

    /// The principal every grant this authority issues names.
    pub fn principal(&self) -> &'static str {
        ENGINE_PRINCIPAL
    }

    /// The proof bytes for one exact `(physical, layout, scope)` triple.
    pub fn proof(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
    ) -> Vec<u8> {
        self.digest(physical, layout, identity, ENGINE_PRINCIPAL)
            .to_vec()
    }

    fn digest(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(GRANT_PROOF_DOMAIN);
        hasher.update(self.secret);
        hasher.update(physical.digest());
        hasher.update(layout.canonical_name().as_bytes());
        hasher.update([0u8]);
        hasher.update(identity.binding_digest().as_bytes());
        hasher.update(principal.as_bytes());
        hasher.finalize().into()
    }
}

impl ScopeGrantVerifier for EngineScopeAuthority {
    fn verify(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        let expected = self.digest(physical, layout, identity, principal);
        if proof.len() != expected.len() || !constant_time_eq(proof, &expected) {
            return Err("engine scope grant authority rejected".to_string());
        }
        Ok(())
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(resource: &str) -> MutationScopeIdentity {
        MutationScopeIdentity::fixed_native(
            "native",
            eg_types::mutation_batch::MutationDomain::ControlPlane,
            resource,
            "test:v1",
        )
        .unwrap()
    }

    fn physical(name: &str) -> PhysicalStoreIdentity {
        PhysicalStoreIdentity::new(name).unwrap()
    }

    #[test]
    fn a_proof_verifies_for_exactly_the_triple_it_was_issued_for() {
        let authority = EngineScopeAuthority::new();
        let store = physical("physical:test:a");
        let identity = scope("a");
        let proof = authority.proof(&store, OwnerLayout::TenantCatalog, &identity);
        assert!(authority
            .verify(
                &store,
                OwnerLayout::TenantCatalog,
                &identity,
                ENGINE_PRINCIPAL,
                &proof
            )
            .is_ok());
        for (other_store, other_layout, other_scope) in [
            (physical("physical:test:b"), OwnerLayout::TenantCatalog, scope("a")),
            (physical("physical:test:a"), OwnerLayout::NodeInfo, scope("a")),
            (physical("physical:test:a"), OwnerLayout::TenantCatalog, scope("b")),
        ] {
            assert!(
                authority
                    .verify(
                        &other_store,
                        other_layout,
                        &other_scope,
                        ENGINE_PRINCIPAL,
                        &proof
                    )
                    .is_err(),
                "a grant proof must not carry to another store, layout or scope"
            );
        }
    }

    #[test]
    fn a_proof_from_another_authority_is_rejected() {
        let store = physical("physical:test:a");
        let identity = scope("a");
        let proof = EngineScopeAuthority::new().proof(&store, OwnerLayout::ColdTier, &identity);
        assert!(EngineScopeAuthority::new()
            .verify(
                &store,
                OwnerLayout::ColdTier,
                &identity,
                ENGINE_PRINCIPAL,
                &proof
            )
            .is_err());
    }

    #[test]
    fn a_proof_for_another_principal_is_rejected() {
        let authority = EngineScopeAuthority::new();
        let store = physical("physical:test:a");
        let identity = scope("a");
        let proof = authority.proof(&store, OwnerLayout::ColdTier, &identity);
        assert!(authority
            .verify(
                &store,
                OwnerLayout::ColdTier,
                &identity,
                "principal:someone-else",
                &proof
            )
            .is_err());
    }
}
