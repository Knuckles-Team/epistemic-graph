//! The one composition-root scope-grant authority for every kernel-owned
//! durable store this binary opens (RF-RULING-004).
//!
//! `eg_storage::StorageKernelV1` never interprets proof bytes: it asks a
//! [`ScopeGrantVerifier`] the composition root supplies whether a principal is
//! entitled to serve one exact logical scope on one exact physical file under
//! one exact layout. This binary IS that root, so this module is where that
//! decision is made, once, for every store it owns.
//! The policy it enforces is
//! "this principal serves on behalf of THIS engine process", and the proof is
//! the per-process secret that answers it: a proof minted by any other process,
//! or presented for any other principal, is refused. The secret never leaves
//! the process and is never persisted; the principal is stable, because it is
//! written into every scope binding and every batch actor this engine records.
//!
//! The proof is deliberately NOT bound to one `(physical, layout, scope)`
//! triple. Domain crates open their own files: `RbacStore::open`,
//! `StatechartStore::open`, `SeriesStore::open` and the rest take
//! `(verifier, principal, proof)` and build the physical identity and the
//! serving scope THEMSELVES, so the composition root cannot compute a
//! triple-bound proof for them without every one of those crates first
//! exporting its physical identity — five crates' worth of new public surface
//! to re-state a fact the kernel already checks. And it does check it: the
//! grant is verified against the store's OWN persisted manifest
//! (`layout == D::LAYOUT` and `layout.accepts(identity)`,
//! `eg_storage::StorageKernelV1::authenticate_scope`) before it can be bound,
//! and a bound handle can only ever address the scope it was minted for. What
//! is left for this authority to decide is exactly what it decides.
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
/// context principal of every maintenance batch this engine commits — so it may
/// not be derived from the per-process secret.
///
/// The value is `principal:sha256:` + SHA-256("epistemic-graph:engine"), because
/// `MutationBatch::validate` (`eg-types` `validate_principal`) accepts ONLY that
/// opaque-digest form: a readable principal string is unrepresentable in a batch,
/// so a store bound under one could never commit a maintenance mutation. It is
/// the same construction `server::mutation_batch::digest::principal_fingerprint`
/// applies to a caller identity, over the engine's own name.
pub const ENGINE_PRINCIPAL: &str =
    "principal:sha256:41290b0e412ac542f312d4312a7a299e771eec66e3ffbbf7edb6369576875fb2";

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

/// The same authority as an owned trait object, for the stores whose `open`
/// takes `Arc<dyn ScopeGrantVerifier>` because they authenticate a new scope
/// lazily, long after `open` returned (`eg_tsdb::SeriesStore`).
pub fn process_verifier() -> Arc<dyn ScopeGrantVerifier> {
    Arc::clone(process_authority()) as Arc<dyn ScopeGrantVerifier>
}

impl std::fmt::Debug for EngineScopeAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineScopeAuthority")
            .finish_non_exhaustive()
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

    /// The proof bytes every store this engine opens presents.
    pub fn proof(&self) -> Vec<u8> {
        self.digest(ENGINE_PRINCIPAL).to_vec()
    }

    fn digest(&self, principal: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(GRANT_PROOF_DOMAIN);
        hasher.update(self.secret);
        hasher.update(principal.as_bytes());
        hasher.finalize().into()
    }
}

impl ScopeGrantVerifier for EngineScopeAuthority {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        _layout: OwnerLayout,
        _identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        let expected = self.digest(principal);
        if !constant_time_eq(proof, &expected) {
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

/// Open an EPHEMERAL SQL catalog at a fresh temp path under THIS process's own
/// grant authority, returning the store and the file it owns.
///
/// `eg_query::TableStore::open_temp` is compiled only under that crate's
/// `cfg(test)` or its off-by-default `dev-scope-grant` feature, because
/// RF-RULING-004 forbids a domain crate from carrying a scope-grant verifier of
/// its own. This binary IS a composition root, so its ephemeral SQL stores are
/// authenticated by the same [`EngineScopeAuthority`] as its durable ones and no
/// build of this binary -- test or production -- links a development stand-in.
///
/// The caller owns the returned path: an ephemeral store is a real redb file and
/// nothing else removes it (see `server::wire::EphemeralStoreGuard`).
#[cfg(feature = "query")]
pub fn open_ephemeral_sql_store() -> Result<(eg_query::TableStore, std::path::PathBuf), String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static EPHEMERAL_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let ordinal = EPHEMERAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "eg_root_sql_tables_{}_{}_{ordinal}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0),
    ));
    let authority = process_authority();
    let store = eg_query::TableStore::open(
        &path,
        process_verifier(),
        authority.principal(),
        &authority.proof(),
    )?;
    Ok((store, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> MutationScopeIdentity {
        MutationScopeIdentity::fixed_native(
            "native",
            eg_types::mutation_batch::MutationDomain::ControlPlane,
            "a",
            "test:v1",
        )
        .unwrap()
    }

    fn physical() -> PhysicalStoreIdentity {
        PhysicalStoreIdentity::new("physical:test:a").unwrap()
    }

    #[test]
    fn this_process_authority_accepts_its_own_proof() {
        let authority = EngineScopeAuthority::new();
        assert!(authority
            .verify(
                &physical(),
                OwnerLayout::TenantCatalog,
                &scope(),
                ENGINE_PRINCIPAL,
                &authority.proof()
            )
            .is_ok());
    }

    #[test]
    fn a_proof_from_another_authority_is_rejected() {
        let proof = EngineScopeAuthority::new().proof();
        assert!(EngineScopeAuthority::new()
            .verify(
                &physical(),
                OwnerLayout::ColdTier,
                &scope(),
                ENGINE_PRINCIPAL,
                &proof
            )
            .is_err());
    }

    #[test]
    fn a_proof_presented_for_another_principal_is_rejected() {
        let authority = EngineScopeAuthority::new();
        assert!(authority
            .verify(
                &physical(),
                OwnerLayout::ColdTier,
                &scope(),
                "principal:someone-else",
                &authority.proof()
            )
            .is_err());
    }

    #[test]
    fn an_empty_or_truncated_proof_is_rejected() {
        let authority = EngineScopeAuthority::new();
        let full = authority.proof();
        for forged in [Vec::new(), full[..full.len() - 1].to_vec(), vec![0u8; 32]] {
            assert!(authority
                .verify(
                    &physical(),
                    OwnerLayout::ColdTier,
                    &scope(),
                    ENGINE_PRINCIPAL,
                    &forged
                )
                .is_err());
        }
    }

    /// A store bound under a principal a `MutationBatch` cannot carry could never
    /// commit a maintenance mutation, and nothing would catch it until runtime.
    #[test]
    fn the_engine_principal_is_a_representable_batch_principal() {
        use sha2::{Digest, Sha256};
        let digest = ENGINE_PRINCIPAL
            .strip_prefix("principal:sha256:")
            .expect("engine principal must carry the opaque-digest prefix");
        assert_eq!(digest.len(), 64);
        assert!(digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')));
        assert_eq!(
            digest,
            hex::encode(Sha256::digest("epistemic-graph:engine".as_bytes()))
        );
    }

    #[test]
    fn the_process_authority_is_one_instance() {
        assert!(std::sync::Arc::ptr_eq(
            process_authority(),
            process_authority()
        ));
    }
}
