//! The one composition-root scope-grant authority for every kernel-owned
//! durable store this binary opens (RF-RULING-004).
//!
//! `eg_storage::StorageKernel` never interprets proof bytes: it asks a
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
//! ## What a proof binds, and what binds the rest
//!
//! A proof is a keyed SHA-256 MAC over the per-process secret and the
//! principal, carrying a fresh random grant id. On its FIRST use the authority
//! records the exact (physical store identity digest, layout name, principal)
//! that grant was spent on; every later use of the same grant must present the
//! same three, so a proof minted for `kv.redb`/`OwnerLayout::Kv` can never
//! authorize a scope on `blob.redb`, on any other layout, or for any other
//! principal. That closes the transplant vector the unbound construction had:
//! one proof used to grant any scope on any layout of any store in the process.
//!
//! It does NOT bind the SCOPE, and that is a deliberate, recorded limit rather
//! than an oversight. A courier-held proof is authenticated once at `open` and
//! then re-presented for every scope the store binds LATER --
//! `eg_tsdb::SeriesStore` keeps its proof precisely so a new series' scope can
//! be authenticated long after `open` returned (`SeriesStore::scope_handle`) --
//! so a one-scope-per-proof rule would refuse every series after the first.
//! Binding the scope at MINT time is not available either: the domain crates
//! build their own physical identity and serving scope internally
//! (`RbacStore::open`, `StatechartStore::open`, `SeriesStore::open`, ...), so
//! the composition root cannot compute a scope-bound proof for them without
//! every one of those crates first exporting its physical identity. What
//! bounds the scope instead is the kernel: `authenticate_scope` checks
//! `layout == D::LAYOUT` and `layout.accepts(identity)` against the store's OWN
//! persisted manifest before a grant can be bound, and a bound handle can only
//! ever address the scope it was minted for.
//!
//! There is exactly one authority per process. Handing a second one out would
//! be a second grant authority for the same files, which is the shape
//! RF-RULING-004 forbids.

use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
use eg_types::MutationScopeIdentity;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// Domain separation for the grant proof. A digest computed under any other
/// tag is not a grant proof.
const GRANT_PROOF_DOMAIN: &[u8] = b"eg/root-scope-grant/v1\0";

/// Domain separation for the store binding a spent grant is pinned to.
const GRANT_BINDING_DOMAIN: &[u8] = b"eg/root-scope-grant-binding/v1\0";

/// Bytes of the random grant id a proof carries ahead of its MAC.
const GRANT_ID_BYTES: usize = 16;

/// The stable principal this engine serves every store-private scope as.
///
/// It is durable state — it lands in `mutation_scope_bindings` and in the
/// context principal of every maintenance batch this engine commits — so it may
/// not be derived from the per-process secret.
///
/// The value is `principal:sha256:` + SHA-256("epistemic-graph:engine"), because
/// `MutationBatch::validate` (`eg-types` `validate_principal`) accepts ONLY that
/// opaque-digest form: a readable principal string is unrepresentable in a batch,
/// so a store bound under one could never commit a maintenance mutation. It is
/// the same construction `server::mutation_batch::digest::principal_fingerprint`
/// applies to a caller identity, over the engine's own name.
pub const ENGINE_PRINCIPAL: &str = crate::server::mutation_batch::ENGINE_LEDGER_PRINCIPAL;

/// The composition root's scope-grant authority.
pub struct EngineScopeAuthority {
    secret: [u8; 32],
    /// The store binding each issued grant id has been spent on, recorded on
    /// that grant's first use. A grant is issued unbound and pinned on use,
    /// because a courier that holds a proof for later scope authentication
    /// cannot state its store's identity at mint time -- the domain crate
    /// builds it. One entry per `proof()` call; a proof is minted per store
    /// open, so this is bounded by the number of durable stores the process
    /// opens.
    spent: Mutex<HashMap<[u8; GRANT_ID_BYTES], [u8; 32]>>,
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
            spent: Mutex::new(HashMap::new()),
        })
    }

    /// The principal every grant this authority issues names.
    pub fn principal(&self) -> &'static str {
        ENGINE_PRINCIPAL
    }

    /// The proof bytes one store this engine opens presents.
    ///
    /// Each call mints a FRESH grant: `grant_id || MAC(secret, grant_id,
    /// principal)`. The grant id is what the first successful verification
    /// pins to a store binding, so two stores opened with two `proof()` calls
    /// hold two grants that can never be swapped for one another.
    pub fn proof(&self) -> Vec<u8> {
        let mut grant_id = [0u8; GRANT_ID_BYTES];
        grant_id.copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let mut proof = grant_id.to_vec();
        proof.extend_from_slice(&self.digest(&grant_id, ENGINE_PRINCIPAL));
        proof
    }

    /// The MAC a grant id carries for `principal`.
    fn digest(&self, grant_id: &[u8; GRANT_ID_BYTES], principal: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(GRANT_PROOF_DOMAIN);
        hasher.update(self.secret);
        hasher.update(grant_id);
        hasher.update(principal.as_bytes());
        hasher.finalize().into()
    }

    /// The store binding a grant is pinned to: the physical store's identity
    /// digest, the layout's canonical name, and the principal. Keyed on the
    /// same per-process secret, so a binding record from one process is
    /// meaningless in another.
    fn binding(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        principal: &str,
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(GRANT_BINDING_DOMAIN);
        hasher.update(self.secret);
        hasher.update(physical.digest());
        hasher.update(layout.canonical_name().as_bytes());
        hasher.update([0u8]);
        hasher.update(principal.as_bytes());
        hasher.finalize().into()
    }

    /// Pin `grant_id` to `binding` on first use; refuse any later use against a
    /// different one. Fail-closed on a poisoned lock: a grant that cannot be
    /// checked is not a grant.
    fn spend(&self, grant_id: [u8; GRANT_ID_BYTES], binding: [u8; 32]) -> Result<(), String> {
        let mut spent = self
            .spent
            .lock()
            .map_err(|_| "engine scope grant authority is poisoned".to_string())?;
        match spent.get(&grant_id) {
            Some(pinned) if *pinned == binding => Ok(()),
            Some(_) => Err("engine scope grant was issued for another store".to_string()),
            None => {
                spent.insert(grant_id, binding);
                Ok(())
            }
        }
    }
}

impl ScopeGrantVerifier for EngineScopeAuthority {
    /// Fail-closed on every dimension a proof can carry: it must be a
    /// well-formed grant this process minted for THIS principal, and it must be
    /// spent on the same (physical store, layout, principal) as every earlier
    /// use of that grant. `identity` is bounded by the kernel's own
    /// `layout.accepts` check against the store's persisted manifest, not here
    /// -- see the module doc for why a courier-held proof cannot bind it.
    fn verify(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        _identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if proof.len() != GRANT_ID_BYTES + 32 {
            return Err("engine scope grant authority rejected".to_string());
        }
        let mut grant_id = [0u8; GRANT_ID_BYTES];
        grant_id.copy_from_slice(&proof[..GRANT_ID_BYTES]);
        let expected = self.digest(&grant_id, principal);
        if !constant_time_eq(&proof[GRANT_ID_BYTES..], &expected) {
            return Err("engine scope grant authority rejected".to_string());
        }
        self.spend(grant_id, self.binding(physical, layout, principal))
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
            eg_types::mutation_batch::DurabilityDomain::ControlPlane,
            "a",
            "test:v1",
        )
        .unwrap()
    }

    fn physical() -> PhysicalStoreIdentity {
        PhysicalStoreIdentity::new("physical:test:a").unwrap()
    }

    fn other_physical() -> PhysicalStoreIdentity {
        PhysicalStoreIdentity::new("physical:test:b").unwrap()
    }

    fn other_scope() -> MutationScopeIdentity {
        MutationScopeIdentity::fixed_native(
            "native",
            eg_types::mutation_batch::DurabilityDomain::ControlPlane,
            "b",
            "test:v1",
        )
        .unwrap()
    }

    /// Spend a fresh grant on `physical()`/`ColdTier`/`scope()`, then present the
    /// SAME grant against `next`. Returns whether the second use was accepted.
    fn transplanted(
        authority: &EngineScopeAuthority,
        next: (&PhysicalStoreIdentity, OwnerLayout, &MutationScopeIdentity),
    ) -> bool {
        let proof = authority.proof();
        authority
            .verify(
                &physical(),
                OwnerLayout::ColdTier,
                &scope(),
                ENGINE_PRINCIPAL,
                &proof,
            )
            .expect("the first use pins the grant");
        authority
            .verify(next.0, next.1, next.2, ENGINE_PRINCIPAL, &proof)
            .is_ok()
    }

    #[test]
    fn a_proof_spent_on_one_store_is_rejected_for_another() {
        let authority = EngineScopeAuthority::new();
        assert!(!transplanted(
            &authority,
            (&other_physical(), OwnerLayout::ColdTier, &scope())
        ));
    }

    #[test]
    fn a_proof_spent_on_one_layout_is_rejected_for_another() {
        let authority = EngineScopeAuthority::new();
        assert!(!transplanted(
            &authority,
            (&physical(), OwnerLayout::TenantCatalog, &scope())
        ));
    }

    /// The recorded limit, asserted so it cannot change silently: a grant is NOT
    /// pinned to one serving scope, because a courier-held proof authenticates
    /// new scopes on its own store long after `open` returned
    /// (`eg_tsdb::SeriesStore::scope_handle`). The scope is bounded by the
    /// kernel's `layout.accepts` check against the store's persisted manifest,
    /// not by the proof. Every OTHER dimension is pinned -- see the two tests
    /// above and `a_proof_presented_for_another_principal_is_rejected`.
    #[test]
    fn a_proof_serves_further_scopes_on_the_store_it_was_spent_on() {
        let authority = EngineScopeAuthority::new();
        assert!(transplanted(
            &authority,
            (&physical(), OwnerLayout::ColdTier, &other_scope())
        ));
    }

    #[test]
    fn each_proof_is_a_distinct_grant() {
        let authority = EngineScopeAuthority::new();
        assert_ne!(authority.proof(), authority.proof());
        // ...so pinning one store's grant leaves the next store's open.
        assert!(transplanted(
            &authority,
            (&physical(), OwnerLayout::ColdTier, &scope())
        ));
        let second = authority.proof();
        assert!(authority
            .verify(
                &other_physical(),
                OwnerLayout::TenantCatalog,
                &other_scope(),
                ENGINE_PRINCIPAL,
                &second
            )
            .is_ok());
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
        // Empty; truncated; over-long; right length, right grant id, zeroed MAC.
        let mut zeroed_mac = full[..GRANT_ID_BYTES].to_vec();
        zeroed_mac.extend_from_slice(&[0u8; 32]);
        let mut extended = full.clone();
        extended.push(0);
        for forged in [
            Vec::new(),
            full[..full.len() - 1].to_vec(),
            extended,
            zeroed_mac,
            vec![0u8; 32],
        ] {
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
