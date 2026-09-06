use super::{authority::*, contract::*, *};

/// Process-local affinity for one exact registry and its descriptor-pinned
/// journal, Current, staging, and generation roots. A cloned
/// [`StateImageAuthority`] may create multiple registries, so authority identity
/// alone is intentionally insufficient for durable publication.
pub struct DirectStateRegistryIdentity {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) contract_sha256: String,
}

/// A concrete store type may occupy exactly one closed direct-state domain. The
/// registry alone erases it; callers can recover values only through this typed
/// domain binding, never by raw `Any`/downcast.
/// # Invariants
///
/// Implementations are serving capability wrappers, not raw/clonable store handles.
/// They must not expose an owned store, `Arc`, transaction, or other value that can
/// outlive the `&self` borrow obtained from [`DirectStateReadSession`]. Long-lived
/// operations retain the session itself for their complete lifetime.
///
/// Both evidence methods are security boundaries, not provider-chosen labels.
/// `dynamic_store_authority_digest` MUST be the domain-separated digest produced
/// by the underlying store's strict current-authority inspection and MUST bind the
/// exact physical store incarnation (canonical root plus device/inode or the
/// platform's equivalent), static owner/layout manifest, schema, scope binding,
/// and durable authority epoch. It therefore changes if a different physical
/// store is substituted even when that store has valid rows and the same layout.
/// `install_evidence_sha256` MUST be a domain-separated canonical digest of the
/// complete strictly validated logical recovery evidence derived from the
/// authenticated immutable Incoming image after adoption. It may be ignored for
/// mutable Current recovery, but Pending recovery MUST recompute it from the exact
/// pinned generation and compare it to the journal. Returning constants, hashing
/// caller-supplied metadata without strict store inspection, or omitting any owned
/// table/row census makes an implementation unsound.
/// Crate-sealed marker traits keep implementations inside this audited engine
/// boundary while avoiding `unsafe trait` as a substitute for enforceable API
/// structure. Each concrete provider opts into the matching marker explicitly.
pub trait DirectStateDomainValue:
    sealed::DirectStateDomainValue + Any + Send + Sync + 'static
{
    const DOMAIN: DirectStateDomain;
    fn dynamic_store_authority_digest(&self) -> Result<[u8; 32], String>;
    fn install_evidence_sha256(&self) -> Result<[u8; 32], String>;
}

/// # Invariants
///
/// An operation stored beside a read session must not be `Clone`, expose an owned
/// store/transaction/`Arc`, or otherwise let usable authority escape a borrow of
/// the operation wrapper. Implementations are a closed, audited provider surface.
pub trait DirectStatePinnedValue: sealed::DirectStatePinnedValue + Send + 'static {}

pub struct DirectStateGenerationEntry {
    pub(super) domain: DirectStateDomain,
    pub(super) type_id: TypeId,
    pub(super) dynamic_store_authority_digest: [u8; 32],
    pub(super) install_evidence_sha256: [u8; 32],
    pub(super) value: Arc<dyn Any + Send + Sync>,
}

impl DirectStateGenerationEntry {
    pub(super) fn new<T: DirectStateDomainValue>(value: Arc<T>) -> Result<Self, String> {
        let dynamic_store_authority_digest = value.dynamic_store_authority_digest()?;
        let install_evidence_sha256 = value.install_evidence_sha256()?;
        if dynamic_store_authority_digest == [0_u8; 32] || install_evidence_sha256 == [0_u8; 32] {
            return Err("direct-state domain value returned absent authority evidence".into());
        }
        Ok(Self {
            domain: T::DOMAIN,
            type_id: TypeId::of::<T>(),
            dynamic_store_authority_digest,
            install_evidence_sha256,
            value,
        })
    }

    pub fn get<T: DirectStateDomainValue>(&self) -> Result<&T, String> {
        if T::DOMAIN != self.domain || self.type_id != TypeId::of::<T>() {
            return Err("direct-state provider requested the wrong domain value".into());
        }
        self.value
            .as_ref()
            .downcast_ref::<T>()
            .ok_or_else(|| "direct-state provider value type erasure failed".to_string())
    }
}

/// One immutable-in-membership process generation containing the complete closed
/// direct-state domain set. The stores inside remain mutable under read sessions;
/// replacing the generation is one atomic Arc swap.
pub struct DirectStateGeneration {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) scope: DirectStateScope,
    pub(super) sections_sha256: String,
    pub(super) entries: BTreeMap<DirectStateDomain, DirectStateGenerationEntry>,
}

impl DirectStateGeneration {
    pub(super) fn validate_closed(&self) -> Result<(), String> {
        if self.entries.len() != DirectStateDomain::ALL.len()
            || DirectStateDomain::ALL.into_iter().any(|domain| {
                self.entries
                    .get(&domain)
                    .is_none_or(|entry| entry.domain != domain)
            })
        {
            return Err("direct-state generation is not the exact closed domain set".into());
        }
        Ok(())
    }

    pub(super) fn get<T: DirectStateDomainValue>(&self) -> Result<&T, String> {
        let entry = self
            .entries
            .get(&T::DOMAIN)
            .ok_or_else(|| format!("direct-state generation omits {:?}", T::DOMAIN))?;
        entry.get::<T>()
    }
}

/// Complete off-serving generation. Construction is registry-private and proves
/// the closed domain census, exact provider types, and one source journal/image.
pub struct AssembledDirectStateGeneration {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) source_journal_sha256: Option<String>,
    pub(super) current_image_sha256: String,
    pub(super) generation: Arc<DirectStateGeneration>,
}
