use super::{
    authority::*, capture::*, contract::*, filesystem::*, generation::*, image::*, journal::*, *,
};

pub struct DirectStateProviderStage {
    pub(super) generation: StagedDirectStateGeneration,
    pub(super) entry: DirectStateGenerationEntry,
}

impl DirectStateProviderStage {
    pub fn new<T: DirectStateDomainValue>(
        generation: StagedDirectStateGeneration,
        value: Arc<T>,
    ) -> Result<Self, String> {
        if generation.section.generation_manifest.domain != T::DOMAIN {
            return Err("direct-state provider value belongs to a different domain".into());
        }
        let entry = DirectStateGenerationEntry::new(value)?;
        if generation
            .section
            .generation_manifest
            .dynamic_store_authority_digest
            != Some(entry.dynamic_store_authority_digest)
            || generation
                .section
                .generation_manifest
                .install_evidence_sha256
                != entry.install_evidence_sha256
        {
            return Err("direct-state staged value evidence differs from its generation".into());
        }
        Ok(Self { generation, entry })
    }
}

pub struct DirectStateRecoveredValue {
    pub(super) domain: DirectStateDomain,
    pub(super) entry: DirectStateGenerationEntry,
}

impl DirectStateRecoveredValue {
    pub fn new<T: DirectStateDomainValue>(value: Arc<T>) -> Result<Self, String> {
        Ok(Self {
            domain: T::DOMAIN,
            entry: DirectStateGenerationEntry::new(value)?,
        })
    }
}

/// # Invariants
///
/// Implementations may inspect immutable input only inside
/// [`DirectStatePhysicalImage::with_pinned_path`]. They must not retain or return
/// that borrowed descriptor path, an opened descriptor, raw store authority, or
/// any value that can reopen the input after the callback returns. Provider state
/// may contain only derived validation evidence required by `stage_replace`.
pub trait DirectStateProvider: sealed::DirectStateProvider + Send + Sync {
    fn domain(&self) -> DirectStateDomain;
    fn value_type_id(&self) -> TypeId;
    fn owner_manifest(&self) -> Result<DirectStateOwnerManifestV1, String>;
    fn staging_directory(&self) -> &Path;
    fn generation_directory(&self) -> &Path;

    /// Create the first current-format immutable source for a genuinely empty
    /// deployment. The provider must prove that no legacy, canonical, or
    /// unjournaled domain authority exists before creating a private capture; it
    /// must fail closed rather than adopt existing bytes. The returned source is
    /// consumed by the ordinary validate/stage/aggregate journal path.
    fn capture_initial(
        &self,
        permit: &StateImageInstallPermit,
        binding: &DirectStateCaptureBinding,
    ) -> Result<DirectStateSectionSource, String>;

    /// Create a stable MVCC image.  Implementations must never copy an open redb
    /// file directly; use the store's supported backup/copy operation.
    fn capture(
        &self,
        permit: &StateImageWritePermit,
        current: &DirectStateGenerationEntry,
        binding: &DirectStateCaptureBinding,
    ) -> Result<DirectStateSectionSource, String>;

    /// Strictly open/validate current-format authority off-serving.  The returned
    /// value is opaque to the coordinator and consumed only by this provider.
    fn validate_payload(
        &self,
        permit: &StateImageInstallPermit,
        manifest: &DirectStateSectionManifestV1,
        received: &DirectStatePhysicalImage,
    ) -> Result<Box<dyn Any + Send>, String>;

    fn stage_replace(
        &self,
        permit: &StateImageInstallPermit,
        manifest: &DirectStateSectionManifestV1,
        received: DirectStatePhysicalImage,
        validated: Box<dyn Any + Send>,
    ) -> Result<DirectStateProviderStage, String>;

    /// Strictly reopen mutable Current authority. Snapshot-time byte or logical
    /// evidence is intentionally not compared after ordinary serving mutations;
    /// the immutable per-generation physical identity, schema, owner, and root are.
    fn recover_current(
        &self,
        permit: &StateImageInstallPermit,
        section: &DirectStateInstallSectionV1,
        generation: &VerifiedDirectStateGeneration,
    ) -> Result<DirectStateRecoveredValue, String>;

    /// Recover Pending only after the coordinator has authenticated and pinned
    /// its exact immutable Incoming image. Implementations must strictly open the
    /// exact journaled generation and compare its physical authority plus exact
    /// install evidence; they cannot omit the typed provenance argument.
    fn recover_pending(
        &self,
        permit: &StateImageInstallPermit,
        section: &DirectStateInstallSectionV1,
        incoming: &VerifiedDirectStateIncoming,
        generation: &VerifiedDirectStateGeneration,
    ) -> Result<DirectStateRecoveredValue, String>;
}

/// Independently configured closed owner-layout census. Provider code cannot
/// self-attest its own table/layout identity at registration time.
pub struct DirectStateRegistryContract {
    pub(super) owners: BTreeMap<DirectStateDomain, DirectStateOwnerManifestV1>,
}

impl DirectStateRegistryContract {
    pub fn new(
        owners: impl IntoIterator<Item = DirectStateOwnerManifestV1>,
    ) -> Result<Self, String> {
        let mut by_domain = BTreeMap::new();
        for owner in owners {
            owner.sha256()?;
            let domain = owner.domain;
            if by_domain.insert(domain, owner).is_some() {
                return Err(format!(
                    "duplicate direct-state owner contract for {domain:?}"
                ));
            }
        }
        if DirectStateDomain::ALL
            .into_iter()
            .any(|domain| !by_domain.contains_key(&domain))
        {
            return Err("direct-state owner contract is not a closed domain set".into());
        }
        Ok(Self { owners: by_domain })
    }

    pub(super) fn owner(
        &self,
        domain: DirectStateDomain,
    ) -> Result<&DirectStateOwnerManifestV1, String> {
        self.owners
            .get(&domain)
            .ok_or_else(|| format!("direct-state owner contract omits {domain:?}"))
    }

    pub(super) fn sha256(&self) -> Result<String, String> {
        let owners = DirectStateDomain::ALL
            .into_iter()
            .map(|domain| self.owner(domain).cloned())
            .collect::<Result<Vec<_>, _>>()?;
        let bytes = rmp_serde::to_vec_named(&owners)
            .map_err(|error| format!("encode direct-state owner contract: {error}"))?;
        if bytes.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
            return Err("direct-state owner contract exceeds its byte bound".into());
        }
        Ok(hex_sha256(&bytes))
    }
}

pub(super) struct DirectStateProviderRegistration {
    pub(super) provider: Arc<dyn DirectStateProvider>,
    pub(super) owner: DirectStateOwnerManifestV1,
    pub(super) staging: PinnedPrivateDirectory,
    pub(super) generations: PinnedPrivateDirectory,
}

impl DirectStateProviderRegistration {
    pub(super) fn roots(
        &self,
        registry_identity: &Arc<DirectStateRegistryIdentity>,
    ) -> Result<RegisteredDirectStateRoots, String> {
        self.validate_live()?;
        Ok(RegisteredDirectStateRoots {
            registry_identity: registry_identity.clone(),
            domain: self.owner.domain,
            staging: self.staging.try_clone_token()?,
            generations: self.generations.try_clone_token()?,
        })
    }

    pub(super) fn validate_live(&self) -> Result<(), String> {
        self.staging
            .validate_live("direct-state provider staging root")?;
        self.generations
            .validate_live("direct-state provider generation root")?;
        if self.provider.staging_directory() != self.staging.path
            || self.provider.generation_directory() != self.generations.path
            || self.provider.owner_manifest()? != self.owner
        {
            return Err("direct-state provider registration changed after binding".into());
        }
        Ok(())
    }

    pub(super) fn pin_generation(
        &self,
        authority_identity: &Arc<StateImageAuthorityIdentity>,
        registry_identity: &Arc<DirectStateRegistryIdentity>,
        section: &DirectStateInstallSectionV1,
    ) -> Result<VerifiedDirectStateGeneration, String> {
        self.validate_live()?;
        if section.generation_manifest.domain != self.owner.domain {
            return Err("direct-state generation belongs to another registered domain".into());
        }
        let path = resolve_generation_path(&self.generations.path, section)?;
        let name = self
            .generations
            .validate_path(&path, "registered direct-state generation")?;
        let authority_file = self
            .generations
            .reader()
            .open_regular(name, "pin registered direct-state generation")?;
        Ok(VerifiedDirectStateGeneration {
            authority_identity: authority_identity.clone(),
            registry_identity: registry_identity.clone(),
            generation_root: self.generations.try_clone_token()?,
            domain: self.owner.domain,
            path,
            authority_file,
        })
    }

    pub(super) fn validate_staged(
        &self,
        registry_identity: &Arc<DirectStateRegistryIdentity>,
        staged: &StagedDirectStateGeneration,
    ) -> Result<(), String> {
        self.validate_live()?;
        if !Arc::ptr_eq(&staged.generation.registry_identity, registry_identity)
            || !Arc::ptr_eq(
                &staged.generation.roots.registry_identity,
                registry_identity,
            )
            || staged.generation.roots.domain != self.owner.domain
        {
            return Err("provider staged a generation for another registry/domain".into());
        }
        validate_same_file_identity(
            &self.generations.authority,
            &staged.generation.roots.generations.authority,
            "staged direct-state generation root",
        )?;
        let expected = resolve_generation_path(&self.generations.path, staged.section())?;
        if staged.generation.path != expected {
            return Err("provider staged outside its registered generation root".into());
        }
        staged
            .generation
            .validate_live("registered staged direct-state generation")
    }

    pub(super) fn validate_capture_source(
        &self,
        source: &DirectStateSectionSource,
    ) -> Result<(), String> {
        self.validate_live()?;
        let capture_root = source.capture_root.as_ref().ok_or_else(|| {
            "local direct-state capture omitted its pinned provider root".to_string()
        })?;
        capture_root.validate_live("direct-state capture provider root")?;
        validate_same_file_identity(
            &self.staging.authority,
            &capture_root.authority,
            "direct-state capture provider root",
        )
    }
}
