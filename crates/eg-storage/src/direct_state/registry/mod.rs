use super::{
    authority::*, capture::*, contract::*, filesystem::*, generation::*, image::*, journal::*,
    provider::*, transport::*, *,
};

mod intake;
mod publication;
mod recovery;

pub use intake::{
    StagedDirectStateSection, StagedWholeGeneration, ValidatedDirectStateSection,
    ValidatedWholeGeneration,
};
pub use recovery::{RecoveredDirectStateGeneration, RecoveredPublishedCleanup};

pub struct DirectStateRegistry {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) current_slot: Arc<SyncRwLock<Option<Arc<DirectStateGeneration>>>>,
    pub(super) journal_path: PathBuf,
    pub(super) current_path: PathBuf,
    pub(super) journal_root: PinnedPrivateDirectory,
    pub(super) current_root: PinnedPrivateDirectory,
    pub(super) providers: BTreeMap<DirectStateDomain, DirectStateProviderRegistration>,
}

impl DirectStateRegistry {
    pub(super) fn validate_registered_owner(
        &self,
        domain: DirectStateDomain,
        owner_manifest_sha256: &str,
    ) -> Result<(), String> {
        let registration = self
            .providers
            .get(&domain)
            .ok_or_else(|| format!("direct-state provider is not registered for {domain:?}"))?;
        registration.validate_live()?;
        if registration.owner.sha256()? != owner_manifest_sha256 {
            return Err("direct-state owner layout differs from its registry binding".into());
        }
        Ok(())
    }
    pub(super) fn validate_filesystem(&self) -> Result<(), String> {
        self.journal_root
            .validate_live("direct-state journal authority root")?;
        self.current_root
            .validate_live("direct-state Current authority root")?;
        for registration in self.providers.values() {
            registration.validate_live()?;
        }
        Ok(())
    }

    pub(super) fn validate_authority_paths(
        &self,
        journal_path: &Path,
        current_path: &Path,
    ) -> Result<(), String> {
        self.validate_filesystem()?;
        if journal_path != self.journal_path || current_path != self.current_path {
            return Err("direct-state durable paths belong to another registry".into());
        }
        Ok(())
    }

    pub fn new(
        authority: &StateImageAuthority,
        journal_path: &Path,
        current_path: &Path,
        contract: DirectStateRegistryContract,
        providers: impl IntoIterator<Item = Arc<dyn DirectStateProvider>>,
    ) -> Result<Self, String> {
        ensure_direct_state_platform_supported()?;
        let contract_sha256 = contract.sha256()?;
        validate_distinct_authority_paths(journal_path, current_path)?;
        let by_domain = bind_providers(&contract, providers)?;
        let (journal_root, current_root) = open_durable_roots(journal_path, current_path)?;
        Ok(Self {
            authority_identity: authority.identity.clone(),
            registry_identity: Arc::new(DirectStateRegistryIdentity {
                authority_identity: authority.identity.clone(),
                contract_sha256,
            }),
            current_slot: authority.current.clone(),
            journal_path: journal_path.to_path_buf(),
            current_path: current_path.to_path_buf(),
            journal_root,
            current_root,
            providers: by_domain,
        })
    }

    pub fn missing_domains(&self) -> Vec<DirectStateDomain> {
        DirectStateDomain::ALL
            .into_iter()
            .filter(|domain| !self.providers.contains_key(domain))
            .collect()
    }

    pub fn require_complete(&self) -> Result<(), String> {
        let missing = self.missing_domains();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "direct-state registry is incomplete; missing {missing:?}"
            ))
        }
    }

    pub fn domains(&self) -> impl Iterator<Item = DirectStateDomain> + '_ {
        DirectStateDomain::ALL
            .into_iter()
            .filter(|domain| self.providers.contains_key(domain))
    }

    pub(super) fn provider(
        &self,
        domain: DirectStateDomain,
    ) -> Result<&Arc<dyn DirectStateProvider>, String> {
        let registration = self
            .providers
            .get(&domain)
            .ok_or_else(|| format!("direct-state provider is not registered for {domain:?}"))?;
        registration.validate_live()?;
        Ok(&registration.provider)
    }
}

fn bind_providers(
    contract: &DirectStateRegistryContract,
    providers: impl IntoIterator<Item = Arc<dyn DirectStateProvider>>,
) -> Result<BTreeMap<DirectStateDomain, DirectStateProviderRegistration>, String> {
    let mut by_domain = BTreeMap::new();
    for provider in providers {
        let domain = provider.domain();
        let declared_owner = provider.owner_manifest()?;
        let owner = contract.owner(domain)?.clone();
        if declared_owner != owner {
            return Err("direct-state provider owner differs from registry contract".into());
        }
        owner.sha256()?;
        let registration = DirectStateProviderRegistration {
            staging: PinnedPrivateDirectory::open(provider.staging_directory())?,
            generations: PinnedPrivateDirectory::open(provider.generation_directory())?,
            provider,
            owner,
        };
        registration.validate_live()?;
        if by_domain.insert(domain, registration).is_some() {
            return Err(format!(
                "multiple direct-state providers registered for {domain:?}"
            ));
        }
    }
    let missing: Vec<_> = DirectStateDomain::ALL
        .into_iter()
        .filter(|domain| !by_domain.contains_key(domain))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "direct-state registry is incomplete; missing {missing:?}"
        ));
    }
    let mut value_types = HashSet::new();
    if by_domain
        .values()
        .any(|registration| !value_types.insert(registration.provider.value_type_id()))
    {
        return Err("direct-state provider value types must be unique by domain".into());
    }
    Ok(by_domain)
}

fn open_durable_roots(
    journal_path: &Path,
    current_path: &Path,
) -> Result<(PinnedPrivateDirectory, PinnedPrivateDirectory), String> {
    let journal_parent = journal_path
        .parent()
        .ok_or_else(|| "direct-state journal has no parent".to_string())?;
    let current_parent = current_path
        .parent()
        .ok_or_else(|| "direct-state Current has no parent".to_string())?;
    ensure_private_directory(journal_parent)?;
    ensure_private_directory(current_parent)?;
    let journal_root = PinnedPrivateDirectory::open(journal_parent)?;
    let current_root = PinnedPrivateDirectory::open(current_parent)?;
    Ok((journal_root, current_root))
}
