use super::{authority::*, capture::*, contract::*, filesystem::*, generation::*, *};

/// Private file produced by the shared bounded spooler.  It is not a validated
/// section token: a provider must still authenticate/open the current store format.
pub struct DirectStatePhysicalImage {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) roots: RegisteredDirectStateRoots,
    pub(super) path: PathBuf,
    pub(super) domain: DirectStateDomain,
    pub(super) manifest_sha256: String,
    pub(super) authority_file: File,
    pub(super) cleanup_on_drop: bool,
}

impl DirectStatePhysicalImage {
    /// Descriptor-backed provider view of the authenticated immutable image.
    /// Providers must never reopen `self.path`: an attacker could ABA-replace it
    /// between validation and parsing. `/proc/self/fd` (or `/dev/fd`) keeps the
    /// provider on the exact no-follow descriptor pinned by the coordinator.
    pub fn with_pinned_path<R>(
        &self,
        inspect: impl FnOnce(&Path) -> Result<R, String>,
    ) -> Result<R, String> {
        self.validate_live("direct-state provider pinned input")?;
        #[cfg(target_os = "linux")]
        let path = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}", self.authority_file.as_raw_fd()))
        };
        #[cfg(all(unix, not(target_os = "linux")))]
        let path = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/dev/fd/{}", self.authority_file.as_raw_fd()))
        };
        #[cfg(not(unix))]
        {
            let _ = inspect;
            return Err("descriptor-backed direct-state provider input is unavailable".into());
        }
        #[cfg(unix)]
        let result = inspect(&path)?;
        #[cfg(unix)]
        self.validate_live("direct-state provider input after inspection")?;
        #[cfg(unix)]
        Ok(result)
    }

    pub fn domain(&self) -> DirectStateDomain {
        self.domain
    }

    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub(super) fn validate_live(&self, label: &str) -> Result<(), String> {
        let root = if self.path.parent() == Some(self.roots.staging.path.as_path()) {
            &self.roots.staging
        } else if self.path.parent() == Some(self.roots.generations.path.as_path()) {
            &self.roots.generations
        } else {
            return Err(format!("{label} is outside its registered provider roots"));
        };
        root.validate_file(&self.path, &self.authority_file, label)
    }

    pub(super) fn validate_content(
        &self,
        manifest: &DirectStateSectionManifest,
    ) -> Result<(), String> {
        validate_file_content(
            &self.authority_file,
            manifest.logical_bytes,
            &manifest.content_sha256,
        )
    }

    pub(super) fn retain_after_commit(&mut self) {
        self.cleanup_on_drop = false;
    }
}

impl Drop for DirectStatePhysicalImage {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let root = if self.path.parent() == Some(self.roots.staging.path.as_path()) {
                &self.roots.staging
            } else if self.path.parent() == Some(self.roots.generations.path.as_path()) {
                &self.roots.generations
            } else {
                return;
            };
            if let Ok(name) = root.validate_path(&self.path, "direct-state cleanup image") {
                let _ = root.mutations().retire_unjournaled_exact(
                    name,
                    &self.authority_file,
                    "direct-state cleanup image",
                );
            }
        }
    }
}

/// Copy an already-framing-verified immutable incoming image to a distinct mutable
/// candidate. Providers strictly inspect and, for mutation stores, adopt/reanchor
/// only this copy. The incoming bytes remain unchanged for crash recovery until
/// the aggregate install journal is promoted to Current.
pub fn prepare_mutable_generation(
    permit: &StateImageInstallPermit,
    incoming: DirectStatePhysicalImage,
    source_manifest: &DirectStateSectionManifest,
    generation_directory: &Path,
) -> Result<PreparedDirectStateGeneration, String> {
    permit.validate_affinity(&incoming.authority_identity)?;
    incoming
        .roots
        .validate_generation_path(generation_directory, "direct-state prepare")?;
    if source_manifest.domain != incoming.domain
        || source_manifest.sha256()? != incoming.manifest_sha256
    {
        return Err("direct-state incoming image differs from its source manifest".into());
    }
    let prepared_name = format!(
        ".direct-state-{}-{}.prepared",
        incoming.domain.as_str(),
        incoming.manifest_sha256
    );
    let prepared_path = generation_directory.join(&prepared_name);
    if let Some(orphan) = incoming
        .roots
        .generations
        .reader()
        .open_optional_regular(&prepared_name, "pin orphan direct-state prepared image")?
    {
        incoming
            .roots
            .generations
            .mutations()
            .retire_unjournaled_exact(
                &prepared_name,
                &orphan,
                "orphan direct-state prepared image",
            )?;
    }
    incoming.validate_live("direct-state immutable input before mutable copy")?;
    let prepared_authority_file = copy_regular_file_exact(
        &incoming.authority_file,
        &incoming.roots.generations,
        &prepared_name,
        source_manifest.logical_bytes,
        &source_manifest.content_sha256,
    )?;
    incoming.roots.generations.sync()?;
    Ok(PreparedDirectStateGeneration {
        authority_identity: incoming.authority_identity.clone(),
        registry_identity: incoming.registry_identity.clone(),
        roots: incoming.roots.try_clone_token()?,
        incoming: Some(incoming),
        prepared_path,
        prepared_authority_file: Some(prepared_authority_file),
        source_manifest: source_manifest.clone(),
        cleanup_prepared_on_drop: true,
    })
}

pub struct PreparedDirectStateGeneration {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) roots: RegisteredDirectStateRoots,
    pub(super) incoming: Option<DirectStatePhysicalImage>,
    pub(super) prepared_path: PathBuf,
    pub(super) prepared_authority_file: Option<File>,
    pub(super) source_manifest: DirectStateSectionManifest,
    pub(super) cleanup_prepared_on_drop: bool,
}

impl PreparedDirectStateGeneration {
    /// Run provider inspection/adoption through the descriptor-pinned image.
    /// The filesystem name is never exposed: callers receive `/proc/self/fd/N`
    /// (or `/dev/fd/N`) for the exact no-follow descriptor retained by this
    /// token, and the original directory entry is identity-checked both before
    /// and after the operation.
    pub fn with_pinned_path<R>(
        &self,
        inspect: impl FnOnce(&Path) -> Result<R, String>,
    ) -> Result<R, String> {
        self.roots.generations.validate_file(
            &self.prepared_path,
            self.prepared_authority_file
                .as_ref()
                .ok_or_else(|| "direct-state prepared authority was consumed".to_string())?,
            "direct-state provider pinned prepared image",
        )?;
        #[cfg(target_os = "linux")]
        let path = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!(
                "/proc/self/fd/{}",
                self.prepared_authority_file
                    .as_ref()
                    .expect("validated prepared descriptor")
                    .as_raw_fd()
            ))
        };
        #[cfg(all(unix, not(target_os = "linux")))]
        let path = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!(
                "/dev/fd/{}",
                self.prepared_authority_file
                    .as_ref()
                    .expect("validated prepared descriptor")
                    .as_raw_fd()
            ))
        };
        #[cfg(not(unix))]
        {
            let _ = inspect;
            return Err("descriptor-backed prepared images are unsupported".into());
        }
        #[cfg(unix)]
        let result = inspect(&path)?;
        #[cfg(unix)]
        self.roots.generations.validate_file(
            &self.prepared_path,
            self.prepared_authority_file
                .as_ref()
                .expect("prepared descriptor remains owned"),
            "direct-state prepared image after provider inspection",
        )?;
        #[cfg(unix)]
        Ok(result)
    }
}

impl Drop for PreparedDirectStateGeneration {
    fn drop(&mut self) {
        if self.cleanup_prepared_on_drop {
            if let (Some(authority), Ok(name)) = (
                self.prepared_authority_file.as_ref(),
                self.roots
                    .generations
                    .validate_path(&self.prepared_path, "direct-state prepared cleanup image"),
            ) {
                let _ = self.roots.generations.mutations().retire_unjournaled_exact(
                    name,
                    authority,
                    "direct-state prepared cleanup image",
                );
            }
        }
    }
}

/// Bind a provider-validated/adopted mutable candidate to its final generation
/// identity. No byte digest is taken after open/adopt: allocator metadata and normal
/// writes may change immediately. Mutation-backed stores bind the incarnation-bound
/// owner-authority digest; plain stores bind their strict provider/root schema authority.
pub fn bind_prepared_generation<T: DirectStateDomainValue>(
    permit: &StateImageInstallPermit,
    mut prepared: PreparedDirectStateGeneration,
    inspected: &T,
    generation_directory: &Path,
) -> Result<StagedDirectStateGeneration, String> {
    permit.validate_affinity(&prepared.authority_identity)?;
    prepared
        .roots
        .validate_generation_path(generation_directory, "direct-state generation bind")?;
    if T::DOMAIN != prepared.source_manifest.domain {
        return Err("prepared direct-state value belongs to another domain".into());
    }
    let section = build_generation_section(&prepared, inspected)?;
    let file_name = section.generation_file.clone();
    let target = generation_directory.join(&file_name);
    let incoming = prepared
        .incoming
        .take()
        .ok_or_else(|| "direct-state prepared input was already consumed".to_string())?;
    let prepared_path = std::mem::take(&mut prepared.prepared_path);
    let prepared_authority_file = prepared
        .prepared_authority_file
        .take()
        .ok_or_else(|| "direct-state prepared authority was already consumed".to_string())?;
    prepared.cleanup_prepared_on_drop = false;
    // `prepared` owns a `Drop` impl, so the staged generation cannot move its
    // fields out. Duplicate the pinned dirfd token and bump the identity refcount
    // here, before publication: after the no-replace link is visible no fallible
    // step may remain, and `try_clone_token` is fallible.
    let staged_roots = prepared.roots.try_clone_token()?;
    let staged_registry_identity = Arc::clone(&prepared.registry_identity);
    let mut prepared_link = DirectStatePhysicalImage {
        authority_identity: prepared.authority_identity.clone(),
        registry_identity: prepared.registry_identity.clone(),
        roots: prepared.roots.try_clone_token()?,
        path: prepared_path,
        domain: section.generation_manifest.domain,
        manifest_sha256: section.generation_manifest.source_manifest_sha256.clone(),
        authority_file: prepared_authority_file,
        cleanup_on_drop: true,
    };
    prepared_link.validate_live("direct-state prepared generation")?;
    let prepared_name = prepared_link
        .roots
        .generations
        .validate_path(&prepared_link.path, "direct-state prepared generation")?
        .to_string();
    let target_authority = prepared_link
        .authority_file
        .try_clone()
        .map_err(|error| format!("clone no-replace generation authority: {error}"))?;
    let prepared_retirement = ExactRetirement::prepare(
        &prepared_link.roots.generations,
        &prepared_link.authority_file,
        &prepared_name,
        "direct-state Prepared generation link",
    )?;
    prepared_link
        .roots
        .generations
        .mutations()
        .link_no_replace(
            &prepared_name,
            &prepared_link.roots.generations,
            &file_name,
            "publish no-replace direct-state generation",
        )?;
    validate_same_file_identity(
        &prepared_link.authority_file,
        &target_authority,
        "no-replace direct-state generation",
    )?;
    prepared_link.cleanup_on_drop = false;
    drop(prepared_link);
    // The no-replace link is now visible and both source links plus the immutable
    // input are already owned by guards. No fallible path can now lose authority;
    // directory durability failure is returned in the staged retry token.
    let mut staged = StagedDirectStateGeneration {
        incoming,
        prepared_retirement: Some(prepared_retirement),
        generation: DirectStatePhysicalImage {
            authority_identity: prepared.authority_identity.clone(),
            registry_identity: staged_registry_identity,
            roots: staged_roots,
            path: target,
            domain: section.generation_manifest.domain,
            manifest_sha256: section.generation_manifest.source_manifest_sha256.clone(),
            authority_file: target_authority,
            cleanup_on_drop: true,
        },
        section,
        directory_durable: false,
        first_durability_error: None,
    };
    if let Err(error) = staged
        .generation
        .validate_live("direct-state no-replace generation")
    {
        staged.first_durability_error = Some(error);
        return Ok(staged);
    }
    if let Err(error) = staged.retry_directory_durability(permit) {
        staged.first_durability_error = Some(error);
    }
    Ok(staged)
}

fn build_generation_section<T: DirectStateDomainValue>(
    prepared: &PreparedDirectStateGeneration,
    inspected: &T,
) -> Result<DirectStateInstallSection, String> {
    let dynamic_store_authority_digest = inspected.dynamic_store_authority_digest()?;
    let install_evidence_sha256 = inspected.install_evidence_sha256()?;
    if dynamic_store_authority_digest == [0_u8; 32] || install_evidence_sha256 == [0_u8; 32] {
        return Err("prepared direct-state value returned absent authority evidence".into());
    }
    let generation_manifest = DirectStateGenerationManifest {
        schema_version: DIRECT_STATE_SCHEMA_VERSION,
        domain: prepared.source_manifest.domain,
        scope: prepared.source_manifest.scope.clone(),
        owner_manifest_sha256: prepared.source_manifest.owner_manifest_sha256.clone(),
        source_manifest_sha256: prepared.source_manifest.sha256()?,
        dynamic_store_authority_digest: Some(dynamic_store_authority_digest),
        install_evidence_sha256,
    };
    generation_manifest.validate(HARD_MAX_DIRECT_STATE_BYTES)?;
    let generation_file = generation_file_name(
        prepared.source_manifest.domain,
        &generation_manifest.sha256()?,
    )?;
    let incoming_file = prepared
        .incoming
        .as_ref()
        .ok_or_else(|| "direct-state prepared input was already consumed".to_string())?
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "direct-state incoming image name is not portable".to_string())?
        .to_string();
    Ok(DirectStateInstallSection {
        source_manifest: prepared.source_manifest.clone(),
        incoming_file,
        generation_manifest,
        generation_file,
    })
}

pub struct StagedDirectStateGeneration {
    pub(super) incoming: DirectStatePhysicalImage,
    pub(super) prepared_retirement: Option<ExactRetirement>,
    pub(super) generation: DirectStatePhysicalImage,
    pub(super) section: DirectStateInstallSection,
    pub(super) directory_durable: bool,
    pub(super) first_durability_error: Option<String>,
}

impl StagedDirectStateGeneration {
    pub fn section(&self) -> &DirectStateInstallSection {
        &self.section
    }

    /// Open the final no-replace generation through its pinned descriptor. The
    /// serving value passed to `DirectStateProviderStage::new` must be created
    /// here after binding; a handle opened on the retired `.prepared` name is not
    /// a valid Current authority.
    pub fn with_pinned_generation_path<R>(
        &self,
        inspect: impl FnOnce(&Path) -> Result<R, String>,
    ) -> Result<R, String> {
        self.generation.with_pinned_path(inspect)
    }

    pub fn first_durability_error(&self) -> Option<&str> {
        self.first_durability_error.as_deref()
    }

    pub(super) fn retry_directory_durability(
        &mut self,
        permit: &StateImageInstallPermit,
    ) -> Result<(), String> {
        permit.validate_affinity(&self.generation.authority_identity)?;
        self.generation
            .validate_live("direct-state generation before durability retry")?;
        if !self.directory_durable {
            self.generation.roots.generations.sync()?;
            self.generation
                .validate_live("direct-state generation after durability retry")?;
            self.directory_durable = true;
        }
        if let Some(retirement) = self.prepared_retirement.as_mut() {
            #[cfg(test)]
            if FAIL_PREPARED_RETIREMENT_AFTER_SYNC.with(Cell::get) {
                return Err("injected Prepared retirement failure after directory sync".into());
            }
            retirement.retry()?;
            self.prepared_retirement = None;
        }
        self.validate_journal_ready()?;
        self.first_durability_error = None;
        Ok(())
    }

    pub(super) fn validate_journal_ready(&self) -> Result<(), String> {
        if !self.directory_durable || self.prepared_retirement.is_some() {
            return Err("direct-state generation still has durability or retirement debt".into());
        }
        self.generation
            .validate_live("journal-ready direct-state generation")
    }

    pub(super) fn into_parts(
        self,
    ) -> Result<
        (
            DirectStatePhysicalImage,
            DirectStatePhysicalImage,
            DirectStateInstallSection,
        ),
        String,
    > {
        self.validate_journal_ready()?;
        Ok((self.incoming, self.generation, self.section))
    }

    pub(super) fn into_journaled_pinned(
        self,
    ) -> Result<
        (
            DirectStateInstallSection,
            DirectStatePhysicalImage,
            DirectStatePhysicalImage,
        ),
        String,
    > {
        let (mut incoming, mut generation, section) = self.into_parts()?;
        incoming.retain_after_commit();
        generation.retain_after_commit();
        Ok((section, incoming, generation))
    }
}

pub fn generation_file_name(
    domain: DirectStateDomain,
    manifest_sha256: &str,
) -> Result<String, String> {
    validate_sha256("direct-state manifest", manifest_sha256)?;
    Ok(format!("{}-{manifest_sha256}.image", domain.as_str()))
}

pub fn incoming_file_name(
    domain: DirectStateDomain,
    manifest_sha256: &str,
) -> Result<String, String> {
    validate_sha256("direct-state source manifest", manifest_sha256)?;
    Ok(format!(
        ".direct-state-{}-{manifest_sha256}.incoming",
        domain.as_str()
    ))
}

pub fn resolve_generation_path(
    generation_directory: &Path,
    section: &DirectStateInstallSection,
) -> Result<PathBuf, String> {
    validate_relative_basename(&section.generation_file)?;
    let expected = generation_file_name(
        section.generation_manifest.domain,
        &section.generation_manifest.sha256()?,
    )?;
    if section.generation_file != expected {
        return Err("direct-state generation name does not match its authority".into());
    }
    Ok(generation_directory.join(&section.generation_file))
}

pub fn resolve_incoming_path(
    incoming_directory: &Path,
    section: &DirectStateInstallSection,
) -> Result<PathBuf, String> {
    validate_relative_basename(&section.incoming_file)?;
    let expected = incoming_file_name(
        section.source_manifest.domain,
        &section.source_manifest.sha256()?,
    )?;
    if section.incoming_file != expected {
        return Err("direct-state incoming name does not match its source authority".into());
    }
    Ok(incoming_directory.join(&section.incoming_file))
}

/// Re-authenticate the immutable incoming bytes retained by an incomplete install.
/// Prepared and Current generations are mutable databases and are deliberately
/// never compared with this snapshot-time digest.
pub fn verify_pending_incoming(
    permit: &StateImageInstallPermit,
    roots: &RegisteredDirectStateRoots,
    section: &DirectStateInstallSection,
    configured_max_bytes: u64,
) -> Result<VerifiedDirectStateIncoming, String> {
    permit.validate_affinity(&roots.registry_identity.authority_identity)?;
    roots.validate_live()?;
    section.source_manifest.validate(configured_max_bytes)?;
    section.generation_manifest.validate(configured_max_bytes)?;
    if section.source_manifest.domain != section.generation_manifest.domain
        || section.source_manifest.scope != section.generation_manifest.scope
        || section.source_manifest.owner_manifest_sha256
            != section.generation_manifest.owner_manifest_sha256
        || section.source_manifest.sha256()? != section.generation_manifest.source_manifest_sha256
    {
        return Err("direct-state pending authority is internally inconsistent".into());
    }
    if roots.domain != section.source_manifest.domain {
        return Err("Pending incoming belongs to another registered domain".into());
    }
    let path = resolve_incoming_path(&roots.staging.path, section)?;
    let name = roots
        .staging
        .validate_path(&path, "Pending incoming image")?;
    let mut file = roots
        .staging
        .reader()
        .open_regular(name, "open direct-state incoming image")?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("stat direct-state incoming image: {error}"))?;
    if metadata.len() != section.source_manifest.logical_bytes {
        return Err("direct-state incoming length differs from its manifest".into());
    }
    let content_sha256 = hash_verified_incoming(&mut file, section.source_manifest.logical_bytes)?;
    if content_sha256 != section.source_manifest.content_sha256 {
        return Err("direct-state incoming content differs from its manifest".into());
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("rewind verified direct-state incoming image: {error}"))?;
    roots
        .staging
        .validate_file(&path, &file, "verified direct-state incoming image")?;
    Ok(VerifiedDirectStateIncoming {
        authority_identity: permit.authority_identity.clone(),
        registry_identity: roots.registry_identity.clone(),
        roots: roots.try_clone_token()?,
        path,
        authority_file: file,
        source_manifest: section.source_manifest.clone(),
    })
}

fn hash_verified_incoming(file: &mut File, logical_bytes: u64) -> Result<String, String> {
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; MAX_DIRECT_STATE_CHUNK_BYTES];
    let mut observed = 0_u64;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| format!("hash direct-state incoming image: {error}"))?;
        if count == 0 {
            break;
        }
        observed = observed
            .checked_add(count as u64)
            .ok_or_else(|| "direct-state incoming length overflow".to_string())?;
        if observed > logical_bytes {
            return Err("direct-state incoming image grew during verification".into());
        }
        hasher.update(&buffer[..count]);
    }
    if observed != logical_bytes {
        return Err("direct-state incoming content differs from its manifest".into());
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Non-Clone proof that retained immutable incoming bytes match their authenticated
/// source manifest. Provider-specific inspection/adoption occurs only on a copy.
pub struct VerifiedDirectStateIncoming {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) roots: RegisteredDirectStateRoots,
    pub(super) path: PathBuf,
    pub(super) authority_file: File,
    pub(super) source_manifest: DirectStateSectionManifest,
}

impl VerifiedDirectStateIncoming {
    /// Reject a token minted by a different registry. The identity was already
    /// retained here; without this proof nothing compared it.
    pub fn validate_registry_affinity(
        &self,
        expected: &Arc<DirectStateRegistryIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.registry_identity, expected) {
            Ok(())
        } else {
            Err("verified direct-state incoming image belongs to another registry".into())
        }
    }

    /// Pinned no-follow descriptor for provider-specific inspection. Providers
    /// must not reopen the path after verification.
    pub fn authority_file(&self, permit: &StateImageInstallPermit) -> Result<&File, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_live(permit)?;
        Ok(&self.authority_file)
    }

    pub fn source_manifest(&self) -> &DirectStateSectionManifest {
        &self.source_manifest
    }

    pub fn validate_live(&self, permit: &StateImageInstallPermit) -> Result<(), String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.roots.staging.validate_file(
            &self.path,
            &self.authority_file,
            "verified direct-state incoming image",
        )
    }
}

/// Registry-minted proof that provider recovery is opening the exact generation
/// basename beneath its pinned, independently configured generation root.
pub struct VerifiedDirectStateGeneration {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) generation_root: PinnedPrivateDirectory,
    pub(super) domain: DirectStateDomain,
    pub(super) path: PathBuf,
    pub(super) authority_file: File,
}

impl VerifiedDirectStateGeneration {
    /// Reject a token minted by a different registry. The identity was already
    /// retained here; without this proof nothing compared it.
    pub fn validate_registry_affinity(
        &self,
        expected: &Arc<DirectStateRegistryIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.registry_identity, expected) {
            Ok(())
        } else {
            Err("verified direct-state generation belongs to another registry".into())
        }
    }

    pub fn with_pinned_path<R>(
        &self,
        permit: &StateImageInstallPermit,
        inspect: impl FnOnce(&Path) -> Result<R, String>,
    ) -> Result<R, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_live()?;
        #[cfg(target_os = "linux")]
        let path = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}", self.authority_file.as_raw_fd()))
        };
        #[cfg(all(unix, not(target_os = "linux")))]
        let path = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/dev/fd/{}", self.authority_file.as_raw_fd()))
        };
        #[cfg(not(unix))]
        {
            let _ = inspect;
            return Err("descriptor-backed generation recovery is unsupported".into());
        }
        #[cfg(unix)]
        let result = inspect(&path)?;
        #[cfg(unix)]
        self.validate_live()?;
        #[cfg(unix)]
        Ok(result)
    }

    pub fn domain(&self) -> DirectStateDomain {
        self.domain
    }

    pub(super) fn validate_live(&self) -> Result<(), String> {
        self.generation_root
            .validate_live("verified direct-state generation root")?;
        self.generation_root.validate_file(
            &self.path,
            &self.authority_file,
            "verified direct-state generation",
        )
    }
}
