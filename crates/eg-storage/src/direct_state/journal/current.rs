use super::*;

impl OpenedCurrentFence {
    pub(in crate::direct_state) fn from_image(
        image: &DirectStateCurrentImage,
    ) -> Result<Self, String> {
        image.validate()?;
        Ok(Self {
            sha256: image.sha256()?,
            scope: image.scope.clone(),
        })
    }
}

pub(in crate::direct_state) fn read_current_image(
    permit: &StateImageInstallPermit,
    registry_identity: &Arc<DirectStateRegistryIdentity>,
    root: &PinnedPrivateDirectory,
    path: &Path,
) -> Result<Option<DurableCurrentImage>, String> {
    let _ = permit;
    let name = root.validate_path(path, "direct-state Current image")?;
    let mut file = match root
        .reader()
        .open_optional_regular(name, "open direct-state current image")?
    {
        Some(file) => file,
        None => return Ok(None),
    };
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_DIRECT_STATE_MANIFEST_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read direct-state current image: {error}"))?;
    if bytes.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
        return Err("direct-state current image exceeds its byte bound".into());
    }
    let current: DirectStateCurrentImage =
        decode_canonical_durable_record(&bytes, "direct-state Current image")?;
    current.validate()?;
    root.sync()?;
    let durable = DurableCurrentImage {
        authority_identity: permit.authority_identity.clone(),
        registry_identity: registry_identity.clone(),
        image: current,
        path: path.to_path_buf(),
        authority_file: file,
        root: root.try_clone_token()?,
    };
    durable.validate_live()?;
    Ok(Some(durable))
}

/// Atomically advance the restart pointer only after every provider has published
/// under the closed state-image gate.  A crash before pointer rename leaves the old
/// current image plus a Published journal to roll forward; a crash after rename
/// leaves matching current+journal and recovery only has to remove the journal.
pub(in crate::direct_state) fn promote_published_install_to_current(
    permit: &StateImageInstallPermit,
    registry_identity: &Arc<DirectStateRegistryIdentity>,
    journal_root: &PinnedPrivateDirectory,
    current_root: &PinnedPrivateDirectory,
    journal_path: &Path,
    current_path: &Path,
    published: &DurablePublishedJournal,
) -> Result<CurrentPointerPublication, String> {
    permit.validate_affinity(&published.authority_identity)?;
    published.validate_registry_affinity(registry_identity)?;
    published.validate_live()?;
    if published.path != journal_path {
        return Err("direct-state install journal is not the expected published image".into());
    }
    let current_name =
        validate_promotion_paths(journal_root, current_root, journal_path, current_path)?;
    let current = current_image_from_journal(&published.journal)?;
    let previous_current =
        read_current_image(permit, registry_identity, current_root, current_path)?;
    let published_token = published.try_clone_token()?;
    if let Some(existing) = previous_current.as_ref() {
        if existing.image == current {
            return replay_current_publication(
                permit,
                registry_identity,
                current_root,
                current_path,
                published_token,
            );
        }
        validate_current_successor(existing, &current)?;
    }
    current_root
        .collector()
        .collect_temporaries(&current_name)?;
    let bytes = rmp_serde::to_vec_named(&current)
        .map_err(|error| format!("encode direct-state current image: {error}"))?;
    let (authority_file, next_name) =
        current_root
            .mutations()
            .write_temporary(&current_name, "current", &bytes)?;
    if let Some(existing) = previous_current.as_ref() {
        existing.validate_live()?;
    }
    let displaced_authority = previous_current
        .as_ref()
        .map(|existing| {
            existing
                .authority_file
                .try_clone()
                .map_err(|error| format!("clone prior Current authority: {error}"))
        })
        .transpose()?;
    let displaced_retirement = displaced_authority
        .as_ref()
        .map(|expected| {
            ExactRetirement::prepare(
                current_root,
                expected,
                &next_name,
                "displaced Current image",
            )
        })
        .transpose()?;
    let current_root_token = current_root.try_clone_token()?;
    let visibility = if displaced_authority.is_some() {
        current_root.mutations().exchange(
            &next_name,
            &current_name,
            "publish direct-state Current image",
        )
    } else {
        current_root.mutations().rename_no_replace(
            &next_name,
            &current_name,
            "publish first direct-state Current image",
        )
    };
    if let Err(error) = visibility {
        let _ = current_root.mutations().retire_unjournaled_exact(
            &next_name,
            &authority_file,
            "failed Current temporary",
        );
        return Err(error);
    }
    let durable = DurableCurrentImage {
        authority_identity: published.authority_identity.clone(),
        registry_identity: registry_identity.clone(),
        image: current,
        path: current_path.to_path_buf(),
        authority_file,
        root: current_root_token,
    };
    Ok(finish_current_publication(
        current_root,
        &next_name,
        displaced_authority.as_ref(),
        durable,
        published_token,
        displaced_retirement,
    ))
}

fn validate_promotion_paths(
    journal_root: &PinnedPrivateDirectory,
    current_root: &PinnedPrivateDirectory,
    journal_path: &Path,
    current_path: &Path,
) -> Result<String, String> {
    let current_name = current_root
        .validate_path(current_path, "direct-state Current image")?
        .to_string();
    journal_root.validate_path(journal_path, "direct-state Published journal")?;
    validate_distinct_authority_paths(journal_path, current_path)?;
    Ok(current_name)
}

fn replay_current_publication(
    permit: &StateImageInstallPermit,
    registry_identity: &Arc<DirectStateRegistryIdentity>,
    current_root: &PinnedPrivateDirectory,
    current_path: &Path,
    mut published: DurablePublishedJournal,
) -> Result<CurrentPointerPublication, String> {
    let current = read_current_image(permit, registry_identity, current_root, current_path)?
        .ok_or_else(|| "exact direct-state Current replay disappeared".to_string())?;
    match published.retire_exact() {
        Ok(()) => Ok(CurrentPointerPublication::Durable(current)),
        Err(first_error) => Ok(CurrentPointerPublication::RecoveryRequired(
            CurrentDurabilityRecovery {
                current,
                published,
                displaced_retirement: None,
                first_error,
            },
        )),
    }
}

fn validate_current_successor(
    previous: &DurableCurrentImage,
    current: &DirectStateCurrentImage,
) -> Result<(), String> {
    let previous = OpenedCurrentFence::from_image(previous.image())?;
    let candidate = OpenedCurrentFence::from_image(current)?;
    candidate.validate_successor_of(&previous)
}

fn finish_current_publication(
    current_root: &PinnedPrivateDirectory,
    displaced_name: &str,
    displaced_authority: Option<&File>,
    current: DurableCurrentImage,
    mut published: DurablePublishedJournal,
    mut displaced_retirement: Option<ExactRetirement>,
) -> CurrentPointerPublication {
    let durability = (|| {
        if let Some(expected) = displaced_authority {
            current_root
                .reader()
                .open_regular(displaced_name, "pin displaced Current image")
                .and_then(|actual| {
                    validate_same_file_identity(expected, &actual, "displaced Current image")
                })?;
        }
        current.validate_live()?;
        current_root.sync()?;
        if let Some(retirement) = displaced_retirement.as_mut() {
            retirement.retry()?;
            displaced_retirement = None;
        }
        Ok::<(), String>(())
    })();
    if let Err(first_error) = durability {
        return CurrentPointerPublication::RecoveryRequired(CurrentDurabilityRecovery {
            current,
            published,
            displaced_retirement,
            first_error,
        });
    }
    // Current is durable. Retiring the superseded Published journal is
    // idempotent cleanup debt and cannot turn committed publication into Err.
    if let Err(first_error) = published.retire_exact() {
        return CurrentPointerPublication::RecoveryRequired(CurrentDurabilityRecovery {
            current,
            published,
            displaced_retirement,
            first_error,
        });
    }
    CurrentPointerPublication::Durable(current)
}

pub(in crate::direct_state) enum CurrentPointerPublication {
    Durable(DurableCurrentImage),
    /// The rename is already visible and therefore must never unwind as an
    /// ordinary error. This token pins that exact Current inode until its parent
    /// directory fsync is retried.
    RecoveryRequired(CurrentDurabilityRecovery),
}

pub struct CurrentDurabilityRecovery {
    pub(in crate::direct_state) current: DurableCurrentImage,
    pub(in crate::direct_state) published: DurablePublishedJournal,
    pub(in crate::direct_state) displaced_retirement: Option<ExactRetirement>,
    pub(in crate::direct_state) first_error: String,
}

impl CurrentDurabilityRecovery {
    pub fn first_error(&self) -> &str {
        &self.first_error
    }
}

/// Non-Clone proof that the exact aggregate Current pointer is durably published.
pub struct DurableCurrentImage {
    pub(in crate::direct_state) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(in crate::direct_state) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(in crate::direct_state) image: DirectStateCurrentImage,
    pub(in crate::direct_state) path: PathBuf,
    pub(in crate::direct_state) authority_file: File,
    pub(in crate::direct_state) root: PinnedPrivateDirectory,
}

pub enum CurrentPromotion {
    Durable(DurableCurrentImage),
    DurabilityRecoveryRequired(CurrentDurabilityRecovery),
    CleanupRecoveryRequired(CurrentCleanupRecovery),
}

pub struct CurrentCleanupRecovery {
    pub(in crate::direct_state) current: DurableCurrentImage,
    pub(in crate::direct_state) first_error: String,
}

pub enum PendingAbandonment {
    Durable(AssembledDirectStateGeneration),
    /// The Pending name is already retired. The token keeps the last-known-good
    /// Current authority and recovered generation alive while directory fsync or
    /// bounded artifact cleanup is retried.
    RecoveryRequired(PendingAbandonRecovery),
}

pub struct PendingAbandonRecovery {
    pub(in crate::direct_state) current: DurableCurrentImage,
    pub(in crate::direct_state) assembled: AssembledDirectStateGeneration,
    pub(in crate::direct_state) journal_root: PinnedPrivateDirectory,
    pub(in crate::direct_state) pending_authority: File,
    pub(in crate::direct_state) live_name: String,
    pub(in crate::direct_state) quarantine_name: String,
    pub(in crate::direct_state) retirement_complete: bool,
    pub(in crate::direct_state) first_error: String,
}

impl PendingAbandonRecovery {
    pub fn first_error(&self) -> &str {
        &self.first_error
    }
}

impl CurrentCleanupRecovery {
    pub fn first_error(&self) -> &str {
        &self.first_error
    }
}

/// Non-Clone proof that one complete generation was assembled, atomically
/// published, and bound to the exact durable Current pointer under this authority.
pub struct DirectStateRecoveryCompletion {
    pub(in crate::direct_state) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(in crate::direct_state) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(in crate::direct_state) generation: Arc<DirectStateGeneration>,
    pub(in crate::direct_state) current_sha256: String,
    pub(in crate::direct_state) authority_epoch: u64,
}

impl std::fmt::Debug for DirectStateRecoveryCompletion {
    /// The authority, registry and generation handles are deliberately withheld:
    /// this is a non-Clone proof token and its `Debug` must not become a way to
    /// observe or reconstruct the authority it proves.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectStateRecoveryCompletion")
            .field("current_sha256", &self.current_sha256)
            .field("authority_epoch", &self.authority_epoch)
            .finish_non_exhaustive()
    }
}

impl DirectStateRecoveryCompletion {
    pub(in crate::direct_state) fn validate(
        &self,
        authority_identity: &Arc<StateImageAuthorityIdentity>,
        current: &DurableCurrentImage,
        published_generation: &Arc<DirectStateGeneration>,
    ) -> Result<(), String> {
        if !Arc::ptr_eq(&self.authority_identity, authority_identity)
            || !Arc::ptr_eq(&current.authority_identity, authority_identity)
            || !Arc::ptr_eq(&self.registry_identity, &current.registry_identity)
            || !Arc::ptr_eq(&self.generation, published_generation)
            || self.current_sha256 != current.image.sha256()?
            || self.authority_epoch != current.image.scope.authority_epoch()
        {
            return Err("direct-state recovery completion belongs to another authority".into());
        }
        Ok(())
    }
}

impl DurableCurrentImage {
    pub fn image(&self) -> &DirectStateCurrentImage {
        &self.image
    }

    pub(in crate::direct_state) fn validate_live(&self) -> Result<(), String> {
        self.image.validate()?;
        self.root
            .validate_file(&self.path, &self.authority_file, "durable Current image")?;
        validate_canonical_durable_record(
            &self.authority_file,
            &self.image,
            "durable Current image",
        )
    }

    pub(in crate::direct_state) fn validate_affinity(
        &self,
        expected: &Arc<StateImageAuthorityIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.authority_identity, expected) {
            Ok(())
        } else {
            Err("durable Current image belongs to another authority".into())
        }
    }
    pub(in crate::direct_state) fn validate_registry_affinity(
        &self,
        expected: &Arc<DirectStateRegistryIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.registry_identity, expected) {
            Ok(())
        } else {
            Err("durable Current image belongs to another registry".into())
        }
    }

    pub(in crate::direct_state) fn try_clone_token(&self) -> Result<Self, String> {
        Ok(Self {
            authority_identity: self.authority_identity.clone(),
            registry_identity: self.registry_identity.clone(),
            image: self.image.clone(),
            path: self.path.clone(),
            authority_file: self
                .authority_file
                .try_clone()
                .map_err(|error| format!("clone durable Current authority: {error}"))?,
            root: self.root.try_clone_token()?,
        })
    }
}
