use super::*;

/// Non-Clone proof that the exact Prepared journal was fsynced and atomically
/// linked at its final no-replace path.
pub struct DurablePreparedJournal {
    pub(in crate::direct_state) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(in crate::direct_state) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(in crate::direct_state) journal: DirectStateInstallJournal,
    pub(in crate::direct_state) path: PathBuf,
    pub(in crate::direct_state) authority_file: File,
    pub(in crate::direct_state) root: PinnedPrivateDirectory,
}

impl DurablePreparedJournal {
    pub fn journal(&self) -> &DirectStateInstallJournal {
        &self.journal
    }

    pub(in crate::direct_state) fn validate_live(&self) -> Result<(), String> {
        self.root
            .validate_file(&self.path, &self.authority_file, "durable Prepared journal")?;
        validate_canonical_durable_record(
            &self.authority_file,
            &self.journal,
            "durable Prepared journal",
        )
    }

    pub(in crate::direct_state) fn validate_affinity(
        &self,
        expected: &Arc<StateImageAuthorityIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.authority_identity, expected) {
            Ok(())
        } else {
            Err("durable Prepared journal belongs to another authority".into())
        }
    }

    pub(in crate::direct_state) fn validate_registry_affinity(
        &self,
        expected: &Arc<DirectStateRegistryIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.registry_identity, expected) {
            Ok(())
        } else {
            Err("durable Prepared journal belongs to another registry".into())
        }
    }

    pub(in crate::direct_state) fn from_authority(
        authority_identity: Arc<StateImageAuthorityIdentity>,
        registry_identity: Arc<DirectStateRegistryIdentity>,
        path: &Path,
        journal: DirectStateInstallJournal,
        authority_file: File,
        root: PinnedPrivateDirectory,
    ) -> Result<Self, String> {
        if journal.phase != DirectStateInstallPhase::Prepared {
            return Err("durable Prepared journal has the wrong phase".into());
        }
        let durable = Self {
            authority_identity,
            registry_identity,
            journal,
            path: path.to_path_buf(),
            authority_file,
            root,
        };
        durable.validate_live()?;
        Ok(durable)
    }
}

pub enum PreparedJournalPublication {
    Durable(PreparedWholeGeneration),
    /// The final no-replace name exists but its directory durability could not be
    /// proven. Files have been retained and serving remains closed; recovery must
    /// retry the parent fsync/read before any publication.
    RecoveryRequired(PreparedJournalRecovery),
    /// At least one visible generation link still needs its parent-directory
    /// durability established. The complete staged set remains owned and may be
    /// retried; no aggregate journal has been made visible.
    GenerationRecoveryRequired(PreparedGenerationRecovery),
}

pub struct PreparedGenerationRecovery {
    pub(in crate::direct_state) staged: StagedWholeGeneration,
    pub(in crate::direct_state) journal_path: PathBuf,
    pub(in crate::direct_state) journal: DirectStateInstallJournal,
    pub(in crate::direct_state) first_error: String,
}

impl PreparedGenerationRecovery {
    pub fn first_error(&self) -> &str {
        &self.first_error
    }

    pub fn retry(
        self,
        registry: &DirectStateRegistry,
        permit: &StateImageInstallPermit,
    ) -> Result<PreparedJournalPublication, String> {
        registry.write_prepared_journal_no_replace(
            permit,
            &self.journal_path,
            self.staged,
            &self.journal,
        )
    }
}

pub struct PreparedJournalRecovery {
    pub(in crate::direct_state) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(in crate::direct_state) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(in crate::direct_state) journal: DirectStateInstallJournal,
    pub(in crate::direct_state) path: PathBuf,
    pub(in crate::direct_state) authority_file: File,
    pub(in crate::direct_state) root: PinnedPrivateDirectory,
    pub(in crate::direct_state) temporary_retirement: Option<ExactRetirement>,
    pub(in crate::direct_state) prepared: VisiblePreparedState,
    pub(in crate::direct_state) first_error: String,
}

impl PreparedJournalRecovery {
    pub fn first_error(&self) -> &str {
        &self.first_error
    }

    pub fn retry_durability(
        mut self,
        registry: &DirectStateRegistry,
        permit: &StateImageInstallPermit,
    ) -> Result<PreparedWholeGeneration, PreparedJournalRecovery> {
        let attempt = (|| {
            permit.validate_affinity(&self.authority_identity)?;
            registry.validate_filesystem()?;
            if !Arc::ptr_eq(&self.registry_identity, &registry.registry_identity)
                || self.path != registry.journal_path
            {
                return Err("Prepared durability recovery belongs to another registry".into());
            }
            self.root.validate_file(
                &self.path,
                &self.authority_file,
                "visible Prepared journal",
            )?;
            self.prepared.validate_live()?;
            self.root.sync()?;
            if let Some(retirement) = self.temporary_retirement.as_mut() {
                retirement.retry()?;
                self.temporary_retirement = None;
            }
            self.root.sync()?;
            self.root.validate_file(
                &self.path,
                &self.authority_file,
                "durable Prepared journal",
            )?;
            self.prepared.validate_live()
        })();
        if let Err(first_error) = attempt {
            self.first_error = first_error;
            return Err(self);
        }
        let durable = DurablePreparedJournal {
            authority_identity: self.authority_identity,
            registry_identity: self.registry_identity,
            journal: self.journal,
            path: self.path,
            authority_file: self.authority_file,
            root: self.root,
        };
        Ok(PreparedWholeGeneration {
            journal: durable,
            assembled: self.prepared.assembled,
        })
    }
}

/// Complete in-process authority retained after the Prepared journal becomes
/// visible but before its parent-directory durability is proven. The file guards
/// pin every journal-referenced Incoming and generation inode; `assembled` keeps
/// the exact provider values needed to continue the same install after retry.
pub(in crate::direct_state) struct VisiblePreparedState {
    pub(in crate::direct_state) assembled: AssembledDirectStateGeneration,
    pub(in crate::direct_state) retained_images:
        Vec<(DirectStatePhysicalImage, DirectStatePhysicalImage)>,
}

impl VisiblePreparedState {
    pub(in crate::direct_state) fn validate_live(&self) -> Result<(), String> {
        for (incoming, generation) in &self.retained_images {
            incoming.validate_live("journaled direct-state Incoming")?;
            generation.validate_live("journaled direct-state generation")?;
        }
        Ok(())
    }
}

pub(in crate::direct_state) fn read_install_journal(
    permit: &StateImageInstallPermit,
    root: &PinnedPrivateDirectory,
    path: &Path,
) -> Result<Option<DirectStateInstallJournal>, String> {
    let _ = permit;
    Ok(read_install_journal_authority(root, path)?.map(|(journal, _file)| journal))
}

pub(in crate::direct_state) fn read_install_journal_authority(
    root: &PinnedPrivateDirectory,
    path: &Path,
) -> Result<Option<(DirectStateInstallJournal, File)>, String> {
    let name = root.validate_path(path, "direct-state install journal")?;
    let mut file = match root
        .reader()
        .open_optional_regular(name, "open direct-state install journal")?
    {
        Some(file) => file,
        None => return Ok(None),
    };
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_DIRECT_STATE_MANIFEST_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read direct-state install journal: {error}"))?;
    if bytes.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
        return Err("direct-state install journal exceeds its byte bound".into());
    }
    let journal: DirectStateInstallJournal =
        decode_canonical_durable_record(&bytes, "direct-state install journal")?;
    journal.validate()?;
    Ok(Some((journal, file)))
}

pub(in crate::direct_state) fn load_prepared_journal(
    permit: &StateImageInstallPermit,
    registry_identity: &Arc<DirectStateRegistryIdentity>,
    root: &PinnedPrivateDirectory,
    path: &Path,
) -> Result<Option<DurablePreparedJournal>, String> {
    let _ = permit;
    let Some((journal, authority_file)) = read_install_journal_authority(root, path)? else {
        return Ok(None);
    };
    root.sync()?;
    DurablePreparedJournal::from_authority(
        permit.authority_identity.clone(),
        registry_identity.clone(),
        path,
        journal,
        authority_file,
        root.try_clone_token()?,
    )
    .map(Some)
}

pub(in crate::direct_state) fn replace_install_journal(
    permit: &StateImageInstallPermit,
    root: &PinnedPrivateDirectory,
    path: &Path,
    prepared: &PreparedWholeGeneration,
    replacement: &DirectStateInstallJournal,
) -> Result<PublishedJournalPublication, String> {
    let expected = &prepared.journal;
    validate_install_transition(permit, path, prepared, replacement)?;
    let current = read_install_journal(permit, root, path)?
        .ok_or_else(|| "direct-state install journal is missing".to_string())?;
    if &current == replacement {
        return load_existing_published(permit, root, path, prepared, replacement);
    }
    expected.validate_live()?;
    if current != expected.journal {
        return Err("direct-state install journal changed before transition".into());
    }
    expected.validate_live()?;
    let journal_name = root
        .validate_path(path, "direct-state install journal")?
        .to_string();
    root.collector().collect_temporaries(&journal_name)?;
    let bytes = rmp_serde::to_vec_named(replacement)
        .map_err(|error| format!("encode direct-state install journal: {error}"))?;
    let (authority_file, next_name) =
        root.mutations()
            .write_temporary(&journal_name, "transition", &bytes)?;
    let displaced_authority = expected
        .authority_file
        .try_clone()
        .map_err(|error| format!("clone Prepared journal before transition: {error}"))?;
    let mut displaced_retirement = ExactRetirement::prepare(
        root,
        &displaced_authority,
        &next_name,
        "displaced Prepared journal",
    )?;
    let published_root = root.try_clone_token()?;
    if let Err(error) = root.mutations().exchange(
        &next_name,
        &journal_name,
        "publish direct-state install journal",
    ) {
        let _ = root.mutations().retire_unjournaled_exact(
            &next_name,
            &authority_file,
            "failed Published journal temporary",
        );
        return Err(error);
    }
    let durable = DurablePublishedJournal {
        authority_identity: prepared.assembled.authority_identity.clone(),
        registry_identity: prepared.assembled.registry_identity.clone(),
        journal: replacement.clone(),
        path: path.to_path_buf(),
        authority_file,
        root: published_root,
        retirement_quarantine: None,
        retirement_complete: false,
    };
    let displaced_result = root
        .reader()
        .open_regular(&next_name, "pin displaced Prepared journal")
        .and_then(|actual| {
            validate_same_file_identity(&displaced_authority, &actual, "displaced Prepared journal")
        });
    if let Err(first_error) = displaced_result {
        return Ok(PublishedJournalPublication::RecoveryRequired(
            PublishedJournalRecovery {
                durable,
                displaced_retirement: Some(displaced_retirement),
                first_error,
            },
        ));
    }
    match root.sync().and_then(|_| displaced_retirement.retry()) {
        Ok(()) => Ok(PublishedJournalPublication::Durable(durable)),
        Err(error) => Ok(PublishedJournalPublication::RecoveryRequired(
            PublishedJournalRecovery {
                durable,
                displaced_retirement: Some(displaced_retirement),
                first_error: error,
            },
        )),
    }
}

fn validate_install_transition(
    permit: &StateImageInstallPermit,
    path: &Path,
    prepared: &PreparedWholeGeneration,
    replacement: &DirectStateInstallJournal,
) -> Result<(), String> {
    let expected = &prepared.journal;
    permit.validate_affinity(&prepared.assembled.authority_identity)?;
    expected.validate_registry_affinity(&prepared.assembled.registry_identity)?;
    prepared.assembled.validate_for_journal(&expected.journal)?;
    if expected.path != path {
        return Err("Prepared journal token belongs to a different path".into());
    }
    if expected.journal.phase != DirectStateInstallPhase::Prepared
        || replacement.phase != DirectStateInstallPhase::Published
        || expected.journal.snapshot_sha256 != replacement.snapshot_sha256
        || expected.journal.scope != replacement.scope
        || expected.journal.sections != replacement.sections
    {
        return Err("invalid direct-state install journal transition".into());
    }
    replacement.validate()
}

fn load_existing_published(
    permit: &StateImageInstallPermit,
    root: &PinnedPrivateDirectory,
    path: &Path,
    prepared: &PreparedWholeGeneration,
    replacement: &DirectStateInstallJournal,
) -> Result<PublishedJournalPublication, String> {
    root.sync()?;
    let durable =
        load_published_journal(permit, &prepared.assembled.registry_identity, root, path)?
            .ok_or_else(|| "Published journal disappeared during durability retry".to_string())?;
    if &durable.journal != replacement {
        return Err("Published journal changed during durability retry".into());
    }
    Ok(PublishedJournalPublication::Durable(durable))
}

pub enum PublishedJournalPublication {
    Durable(DurablePublishedJournal),
    /// Rename made the Published authority visible, but its directory durability
    /// is not yet proven. The pinned replacement remains owned for exact retry.
    RecoveryRequired(PublishedJournalRecovery),
}

pub struct PublishedJournalRecovery {
    pub(in crate::direct_state) durable: DurablePublishedJournal,
    pub(in crate::direct_state) displaced_retirement: Option<ExactRetirement>,
    pub(in crate::direct_state) first_error: String,
}

impl std::fmt::Debug for PublishedJournalRecovery {
    /// The durable journal and the retirement debt own live descriptors, so
    /// `Debug` reports only the retry diagnosis: why durability failed and
    /// whether a displaced inode is still owed a retirement.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublishedJournalRecovery")
            .field("first_error", &self.first_error)
            .field(
                "displaced_retirement_pending",
                &self.displaced_retirement.is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl PublishedJournalRecovery {
    pub fn first_error(&self) -> &str {
        &self.first_error
    }

    pub fn retry_durability(
        mut self,
        registry: &DirectStateRegistry,
        permit: &StateImageInstallPermit,
    ) -> Result<DurablePublishedJournal, PublishedJournalRecovery> {
        let attempt = (|| {
            permit.validate_affinity(&self.durable.authority_identity)?;
            registry.validate_filesystem()?;
            if !Arc::ptr_eq(&self.durable.registry_identity, &registry.registry_identity)
                || self.durable.path != registry.journal_path
            {
                return Err("Published durability recovery belongs to another registry".into());
            }
            self.durable.validate_retirement_state()?;
            if let Some(retirement) = self.displaced_retirement.as_mut() {
                retirement.retry()?;
                self.displaced_retirement = None;
            }
            self.durable.root.sync()
        })();
        if let Err(first_error) = attempt {
            self.first_error = first_error;
            return Err(self);
        }
        Ok(self.durable)
    }
}

pub struct DurablePublishedJournal {
    pub(in crate::direct_state) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(in crate::direct_state) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(in crate::direct_state) journal: DirectStateInstallJournal,
    pub(in crate::direct_state) path: PathBuf,
    pub(in crate::direct_state) authority_file: File,
    pub(in crate::direct_state) root: PinnedPrivateDirectory,
    pub(in crate::direct_state) retirement_quarantine: Option<String>,
    pub(in crate::direct_state) retirement_complete: bool,
}

impl DurablePublishedJournal {
    pub(in crate::direct_state) fn from_authority(
        authority_identity: Arc<StateImageAuthorityIdentity>,
        registry_identity: Arc<DirectStateRegistryIdentity>,
        path: &Path,
        journal: DirectStateInstallJournal,
        authority_file: File,
        root: PinnedPrivateDirectory,
    ) -> Result<Self, String> {
        let durable = Self {
            authority_identity,
            registry_identity,
            journal,
            path: path.to_path_buf(),
            authority_file,
            root,
            retirement_quarantine: None,
            retirement_complete: false,
        };
        durable.validate_live()?;
        Ok(durable)
    }

    pub(in crate::direct_state) fn validate_live(&self) -> Result<(), String> {
        if self.journal.phase != DirectStateInstallPhase::Published {
            return Err("durable Published journal has the wrong phase".into());
        }
        self.root.validate_file(
            &self.path,
            &self.authority_file,
            "durable Published journal",
        )?;
        validate_canonical_durable_record(
            &self.authority_file,
            &self.journal,
            "durable Published journal",
        )
    }

    pub(in crate::direct_state) fn validate_retirement_state(&self) -> Result<(), String> {
        if self.retirement_complete {
            return Ok(());
        }
        if let Some(name) = self.retirement_quarantine.as_deref() {
            let actual = self
                .root
                .reader()
                .open_regular(name, "pin quarantined Published journal")?;
            return validate_same_file_identity(
                &self.authority_file,
                &actual,
                "quarantined Published journal",
            );
        }
        self.validate_live()
    }

    pub(in crate::direct_state) fn validate_affinity(
        &self,
        expected: &Arc<StateImageAuthorityIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.authority_identity, expected) {
            Ok(())
        } else {
            Err("durable Published journal belongs to another authority".into())
        }
    }

    pub(in crate::direct_state) fn validate_registry_affinity(
        &self,
        expected: &Arc<DirectStateRegistryIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.registry_identity, expected) {
            Ok(())
        } else {
            Err("durable Published journal belongs to another registry".into())
        }
    }

    pub(in crate::direct_state) fn try_clone_token(&self) -> Result<Self, String> {
        Ok(Self {
            authority_identity: self.authority_identity.clone(),
            registry_identity: self.registry_identity.clone(),
            journal: self.journal.clone(),
            path: self.path.clone(),
            authority_file: self
                .authority_file
                .try_clone()
                .map_err(|error| format!("clone durable Published authority: {error}"))?,
            root: self.root.try_clone_token()?,
            retirement_quarantine: self.retirement_quarantine.clone(),
            retirement_complete: self.retirement_complete,
        })
    }

    /// Retire only the exact path-pinned Published journal represented by this
    /// token. A path replacement must remain untouched and keep recovery
    /// required; a successful unlink is already the visible cleanup commit, so
    /// a later directory-sync failure is recoverable residue rather than a
    /// reason to target another inode by path.
    pub(in crate::direct_state) fn retire_exact(&mut self) -> Result<(), String> {
        if self.retirement_complete {
            return self.root.sync();
        }
        let journal_name = self
            .root
            .validate_path(&self.path, "durable Published journal")?
            .to_string();
        if self.retirement_quarantine.is_none() {
            self.validate_live()?;
            let quarantine = format!(
                ".{journal_name}.retired.{}.{}",
                std::process::id(),
                NEXT_DIRECT_STATE_TEMP.fetch_add(1, Ordering::Relaxed)
            );
            self.root.mutations().rename_no_replace(
                &journal_name,
                &quarantine,
                "quarantine Published journal",
            )?;
            self.retirement_quarantine = Some(quarantine);
        }
        let quarantine = self
            .retirement_quarantine
            .as_deref()
            .expect("retirement quarantine was assigned");
        let moved = self
            .root
            .reader()
            .open_regular(quarantine, "pin quarantined Published journal")?;
        validate_same_file_identity(
            &self.authority_file,
            &moved,
            "quarantined Published journal",
        )?;
        self.root.sync()?;
        self.root
            .mutations()
            .unlink(quarantine, "remove quarantined Published journal")?;
        self.retirement_complete = true;
        self.root.sync()
    }
}

pub(in crate::direct_state) fn load_published_journal(
    permit: &StateImageInstallPermit,
    registry_identity: &Arc<DirectStateRegistryIdentity>,
    root: &PinnedPrivateDirectory,
    path: &Path,
) -> Result<Option<DurablePublishedJournal>, String> {
    let _ = permit;
    let Some((journal, authority_file)) = read_install_journal_authority(root, path)? else {
        return Ok(None);
    };
    if journal.phase != DirectStateInstallPhase::Published {
        return Err("durable Published journal has the wrong phase".into());
    }
    root.sync()?;
    DurablePublishedJournal::from_authority(
        permit.authority_identity.clone(),
        registry_identity.clone(),
        path,
        journal,
        authority_file,
        root.try_clone_token()?,
    )
    .map(Some)
}
