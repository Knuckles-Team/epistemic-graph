use super::*;

impl StateImageAuthority {
    pub(super) fn publish_current_for_registry(
        &self,
        registry_identity: &Arc<DirectStateRegistryIdentity>,
        permit: &StateImageInstallPermit,
        current: &DurableCurrentImage,
        assembled: AssembledDirectStateGeneration,
    ) -> Result<DirectStateRecoveryCompletion, String> {
        permit.validate_affinity(&self.identity)?;
        if !Arc::ptr_eq(&current.authority_identity, &self.identity) {
            return Err("durable Current belongs to a different state-image authority".into());
        }
        current.validate_live()?;
        assembled.validate_for_current(&self.identity, registry_identity, current.image())?;
        let candidate = OpenedCurrentFence::from_image(current.image())?;
        self.validate_current_successor(&candidate)?;
        let current_sha256 = candidate.sha256.clone();
        let authority_epoch = candidate.authority_epoch();
        let generation = assembled.generation;
        *self
            .current
            .write()
            .map_err(|_| "direct-state generation slot is poisoned".to_string())? =
            Some(generation.clone());
        Ok(DirectStateRecoveryCompletion {
            authority_identity: self.identity.clone(),
            registry_identity: registry_identity.clone(),
            generation,
            current_sha256,
            authority_epoch,
        })
    }

    pub fn open(
        &self,
        permit: StateImageInstallPermit,
        current: &DurableCurrentImage,
        completion: DirectStateRecoveryCompletion,
    ) -> Result<(), String> {
        permit.validate_affinity(&self.identity)?;
        current.validate_live()?;
        let published_generation = self
            .current
            .read()
            .map_err(|_| "direct-state generation slot is poisoned".to_string())?
            .clone()
            .ok_or_else(|| "direct-state generation is not published".to_string())?;
        completion.validate(&self.identity, current, &published_generation)?;
        let candidate = OpenedCurrentFence::from_image(current.image())?;
        self.validate_current_successor(&candidate)?;
        let authority_epoch = match &current.image.scope {
            DirectStateScope::DefaultGlobal {
                authority_epoch, ..
            } => *authority_epoch,
        };
        if authority_epoch == 0 || authority_epoch < self.authority_epoch.load(Ordering::Acquire) {
            return Err("direct-state authority epoch is stale or invalid".into());
        }
        self.authority_epoch
            .store(authority_epoch, Ordering::Release);
        *self
            .opened_current
            .write()
            .map_err(|_| "direct-state opened Current fence is poisoned".to_string())? =
            Some(candidate);
        self.ready.store(true, Ordering::Release);
        Ok(())
    }
}

impl DirectStateRegistry {
    pub fn load_prepared_journal(
        &self,
        permit: &StateImageInstallPermit,
        path: &Path,
    ) -> Result<Option<DurablePreparedJournal>, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_filesystem()?;
        if path != self.journal_path {
            return Err("Prepared journal path belongs to another registry".into());
        }
        load_prepared_journal(permit, &self.registry_identity, &self.journal_root, path)
    }

    pub fn load_published_journal(
        &self,
        permit: &StateImageInstallPermit,
        path: &Path,
    ) -> Result<Option<DurablePublishedJournal>, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_filesystem()?;
        if path != self.journal_path {
            return Err("Published journal path belongs to another registry".into());
        }
        load_published_journal(permit, &self.registry_identity, &self.journal_root, path)
    }

    pub fn read_current_image(
        &self,
        permit: &StateImageInstallPermit,
        path: &Path,
    ) -> Result<Option<DurableCurrentImage>, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_filesystem()?;
        if path != self.current_path {
            return Err("Current image path belongs to another registry".into());
        }
        read_current_image(permit, &self.registry_identity, &self.current_root, path)
    }

    /// Atomically publish only a generation assembled by this exact registry.
    /// This is deliberately the sole serving-pointer publication surface: a
    /// cloned `StateImageAuthority` can back multiple registries, but their
    /// descriptor-pinned filesystem roots are distinct authorities and their
    /// durable tokens cannot be cross-composed.
    pub fn publish_current(
        &self,
        authority: &StateImageAuthority,
        permit: &StateImageInstallPermit,
        current: &DurableCurrentImage,
        assembled: AssembledDirectStateGeneration,
    ) -> Result<DirectStateRecoveryCompletion, String> {
        permit.validate_affinity(&self.authority_identity)?;
        authority.validate_identity(&self.authority_identity)?;
        self.validate_filesystem()?;
        if current.path != self.current_path
            || !Arc::ptr_eq(&current.registry_identity, &self.registry_identity)
            || !Arc::ptr_eq(&assembled.registry_identity, &self.registry_identity)
        {
            return Err("direct-state Current publication belongs to another registry".into());
        }
        current.validate_live()?;
        authority.publish_current_for_registry(&self.registry_identity, permit, current, assembled)
    }

    pub fn transition_prepared_to_published(
        &self,
        permit: &StateImageInstallPermit,
        path: &Path,
        prepared: &PreparedWholeGeneration,
        replacement: &DirectStateInstallJournalV1,
    ) -> Result<PublishedJournalPublication, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_filesystem()?;
        if path != self.journal_path {
            return Err("journal transition path belongs to another registry".into());
        }
        if !Arc::ptr_eq(
            &prepared.assembled.authority_identity,
            &self.authority_identity,
        ) || !Arc::ptr_eq(
            &prepared.assembled.registry_identity,
            &self.registry_identity,
        ) || prepared
            .journal
            .validate_registry_affinity(&self.registry_identity)
            .is_err()
        {
            return Err("Prepared whole generation belongs to another registry".into());
        }
        replace_install_journal(permit, &self.journal_root, path, prepared, replacement)
    }

    pub fn promote_published_to_current(
        &self,
        permit: &StateImageInstallPermit,
        journal_path: &Path,
        current_path: &Path,
        published: &DurablePublishedJournal,
    ) -> Result<CurrentPromotion, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_authority_paths(journal_path, current_path)?;
        if !Arc::ptr_eq(&published.authority_identity, &self.authority_identity) {
            return Err("Published journal belongs to another registry".into());
        }
        published.validate_registry_affinity(&self.registry_identity)?;
        let current = match promote_published_install_to_current(
            permit,
            &self.registry_identity,
            &self.journal_root,
            &self.current_root,
            journal_path,
            current_path,
            published,
        )? {
            CurrentPointerPublication::Durable(current) => current,
            CurrentPointerPublication::RecoveryRequired(recovery) => {
                return Ok(CurrentPromotion::DurabilityRecoveryRequired(recovery));
            }
        };
        match self.cleanup_current_artifacts(permit, &current) {
            Ok(()) => Ok(CurrentPromotion::Durable(current)),
            Err(first_error) => Ok(CurrentPromotion::CleanupRecoveryRequired(
                CurrentCleanupRecovery {
                    current,
                    first_error,
                },
            )),
        }
    }

    pub fn retry_current_durability(
        &self,
        permit: &StateImageInstallPermit,
        mut recovery: CurrentDurabilityRecovery,
    ) -> Result<CurrentPromotion, CurrentDurabilityRecovery> {
        let validation = permit
            .validate_affinity(&self.authority_identity)
            .and_then(|_| self.validate_filesystem())
            .and_then(|_| {
                recovery
                    .current
                    .validate_registry_affinity(&self.registry_identity)
            })
            .and_then(|_| {
                recovery
                    .published
                    .validate_registry_affinity(&self.registry_identity)
            })
            .and_then(|_| {
                if recovery.current.path == self.current_path
                    && recovery.published.path == self.journal_path
                {
                    Ok(())
                } else {
                    Err("Current durability recovery belongs to another registry".into())
                }
            })
            .and_then(|_| recovery.current.validate_live())
            .and_then(|_| recovery.published.validate_retirement_state())
            .and_then(|_| {
                if current_image_from_journal(&recovery.published.journal)?
                    == recovery.current.image
                {
                    Ok(())
                } else {
                    Err("Current durability recovery journal pairing changed".into())
                }
            })
            .and_then(|_| recovery.current.root.sync());
        if let Err(first_error) = validation {
            recovery.first_error = first_error;
            return Err(recovery);
        }
        let CurrentDurabilityRecovery {
            current,
            mut published,
            mut displaced_retirement,
            ..
        } = recovery;
        if let Some(retirement) = displaced_retirement.as_mut() {
            if let Err(first_error) = retirement.retry() {
                return Err(CurrentDurabilityRecovery {
                    current,
                    published,
                    displaced_retirement,
                    first_error,
                });
            }
            displaced_retirement = None;
        }
        if let Err(first_error) = published.retire_exact() {
            return Err(CurrentDurabilityRecovery {
                current,
                published,
                displaced_retirement,
                first_error,
            });
        }
        match self.cleanup_current_artifacts(permit, &current) {
            Ok(()) => Ok(CurrentPromotion::Durable(current)),
            Err(first_error) => Ok(CurrentPromotion::CleanupRecoveryRequired(
                CurrentCleanupRecovery {
                    current,
                    first_error,
                },
            )),
        }
    }

    pub fn retry_current_cleanup(
        &self,
        permit: &StateImageInstallPermit,
        recovery: CurrentCleanupRecovery,
    ) -> Result<DurableCurrentImage, CurrentCleanupRecovery> {
        if recovery
            .current
            .validate_registry_affinity(&self.registry_identity)
            .is_err()
            || recovery.current.path != self.current_path
        {
            return Err(recovery);
        }
        match self.cleanup_current_artifacts(permit, &recovery.current) {
            Ok(()) => Ok(recovery.current),
            Err(first_error) => Err(CurrentCleanupRecovery {
                current: recovery.current,
                first_error,
            }),
        }
    }

    /// Publish the Prepared journal and transfer the complete closed staged set
    /// at the exact no-replace link point. Once the final name exists, guards are
    /// disarmed even if directory fsync fails, so no visible journal can reference
    /// files deleted by unwinding.
    pub fn write_prepared_journal_no_replace(
        &self,
        permit: &StateImageInstallPermit,
        path: &Path,
        mut staged: StagedWholeGeneration,
        journal: &DirectStateInstallJournalV1,
    ) -> Result<PreparedJournalPublication, String> {
        let (current_image, current_image_sha256, section_set_sha256, journal_sha256) =
            validate_prepared_set(self, permit, path, &staged, journal)?;
        for candidate in &mut staged.sections {
            if let Err(error) = candidate.generation.retry_directory_durability(permit) {
                return Ok(PreparedJournalPublication::GenerationRecoveryRequired(
                    PreparedGenerationRecovery {
                        staged,
                        journal_path: path.to_path_buf(),
                        journal: journal.clone(),
                        first_error: error,
                    },
                ));
            }
        }
        let staged = staged.sections;
        let journal_name = self
            .journal_root
            .validate_path(path, "direct-state install journal")?
            .to_string();
        self.journal_root
            .collector()
            .collect_temporaries(&journal_name)?;
        let bytes = rmp_serde::to_vec_named(journal)
            .map_err(|error| format!("encode direct-state install journal: {error}"))?;
        let (authority_file, temporary) =
            self.journal_root
                .mutations()
                .write_temporary(&journal_name, "prepared", &bytes)?;
        let temporary_retirement = ExactRetirement::prepare(
            &self.journal_root,
            &authority_file,
            &temporary,
            "Prepared journal temporary link",
        )?;
        let journal_root_token = self.journal_root.try_clone_token()?;
        for candidate in &staged {
            candidate.generation.validate_journal_ready()?;
        }
        if let Err(error) = self.journal_root.mutations().link_no_replace(
            &temporary,
            &self.journal_root,
            &journal_name,
            "publish no-replace direct-state journal",
        ) {
            let _ = self.journal_root.mutations().retire_unjournaled_exact(
                &temporary,
                &authority_file,
                "failed Prepared journal temporary",
            );
            return Err(error);
        }
        let prepared = assemble_visible_prepared(
            self,
            staged,
            current_image,
            current_image_sha256,
            section_set_sha256,
            journal_sha256,
        );
        Ok(finish_prepared_publication(
            self,
            path,
            journal,
            authority_file,
            journal_root_token,
            temporary_retirement,
            prepared,
        ))
    }
}

fn validate_prepared_set(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    path: &Path,
    staged: &StagedWholeGeneration,
    journal: &DirectStateInstallJournalV1,
) -> Result<(DirectStateCurrentImageV1, String, String, String), String> {
    permit.validate_affinity(&registry.authority_identity)?;
    registry.validate_filesystem()?;
    if path != registry.journal_path {
        return Err("Prepared journal path belongs to another registry".into());
    }
    if !Arc::ptr_eq(&staged.authority_identity, &registry.authority_identity) {
        return Err("staged whole generation belongs to another authority".into());
    }
    if !Arc::ptr_eq(&staged.registry_identity, &registry.registry_identity) {
        return Err("staged whole generation belongs to another registry".into());
    }
    if staged.scope != journal.scope
        || journal.sections.iter().any(|section| {
            section.source_manifest.capture_set_sha256 != staged.capture_set_sha256
                || section.source_manifest.source_generation_sha256
                    != staged.source_generation_sha256
        })
    {
        return Err("Prepared journal differs from its staged capture set".into());
    }
    validate_staged_sections(registry, staged, journal)?;
    let current = current_image_from_journal(journal)?;
    Ok((
        current.clone(),
        current.sha256()?,
        sections_sha256(&current.sections)?,
        journal.sha256()?,
    ))
}

fn validate_staged_sections(
    registry: &DirectStateRegistry,
    staged: &StagedWholeGeneration,
    journal: &DirectStateInstallJournalV1,
) -> Result<(), String> {
    journal.validate()?;
    if journal.phase != DirectStateInstallPhase::Prepared
        || staged.sections.len() != DirectStateDomain::ALL.len()
        || staged
            .sections
            .iter()
            .zip(&journal.sections)
            .any(|(candidate, durable)| candidate.section() != durable)
    {
        return Err("direct-state staged generation set differs from Prepared journal".into());
    }
    for candidate in &staged.sections {
        let provider = registry.provider(candidate.domain)?;
        if candidate.entry.domain != candidate.domain
            || candidate.entry.type_id != provider.value_type_id()
        {
            return Err("direct-state staged set contains an unexpected authority type".into());
        }
    }
    Ok(())
}

fn assemble_visible_prepared(
    registry: &DirectStateRegistry,
    staged: Vec<StagedDirectStateSection>,
    current: DirectStateCurrentImageV1,
    current_image_sha256: String,
    section_set_sha256: String,
    journal_sha256: String,
) -> VisiblePreparedState {
    let journaled = staged.into_iter().map(|staged| {
        let (_section, incoming, generation) = staged
            .generation
            .into_journaled_pinned()
            .expect("journal readiness was validated before Prepared publication");
        (staged.domain, staged.entry, incoming, generation)
    });
    let mut entries = BTreeMap::new();
    let mut retained_images = Vec::with_capacity(DirectStateDomain::ALL.len());
    for (domain, entry, incoming, generation) in journaled {
        entries.insert(domain, entry);
        retained_images.push((incoming, generation));
    }
    VisiblePreparedState {
        assembled: AssembledDirectStateGeneration {
            authority_identity: registry.authority_identity.clone(),
            registry_identity: registry.registry_identity.clone(),
            source_journal_sha256: Some(journal_sha256),
            current_image_sha256,
            generation: Arc::new(DirectStateGeneration {
                authority_identity: registry.authority_identity.clone(),
                scope: current.scope,
                sections_sha256: section_set_sha256,
                entries,
            }),
        },
        retained_images,
    }
}

fn finish_prepared_publication(
    registry: &DirectStateRegistry,
    path: &Path,
    journal: &DirectStateInstallJournalV1,
    authority_file: File,
    root: PinnedPrivateDirectory,
    mut retirement: ExactRetirement,
    prepared: VisiblePreparedState,
) -> PreparedJournalPublication {
    let durability = prepared
        .validate_live()
        .and_then(|_| {
            registry.journal_root.validate_file(
                path,
                &authority_file,
                "visible direct-state Prepared journal",
            )
        })
        .and_then(|_| registry.journal_root.sync())
        .and_then(|_| retirement.retry())
        .and_then(|_| {
            registry.journal_root.validate_file(
                path,
                &authority_file,
                "durable direct-state Prepared journal",
            )
        });
    match durability {
        Ok(()) => PreparedJournalPublication::Durable(PreparedWholeGeneration {
            journal: DurablePreparedJournal {
                authority_identity: registry.authority_identity.clone(),
                registry_identity: registry.registry_identity.clone(),
                journal: journal.clone(),
                path: path.to_path_buf(),
                authority_file,
                root,
            },
            assembled: prepared.assembled,
        }),
        Err(first_error) => PreparedJournalPublication::RecoveryRequired(PreparedJournalRecovery {
            authority_identity: registry.authority_identity.clone(),
            registry_identity: registry.registry_identity.clone(),
            journal: journal.clone(),
            path: path.to_path_buf(),
            authority_file,
            root,
            temporary_retirement: Some(retirement),
            prepared,
            first_error,
        }),
    }
}
