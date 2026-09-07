use super::*;

pub enum RecoveredDirectStateGeneration {
    Ready(AssembledDirectStateGeneration),
    CleanupRequired(Box<RecoveredPublishedCleanup>),
}

impl RecoveredDirectStateGeneration {
    #[cfg(test)]
    pub(in crate::direct_state) fn into_ready(
        self,
    ) -> Result<AssembledDirectStateGeneration, String> {
        match self {
            Self::Ready(assembled) => Ok(assembled),
            Self::CleanupRequired(_) => {
                Err("direct-state recovery still has durable cleanup debt".into())
            }
        }
    }
}

pub struct RecoveredPublishedCleanup {
    pub(super) assembled: AssembledDirectStateGeneration,
    pub(super) published: DurablePublishedJournal,
    pub(super) first_error: String,
}

impl RecoveredPublishedCleanup {
    pub fn first_error(&self) -> &str {
        &self.first_error
    }
}

impl AssembledDirectStateGeneration {
    pub(in crate::direct_state) fn validate_for_journal(
        &self,
        journal: &DirectStateInstallJournal,
    ) -> Result<(), String> {
        let journal_sha256 = journal.sha256()?;
        if !Arc::ptr_eq(
            &self.authority_identity,
            &self.generation.authority_identity,
        ) || self.source_journal_sha256.as_deref() != Some(journal_sha256.as_str())
        {
            return Err("assembled direct-state generation belongs to another journal".into());
        }
        let current = current_image_from_journal(journal)?;
        self.validate_for_current(&self.authority_identity, &self.registry_identity, &current)
    }

    pub(super) fn validate_for_current(
        &self,
        authority_identity: &Arc<StateImageAuthorityIdentity>,
        registry_identity: &Arc<DirectStateRegistryIdentity>,
        current: &DirectStateCurrentImage,
    ) -> Result<(), String> {
        self.generation.validate_closed()?;
        if !Arc::ptr_eq(&self.authority_identity, authority_identity)
            || !Arc::ptr_eq(&self.registry_identity, registry_identity)
            || self.current_image_sha256 != current.sha256()?
            || self.generation.scope != current.scope
            || self.generation.sections_sha256 != sections_sha256(&current.sections)?
        {
            return Err("assembled direct-state generation differs from durable Current".into());
        }
        Ok(())
    }
}

fn validate_recovery_inputs(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    current: Option<&DurableCurrentImage>,
    pending: Option<&DurablePendingJournal<'_>>,
) -> Result<(), String> {
    permit.validate_affinity(&registry.authority_identity)?;
    registry.validate_filesystem()?;
    registry.require_complete()?;
    if let Some(current) = current {
        current.validate_registry_affinity(&registry.registry_identity)?;
        current.validate_affinity(&registry.authority_identity)?;
        current.validate_live()?;
        if current.path != registry.current_path {
            return Err("recovered Current belongs to another registry path".into());
        }
    }
    if let Some(pending) = pending {
        pending.validate_registry_affinity(&registry.registry_identity)?;
        pending.validate_affinity(&registry.authority_identity)?;
        pending.validate_live()?;
        if pending.path() != registry.journal_path {
            return Err("recovered Pending belongs to another registry path".into());
        }
    }
    Ok(())
}

fn collect_unauthorized_residue(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
) -> Result<(), String> {
    // With no durable Current or Pending authority no managed image can be retained.
    for domain in registry.domains() {
        let provider = registry.provider(domain)?;
        let roots = registry
            .providers
            .get(&domain)
            .ok_or_else(|| "direct-state provider registration disappeared".to_string())?
            .roots(&registry.registry_identity)?;
        collect_unreferenced_physical_files(permit, &roots.staging, domain, None, &[])?;
        if provider.generation_directory() != provider.staging_directory() {
            collect_unreferenced_physical_files(permit, &roots.generations, domain, None, &[])?;
        }
    }
    Ok(())
}

fn classify_pending<'a>(
    current: Option<&DirectStateCurrentImage>,
    pending: Option<&'a DurablePendingJournal<'_>>,
) -> Result<
    (
        Option<&'a DirectStateInstallJournal>,
        Option<DurablePublishedJournal>,
    ),
    String,
> {
    let committed_residue = match (current, pending) {
        (Some(current), Some(DurablePendingJournal::Published(published))) => {
            current_image_from_journal(&published.journal)? == *current
        }
        _ => false,
    };
    if committed_residue {
        let Some(DurablePendingJournal::Published(published)) = pending else {
            return Err("Published residue token disappeared".into());
        };
        return Ok((None, Some(published.try_clone_token()?)));
    }
    Ok((pending.map(|token| token.journal()), None))
}

fn verify_pending_inputs(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    pending: Option<&DirectStateInstallJournal>,
) -> Result<BTreeMap<DirectStateDomain, VerifiedDirectStateIncoming>, String> {
    let mut verified = BTreeMap::new();
    let Some(journal) = pending else {
        return Ok(verified);
    };
    for domain in registry.domains() {
        let section = section_for_domain(&journal.sections, domain).ok_or_else(|| {
            format!("Pending direct-state authority omits required {domain:?} section")
        })?;
        let roots = registry
            .providers
            .get(&domain)
            .ok_or_else(|| "direct-state provider registration disappeared".to_string())?
            .roots(&registry.registry_identity)?;
        verified.insert(
            domain,
            verify_pending_incoming(permit, &roots, section, HARD_MAX_DIRECT_STATE_BYTES)?,
        );
    }
    Ok(verified)
}

fn recover_entries(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    current: Option<&DirectStateCurrentImage>,
    pending: Option<&DirectStateInstallJournal>,
    verified: &BTreeMap<DirectStateDomain, VerifiedDirectStateIncoming>,
) -> Result<BTreeMap<DirectStateDomain, DirectStateGenerationEntry>, String> {
    let mut entries = BTreeMap::new();
    for domain in registry.domains() {
        let entry = recover_domain_entry(registry, permit, current, pending, verified, domain)?;
        if entries.insert(domain, entry).is_some() {
            return Err("direct-state recovery duplicated a domain authority".into());
        }
    }
    Ok(entries)
}

fn recover_domain_entry(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    current: Option<&DirectStateCurrentImage>,
    pending: Option<&DirectStateInstallJournal>,
    verified: &BTreeMap<DirectStateDomain, VerifiedDirectStateIncoming>,
    domain: DirectStateDomain,
) -> Result<DirectStateGenerationEntry, String> {
    let pending_section = pending.and_then(|row| section_for_domain(&row.sections, domain));
    let current_section = current.and_then(|row| section_for_domain(&row.sections, domain));
    let section = pending_section.or(current_section).ok_or_else(|| {
        format!("direct-state recovery authority omits required {domain:?} section")
    })?;
    let registration = registry
        .providers
        .get(&domain)
        .ok_or_else(|| "direct-state provider registration disappeared".to_string())?;
    collect_domain_residue(
        permit,
        registration,
        &registry.registry_identity,
        domain,
        current_section,
        pending_section,
    )?;
    registry
        .validate_registered_owner(domain, &section.generation_manifest.owner_manifest_sha256)?;
    let pinned = registration.pin_generation(
        &registry.authority_identity,
        &registry.registry_identity,
        section,
    )?;
    let provider = registry.provider(domain)?;
    let recovered = match pending_section {
        Some(_) => provider.recover_pending(
            permit,
            section,
            verified
                .get(&domain)
                .ok_or_else(|| format!("Pending {domain:?} input was not authenticated"))?,
            &pinned,
        )?,
        None => provider.recover_current(permit, section, &pinned)?,
    };
    pinned.validate_live()?;
    validate_recovered_entry(
        provider,
        domain,
        section,
        pending_section.is_some(),
        &recovered,
    )?;
    Ok(recovered.entry)
}

fn collect_domain_residue(
    permit: &StateImageInstallPermit,
    registration: &DirectStateProviderRegistration,
    registry_identity: &Arc<DirectStateRegistryIdentity>,
    domain: DirectStateDomain,
    current: Option<&DirectStateInstallSection>,
    pending: Option<&DirectStateInstallSection>,
) -> Result<(), String> {
    let roots = registration.roots(registry_identity)?;
    let retained_incoming = pending.map(|row| row.incoming_file.as_str());
    let retained_generations = [
        current.map(|row| row.generation_file.as_str()),
        pending.map(|row| row.generation_file.as_str()),
    ];
    collect_unreferenced_physical_files(
        permit,
        &roots.staging,
        domain,
        retained_incoming,
        &retained_generations,
    )?;
    if registration.provider.generation_directory() != registration.provider.staging_directory() {
        collect_unreferenced_physical_files(
            permit,
            &roots.generations,
            domain,
            retained_incoming,
            &retained_generations,
        )?;
    }
    Ok(())
}

fn validate_recovered_entry(
    provider: &Arc<dyn DirectStateProvider>,
    domain: DirectStateDomain,
    section: &DirectStateInstallSection,
    pending: bool,
    recovered: &DirectStateRecoveredValue,
) -> Result<(), String> {
    if recovered.domain != domain
        || recovered.entry.domain != domain
        || recovered.entry.type_id != provider.value_type_id()
        || Some(recovered.entry.dynamic_store_authority_digest)
            != section.generation_manifest.dynamic_store_authority_digest
        || (pending
            && recovered.entry.install_evidence_sha256
                != section.generation_manifest.install_evidence_sha256)
    {
        return Err("direct-state provider recovered an unexpected authority type".into());
    }
    Ok(())
}

fn assemble_recovered(
    registry: &DirectStateRegistry,
    current: Option<&DurableCurrentImage>,
    pending: Option<&DirectStateInstallJournal>,
    entries: BTreeMap<DirectStateDomain, DirectStateGenerationEntry>,
) -> Result<AssembledDirectStateGeneration, String> {
    let target = match pending {
        Some(journal) => current_image_from_journal(journal)?,
        None => current
            .expect("recovery requires current when pending is absent")
            .image
            .clone(),
    };
    Ok(AssembledDirectStateGeneration {
        authority_identity: registry.authority_identity.clone(),
        registry_identity: registry.registry_identity.clone(),
        source_journal_sha256: pending
            .map(DirectStateInstallJournal::sha256)
            .transpose()?,
        current_image_sha256: target.sha256()?,
        generation: Arc::new(DirectStateGeneration {
            authority_identity: registry.authority_identity.clone(),
            scope: target.scope.clone(),
            sections_sha256: sections_sha256(&target.sections)?,
            entries,
        }),
    })
}

fn validate_pending_abandonment(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    journal_path: &Path,
    current: &DurableCurrentImage,
    pending: &DurablePendingJournal<'_>,
) -> Result<(), String> {
    permit.validate_affinity(&registry.authority_identity)?;
    registry.validate_filesystem()?;
    if journal_path != registry.journal_path || current.path != registry.current_path {
        return Err("Pending abandonment paths belong to another registry".into());
    }
    registry.require_complete()?;
    current.validate_affinity(&registry.authority_identity)?;
    current.validate_registry_affinity(&registry.registry_identity)?;
    pending.validate_affinity(&registry.authority_identity)?;
    pending.validate_registry_affinity(&registry.registry_identity)?;
    current.validate_live()?;
    pending.validate_live()?;
    if pending.path() != journal_path {
        return Err("Prepared journal token belongs to a different path".into());
    }
    let observed = read_install_journal(permit, &registry.journal_root, journal_path)?
        .ok_or_else(|| "Pending direct-state journal is missing".to_string())?;
    if observed != *pending.journal() {
        return Err("Pending direct-state journal changed before abandonment".into());
    }
    pending.validate_live()
}

fn recover_rollback_entries(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    current: &DurableCurrentImage,
) -> Result<BTreeMap<DirectStateDomain, DirectStateGenerationEntry>, String> {
    let mut entries = BTreeMap::new();
    for domain in registry.domains() {
        let section = section_for_domain(&current.image().sections, domain)
            .ok_or_else(|| format!("Current image omits required {domain:?} rollback authority"))?;
        let registration = registry
            .providers
            .get(&domain)
            .ok_or_else(|| "direct-state provider registration disappeared".to_string())?;
        registry.validate_registered_owner(
            domain,
            &section.generation_manifest.owner_manifest_sha256,
        )?;
        let pinned = registration.pin_generation(
            &registry.authority_identity,
            &registry.registry_identity,
            section,
        )?;
        let provider = registry.provider(domain)?;
        let recovered = provider.recover_current(permit, section, &pinned)?;
        pinned.validate_live()?;
        if recovered.domain != domain
            || recovered.entry.domain != domain
            || recovered.entry.type_id != provider.value_type_id()
            || Some(recovered.entry.dynamic_store_authority_digest)
                != section.generation_manifest.dynamic_store_authority_digest
        {
            return Err("direct-state provider recovered an unexpected rollback type".into());
        }
        entries.insert(domain, recovered.entry);
    }
    Ok(entries)
}

fn prepare_pending_abandonment(
    registry: &DirectStateRegistry,
    journal_path: &Path,
    current: &DurableCurrentImage,
    pending: &DurablePendingJournal<'_>,
    assembled: AssembledDirectStateGeneration,
) -> Result<PendingAbandonRecovery, String> {
    let journal_name = registry
        .journal_root
        .validate_path(journal_path, "rejected Pending journal")?
        .to_string();
    let quarantine_name = format!(
        ".{journal_name}.abandoned.{}.{}",
        std::process::id(),
        NEXT_DIRECT_STATE_TEMP.fetch_add(1, Ordering::Relaxed)
    );
    let recovery = PendingAbandonRecovery {
        current: current.try_clone_token()?,
        assembled,
        journal_root: registry.journal_root.try_clone_token()?,
        pending_authority: pending.try_clone_authority()?,
        live_name: journal_name.clone(),
        quarantine_name: quarantine_name.clone(),
        retirement_complete: false,
        first_error: "Pending retirement durability has not been confirmed".into(),
    };
    registry.journal_root.validate_file(
        journal_path,
        &recovery.pending_authority,
        "rejected Pending journal before retirement",
    )?;
    registry.journal_root.mutations().rename_no_replace(
        &journal_name,
        &quarantine_name,
        "quarantine rejected Pending journal",
    )?;
    Ok(recovery)
}

impl DirectStateRegistry {
    pub fn recover_all(
        &self,
        permit: &StateImageInstallPermit,
        current: Option<&DurableCurrentImage>,
        pending: Option<DurablePendingJournal<'_>>,
    ) -> Result<RecoveredDirectStateGeneration, String> {
        validate_recovery_inputs(self, permit, current, pending.as_ref())?;
        if current.is_none() && pending.is_none() {
            collect_unauthorized_residue(self, permit)?;
            return Err("direct-state recovery has no durable generation authority".into());
        }
        let current_image = current.map(DurableCurrentImage::image);
        let (pending_journal, mut published_residue) =
            classify_pending(current_image, pending.as_ref())?;
        let verified_pending = verify_pending_inputs(self, permit, pending_journal)?;
        let entries = recover_entries(
            self,
            permit,
            current_image,
            pending_journal,
            &verified_pending,
        )?;
        if pending_journal.is_none() {
            self.cleanup_current_artifacts(
                permit,
                current.expect("recovery requires current when pending is absent"),
            )?;
        }
        let assembled = assemble_recovered(self, current, pending_journal, entries)?;
        if let Some(mut published) = published_residue.take() {
            if let Err(first_error) = published.retire_exact() {
                return Ok(RecoveredDirectStateGeneration::CleanupRequired(Box::new(
                    RecoveredPublishedCleanup {
                        assembled,
                        published,
                        first_error,
                    },
                )));
            }
        }
        Ok(RecoveredDirectStateGeneration::Ready(assembled))
    }

    pub fn retry_recovered_published_cleanup(
        &self,
        permit: &StateImageInstallPermit,
        mut recovery: Box<RecoveredPublishedCleanup>,
    ) -> Result<RecoveredDirectStateGeneration, Box<RecoveredPublishedCleanup>> {
        let result = permit
            .validate_affinity(&self.authority_identity)
            .and_then(|_| self.validate_filesystem())
            .and_then(|_| {
                recovery
                    .published
                    .validate_registry_affinity(&self.registry_identity)
            })
            .and_then(|_| {
                if recovery.published.path == self.journal_path
                    && Arc::ptr_eq(
                        &recovery.assembled.registry_identity,
                        &self.registry_identity,
                    )
                {
                    Ok(())
                } else {
                    Err("Published cleanup recovery belongs to another registry".into())
                }
            })
            .and_then(|_| recovery.published.retire_exact());
        if let Err(first_error) = result {
            recovery.first_error = first_error;
            return Err(recovery);
        }
        Ok(RecoveredDirectStateGeneration::Ready(recovery.assembled))
    }

    /// Bind a restart- or fsync-recovered Prepared journal to the complete
    /// generation independently reopened from its exact seven durable sections.
    pub fn bind_recovered_prepared(
        &self,
        permit: &StateImageInstallPermit,
        journal: DurablePreparedJournal,
        assembled: AssembledDirectStateGeneration,
    ) -> Result<PreparedWholeGeneration, String> {
        permit.validate_affinity(&self.authority_identity)?;
        if !Arc::ptr_eq(&journal.authority_identity, &self.authority_identity) {
            return Err("recovered Prepared journal belongs to another authority".into());
        }
        journal.validate_registry_affinity(&self.registry_identity)?;
        if journal.path != self.journal_path {
            return Err("recovered Prepared journal belongs to another registry path".into());
        }
        if !Arc::ptr_eq(&assembled.authority_identity, &self.authority_identity)
            || !Arc::ptr_eq(&assembled.registry_identity, &self.registry_identity)
            || !Arc::ptr_eq(
                &assembled.generation.authority_identity,
                &self.authority_identity,
            )
        {
            return Err("recovered generation belongs to another authority".into());
        }
        journal.validate_live()?;
        assembled.validate_for_journal(&journal.journal)?;
        Ok(PreparedWholeGeneration { journal, assembled })
    }

    /// Durably reject an unusable Pending generation set and roll every stable
    /// provider handle back to the last complete Current image before reopening.
    /// Pending artifacts are collected only after its journal retirement fsyncs.
    pub fn abandon_pending_to_current(
        &self,
        permit: &StateImageInstallPermit,
        journal_path: &Path,
        current: &DurableCurrentImage,
        pending: DurablePendingJournal<'_>,
    ) -> Result<PendingAbandonment, String> {
        validate_pending_abandonment(self, permit, journal_path, current, &pending)?;
        let entries = recover_rollback_entries(self, permit, current)?;
        let generation = Arc::new(DirectStateGeneration {
            authority_identity: self.authority_identity.clone(),
            scope: current.image.scope.clone(),
            sections_sha256: sections_sha256(&current.image.sections)?,
            entries,
        });
        let assembled = AssembledDirectStateGeneration {
            authority_identity: self.authority_identity.clone(),
            registry_identity: self.registry_identity.clone(),
            source_journal_sha256: None,
            current_image_sha256: current.image.sha256()?,
            generation,
        };
        let recovery =
            prepare_pending_abandonment(self, journal_path, current, &pending, assembled)?;
        match self.retry_pending_abandonment(permit, Box::new(recovery)) {
            Ok(assembled) => Ok(PendingAbandonment::Durable(assembled)),
            Err(recovery) => Ok(PendingAbandonment::RecoveryRequired(recovery)),
        }
    }

    pub fn retry_pending_abandonment(
        &self,
        permit: &StateImageInstallPermit,
        mut recovery: Box<PendingAbandonRecovery>,
    ) -> Result<AssembledDirectStateGeneration, Box<PendingAbandonRecovery>> {
        let attempt = (|| {
            permit.validate_affinity(&self.authority_identity)?;
            self.validate_filesystem()?;
            recovery
                .current
                .validate_registry_affinity(&self.registry_identity)?;
            if recovery.current.path != self.current_path
                || !Arc::ptr_eq(
                    &recovery.assembled.registry_identity,
                    &self.registry_identity,
                )
                || recovery.journal_root.path != self.journal_root.path
            {
                return Err("Pending abandonment recovery belongs to another registry".into());
            }
            recovery.current.validate_live()?;
            if !recovery.retirement_complete {
                if let Some(actual) = recovery.journal_root.reader().open_optional_regular(
                    &recovery.quarantine_name,
                    "pin quarantined rejected Pending journal",
                )? {
                    validate_same_file_identity(
                        &recovery.pending_authority,
                        &actual,
                        "quarantined rejected Pending journal",
                    )?;
                    recovery.journal_root.sync()?;
                    recovery.journal_root.mutations().unlink(
                        &recovery.quarantine_name,
                        "remove quarantined rejected Pending journal",
                    )?;
                }
                recovery.retirement_complete = true;
            }
            recovery.journal_root.sync()?;
            if recovery
                .journal_root
                .reader()
                .open_optional_regular(
                    &recovery.live_name,
                    "prove canonical Pending absence after retirement",
                )?
                .is_some()
            {
                return Err("canonical Pending authority reappeared during abandonment".into());
            }
            recovery.journal_root.sync()?;
            self.cleanup_current_artifacts(permit, &recovery.current)
        })();
        if let Err(first_error) = attempt {
            recovery.first_error = first_error;
            return Err(recovery);
        }
        Ok(recovery.assembled)
    }

    /// Collect every managed artifact except the exact durable Current generation.
    /// This runs after Current commits and before readiness reopens, bounding disk
    /// across repeated live installs without risking the last-known-good authority.
    pub fn cleanup_current_artifacts(
        &self,
        permit: &StateImageInstallPermit,
        current: &DurableCurrentImage,
    ) -> Result<(), String> {
        permit.validate_affinity(&self.authority_identity)?;
        current.validate_affinity(&self.authority_identity)?;
        current.validate_registry_affinity(&self.registry_identity)?;
        if current.path != self.current_path {
            return Err("Current cleanup belongs to another registry path".into());
        }
        current.validate_live()?;
        let journal_name = self
            .journal_root
            .validate_path(&self.journal_path, "direct-state journal cleanup")?;
        let current_name = self
            .current_root
            .validate_path(&self.current_path, "direct-state Current cleanup")?;
        self.journal_root
            .collector()
            .collect_temporaries(journal_name)?;
        self.journal_root
            .collector()
            .collect_retired_residue(journal_name)?;
        self.current_root
            .collector()
            .collect_temporaries(current_name)?;
        self.current_root
            .collector()
            .collect_retired_residue(current_name)?;
        for section in &current.image.sections {
            let domain = section.generation_manifest.domain;
            let provider = self.provider(domain)?;
            let roots = self
                .providers
                .get(&domain)
                .ok_or_else(|| "direct-state provider registration disappeared".to_string())?
                .roots(&self.registry_identity)?;
            let retained = [Some(section.generation_file.as_str())];
            collect_unreferenced_physical_files(permit, &roots.staging, domain, None, &retained)?;
            if provider.generation_directory() != provider.staging_directory() {
                collect_unreferenced_physical_files(
                    permit,
                    &roots.generations,
                    domain,
                    None,
                    &retained,
                )?;
            }
        }
        Ok(())
    }
}

pub(super) fn collect_unreferenced_physical_files(
    permit: &StateImageInstallPermit,
    directory: &PinnedPrivateDirectory,
    domain: DirectStateDomain,
    retained_incoming: Option<&str>,
    retained_generations: &[Option<&str>],
) -> Result<(), String> {
    let _ = permit;
    directory.validate_live("direct-state collection root")?;
    for (ordinal, entry) in std::fs::read_dir(directory.reader().descriptor_root_path())
        .map_err(|error| format!("scan direct-state private directory: {error}"))?
        .enumerate()
    {
        if ordinal >= MAX_DIRECT_STATE_GC_ENTRIES {
            return Err("direct-state private directory exceeds its GC entry bound".into());
        }
        let entry = entry.map_err(|error| format!("read direct-state private entry: {error}"))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "direct-state private entry name is not UTF-8".to_string())?;
        let retired_base = name.strip_prefix('.').and_then(|name| {
            name.split_once(".retired.")
                .map(|(base, _)| base.to_string())
        });
        let managed_name = retired_base.as_deref().unwrap_or(&name);
        let managed = is_managed_capture_name(managed_name, domain)
            || is_managed_digest_name(
                managed_name,
                &format!(".direct-state-{}-", domain.as_str()),
                ".incoming",
            )
            || is_managed_digest_name(
                managed_name,
                &format!(".direct-state-{}-", domain.as_str()),
                ".prepared",
            )
            || is_managed_digest_name(managed_name, &format!("{}-", domain.as_str()), ".image");
        if !managed
            || retained_incoming.is_some_and(|retained| retained == name)
            || retained_generations
                .iter()
                .flatten()
                .any(|retained| *retained == name)
        {
            continue;
        }
        let authority = directory
            .reader()
            .open_regular(&name, "pin orphan direct-state file")?;
        if retired_base.is_some() {
            let mut retirement = ExactRetirement {
                root: directory.try_clone_token()?,
                expected: authority,
                live_name: managed_name.to_string(),
                quarantine_name: name,
                label: "orphan direct-state retirement residue",
                phase: ExactRetirementPhase::Quarantined,
            };
            retirement.retry()?;
        } else {
            directory.mutations().retire_unjournaled_exact(
                &name,
                &authority,
                "orphan direct-state file",
            )?;
        }
    }
    directory.sync()
}
