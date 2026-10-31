use super::*;

/// Provider-private result admitted by the shared framing validator.  Construction
/// is intentionally registry-only and the token is intentionally non-Clone.
pub struct ValidatedDirectStateSection {
    pub(super) domain: DirectStateDomain,
    pub(super) manifest: DirectStateSectionManifest,
    pub(super) received: DirectStatePhysicalImage,
    pub(super) provider_state: Box<dyn Any + Send>,
}

pub struct ValidatedWholeGeneration {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) capture_set_sha256: String,
    pub(super) source_generation_sha256: String,
    pub(super) scope: DirectStateScope,
    pub(super) sections: Vec<ValidatedDirectStateSection>,
}

/// Off-serving replacement admitted for publication.  Construction is
/// registry-only and the token is intentionally non-Clone.
pub struct StagedDirectStateSection {
    pub(super) domain: DirectStateDomain,
    pub(super) generation: StagedDirectStateGeneration,
    pub(super) entry: DirectStateGenerationEntry,
}

pub struct StagedWholeGeneration {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) capture_set_sha256: String,
    pub(super) source_generation_sha256: String,
    pub(super) scope: DirectStateScope,
    pub(super) sections: Vec<StagedDirectStateSection>,
}

impl StagedWholeGeneration {
    pub fn prepared_journal(
        &self,
        snapshot_sha256: String,
    ) -> Result<DirectStateInstallJournal, String> {
        validate_sha256("direct-state snapshot", &snapshot_sha256)?;
        let journal = DirectStateInstallJournal {
            schema_version: DIRECT_STATE_SCHEMA_VERSION,
            snapshot_sha256,
            scope: self.scope.clone(),
            phase: DirectStateInstallPhase::Prepared,
            sections: self
                .sections
                .iter()
                .map(|section| section.section().clone())
                .collect(),
        };
        journal.validate()?;
        Ok(journal)
    }
}

impl StagedDirectStateSection {
    pub fn section(&self) -> &DirectStateInstallSection {
        self.generation.section()
    }
}

impl DirectStateRegistry {
    /// Mint initial sources only while serving is closed and both aggregate
    /// durable authorities are absent. Providers independently prove their
    /// domain roots are empty/current-v1-safe; any existing bytes reject the
    /// entire attempt. The caller must feed all returned sources through
    /// `validate` -> `stage_replace` -> one aggregate Prepared/Published/Current
    /// transition before `StateImageAuthority::publish_current` and `open`.
    pub fn capture_initial_generation(
        &self,
        permit: &StateImageInstallPermit,
        journal_path: &Path,
        current_path: &Path,
        scope: DirectStateScope,
        configured_max_bytes: u64,
    ) -> Result<CapturedWholeGeneration, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_authority_paths(journal_path, current_path)?;
        self.require_complete()?;
        validate_bootstrap_preflight(
            self,
            permit,
            journal_path,
            current_path,
            &scope,
            configured_max_bytes,
        )?;
        let source_generation_sha256 = hex_sha256(
            format!(
                "direct-state-bootstrap-v1\0{}\0{}",
                scope.authority_epoch(),
                uuid::Uuid::new_v4().simple()
            )
            .as_bytes(),
        );
        let capture_set_sha256 = hex_sha256(
            format!(
                "direct-state-capture-set-v1\0{}\0{}",
                source_generation_sha256,
                scope.control_applied_index()
            )
            .as_bytes(),
        );
        let sources = capture_initial_sources(
            self,
            permit,
            &scope,
            &capture_set_sha256,
            &source_generation_sha256,
            configured_max_bytes,
        )?;
        validate_bootstrap_files_absent(
            self,
            permit,
            journal_path,
            current_path,
            "direct-state bootstrap authority appeared during source capture",
        )?;
        let captured = CapturedWholeGeneration {
            authority_identity: self.authority_identity.clone(),
            registry_identity: self.registry_identity.clone(),
            registry_contract_sha256: self.registry_identity.contract_sha256.clone(),
            capture_set_sha256,
            source_generation_sha256,
            scope,
            sources,
        };
        captured.validate_closed()?;
        Ok(captured)
    }

    pub fn capture_all(
        &self,
        permit: &StateImageWritePermit,
        scope: DirectStateScope,
        configured_max_bytes: u64,
    ) -> Result<CapturedWholeGeneration, String> {
        permit.validate_affinity(&self.authority_identity)?;
        let current = permit.current_generation(&self.authority_identity)?;
        scope.validate()?;
        if configured_max_bytes > HARD_MAX_DIRECT_STATE_BYTES {
            return Err("configured direct-state bound exceeds the hard limit".into());
        }
        let (current_index, current_epoch, capture_index, capture_epoch) =
            match (&current.scope, &scope) {
                (
                    DirectStateScope::DefaultGlobal {
                        control_applied_index: current_index,
                        authority_epoch: current_epoch,
                        ..
                    },
                    DirectStateScope::DefaultGlobal {
                        control_applied_index: capture_index,
                        authority_epoch: capture_epoch,
                        ..
                    },
                ) => (
                    *current_index,
                    *current_epoch,
                    *capture_index,
                    *capture_epoch,
                ),
            };
        if capture_epoch != current_epoch || capture_index < current_index {
            return Err("direct-state capture scope is stale or crosses an authority epoch".into());
        }
        let source_generation_sha256 = hex_sha256(
            format!(
                "direct-state-source-generation-v1\0{}\0{}\0{}\0{}",
                current.sections_sha256,
                capture_index,
                capture_epoch,
                uuid::Uuid::new_v4().simple()
            )
            .as_bytes(),
        );
        let capture_set_sha256 = hex_sha256(
            format!(
                "direct-state-capture-set-v1\0{}\0{}",
                source_generation_sha256,
                uuid::Uuid::new_v4().simple()
            )
            .as_bytes(),
        );
        let mut sources = Vec::with_capacity(DirectStateDomain::ALL.len());
        for domain in DirectStateDomain::ALL {
            let binding = DirectStateCaptureBinding {
                scope: scope.clone(),
                capture_set_sha256: capture_set_sha256.clone(),
                source_generation_sha256: source_generation_sha256.clone(),
                roots: self
                    .providers
                    .get(&domain)
                    .ok_or_else(|| "direct-state provider registration disappeared".to_string())?
                    .roots(&self.registry_identity)?,
            };
            sources.push(self.capture_domain(
                permit,
                current,
                domain,
                &binding,
                configured_max_bytes,
            )?);
        }
        let captured = CapturedWholeGeneration {
            authority_identity: self.authority_identity.clone(),
            registry_identity: self.registry_identity.clone(),
            registry_contract_sha256: self.registry_identity.contract_sha256.clone(),
            capture_set_sha256,
            source_generation_sha256,
            scope,
            sources,
        };
        captured.validate_closed()?;
        Ok(captured)
    }

    /// Admit the exact closed set reconstructed by snapshot transport. Serialized
    /// capture/source-generation identities make a mixed set reject even when
    /// every individual section and chunk is otherwise valid.
    pub fn receive_all(
        &self,
        permit: &StateImageInstallPermit,
        transport: DirectStateTransportGeneration,
    ) -> Result<CapturedWholeGeneration, String> {
        permit.validate_affinity(&self.authority_identity)?;
        if !Arc::ptr_eq(&transport.authority_identity, &self.authority_identity)
            || !Arc::ptr_eq(&transport.registry_identity, &self.registry_identity)
        {
            return Err("local direct-state transport belongs to another registry".into());
        }
        transport.validate_closed()?;
        let DirectStateTransportGeneration {
            header, sources, ..
        } = transport;
        let captured = CapturedWholeGeneration {
            authority_identity: self.authority_identity.clone(),
            registry_identity: self.registry_identity.clone(),
            registry_contract_sha256: header.registry_contract_sha256,
            capture_set_sha256: header.capture_set_sha256,
            source_generation_sha256: header.source_generation_sha256,
            scope: header.scope,
            sources,
        };
        captured.validate_closed()?;
        Ok(captured)
    }

    /// Bind one already-authenticated remote Raft snapshot payload to this exact
    /// destination registry. The serialized owner-contract digest and DEFAULT_GROUP
    /// fence are checked before any section stream is consumed.
    pub fn receive_authenticated_remote(
        &self,
        permit: &StateImageInstallPermit,
        expected_capture_set_sha256: &str,
        expected_source_generation_sha256: &str,
        expected_scope: &DirectStateScope,
        remote: DirectStateRemoteTransport,
    ) -> Result<CapturedWholeGeneration, String> {
        permit.validate_affinity(&self.authority_identity)?;
        self.validate_filesystem()?;
        validate_sha256("expected remote capture set", expected_capture_set_sha256)?;
        validate_sha256(
            "expected remote source generation",
            expected_source_generation_sha256,
        )?;
        expected_scope.validate()?;
        if expected_scope.control_group() != DEFAULT_GROUP
            || remote.header.scope != *expected_scope
            || remote.header.capture_set_sha256 != expected_capture_set_sha256
            || remote.header.source_generation_sha256 != expected_source_generation_sha256
        {
            return Err("remote direct-state snapshot is not DEFAULT_GROUP authority".into());
        }
        let transport = DirectStateTransportGeneration::received_remote(
            permit,
            &self.registry_identity,
            &self.registry_identity.contract_sha256,
            remote.header,
            remote.sections,
        )?;
        self.receive_all(permit, transport)
    }

    pub(super) fn capture_domain(
        &self,
        permit: &StateImageWritePermit,
        current: &DirectStateGeneration,
        domain: DirectStateDomain,
        binding: &DirectStateCaptureBinding,
        configured_max_bytes: u64,
    ) -> Result<DirectStateSectionSource, String> {
        let provider = self.provider(domain)?;
        let current_entry = current
            .entries
            .get(&domain)
            .ok_or_else(|| format!("direct-state capture generation omits {domain:?}"))?;
        if current_entry.domain != domain || current_entry.type_id != provider.value_type_id() {
            return Err("direct-state capture generation contains the wrong authority type".into());
        }
        let expected_scope = binding.scope.clone();
        let source = provider.capture(permit, current_entry, binding)?;
        self.providers
            .get(&domain)
            .ok_or_else(|| "direct-state provider registration disappeared".to_string())?
            .validate_capture_source(&source)?;
        if !Arc::ptr_eq(&source.authority_identity, &self.authority_identity)
            || !Arc::ptr_eq(&source.registry_identity, &self.registry_identity)
        {
            return Err("direct-state capture belongs to another registry".into());
        }
        if source.manifest().domain != domain {
            return Err("direct-state provider returned a different domain".into());
        }
        if source.manifest().scope != expected_scope {
            return Err("direct-state provider returned a different scope fence".into());
        }
        source.manifest().validate(configured_max_bytes)?;
        self.validate_registered_owner(domain, &source.manifest().owner_manifest_sha256)?;
        Ok(source)
    }

    pub fn validate_all(
        &self,
        permit: &StateImageInstallPermit,
        captured: CapturedWholeGeneration,
        configured_max_bytes: u64,
        control_applied_index: u64,
        authority_epoch: u64,
    ) -> Result<ValidatedWholeGeneration, String> {
        permit.validate_affinity(&self.authority_identity)?;
        if !Arc::ptr_eq(&captured.authority_identity, &self.authority_identity) {
            return Err("captured direct-state generation belongs to another authority".into());
        }
        if !Arc::ptr_eq(&captured.registry_identity, &self.registry_identity) {
            return Err("captured direct-state generation belongs to another registry".into());
        }
        captured.validate_closed()?;
        captured
            .scope
            .validate_exact(control_applied_index, authority_epoch)?;
        validate_aggregate_section_bounds(
            captured
                .sources
                .iter()
                .map(DirectStateSectionSource::manifest),
            configured_max_bytes,
        )?;
        let capture_set_sha256 = captured.capture_set_sha256;
        let source_generation_sha256 = captured.source_generation_sha256;
        let scope = captured.scope;
        let mut validated = Vec::with_capacity(DirectStateDomain::ALL.len());
        for source in captured.sources {
            validated.push(self.validate_section(
                permit,
                source,
                configured_max_bytes,
                control_applied_index,
                authority_epoch,
            )?);
        }
        Ok(ValidatedWholeGeneration {
            authority_identity: self.authority_identity.clone(),
            registry_identity: self.registry_identity.clone(),
            capture_set_sha256,
            source_generation_sha256,
            scope,
            sections: validated,
        })
    }

    pub(super) fn validate_section(
        &self,
        permit: &StateImageInstallPermit,
        source: DirectStateSectionSource,
        configured_max_bytes: u64,
        control_applied_index: u64,
        authority_epoch: u64,
    ) -> Result<ValidatedDirectStateSection, String> {
        permit.validate_affinity(&self.authority_identity)?;
        if !Arc::ptr_eq(&source.authority_identity, &self.authority_identity) {
            return Err("direct-state source belongs to another authority".into());
        }
        let (manifest, chunks) = source.into_parts();
        manifest.validate(configured_max_bytes)?;
        manifest
            .scope
            .validate_exact(control_applied_index, authority_epoch)?;
        let domain = manifest.domain;
        let provider = self.provider(domain)?;
        self.validate_registered_owner(domain, &manifest.owner_manifest_sha256)?;
        let mut validating = ValidatingDirectStateChunks {
            manifest_sha256: manifest.sha256()?,
            expected_chunks: manifest.chunk_count,
            expected_bytes: manifest.logical_bytes,
            expected_content_sha256: manifest.content_sha256.clone(),
            next_ordinal: 0,
            observed_bytes: 0,
            content_hasher: Sha256::new(),
            chunks,
        };
        let roots = self
            .providers
            .get(&domain)
            .ok_or_else(|| "direct-state provider registration disappeared".to_string())?
            .roots(&self.registry_identity)?;
        let received = validating.spool_physical_image(permit, &roots)?;
        validating.finish()?;
        received.validate_live("direct-state input before provider validation")?;
        received.validate_content(&manifest)?;
        let provider_state = provider.validate_payload(permit, &manifest, &received)?;
        received.validate_live("direct-state input after provider validation")?;
        received.validate_content(&manifest)?;
        Ok(ValidatedDirectStateSection {
            domain,
            manifest,
            received,
            provider_state,
        })
    }

    pub fn stage_all(
        &self,
        permit: &StateImageInstallPermit,
        validated: ValidatedWholeGeneration,
    ) -> Result<StagedWholeGeneration, String> {
        permit.validate_affinity(&self.authority_identity)?;
        if !Arc::ptr_eq(&validated.authority_identity, &self.authority_identity) {
            return Err("validated whole generation belongs to another authority".into());
        }
        if !Arc::ptr_eq(&validated.registry_identity, &self.registry_identity) {
            return Err("validated whole generation belongs to another registry".into());
        }
        let mut staged = Vec::with_capacity(DirectStateDomain::ALL.len());
        for section in validated.sections {
            staged.push(self.stage_section(permit, section)?);
        }
        let result = StagedWholeGeneration {
            authority_identity: self.authority_identity.clone(),
            registry_identity: self.registry_identity.clone(),
            capture_set_sha256: validated.capture_set_sha256,
            source_generation_sha256: validated.source_generation_sha256,
            scope: validated.scope,
            sections: staged,
        };
        for section in &result.sections {
            let source = &section.section().source_manifest;
            if source.capture_set_sha256 != result.capture_set_sha256
                || source.source_generation_sha256 != result.source_generation_sha256
                || source.scope != result.scope
            {
                return Err("staged whole generation lost its capture-set authority".into());
            }
        }
        Ok(result)
    }

    pub(super) fn stage_section(
        &self,
        permit: &StateImageInstallPermit,
        validated: ValidatedDirectStateSection,
    ) -> Result<StagedDirectStateSection, String> {
        permit.validate_affinity(&self.authority_identity)?;
        let provider = self.provider(validated.domain)?;
        validated
            .received
            .validate_live("direct-state input before provider staging")?;
        validated.received.validate_content(&validated.manifest)?;
        let staged = provider.stage_replace(
            permit,
            &validated.manifest,
            validated.received,
            validated.provider_state,
        )?;
        self.providers
            .get(&validated.domain)
            .ok_or_else(|| "direct-state provider registration disappeared".to_string())?
            .validate_staged(&self.registry_identity, &staged.generation)?;
        let source_sha256 = validated.manifest.sha256()?;
        let section = staged.generation.section();
        if section.generation_manifest.domain != validated.domain
            || section.generation_manifest.scope != validated.manifest.scope
            || section.generation_manifest.owner_manifest_sha256
                != validated.manifest.owner_manifest_sha256
            || section.generation_manifest.source_manifest_sha256 != source_sha256
        {
            return Err("direct-state provider staged authority differs from its source".into());
        }
        self.validate_registered_owner(
            validated.domain,
            &section.generation_manifest.owner_manifest_sha256,
        )?;
        if staged.entry.domain != validated.domain
            || staged.entry.type_id != provider.value_type_id()
        {
            return Err("direct-state provider staged an unexpected authority type".into());
        }
        Ok(StagedDirectStateSection {
            domain: validated.domain,
            generation: staged.generation,
            entry: staged.entry,
        })
    }
}

fn validate_bootstrap_preflight(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    journal_path: &Path,
    current_path: &Path,
    scope: &DirectStateScope,
    configured_max_bytes: u64,
) -> Result<(), String> {
    if registry
        .current_slot
        .read()
        .map_err(|_| "direct-state generation slot is poisoned".to_string())?
        .is_some()
    {
        return Err("direct-state bootstrap cannot replace an in-memory Current".into());
    }
    scope.validate()?;
    if configured_max_bytes > HARD_MAX_DIRECT_STATE_BYTES {
        return Err("configured direct-state bound exceeds the hard limit".into());
    }
    ensure_private_directory(
        journal_path
            .parent()
            .ok_or_else(|| "direct-state bootstrap journal has no parent".to_string())?,
    )?;
    ensure_private_directory(
        current_path
            .parent()
            .ok_or_else(|| "direct-state bootstrap Current has no parent".to_string())?,
    )?;
    validate_distinct_authority_paths(journal_path, current_path)?;
    validate_bootstrap_files_absent(
        registry,
        permit,
        journal_path,
        current_path,
        "direct-state bootstrap requires absent Current and Pending authority",
    )
}

fn validate_bootstrap_files_absent(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    journal_path: &Path,
    current_path: &Path,
    error: &str,
) -> Result<(), String> {
    let journal_exists =
        read_install_journal(permit, &registry.journal_root, journal_path)?.is_some();
    let current_exists = read_current_image(
        permit,
        &registry.registry_identity,
        &registry.current_root,
        current_path,
    )?
    .is_some();
    if journal_exists || current_exists {
        return Err(error.to_string());
    }
    Ok(())
}

fn capture_initial_sources(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    scope: &DirectStateScope,
    capture_set_sha256: &str,
    source_generation_sha256: &str,
    configured_max_bytes: u64,
) -> Result<Vec<DirectStateSectionSource>, String> {
    let mut sources = Vec::with_capacity(DirectStateDomain::ALL.len());
    for domain in registry.domains() {
        let registration = registry
            .providers
            .get(&domain)
            .ok_or_else(|| "direct-state provider registration disappeared".to_string())?;
        let binding = DirectStateCaptureBinding {
            scope: scope.clone(),
            capture_set_sha256: capture_set_sha256.to_string(),
            source_generation_sha256: source_generation_sha256.to_string(),
            roots: registration.roots(&registry.registry_identity)?,
        };
        let source = registry
            .provider(domain)?
            .capture_initial(permit, &binding)?;
        registration.validate_capture_source(&source)?;
        if !Arc::ptr_eq(&source.authority_identity, &registry.authority_identity)
            || !Arc::ptr_eq(&source.registry_identity, &registry.registry_identity)
            || source.manifest.domain != domain
            || source.manifest.scope != *scope
        {
            return Err("direct-state bootstrap provider returned mismatched authority".into());
        }
        source.manifest.validate(configured_max_bytes)?;
        registry.validate_registered_owner(domain, &source.manifest().owner_manifest_sha256)?;
        sources.push(source);
    }
    Ok(sources)
}
