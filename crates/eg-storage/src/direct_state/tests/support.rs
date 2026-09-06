use super::super::{
    authority::*, capture::*, contract::*, filesystem::*, generation::*, journal::*, registry::*, *,
};
use std::sync::atomic::AtomicUsize;

thread_local! {
    /// Per-case capture/transport observation counters.
    ///
    /// The test binary runs cases in parallel and each case resets a counter, acts,
    /// then asserts on it; a process-global counter therefore observed every other
    /// case's providers as well. Every case drives its providers synchronously on its
    /// own thread, so the counters are thread-scoped.
    pub(super) static PROVIDER_CAPTURE_CALLS: AtomicUsize = const { AtomicUsize::new(0) };
    pub(super) static PROVIDER_CAPTURE_GENERATION: AtomicUsize = const { AtomicUsize::new(0) };
    pub(super) static TRANSPORT_CHUNK_READS: AtomicUsize = const { AtomicUsize::new(0) };
}

pub(super) struct CountingChunkStream {
    inner: Box<dyn DirectStateChunkStream>,
}

impl DirectStateChunkStream for CountingChunkStream {
    fn next_chunk(&mut self) -> Result<Option<DirectStateChunkV1>, String> {
        TRANSPORT_CHUNK_READS.with(|counter| counter.fetch_add(1, Ordering::Relaxed));
        self.inner.next_chunk()
    }
}

pub(super) fn count_local_transport_reads(transport: &mut DirectStateTransportGeneration) {
    for source in &mut transport.sources {
        let inner = std::mem::replace(
            &mut source.chunks,
            Box::new(std::iter::empty::<Result<DirectStateChunkV1, String>>()),
        );
        source.chunks = Box::new(CountingChunkStream { inner });
    }
}

pub(super) fn count_remote_transport_reads(remote: &mut DirectStateRemoteTransportV1) {
    for (_, chunks) in &mut remote.sections {
        let inner = std::mem::replace(
            chunks,
            Box::new(std::iter::empty::<Result<DirectStateChunkV1, String>>()),
        );
        *chunks = Box::new(CountingChunkStream { inner });
    }
}

#[derive(Debug)]
pub(super) struct BlobValue(pub(super) u64, pub(super) [u8; 32], pub(super) [u8; 32]);
impl sealed::DirectStateDomainValue for BlobValue {}
impl DirectStateDomainValue for BlobValue {
    const DOMAIN: DirectStateDomain = DirectStateDomain::Blob;

    fn dynamic_store_authority_digest(&self) -> Result<[u8; 32], String> {
        Ok(self.1)
    }

    fn install_evidence_sha256(&self) -> Result<[u8; 32], String> {
        Ok(self.2)
    }
}

macro_rules! test_value {
    ($name:ident, $domain:expr) => {
        struct $name([u8; 32], [u8; 32]);
        impl sealed::DirectStateDomainValue for $name {}
        impl DirectStateDomainValue for $name {
            const DOMAIN: DirectStateDomain = $domain;

            fn dynamic_store_authority_digest(&self) -> Result<[u8; 32], String> {
                Ok(self.0)
            }

            fn install_evidence_sha256(&self) -> Result<[u8; 32], String> {
                Ok(self.1)
            }
        }
    };
}

test_value!(ClusterControlValue, DirectStateDomain::ClusterControl);
test_value!(KeyValueValue, DirectStateDomain::KeyValue);
test_value!(TimeSeriesValue, DirectStateDomain::TimeSeries);
test_value!(AnalyticsJobsValue, DirectStateDomain::AnalyticsJobs);
test_value!(StatechartsValue, DirectStateDomain::Statecharts);
test_value!(SqliteCatalogValue, DirectStateDomain::SqliteCatalog);

pub(super) struct Provider {
    domain: DirectStateDomain,
    staging: PathBuf,
    generations: PathBuf,
}

pub(super) struct FlowProvider {
    domain: DirectStateDomain,
    staging: PathBuf,
    generations: PathBuf,
}

impl FlowProvider {
    fn bytes(&self) -> Vec<u8> {
        format!("direct-state-flow:{:?}", self.domain).into_bytes()
    }

    pub(super) fn physical_authority(path: &Path) -> Result<[u8; 32], String> {
        let metadata = std::fs::metadata(path).map_err(|error| error.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Sha256::digest(format!("{}:{}", metadata.dev(), metadata.ino()).as_bytes()).into())
        }
        #[cfg(not(unix))]
        {
            Ok(Sha256::digest(format!("{:?}", metadata).as_bytes()).into())
        }
    }

    fn recovered_value(
        &self,
        authority: [u8; 32],
        install: [u8; 32],
    ) -> Result<DirectStateRecoveredValue, String> {
        match self.domain {
            DirectStateDomain::ClusterControl => {
                DirectStateRecoveredValue::new(Arc::new(ClusterControlValue(authority, install)))
            }
            DirectStateDomain::Blob => {
                DirectStateRecoveredValue::new(Arc::new(BlobValue(7, authority, install)))
            }
            DirectStateDomain::KeyValue => {
                DirectStateRecoveredValue::new(Arc::new(KeyValueValue(authority, install)))
            }
            DirectStateDomain::TimeSeries => {
                DirectStateRecoveredValue::new(Arc::new(TimeSeriesValue(authority, install)))
            }
            DirectStateDomain::AnalyticsJobs => {
                DirectStateRecoveredValue::new(Arc::new(AnalyticsJobsValue(authority, install)))
            }
            DirectStateDomain::Statecharts => {
                DirectStateRecoveredValue::new(Arc::new(StatechartsValue(authority, install)))
            }
            DirectStateDomain::SqliteCatalog => {
                DirectStateRecoveredValue::new(Arc::new(SqliteCatalogValue(authority, install)))
            }
        }
    }

    fn recover_exact(
        &self,
        permit: &StateImageInstallPermit,
        section: &DirectStateInstallSectionV1,
        generation: &VerifiedDirectStateGeneration,
        compare_install_evidence: bool,
    ) -> Result<DirectStateRecoveredValue, String> {
        if generation.domain() != self.domain {
            return Err("test provider received another generation domain".into());
        }
        let authority = generation.with_pinned_path(permit, Self::physical_authority)?;
        if section.generation_manifest.dynamic_store_authority_digest != Some(authority) {
            return Err("test generation physical authority mismatch".into());
        }
        if compare_install_evidence
            && section.generation_manifest.install_evidence_sha256
                != <[u8; 32]>::from(Sha256::digest(self.bytes()))
        {
            return Err("test generation install evidence mismatch".into());
        }
        self.recovered_value(
            authority,
            section.generation_manifest.install_evidence_sha256,
        )
    }
}

impl sealed::DirectStateProvider for FlowProvider {}
impl DirectStateProvider for FlowProvider {
    fn domain(&self) -> DirectStateDomain {
        self.domain
    }

    fn value_type_id(&self) -> TypeId {
        Provider::value_type_id_for(self.domain)
    }

    fn owner_manifest(&self) -> Result<DirectStateOwnerManifestV1, String> {
        Ok(DirectStateOwnerManifestV1 {
            schema_version: DIRECT_STATE_SCHEMA_VERSION,
            domain: self.domain,
            authority_kind: DirectStateAuthorityKind::PlainRedb,
            store_schema_version: 1,
            tables: vec![format!("{:?}_rows", self.domain).to_ascii_lowercase()],
        })
    }

    fn staging_directory(&self) -> &Path {
        &self.staging
    }

    fn generation_directory(&self) -> &Path {
        &self.generations
    }

    fn capture_initial(
        &self,
        permit: &StateImageInstallPermit,
        binding: &DirectStateCaptureBinding,
    ) -> Result<DirectStateSectionSource, String> {
        PROVIDER_CAPTURE_CALLS.with(|counter| counter.fetch_add(1, Ordering::Relaxed));
        capture_physical_image_source(
            permit,
            &self.staging,
            self.domain,
            binding,
            self.owner_manifest()?.sha256()?,
            1024,
            |path| std::fs::write(path, self.bytes()).map_err(|error| error.to_string()),
        )
    }

    fn capture(
        &self,
        permit: &StateImageWritePermit,
        _: &DirectStateGenerationEntry,
        binding: &DirectStateCaptureBinding,
    ) -> Result<DirectStateSectionSource, String> {
        PROVIDER_CAPTURE_CALLS.with(|counter| counter.fetch_add(1, Ordering::Relaxed));
        capture_physical_image_source(
            permit,
            &self.staging,
            self.domain,
            binding,
            self.owner_manifest()?.sha256()?,
            1024,
            |path| std::fs::write(path, self.bytes()).map_err(|error| error.to_string()),
        )
    }

    fn validate_payload(
        &self,
        _: &StateImageInstallPermit,
        _: &DirectStateSectionManifestV1,
        received: &DirectStatePhysicalImage,
    ) -> Result<Box<dyn Any + Send>, String> {
        let bytes = received
            .with_pinned_path(|path| std::fs::read(path).map_err(|error| error.to_string()))?;
        if bytes != self.bytes() {
            return Err("test provider input mismatch".into());
        }
        Ok(Box::new(bytes))
    }

    fn stage_replace(
        &self,
        permit: &StateImageInstallPermit,
        manifest: &DirectStateSectionManifestV1,
        received: DirectStatePhysicalImage,
        validated: Box<dyn Any + Send>,
    ) -> Result<DirectStateProviderStage, String> {
        let bytes = *validated
            .downcast::<Vec<u8>>()
            .map_err(|_| "test provider validation type mismatch".to_string())?;
        let prepared = prepare_mutable_generation(permit, received, manifest, &self.generations)?;
        let physical_authority = prepared.with_pinned_path(Self::physical_authority)?;
        let install_evidence = Sha256::digest(bytes).into();
        match self.domain {
            DirectStateDomain::ClusterControl => {
                let value = Arc::new(ClusterControlValue(physical_authority, install_evidence));
                let staged =
                    bind_prepared_generation(permit, prepared, value.as_ref(), &self.generations)?;
                let serving_authority =
                    staged.with_pinned_generation_path(Self::physical_authority)?;
                DirectStateProviderStage::new(
                    staged,
                    Arc::new(ClusterControlValue(serving_authority, install_evidence)),
                )
            }
            DirectStateDomain::Blob => {
                let value = Arc::new(BlobValue(7, physical_authority, install_evidence));
                let staged =
                    bind_prepared_generation(permit, prepared, value.as_ref(), &self.generations)?;
                let serving_authority =
                    staged.with_pinned_generation_path(Self::physical_authority)?;
                DirectStateProviderStage::new(
                    staged,
                    Arc::new(BlobValue(7, serving_authority, install_evidence)),
                )
            }
            DirectStateDomain::KeyValue => {
                let value = Arc::new(KeyValueValue(physical_authority, install_evidence));
                let staged =
                    bind_prepared_generation(permit, prepared, value.as_ref(), &self.generations)?;
                let serving_authority =
                    staged.with_pinned_generation_path(Self::physical_authority)?;
                DirectStateProviderStage::new(
                    staged,
                    Arc::new(KeyValueValue(serving_authority, install_evidence)),
                )
            }
            DirectStateDomain::TimeSeries => {
                let value = Arc::new(TimeSeriesValue(physical_authority, install_evidence));
                let staged =
                    bind_prepared_generation(permit, prepared, value.as_ref(), &self.generations)?;
                let serving_authority =
                    staged.with_pinned_generation_path(Self::physical_authority)?;
                DirectStateProviderStage::new(
                    staged,
                    Arc::new(TimeSeriesValue(serving_authority, install_evidence)),
                )
            }
            DirectStateDomain::AnalyticsJobs => {
                let value = Arc::new(AnalyticsJobsValue(physical_authority, install_evidence));
                let staged =
                    bind_prepared_generation(permit, prepared, value.as_ref(), &self.generations)?;
                let serving_authority =
                    staged.with_pinned_generation_path(Self::physical_authority)?;
                DirectStateProviderStage::new(
                    staged,
                    Arc::new(AnalyticsJobsValue(serving_authority, install_evidence)),
                )
            }
            DirectStateDomain::Statecharts => {
                let value = Arc::new(StatechartsValue(physical_authority, install_evidence));
                let staged =
                    bind_prepared_generation(permit, prepared, value.as_ref(), &self.generations)?;
                let serving_authority =
                    staged.with_pinned_generation_path(Self::physical_authority)?;
                DirectStateProviderStage::new(
                    staged,
                    Arc::new(StatechartsValue(serving_authority, install_evidence)),
                )
            }
            DirectStateDomain::SqliteCatalog => {
                let value = Arc::new(SqliteCatalogValue(physical_authority, install_evidence));
                let staged =
                    bind_prepared_generation(permit, prepared, value.as_ref(), &self.generations)?;
                let serving_authority =
                    staged.with_pinned_generation_path(Self::physical_authority)?;
                DirectStateProviderStage::new(
                    staged,
                    Arc::new(SqliteCatalogValue(serving_authority, install_evidence)),
                )
            }
        }
    }

    fn recover_current(
        &self,
        permit: &StateImageInstallPermit,
        section: &DirectStateInstallSectionV1,
        generation: &VerifiedDirectStateGeneration,
    ) -> Result<DirectStateRecoveredValue, String> {
        self.recover_exact(permit, section, generation, false)
    }

    fn recover_pending(
        &self,
        permit: &StateImageInstallPermit,
        section: &DirectStateInstallSectionV1,
        incoming: &VerifiedDirectStateIncoming,
        generation: &VerifiedDirectStateGeneration,
    ) -> Result<DirectStateRecoveredValue, String> {
        incoming.validate_live(permit)?;
        if incoming.source_manifest() != &section.source_manifest {
            return Err("test provider Pending provenance mismatch".into());
        }
        self.recover_exact(permit, section, generation, true)
    }
}

impl Provider {
    fn value_type_id_for(domain: DirectStateDomain) -> TypeId {
        match domain {
            DirectStateDomain::ClusterControl => TypeId::of::<ClusterControlValue>(),
            DirectStateDomain::Blob => TypeId::of::<BlobValue>(),
            DirectStateDomain::KeyValue => TypeId::of::<KeyValueValue>(),
            DirectStateDomain::TimeSeries => TypeId::of::<TimeSeriesValue>(),
            DirectStateDomain::AnalyticsJobs => TypeId::of::<AnalyticsJobsValue>(),
            DirectStateDomain::Statecharts => TypeId::of::<StatechartsValue>(),
            DirectStateDomain::SqliteCatalog => TypeId::of::<SqliteCatalogValue>(),
        }
    }
}

impl sealed::DirectStateProvider for Provider {}
impl DirectStateProvider for Provider {
    fn domain(&self) -> DirectStateDomain {
        self.domain
    }

    fn value_type_id(&self) -> TypeId {
        Self::value_type_id_for(self.domain)
    }

    fn owner_manifest(&self) -> Result<DirectStateOwnerManifestV1, String> {
        Ok(DirectStateOwnerManifestV1 {
            schema_version: DIRECT_STATE_SCHEMA_VERSION,
            domain: self.domain,
            authority_kind: DirectStateAuthorityKind::PlainRedb,
            store_schema_version: 1,
            tables: vec!["rows".to_string()],
        })
    }

    fn staging_directory(&self) -> &Path {
        &self.staging
    }

    fn generation_directory(&self) -> &Path {
        &self.generations
    }

    fn capture_initial(
        &self,
        _: &StateImageInstallPermit,
        _: &DirectStateCaptureBinding,
    ) -> Result<DirectStateSectionSource, String> {
        PROVIDER_CAPTURE_CALLS.with(|counter| counter.fetch_add(1, Ordering::Relaxed));
        Err("test provider capture must not run".into())
    }

    fn capture(
        &self,
        _: &StateImageWritePermit,
        current: &DirectStateGenerationEntry,
        _: &DirectStateCaptureBinding,
    ) -> Result<DirectStateSectionSource, String> {
        PROVIDER_CAPTURE_CALLS.with(|counter| counter.fetch_add(1, Ordering::Relaxed));
        if self.domain == DirectStateDomain::Blob {
            let blob = current
                .get::<BlobValue>()
                .expect("Blob test generation type");
            PROVIDER_CAPTURE_GENERATION
                .with(|counter| counter.store(blob.0 as usize, Ordering::Relaxed));
        }
        Err("test provider capture must not run".into())
    }

    fn validate_payload(
        &self,
        _: &StateImageInstallPermit,
        _: &DirectStateSectionManifestV1,
        _: &DirectStatePhysicalImage,
    ) -> Result<Box<dyn Any + Send>, String> {
        unreachable!()
    }

    fn stage_replace(
        &self,
        _: &StateImageInstallPermit,
        _: &DirectStateSectionManifestV1,
        _: DirectStatePhysicalImage,
        _: Box<dyn Any + Send>,
    ) -> Result<DirectStateProviderStage, String> {
        unreachable!()
    }

    fn recover_current(
        &self,
        _: &StateImageInstallPermit,
        _: &DirectStateInstallSectionV1,
        _: &VerifiedDirectStateGeneration,
    ) -> Result<DirectStateRecoveredValue, String> {
        unreachable!()
    }

    fn recover_pending(
        &self,
        _: &StateImageInstallPermit,
        _: &DirectStateInstallSectionV1,
        _: &VerifiedDirectStateIncoming,
        _: &VerifiedDirectStateGeneration,
    ) -> Result<DirectStateRecoveredValue, String> {
        unreachable!()
    }
}

/// A temporary directory that already satisfies the private-root contract.
///
/// `tempfile::tempdir` creates with mode 0o777 masked by the ambient umask, so under
/// the common 0o022/0o002 umasks it yields 0o755/0o775 and every
/// `PinnedPrivateDirectory::open` on it fails the mode-0700 check. These tests assert
/// direct-state behaviour, not the caller's umask, so they mint their own 0700 root.
pub(super) fn private_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

/// The one shared staging/generations pair for this test process.
///
/// Two properties are required and neither held before. `ensure_private_directory`
/// opens and mode-checks the *parent* of the directory it creates, so a root placed
/// directly under the 1777 system temp dir can never be created: the pair lives
/// inside a 0700 temporary directory that lasts as long as the process. And the
/// creation is not idempotent under concurrency — cases run in parallel and raced
/// each other's `mkdirat` — so it happens exactly once, behind `OnceLock`.
fn provider_test_roots() -> &'static (tempfile::TempDir, PathBuf, PathBuf) {
    static ROOTS: std::sync::OnceLock<(tempfile::TempDir, PathBuf, PathBuf)> =
        std::sync::OnceLock::new();
    ROOTS.get_or_init(|| {
        let root = private_tempdir();
        let staging = root.path().join("staging");
        let generations = root.path().join("generations");
        ensure_private_directory(&staging).unwrap();
        ensure_private_directory(&generations).unwrap();
        (root, staging, generations)
    })
}

pub(super) fn providers() -> Vec<Arc<dyn DirectStateProvider>> {
    let (_, staging, generations) = provider_test_roots();
    DirectStateDomain::ALL
        .into_iter()
        .map(|domain| {
            Arc::new(Provider {
                domain,
                staging: staging.clone(),
                generations: generations.clone(),
            }) as Arc<dyn DirectStateProvider>
        })
        .collect()
}

pub(super) fn flow_providers(root: &Path) -> Vec<Arc<dyn DirectStateProvider>> {
    let staging = root.join("staging");
    let generations = root.join("generations");
    ensure_private_directory(&staging).unwrap();
    ensure_private_directory(&generations).unwrap();
    DirectStateDomain::ALL
        .into_iter()
        .map(|domain| {
            Arc::new(FlowProvider {
                domain,
                staging: staging.clone(),
                generations: generations.clone(),
            }) as Arc<dyn DirectStateProvider>
        })
        .collect()
}

pub(super) fn make_registry(
    authority: &StateImageAuthority,
    providers: Vec<Arc<dyn DirectStateProvider>>,
) -> Result<DirectStateRegistry, String> {
    let root = providers
        .first()
        .ok_or_else(|| "test provider set is empty".to_string())?
        .staging_directory()
        .to_path_buf();
    registry_at(
        authority,
        &root.join("direct-state.pending"),
        &root.join("direct-state.current"),
        providers,
    )
}

pub(super) fn registry_at(
    authority: &StateImageAuthority,
    journal: &Path,
    current: &Path,
    providers: Vec<Arc<dyn DirectStateProvider>>,
) -> Result<DirectStateRegistry, String> {
    let owners = providers
        .iter()
        .map(|provider| provider.owner_manifest())
        .collect::<Result<Vec<_>, _>>()?;
    DirectStateRegistry::new(
        authority,
        journal,
        current,
        DirectStateRegistryContract::new(owners)?,
        providers,
    )
}

pub(super) fn wire_roundtrip(
    registry: &DirectStateRegistry,
    permit: &StateImageInstallPermit,
    captured: CapturedWholeGeneration,
) -> DirectStateTransportGeneration {
    let remote = captured.into_transport().into_authenticated_remote_wire();
    let sections = remote
        .sections
        .into_iter()
        .map(|source| {
            let (manifest, chunks) = source;
            let encoded = rmp_serde::to_vec_named(&manifest).unwrap();
            let decoded = rmp_serde::from_slice(&encoded).unwrap();
            (decoded, chunks)
        })
        .collect();
    DirectStateTransportGeneration::received_remote(
        permit,
        &registry.registry_identity,
        &registry.registry_identity.contract_sha256,
        remote.header,
        sections,
    )
    .unwrap()
}

pub(super) fn write_test_published(
    authority: &StateImageAuthority,
    registry: &DirectStateRegistry,
    path: &Path,
    journal: DirectStateInstallJournalV1,
) -> DurablePublishedJournal {
    let bytes = rmp_serde::to_vec_named(&journal).unwrap();
    let mut options = OpenOptions::new();
    // The published journal's authority descriptor is read back for exact-byte
    // revalidation; a write-only descriptor fails every such retry with EBADF.
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    sync_directory(path.parent().unwrap()).unwrap();
    DurablePublishedJournal {
        authority_identity: authority.identity.clone(),
        registry_identity: registry.registry_identity.clone(),
        journal,
        path: path.to_path_buf(),
        authority_file: file,
        root: registry.journal_root.try_clone_token().unwrap(),
        retirement_quarantine: None,
        retirement_complete: false,
    }
}

pub(super) fn entry<T: DirectStateDomainValue>(value: T) -> DirectStateGenerationEntry {
    DirectStateGenerationEntry::new(Arc::new(value)).unwrap()
}

pub(super) fn generation(authority: &StateImageAuthority) -> Arc<DirectStateGeneration> {
    Arc::new(DirectStateGeneration {
        authority_identity: authority.identity.clone(),
        scope: DirectStateScope::DefaultGlobal {
            control_group: DEFAULT_GROUP,
            control_applied_index: 1,
            authority_epoch: 1,
        },
        sections_sha256: "a".repeat(64),
        entries: BTreeMap::from([
            (
                DirectStateDomain::ClusterControl,
                entry(ClusterControlValue([1; 32], [2; 32])),
            ),
            (
                DirectStateDomain::Blob,
                entry(BlobValue(1, [3; 32], [4; 32])),
            ),
            (
                DirectStateDomain::KeyValue,
                entry(KeyValueValue([5; 32], [6; 32])),
            ),
            (
                DirectStateDomain::TimeSeries,
                entry(TimeSeriesValue([7; 32], [8; 32])),
            ),
            (
                DirectStateDomain::AnalyticsJobs,
                entry(AnalyticsJobsValue([9; 32], [10; 32])),
            ),
            (
                DirectStateDomain::Statecharts,
                entry(StatechartsValue([11; 32], [12; 32])),
            ),
            (
                DirectStateDomain::SqliteCatalog,
                entry(SqliteCatalogValue([13; 32], [14; 32])),
            ),
        ]),
    })
}

/// One journal section for `domain`, bound to `scope` and to the single capture set
/// the closed-domain contract requires every section of a journal to share.
fn placeholder_section(
    domain: DirectStateDomain,
    scope: &DirectStateScope,
) -> DirectStateInstallSectionV1 {
    let source_manifest = DirectStateSectionManifestV1 {
        schema_version: DIRECT_STATE_SCHEMA_VERSION,
        domain,
        scope: scope.clone(),
        capture_set_sha256: "c".repeat(64),
        source_generation_sha256: "d".repeat(64),
        owner_manifest_sha256: "b".repeat(64),
        logical_bytes: 1,
        chunk_count: 1,
        content_sha256: "e".repeat(64),
    };
    let generation_manifest = DirectStateGenerationManifestV1 {
        schema_version: DIRECT_STATE_SCHEMA_VERSION,
        domain,
        scope: scope.clone(),
        owner_manifest_sha256: source_manifest.owner_manifest_sha256.clone(),
        source_manifest_sha256: source_manifest.sha256().unwrap(),
        dynamic_store_authority_digest: Some([1; 32]),
        install_evidence_sha256: [2; 32],
    };
    DirectStateInstallSectionV1 {
        incoming_file: incoming_file_name(domain, &source_manifest.sha256().unwrap()).unwrap(),
        generation_file: generation_file_name(domain, &generation_manifest.sha256().unwrap())
            .unwrap(),
        source_manifest,
        generation_manifest,
    }
}

/// A journal that carries the exact closed domain set in canonical domain order.
/// An empty section list is not a valid journal: `DirectStateInstallJournalV1::validate`
/// requires one section per `DirectStateDomain::ALL` entry, so a section-less
/// placeholder cannot reach any behaviour these tests assert.
pub(super) fn placeholder_journal(phase: DirectStateInstallPhase) -> DirectStateInstallJournalV1 {
    let scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 1,
        authority_epoch: 1,
    };
    let sections = DirectStateDomain::ALL
        .into_iter()
        .map(|domain| placeholder_section(domain, &scope))
        .collect();
    DirectStateInstallJournalV1 {
        schema_version: DIRECT_STATE_SCHEMA_VERSION,
        snapshot_sha256: "a".repeat(64),
        scope,
        phase,
        sections,
    }
}

pub(super) fn capture_binding(
    permit: &StateImageWritePermit,
    scope: DirectStateScope,
    directory: &Path,
) -> DirectStateCaptureBinding {
    let registry_identity = Arc::new(DirectStateRegistryIdentity {
        authority_identity: permit.authority_identity.clone(),
        contract_sha256: "e".repeat(64),
    });
    DirectStateCaptureBinding {
        scope,
        capture_set_sha256: "c".repeat(64),
        source_generation_sha256: "d".repeat(64),
        roots: RegisteredDirectStateRoots {
            registry_identity,
            domain: DirectStateDomain::Blob,
            staging: PinnedPrivateDirectory::open(directory).unwrap(),
            generations: PinnedPrivateDirectory::open(directory).unwrap(),
        },
    }
}
