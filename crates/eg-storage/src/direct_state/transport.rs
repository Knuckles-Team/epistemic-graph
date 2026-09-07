use super::{authority::*, capture::*, contract::*, filesystem::*, generation::*, *};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectStateChunk {
    pub manifest_sha256: String,
    pub ordinal: u64,
    pub item_count: u64,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
    pub sha256: String,
}

/// Object-safe pull stream.  It keeps full physical images out of RAM and permits
/// snapshot transport to spool to private files before redb validation/open.
pub trait DirectStateChunkStream: Send {
    fn next_chunk(&mut self) -> Result<Option<DirectStateChunk>, String>;
}

impl<I> DirectStateChunkStream for I
where
    I: Iterator<Item = Result<DirectStateChunk, String>> + Send,
{
    fn next_chunk(&mut self) -> Result<Option<DirectStateChunk>, String> {
        self.next().transpose()
    }
}

pub struct DirectStateSectionSource {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) manifest: DirectStateSectionManifest,
    pub(super) chunks: Box<dyn DirectStateChunkStream>,
    pub(super) capture_root: Option<PinnedPrivateDirectory>,
}

impl DirectStateSectionSource {
    pub(super) fn new(
        permit: &StateImageWritePermit,
        registry_identity: Arc<DirectStateRegistryIdentity>,
        manifest: DirectStateSectionManifest,
        chunks: Box<dyn DirectStateChunkStream>,
        capture_root: PinnedPrivateDirectory,
    ) -> Self {
        Self {
            authority_identity: permit.authority_identity.clone(),
            registry_identity,
            manifest,
            chunks,
            capture_root: Some(capture_root),
        }
    }

    pub fn manifest(&self) -> &DirectStateSectionManifest {
        &self.manifest
    }

    /// Reconstitute one wire section. It cannot be validated independently: the
    /// registry accepts it only inside an exact closed whole-generation set.
    pub(super) fn received(
        permit: &StateImageInstallPermit,
        registry_identity: &Arc<DirectStateRegistryIdentity>,
        manifest: DirectStateSectionManifest,
        chunks: Box<dyn DirectStateChunkStream>,
    ) -> Self {
        Self {
            authority_identity: permit.authority_identity.clone(),
            registry_identity: registry_identity.clone(),
            manifest,
            chunks,
            capture_root: None,
        }
    }

    /// Consume one already-bounded section for sibling snapshot transport.
    /// The only constructor reachable by that transport is the closed aggregate
    /// [`DirectStateTransportGeneration::into_authenticated_remote_wire`], so exposing the drain does
    /// not permit callers to mint or validate an incomplete generation.
    pub fn into_parts(
        self,
    ) -> (
        DirectStateSectionManifest,
        Box<dyn DirectStateChunkStream>,
    ) {
        (self.manifest, self.chunks)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectStateTransportHeader {
    pub(super) schema_version: u16,
    pub(super) registry_contract_sha256: String,
    pub(super) capture_set_sha256: String,
    pub(super) source_generation_sha256: String,
    pub(super) scope: DirectStateScope,
}

/// Closed transport authority. Extraction retains the aggregate header beside
/// the canonical section vector; reception can therefore never infer whole-set
/// identity from an attacker-selected first section.
pub struct DirectStateTransportGeneration {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) header: DirectStateTransportHeader,
    pub(super) sources: Vec<DirectStateSectionSource>,
}

/// The transport's ordered wire sections: each canonical section manifest paired
/// with the chunk stream that carries its bytes. Named once because the wire
/// payload, its two accessors and the reception entrypoint all name the same set.
pub type DirectStateWireSections = Vec<(
    DirectStateSectionManifest,
    Box<dyn DirectStateChunkStream>,
)>;

/// Serializable transport payload. It deliberately contains no process-local
/// registry capability; only `DirectStateRegistry::receive_authenticated_remote`
/// may bind it to a destination after the outer Raft snapshot envelope has been
/// authenticated.
pub struct DirectStateRemoteTransport {
    pub(super) header: DirectStateTransportHeader,
    pub(super) sections: DirectStateWireSections,
}

impl DirectStateRemoteTransport {
    pub fn from_wire_parts(
        header: DirectStateTransportHeader,
        sections: DirectStateWireSections,
    ) -> Self {
        Self { header, sections }
    }

    pub fn into_wire_parts(
        self,
    ) -> (DirectStateTransportHeader, DirectStateWireSections) {
        (self.header, self.sections)
    }
}

impl DirectStateTransportGeneration {
    pub fn into_authenticated_remote_wire(self) -> DirectStateRemoteTransport {
        DirectStateRemoteTransport {
            header: self.header,
            sections: self
                .sources
                .into_iter()
                .map(DirectStateSectionSource::into_parts)
                .collect(),
        }
    }

    pub(super) fn received_remote(
        permit: &StateImageInstallPermit,
        registry_identity: &Arc<DirectStateRegistryIdentity>,
        expected_contract_sha256: &str,
        header: DirectStateTransportHeader,
        sections: DirectStateWireSections,
    ) -> Result<Self, String> {
        permit.validate_affinity(&registry_identity.authority_identity)?;
        if header.schema_version != DIRECT_STATE_SCHEMA_VERSION {
            return Err("unsupported direct-state transport header schema".into());
        }
        validate_sha256("transport capture set", &header.capture_set_sha256)?;
        validate_sha256(
            "transport source generation",
            &header.source_generation_sha256,
        )?;
        validate_sha256(
            "transport registry contract",
            &header.registry_contract_sha256,
        )?;
        if header.registry_contract_sha256 != expected_contract_sha256 {
            return Err("remote direct-state owner contract differs from this registry".into());
        }
        header.scope.validate()?;
        let sources = sections
            .into_iter()
            .map(|(manifest, chunks)| {
                DirectStateSectionSource::received(permit, registry_identity, manifest, chunks)
            })
            .collect();
        let transport = Self {
            authority_identity: permit.authority_identity.clone(),
            registry_identity: registry_identity.clone(),
            header,
            sources,
        };
        transport.validate_closed()?;
        Ok(transport)
    }

    pub(super) fn validate_closed(&self) -> Result<(), String> {
        if self.header.registry_contract_sha256 != self.registry_identity.contract_sha256 {
            return Err("transported direct-state owner contract changed".into());
        }
        if self.sources.len() != DirectStateDomain::ALL.len() {
            return Err("transported direct-state generation is incomplete".into());
        }
        for (expected, source) in DirectStateDomain::ALL.iter().zip(&self.sources) {
            if !Arc::ptr_eq(&source.authority_identity, &self.authority_identity)
                || !Arc::ptr_eq(&source.registry_identity, &self.registry_identity)
                || source.manifest.domain != *expected
                || source.manifest.scope != self.header.scope
                || source.manifest.capture_set_sha256 != self.header.capture_set_sha256
                || source.manifest.source_generation_sha256 != self.header.source_generation_sha256
            {
                return Err("transported direct-state generation contains a mixed section".into());
            }
        }
        Ok(())
    }
}

/// Non-Clone proof that all seven immutable sources were captured under one
/// exclusive permit from one exact whole Current generation. Callers cannot
/// assemble this token from separately captured domain images.
pub struct CapturedWholeGeneration {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) registry_contract_sha256: String,
    pub(super) capture_set_sha256: String,
    pub(super) source_generation_sha256: String,
    pub(super) scope: DirectStateScope,
    pub(super) sources: Vec<DirectStateSectionSource>,
}

impl CapturedWholeGeneration {
    pub fn scope(&self) -> &DirectStateScope {
        &self.scope
    }

    pub fn capture_set_sha256(&self) -> &str {
        &self.capture_set_sha256
    }

    pub fn source_generation_sha256(&self) -> &str {
        &self.source_generation_sha256
    }

    /// Transfer the closed captured set to the streaming transport. The receiver
    /// must reconstruct it only through `DirectStateRegistry::receive_all`, which
    /// rechecks the two serialized set identities and canonical domain order.
    pub fn into_transport(self) -> DirectStateTransportGeneration {
        DirectStateTransportGeneration {
            authority_identity: self.authority_identity,
            registry_identity: self.registry_identity,
            header: DirectStateTransportHeader {
                schema_version: DIRECT_STATE_SCHEMA_VERSION,
                registry_contract_sha256: self.registry_contract_sha256,
                capture_set_sha256: self.capture_set_sha256,
                source_generation_sha256: self.source_generation_sha256,
                scope: self.scope,
            },
            sources: self.sources,
        }
    }

    pub(super) fn validate_closed(&self) -> Result<(), String> {
        validate_sha256("capture set", &self.capture_set_sha256)?;
        validate_sha256("source generation", &self.source_generation_sha256)?;
        validate_sha256("registry contract", &self.registry_contract_sha256)?;
        if self.registry_contract_sha256 != self.registry_identity.contract_sha256 {
            return Err("captured direct-state owner contract changed".into());
        }
        if self.sources.len() != DirectStateDomain::ALL.len() {
            return Err("captured direct-state generation is incomplete".into());
        }
        for (expected, source) in DirectStateDomain::ALL.iter().zip(&self.sources) {
            if !Arc::ptr_eq(&source.authority_identity, &self.authority_identity)
                || !Arc::ptr_eq(&source.registry_identity, &self.registry_identity)
                || source.manifest.domain != *expected
                || source.manifest.scope != self.scope
                || source.manifest.capture_set_sha256 != self.capture_set_sha256
                || source.manifest.source_generation_sha256 != self.source_generation_sha256
            {
                return Err("captured direct-state generation contains a mixed section".into());
            }
        }
        Ok(())
    }
}

/// Reader which verifies transport bounds and hashes before a provider can obtain
/// a validated token.  Providers may spool its bytes to a private staged file.
pub struct ValidatingDirectStateChunks {
    pub(super) manifest_sha256: String,
    pub(super) expected_chunks: u64,
    pub(super) expected_bytes: u64,
    pub(super) expected_content_sha256: String,
    pub(super) next_ordinal: u64,
    pub(super) observed_bytes: u64,
    pub(super) content_hasher: Sha256,
    pub(super) chunks: Box<dyn DirectStateChunkStream>,
}

impl ValidatingDirectStateChunks {
    pub fn next_chunk(&mut self) -> Result<Option<DirectStateChunk>, String> {
        let Some(chunk) = self.chunks.next_chunk()? else {
            return Ok(None);
        };
        validate_chunk_header(self, &chunk)?;
        validate_chunk_payload(&chunk)?;
        self.observed_bytes = self
            .observed_bytes
            .checked_add(chunk.bytes.len() as u64)
            .ok_or_else(|| "direct-state byte count overflow".to_string())?;
        if self.observed_bytes > self.expected_bytes {
            return Err("direct-state chunks exceed declared byte length".into());
        }
        self.content_hasher.update(&chunk.bytes);
        self.next_ordinal += 1;
        Ok(Some(chunk))
    }

    /// Stream one framed physical image to a private create-new file and fsync it.
    /// The provider must subsequently open and strictly validate the image before
    /// returning from `validate_payload`; spooling alone is never admission.
    pub fn spool_physical_image(
        &mut self,
        permit: &StateImageWritePermit,
        roots: &RegisteredDirectStateRoots,
    ) -> Result<DirectStatePhysicalImage, String> {
        permit.validate_affinity(&roots.registry_identity.authority_identity)?;
        roots.validate_live()?;
        let domain = roots.domain;
        let file_name = incoming_file_name(domain, &self.manifest_sha256)?;
        let path = roots.staging.path.join(&file_name);
        let mut file = roots
            .staging
            .mutations()
            .create_new(&file_name, "create direct-state staged image")?;
        let result = (|| {
            while let Some(chunk) = self.next_chunk()? {
                file.write_all(&chunk.bytes)
                    .map_err(|error| format!("write direct-state staged image: {error}"))?;
            }
            file.sync_all()
                .map_err(|error| format!("fsync direct-state staged image: {error}"))?;
            roots.staging.sync()?;
            Ok(())
        })();
        if let Err(error) = result {
            if roots
                .staging
                .validate_file(&path, &file, "failed direct-state incoming spool")
                .is_ok()
            {
                let _ = roots
                    .staging
                    .mutations()
                    .unlink(&file_name, "retire failed direct-state incoming spool");
                let _ = roots.staging.sync();
            }
            return Err(error);
        }
        Ok(DirectStatePhysicalImage {
            authority_identity: permit.authority_identity.clone(),
            registry_identity: roots.registry_identity.clone(),
            roots: roots.try_clone_token()?,
            path,
            domain,
            manifest_sha256: self.manifest_sha256.clone(),
            authority_file: file,
            cleanup_on_drop: true,
        })
    }

    pub(super) fn finish(self) -> Result<(), String> {
        if self.next_ordinal != self.expected_chunks || self.observed_bytes != self.expected_bytes {
            return Err("direct-state stream ended before its declared boundary".into());
        }
        let observed = format!("{:x}", self.content_hasher.finalize());
        if observed != self.expected_content_sha256 {
            return Err("direct-state section content digest mismatch".into());
        }
        Ok(())
    }
}

fn validate_chunk_header(
    stream: &ValidatingDirectStateChunks,
    chunk: &DirectStateChunk,
) -> Result<(), String> {
    if stream.next_ordinal >= stream.expected_chunks || chunk.ordinal != stream.next_ordinal {
        return Err("direct-state chunk ordinal is missing, duplicated, or reordered".into());
    }
    if chunk.manifest_sha256 != stream.manifest_sha256 {
        return Err("direct-state chunk is bound to a different manifest".into());
    }
    Ok(())
}

fn validate_chunk_payload(chunk: &DirectStateChunk) -> Result<(), String> {
    validate_sha256("direct-state chunk", &chunk.sha256)?;
    if chunk.bytes.is_empty() || chunk.bytes.len() > MAX_DIRECT_STATE_CHUNK_BYTES {
        return Err("direct-state chunk byte length is outside bounds".into());
    }
    if chunk.item_count > MAX_DIRECT_STATE_CHUNK_ITEMS as u64 {
        return Err("direct-state chunk item count is outside bounds".into());
    }
    if hex_sha256(&chunk.bytes) != chunk.sha256 {
        return Err("direct-state chunk digest mismatch".into());
    }
    Ok(())
}
