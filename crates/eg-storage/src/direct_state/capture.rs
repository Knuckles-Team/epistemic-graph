use super::{authority::*, contract::*, filesystem::*, generation::*, transport::*, *};

/// Registry- and domain-affine authority for the two provider roots. Providers
/// receive it only inside coordinator-minted capture/image tokens; arbitrary paths
/// never authorize filesystem effects.
pub struct RegisteredDirectStateRoots {
    pub(super) registry_identity: Arc<DirectStateRegistryIdentity>,
    pub(super) domain: DirectStateDomain,
    pub(super) staging: PinnedPrivateDirectory,
    pub(super) generations: PinnedPrivateDirectory,
}

impl RegisteredDirectStateRoots {
    pub(super) fn try_clone_token(&self) -> Result<Self, String> {
        Ok(Self {
            registry_identity: self.registry_identity.clone(),
            domain: self.domain,
            staging: self.staging.try_clone_token()?,
            generations: self.generations.try_clone_token()?,
        })
    }

    pub(super) fn validate_live(&self) -> Result<(), String> {
        self.staging
            .validate_live("registered direct-state staging root")?;
        self.generations
            .validate_live("registered direct-state generation root")
    }

    pub(super) fn validate_staging_path(&self, path: &Path, label: &str) -> Result<(), String> {
        self.validate_live()?;
        if path != self.staging.path {
            return Err(format!("{label} requested another registry staging root"));
        }
        Ok(())
    }

    pub(super) fn validate_generation_path(&self, path: &Path, label: &str) -> Result<(), String> {
        self.validate_live()?;
        if path != self.generations.path {
            return Err(format!(
                "{label} requested another registry generation root"
            ));
        }
        Ok(())
    }
}

pub struct DirectStateCaptureBinding {
    pub(super) scope: DirectStateScope,
    pub(super) capture_set_sha256: String,
    pub(super) source_generation_sha256: String,
    pub(super) roots: RegisteredDirectStateRoots,
}

impl DirectStateCaptureBinding {
    pub fn scope(&self) -> &DirectStateScope {
        &self.scope
    }

    pub(super) fn validate_domain(&self, domain: DirectStateDomain) -> Result<(), String> {
        if self.roots.domain != domain {
            return Err("direct-state capture binding belongs to another domain".into());
        }
        self.roots.validate_live()
    }
}

/// Build the canonical bounded chunk stream for a stable physical artifact.  The
/// caller must create `path` through a live store's MVCC backup operation and keep
/// it immutable until the returned stream is consumed. Ownership of that temporary
/// capture transfers to this function; the stream removes it on completion, error,
/// or drop so cancellation cannot leak capture files.
pub(super) fn physical_image_source(
    permit: &StateImageWritePermit,
    path: PathBuf,
    domain: DirectStateDomain,
    binding: &DirectStateCaptureBinding,
    owner_manifest_sha256: String,
    configured_max_bytes: u64,
) -> Result<DirectStateSectionSource, String> {
    binding.validate_domain(domain)?;
    let name = binding
        .roots
        .staging
        .validate_path(&path, "direct-state capture")?;
    let capture_root = binding.roots.staging.try_clone_token()?;
    let authority_file = capture_root
        .reader()
        .open_regular(name, "open direct-state physical image")?;
    let mut capture = PinnedCapturePath {
        path: Some(path),
        authority_file: Some(authority_file),
        root: capture_root.try_clone_token()?,
    };
    binding.scope.validate()?;
    validate_sha256("owner manifest", &owner_manifest_sha256)?;
    let path = capture.path.as_deref().expect("pinned direct-state path");
    let input = capture
        .authority_file
        .as_mut()
        .expect("pinned direct-state descriptor");
    capture_root.validate_file(path, input, "direct-state physical image")?;
    let metadata = input
        .metadata()
        .map_err(|error| format!("stat direct-state physical image: {error}"))?;
    let logical_bytes = metadata.len();
    if configured_max_bytes > HARD_MAX_DIRECT_STATE_BYTES || logical_bytes > configured_max_bytes {
        return Err("direct-state physical image exceeds its byte bound".into());
    }
    let content_sha256 = hash_physical_image(&capture_root, path, input, logical_bytes)?;
    let chunk_count = logical_bytes.saturating_add(MAX_DIRECT_STATE_CHUNK_BYTES as u64 - 1)
        / MAX_DIRECT_STATE_CHUNK_BYTES as u64;
    let manifest = DirectStateSectionManifestV1 {
        schema_version: DIRECT_STATE_SCHEMA_VERSION,
        domain,
        scope: binding.scope.clone(),
        capture_set_sha256: binding.capture_set_sha256.clone(),
        source_generation_sha256: binding.source_generation_sha256.clone(),
        owner_manifest_sha256,
        logical_bytes,
        chunk_count,
        content_sha256,
    };
    manifest.validate(configured_max_bytes)?;
    let manifest_sha256 = manifest.sha256()?;
    let (input, source_path) = capture.into_parts();
    let chunks = PhysicalFileChunkStream {
        file: input,
        source_path,
        root: capture_root.try_clone_token()?,
        manifest_sha256,
        ordinal: 0,
        remaining: logical_bytes,
    };
    Ok(DirectStateSectionSource::new(
        permit,
        binding.roots.registry_identity.clone(),
        manifest,
        Box::new(chunks),
        capture_root,
    ))
}

fn hash_physical_image(
    capture_root: &PinnedPrivateDirectory,
    path: &Path,
    input: &mut File,
    logical_bytes: u64,
) -> Result<String, String> {
    let mut content_hasher = Sha256::new();
    let mut buffer = vec![0_u8; MAX_DIRECT_STATE_CHUNK_BYTES];
    let mut observed = 0_u64;
    loop {
        let count = input
            .read(&mut buffer)
            .map_err(|error| format!("hash direct-state physical image: {error}"))?;
        if count == 0 {
            break;
        }
        observed = observed
            .checked_add(count as u64)
            .ok_or_else(|| "direct-state physical image length overflow".to_string())?;
        if observed > logical_bytes {
            return Err("direct-state physical image changed during capture".into());
        }
        content_hasher.update(&buffer[..count]);
    }
    capture_root.validate_file(path, input, "hashed direct-state physical image")?;
    input
        .seek(SeekFrom::Start(0))
        .map_err(|error| format!("rewind direct-state physical image: {error}"))?;
    if observed != logical_bytes {
        return Err("direct-state physical image changed during capture".into());
    }
    Ok(format!("{:x}", content_hasher.finalize()))
}

pub(super) struct PinnedCapturePath {
    pub(super) path: Option<PathBuf>,
    pub(super) authority_file: Option<File>,
    pub(super) root: PinnedPrivateDirectory,
}

impl PinnedCapturePath {
    pub(super) fn into_parts(mut self) -> (File, PathBuf) {
        let file = self
            .authority_file
            .take()
            .expect("pinned direct-state capture descriptor");
        let path = self.path.take().expect("pinned direct-state capture path");
        (file, path)
    }
}

impl Drop for PinnedCapturePath {
    fn drop(&mut self) {
        let (Some(path), Some(authority)) = (&self.path, &self.authority_file) else {
            return;
        };
        if let Ok(name) = self.root.validate_path(path, "failed direct-state capture") {
            let _ = self.root.mutations().retire_unjournaled_exact(
                name,
                authority,
                "failed direct-state capture",
            );
        }
    }
}

/// Allocate the one shared crash-collectable capture path, invoke the store's
/// supported MVCC snapshot operation, and transfer cleanup ownership to the
/// bounded chunk stream. Providers must not invent domain-local capture prefixes.
pub fn capture_physical_image_source<F>(
    permit: &StateImageWritePermit,
    staging_directory: &Path,
    domain: DirectStateDomain,
    binding: &DirectStateCaptureBinding,
    owner_manifest_sha256: String,
    configured_max_bytes: u64,
    capture: F,
) -> Result<DirectStateSectionSource, String>
where
    F: FnOnce(&Path) -> Result<(), String>,
{
    binding.validate_domain(domain)?;
    binding
        .roots
        .validate_staging_path(staging_directory, "direct-state capture")?;
    collect_capture_orphans(&binding.roots.staging, domain)?;
    let ordinal = NEXT_DIRECT_STATE_TEMP.fetch_add(1, Ordering::Relaxed);
    let name = capture_file_name(domain, std::process::id(), ordinal)?;
    let path = staging_directory.join(&name);
    if binding
        .roots
        .staging
        .reader()
        .open_optional_regular(&name, "pin unique direct-state capture path")?
        .is_some()
    {
        return Err("unique direct-state capture path unexpectedly exists".into());
    }
    let descriptor_path = binding.roots.staging.reader().descriptor_path(&name)?;
    if let Err(error) = capture(&descriptor_path) {
        cleanup_capture_attempt(&binding.roots.staging, &name).map_err(|cleanup| {
            format!("{error}; cleanup failed direct-state capture attempt: {cleanup}")
        })?;
        return Err(error);
    }
    physical_image_source(
        permit,
        path,
        domain,
        binding,
        owner_manifest_sha256,
        configured_max_bytes,
    )
}

pub(super) struct PhysicalFileChunkStream {
    pub(super) file: File,
    pub(super) source_path: PathBuf,
    pub(super) root: PinnedPrivateDirectory,
    pub(super) manifest_sha256: String,
    pub(super) ordinal: u64,
    pub(super) remaining: u64,
}

impl Drop for PhysicalFileChunkStream {
    fn drop(&mut self) {
        if let Ok(name) = self
            .root
            .validate_path(&self.source_path, "direct-state capture cleanup")
        {
            let _ = self.root.mutations().retire_unjournaled_exact(
                name,
                &self.file,
                "direct-state capture cleanup",
            );
        }
    }
}

impl DirectStateChunkStream for PhysicalFileChunkStream {
    fn next_chunk(&mut self) -> Result<Option<DirectStateChunkV1>, String> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let expected = self.remaining.min(MAX_DIRECT_STATE_CHUNK_BYTES as u64) as usize;
        let mut bytes = vec![0_u8; expected];
        self.file
            .read_exact(&mut bytes)
            .map_err(|error| format!("read direct-state physical image chunk: {error}"))?;
        self.remaining -= expected as u64;
        let chunk = DirectStateChunkV1 {
            manifest_sha256: self.manifest_sha256.clone(),
            ordinal: self.ordinal,
            // Physical redb images are opaque byte streams, not logical row batches.
            item_count: 0,
            sha256: hex_sha256(&bytes),
            bytes,
        };
        self.ordinal += 1;
        Ok(Some(chunk))
    }
}

pub(super) fn collect_capture_orphans(
    directory: &PinnedPrivateDirectory,
    domain: DirectStateDomain,
) -> Result<(), String> {
    directory.validate_live("direct-state capture root")?;
    for (ordinal, entry) in std::fs::read_dir(directory.reader().descriptor_root_path())
        .map_err(|error| format!("scan direct-state capture directory: {error}"))?
        .enumerate()
    {
        if ordinal >= MAX_DIRECT_STATE_GC_ENTRIES {
            return Err("direct-state capture directory exceeds its GC entry bound".into());
        }
        let entry = entry.map_err(|error| format!("read direct-state capture entry: {error}"))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "direct-state capture entry name is not UTF-8".to_string())?;
        let retired_base = name.strip_prefix('.').and_then(|name| {
            name.split_once(".retired.")
                .map(|(base, _)| base.to_string())
        });
        if retired_base
            .as_deref()
            .is_some_and(|base| is_managed_capture_name(base, domain))
        {
            let expected = directory
                .reader()
                .open_regular(&name, "pin retired capture residue")?;
            let mut retirement = ExactRetirement {
                root: directory.try_clone_token()?,
                expected,
                live_name: retired_base.expect("checked retired base"),
                quarantine_name: name,
                label: "retired direct-state capture residue",
                phase: ExactRetirementPhase::Quarantined,
            };
            retirement.retry()?;
        } else if is_managed_capture_name(&name, domain) {
            cleanup_capture_attempt(directory, &name)?;
        }
    }
    Ok(())
}

pub(super) fn cleanup_capture_attempt(
    directory: &PinnedPrivateDirectory,
    name: &str,
) -> Result<(), String> {
    let Some(authority) = directory
        .reader()
        .open_optional_regular(name, "pin direct-state capture cleanup")?
    else {
        return Ok(());
    };
    directory
        .mutations()
        .retire_unjournaled_exact(name, &authority, "direct-state capture cleanup")
}

pub(super) fn capture_file_name(
    domain: DirectStateDomain,
    process_id: u32,
    ordinal: u64,
) -> Result<String, String> {
    if process_id == 0 || ordinal == 0 {
        return Err("direct-state capture identity must be nonzero".into());
    }
    Ok(format!(
        ".direct-state-{}.capture.{process_id}.{ordinal}",
        domain.as_str()
    ))
}

pub(super) fn is_managed_capture_name(name: &str, domain: DirectStateDomain) -> bool {
    let prefix = format!(".direct-state-{}.capture.", domain.as_str());
    let Some(suffix) = name.strip_prefix(&prefix) else {
        return false;
    };
    let mut parts = suffix.split('.');
    let (Some(process), Some(ordinal), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    let Ok(process_id) = process.parse::<u32>() else {
        return false;
    };
    let Ok(sequence) = ordinal.parse::<u64>() else {
        return false;
    };
    process_id != 0
        && sequence != 0
        && process == process_id.to_string()
        && ordinal == sequence.to_string()
}

pub(super) fn is_managed_digest_name(name: &str, prefix: &str, suffix: &str) -> bool {
    let Some(digest) = name
        .strip_prefix(prefix)
        .and_then(|name| name.strip_suffix(suffix))
    else {
        return false;
    };
    validate_sha256("managed direct-state file", digest).is_ok()
}
