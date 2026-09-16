//! Descriptor-bound, disk-bounded inspection. Preview bytes never have a name
//! when native recovery can write them, and never acquire serving authority.

use super::*;
#[cfg(target_os = "linux")]
use crate::direct_state::MAX_DIRECT_STATE_CHUNK_BYTES;
use crate::direct_state::{PinnedPrivateDirectory, HARD_MAX_DIRECT_STATE_BYTES};
use redb::backends::FileBackend;
use redb::StorageBackend;
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
use std::io;
#[cfg(target_os = "linux")]
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, Ordering};

mod bounded;
use bounded::BoundedPreviewBackend;

pub(super) fn validate_options(root: &Path, maximum: u64) -> Result<(), String> {
    if !cfg!(target_os = "linux") {
        return Err("SQL checkpoint private inspection is supported only on Linux".into());
    }
    if !root.is_absolute()
        || root.starts_with("/tmp")
        || root.components().any(|part| part.as_os_str() == ".cache")
    {
        return Err(
            "SQL checkpoint inspection requires an explicit private LOCAL staging root".into(),
        );
    }
    if maximum == 0 || maximum > HARD_MAX_DIRECT_STATE_BYTES {
        return Err("SQL checkpoint preview budget is outside storage limits".into());
    }
    Ok(())
}

/// Lock exactly once through native FileBackend, then prove that this filesystem
/// did not take its unsupported-lock fallback. The probe has a separate open
/// description and must conflict; it is never a logical inspection source.
pub(super) fn exclusive_backend(file: File, path: &Path) -> Result<(FileBackend, File), String> {
    let pinned = file.try_clone().map_err(|error| error.to_string())?;
    let backend = FileBackend::new(file).map_err(|error| error.to_string())?;
    let supported = prove_exclusive_lock(&pinned, path);
    if let Err(error) = supported {
        backend
            .close()
            .map_err(|close| format!("{error}; release SQL file lock: {close}"))?;
        return Err(error);
    }
    Ok((backend, pinned))
}

#[cfg(target_os = "linux")]
fn prove_exclusive_lock(pinned: &File, path: &Path) -> Result<(), String> {
    let probe = File::open(path).map_err(|error| error.to_string())?;
    // The scratch file is deliberately still empty at this point. This proves
    // lock identity, not the nonempty original predecessor's write authority.
    let expected = pinned.metadata().map_err(|error| error.to_string())?;
    let actual = probe.metadata().map_err(|error| error.to_string())?;
    if !expected.is_file()
        || !actual.is_file()
        || expected.dev() != actual.dev()
        || expected.ino() != actual.ino()
    {
        return Err("SQL checkpoint lock probe is not the pinned physical file".into());
    }
    match probe.try_lock_shared() {
        Err(std::fs::TryLockError::WouldBlock) => Ok(()),
        Ok(()) => Err("SQL checkpoint filesystem did not enforce the exclusive lock".into()),
        Err(error) => Err(format!(
            "SQL checkpoint lock support could not be proved: {error}"
        )),
    }
}

#[cfg(not(target_os = "linux"))]
fn prove_exclusive_lock(_pinned: &File, _path: &Path) -> Result<(), String> {
    Err("SQL checkpoint lock proof is unsupported on this platform".into())
}

pub(super) fn inspect(
    source: &File,
    incarnation: &StoreIncarnation,
    physical: &PhysicalStoreIdentity,
    integrity: Option<&dyn PrivatePayloadIntegrity>,
    options: &SqlSourceCheckpointInspectionOptions,
) -> Result<(OwnerManifest, StrictRecoveryEvidence, [u8; 32]), String> {
    validate_source_size(source, options.max_bytes)?;
    let mut scratch = SqlPreviewScratch::create(&options.staging_root)?;
    let (backend, preview_file) = exclusive_backend(
        scratch
            .file
            .try_clone()
            .map_err(|error| error.to_string())?,
        &scratch.path,
    )?;
    scratch.retire()?;
    reserve(&preview_file, options.max_bytes).map_err(|error| error.to_string())?;
    let fingerprint = copy_and_hash(source, &preview_file, options.max_bytes)?;
    let poison = Arc::new(AtomicBool::new(false));
    let bounded =
        BoundedPreviewBackend::new(backend, preview_file, options.max_bytes, poison.clone());
    let result = inspect_recovered_preview(bounded, incarnation, physical, integrity);
    // The function above explicitly releases every read and the Database; its
    // allocator/trim Drop writes are included in the surviving poison state.
    if poison.load(Ordering::Acquire) {
        return Err("SQL checkpoint preview I/O failed, including native close".into());
    }
    let (manifest, evidence) = result?;
    Ok((manifest, evidence, fingerprint))
}

fn inspect_recovered_preview(
    backend: BoundedPreviewBackend,
    incarnation: &StoreIncarnation,
    physical: &PhysicalStoreIdentity,
    integrity: Option<&dyn PrivatePayloadIntegrity>,
) -> Result<(OwnerManifest, StrictRecoveryEvidence), String> {
    let database = upgrade_builder()
        .create_with_backend(backend)
        .map_err(|error| error.to_string())?;
    let result = inspect_preview_read(&database, incarnation, physical, integrity);
    drop(database);
    result
}

fn inspect_preview_read(
    database: &Database,
    incarnation: &StoreIncarnation,
    physical: &PhysicalStoreIdentity,
    integrity: Option<&dyn PrivatePayloadIntegrity>,
) -> Result<(OwnerManifest, StrictRecoveryEvidence), String> {
    let read = database.begin_read().map_err(|error| error.to_string())?;
    let result = inspect_predecessor(&read, incarnation, physical, integrity);
    drop(read);
    result
}

struct SqlPreviewScratch {
    root: PinnedPrivateDirectory,
    path: PathBuf,
    name: String,
    file: File,
    retired: bool,
}

impl SqlPreviewScratch {
    fn create(path: &Path) -> Result<Self, String> {
        let root = PinnedPrivateDirectory::open(path)?;
        let name = format!("sql-checkpoint-preview-{}", uuid::Uuid::new_v4());
        let file = root
            .mutations()
            .create_new(&name, "create private SQL checkpoint preview")?;
        Ok(Self {
            root,
            path: path.join(&name),
            name,
            file,
            retired: false,
        })
    }

    fn retire(&mut self) -> Result<(), String> {
        self.root.validate_live("SQL checkpoint preview root")?;
        self.root
            .validate_file(&self.path, &self.file, "SQL checkpoint preview")?;
        validate_links(&self.file, 1)?;
        self.root.mutations().retire_unjournaled_exact(
            &self.name,
            &self.file,
            "SQL checkpoint preview",
        )?;
        validate_links(&self.file, 0)?;
        self.retired = true;
        Ok(())
    }
}

impl Drop for SqlPreviewScratch {
    fn drop(&mut self) {
        if !self.retired {
            // No data is copied or allocated before exact retirement. Failure
            // can leave only an empty managed name/quarantine for scoped cleanup.
            let _ = self.root.mutations().retire_unjournaled_exact(
                &self.name,
                &self.file,
                "failed SQL checkpoint preview",
            );
        }
    }
}

#[cfg(target_os = "linux")]
fn validate_links(file: &File, expected: u64) -> Result<(), String> {
    if file.metadata().map_err(|error| error.to_string())?.nlink() != expected {
        return Err("SQL checkpoint preview has unexpected physical links".into());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn validate_links(_file: &File, _expected: u64) -> Result<(), String> {
    Err("SQL checkpoint anonymous preview is unsupported on this platform".into())
}

#[cfg(target_os = "linux")]
pub(super) fn reserve(file: &File, maximum: u64) -> io::Result<()> {
    rustix::fs::fallocate(file, rustix::fs::FallocateFlags::KEEP_SIZE, 0, maximum)
        .map_err(Into::into)
}

#[cfg(not(target_os = "linux"))]
pub(super) fn reserve(_file: &File, _maximum: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SQL checkpoint allocation is unsupported",
    ))
}

fn copy_and_hash(source: &File, target: &File, maximum: u64) -> Result<[u8; 32], String> {
    let size = validate_source_size(source, maximum)?;
    let fingerprint = stream_fingerprint(source, size, Some(target))?;
    if source.metadata().map_err(|error| error.to_string())?.len() != size {
        return Err("SQL checkpoint source changed length during inspection".into());
    }
    target.sync_data().map_err(|error| error.to_string())?;
    Ok(fingerprint)
}

fn validate_source_size(source: &File, maximum: u64) -> Result<u64, String> {
    let size = source.metadata().map_err(|error| error.to_string())?.len();
    if size == 0 || size > maximum {
        return Err("SQL checkpoint source exceeds the configured preview budget".into());
    }
    Ok(size)
}

pub(super) fn validate_fingerprint(
    file: &File,
    expected_size: u64,
    expected: [u8; 32],
) -> Result<(), String> {
    let size = file.metadata().map_err(|error| error.to_string())?.len();
    if size != expected_size
        || size == 0
        || size > HARD_MAX_DIRECT_STATE_BYTES
        || stream_fingerprint(file, size, None)? != expected
    {
        return Err("SQL checkpoint physical bytes changed after inspection".into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn stream_fingerprint(source: &File, size: u64, target: Option<&File>) -> Result<[u8; 32], String> {
    let mut buffer = vec![0_u8; MAX_DIRECT_STATE_CHUNK_BYTES];
    let mut hash = Sha256::new();
    let mut offset = 0_u64;
    while offset < size {
        let count = (size - offset).min(buffer.len() as u64) as usize;
        source
            .read_exact_at(&mut buffer[..count], offset)
            .map_err(|error| error.to_string())?;
        hash.update(&buffer[..count]);
        if let Some(target) = target {
            target
                .write_all_at(&buffer[..count], offset)
                .map_err(|error| error.to_string())?;
        }
        offset += count as u64;
    }
    Ok(hash.finalize().into())
}

#[cfg(not(target_os = "linux"))]
fn stream_fingerprint(
    _source: &File,
    _size: u64,
    _target: Option<&File>,
) -> Result<[u8; 32], String> {
    Err("SQL checkpoint descriptor I/O is unsupported on this platform".into())
}
