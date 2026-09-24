//! Durable side-file publication for the observability store: a validated,
//! private directory authority, atomic tmp+rename snapshot writes, bounded
//! snapshot reads, and the BUG-016 trace snapshot (write and recovery).

use std::io::Read as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex as StdMutex, OnceLock};

use crate::lock_recovery::LockRecovery;

#[cfg(feature = "traces")]
use super::ObsState;

pub(super) const MAX_SNAPSHOT_BYTES: usize = eg_types::msgpack::MAX_PROPERTY_BYTES;
pub(super) const SNAPSHOT_WRITE_ERROR: &str = "observability snapshot write failed";
pub(super) const SNAPSHOT_READ_ERROR: &str = "observability snapshot read failed";
pub(super) const OBS_PERSISTENCE_DIRECTORY_ERROR: &str =
    "observability persistence directory is unavailable";

#[cfg(all(
    target_os = "linux",
    any(
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "powerpc",
        target_arch = "powerpc64"
    )
))]
const LINUX_O_DIRECTORY: i32 = 16_384;
#[cfg(all(
    target_os = "linux",
    any(
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "powerpc",
        target_arch = "powerpc64"
    )
))]
const LINUX_O_NOFOLLOW: i32 = 32_768;
#[cfg(all(
    target_os = "linux",
    any(target_arch = "sparc", target_arch = "sparc64")
))]
const LINUX_O_NONBLOCK: i32 = 16_384;
#[cfg(all(
    target_os = "linux",
    any(target_arch = "sparc", target_arch = "sparc64")
))]
const LINUX_O_CLOEXEC: i32 = 4_194_304;
#[cfg(all(
    target_os = "linux",
    any(
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6"
    )
))]
const LINUX_O_NONBLOCK: i32 = 128;
#[cfg(all(
    target_os = "linux",
    not(any(
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "powerpc",
        target_arch = "powerpc64"
    ))
))]
const LINUX_O_DIRECTORY: i32 = 65_536;
#[cfg(all(
    target_os = "linux",
    not(any(
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "powerpc",
        target_arch = "powerpc64"
    ))
))]
const LINUX_O_NOFOLLOW: i32 = 131_072;
#[cfg(all(
    target_os = "linux",
    not(any(
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6",
        target_arch = "sparc",
        target_arch = "sparc64"
    ))
))]
const LINUX_O_NONBLOCK: i32 = 2_048;
#[cfg(all(
    target_os = "linux",
    not(any(target_arch = "sparc", target_arch = "sparc64"))
))]
const LINUX_O_CLOEXEC: i32 = 524_288;

/// BUG-016 durable path: `{persist_dir}/obs/traces.msgpack`.
#[cfg(feature = "traces")]
pub(super) fn traces_snapshot_path(obs_base: &Path) -> PathBuf {
    obs_base.join("traces.msgpack")
}

/// Serialize snapshot publication inside this process. The authoritative bytes
/// still live in the destination file; this lock only prevents concurrent sweep
/// and ingest workers from racing replacement of one stream's side file.
pub(super) fn snapshot_write_lock() -> &'static StdMutex<()> {
    static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| StdMutex::new(()))
}

/// An opened, validated directory authority. On Unix it must not be writable by
/// group/other, making same-identity processes the explicit filesystem trust
/// boundary; the process lock serializes writers within this engine. On Linux, all subsequent
/// child operations resolve through `/proc/self/fd/<fd>` so renaming or replacing
/// an ancestor cannot redirect a transaction after validation.
pub(super) struct SnapshotDirectory {
    handle: std::fs::File,
    pub(super) io_path: PathBuf,
    original_path: PathBuf,
}

impl SnapshotDirectory {
    pub(super) fn open(path: &Path, create: bool, error: &str) -> Result<Self, String> {
        if create {
            require_unsymlinked_existing_ancestry(path, error)?;
            create_private_directory_tree(path, error)?;
        }
        require_unsymlinked_directory_tree(path, error)?;
        let handle = open_directory_nofollow(path).map_err(|_| error.to_string())?;
        require_trusted_directory(&handle, error)?;
        require_matching_opened_path(&handle, path, error)?;
        Ok(Self::from_opened(handle, path.to_path_buf()))
    }

    fn from_opened(handle: std::fs::File, original_path: PathBuf) -> Self {
        #[cfg(target_os = "linux")]
        let io_path = {
            use std::os::fd::AsRawFd as _;
            PathBuf::from(format!("/proc/self/fd/{}", handle.as_raw_fd()))
        };
        #[cfg(not(target_os = "linux"))]
        let io_path = original_path.clone();
        Self {
            handle,
            io_path,
            original_path,
        }
    }

    pub(super) fn child(&self, name: &std::ffi::OsStr) -> PathBuf {
        self.io_path.join(name)
    }

    fn sync(&self, error: &str) -> Result<(), String> {
        self.handle.sync_all().map_err(|_| error.to_string())
    }

    pub(super) fn require_still_named(&self, error: &str) -> Result<(), String> {
        require_unsymlinked_directory_tree(&self.original_path, error)?;
        require_matching_opened_path(&self.handle, &self.original_path, error).map(|_| ())
    }

    pub(super) fn open_child_directory(
        &self,
        name: &std::ffi::OsStr,
        error: &str,
    ) -> Result<Option<Self>, String> {
        let child_path = self.child(name);
        let handle = match open_directory_nofollow(&child_path) {
            Ok(handle) => handle,
            Err(io_error) if io_error.kind() == std::io::ErrorKind::NotFound => {
                self.require_still_named(error)?;
                return Ok(None);
            }
            Err(_) => return Err(error.to_string()),
        };
        require_trusted_directory(&handle, error)?;
        require_matching_opened_path(&handle, &child_path, error)?;
        self.require_still_named(error)?;
        let original_path = self.original_path.join(name);
        require_unsymlinked_directory_tree(&original_path, error)?;
        require_matching_opened_path(&handle, &original_path, error)?;
        Ok(Some(Self::from_opened(handle, original_path)))
    }
}

#[cfg(unix)]
fn create_private_directory_tree(path: &Path, error: &str) -> Result<(), String> {
    use std::os::unix::fs::DirBuilderExt as _;

    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder.create(path).map_err(|_| error.to_string())
}

#[cfg(not(unix))]
fn create_private_directory_tree(path: &Path, error: &str) -> Result<(), String> {
    std::fs::create_dir_all(path).map_err(|_| error.to_string())
}

#[cfg(unix)]
fn require_trusted_directory(handle: &std::fs::File, error: &str) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = handle
        .metadata()
        .map_err(|_| error.to_string())?
        .permissions()
        .mode();
    if mode & 0o022 != 0 {
        return Err(error.to_string());
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_trusted_directory(_handle: &std::fs::File, _error: &str) -> Result<(), String> {
    Ok(())
}

fn require_unsymlinked_existing_ancestry(path: &Path, error: &str) -> Result<(), String> {
    let absolute = std::path::absolute(path).map_err(|_| error.to_string())?;
    for component in absolute.ancestors() {
        match std::fs::symlink_metadata(component) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(error.to_string());
            }
            Ok(_) => {}
            Err(io_error) if io_error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(error.to_string()),
        }
    }
    Ok(())
}

fn require_unsymlinked_directory_tree(path: &Path, error: &str) -> Result<(), String> {
    let absolute = std::path::absolute(path).map_err(|_| error.to_string())?;
    for component in absolute.ancestors() {
        let metadata = std::fs::symlink_metadata(component).map_err(|_| error.to_string())?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(error.to_string());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_directory_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(LINUX_O_DIRECTORY | LINUX_O_NOFOLLOW | LINUX_O_CLOEXEC)
        .open(path)
}

#[cfg(not(target_os = "linux"))]
fn open_directory_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

#[cfg(target_os = "linux")]
fn open_file_nofollow(path: &Path, write_new: bool) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = std::fs::OpenOptions::new();
    options.custom_flags(LINUX_O_NONBLOCK | LINUX_O_NOFOLLOW | LINUX_O_CLOEXEC);
    if write_new {
        options.write(true).create_new(true).mode(0o600);
    } else {
        options.read(true);
    }
    options.open(path)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn open_file_nofollow(path: &Path, write_new: bool) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = std::fs::OpenOptions::new();
    if write_new {
        options.write(true).create_new(true).mode(0o600);
    } else {
        options.read(true);
    }
    options.open(path)
}

#[cfg(not(unix))]
fn open_file_nofollow(path: &Path, write_new: bool) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    if write_new {
        options.write(true).create_new(true);
    } else {
        options.read(true);
    }
    options.open(path)
}

fn require_matching_opened_path(
    handle: &std::fs::File,
    path: &Path,
    error: &str,
) -> Result<std::fs::Metadata, String> {
    let opened = handle.metadata().map_err(|_| error.to_string())?;
    let named = std::fs::symlink_metadata(path).map_err(|_| error.to_string())?;
    if named.file_type().is_symlink() || !same_file_identity(&opened, &named) {
        return Err(error.to_string());
    }
    require_trusted_file(&opened, error)?;
    Ok(opened)
}

#[cfg(unix)]
fn require_trusted_file(metadata: &std::fs::Metadata, error: &str) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt as _;

    if metadata.is_file() && metadata.permissions().mode() & 0o022 != 0 {
        return Err(error.to_string());
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_trusted_file(_metadata: &std::fs::Metadata, _error: &str) -> Result<(), String> {
    Ok(())
}

#[cfg(unix)]
fn same_file_identity(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.file_type() == right.file_type() && left.len() == right.len()
}

/// Write an owned snapshot through one durable publication transaction:
/// create a same-directory temporary, write + fsync it, atomically rename it,
/// then fsync the parent directory. Every pre-publication error removes only this
/// attempt's file. The process-wide lock makes the temporary single-owner, while
/// a regular stale temporary from a crashed process is removed before reuse.
/// Symlinked/non-regular authorities fail closed.
pub(super) fn write_snapshot_atomically_blocking<F>(
    path: PathBuf,
    snapshot: F,
) -> Result<(), String>
where
    F: FnOnce() -> Result<Vec<u8>, String>,
{
    write_snapshot_atomically_blocking_with(path, snapshot, || Ok(()), || {})
}

/// The injected pre-publish check exists solely so a focused test can prove that
/// a failed transaction preserves the prior authority and cleans its temporary.
pub(super) fn write_snapshot_atomically_blocking_with<F, P, A>(
    path: PathBuf,
    snapshot: F,
    before_publish: P,
    after_publish: A,
) -> Result<(), String>
where
    F: FnOnce() -> Result<Vec<u8>, String>,
    P: FnOnce() -> Result<(), String>,
    A: FnOnce(),
{
    let _guard = snapshot_write_lock().lock_recovering("obs snapshot write lock");
    let bytes = snapshot().map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(SNAPSHOT_WRITE_ERROR.to_string());
    }
    let (parent, destination, temporary) = prepare_snapshot_publication(&path)?;
    let result = (|| {
        let mut file =
            open_file_nofollow(&temporary, true).map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
        let metadata = require_matching_opened_path(&file, &temporary, SNAPSHOT_WRITE_ERROR)?;
        if !metadata.is_file() {
            return Err(SNAPSHOT_WRITE_ERROR.to_string());
        }
        before_publish().map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
        std::fs::rename(&temporary, &destination).map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
        require_matching_opened_path(&file, &destination, SNAPSHOT_WRITE_ERROR)?;
        parent.sync(SNAPSHOT_WRITE_ERROR)?;
        parent.require_still_named(SNAPSHOT_WRITE_ERROR)?;
        after_publish();
        Ok(())
    })();
    if result.is_err() && cleanup_snapshot_temporary(&temporary).is_err() {
        return Err(SNAPSHOT_WRITE_ERROR.to_string());
    }
    result
}

fn prepare_snapshot_publication(
    path: &Path,
) -> Result<(SnapshotDirectory, PathBuf, PathBuf), String> {
    let parent_path = path
        .parent()
        .ok_or_else(|| SNAPSHOT_WRITE_ERROR.to_string())?;
    let file_name = path
        .file_name()
        .ok_or_else(|| SNAPSHOT_WRITE_ERROR.to_string())?;
    let parent = SnapshotDirectory::open(parent_path, true, SNAPSHOT_WRITE_ERROR)?;
    let destination = parent.child(file_name);
    require_regular_snapshot_or_missing(&destination)?;
    let temporary_name = format!("{}.tmp", file_name.to_string_lossy());
    let temporary = parent.child(std::ffi::OsStr::new(&temporary_name));
    prepare_snapshot_temporary(&temporary)?;
    Ok((parent, destination, temporary))
}

fn require_regular_snapshot_or_missing(path: &Path) -> Result<(), String> {
    match open_file_nofollow(path, false) {
        Ok(file) => {
            require_matching_opened_path(&file, path, SNAPSHOT_WRITE_ERROR).and_then(|metadata| {
                if metadata.is_file() {
                    Ok(())
                } else {
                    Err(SNAPSHOT_WRITE_ERROR.to_string())
                }
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(SNAPSHOT_WRITE_ERROR.to_string()),
    }
}

fn prepare_snapshot_temporary(temporary: &Path) -> Result<(), String> {
    match open_file_nofollow(temporary, false) {
        Ok(file) => {
            let metadata = require_matching_opened_path(&file, temporary, SNAPSHOT_WRITE_ERROR)?;
            if !metadata.is_file() {
                return Err(SNAPSHOT_WRITE_ERROR.to_string());
            }
            std::fs::remove_file(temporary).map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(SNAPSHOT_WRITE_ERROR.to_string()),
    }
    Ok(())
}

fn cleanup_snapshot_temporary(temporary: &Path) -> Result<(), String> {
    match std::fs::remove_file(temporary) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(SNAPSHOT_WRITE_ERROR.to_string()),
    }
}

/// Read one optional bounded regular snapshot. Missing is distinct from an
/// unavailable, symlinked, oversized, or changing authority, all of which fail
/// closed without exposing its filesystem path or host error detail.
pub(super) fn read_snapshot_blocking(path: &Path) -> Result<Option<Vec<u8>>, String> {
    let parent = path
        .parent()
        .ok_or_else(|| SNAPSHOT_READ_ERROR.to_string())?;
    let name = path
        .file_name()
        .ok_or_else(|| SNAPSHOT_READ_ERROR.to_string())?;
    let directory = SnapshotDirectory::open(parent, false, SNAPSHOT_READ_ERROR)?;
    let bytes = read_snapshot_from_directory(&directory, name)?;
    directory.require_still_named(SNAPSHOT_READ_ERROR)?;
    Ok(bytes)
}

pub(super) fn read_snapshot_from_directory(
    directory: &SnapshotDirectory,
    name: &std::ffi::OsStr,
) -> Result<Option<Vec<u8>>, String> {
    let path = directory.child(name);
    let file = match open_file_nofollow(&path, false) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(SNAPSHOT_READ_ERROR.to_string()),
    };
    let metadata = require_matching_opened_path(&file, &path, SNAPSHOT_READ_ERROR)?;
    if !metadata.is_file() || metadata.len() > MAX_SNAPSHOT_BYTES as u64 {
        return Err(SNAPSHOT_READ_ERROR.to_string());
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_SNAPSHOT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| SNAPSHOT_READ_ERROR.to_string())?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(SNAPSHOT_READ_ERROR.to_string());
    }
    Ok(Some(bytes))
}

/// BUG-016 durable-tier recovery: load the last snapshot written by
/// `ObsState::persist_traces`, if any. `Ok(None)` when no snapshot file exists yet
/// (a fresh persist dir, or a restart before the first sweep tick ever ran --
/// spans since the last tick are RAM-only by design, see `persist_traces`'s doc
/// comment). Bytes recovered from durable storage go through the SAME bounded
/// structural preflight (`eg_types::msgpack::validate_single_value`) every other
/// durable snapshot reader in this engine uses before deserializing -- `eg-tsdb`'s
/// `traces` feature deliberately carries no dependency on `eg-types` (its own
/// "Pi contract" doc comment), so `SpanStore::recover` itself does only the
/// structural `rmp_serde` decode; this caller supplies the bound.
#[cfg(feature = "traces")]
pub(super) fn load_traces_snapshot(
    obs_base: &Path,
) -> Result<Option<eg_tsdb::traces::SpanStore>, String> {
    let path = traces_snapshot_path(obs_base);
    let bytes = match read_snapshot_blocking(&path)? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    eg_types::msgpack::validate_single_value(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(
            eg_types::msgpack::MAX_PROPERTY_BYTES,
            eg_types::msgpack::MAX_PROPERTY_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "trace snapshot is invalid or exceeds its bounds".to_string())?;
    eg_tsdb::traces::SpanStore::recover(&bytes)
        .map(Some)
        .map_err(|_| "trace snapshot is corrupt".to_string())
}

#[cfg(feature = "traces")]
impl ObsState {
    /// BUG-016 durable tier: snapshot the in-memory span store to
    /// `{persist_dir}/obs/traces.msgpack` (tmp-file + atomic rename). Intended to
    /// be called from a periodic sweep task (mirroring
    /// `server::persistence::provenance_anchor`'s cadence, armed by
    /// `EPISTEMIC_GRAPH_OBS_TRACES_PERSIST_SECS`), NOT from the per-request ingest
    /// path -- that would reintroduce BUG-017's write-amplification defect class.
    /// A no-op (`Ok(())`) when this instance has no configured durable persist dir
    /// (ephemeral/test instances), mirroring `provenance_anchor::sweep`'s
    /// no-op-when-unconfigured contract.
    #[cfg(feature = "traces")]
    pub async fn persist_traces(&self) -> Result<(), String> {
        let Some(base) = self.obs_base.clone() else {
            return Ok(());
        };
        let traces = self.traces.clone();
        ::tokio::task::spawn_blocking(move || {
            write_snapshot_atomically_blocking(traces_snapshot_path(&base), move || {
                Ok(traces.snapshot())
            })
        })
        .await
        .map_err(|_| "observability persistence worker failed".to_string())?
    }
}
