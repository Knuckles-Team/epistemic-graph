//! Observability log ingestion front door (CONCEPT:AU-KG.ingest.self-ingest) — the first slice of
//! Phase T ("surpass OpenObserve").
//!
//! ## Why this exists
//!
//! The engine already IS OpenObserve's stack: Rust + redb + Arrow/DataFusion + a
//! Tantivy full-text index (`eg-text`) + a content-addressed blob store on S3
//! (`blob-s3`). OpenObserve (O2) is: ingest logs/metrics/traces over standard
//! wire protocols → land them as time-series + full-text → roll them into
//! Parquet-on-object-store → search with SQL. This module lands the **log
//! ingestion front door**; [`segment`] lands the **Parquet segment substrate**
//! (CONCEPT:EG-KG.retrieval.observability-search). Search / PromQL / dashboards are later Phase T items.
//!
//! ## The listener
//!
//! A HAND-ROLLED HTTP/1.1 listener over `tokio::net::TcpListener` — the SAME
//! dependency-free idiom as [`crate::metrics::serve`] and
//! [`crate::server::sparql_http`] (NO axum/hyper/warp, so the Pi contract holds).
//! Bound via `EPISTEMIC_GRAPH_OBS_ADDR`. It accepts log records over three
//! interchange shapes so EXISTING agents/collectors point at it unchanged:
//!
//!  * `POST /v1/logs`            — OTLP/HTTP JSON (`ExportLogsServiceRequest`)
//!  * `POST /_bulk`              — Elasticsearch `_bulk` NDJSON (action/doc pairs)
//!  * `POST /<stream>/_doc`      — Elasticsearch single-doc index
//!  * `POST /` or `/api/logs`    — plain JSON-lines (one JSON object per line)
//!
//! Every shape is normalized into a common [`LogRecord`] and stored into (a) an
//! `eg-tsdb` series keyed by stream (time-range + retention) and (b) a per-stream
//! `eg-text` Tantivy index (full-text search) — schema-on-read: attributes are a
//! dynamic map, never a fixed column. Multi-tenant by **stream**: an org/stream
//! name gets its own tsdb series id + its own text-index namespace.

pub mod search;
pub mod segment;

/// CONCEPT:EG-OS.observability.prometheus-ingest — the observability EGRESS half: OTLP/HTTP-JSON export of the engine's
/// OWN Prometheus metrics + its stored distributed-trace spans to an external
/// OpenTelemetry collector (closing the loop the engine already ingests). Behind the
/// `otel-export` feature.
#[cfg(feature = "otel-export")]
pub mod otel_export;

/// CONCEPT:EG-OS.observability.prometheus-ingest — the Prometheus `remote_write` receiver: land snappy-compressed
/// protobuf `WriteRequest` POSTs (`/api/v1/write`) into the durable eg-tsdb SeriesStore
/// so an external Prometheus can PUSH to the engine. Behind the `otel-export` feature.
#[cfg(feature = "otel-export")]
pub mod remote_write;

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::server::blob::store::{ChunkStore, RedbChunkStore};
use eg_text::TextIndex;
use eg_tsdb::point::Point;
use eg_tsdb::store::SeriesStore;

pub use search::{LogQuery, DEFAULT_SEARCH_SIZE};
pub use segment::SegmentManifest;

/// Env var carrying the observability-ingest listener bind address (`host:port`),
/// e.g. `127.0.0.1:5080` (O2's default log-ingest port). Unset ⇒ no listener.
pub const OBS_ADDR_ENV: &str = "EPISTEMIC_GRAPH_OBS_ADDR";
/// Env var overriding the per-stream buffered-record count that triggers a Parquet
/// segment flush (CONCEPT:EG-KG.retrieval.observability-search). Default [`DEFAULT_FLUSH_RECORDS`].
pub const OBS_FLUSH_RECORDS_ENV: &str = "EPISTEMIC_GRAPH_OBS_FLUSH_RECORDS";

/// Default flush window: buffer this many records per stream, then roll a segment.
pub const DEFAULT_FLUSH_RECORDS: usize = 1024;
/// TSDB time-partition width for a log series: 1 hour of wall-clock per chunk.
const SERIES_BUCKET_NS: u64 = 3_600_000_000_000;
/// Hard network bounds for the dependency-free observability HTTP listener.
const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAX_HTTP_HEADER_LINE_BYTES: usize = 16 * 1024;
const MAX_HTTP_HEADERS: usize = 128;
const MAX_HTTP_TARGET_BYTES: usize = 8 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_HTTP_CONNECTIONS: usize = 256;
const HTTP_READ_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SNAPSHOT_BYTES: usize = eg_types::msgpack::MAX_PROPERTY_BYTES;
const SNAPSHOT_WRITE_ERROR: &str = "observability snapshot write failed";
const SNAPSHOT_READ_ERROR: &str = "observability snapshot read failed";
const OBS_PERSISTENCE_DIRECTORY_ERROR: &str = "observability persistence directory is unavailable";
const MANIFEST_DIRECTORY_ERROR: &str = "segment manifest directory is unavailable";
const MANIFEST_READ_ERROR: &str = "segment manifest is unavailable";
const MANIFEST_BOUNDS_ERROR: &str = "segment manifests exceed recovery bounds";
const MAX_MANIFEST_FILES: usize = 16_384;
const MAX_MANIFEST_TOTAL_BYTES: usize = MAX_SNAPSHOT_BYTES;
const MAX_MANIFEST_TOTAL_ITEMS: usize = eg_types::msgpack::MAX_PROPERTY_ITEMS;

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

/// One normalized log record — the common shape every wire format is parsed into.
/// `attrs` is a dynamic string map (schema-on-read); the fixed fields are the ones
/// every observability format carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// Timestamp, epoch nanoseconds (the tsdb series key).
    pub ts: i64,
    /// Org/stream namespace (the tenancy key → its own series + text namespace).
    pub stream: String,
    /// Severity text (e.g. `INFO`/`WARN`/`ERROR`); empty if the source omitted it.
    pub severity: String,
    /// The log message body.
    pub body: String,
    /// Dynamic attributes (schema-on-read), sorted for a stable serialization.
    pub attrs: BTreeMap<String, String>,
}

/// The ingest counts an ingest call landed, shaped into the wire response.
#[derive(Clone, Copy, Debug, Default)]
pub struct IngestOutcome {
    pub accepted: usize,
    pub segments_flushed: usize,
}

/// Wall-clock now in epoch nanoseconds.
fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Map a severity text onto a numeric level (OTLP-ish) for the tsdb series value, so
/// the series is a meaningful "severity over time" signal, not just a count.
fn severity_number(sev: &str) -> f64 {
    match sev.trim().to_ascii_uppercase().as_str() {
        "TRACE" => 1.0,
        "DEBUG" => 5.0,
        "INFO" | "INFORMATION" => 9.0,
        "WARN" | "WARNING" => 13.0,
        "ERROR" | "ERR" => 17.0,
        "FATAL" | "CRITICAL" | "CRIT" => 21.0,
        _ => 0.0,
    }
}

/// Collision-resistant, opaque filesystem component for one exact stream name.
/// Domain separation prevents an index directory and manifest file from sharing
/// an identity scheme with unrelated persistent data.
fn stream_storage_key(stream: &str) -> String {
    use sha2::{Digest as _, Sha256};

    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/observability-stream\0");
    digest.update(stream.as_bytes());
    hex::encode(digest.finalize())
}

/// BUG-016 durable path: `{persist_dir}/obs/traces.msgpack`.
#[cfg(feature = "traces")]
fn traces_snapshot_path(obs_base: &Path) -> PathBuf {
    obs_base.join("traces.msgpack")
}

/// BUG-210 durable path for one stream's segment manifest list:
/// `{persist_dir}/obs/segments/<sanitized-stream>.msgpack`.
fn segment_manifest_path(obs_base: &Path, stream: &str) -> PathBuf {
    obs_base
        .join("segments")
        .join(format!("{}.msgpack", stream_storage_key(stream)))
}

/// Serialize snapshot publication inside this process. The authoritative bytes
/// still live in the destination file; this lock only prevents concurrent sweep
/// and ingest workers from racing replacement of one stream's side file.
fn snapshot_write_lock() -> &'static StdMutex<()> {
    static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| StdMutex::new(()))
}

/// An opened, validated directory authority. On Unix it must not be writable by
/// group/other, making same-identity processes the explicit filesystem trust
/// boundary; the process lock serializes writers within this engine. On Linux, all subsequent
/// child operations resolve through `/proc/self/fd/<fd>` so renaming or replacing
/// an ancestor cannot redirect a transaction after validation.
struct SnapshotDirectory {
    handle: std::fs::File,
    io_path: PathBuf,
    original_path: PathBuf,
}

impl SnapshotDirectory {
    fn open(path: &Path, create: bool, error: &str) -> Result<Self, String> {
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

    fn child(&self, name: &std::ffi::OsStr) -> PathBuf {
        self.io_path.join(name)
    }

    fn sync(&self, error: &str) -> Result<(), String> {
        self.handle.sync_all().map_err(|_| error.to_string())
    }

    fn require_still_named(&self, error: &str) -> Result<(), String> {
        require_unsymlinked_directory_tree(&self.original_path, error)?;
        require_matching_opened_path(&self.handle, &self.original_path, error).map(|_| ())
    }

    fn open_child_directory(
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
fn write_snapshot_atomically_blocking<F>(path: PathBuf, snapshot: F) -> Result<(), String>
where
    F: FnOnce() -> Result<Vec<u8>, String>,
{
    write_snapshot_atomically_blocking_with(path, snapshot, || Ok(()), || {})
}

/// The injected pre-publish check exists solely so a focused test can prove that
/// a failed transaction preserves the prior authority and cleans its temporary.
fn write_snapshot_atomically_blocking_with<F, P, A>(
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
    let _guard = snapshot_write_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
fn read_snapshot_blocking(path: &Path) -> Result<Option<Vec<u8>>, String> {
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

fn read_snapshot_from_directory(
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
fn load_traces_snapshot(obs_base: &Path) -> Result<Option<eg_tsdb::traces::SpanStore>, String> {
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

/// BUG-210 durable-tier recovery: rebuild the segment-manifest prune index
/// (`ObsState::segments`) from every `{persist_dir}/obs/segments/*.msgpack` side
/// file written by `persist_stream_segments`. Returns empty maps when the directory
/// does not exist yet in a fresh persist dir. Each file's bytes go through the same bounded
/// structural preflight `load_traces_snapshot` uses.
fn load_segment_manifests(
    obs_base: &Path,
) -> Result<
    (
        Vec<SegmentManifest>,
        HashMap<String, Vec<usize>>,
        HashMap<String, (usize, usize)>,
        usize,
        usize,
    ),
    String,
> {
    let Some(directory) = open_segment_manifest_directory(obs_base)? else {
        return Ok((Vec::new(), HashMap::new(), HashMap::new(), 0, 0));
    };
    let names = segment_manifest_names(&directory)?;
    let limits = eg_types::msgpack::MsgpackLimits::new(
        eg_types::msgpack::MAX_PROPERTY_BYTES,
        eg_types::msgpack::MAX_PROPERTY_ITEMS,
        eg_types::msgpack::DEFAULT_MAX_DEPTH,
    );
    let mut manifests = Vec::new();
    let mut positions: HashMap<String, Vec<usize>> = HashMap::new();
    let mut usage = HashMap::new();
    let mut total_bytes = 0usize;
    let mut total_items = 0usize;
    for name in names {
        let bytes = read_snapshot_from_directory(&directory, &name)?
            .ok_or_else(|| MANIFEST_READ_ERROR.to_string())?;
        include_manifest_budget(&mut total_bytes, bytes.len(), MAX_MANIFEST_TOTAL_BYTES)?;
        let stream_manifests: Vec<SegmentManifest> =
            eg_types::msgpack::decode_bounded(&bytes, limits)
                .map_err(|_| "segment manifest is invalid or exceeds its bounds".to_string())?;
        include_manifest_budget(
            &mut total_items,
            stream_manifests.len(),
            MAX_MANIFEST_TOTAL_ITEMS,
        )?;
        let stream = stream_manifests
            .first()
            .map(|manifest| manifest.stream.clone())
            .ok_or_else(|| "segment manifest is invalid or exceeds its bounds".to_string())?;
        let expected_name = format!("{}.msgpack", stream_storage_key(&stream));
        if name != std::ffi::OsStr::new(&expected_name)
            || stream_manifests
                .iter()
                .any(|manifest| manifest.stream != stream)
            || positions.contains_key(&stream)
        {
            return Err("segment manifest is invalid or exceeds its bounds".to_string());
        }
        let first_position = manifests.len();
        positions.insert(
            stream.clone(),
            (first_position..first_position + stream_manifests.len()).collect(),
        );
        usage.insert(stream.clone(), (bytes.len(), stream_manifests.len()));
        manifests.extend(stream_manifests);
    }
    directory.require_still_named(MANIFEST_DIRECTORY_ERROR)?;
    Ok((manifests, positions, usage, total_bytes, total_items))
}

fn include_manifest_budget(total: &mut usize, increment: usize, max: usize) -> Result<(), String> {
    *total = (*total)
        .checked_add(increment)
        .filter(|candidate| *candidate <= max)
        .ok_or_else(|| MANIFEST_BOUNDS_ERROR.to_string())?;
    Ok(())
}

fn open_segment_manifest_directory(obs_base: &Path) -> Result<Option<SnapshotDirectory>, String> {
    let base = SnapshotDirectory::open(obs_base, false, MANIFEST_DIRECTORY_ERROR)?;
    base.open_child_directory(std::ffi::OsStr::new("segments"), MANIFEST_DIRECTORY_ERROR)
}

fn segment_manifest_names(
    directory: &SnapshotDirectory,
) -> Result<Vec<std::ffi::OsString>, String> {
    let entries =
        std::fs::read_dir(&directory.io_path).map_err(|_| MANIFEST_DIRECTORY_ERROR.to_string())?;
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| MANIFEST_DIRECTORY_ERROR.to_string())?;
        let name = entry.file_name();
        if Path::new(&name).extension().and_then(|ext| ext.to_str()) != Some("msgpack") {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|_| MANIFEST_READ_ERROR.to_string())?;
        if file_type.is_symlink() || !file_type.is_file() {
            return Err(MANIFEST_READ_ERROR.to_string());
        }
        if names.len() >= MAX_MANIFEST_FILES {
            return Err(MANIFEST_BOUNDS_ERROR.to_string());
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}

fn encode_bounded_stream_manifests(
    state: &ObsState,
    target_stream: &str,
    pending_usage: &Cell<Option<(usize, usize, usize, usize)>>,
) -> Result<Vec<u8>, String> {
    let segments = state.segments.lock();
    let positions = state.manifest_positions.lock();
    let target_positions = positions
        .get(target_stream)
        .ok_or_else(|| SNAPSHOT_WRITE_ERROR.to_string())?;
    let target: Vec<&SegmentManifest> = target_positions
        .iter()
        .map(|position| {
            segments
                .get(*position)
                .ok_or_else(|| SNAPSHOT_WRITE_ERROR.to_string())
        })
        .collect::<Result<_, _>>()?;
    let bytes = rmp_serde::to_vec_named(&target).map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
    let usage = state.manifest_usage.lock();
    let previous = usage.0.get(target_stream).copied().unwrap_or((0, 0));
    let file_count = usage.0.len()
        + if usage.0.contains_key(target_stream) {
            0
        } else {
            1
        };
    let total_bytes = usage
        .1
        .checked_sub(previous.0)
        .and_then(|total| total.checked_add(bytes.len()))
        .filter(|total| *total <= MAX_MANIFEST_TOTAL_BYTES);
    let total_items = usage
        .2
        .checked_sub(previous.1)
        .and_then(|total| total.checked_add(target_positions.len()))
        .filter(|total| *total <= MAX_MANIFEST_TOTAL_ITEMS);
    if file_count > MAX_MANIFEST_FILES || total_bytes.is_none() || total_items.is_none() {
        return Err(MANIFEST_BOUNDS_ERROR.to_string());
    }
    pending_usage.set(Some((
        bytes.len(),
        target_positions.len(),
        total_bytes.expect("checked above"),
        total_items.expect("checked above"),
    )));
    Ok(bytes)
}

fn record_segment_manifest(
    segments: &Mutex<Vec<SegmentManifest>>,
    positions: &Mutex<HashMap<String, Vec<usize>>>,
    manifest: SegmentManifest,
) {
    let stream = manifest.stream.clone();
    let mut segments = segments.lock();
    let mut positions = positions.lock();
    let position = segments.len();
    segments.push(manifest);
    positions.entry(stream).or_default().push(position);
}

/// The self-contained ingest state: a tsdb series store + per-stream text indices +
/// the blob CAS for Parquet segments + per-stream flush buffers + the recorded
/// segment manifests. NOT tied to the graph `ServerState` — it is the observability
/// tier's own substrate.
pub struct ObsState {
    /// Time-series store (series id = `obs:logs:<stream>`).
    series: Arc<SeriesStore>,
    /// Blob CAS the Parquet segments land in (S3-backed when `blob-s3` is on).
    blob: Arc<dyn ChunkStore>,
    /// Per-stream Tantivy full-text indices, created lazily.
    indices: Mutex<HashMap<String, TextIndex>>,
    /// Per-stream buffers of records awaiting a Parquet flush.
    buffers: Mutex<HashMap<String, Vec<LogRecord>>>,
    /// Recorded segment manifests (the prune index), newest last.
    segments: Mutex<Vec<SegmentManifest>>,
    /// Positions into `segments`, grouped by stream, so one persistence flush
    /// visits only its tenant's manifests while search keeps its vector contract.
    manifest_positions: Mutex<HashMap<String, Vec<usize>>>,
    /// Per-stream and aggregate bounds for the manifest files that have completed
    /// durable publication. Updated before the global publication lock is released.
    manifest_usage: Mutex<(HashMap<String, (usize, usize)>, usize, usize)>,
    /// Base dir for persistent text indices; `None` ⇒ in-memory indices (tests).
    text_dir: Option<std::path::PathBuf>,
    /// `{persist_dir}/obs` -- the base this module's own durable side-files
    /// (`traces.msgpack` for BUG-016, `segments/<stream>.msgpack` for BUG-210)
    /// live under. `None` ⇒ ephemeral/test instance (mirrors `text_dir`'s "None ⇒
    /// in-memory" contract): nothing beyond the tsdb series + blob CAS the
    /// `None`-branch of `open()` already gives a temp dir is durable in that mode.
    obs_base: Option<PathBuf>,
    /// Roll a segment once a stream buffers this many records.
    flush_threshold: usize,
    /// Monotonic doc-id sequence for text-index document ids.
    next_doc: AtomicU64,
    /// CONCEPT:EG-OS.observability.trace-assembly — the distributed-trace span store (in-memory; a durable tier
    /// mirroring logs is a follow-up). Present only under the `traces` sub-feature.
    #[cfg(feature = "traces")]
    traces: Arc<eg_tsdb::traces::SpanStore>,
}

fn open_persistent_obs_stores(base: &Path) -> Result<(SeriesStore, RedbChunkStore), String> {
    let authority = SnapshotDirectory::open(base, true, OBS_PERSISTENCE_DIRECTORY_ERROR)?;
    let series = SeriesStore::open_in_dir(
        &authority.io_path,
        crate::store_authority::process_verifier(),
        crate::store_authority::process_authority().principal(),
        &crate::store_authority::process_authority().proof(),
    )
    .map_err(|_| "observability series store is unavailable".to_string())?;
    let blob = RedbChunkStore::open(
        &authority
            .child(std::ffi::OsStr::new("blob"))
            .to_string_lossy(),
    )
    .map_err(|_| "observability blob store is unavailable".to_string())?;
    authority.require_still_named(OBS_PERSISTENCE_DIRECTORY_ERROR)?;
    Ok((series, blob))
}

/// The complete synchronous construction/recovery boundary used by both the
/// public async constructor and test-only ephemeral construction.
fn open_obs_state_blocking(
    persist_dir: Option<&str>,
    flush_threshold: usize,
) -> Result<ObsState, String> {
    let (series, blob, text_dir, obs_base) = match persist_dir {
        Some(dir) => {
            let base = Path::new(dir).join("obs");
            let (series, blob) = open_persistent_obs_stores(&base)?;
            (series, blob, Some(base.join("text")), Some(base))
        }
        None => {
            let base =
                std::env::temp_dir().join(format!("eg-obs-{}-{}", std::process::id(), now_ns()));
            let series = SeriesStore::open_in_dir(
                &base,
                crate::store_authority::process_verifier(),
                crate::store_authority::process_authority().principal(),
                &crate::store_authority::process_authority().proof(),
            )
            .map_err(|_| "observability series store is unavailable".to_string())?;
            let blob = RedbChunkStore::open(&base.join("blob").to_string_lossy())
                .map_err(|_| "observability blob store is unavailable".to_string())?;
            (series, blob, None, None)
        }
    };
    let (segments, manifest_positions, manifest_usage, manifest_bytes, manifest_items) =
        match obs_base.as_deref() {
            Some(base) => load_segment_manifests(base)?,
            None => (Vec::new(), HashMap::new(), HashMap::new(), 0, 0),
        };
    #[cfg(feature = "traces")]
    let traces = match obs_base.as_deref() {
        Some(base) => match load_traces_snapshot(base)? {
            Some(traces) => traces,
            None => eg_tsdb::traces::SpanStore::new(),
        },
        None => eg_tsdb::traces::SpanStore::new(),
    };
    Ok(ObsState {
        series: Arc::new(series),
        blob: Arc::new(blob),
        indices: Mutex::new(HashMap::new()),
        buffers: Mutex::new(HashMap::new()),
        segments: Mutex::new(segments),
        manifest_positions: Mutex::new(manifest_positions),
        manifest_usage: Mutex::new((manifest_usage, manifest_bytes, manifest_items)),
        text_dir,
        obs_base,
        flush_threshold: flush_threshold.max(1),
        next_doc: AtomicU64::new(1),
        #[cfg(feature = "traces")]
        traces: Arc::new(traces),
    })
}

impl ObsState {
    /// Open the ingest substrate under a persist dir (durable series + blob CAS +
    /// on-disk text indices under `{persist_dir}/obs/…`). With `persist_dir = None`
    /// everything is in a temp dir / in-memory (tests / ephemeral). The complete
    /// initialization and recovery boundary runs on Tokio's blocking pool.
    pub async fn open(persist_dir: Option<&str>, flush_threshold: usize) -> Result<Self, String> {
        let persist_dir = persist_dir.map(str::to_owned);
        ::tokio::task::spawn_blocking(move || {
            open_obs_state_blocking(persist_dir.as_deref(), flush_threshold)
        })
        .await
        .map_err(|_| "observability persistence worker failed".to_string())?
    }

    /// CONCEPT:EG-OS.observability.trace-assembly — the distributed-trace span store handle, used by the trace
    /// facade (`src/server/traces`) to ingest spans and serve trace search/assembly.
    #[cfg(feature = "traces")]
    pub fn trace_store(&self) -> Arc<eg_tsdb::traces::SpanStore> {
        self.traces.clone()
    }

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

    /// In-memory ingest state (temp series/blob, RAM text indices) — for tests.
    pub fn in_memory(flush_threshold: usize) -> Result<Self, String> {
        open_obs_state_blocking(None, flush_threshold)
    }

    /// Ingest a batch of normalized records: append each stream's points to its tsdb
    /// series, upsert each record into its stream's text index, buffer it for a
    /// Parquet flush, and roll a segment for any stream that crossed the threshold.
    pub fn ingest(&self, records: Vec<LogRecord>) -> Result<IngestOutcome, String> {
        if records.is_empty() {
            return Ok(IngestOutcome::default());
        }
        // Group by stream so each series append + index commit is one batch.
        let mut by_stream: HashMap<String, Vec<LogRecord>> = HashMap::new();
        for r in records {
            by_stream.entry(r.stream.clone()).or_default().push(r);
        }

        let mut accepted = 0usize;
        let mut segments_flushed = 0usize;
        for (stream, recs) in by_stream {
            accepted += recs.len();

            // (a) tsdb series — time-range + retention.
            let series_id = format!("obs:logs:{stream}");
            let points: Vec<Point> = recs
                .iter()
                .map(|r| Point {
                    ts: r.ts,
                    values: vec![severity_number(&r.severity)],
                })
                .collect();
            self.series
                .append_batch(
                    &series_id,
                    1,
                    SERIES_BUCKET_NS,
                    &["severity".to_string()],
                    &points,
                )
                .map_err(|e| e.to_string())?;

            // (b) full-text index — schema-on-read (attrs flattened into the text).
            self.with_index(&stream, |ix| {
                for r in &recs {
                    let seq = self.next_doc.fetch_add(1, Ordering::Relaxed);
                    let doc_id = format!("{stream}:{seq}");
                    ix.upsert(&doc_id, &text_body(r));
                }
                ix.commit().map_err(|e| e.to_string())
            })?;

            // (c) buffer for Parquet; flush a segment past the threshold.
            let drained = {
                let mut buffers = self.buffers.lock();
                let buf = buffers.entry(stream.clone()).or_default();
                buf.extend(recs);
                if buf.len() >= self.flush_threshold {
                    Some(std::mem::take(buf))
                } else {
                    None
                }
            };
            if let Some(batch) = drained {
                if let Some(manifest) =
                    segment::flush_records_to_segment(self.blob.as_ref(), &stream, &batch)?
                {
                    record_segment_manifest(&self.segments, &self.manifest_positions, manifest);
                    // BUG-210: durably index this stream's manifest list at the SAME
                    // cadence as the flush itself (bounded: one small rewrite per
                    // `flush_threshold`-many records, not per request).
                    self.persist_stream_segments(&stream)?;
                    segments_flushed += 1;
                }
            }
        }
        Ok(IngestOutcome {
            accepted,
            segments_flushed,
        })
    }

    /// Force-flush a stream's buffered records into a Parquet segment NOW (used at
    /// shutdown / tests / a small stream that never crosses the threshold). Returns
    /// the manifest, or `None` if the buffer was empty.
    pub fn flush_stream(&self, stream: &str) -> Result<Option<SegmentManifest>, String> {
        let batch = {
            let mut buffers = self.buffers.lock();
            match buffers.get_mut(stream) {
                Some(buf) if !buf.is_empty() => std::mem::take(buf),
                _ => return Ok(None),
            }
        };
        let manifest = segment::flush_records_to_segment(self.blob.as_ref(), stream, &batch)?;
        if let Some(m) = &manifest {
            record_segment_manifest(&self.segments, &self.manifest_positions, m.clone());
            // BUG-210: see the matching comment in `ingest`.
            self.persist_stream_segments(stream)?;
        }
        Ok(manifest)
    }

    /// BUG-210 durable tier: durably persist ONE stream's segment manifest list to
    /// `{persist_dir}/obs/segments/<sanitized-stream>.msgpack` (tmp-file + atomic
    /// rename, the SAME convention `persist_traces` uses). A no-op (`Ok(())`) when
    /// this instance has no configured durable persist dir (ephemeral/test
    /// instances) -- the manifest already lives only in RAM in that mode, mirroring
    /// `text_dir`'s existing "`None` ⇒ in-memory" contract.
    fn persist_stream_segments(&self, stream: &str) -> Result<(), String> {
        let Some(base) = self.obs_base.as_deref() else {
            return Ok(());
        };
        let pending_usage = Cell::new(None);
        write_snapshot_atomically_blocking_with(
            segment_manifest_path(base, stream),
            || encode_bounded_stream_manifests(self, stream, &pending_usage),
            || Ok(()),
            || {
                let (bytes, items, total_bytes, total_items) = pending_usage
                    .take()
                    .expect("manifest usage is prepared before publication");
                let mut usage = self.manifest_usage.lock();
                usage.0.insert(stream.to_string(), (bytes, items));
                usage.1 = total_bytes;
                usage.2 = total_items;
            },
        )
    }

    /// BM25 search a stream's text index — returns `(doc_id, score)` hits. The
    /// query-back surface tests + a future EG-162 search endpoint use.
    pub fn search(&self, stream: &str, query: &str, k: usize) -> Vec<eg_text::TextHit> {
        let mut indices = self.indices.lock();
        match indices.get_mut(stream) {
            Some(ix) => ix.search(query, k),
            None => Vec::new(),
        }
    }

    /// The durable time-series store handle — used by the PromQL facade
    /// (CONCEPT:EG-KG.query.prometheus-http-query-api) to resolve selectors / enumerate labels over the same series
    /// the log ingest lands in.
    pub fn series_store(&self) -> Arc<SeriesStore> {
        self.series.clone()
    }

    /// Read a stream's tsdb series points over `[from, to)` (epoch-ns) — proves the
    /// records landed in the time-series tier.
    pub fn series_range(&self, stream: &str, from: i64, to: i64) -> Result<Vec<Point>, String> {
        self.series
            .range(&format!("obs:logs:{stream}"), from, to)
            .map_err(|e| e.to_string())
    }

    /// Segment manifests recorded for a stream (the prune index).
    pub fn segments_for(&self, stream: &str) -> Vec<SegmentManifest> {
        self.segments
            .lock()
            .iter()
            .filter(|manifest| manifest.stream == stream)
            .cloned()
            .collect()
    }

    /// Read a flushed Parquet segment's records back out of the blob CAS.
    pub fn read_segment(&self, manifest: &SegmentManifest) -> Result<Vec<LogRecord>, String> {
        let bytes = segment::read_segment_bytes(self.blob.as_ref(), &manifest.blob_digest)?;
        segment::parquet_to_records(&bytes)
    }

    /// Run `f` with a mutable handle to `stream`'s text index, creating it lazily
    /// (on-disk under `text_dir` when persistent, else in-RAM).
    fn with_index<T>(
        &self,
        stream: &str,
        f: impl FnOnce(&mut TextIndex) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut indices = self.indices.lock();
        if !indices.contains_key(stream) {
            let ix = match &self.text_dir {
                Some(base) => {
                    let dir = base.join(stream_storage_key(stream));
                    std::fs::create_dir_all(&dir).map_err(|_| {
                        "observability text index directory is unavailable".to_string()
                    })?;
                    TextIndex::open(&dir)
                        .map_err(|_| "observability text index is unavailable".to_string())?
                }
                None => TextIndex::in_memory()
                    .map_err(|_| "observability text index is unavailable".to_string())?,
            };
            indices.insert(stream.to_string(), ix);
        }
        let ix = indices.get_mut(stream).expect("just inserted");
        f(ix)
    }
}

/// The searchable text for a record: the body plus its flattened attributes, so a
/// full-text query can match on either (schema-on-read).
fn text_body(r: &LogRecord) -> String {
    let mut s = r.body.clone();
    if !r.severity.is_empty() {
        s.push(' ');
        s.push_str(&r.severity);
    }
    for (k, v) in &r.attrs {
        s.push(' ');
        s.push_str(k);
        s.push('=');
        s.push_str(v);
    }
    s
}

// ── wire-format parsers ───────────────────────────────────────────────────────

/// Extract a timestamp (epoch-ns) from a generic doc, tolerating the common shapes:
/// OTLP `timeUnixNano` (ns string/number), `@timestamp`/`timestamp`/`time` as epoch
/// ms or an ISO-8601-ish number. Falls back to `now`.
fn extract_ts(doc: &serde_json::Value) -> i64 {
    if let Some(n) = extract_nano_ts(doc) {
        return n;
    }
    if let Some(n) = extract_millis_ts(doc) {
        return n;
    }
    now_ns()
}

/// OTLP nano timestamp (string or number of nanoseconds). Half of [`extract_ts`]'s
/// field scan.
fn extract_nano_ts(doc: &serde_json::Value) -> Option<i64> {
    for key in ["timeUnixNano", "observedTimeUnixNano"] {
        if let Some(v) = doc.get(key) {
            if let Some(n) = v.as_str().and_then(|s| s.parse::<i64>().ok()) {
                return Some(n);
            }
            if let Some(n) = v.as_i64() {
                return Some(n);
            }
        }
    }
    None
}

/// Epoch-millis style fields → ns. A plain integer is treated as milliseconds (the
/// Elastic/O2 convention); a non-numeric (ISO string) is left for [`extract_ts`]'s
/// `now_ns()` fallback. The other half of [`extract_ts`]'s field scan.
fn extract_millis_ts(doc: &serde_json::Value) -> Option<i64> {
    for key in ["@timestamp", "timestamp", "time", "_timestamp"] {
        if let Some(v) = doc.get(key) {
            if let Some(ms) = v.as_i64() {
                return Some(ms.saturating_mul(1_000_000));
            }
            if let Some(ms) = v.as_f64() {
                return Some((ms * 1_000_000.0) as i64);
            }
        }
    }
    None
}

/// Extract the severity text from the common field names.
fn extract_severity(doc: &serde_json::Value) -> String {
    for key in ["severityText", "severity", "level", "log.level", "loglevel"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    String::new()
}

/// Extract the message body from the common field names (OTLP `body.stringValue`,
/// or `message`/`msg`/`body`).
fn extract_body(doc: &serde_json::Value) -> String {
    if let Some(b) = doc.get("body") {
        if let Some(s) = b.get("stringValue").and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(s) = b.as_str() {
            return s.to_string();
        }
    }
    for key in ["message", "msg", "log", "_message"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    String::new()
}

/// The set of doc keys consumed into fixed [`LogRecord`] fields (so they are not
/// ALSO duplicated into `attrs`).
const RESERVED_KEYS: &[&str] = &[
    "timeUnixNano",
    "observedTimeUnixNano",
    "@timestamp",
    "timestamp",
    "time",
    "_timestamp",
    "severityText",
    "severity",
    "level",
    "log.level",
    "loglevel",
    "body",
    "message",
    "msg",
    "log",
    "_message",
    "stream",
    "_stream",
    "attributes",
];

/// Collect the remaining scalar fields of a doc into the dynamic attribute map, plus
/// any OTLP `attributes` array (`[{key,value:{stringValue}}]`).
fn extract_attrs(doc: &serde_json::Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(obj) = doc.as_object() {
        for (k, v) in obj {
            if RESERVED_KEYS.contains(&k.as_str()) {
                continue;
            }
            out.insert(k.clone(), scalar_to_string(v));
        }
    }
    // OTLP-style attributes array.
    if let Some(arr) = doc.get("attributes").and_then(|v| v.as_array()) {
        for a in arr {
            if let (Some(k), Some(val)) = (a.get("key").and_then(|v| v.as_str()), a.get("value")) {
                out.insert(k.to_string(), otlp_anyvalue(val));
            }
        }
    }
    out
}

/// Render a JSON scalar (or nested value) as a compact attribute string.
fn scalar_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Unwrap an OTLP `AnyValue` (`{stringValue|intValue|doubleValue|boolValue}`).
fn otlp_anyvalue(v: &serde_json::Value) -> String {
    for key in ["stringValue", "intValue", "doubleValue", "boolValue"] {
        if let Some(inner) = v.get(key) {
            return scalar_to_string(inner);
        }
    }
    scalar_to_string(v)
}

/// Resolve the stream for a doc: an explicit `stream`/`_stream` field wins, else the
/// caller's default (path/query-derived).
fn extract_stream(doc: &serde_json::Value, default_stream: &str) -> String {
    for key in ["stream", "_stream"] {
        if let Some(s) = doc.get(key).and_then(|v| v.as_str()) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    default_stream.to_string()
}

/// Normalize a single generic doc into a [`LogRecord`] with the given default stream.
fn doc_to_record(doc: &serde_json::Value, default_stream: &str) -> LogRecord {
    LogRecord {
        ts: extract_ts(doc),
        stream: extract_stream(doc, default_stream),
        severity: extract_severity(doc),
        body: extract_body(doc),
        attrs: extract_attrs(doc),
    }
}

/// Parse an OTLP/HTTP JSON `ExportLogsServiceRequest`
/// (`resourceLogs[].scopeLogs[].logRecords[]`) into records. The stream is derived
/// from the resource's `service.name` attribute, else `default_stream`.
pub fn parse_otlp_logs(body: &str, default_stream: &str) -> Result<Vec<LogRecord>, String> {
    let root: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("parse OTLP JSON: {e}"))?;
    let mut out = Vec::new();
    let resource_logs = root
        .get("resourceLogs")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for rl in &resource_logs {
        let stream = resource_log_stream(rl, default_stream);
        push_scope_log_records(rl, &stream, &mut out);
    }
    Ok(out)
}

/// Resolve one `resourceLogs` entry's stream name from its `resource.attributes`
/// `service.name`, falling back to `default_stream`. Extracted from
/// [`parse_otlp_logs`]'s per-resource loop.
fn resource_log_stream(rl: &serde_json::Value, default_stream: &str) -> String {
    let mut stream = default_stream.to_string();
    if let Some(attrs) = rl
        .get("resource")
        .and_then(|r| r.get("attributes"))
        .and_then(|v| v.as_array())
    {
        for a in attrs {
            if a.get("key").and_then(|v| v.as_str()) == Some("service.name") {
                if let Some(val) = a.get("value") {
                    let s = otlp_anyvalue(val);
                    if !s.is_empty() {
                        stream = s;
                    }
                }
            }
        }
    }
    stream
}

/// Push every log record from one `resourceLogs` entry's `scopeLogs[].logRecords[]`
/// into `out`. Extracted from [`parse_otlp_logs`]'s per-resource loop.
fn push_scope_log_records(rl: &serde_json::Value, stream: &str, out: &mut Vec<LogRecord>) {
    for sl in rl
        .get("scopeLogs")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
    {
        for lr in sl
            .get("logRecords")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            out.push(doc_to_record(lr, stream));
        }
    }
}

/// Parse an Elasticsearch `_bulk` NDJSON body: alternating action/source lines
/// (`{"index":{"_index":"logs"}}\n{doc}\n`). The `_index` names the stream (else
/// `default_stream`). `create`/`index` actions carry a following source line;
/// `delete` actions (no source) are skipped.
pub fn parse_es_bulk(body: &str, default_stream: &str) -> Vec<LogRecord> {
    let mut out = Vec::new();
    let mut lines = body.lines().filter(|l| !l.trim().is_empty());
    while let Some(action_line) = lines.next() {
        let action: serde_json::Value = match serde_json::from_str(action_line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // The action object has ONE key: index|create|update|delete.
        let (op, meta) = match action.as_object().and_then(|o| o.iter().next()) {
            Some((k, v)) => (k.as_str(), v),
            None => continue,
        };
        if op == "delete" {
            continue; // no source line follows
        }
        let Some(source_line) = lines.next() else {
            break;
        };
        let doc: serde_json::Value = match serde_json::from_str(source_line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stream = meta
            .get("_index")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(default_stream);
        out.push(doc_to_record(&doc, stream));
    }
    out
}

/// Parse a plain JSON-lines body (one JSON object per line) into records.
pub fn parse_json_lines(body: &str, default_stream: &str) -> Vec<LogRecord> {
    let trimmed = body.trim_start();
    // Tolerate a single JSON array too (`[ {...}, {...} ]`).
    if trimmed.starts_with('[') {
        if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(body) {
            return arr
                .iter()
                .map(|d| doc_to_record(d, default_stream))
                .collect();
        }
    }
    body.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .map(|d| doc_to_record(&d, default_stream))
        .collect()
}

// ── HTTP listener (hand-rolled, no axum/hyper — the Pi contract) ────────────────

/// A parsed HTTP request: method, raw target, content type, body.
struct HttpRequest {
    method: String,
    target: String,
    content_type: String,
    body: String,
    /// CONCEPT:EG-OS.observability.prometheus-ingest — the ORIGINAL (un-lossy-UTF-8'd) request bytes, kept for the
    /// Prometheus `remote_write` receiver whose body is snappy-compressed BINARY (the
    /// `body` String would corrupt it). Only populated/read under `otel-export`.
    #[cfg(feature = "otel-export")]
    body_bytes: Vec<u8>,
}

/// Read one HTTP/1.1 request: headers to the blank line, then `Content-Length` body.
/// Mirrors [`crate::server::sparql_http`]'s reader (bounded header flood guard).
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<HttpRequest> {
    let mut buf = Vec::new();
    let header_end = read_header_bytes(stream, &mut buf).await?;

    let head = std::str::from_utf8(&buf[..header_end]).ok()?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let (method, target, version) = parse_request_line(request_line)?;

    let headers = parse_headers(lines)?;
    if version == "HTTP/1.1" && headers.host_count != 1 {
        return None;
    }
    let content_length = headers.content_length.unwrap_or(0);
    let body = read_body(stream, &buf, header_end, content_length).await?;

    Some(HttpRequest {
        method,
        target,
        content_type: headers.content_type,
        body: String::from_utf8_lossy(&body).to_string(),
        #[cfg(feature = "otel-export")]
        body_bytes: body,
    })
}

/// Read into `buf` until the `\r\n\r\n` header/body boundary appears, bounded by
/// `MAX_HTTP_HEADER_BYTES`. Returns the boundary offset. Extracted from
/// [`read_request`].
async fn read_header_bytes(stream: &mut tokio::net::TcpStream, buf: &mut Vec<u8>) -> Option<usize> {
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(buf, b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_HTTP_HEADER_BYTES {
            return None;
        }
    };
    if header_end > MAX_HTTP_HEADER_BYTES {
        return None;
    }
    Some(header_end)
}

/// Parse + validate the HTTP request line (`METHOD target VERSION`): only
/// GET/POST/OPTIONS, only HTTP/1.0 or HTTP/1.1, an absolute-path target with no control
/// bytes, within `MAX_HTTP_TARGET_BYTES`. Extracted from [`read_request`].
fn parse_request_line(request_line: &str) -> Option<(String, String, &str)> {
    if request_line.len() > MAX_HTTP_TARGET_BYTES + 32 {
        return None;
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let version = parts.next()?;
    if parts.next().is_some()
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || !matches!(method.as_str(), "GET" | "POST" | "OPTIONS")
        || target.len() > MAX_HTTP_TARGET_BYTES
        || !target.starts_with('/')
        || target.bytes().any(|byte| byte.is_ascii_control())
    {
        return None;
    }
    Some((method, target, version))
}

/// The header values [`parse_headers`] extracts while validating each header line.
#[derive(Default)]
struct ParsedHeaders {
    content_length: Option<usize>,
    content_type: String,
    host_count: usize,
}

/// Validate + fold every header line (bounded count/length, RFC 7230 token/field-value
/// syntax), collecting `content-length`/`content-type`/`host` and rejecting
/// `transfer-encoding` outright (chunked framing is intentionally unsupported —
/// accepting it as an empty body would create request-smuggling ambiguity). Extracted
/// from [`read_request`].
fn parse_headers<'a>(lines: impl Iterator<Item = &'a str>) -> Option<ParsedHeaders> {
    let mut headers = ParsedHeaders::default();
    for (index, line) in lines.enumerate() {
        if index >= MAX_HTTP_HEADERS || line.len() > MAX_HTTP_HEADER_LINE_BYTES {
            return None;
        }
        let (key, value) = line.split_once(':')?;
        if !is_valid_header_line(key, value) {
            return None;
        }
        apply_header(&mut headers, key, value)?;
    }
    Some(headers)
}

/// RFC 7230 `field-name`/`field-value` syntax check for one header line. Extracted from
/// [`parse_headers`].
fn is_valid_header_line(key: &str, value: &str) -> bool {
    !key.is_empty()
        && key.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_control() && byte != b'\t')
}

/// Fold one validated header's `(key, value)` into `headers`. `None` signals a
/// rejection (a repeated/oversized `content-length`, more than one non-empty `host`, or
/// any `transfer-encoding`). Extracted from [`parse_headers`].
fn apply_header(headers: &mut ParsedHeaders, key: &str, value: &str) -> Option<()> {
    match key.to_ascii_lowercase().as_str() {
        "content-length" => {
            if headers.content_length.is_some() {
                return None;
            }
            let parsed = value.trim().parse::<usize>().ok()?;
            if parsed > MAX_HTTP_BODY_BYTES {
                return None;
            }
            headers.content_length = Some(parsed);
        }
        "content-type" => headers.content_type = value.trim().to_ascii_lowercase(),
        "host" => {
            headers.host_count += 1;
            if headers.host_count > 1 || value.trim().is_empty() {
                return None;
            }
        }
        "transfer-encoding" => return None,
        _ => {}
    }
    Some(())
}

/// Read the request body to `content_length` (already bounded to
/// `MAX_HTTP_BODY_BYTES` by [`apply_header`]), starting from whatever body bytes
/// already arrived in `buf` past the header boundary. Extracted from [`read_request`].
async fn read_body(
    stream: &mut tokio::net::TcpStream,
    buf: &[u8],
    header_end: usize,
    content_length: usize,
) -> Option<Vec<u8>> {
    let mut tmp = [0u8; 4096];
    let mut body = buf[header_end + 4..].to_vec();
    if body.len() > content_length || body.len() > MAX_HTTP_BODY_BYTES {
        return None;
    }
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        if body.len().saturating_add(n) > content_length
            || body.len().saturating_add(n) > MAX_HTTP_BODY_BYTES
        {
            return None;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    Some(body)
}

/// Serve the observability log-ingestion HTTP surface on `listener`, backed by
/// `state`. One task per connection, one response per request, connection: close —
/// the SAME dependency-free idiom as the SPARQL / metrics listeners.
pub async fn serve(listener: TcpListener, state: Arc<ObsState>) {
    serve_inner(listener, state, None).await;
}

/// Production observability listener linked to the engine's live isolation
/// policy. The PromQL, trace and log-search routes have no verified request
/// envelope, so they can be failed closed as soon as secure/RLS policy activates.
pub async fn serve_with_security(
    listener: TcpListener,
    state: Arc<ObsState>,
    security_state: Arc<tokio::sync::RwLock<crate::server::ServerState>>,
) {
    serve_inner(listener, state, Some(security_state)).await;
}

async fn serve_inner(
    listener: TcpListener,
    state: Arc<ObsState>,
    security_state: Option<Arc<tokio::sync::RwLock<crate::server::ServerState>>>,
) {
    let connections = Arc::new(tokio::sync::Semaphore::new(MAX_HTTP_CONNECTIONS));
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let Ok(connection_permit) = connections.clone().try_acquire_owned() else {
            // Drop excess sockets immediately. Spawning a rejection task here
            // would recreate the same unbounded-task resource exhaustion.
            drop(stream);
            continue;
        };
        let state = state.clone();
        let security_state = security_state.clone();
        tokio::spawn(async move {
            let _connection_permit = connection_permit;
            let (status, ctype, body) =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_request(&mut stream)).await {
                    Ok(Some(req)) => handle(&state, security_state.as_ref(), req).await,
                    Ok(None) => (
                        "400 Bad Request",
                        "text/plain",
                        "malformed HTTP request".to_string(),
                    ),
                    Err(_) => (
                        "408 Request Timeout",
                        "text/plain",
                        "request read timeout".to_string(),
                    ),
                };
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

/// Every distinct operation the observability surface serves, classified by
/// **route**, never by HTTP-verb shape (GOC-62-keycloak-auth-standard.md §5:
/// "classify by declared operation semantics ... never by guessing from the
/// verb, the path shape, or which paths happened to be enumerated when the
/// gate was written").
///
/// BUG-037 (P0): the prior gate (`is_observability_read_carrier`) matched
/// `method == "GET"` plus a handful of named query-shaped paths. Every
/// ingest path this module routes below — `POST /v1/logs` (OTLP), `POST
/// /_bulk`/`POST */_bulk` (ES bulk), `POST /<stream>/_doc` (ES single-doc),
/// `POST /`/`/api/logs`/`/logs` (JSON-lines), `POST /v1/traces` (OTLP trace
/// ingest), and `POST /api/v1/write` (Prometheus `remote_write`, feature
/// `otel-export`) — is a `Method::POST` that names none of the read paths,
/// so the gate was never reached for any of them, in every deployment
/// configuration including `serve_with_security`. A mutation must be
/// authorized AT LEAST as strictly as a read, never less (same doc, same
/// section).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObsOperation {
    /// A query/search/label-listing op: PromQL (`/api/v1/query*`,
    /// `/api/v1/labels`, `/api/v1/label/*`), the EG-162 `_search` surface
    /// (SQL or structured, any of `/api/_search`, `/_search`,
    /// `*/_search`), or trace search/assembly/dependency-graph reads
    /// (`/api/traces`, `/api/traces/*`, `/api/dependencies`,
    /// `/api/services/dependencies`).
    Read,
    /// An ingest/write op — every other path this listener accepts (see the
    /// enumeration above), and the fail-closed default for any path this
    /// classifier does not explicitly recognize as a `Read`: a surface that
    /// cannot mint a caller must refuse, never proceed
    /// (GOC-62-keycloak-auth-standard.md §4), so an as-yet-unenumerated path
    /// gets the HIGHER obligation rather than silently passing through the
    /// way the old GET-shaped allowlist did.
    Mutation,
}

/// Classify `path` (already stripped of query string) as a [`ObsOperation`].
/// Never called for the two no-data control requests `handle` answers
/// BEFORE reaching the gate — CORS preflight (`OPTIONS`) and the health
/// probe (`GET /healthz` / `GET /`) — so every path this function sees
/// carries observability data one way or the other.
fn classify_observability_operation(path: &str) -> ObsOperation {
    if is_read_only_obs_path(path) {
        ObsOperation::Read
    } else {
        ObsOperation::Mutation
    }
}

/// The read-only observability endpoints: PromQL/labels queries, search, traces, and
/// service-dependency lookups. A table-driven rewrite of
/// [`classify_observability_operation`]'s membership test (same predicate: exact path,
/// prefix, or `/_search` suffix).
fn is_read_only_obs_path(path: &str) -> bool {
    const EXACT: &[&str] = &[
        "/api/v1/labels",
        "/api/_search",
        "/_search",
        "/api/traces",
        "/api/dependencies",
        "/api/services/dependencies",
    ];
    const PREFIXES: &[&str] = &["/api/v1/query", "/api/v1/label/", "/api/traces/"];

    EXACT.contains(&path)
        || PREFIXES.iter().any(|prefix| path.starts_with(prefix))
        || path.ends_with("/_search")
}

async fn observability_access_denied(
    security_state: Option<&Arc<tokio::sync::RwLock<crate::server::ServerState>>>,
) -> bool {
    if security_state.is_none() {
        return false;
    }
    // A18: neither observability reads NOR ingest/mutations carry a
    // credential this surface can verify yet (no `eg2.` envelope, bearer
    // token, or other proof), so no `CarrierAuthority` can ever be minted
    // here today; this always denies under `serve_with_security`, honestly
    // (via the real check) rather than via the old unconditional stub —
    // and, per BUG-037, applies identically to both `ObsOperation` arms.
    crate::server::access::unauthenticated_carrier_denied(None)
}

/// Route + execute an ingest request → `(status, content_type, body)`.
async fn handle(
    state: &Arc<ObsState>,
    security_state: Option<&Arc<tokio::sync::RwLock<crate::server::ServerState>>>,
    req: HttpRequest,
) -> (&'static str, &'static str, String) {
    let (path, query) = match req.target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (req.target.as_str(), ""),
    };
    if let Some(resp) = control_response(&req.method, path) {
        return resp;
    }
    // BUG-037: gate every remaining path (both `Read` and `Mutation`) — not
    // just the ones that happen to be GET/query-shaped. `handle`'s two
    // no-data control returns (OPTIONS above, the health probe just above)
    // already ran, so anything reaching this point genuinely serves
    // observability data one way or the other.
    if observability_access_denied(security_state).await {
        return access_denied_response(classify_observability_operation(path));
    }

    // CONCEPT:EG-KG.query.prometheus-http-query-api — the Prometheus HTTP query API (GET or POST), routed BEFORE the
    // POST-only ingest guard (instant queries are typically GET). Gated on `promql`,
    // which implies `obs`; absent that feature these paths fall through to 404.
    #[cfg(feature = "promql")]
    if let Some(resp) = try_promql_route(state, &req.method, path, query, &req.body).await {
        return resp;
    }

    // CONCEPT:EG-OS.observability.trace-assembly — distributed-trace surface (GET or POST), routed BEFORE the
    // POST-only ingest guard (trace SEARCH / assembly / dependency-graph are GET).
    // Gated on `traces`, which implies `obs`; absent that feature these paths fall
    // through to 404. Covers OTLP-JSON ingest (`POST /v1/traces`), trace search
    // (`/api/traces`), single-trace assembly (`/api/traces/<id>`) and the
    // service-dependency graph (`/api/dependencies`).
    #[cfg(feature = "traces")]
    if let Some(resp) = try_traces_route(state, &req.method, path, query, &req.body).await {
        return resp;
    }

    // CONCEPT:EG-OS.observability.prometheus-ingest — the Prometheus `remote_write` receiver (`POST /api/v1/write`):
    // decode the snappy-compressed protobuf WriteRequest from the RAW body bytes (the
    // lossy-UTF-8 `body` String would corrupt the binary) and land its samples in the
    // durable eg-tsdb SeriesStore. Gated on `otel-export`; absent the feature this path
    // falls through to the unknown-ingest 404.
    #[cfg(feature = "otel-export")]
    if let Some(resp) = try_otel_write_route(state, &req.method, path, &req.body_bytes).await {
        return resp;
    }

    handle_ingest(state, &req, path, query).await
}

/// The POST-only ingest path: `_search` (EG-162), then route-by-shape log ingest.
/// Extracted from [`handle`]'s tail — everything after the GET-friendly gated routes.
async fn handle_ingest(
    state: &Arc<ObsState>,
    req: &HttpRequest,
    path: &str,
    query: &str,
) -> (&'static str, &'static str, String) {
    if req.method != "POST" {
        return (
            "405 Method Not Allowed",
            "text/plain",
            "POST only".to_string(),
        );
    }

    // EG-162 search surface: O2/Elasticsearch `_search`-shaped query API. Routed
    // BEFORE ingest (no ingest path ends with `_search`).
    if path == "/api/_search" || path == "/_search" || path.ends_with("/_search") {
        return handle_search(state, path, query, &req.body).await;
    }

    // The `stream` query param is the default stream for shapes that don't name one.
    let default_stream = query_param(query, "stream").unwrap_or_else(|| "default".to_string());

    // Route by path → parse into records + choose the response shape.
    let Some((records, shape)) = route_ingest_records(path, &req.body, &default_stream) else {
        return (
            "404 Not Found",
            "text/plain",
            "unknown ingest path".to_string(),
        );
    };

    let records = match records {
        Ok(r) => r,
        Err(e) => return ("400 Bad Request", "text/plain", e),
    };

    // Ingest OFF the reactor (redb + Tantivy commit are blocking).
    let st = state.clone();
    let outcome = ::tokio::task::spawn_blocking(move || st.ingest(records)).await;
    match outcome {
        Ok(Ok(o)) => format_ingest_success(shape, &o),
        Ok(Err(error)) => {
            tracing::warn!(%error, "observability ingest failed");
            (
                "500 Internal Server Error",
                "text/plain",
                "observability ingest failed".to_string(),
            )
        }
        Err(_) => (
            "500 Internal Server Error",
            "text/plain",
            "observability ingest worker failed".to_string(),
        ),
    }
}

/// The two no-data control responses [`handle`] answers BEFORE the access-denied gate:
/// CORS preflight (`OPTIONS`) and the health probe (`GET /healthz` / `GET /`). `None`
/// for anything else (falls through to the gated routes). Extracted from [`handle`].
fn control_response(method: &str, path: &str) -> Option<(&'static str, &'static str, String)> {
    if method == "OPTIONS" {
        return Some(("204 No Content", "text/plain", String::new()));
    }
    if path == "/healthz" || path == "/" && method == "GET" {
        return Some(("200 OK", "text/plain", "ok".to_string()));
    }
    None
}

/// The literal 403 response body for a denied observability request — per-operation
/// text (not a `format!` interpolation) so the exact denial string stays greppable
/// verbatim in source, matching every other carrier's static denial string
/// (`scripts/check_universal_read_rls.py`). Extracted from [`handle`] (BUG-037: the
/// SAME check gates both `ObsOperation` arms).
fn access_denied_response(op: ObsOperation) -> (&'static str, &'static str, String) {
    let body = match op {
        ObsOperation::Read => {
            "ACCESS_DENIED: observability read carriers require verified tenant ownership"
        }
        ObsOperation::Mutation => {
            "ACCESS_DENIED: observability ingest carriers require verified tenant ownership"
        }
    };
    ("403 Forbidden", "text/plain", body.to_string())
}

/// The Prometheus HTTP query API route (`promql` feature). `None` when `path` doesn't
/// match, so [`handle`] falls through to its next route. Extracted from [`handle`].
#[cfg(feature = "promql")]
async fn try_promql_route(
    state: &Arc<ObsState>,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
) -> Option<(&'static str, &'static str, String)> {
    if path.starts_with("/api/v1/query")
        || path == "/api/v1/labels"
        || path.starts_with("/api/v1/label/")
    {
        return Some(crate::server::promql::handle(state, method, path, query, body).await);
    }
    None
}

/// The distributed-trace surface route (`traces` feature): OTLP-JSON ingest, trace
/// search/assembly, and the service-dependency graph. `None` when `path` doesn't match.
/// Extracted from [`handle`].
#[cfg(feature = "traces")]
async fn try_traces_route(
    state: &Arc<ObsState>,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
) -> Option<(&'static str, &'static str, String)> {
    if path == "/v1/traces"
        || path == "/api/traces"
        || path.starts_with("/api/traces/")
        || path == "/api/dependencies"
        || path == "/api/services/dependencies"
    {
        return Some(crate::server::traces::handle(state, method, path, query, body).await);
    }
    None
}

/// The Prometheus `remote_write` receiver route (`otel-export` feature). `None` when
/// `path` doesn't match. Extracted from [`handle`].
#[cfg(feature = "otel-export")]
async fn try_otel_write_route(
    state: &Arc<ObsState>,
    method: &str,
    path: &str,
    body_bytes: &[u8],
) -> Option<(&'static str, &'static str, String)> {
    if path == "/api/v1/write" {
        return Some(remote_write::handle(state, method, body_bytes).await);
    }
    None
}

/// The routed log-ingest shape [`handle`] parses `body` into, driving both which parser
/// runs and which success response format [`format_ingest_success`] uses.
enum IngestShape {
    Otlp,
    EsBulk,
    EsDoc,
    Lines,
}

/// Route `path` to its ingest parser, producing the parsed records (or the parse error)
/// plus which [`IngestShape`] to format the success response as. `None` when `path`
/// matches no known ingest shape (→ 404). Extracted from [`handle`].
fn route_ingest_records(
    path: &str,
    body: &str,
    default_stream: &str,
) -> Option<(Result<Vec<LogRecord>, String>, IngestShape)> {
    if path == "/v1/logs" {
        return Some((parse_otlp_logs(body, default_stream), IngestShape::Otlp));
    }
    if path == "/_bulk" || path.ends_with("/_bulk") {
        return Some((Ok(parse_es_bulk(body, default_stream)), IngestShape::EsBulk));
    }
    if let Some(stream) = es_doc_stream(path) {
        // `/<stream>/_doc` — a single ES document.
        let doc: Result<serde_json::Value, String> =
            serde_json::from_str(body).map_err(|e| format!("parse _doc JSON: {e}"));
        return Some((
            doc.map(|d| vec![doc_to_record(&d, &stream)]),
            IngestShape::EsDoc,
        ));
    }
    if path == "/" || path == "/api/logs" || path == "/logs" {
        return Some((
            Ok(parse_json_lines(body, default_stream)),
            IngestShape::Lines,
        ));
    }
    None
}

/// Format the ingest success response for one [`IngestShape`]. Extracted from
/// [`handle`]'s tail.
fn format_ingest_success(
    shape: IngestShape,
    outcome: &IngestOutcome,
) -> (&'static str, &'static str, String) {
    match shape {
        IngestShape::Otlp => (
            "200 OK",
            "application/json",
            "{\"partialSuccess\":{}}".to_string(),
        ),
        IngestShape::EsBulk => (
            "200 OK",
            "application/json",
            es_bulk_response(outcome.accepted),
        ),
        IngestShape::EsDoc => (
            "201 Created",
            "application/json",
            "{\"result\":\"created\",\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0}}"
                .to_string(),
        ),
        IngestShape::Lines => (
            "200 OK",
            "application/json",
            format!(
                "{{\"successful\":{},\"failed\":0,\"segments\":{}}}",
                outcome.accepted, outcome.segments_flushed
            ),
        ),
    }
}

// ── EG-162 search surface (O2 / Elasticsearch `_search`) ────────────────────────

/// Route + execute a `_search` request → `(status, content_type, body)`.
///
/// Two modes on the SAME endpoint, discriminated by the JSON body:
///  * a raw SQL query — `{"sql": "SELECT …"}` (or `{"query":{"sql":"…"}}`, the O2
///    shape) — runs DataFusion over the `logs` table (segments + hot buffers);
///  * a structured log search — `{stream, start_time, end_time, query, size, …}` —
///    returns O2/ES-shaped hits UNIONed across the hot + cold tiers.
///
/// The stream may come from the path (`/api/<org>/<stream>/_search`) or the body.
async fn handle_search(
    state: &Arc<ObsState>,
    path: &str,
    query: &str,
    body: &str,
) -> (&'static str, &'static str, String) {
    let val: serde_json::Value = if body.trim().is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_str(body) {
            Ok(v) => v,
            Err(e) => {
                return (
                    "400 Bad Request",
                    "text/plain",
                    format!("parse _search JSON: {e}"),
                )
            }
        }
    };

    // SQL mode: top-level `sql`, or the O2 `{"query":{"sql":…}}` nesting.
    let sql = val.get("sql").and_then(|v| v.as_str()).or_else(|| {
        val.get("query")
            .and_then(|q| q.get("sql"))
            .and_then(|v| v.as_str())
    });
    if let Some(sql) = sql {
        return run_sql_search(state, sql).await;
    }

    // Structured search mode: resolve the stream (path wins, then body, then `?stream`).
    let Some(stream) = resolve_search_stream(path, query, &val) else {
        return (
            "400 Bad Request",
            "text/plain",
            "search requires a stream (path /api/<org>/<stream>/_search or body `stream`)"
                .to_string(),
        );
    };

    let q = parse_log_query(&val, stream);
    let st = state.clone();
    match ::tokio::task::spawn_blocking(move || st.search_logs(&q)).await {
        Ok(Ok(hits)) => ("200 OK", "application/json", es_search_response(&hits)),
        Ok(Err(error)) => {
            tracing::warn!(%error, "observability search failed");
            (
                "400 Bad Request",
                "text/plain",
                "observability search failed".to_string(),
            )
        }
        Err(_) => (
            "500 Internal Server Error",
            "text/plain",
            "observability search worker failed".to_string(),
        ),
    }
}

/// The SQL-mode branch of [`handle_search`]: run `sql` off the reactor via
/// `search_sql`. Extracted from [`handle_search`].
async fn run_sql_search(state: &Arc<ObsState>, sql: &str) -> (&'static str, &'static str, String) {
    let sql = sql.to_string();
    let st = state.clone();
    match ::tokio::task::spawn_blocking(move || st.search_sql(&sql)).await {
        Ok(Ok(res)) => ("200 OK", "application/json", sql_search_response(&res)),
        Ok(Err(error)) => {
            tracing::warn!(%error, "observability SQL search failed");
            (
                "400 Bad Request",
                "text/plain",
                "observability SQL search failed".to_string(),
            )
        }
        Err(_) => (
            "500 Internal Server Error",
            "text/plain",
            "observability SQL search worker failed".to_string(),
        ),
    }
}

/// Resolve the structured-search-mode stream: the path (`/api/<org>/<stream>/_search`)
/// wins, then a `stream`/`_stream`/`index`/`_index` body key, then the `?stream` query
/// param. Extracted from [`handle_search`].
fn resolve_search_stream(path: &str, query: &str, val: &serde_json::Value) -> Option<String> {
    search_stream_from_path(path)
        .or_else(|| search_stream_from_body(val))
        .or_else(|| query_param(query, "stream"))
}

/// The body-key fallback of [`resolve_search_stream`]: the first non-empty
/// `stream`/`_stream`/`index`/`_index` key.
fn search_stream_from_body(val: &serde_json::Value) -> Option<String> {
    for key in ["stream", "_stream", "index", "_index"] {
        if let Some(s) = val
            .get(key)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            return Some(s.to_string());
        }
    }
    None
}

/// If `path` is `/api/<org>/<stream>/_search`, return `<stream>`; else `None`
/// (`/api/_search` and `/_search` carry the stream in the body).
fn search_stream_from_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    let parts: Vec<&str> = trimmed.split('/').collect();
    // /api/<org>/<stream>/_search  → ["api", org, stream, "_search"]
    if parts.len() == 4 && parts[0] == "api" && parts[3] == "_search" && !parts[2].is_empty() {
        return Some(parts[2].to_string());
    }
    // /<stream>/_search → ["stream", "_search"] (but NOT the org-less "/api/_search").
    if parts.len() == 2 && parts[1] == "_search" && !parts[0].is_empty() && parts[0] != "api" {
        return Some(parts[0].to_string());
    }
    None
}

/// Build a [`LogQuery`] from the parsed `_search` JSON body. Tolerates the common
/// shapes: `start_time`/`end_time` (O2) or `from`/`to` for the window; a full-text
/// `query` string (bare, or the ES `{"query_string":{"query":…}}` nesting); a
/// `filters` object of attribute equalities + a `severity`; and `size`.
fn parse_log_query(val: &serde_json::Value, stream: String) -> LogQuery {
    let ts = |keys: &[&str]| -> Option<i64> {
        for k in keys {
            if let Some(n) = val.get(*k).and_then(|v| v.as_i64()) {
                return Some(n);
            }
        }
        None
    };
    let from = ts(&["start_time", "from_ts", "from"]).unwrap_or(i64::MIN);
    let to = ts(&["end_time", "to_ts", "to"]).unwrap_or(i64::MAX);

    // Full-text terms: bare `query`/`q` string, or ES `query.query_string.query`.
    let terms = val
        .get("query")
        .and_then(|q| q.as_str())
        .map(str::to_string)
        .or_else(|| val.get("q").and_then(|v| v.as_str()).map(str::to_string))
        .or_else(|| {
            val.get("query")
                .and_then(|q| q.get("query_string"))
                .and_then(|qs| qs.get("query"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .filter(|s| !s.trim().is_empty());

    let severity = val
        .get("severity")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let mut filters = Vec::new();
    if let Some(obj) = val.get("filters").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            filters.push((k.clone(), scalar_to_string(v)));
        }
    }

    let size = val
        .get("size")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_SEARCH_SIZE);

    LogQuery {
        stream,
        from,
        to,
        terms,
        filters,
        severity,
        size,
    }
}

/// Render one log record as an Elasticsearch/O2 `_source` object.
fn record_source(r: &LogRecord) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("_timestamp".into(), serde_json::json!(r.ts));
    obj.insert("stream".into(), serde_json::json!(r.stream));
    obj.insert("severity".into(), serde_json::json!(r.severity));
    obj.insert("message".into(), serde_json::json!(r.body));
    for (k, v) in &r.attrs {
        // Never let an attribute clobber a reserved field.
        if !obj.contains_key(k) {
            obj.insert(k.clone(), serde_json::json!(v));
        }
    }
    serde_json::Value::Object(obj)
}

/// The Elasticsearch/O2 `_search` response envelope over the matched records.
fn es_search_response(hits: &[LogRecord]) -> String {
    let items: Vec<serde_json::Value> = hits
        .iter()
        .map(|r| {
            serde_json::json!({
                "_index": r.stream,
                "_score": 1.0,
                "_source": record_source(r),
            })
        })
        .collect();
    serde_json::json!({
        "took": 0,
        "timed_out": false,
        "hits": {
            "total": { "value": hits.len(), "relation": "eq" },
            "hits": items,
        }
    })
    .to_string()
}

/// The SQL `_search` response: columns + rows, plus row objects (`hits`) keyed by
/// column name (the O2 SQL result shape).
fn sql_search_response(res: &eg_query::TypedQueryResult) -> String {
    let cols: Vec<&str> = res.columns.iter().map(|c| c.name.as_str()).collect();
    let hits: Vec<serde_json::Value> = res
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (i, cell) in row.iter().enumerate() {
                if let Some(name) = cols.get(i) {
                    obj.insert((*name).to_string(), cell.clone());
                }
            }
            serde_json::Value::Object(obj)
        })
        .collect();
    serde_json::json!({
        "took": 0,
        "total": res.rows.len(),
        "columns": cols,
        "hits": hits,
    })
    .to_string()
}

/// An ES `_bulk` response: `errors:false` with one `index`/`created` item per doc.
fn es_bulk_response(n: usize) -> String {
    let items: Vec<serde_json::Value> = (0..n)
        .map(|_| serde_json::json!({"index":{"status":201,"result":"created"}}))
        .collect();
    serde_json::json!({ "took": 0, "errors": false, "items": items }).to_string()
}

/// If `path` is `/<stream>/_doc` (optionally `/<stream>/_doc/<id>`), return `<stream>`.
fn es_doc_stream(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    let mut parts = trimmed.split('/');
    let stream = parts.next()?;
    let doc_marker = parts.next()?;
    if stream.is_empty() || doc_marker != "_doc" {
        return None;
    }
    Some(stream.to_string())
}

/// Extract a query-string parameter value (no percent-decoding needed for a bare
/// stream name; kept minimal).
fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key && !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_temporaries(parent: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(parent)
            .expect("read snapshot parent")
            .map(|entry| entry.expect("read snapshot entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".msgpack.tmp"))
            })
            .collect()
    }

    #[cfg(feature = "traces")]
    #[tokio::test(flavor = "current_thread")]
    async fn public_trace_persistence_yields_the_current_thread_reactor() {
        let dir = tempfile::tempdir().expect("temp dir");
        let persist_dir = dir.path().to_str().expect("utf8 temp path");
        let obs = ObsState::open(Some(persist_dir), 1024).await.expect("open");
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let counter = Arc::new(AtomicU64::new(0));
        let observed = counter.clone();

        let lock_holder = std::thread::spawn(move || {
            let _guard = snapshot_write_lock()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entered_tx.send(()).expect("announce held lock");
            release_rx.recv().expect("release held lock");
        });
        entered_rx
            .await
            .expect("lock holder reached deterministic barrier");
        let progress = async move {
            tokio::task::yield_now().await;
            observed.fetch_add(1, Ordering::SeqCst);
            release_tx.send(()).expect("release persistence lock");
        };
        let (persisted, ()) = tokio::join!(obs.persist_traces(), progress);
        persisted.expect("public trace persistence");
        lock_holder.join().expect("join lock holder");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "the current-thread reactor must progress while persistence is blocked"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_waiter_does_not_cancel_started_atomic_publication() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("snapshot.msgpack");
        let written_path = path.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

        let waiter = tokio::spawn(async move {
            ::tokio::task::spawn_blocking(move || {
                entered_tx
                    .send(())
                    .map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
                release_rx
                    .recv()
                    .map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
                let result = write_snapshot_atomically_blocking(written_path, || {
                    Ok(b"committed-after-cancellation".to_vec())
                });
                if finished_tx.send(result.clone()).is_err() {
                    return Err(SNAPSHOT_WRITE_ERROR.to_string());
                }
                result
            })
            .await
            .map_err(|_| "observability persistence worker failed".to_string())?
        });
        entered_rx
            .await
            .expect("worker reached deterministic barrier");
        waiter.abort();
        release_tx.send(()).expect("release persistence worker");
        finished_rx
            .await
            .expect("started worker must finish")
            .expect("atomic publication");

        assert_eq!(
            std::fs::read(&path).expect("published snapshot"),
            b"committed-after-cancellation"
        );
        assert!(snapshot_temporaries(dir.path()).is_empty());
    }

    #[test]
    fn failed_atomic_publication_preserves_authority_and_cleans_temporary() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("snapshot.msgpack");
        write_snapshot_atomically_blocking(path.clone(), || Ok(b"prior-authority".to_vec()))
            .expect("seed private authority");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
                .expect("make the boundary shared read-only");
        }

        let error = write_snapshot_atomically_blocking_with(
            path.clone(),
            || Ok(b"unpublished".to_vec()),
            || Err("injected failure detail".to_string()),
            || {},
        )
        .expect_err("injected failure must abort publication");

        assert_eq!(error, SNAPSHOT_WRITE_ERROR);
        assert_eq!(
            std::fs::read(&path).expect("read prior authority"),
            b"prior-authority"
        );
        assert!(snapshot_temporaries(dir.path()).is_empty());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.path())
                .expect("directory metadata")
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o022,
                0,
                "publication must require its trust boundary"
            );
        }
    }

    #[test]
    fn panicked_snapshot_attempt_does_not_poison_later_publication() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("snapshot.msgpack");
        let failed_path = path.clone();

        let panic = std::panic::catch_unwind(|| {
            let _result = write_snapshot_atomically_blocking_with(
                failed_path,
                || Ok(b"abandoned".to_vec()),
                || panic!("injected publication panic"),
                || {},
            );
        });
        assert!(panic.is_err(), "the injected panic must escape its attempt");

        write_snapshot_atomically_blocking(path.clone(), || Ok(b"recovered".to_vec()))
            .expect("later publication recovers the poisoned lock");
        assert_eq!(std::fs::read(path).expect("read authority"), b"recovered");
        assert!(snapshot_temporaries(dir.path()).is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parent_replacement_cannot_redirect_an_opened_publication() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("temp dir");
        let parent = dir.path().join("authority");
        let moved_parent = dir.path().join("moved-authority");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&parent).expect("create authority");
        std::fs::create_dir_all(&outside).expect("create outside");
        let path = parent.join("snapshot.msgpack");

        let error = write_snapshot_atomically_blocking_with(
            path,
            || Ok(b"opened-authority".to_vec()),
            || {
                std::fs::rename(&parent, &moved_parent)
                    .map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
                symlink(&outside, &parent).map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())
            },
            || {},
        )
        .expect_err("renamed authority must not be acknowledged");

        assert_eq!(error, SNAPSHOT_WRITE_ERROR);
        assert!(!outside.join("snapshot.msgpack").exists());
        assert_eq!(
            std::fs::read(moved_parent.join("snapshot.msgpack")).expect("fd-bound publication"),
            b"opened-authority"
        );
    }

    #[test]
    fn oversized_and_nonregular_snapshots_fail_closed() {
        let dir = tempfile::tempdir().expect("temp dir");
        let oversized = dir.path().join("oversized.msgpack");
        std::fs::File::create(&oversized)
            .and_then(|file| file.set_len(MAX_SNAPSHOT_BYTES as u64 + 1))
            .expect("create sparse oversized snapshot");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&oversized, std::fs::Permissions::from_mode(0o600))
                .expect("make oversized authority private");
        }
        assert_eq!(
            read_snapshot_blocking(&oversized).expect_err("oversized snapshot must fail"),
            SNAPSHOT_READ_ERROR
        );

        let nonregular = dir.path().join("directory.msgpack");
        std::fs::create_dir(&nonregular).expect("create nonregular snapshot authority");
        assert_eq!(
            read_snapshot_blocking(&nonregular).expect_err("directory read must fail"),
            SNAPSHOT_READ_ERROR
        );
        assert_eq!(
            write_snapshot_atomically_blocking(nonregular, || Ok(Vec::new()))
                .expect_err("directory publication must fail"),
            SNAPSHOT_WRITE_ERROR
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let writable = dir.path().join("writable.msgpack");
            std::fs::write(&writable, b"untrusted").expect("write broad authority");
            std::fs::set_permissions(&writable, std::fs::Permissions::from_mode(0o666))
                .expect("make authority broadly writable");
            assert_eq!(
                read_snapshot_blocking(&writable).expect_err("broad read authority must fail"),
                SNAPSHOT_READ_ERROR
            );
            assert_eq!(
                write_snapshot_atomically_blocking(writable, || Ok(b"replacement".to_vec()))
                    .expect_err("broad destination authority must fail"),
                SNAPSHOT_WRITE_ERROR
            );

            let private = dir.path().join("private.msgpack");
            write_snapshot_atomically_blocking(private.clone(), || Ok(b"private".to_vec()))
                .expect("publish private snapshot");
            let mode = std::fs::metadata(private)
                .expect("private snapshot metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0, "new snapshots must be mode 0600");
        }
    }

    #[test]
    fn aggregate_manifest_recovery_budget_is_bounded() {
        let mut total = MAX_MANIFEST_FILES;
        assert_eq!(
            include_manifest_budget(&mut total, 1, MAX_MANIFEST_FILES).expect_err("file overflow"),
            MANIFEST_BOUNDS_ERROR
        );

        let mut total = 0usize;
        include_manifest_budget(
            &mut total,
            MAX_MANIFEST_TOTAL_BYTES,
            MAX_MANIFEST_TOTAL_BYTES,
        )
        .expect("exact byte bound");
        assert_eq!(
            include_manifest_budget(&mut total, 1, MAX_MANIFEST_TOTAL_BYTES)
                .expect_err("byte overflow"),
            MANIFEST_BOUNDS_ERROR
        );

        let mut total = 0usize;
        include_manifest_budget(
            &mut total,
            MAX_MANIFEST_TOTAL_ITEMS,
            MAX_MANIFEST_TOTAL_ITEMS,
        )
        .expect("exact item bound");
        assert_eq!(
            include_manifest_budget(&mut total, 1, MAX_MANIFEST_TOTAL_ITEMS)
                .expect_err("item overflow"),
            MANIFEST_BOUNDS_ERROR
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn public_open_rejects_a_nonregular_manifest_authority() {
        let dir = tempfile::tempdir().expect("temp dir");
        let manifests = dir.path().join("obs/segments");
        std::fs::create_dir_all(&manifests).expect("create manifest directory");
        std::fs::create_dir(manifests.join("nonregular.msgpack"))
            .expect("create nonregular manifest");
        let persist_dir = dir.path().to_str().expect("utf8 temp path");

        let error = match ObsState::open(Some(persist_dir), 1).await {
            Err(error) => error,
            Ok(_) => panic!("nonregular manifest authority must fail"),
        };
        assert_eq!(error, MANIFEST_READ_ERROR);
    }

    #[cfg(feature = "traces")]
    #[tokio::test(flavor = "current_thread")]
    async fn public_open_rejects_an_oversized_trace_snapshot() {
        let dir = tempfile::tempdir().expect("temp dir");
        let obs_base = dir.path().join("obs");
        std::fs::create_dir_all(&obs_base).expect("create obs directory");
        std::fs::File::create(traces_snapshot_path(&obs_base))
            .and_then(|file| file.set_len(MAX_SNAPSHOT_BYTES as u64 + 1))
            .expect("create sparse oversized trace snapshot");
        let persist_dir = dir.path().to_str().expect("utf8 temp path");

        let error = match ObsState::open(Some(persist_dir), 1).await {
            Err(error) => error,
            Ok(_) => panic!("oversized trace snapshot must fail"),
        };
        assert_eq!(error, SNAPSHOT_READ_ERROR);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn colliding_stream_names_publish_concurrently_without_aliasing() {
        fn record(stream: &str, ts: i64) -> LogRecord {
            LogRecord {
                ts,
                stream: stream.to_string(),
                severity: "INFO".to_string(),
                body: format!("body-{stream}"),
                attrs: BTreeMap::new(),
            }
        }

        let dir = tempfile::tempdir().expect("temp dir");
        let persist_dir = dir.path().to_str().expect("utf8 temp path");
        let obs = Arc::new(ObsState::open(Some(persist_dir), 1).await.expect("open"));
        let first = obs.clone();
        let second = obs.clone();
        let third = obs.clone();
        let fourth = obs.clone();
        let (first_result, second_result, third_result, fourth_result) = tokio::join!(
            ::tokio::task::spawn_blocking(move || first.ingest(vec![record("tenant/a", 1)])),
            ::tokio::task::spawn_blocking(move || second.ingest(vec![record("tenant_a", 2)])),
            ::tokio::task::spawn_blocking(move || third.ingest(vec![record("same", 3)])),
            ::tokio::task::spawn_blocking(move || fourth.ingest(vec![record("same", 4)])),
        );
        first_result
            .expect("join first ingest")
            .expect("first ingest");
        second_result
            .expect("join second ingest")
            .expect("second ingest");
        third_result
            .expect("join third ingest")
            .expect("third ingest");
        fourth_result
            .expect("join fourth ingest")
            .expect("fourth ingest");
        assert_ne!(
            segment_manifest_path(&dir.path().join("obs"), "tenant/a"),
            segment_manifest_path(&dir.path().join("obs"), "tenant_a")
        );
        drop(obs);

        let reopened = ObsState::open(Some(persist_dir), 1).await.expect("reopen");
        assert_eq!(reopened.segments_for("tenant/a").len(), 1);
        assert_eq!(reopened.segments_for("tenant_a").len(), 1);
        assert_eq!(
            reopened.segments_for("same").len(),
            2,
            "the last same-stream publication must include both concurrent segments"
        );
    }

    #[test]
    fn missing_manifest_directory_is_an_empty_fresh_authority() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert!(load_segment_manifests(dir.path())
            .expect("missing manifest directory")
            .0
            .is_empty());
    }

    #[test]
    fn corrupt_manifest_fails_closed_without_path_disclosure() {
        let dir = tempfile::tempdir().expect("temp dir");
        let manifests = dir.path().join("segments");
        std::fs::create_dir_all(&manifests).expect("create manifests");
        let path = manifests.join("private-stream.msgpack");
        write_snapshot_atomically_blocking(path.clone(), || Ok(b"not-messagepack".to_vec()))
            .expect("write private corrupt manifest");

        let error = load_segment_manifests(dir.path()).expect_err("corruption must fail");
        assert_eq!(error, "segment manifest is invalid or exceeds its bounds");
        assert!(!error.contains("private-stream"));
        assert!(!error.contains(&dir.path().display().to_string()));
    }

    #[cfg(feature = "traces")]
    #[test]
    fn corrupt_trace_snapshot_fails_closed_without_path_disclosure() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = traces_snapshot_path(dir.path());
        write_snapshot_atomically_blocking(path.clone(), || Ok(b"not-messagepack".to_vec()))
            .expect("write private corrupt trace snapshot");

        let error = match load_traces_snapshot(dir.path()) {
            Err(error) => error,
            Ok(_) => panic!("corruption must fail"),
        };
        assert_eq!(error, "trace snapshot is invalid or exceeds its bounds");
        assert!(!error.contains(&path.display().to_string()));
        assert!(!error.contains(&dir.path().display().to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_snapshot_and_manifest_authorities_fail_closed() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("temp dir");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&outside).expect("create outside");
        let outside_file = outside.join("authority.msgpack");
        std::fs::write(&outside_file, b"outside-authority").expect("seed outside");

        let destination = dir.path().join("snapshot.msgpack");
        symlink(&outside_file, &destination).expect("symlink destination");
        let read_error =
            read_snapshot_blocking(&destination).expect_err("symlinked read authority must fail");
        assert_eq!(read_error, SNAPSHOT_READ_ERROR);
        assert!(!read_error.contains(&destination.display().to_string()));
        let write_error = write_snapshot_atomically_blocking(destination, || {
            Ok(b"must-not-follow-symlink".to_vec())
        })
        .expect_err("symlinked write authority must fail");
        assert_eq!(write_error, SNAPSHOT_WRITE_ERROR);
        assert_eq!(
            std::fs::read(&outside_file).expect("outside authority unchanged"),
            b"outside-authority"
        );

        let obs_base = dir.path().join("obs");
        std::fs::create_dir_all(&obs_base).expect("create obs base");
        symlink(&outside, obs_base.join("segments")).expect("symlink manifest directory");
        assert_eq!(
            load_segment_manifests(&obs_base).expect_err("symlinked directory must fail"),
            "segment manifest directory is unavailable"
        );

        let linked_base = dir.path().join("linked-obs");
        symlink(&outside, &linked_base).expect("symlink manifest ancestor");
        std::fs::create_dir_all(outside.join("segments")).expect("create outside manifests");
        assert_eq!(
            load_segment_manifests(&linked_base).expect_err("symlinked ancestor must fail"),
            "segment manifest directory is unavailable"
        );

        let dangling_base = dir.path().join("dangling-obs");
        symlink(dir.path().join("missing-target"), &dangling_base)
            .expect("create dangling ancestor");
        assert_eq!(
            load_segment_manifests(&dangling_base).expect_err("dangling ancestor must fail"),
            MANIFEST_DIRECTORY_ERROR
        );
        assert_eq!(
            read_snapshot_blocking(&dangling_base.join("traces.msgpack"))
                .expect_err("dangling read ancestor must fail"),
            SNAPSHOT_READ_ERROR
        );
    }

    #[test]
    fn otlp_parses_and_derives_stream_from_service_name() {
        let body = r#"{
          "resourceLogs":[{
            "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeLogs":[{"logRecords":[
              {"timeUnixNano":"1700000000000000000","severityText":"ERROR",
               "body":{"stringValue":"payment declined"},
               "attributes":[{"key":"order","value":{"stringValue":"o-42"}}]},
              {"timeUnixNano":"1700000000000000001","severityText":"INFO",
               "body":{"stringValue":"cart viewed"}}
            ]}]
          }]
        }"#;
        let recs = parse_otlp_logs(body, "default").unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].stream, "checkout");
        assert_eq!(recs[0].severity, "ERROR");
        assert_eq!(recs[0].body, "payment declined");
        assert_eq!(recs[0].ts, 1_700_000_000_000_000_000);
        assert_eq!(recs[0].attrs.get("order").unwrap(), "o-42");
    }

    #[test]
    fn es_bulk_parses_action_doc_pairs() {
        let body = "{\"index\":{\"_index\":\"weblogs\"}}\n\
                    {\"@timestamp\":1700,\"level\":\"WARN\",\"message\":\"slow\"}\n\
                    {\"create\":{\"_index\":\"weblogs\"}}\n\
                    {\"message\":\"ok\",\"level\":\"INFO\"}\n\
                    {\"delete\":{\"_index\":\"weblogs\",\"_id\":\"x\"}}\n";
        let recs = parse_es_bulk(body, "default");
        assert_eq!(recs.len(), 2, "delete carries no source line");
        assert_eq!(recs[0].stream, "weblogs");
        assert_eq!(recs[0].severity, "WARN");
        assert_eq!(recs[0].body, "slow");
        assert_eq!(recs[0].ts, 1700 * 1_000_000, "epoch-ms → ns");
        assert_eq!(recs[1].body, "ok");
    }

    #[test]
    fn json_lines_parse_with_default_and_explicit_stream() {
        let body = "{\"message\":\"a\",\"level\":\"INFO\"}\n\
                    {\"message\":\"b\",\"stream\":\"other\",\"level\":\"ERROR\"}\n";
        let recs = parse_json_lines(body, "svc");
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].stream, "svc");
        assert_eq!(recs[1].stream, "other");
    }

    #[test]
    fn es_doc_path_extracts_stream() {
        assert_eq!(es_doc_stream("/logs/_doc").as_deref(), Some("logs"));
        assert_eq!(es_doc_stream("/logs/_doc/abc").as_deref(), Some("logs"));
        assert_eq!(es_doc_stream("/_bulk"), None);
        assert_eq!(es_doc_stream("/v1/logs"), None);
    }

    #[test]
    fn ingest_lands_in_tsdb_series_and_text_index() {
        let obs = ObsState::in_memory(1024).unwrap();
        let recs = vec![
            LogRecord {
                ts: 1000,
                stream: "app".into(),
                severity: "ERROR".into(),
                body: "database connection refused".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 2000,
                stream: "app".into(),
                severity: "INFO".into(),
                body: "server listening on port".into(),
                attrs: BTreeMap::new(),
            },
        ];
        let out = obs.ingest(recs).unwrap();
        assert_eq!(out.accepted, 2);

        // (a) tsdb: both points land in the series, ts-ordered.
        let points = obs.series_range("app", 0, 10_000).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].ts, 1000);
        assert_eq!(points[0].values[0], severity_number("ERROR"));

        // (b) text: a BM25 query retrieves the matching record.
        let hits = obs.search("app", "database connection", 10);
        assert_eq!(hits.len(), 1, "one doc matches 'database connection'");
        assert!(hits[0].id.starts_with("app:"));
        // A term only in the OTHER doc matches that one, not the first.
        let listening = obs.search("app", "listening port", 10);
        assert_eq!(listening.len(), 1);
    }

    #[test]
    fn threshold_flush_rolls_a_parquet_segment_that_round_trips() {
        // flush_threshold=2 → the 2-record ingest rolls one segment immediately.
        let obs = ObsState::in_memory(2).unwrap();
        let recs = vec![
            LogRecord {
                ts: 10,
                stream: "s".into(),
                severity: "INFO".into(),
                body: "one".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 20,
                stream: "s".into(),
                severity: "WARN".into(),
                body: "two".into(),
                attrs: BTreeMap::new(),
            },
        ];
        let out = obs.ingest(recs).unwrap();
        assert_eq!(out.segments_flushed, 1);

        let segs = obs.segments_for("s");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].row_count, 2);
        assert_eq!(segs[0].min_ts, 10);
        assert_eq!(segs[0].max_ts, 20);

        // The Parquet segment round-trips out of the CAS.
        let back = obs.read_segment(&segs[0]).unwrap();
        assert_eq!(back.len(), 2);
        let bodies: Vec<&str> = back.iter().map(|r| r.body.as_str()).collect();
        assert!(bodies.contains(&"one") && bodies.contains(&"two"));
    }

    #[test]
    fn small_stream_force_flush_via_flush_stream() {
        // Below the threshold: nothing auto-flushes, but flush_stream forces it.
        let obs = ObsState::in_memory(1000).unwrap();
        obs.ingest(vec![LogRecord {
            ts: 5,
            stream: "tiny".into(),
            severity: "INFO".into(),
            body: "hello".into(),
            attrs: BTreeMap::new(),
        }])
        .unwrap();
        assert!(obs.segments_for("tiny").is_empty());
        let m = obs.flush_stream("tiny").unwrap().expect("forced segment");
        assert_eq!(m.row_count, 1);
        assert!(
            obs.flush_stream("tiny").unwrap().is_none(),
            "buffer drained"
        );
    }

    /// BUG-016 -- end-to-end restart proof against a REAL `ObsState` bound to a
    /// real persist dir. Proves BOTH halves: (1) a restart with NO explicit
    /// `persist_traces()` call correctly starts empty -- persistence is never
    /// implicit on the hot ingest path (that would reintroduce BUG-017's
    /// per-request write-amplification defect class); (2) a restart AFTER
    /// `persist_traces()` recovers the exact trace, byte-identically re-derivable.
    #[cfg(feature = "traces")]
    #[tokio::test(flavor = "current_thread")]
    async fn bug_016_traces_survive_an_obsstate_restart_only_after_persist_traces() {
        let dir = tempfile::tempdir().expect("temp dir");
        let persist_dir = dir.path().to_str().expect("utf8 temp path");

        let span = eg_tsdb::traces::Span {
            trace_id: "t1".into(),
            span_id: "root".into(),
            parent_span_id: String::new(),
            service: "checkout".into(),
            operation: "POST /order".into(),
            start_time: 100,
            duration: 50,
            status: "OK".into(),
            attributes: BTreeMap::new(),
            events: Vec::new(),
        };

        // (1) Ingest a span but never call `persist_traces` -- a restart at this
        // point must see NOTHING durable yet (the bounded-staleness window).
        {
            let obs = ObsState::open(Some(persist_dir), 1024).await.expect("open");
            obs.trace_store().add_span(span.clone());
            assert_eq!(obs.trace_store().trace_count(), 1, "ingested in RAM");
        }
        {
            let reopened = ObsState::open(Some(persist_dir), 1024)
                .await
                .expect("reopen");
            assert_eq!(
                reopened.trace_store().trace_count(),
                0,
                "no persist_traces() call ever ran, so restart must start empty"
            );
        }

        // (2) Ingest again and THIS time call `persist_traces` before "restarting".
        {
            let obs = ObsState::open(Some(persist_dir), 1024).await.expect("open");
            obs.trace_store().add_span(span.clone());
            obs.persist_traces().await.expect("persist_traces");
        }
        {
            let reopened = ObsState::open(Some(persist_dir), 1024)
                .await
                .expect("reopen");
            assert_eq!(
                reopened.trace_store().trace_count(),
                1,
                "persist_traces() ran, so the restart must recover the trace"
            );
            let recovered = reopened.trace_store().assemble("t1").expect("trace t1");
            assert_eq!(recovered.span_count, 1);
            assert_eq!(recovered.services, vec!["checkout".to_string()]);
        }
    }

    /// BUG-210 -- end-to-end restart proof for the segment-manifest prune index.
    /// Ingest past `flush_threshold` (forcing a real segment flush) against a real
    /// persist dir, confirm `segments_for` is non-empty, "restart" (drop + reopen
    /// on the SAME persist dir), and assert the manifest -- and the segment's rows
    /// via `read_segment` -- are STILL reachable. Pre-fix this assertion fails (0
    /// segments after reopen, per the ledger's root-cause finding); post-fix it
    /// must pass.
    #[tokio::test(flavor = "current_thread")]
    async fn bug_210_segment_manifests_survive_an_obsstate_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let persist_dir = dir.path().to_str().expect("utf8 temp path");

        {
            let obs = ObsState::open(Some(persist_dir), 2).await.expect("open");
            let recs = vec![
                LogRecord {
                    ts: 10,
                    stream: "s".into(),
                    severity: "INFO".into(),
                    body: "one".into(),
                    attrs: BTreeMap::new(),
                },
                LogRecord {
                    ts: 20,
                    stream: "s".into(),
                    severity: "WARN".into(),
                    body: "two".into(),
                    attrs: BTreeMap::new(),
                },
            ];
            let out = obs.ingest(recs).expect("ingest");
            assert_eq!(out.segments_flushed, 1, "threshold=2 rolls one segment");
            assert_eq!(
                obs.segments_for("s").len(),
                1,
                "durably indexed before restart"
            );
        }

        let reopened = ObsState::open(Some(persist_dir), 2).await.expect("reopen");
        let segs = reopened.segments_for("s");
        assert_eq!(
            segs.len(),
            1,
            "the segment manifest must survive a restart, not silently vanish"
        );
        assert_eq!(segs[0].row_count, 2);
        assert_eq!(segs[0].min_ts, 10);
        assert_eq!(segs[0].max_ts, 20);

        // The bytes themselves were always durable in the blob CAS; the fix is
        // that the manifest recovered above makes them REACHABLE again.
        let rows = reopened.read_segment(&segs[0]).expect("read_segment");
        assert_eq!(rows.len(), 2);
        let bodies: Vec<&str> = rows.iter().map(|r| r.body.as_str()).collect();
        assert!(bodies.contains(&"one") && bodies.contains(&"two"));
    }

    #[tokio::test]
    async fn http_otlp_ingest_end_to_end() {
        let obs = Arc::new(ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        let body = r#"{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"4200","severityText":"INFO","body":{"stringValue":"unique_marker_token served"}}]}]}]}"#;
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST /v1/logs HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8_lossy(&resp);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("partialSuccess"));

        // The record landed: tsdb series + searchable text.
        let points = obs.series_range("api", 0, 10_000).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 4200);
        let hits = obs.search("api", "unique_marker_token", 10);
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn http_es_bulk_ingest_end_to_end() {
        let obs = Arc::new(ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        let body = "{\"index\":{\"_index\":\"esstream\"}}\n\
                    {\"@timestamp\":7,\"level\":\"ERROR\",\"message\":\"es_bulk_marker boom\"}\n";
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST /_bulk HTTP/1.1\r\nHost: x\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        let text = String::from_utf8_lossy(&resp);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("\"errors\":false"));

        let hits = obs.search("esstream", "es_bulk_marker", 10);
        assert_eq!(hits.len(), 1);
        let points = obs.series_range("esstream", 0, 100_000_000).unwrap();
        assert_eq!(points.len(), 1);
    }

    /// Send a POST to `addr` and return the response text.
    #[cfg(test)]
    async fn post(addr: std::net::SocketAddr, path: &str, body: &str) -> String {
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        sock.read_to_end(&mut resp).await.unwrap();
        String::from_utf8_lossy(&resp).to_string()
    }

    #[tokio::test]
    async fn http_preflight_rejects_ambiguous_or_oversized_framing() {
        let obs = Arc::new(ObsState::in_memory(1024).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener, obs).await });

        async fn raw(addr: std::net::SocketAddr, request: &str) -> String {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            sock.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            sock.read_to_end(&mut response).await.unwrap();
            String::from_utf8_lossy(&response).to_string()
        }

        let duplicate = raw(
            addr,
            "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(duplicate.starts_with("HTTP/1.1 400 Bad Request"));

        let chunked = raw(
            addr,
            "POST /v1/logs HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        )
        .await;
        assert!(chunked.starts_with("HTTP/1.1 400 Bad Request"));

        let oversized = raw(
            addr,
            &format!(
                "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
                MAX_HTTP_BODY_BYTES + 1
            ),
        )
        .await;
        assert!(oversized.starts_with("HTTP/1.1 400 Bad Request"));
    }

    /// The `_search` HTTP surface: a structured search returns ES-shaped hits, and a
    /// `{"sql":…}` body aggregates over the log segments (CONCEPT:EG-KG.query.concept-4).
    #[tokio::test]
    async fn http_search_end_to_end() {
        // flush_threshold=2 → two records roll a segment, one stays hot.
        let obs = Arc::new(ObsState::in_memory(2).unwrap());
        obs.ingest(vec![
            LogRecord {
                ts: 100,
                stream: "web".into(),
                severity: "ERROR".into(),
                body: "search_marker cold".into(),
                attrs: BTreeMap::new(),
            },
            LogRecord {
                ts: 200,
                stream: "web".into(),
                severity: "INFO".into(),
                body: "quiet cold".into(),
                attrs: BTreeMap::new(),
            },
        ])
        .unwrap();
        obs.ingest(vec![LogRecord {
            ts: 300,
            stream: "web".into(),
            severity: "WARN".into(),
            body: "search_marker hot".into(),
            attrs: BTreeMap::new(),
        }])
        .unwrap();
        assert_eq!(obs.segments_for("web").len(), 1);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = obs.clone();
        tokio::spawn(async move { serve(listener, served).await });

        // Structured search: time window spanning both tiers, path-derived stream.
        let text = post(
            addr,
            "/api/default/web/_search",
            r#"{"start_time":0,"end_time":1000}"#,
        )
        .await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        let json_body = text.split("\r\n\r\n").nth(1).unwrap();
        let v: serde_json::Value = serde_json::from_str(json_body).unwrap();
        assert_eq!(v["hits"]["total"]["value"], 3, "both tiers: {json_body}");

        // Full-text term via the `query` string → only the two "search_marker" records.
        let text = post(
            addr,
            "/api/_search",
            r#"{"stream":"web","query":"search_marker"}"#,
        )
        .await;
        let v: serde_json::Value =
            serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(v["hits"]["total"]["value"], 2);

        // SQL mode: count by severity over the `logs` table (segments + hot).
        let text = post(
            addr,
            "/api/_search",
            r#"{"sql":"SELECT severity, count(*) AS n FROM logs GROUP BY severity ORDER BY severity"}"#,
        )
        .await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        let v: serde_json::Value =
            serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(v["total"], 3, "ERROR/INFO/WARN buckets: {text}");
        assert_eq!(v["hits"][0]["severity"], "ERROR");
        assert_eq!(v["hits"][0]["n"], 1);
    }

    // ── GOC-62 D3(b): failing-first proof of BUG-037 ────────────────────────
    //
    // BUG-037 (P0, DEC-015 §1 finding #2 / GOC-62-keycloak-auth-standard.md §4):
    // `is_observability_read_carrier` (above) matches ONLY GET/query-shaped
    // paths, so `observability_read_denied`'s check in `handle` is never even
    // REACHED for a POST/ingest path -- every ingest path (`/v1/logs`, `/_bulk`,
    // `/<stream>/_doc`, `/`, and (feature `otel-export`) `/api/v1/write`) skips
    // authorization entirely, in EVERY deployment configuration including
    // `serve_with_security` (the production listener). This is materially worse
    // than every other EG auxiliary surface (Iceberg REST, SPARQL reads,
    // federation, `/nl`), which at minimum deny unconditionally.
    //
    // This test proves it by calling the SAME `handle()` this module's own
    // `serve_inner`/`serve_with_security` route every real request through,
    // with a non-`None` `security_state` (i.e. "a secured deployment") and NO
    // credential at all on a POST /v1/logs ingest request. A read-shaped GET is
    // exercised alongside as the contrast case (correctly denied), so this test
    // fails for the RIGHT reason -- an ingest bypass -- not because the whole
    // security posture is broken.
    //
    // Red today, by design (GOC-62 D3(b)). Turns green only once ingest paths
    // are folded into the SAME carrier check `is_observability_read_carrier`
    // gates reads with -- see GOC-62-keycloak-auth-standard.md §4/§5's method-
    // sensitivity rule: "read-vs-mutation classification, never a route/path-
    // shape heuristic".

    /// A minimal, real `ServerState` -- `unauthenticated_carrier_denied`/
    /// `observability_read_denied` only branch on `security_state.is_some()`
    /// (verified by direct reading, `access.rs::unauthenticated_carrier_denied`:
    /// `carrier.is_none()` unconditionally, since no carrier mechanism exists for
    /// this surface today), so ANY validly-constructed `ServerState` proves the
    /// "secured deployment" precondition. Use the public canonical constructor
    /// so feature-gated fields stay in sync with the rest of the engine fixtures.
    fn goc62_bug037_security_state(
    ) -> std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>> {
        let mut state = crate::server::ServerState::new_for_test(
            "goc62-bug037-test-secret", // nosec B105 - test only  // sanitizer:ignore — synthetic in-process test fixture, never a live credential
            crate::isolation::IsolationLayer::new(),
        );
        // Preserve this fixture's prior omission of the optional CDC hub.
        #[cfg(feature = "streaming")]
        {
            state.cdc = None;
        }
        std::sync::Arc::new(tokio::sync::RwLock::new(state))
    }

    #[tokio::test]
    async fn bug_037_obs_ingest_post_bypasses_the_deny_gate() {
        let obs = std::sync::Arc::new(ObsState::in_memory(1024).unwrap());
        let security_state = goc62_bug037_security_state();

        // Contrast case: a read-shaped GET IS correctly denied under a secured
        // deployment -- proves the harness is exercising the real gate, not a
        // vacuous stub.
        let read_req = HttpRequest {
            method: "GET".to_string(),
            target: "/api/v1/query?query=up".to_string(),
            content_type: "text/plain".to_string(),
            body: String::new(),
            #[cfg(feature = "otel-export")]
            body_bytes: Vec::new(),
        };
        let (read_status, _, _) = handle(&obs, Some(&security_state), read_req).await;
        assert_eq!(
            read_status, "403 Forbidden",
            "precondition: reads under a secured deployment must still deny with \
             no carrier -- if this fails, the harness itself is broken, not just \
             the ingest gate"
        );

        // The actual BUG-037 proof: an ingest POST, same secured deployment, same
        // zero credential -- must ALSO deny, but does not.
        let ingest_body = r#"{"resourceLogs":[]}"#;
        let ingest_req = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/logs".to_string(),
            content_type: "application/json".to_string(),
            body: ingest_body.to_string(),
            #[cfg(feature = "otel-export")]
            body_bytes: ingest_body.as_bytes().to_vec(),
        };
        let (ingest_status, _, ingest_body_out) =
            handle(&obs, Some(&security_state), ingest_req).await;

        assert_eq!(
            ingest_status, "403 Forbidden",
            "BUG-037 FAIL-OPEN: POST /v1/logs (an ingest/mutation path) was \
             accepted with a secured deployment and ZERO credential -- \
             is_observability_read_carrier only matches GET/query-shaped paths, \
             so observability_read_denied's check in `handle` is never reached \
             for this request. Got status {ingest_status:?}, body: \
             {ingest_body_out:?}"
        );
    }
}
