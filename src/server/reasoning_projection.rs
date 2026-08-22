//! Durable, incremental epistemic/causal/materialization projection worker.
//!
//! The authoritative MutationBatch outbox is the scheduling input; its digest-only
//! lease resolves the canonical operation/state record from the same authority.
//! Each lease is applied idempotently, the compact projection is atomically
//! snapshotted, and only then is it acknowledged with its durable cursor.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eg_epistemic::{
    IncrementalDelta, IncrementalReasoningIndex, ProjectionPosition, ReasoningProjectionWakeup,
};
use eg_types::mutation_batch::{MutationBatchStatus, MutationOutboxLease, MutationVersionScope};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::server::state::ServerState;

const CONSUMER: &str = "reasoning-projection-v1";
const PROJECTION: &str = "epistemic-causal-materialized-v1";
const MAX_PROJECTION_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PROJECTION_SNAPSHOT_ITEMS: usize = 4_000_000;
const MAX_PROJECTION_SNAPSHOT_DEPTH: usize = 64;
const PROJECTION_SNAPSHOT_SIZE_LIMIT_ERROR: &str =
    "reasoning projection snapshot exceeds its size limit";

/// Constant-memory output guard for durable projection serialization. The inner
/// writer never receives a byte slice that would move the image past `max_bytes`.
struct BoundedSnapshotWriter<'a, W: Write + ?Sized> {
    inner: &'a mut W,
    written: u64,
    max_bytes: u64,
    limit_exceeded: bool,
}

impl<'a, W: Write + ?Sized> BoundedSnapshotWriter<'a, W> {
    fn new(inner: &'a mut W, max_bytes: u64) -> Self {
        Self {
            inner,
            written: 0,
            max_bytes,
            limit_exceeded: false,
        }
    }

    fn admit(&mut self, len: usize) -> std::io::Result<u64> {
        let len = u64::try_from(len).map_err(|_| {
            self.limit_exceeded = true;
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                PROJECTION_SNAPSHOT_SIZE_LIMIT_ERROR,
            )
        })?;
        if len > self.max_bytes.saturating_sub(self.written) {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(PROJECTION_SNAPSHOT_SIZE_LIMIT_ERROR));
        }
        Ok(len)
    }
}

impl<W: Write + ?Sized> Write for BoundedSnapshotWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let admitted = self.admit(buf.len())?;
        let written = self.inner.write(buf)?;
        if written > buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot writer reported an invalid byte count",
            ));
        }
        let written = u64::try_from(written).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot writer byte count overflowed",
            )
        })?;
        debug_assert!(written <= admitted);
        self.written += written;
        Ok(written as usize)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let admitted = self.admit(buf.len())?;
        self.inner.write_all(buf)?;
        self.written += admitted;
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Serializes local projection image replacement. Consensus/outbox ordering is the
/// authority; this lock only prevents two same-process readers from racing the
/// atomic file replacement and is never itself consulted as reasoning state.
fn projection_write_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Start the singleton projection loop after graph recovery.  A backend without
/// durable outbox leases is left untouched; authoritative redb implements them.
pub fn spawn(state: Arc<RwLock<ServerState>>) {
    tokio::spawn(async move {
        loop {
            let (persistence, persist_dir, graphs) = {
                let state = state.read().await;
                (
                    state.persistence.clone(),
                    state.persist_dir.clone(),
                    state
                        .registry
                        .all_entries()
                        .into_iter()
                        .map(|entry| (entry.name.clone(), entry.core.clone()))
                        .collect::<Vec<_>>(),
                )
            };
            let Some(persistence) = persistence else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            let mut progressed = false;
            for (graph, core) in graphs {
                let graph_fname = crate::persist::sanitize(&graph);
                {
                    let _guard = match projection_write_lock().lock() {
                        Ok(guard) => guard,
                        Err(_) => continue,
                    };
                    let index = match load_index(persist_dir.as_deref(), &graph_fname) {
                        Ok(Some(index)) => index,
                        Ok(None) => {
                            let index = IncrementalReasoningIndex::from_graph_view(
                                &core.analysis_snapshot(),
                            );
                            if persist_index(persist_dir.as_deref(), &graph_fname, &index).is_err()
                            {
                                continue;
                            }
                            index
                        }
                        // A present-but-invalid authority is never replaced from RAM.
                        // Operator repair is required; silently bootstrapping would
                        // turn corruption into an apparently valid empty answer.
                        Err(_) => continue,
                    };
                    drop(index);
                }
                let leases = match persistence
                    .claim_mutation_outbox(&graph_fname, CONSUMER, now_ms(), 30_000, 64)
                    .await
                {
                    Ok(leases) => leases,
                    Err(_) => continue,
                };
                for lease in leases {
                    // Reading the cursor is an explicit restart/reconciliation
                    // boundary. A sidecar may be one event AHEAD after a crash
                    // between snapshot and ack; exact-position apply is idempotent.
                    if persistence
                        .read_mutation_projection_cursor(
                            &graph_fname,
                            PROJECTION,
                            &lease.record.tenant,
                        )
                        .await
                        .is_err()
                    {
                        break;
                    }
                    // Projection wake-ups contain only domain-separated identities
                    // and closed categorical tags. Bind them to the authoritative
                    // operation digest before allowing the side index to advance.
                    let wakeup = if lease.record.intent.topic == "engine.projection.rebuild" {
                        let record = match persistence
                            .read_mutation_batch(&graph_fname, &lease.record.batch_id)
                            .await
                        {
                            Ok(Some(record)) if record.status == MutationBatchStatus::Committed => {
                                record
                            }
                            _ => break,
                        };
                        let wakeup: ReasoningProjectionWakeup =
                            match eg_types::msgpack::decode_bounded(
                                &lease.record.intent.payload,
                                eg_types::msgpack::MsgpackLimits::new(
                                    64 * 1024 * 1024,
                                    1_000_000,
                                    64,
                                ),
                            ) {
                                Ok(value) => value,
                                Err(_) => break,
                            };
                        if wakeup.validate().is_err()
                            || wakeup.operation_count as usize != record.batch.operations.len()
                        {
                            break;
                        }
                        let operations = match rmp_serde::to_vec_named(&record.batch.operations) {
                            Ok(value) => value,
                            Err(_) => break,
                        };
                        if hex::encode(Sha256::digest(operations)) != wakeup.operations_sha256 {
                            break;
                        }
                        Some(wakeup)
                    } else {
                        None
                    };
                    let (newly_stale, stale_count) = {
                        let _guard = match projection_write_lock().lock() {
                            Ok(guard) => guard,
                            Err(_) => break,
                        };
                        let mut index = match load_index(persist_dir.as_deref(), &graph_fname) {
                            Ok(Some(index)) => index,
                            _ => break,
                        };
                        let delta = match apply_lease(&mut index, &core, &lease, wakeup.as_ref()) {
                            Ok(delta) => delta,
                            Err(_) => break,
                        };
                        if persist_index(persist_dir.as_deref(), &graph_fname, &index).is_err() {
                            break;
                        }
                        (
                            delta.newly_stale.len(),
                            index.stale_materializations().len(),
                        )
                    };
                    crate::metrics::epistemic_materializations_staled(newly_stale as u64);
                    crate::metrics::set_epistemic_materializations_stale(stale_count as i64);
                    if persistence
                        .ack_mutation_outbox(&graph_fname, &lease, PROJECTION, now_ms())
                        .await
                        .is_err()
                    {
                        // Do not apply later leased rows after an ordering gap. The
                        // sidecar is at most one event ahead; exact-position replay
                        // is harmless after this lease expires.
                        break;
                    }
                    progressed = true;
                }
            }
            if !progressed {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    });
}

fn apply_lease(
    index: &mut IncrementalReasoningIndex,
    core: &eg_core::graph::GraphCore,
    lease: &MutationOutboxLease,
    wakeup: Option<&ReasoningProjectionWakeup>,
) -> Result<IncrementalDelta, String> {
    if lease.record.version_scope != MutationVersionScope::Graph {
        return Err("reasoning projection requires a graph-authoritative event".to_string());
    }
    let position = ProjectionPosition {
        batch_id: lease.record.batch_id.clone(),
        ordinal: lease.record.ordinal,
        source_graph_version: lease.record.source_graph_version,
    };
    if let Some(wakeup) = wakeup {
        index.apply_wakeup(position, wakeup, &core.analysis_snapshot())
    } else {
        index.apply_batch(position, &[])
    }
}

fn snapshot_path(persist_dir: Option<&str>, graph_fname: &str) -> Option<PathBuf> {
    let root = persist_dir.filter(|value| !value.is_empty())?;
    let digest = hex::encode(Sha256::digest(graph_fname.as_bytes()));
    Some(
        Path::new(root)
            .join("reasoning-projections")
            .join(format!("{digest}.msgpack")),
    )
}

fn load_index(
    persist_dir: Option<&str>,
    graph_fname: &str,
) -> Result<Option<IncrementalReasoningIndex>, String> {
    load_index_with_limits(
        persist_dir,
        graph_fname,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_PROJECTION_SNAPSHOT_BYTES as usize,
            MAX_PROJECTION_SNAPSHOT_ITEMS,
            MAX_PROJECTION_SNAPSHOT_DEPTH,
        ),
    )
}

fn load_index_with_limits(
    persist_dir: Option<&str>,
    graph_fname: &str,
    limits: eg_types::msgpack::MsgpackLimits,
) -> Result<Option<IncrementalReasoningIndex>, String> {
    let Some(path) = snapshot_path(persist_dir, graph_fname) else {
        return Ok(None);
    };
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("reasoning projection snapshot is unavailable".to_string()),
    };
    if file
        .metadata()
        .map_err(|_| "reasoning projection snapshot is unavailable".to_string())?
        .len()
        > limits.max_bytes as u64
    {
        return Err("reasoning projection snapshot exceeds its size limit".to_string());
    }
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take((limits.max_bytes as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| "reasoning projection snapshot is unavailable".to_string())?;
    if bytes.len() > limits.max_bytes {
        return Err("reasoning projection snapshot exceeds its size limit".to_string());
    }
    let index: IncrementalReasoningIndex = eg_types::msgpack::decode_bounded(&bytes, limits)
        .map_err(|_| "reasoning projection snapshot is invalid".to_string())?;
    index.validate()?;
    Ok(Some(index))
}

/// Read the durable reasoning authority for one graph. Missing and corrupt images
/// are distinct hard errors; served queries never manufacture an empty projection.
pub fn read_index(
    persist_dir: Option<&str>,
    graph_name: &str,
) -> Result<IncrementalReasoningIndex, String> {
    let graph_fname = crate::persist::sanitize(graph_name);
    load_index(persist_dir, &graph_fname)?
        .ok_or_else(|| "reasoning projection is not initialized".to_string())
}

pub fn materialization_status(
    persist_dir: Option<&str>,
    graph_name: &str,
    node_id: &str,
) -> Result<(Option<eg_epistemic::ProjectedMaterializationStatus>, u64), String> {
    let index = read_index(persist_dir, graph_name)?;
    let source_graph_version = index
        .position
        .as_ref()
        .map_or(0, |position| position.source_graph_version);
    Ok((index.status_of(node_id), source_graph_version))
}

pub fn stale_materializations(
    persist_dir: Option<&str>,
    graph_name: &str,
) -> Result<(Vec<String>, u64), String> {
    let index = read_index(persist_dir, graph_name)?;
    let source_graph_version = index
        .position
        .as_ref()
        .map_or(0, |position| position.source_graph_version);
    Ok((
        index.stale_materializations().iter().cloned().collect(),
        source_graph_version,
    ))
}

/// Fenced recompute/writeback against the exact durable projection watermark.
/// Provenance comes only from the authoritative graph post-image, never request
/// fields. The updated projection is fsync'd before its result is returned.
pub fn recompute_materialization(
    persist_dir: Option<&str>,
    graph_name: &str,
    view: &eg_core::graph::GraphView,
    authoritative_graph_version: u64,
    node_id: &str,
    expected_source_graph_version: u64,
) -> Result<(eg_epistemic::ProjectedMaterialization, u64), String> {
    if authoritative_graph_version != expected_source_graph_version {
        return Err("STALE_RECOMPUTE_FENCE: authoritative graph version changed".to_string());
    }
    let _guard = projection_write_lock()
        .lock()
        .map_err(|_| "reasoning projection write lock is unavailable".to_string())?;
    let graph_fname = crate::persist::sanitize(graph_name);
    let mut index = load_index(persist_dir, &graph_fname)?
        .ok_or_else(|| "reasoning projection is not initialized".to_string())?;
    let fence_epoch = index.claim_recompute(node_id, expected_source_graph_version)?;
    let provenance = view.node_properties.get(node_id).map(|properties| {
        let outgoing = view
            .edge_properties
            .iter()
            .filter_map(|((source, target), versions)| {
                (source == node_id)
                    .then(|| {
                        versions
                            .last()
                            .map(|properties| (target.clone(), properties.as_slice()))
                    })
                    .flatten()
            });
        eg_epistemic::resolve_provenance(Some(properties.as_slice()), outgoing)
    });
    let materialization = index.complete_recompute(
        node_id,
        expected_source_graph_version,
        fence_epoch,
        provenance,
    )?;
    persist_index(persist_dir, &graph_fname, &index)?;
    crate::metrics::set_epistemic_materializations_stale(
        index.stale_materializations().len() as i64
    );
    Ok((materialization, fence_epoch))
}

fn persist_index(
    persist_dir: Option<&str>,
    graph_fname: &str,
    index: &IncrementalReasoningIndex,
) -> Result<(), String> {
    persist_index_with_limit(
        persist_dir,
        graph_fname,
        index,
        MAX_PROJECTION_SNAPSHOT_BYTES,
    )
}

fn encode_index_bounded<W: Write + ?Sized>(
    writer: &mut W,
    index: &IncrementalReasoningIndex,
    max_bytes: u64,
) -> Result<u64, String> {
    let mut writer = BoundedSnapshotWriter::new(writer, max_bytes);
    let encoded = rmp_serde::encode::write_named(&mut writer, index);
    if writer.limit_exceeded {
        return Err(PROJECTION_SNAPSHOT_SIZE_LIMIT_ERROR.to_string());
    }
    encoded.map_err(|error| error.to_string())?;
    writer.flush().map_err(|error| error.to_string())?;
    Ok(writer.written)
}

fn persist_index_with_limit(
    persist_dir: Option<&str>,
    graph_fname: &str,
    index: &IncrementalReasoningIndex,
    max_bytes: u64,
) -> Result<(), String> {
    index.validate()?;
    let Some(path) = snapshot_path(persist_dir, graph_fname) else {
        return Err(
            "reasoning projection requires a configured durable persistence directory".to_string(),
        );
    };
    let parent = path
        .parent()
        .ok_or_else(|| "reasoning projection path has no parent".to_string())?
        .to_path_buf();
    std::fs::create_dir_all(&parent).map_err(|error| error.to_string())?;
    let temporary = path.with_extension("msgpack.tmp");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    if let Err(error) = encode_index_bounded(&mut file, index, max_bytes) {
        drop(file);
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = file.sync_all() {
        drop(file);
        let _ = std::fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    drop(file);
    if let Err(error) = replace_snapshot(&temporary, &path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    // Persist the directory entry before acknowledging the outbox cursor. Some
    // platforms do not permit opening a directory as a file; data-file fsync is
    // still mandatory there and the directory sync is best effort.
    if let Ok(directory) = std::fs::File::open(&parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_snapshot(temporary: &Path, path: &Path) -> std::io::Result<()> {
    std::fs::rename(temporary, path)
}

// Win32 atomic-replace FFI. This is the C-FFI exception the repo's
// `#![deny(unsafe_code)]` (src/lib.rs) permits only via a scoped `#[allow]` plus a
// soundness note, exactly as `server/dds.rs` and `eg-compute/src/ast/parser.rs` do;
// it is gated to `#[cfg(windows)]`, not a blanket crate-level allow.
//
// Soundness: both calls take pointers into `Vec<u16>` buffers (`target`,
// `replacement`) that are live for the whole call and NUL-terminated by `wide()`,
// which also rejects interior NULs so the OS cannot read past the terminator. The
// remaining arguments are null, which both APIs document as valid (no backup file,
// no reserved data). Neither call retains a pointer after returning, and both
// report failure through the return value, read via `last_os_error()` before any
// further FFI call can overwrite it. No Rust aliasing invariant is involved.
//
// The `unsafe` is unavoidable rather than a convenience: `std::fs::rename` maps to
// `MoveFileExW` WITHOUT `MOVEFILE_WRITE_THROUGH`, so it cannot provide the durable
// same-volume swap this snapshot publication depends on.
#[cfg(windows)]
#[allow(unsafe_code)]
fn replace_snapshot(temporary: &Path, path: &Path) -> std::io::Result<()> {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::ptr;

    #[link(name = "Kernel32")]
    extern "system" {
        fn ReplaceFileW(
            replaced_file_name: *const u16,
            replacement_file_name: *const u16,
            backup_file_name: *const u16,
            replace_flags: u32,
            exclude: *mut c_void,
            reserved: *mut c_void,
        ) -> i32;
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    const REPLACEFILE_WRITE_THROUGH: u32 = 0x0000_0001;
    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;

    fn wide(path: &Path) -> std::io::Result<Vec<u16>> {
        let mut value = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if value.contains(&0) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "snapshot path contains NUL",
            ));
        }
        value.push(0);
        Ok(value)
    }

    let target = wide(path)?;
    let replacement = wide(temporary)?;
    if path.exists() {
        // ReplaceFileW swaps same-volume files without a delete-then-rename gap.
        // Failure leaves the original target intact, which is the rollback contract.
        let replaced = unsafe {
            ReplaceFileW(
                target.as_ptr(),
                replacement.as_ptr(),
                ptr::null(),
                REPLACEFILE_WRITE_THROUGH,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if replaced != 0 {
            return Ok(());
        }
        return Err(std::io::Error::last_os_error());
    }

    // Initial publication has no target to replace. MOVEFILE_WRITE_THROUGH makes
    // the same-directory rename durable; REPLACE_EXISTING closes the existence race
    // without creating a delete gap.
    let moved = unsafe {
        MoveFileExW(
            replacement.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use eg_epistemic::ProjectedMaterializationStatus;
    use eg_types::protocol::Method;

    use super::*;

    fn test_root() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "eg-reasoning-projection-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn derived_properties() -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({
            "invalidation_deps": ["base"],
            "generating_activity": "model",
        }))
        .unwrap()
    }

    fn stale_index() -> IncrementalReasoningIndex {
        let mut index = IncrementalReasoningIndex::default();
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "create".to_string(),
                    ordinal: 0,
                    source_graph_version: 1,
                },
                &[Method::AddNode {
                    node_id: "derived".to_string(),
                    properties_msgpack: derived_properties(),
                }],
            )
            .unwrap();
        index
            .apply_batch(
                ProjectionPosition {
                    batch_id: "invalidate".to_string(),
                    ordinal: 0,
                    source_graph_version: 2,
                },
                &[Method::CompareAndSetNodeFields {
                    node_id: "base".to_string(),
                    conditions_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
                    updates_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
                }],
            )
            .unwrap();
        index
    }

    #[test]
    fn durable_reader_fails_closed_for_missing_and_corrupt_projection() {
        let root = test_root();
        let root_str = root.to_string_lossy();
        assert!(read_index(Some(&root_str), "graph")
            .unwrap_err()
            .contains("not initialized"));

        let path = snapshot_path(Some(&root_str), "graph").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"not-a-projection").unwrap();
        assert!(read_index(Some(&root_str), "graph")
            .unwrap_err()
            .contains("invalid"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_snapshot_replaces_an_existing_image() {
        let root = test_root();
        let root_str = root.to_string_lossy();
        let initial = stale_index();
        persist_index(Some(&root_str), "graph", &initial).unwrap();

        let mut replacement = stale_index();
        replacement
            .apply_batch(
                ProjectionPosition {
                    batch_id: "replacement".to_string(),
                    ordinal: 0,
                    source_graph_version: 3,
                },
                &[],
            )
            .unwrap();
        persist_index(Some(&root_str), "graph", &replacement).unwrap();

        let restored = read_index(Some(&root_str), "graph").unwrap();
        assert_eq!(restored, replacement);
        assert_eq!(
            restored
                .position
                .as_ref()
                .map(|position| position.source_graph_version),
            Some(3)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn bounded_snapshot_encoding_never_crosses_cap_or_replaces_prior_image() {
        #[derive(Default)]
        struct CountingWriter {
            written: u64,
        }

        impl std::io::Write for CountingWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.written += buf.len() as u64;
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        const TEST_CAP: u64 = 32;
        let initial = stale_index();
        let mut sink = CountingWriter::default();
        let error = encode_index_bounded(&mut sink, &initial, TEST_CAP).unwrap_err();
        assert!(error.contains("size limit"));
        assert!(sink.written <= TEST_CAP);

        let root = test_root();
        let root_str = root.to_string_lossy();
        persist_index(Some(&root_str), "graph", &initial).unwrap();

        let mut replacement = stale_index();
        replacement
            .apply_batch(
                ProjectionPosition {
                    batch_id: "replacement".to_string(),
                    ordinal: 0,
                    source_graph_version: 3,
                },
                &[],
            )
            .unwrap();
        let error =
            persist_index_with_limit(Some(&root_str), "graph", &replacement, TEST_CAP).unwrap_err();
        assert!(error.contains("size limit"));
        assert_eq!(read_index(Some(&root_str), "graph").unwrap(), initial);
        assert!(!snapshot_path(Some(&root_str), "graph")
            .unwrap()
            .with_extension("msgpack.tmp")
            .exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_reader_rejects_an_oversized_file_before_reading_it() {
        let root = test_root();
        let root_str = root.to_string_lossy();
        let path = snapshot_path(Some(&root_str), "graph").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, [0_u8; 5]).unwrap();

        let error = load_index_with_limits(
            Some(&root_str),
            "graph",
            eg_types::msgpack::MsgpackLimits::new(4, 4, 4),
        )
        .unwrap_err();
        assert!(error.contains("size limit"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_reader_rejects_a_declared_allocation_bomb() {
        let root = test_root();
        let root_str = root.to_string_lossy();
        let path = snapshot_path(Some(&root_str), "graph").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // MessagePack array32 claiming 2^32-1 items with no body. Structural
        // preflight rejects the declaration before serde can reserve a vector.
        std::fs::write(path, [0xdd, 0xff, 0xff, 0xff, 0xff]).unwrap();

        assert!(read_index(Some(&root_str), "graph")
            .unwrap_err()
            .contains("invalid"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn recompute_is_version_fenced_and_durable() {
        let root = test_root();
        let root_str = root.to_string_lossy();
        persist_index(Some(&root_str), "graph", &stale_index()).unwrap();

        let core = eg_core::graph::GraphCore::new();
        core.add_node("derived".to_string(), derived_properties());
        let view = core.analysis_snapshot();

        let mismatch = recompute_materialization(Some(&root_str), "graph", &view, 3, "derived", 2)
            .unwrap_err();
        assert!(mismatch.contains("authoritative graph version changed"));

        let (materialization, fence_epoch) =
            recompute_materialization(Some(&root_str), "graph", &view, 2, "derived", 2).unwrap();
        assert_eq!(
            materialization.status,
            ProjectedMaterializationStatus::Fresh
        );
        assert_eq!(fence_epoch, 1);
        assert_eq!(
            read_index(Some(&root_str), "graph")
                .unwrap()
                .status_of("derived"),
            Some(ProjectedMaterializationStatus::Fresh)
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
