//! Durable, incremental epistemic/causal/materialization projection worker.
//!
//! The authoritative MutationBatch outbox is the scheduling input; its digest-only
//! lease resolves the canonical operation/state record from the same authority.
//! Each lease is applied idempotently, the compact projection is atomically
//! snapshotted, and only then is it acknowledged with its durable cursor.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eg_epistemic::{
    IncrementalDelta, IncrementalReasoningIndex, ProjectionPosition, ReasoningProjectionWakeup,
};
use eg_types::mutation_batch::{CommittedVersion, MutationBatchStatus, MutationOutboxLease};
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
fn projection_write_lock() -> &'static Arc<tokio::sync::Mutex<()>> {
    static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
}

/// Run one complete projection filesystem transaction off the Tokio executor.
///
/// The closure owns every path, snapshot, and graph handle. After task admission,
/// cancellation of the awaiting request cannot drop
/// the process-local serialization guard or a temporary snapshot halfway through
/// publication: the blocking job runs its cleanup to completion before releasing
/// the guard.
async fn run_projection_job<T, F>(job: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    // Async admission keeps contenders off the bounded blocking pool. The
    // owned guard is then transferred into the accepted job, so cancellation
    // of this waiter cannot release it before the filesystem transaction ends.
    let guard = projection_write_lock().clone().lock_owned().await;
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        job()
    })
    .await
    .map_err(|_| "reasoning projection persistence task failed".to_string())?
}

/// Start the singleton projection loop after graph recovery.  A backend without
/// durable outbox leases is left untouched; authoritative redb implements them.
pub fn spawn(state: Arc<RwLock<ServerState>>) {
    tokio::spawn(projection_loop(state));
}

struct ProjectionContext {
    persistence: Arc<dyn crate::server::persistence::PersistenceBackend>,
    persist_dir: Option<String>,
    graphs: Vec<(String, Arc<eg_core::graph::GraphCore>)>,
}

async fn projection_loop(state: Arc<RwLock<ServerState>>) {
    loop {
        let Some(context) = projection_context(&state).await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let progressed = process_graphs(&context).await;
        if !progressed {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

async fn projection_context(state: &Arc<RwLock<ServerState>>) -> Option<ProjectionContext> {
    let state = state.read().await;
    let persistence = state.persistence.clone();
    let persist_dir = state.persist_dir.clone();
    let graphs = state
        .registry
        .all_entries()
        .into_iter()
        .map(|entry| (entry.name.clone(), entry.core.clone()))
        .collect();
    let persistence = persistence?;
    Some(ProjectionContext {
        persistence,
        persist_dir,
        graphs,
    })
}

async fn process_graphs(context: &ProjectionContext) -> bool {
    let mut progressed = false;
    for (graph, core) in &context.graphs {
        if process_graph(context, graph, core).await {
            progressed = true;
        }
    }
    progressed
}

async fn process_graph(
    context: &ProjectionContext,
    graph: &str,
    core: &Arc<eg_core::graph::GraphCore>,
) -> bool {
    let graph_fname = crate::persist::sanitize(graph);
    if !initialize_index(
        context.persist_dir.clone(),
        graph_fname.clone(),
        core.clone(),
    )
    .await
    {
        return false;
    }
    let leases = match context
        .persistence
        .claim_mutation_outbox(&graph_fname, CONSUMER, current_time_ms(), 30_000, 64)
        .await
    {
        Ok(leases) => leases,
        Err(_) => return false,
    };
    process_leases(
        &context.persistence,
        context.persist_dir.clone(),
        &graph_fname,
        core.clone(),
        leases,
    )
    .await
}

async fn initialize_index(
    persist_dir: Option<String>,
    graph_fname: String,
    core: Arc<eg_core::graph::GraphCore>,
) -> bool {
    match run_projection_job(move || {
        match load_index(persist_dir.as_deref(), &graph_fname) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => {
                let index = IncrementalReasoningIndex::from_graph_view(&core.analysis_snapshot());
                persist_index(persist_dir.as_deref(), &graph_fname, &index)?;
                Ok(true)
            }
            // A present-but-invalid authority is never replaced from RAM.
            // Operator repair is required; silently bootstrapping would turn
            // corruption into an apparently valid empty answer.
            Err(error) => Err(error),
        }
    })
    .await
    {
        Ok(initialized) => initialized,
        Err(_) => {
            tracing::warn!("reasoning projection initialization failed");
            false
        }
    }
}

async fn process_leases(
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    persist_dir: Option<String>,
    graph_fname: &str,
    core: Arc<eg_core::graph::GraphCore>,
    leases: Vec<MutationOutboxLease>,
) -> bool {
    let mut progressed = false;
    for lease in leases {
        if !process_lease(
            persistence,
            persist_dir.clone(),
            graph_fname,
            core.clone(),
            &lease,
        )
        .await
        {
            break;
        }
        progressed = true;
    }
    progressed
}

async fn process_lease(
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    persist_dir: Option<String>,
    graph_fname: &str,
    core: Arc<eg_core::graph::GraphCore>,
    lease: &MutationOutboxLease,
) -> bool {
    // Reading the cursor is an explicit restart/reconciliation boundary. A
    // sidecar may be one event AHEAD after a crash between snapshot and ack;
    // exact-position apply is idempotent.
    if persistence
        .read_mutation_projection_cursor(
            graph_fname,
            PROJECTION,
            lease.record.identity.tenant().as_str(),
        )
        .await
        .is_err()
    {
        return false;
    }
    // Projection wake-ups contain only domain-separated identities and closed
    // categorical tags. Bind them to the authoritative operation digest before
    // allowing the side index to advance.
    let wakeup = match resolve_projection_wakeup(persistence, graph_fname, lease).await {
        Ok(wakeup) => wakeup,
        Err(()) => return false,
    };
    let Some((newly_stale, stale_count)) = apply_lease_and_publish(
        persist_dir,
        graph_fname.to_string(),
        core,
        lease.clone(),
        wakeup,
    )
    .await
    else {
        return false;
    };
    crate::metrics::epistemic_materializations_staled(newly_stale as u64);
    crate::metrics::set_epistemic_materializations_stale(stale_count as i64);
    if persistence
        .ack_mutation_outbox(graph_fname, lease, PROJECTION, current_time_ms())
        .await
        .is_err()
    {
        // Do not apply later leased rows after an ordering gap. The sidecar is
        // at most one event ahead; exact-position replay is harmless after this
        // lease expires.
        return false;
    }
    true
}

async fn resolve_projection_wakeup(
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    graph_fname: &str,
    lease: &MutationOutboxLease,
) -> Result<Option<ReasoningProjectionWakeup>, ()> {
    if lease.record.intent.topic != "engine.projection.rebuild" {
        return Ok(None);
    }
    let record = persistence
        .read_mutation_batch(graph_fname, &lease.record.batch_id)
        .await
        .map_err(|_| ())?
        .filter(|record| record.status == MutationBatchStatus::Committed)
        .ok_or(())?;
    let wakeup: ReasoningProjectionWakeup = eg_types::msgpack::decode_bounded(
        &lease.record.intent.payload,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024 * 1024, 1_000_000, 64),
    )
    .map_err(|_| ())?;
    validate_projection_wakeup(&wakeup, &record.batch)?;
    Ok(Some(wakeup))
}

fn validate_projection_wakeup(
    wakeup: &ReasoningProjectionWakeup,
    batch: &eg_types::mutation_batch::MutationBatch,
) -> Result<(), ()> {
    if wakeup.validate().is_err() || wakeup.operation_count as usize != batch.operations.len() {
        return Err(());
    }
    let operations = rmp_serde::to_vec_named(&batch.operations).map_err(|_| ())?;
    if hex::encode(Sha256::digest(operations)) != wakeup.operations_sha256 {
        return Err(());
    }
    Ok(())
}

async fn apply_lease_and_publish(
    persist_dir: Option<String>,
    graph_fname: String,
    core: Arc<eg_core::graph::GraphCore>,
    lease: MutationOutboxLease,
    wakeup: Option<ReasoningProjectionWakeup>,
) -> Option<(usize, usize)> {
    match run_projection_job(move || {
        let mut index = load_index(persist_dir.as_deref(), &graph_fname)?
            .ok_or_else(|| "reasoning projection is not initialized".to_string())?;
        let delta = apply_lease(&mut index, &core, &lease, wakeup.as_ref())?;
        persist_index(persist_dir.as_deref(), &graph_fname, &index)?;
        Ok((
            delta.newly_stale.len(),
            index.stale_materializations().len(),
        ))
    })
    .await
    {
        Ok(result) => Some(result),
        Err(_) => {
            tracing::warn!("reasoning projection lease persistence failed");
            None
        }
    }
}

fn apply_lease(
    index: &mut IncrementalReasoningIndex,
    core: &eg_core::graph::GraphCore,
    lease: &MutationOutboxLease,
    wakeup: Option<&ReasoningProjectionWakeup>,
) -> Result<IncrementalDelta, String> {
    // Fails closed on any non-`Graph` committed version (`Native` or `None`),
    // matching the original `version_scope != Graph` guard exactly while also
    // extracting the source version it authorizes below.
    let CommittedVersion::Graph { source, .. } = lease.record.committed_version else {
        return Err("reasoning projection requires a graph-authoritative event".to_string());
    };
    let position = ProjectionPosition {
        batch_id: lease.record.batch_id.clone(),
        ordinal: lease.record.ordinal,
        source_graph_version: source,
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
    let Some(mut file) = open_snapshot_file(persist_dir, &path)? else {
        return Ok(None);
    };
    decode_snapshot_file(&mut file, limits).map(Some)
}

fn open_snapshot_file(
    persist_dir: Option<&str>,
    path: &Path,
) -> Result<Option<std::fs::File>, String> {
    let Some(root) = persist_dir.map(Path::new) else {
        return Ok(None);
    };
    let Some(parent) = path.parent() else {
        return Err("reasoning projection snapshot is unavailable".to_string());
    };
    for directory in [root, parent] {
        match std::fs::symlink_metadata(directory) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err("reasoning projection snapshot is unavailable".to_string());
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err("reasoning projection snapshot is unavailable".to_string()),
        }
    }
    open_regular_snapshot(path)
}

fn open_regular_snapshot(path: &Path) -> Result<Option<std::fs::File>, String> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("reasoning projection snapshot is unavailable".to_string()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("reasoning projection snapshot is unavailable".to_string());
    }
    match std::fs::File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err("reasoning projection snapshot is unavailable".to_string()),
    }
}

fn decode_snapshot_file(
    file: &mut std::fs::File,
    limits: eg_types::msgpack::MsgpackLimits,
) -> Result<IncrementalReasoningIndex, String> {
    if file
        .metadata()
        .map_err(|_| "reasoning projection snapshot is unavailable".to_string())?
        .len()
        > limits.max_bytes as u64
    {
        return Err("reasoning projection snapshot exceeds its size limit".to_string());
    }
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut *file)
        .take((limits.max_bytes as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| "reasoning projection snapshot is unavailable".to_string())?;
    if bytes.len() > limits.max_bytes {
        return Err("reasoning projection snapshot exceeds its size limit".to_string());
    }
    let index: IncrementalReasoningIndex = eg_types::msgpack::decode_bounded(&bytes, limits)
        .map_err(|_| "reasoning projection snapshot is invalid".to_string())?;
    index.validate()?;
    Ok(index)
}

/// Read the durable reasoning authority for one graph. Missing and corrupt images
/// are distinct hard errors; served queries never manufacture an empty projection.
pub async fn read_index(
    persist_dir: Option<&str>,
    graph_name: &str,
) -> Result<IncrementalReasoningIndex, String> {
    let graph_fname = crate::persist::sanitize(graph_name);
    let persist_dir = persist_dir.map(str::to_owned);
    run_projection_job(move || {
        load_index(persist_dir.as_deref(), &graph_fname)?
            .ok_or_else(|| "reasoning projection is not initialized".to_string())
    })
    .await
}

pub async fn materialization_status(
    persist_dir: Option<&str>,
    graph_name: &str,
    node_id: &str,
) -> Result<(Option<eg_epistemic::ProjectedMaterializationStatus>, u64), String> {
    let index = read_index(persist_dir, graph_name).await?;
    let source_graph_version = index
        .position
        .as_ref()
        .map_or(0, |position| position.source_graph_version);
    Ok((index.status_of(node_id), source_graph_version))
}

pub async fn stale_materializations(
    persist_dir: Option<&str>,
    graph_name: &str,
) -> Result<(Vec<String>, u64), String> {
    let index = read_index(persist_dir, graph_name).await?;
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
pub async fn recompute_materialization(
    persist_dir: Option<&str>,
    graph_name: &str,
    view: eg_core::graph::GraphView,
    authoritative_graph_version: u64,
    node_id: &str,
    expected_source_graph_version: u64,
) -> Result<(eg_epistemic::ProjectedMaterialization, u64), String> {
    if authoritative_graph_version != expected_source_graph_version {
        return Err("STALE_RECOMPUTE_FENCE: authoritative graph version changed".to_string());
    }
    let graph_fname = crate::persist::sanitize(graph_name);
    let persist_dir = persist_dir.map(str::to_owned);
    let node_id = node_id.to_string();
    let (materialization, fence_epoch, stale_count) = run_projection_job(move || {
        let mut index = load_index(persist_dir.as_deref(), &graph_fname)?
            .ok_or_else(|| "reasoning projection is not initialized".to_string())?;
        let fence_epoch = index.claim_recompute(&node_id, expected_source_graph_version)?;
        let provenance = resolve_materialization_provenance(&view, &node_id);
        let materialization = index.complete_recompute(
            &node_id,
            expected_source_graph_version,
            fence_epoch,
            provenance,
        )?;
        persist_index(persist_dir.as_deref(), &graph_fname, &index)?;
        Ok((
            materialization,
            fence_epoch,
            index.stale_materializations().len(),
        ))
    })
    .await?;
    crate::metrics::set_epistemic_materializations_stale(stale_count as i64);
    Ok((materialization, fence_epoch))
}

fn resolve_materialization_provenance(
    view: &eg_core::graph::GraphView,
    node_id: &str,
) -> Option<(std::collections::BTreeSet<String>, Option<String>)> {
    let properties = view.node_properties.get(node_id)?;
    // Yield EVERY parallel entry per outgoing pair (mirrors
    // `eg_epistemic::incremental::register_from_graph_view`): `resolve_provenance`
    // itself filters by each entry's `relationship`, so narrowing to
    // `versions.last()` here would silently drop a `DERIVED_FROM`/`GENERATED_BY`
    // edge shadowed by a later, different-relationship edge to the same target
    // (see the read/write model note on `GraphCore::edge_properties`).
    let mut outgoing = Vec::new();
    for ((source, target), versions) in &view.edge_properties {
        if source != node_id {
            continue;
        }
        for edge_properties in versions {
            outgoing.push((target.clone(), edge_properties.as_slice()));
        }
    }
    Some(eg_epistemic::resolve_provenance(
        Some(properties.as_slice()),
        outgoing,
    ))
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
    encoded.map_err(|_| "reasoning projection snapshot could not be encoded".to_string())?;
    writer
        .flush()
        .map_err(|_| "reasoning projection snapshot could not be persisted".to_string())?;
    Ok(writer.written)
}

fn persist_index(
    persist_dir: Option<&str>,
    graph_fname: &str,
    index: &IncrementalReasoningIndex,
) -> Result<(), String> {
    persist_snapshot(
        persist_dir,
        graph_fname,
        index,
        MAX_PROJECTION_SNAPSHOT_BYTES,
    )
}

fn persist_snapshot(
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
    let root = Path::new(persist_dir.ok_or_else(|| {
        "reasoning projection requires a configured durable persistence directory".to_string()
    })?);
    let parent = path
        .parent()
        .ok_or_else(|| "reasoning projection snapshot path is invalid".to_string())?;
    prepare_snapshot_directory(root, parent)?;
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err("reasoning projection snapshot is unavailable".to_string());
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("reasoning projection snapshot is unavailable".to_string()),
    }
    let temporary = path.with_extension("msgpack.tmp");
    let file = create_temporary_snapshot(&temporary)?;
    write_and_publish_snapshot(file, &temporary, &path, parent, index, max_bytes)
}

fn prepare_snapshot_directory(root: &Path, parent: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err("reasoning projection snapshot directory is unavailable".to_string());
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(root).map_err(|_| {
                "reasoning projection snapshot directory is unavailable".to_string()
            })?;
        }
        Err(_) => {
            return Err("reasoning projection snapshot directory is unavailable".to_string());
        }
    }
    std::fs::create_dir_all(parent)
        .map_err(|_| "reasoning projection snapshot directory is unavailable".to_string())?;
    for directory in [root, parent] {
        let metadata = std::fs::symlink_metadata(directory)
            .map_err(|_| "reasoning projection snapshot directory is unavailable".to_string())?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err("reasoning projection snapshot directory is unavailable".to_string());
        }
    }
    Ok(())
}

fn create_temporary_snapshot(temporary: &Path) -> Result<std::fs::File, String> {
    match std::fs::symlink_metadata(temporary) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err("reasoning projection temporary snapshot is unavailable".to_string());
        }
        Ok(_) => std::fs::remove_file(temporary)
            .map_err(|_| "reasoning projection temporary snapshot is unavailable".to_string())?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err("reasoning projection temporary snapshot is unavailable".to_string());
        }
    }
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)
        .map_err(|_| "reasoning projection temporary snapshot is unavailable".to_string())
}

fn write_and_publish_snapshot(
    mut file: std::fs::File,
    temporary: &Path,
    path: &Path,
    _parent: &Path,
    index: &IncrementalReasoningIndex,
    max_bytes: u64,
) -> Result<(), String> {
    if let Err(error) = encode_index_bounded(&mut file, index, max_bytes) {
        drop(file);
        cleanup_temporary_snapshot(temporary)?;
        return Err(error);
    }
    if file.sync_all().is_err() {
        drop(file);
        cleanup_temporary_snapshot(temporary)?;
        return Err("reasoning projection snapshot could not be persisted".to_string());
    }
    // Close the writable handle before replacement, which Windows requires.
    drop(file);
    if replace_snapshot(temporary, path).is_err() {
        cleanup_temporary_snapshot(temporary)?;
        return Err("reasoning projection snapshot could not be published".to_string());
    }
    #[cfg(not(windows))]
    {
        let directory = std::fs::File::open(_parent)
            .map_err(|_| "reasoning projection snapshot directory is unavailable".to_string())?;
        directory.sync_all().map_err(|_| {
            "reasoning projection snapshot directory could not be persisted".to_string()
        })?;
    }
    // ReplaceFileW/MoveFileExW use their write-through flags below. Windows
    // does not support opening a directory through std::fs::File for a second fsync.
    Ok(())
}

/// Rollback cleanup used only after a failed temporary-image transaction.
/// Never follows or removes a symlink an external actor may have swapped in.
fn cleanup_temporary_snapshot(temporary: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(temporary) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            std::fs::remove_file(temporary).map_err(|_| {
                "reasoning projection temporary snapshot could not be cleaned".to_string()
            })
        }
        Ok(_) => Err("reasoning projection temporary snapshot could not be cleaned".to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("reasoning projection temporary snapshot could not be cleaned".to_string()),
    }
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

fn current_time_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Condvar, Mutex as StdMutex};

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

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_waiter_cannot_release_an_inflight_persistence_job() {
        let release = Arc::new((StdMutex::new(false), Condvar::new()));
        let release_job = release.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let first = tokio::spawn(run_projection_job(move || {
            started_tx
                .send(())
                .map_err(|_| "test start receiver is unavailable".to_string())?;
            let (lock, ready) = &*release_job;
            let mut released = lock
                .lock()
                .map_err(|_| "test release lock is unavailable".to_string())?;
            while !*released {
                released = ready
                    .wait(released)
                    .map_err(|_| "test release lock is unavailable".to_string())?;
            }
            Ok(())
        }));
        started_rx.await.unwrap();

        first.abort();
        assert!(projection_write_lock().try_lock().is_err());

        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
        let second = tokio::spawn(run_projection_job(move || {
            acquired_tx
                .send(())
                .map_err(|_| "test acquisition receiver is unavailable".to_string())?;
            Ok(())
        }));

        let (lock, ready) = &*release;
        *lock.lock().unwrap() = true;
        ready.notify_one();
        acquired_rx.await.unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn durable_reader_fails_closed_for_missing_and_corrupt_projection() {
        let root = test_root();
        let root_str = root.to_string_lossy();
        assert!(read_index(Some(&root_str), "graph")
            .await
            .unwrap_err()
            .contains("not initialized"));

        let path = snapshot_path(Some(&root_str), "graph").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"not-a-projection").unwrap();
        assert!(read_index(Some(&root_str), "graph")
            .await
            .unwrap_err()
            .contains("invalid"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn durable_snapshot_replaces_an_existing_image() {
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

        let restored = read_index(Some(&root_str), "graph").await.unwrap();
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

    #[tokio::test(flavor = "current_thread")]
    async fn interrupted_temporary_image_is_recovered_on_restart() {
        let root = test_root();
        let root_str = root.to_string_lossy().to_string();
        let initial = stale_index();
        persist_index(Some(&root_str), "graph", &initial).unwrap();
        let temporary = snapshot_path(Some(&root_str), "graph")
            .unwrap()
            .with_extension("msgpack.tmp");
        std::fs::write(&temporary, b"interrupted-publication").unwrap();

        let mut replacement = stale_index();
        replacement
            .apply_batch(
                ProjectionPosition {
                    batch_id: "restart".to_string(),
                    ordinal: 0,
                    source_graph_version: 3,
                },
                &[],
            )
            .unwrap();
        let persist_root = root_str.clone();
        let expected = replacement.clone();
        run_projection_job(move || persist_index(Some(&persist_root), "graph", &replacement))
            .await
            .unwrap();

        assert_eq!(
            read_index(Some(&root_str), "graph").await.unwrap(),
            expected
        );
        assert!(!temporary.exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_and_temporary_symlinks_fail_closed_without_touching_targets() {
        use std::os::unix::fs::symlink;

        let root = test_root();
        let root_str = root.to_string_lossy().to_string();
        let outside = root.with_extension("outside");
        std::fs::write(&outside, b"outside-sentinel").unwrap();
        let path = snapshot_path(Some(&root_str), "graph").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        symlink(&outside, &path).unwrap();

        let error = read_index(Some(&root_str), "graph").await.unwrap_err();
        assert_eq!(error, "reasoning projection snapshot is unavailable");
        assert!(!error.contains(&root_str));
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside-sentinel");

        std::fs::remove_file(&path).unwrap();
        persist_index(Some(&root_str), "graph", &stale_index()).unwrap();
        let temporary = path.with_extension("msgpack.tmp");
        symlink(&outside, &temporary).unwrap();
        let core = eg_core::graph::GraphCore::new();
        core.add_node("derived".to_string(), derived_properties());
        let error = recompute_materialization(
            Some(&root_str),
            "graph",
            core.analysis_snapshot(),
            2,
            "derived",
            2,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            "reasoning projection temporary snapshot is unavailable"
        );
        assert!(!error.contains(&root_str));
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside-sentinel");
        assert_eq!(
            read_index(Some(&root_str), "graph").await.unwrap(),
            stale_index()
        );

        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_file(outside);

        let linked_root = test_root();
        let linked_root_str = linked_root.to_string_lossy().to_string();
        let linked_outside = linked_root.with_extension("outside-directory");
        std::fs::create_dir_all(&linked_outside).unwrap();
        symlink(&linked_outside, &linked_root).unwrap();
        let error = run_projection_job(move || {
            persist_index(Some(&linked_root_str), "graph", &stale_index())
        })
        .await
        .unwrap_err();
        assert_eq!(
            error,
            "reasoning projection snapshot directory is unavailable"
        );
        assert!(!linked_outside.join("reasoning-projections").exists());
        std::fs::remove_file(linked_root).unwrap();
        let _ = std::fs::remove_dir_all(linked_outside);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_snapshot_encoding_never_crosses_cap_or_replaces_prior_image() {
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
        let persist_root = root_str.to_string();
        let error = run_projection_job(move || {
            persist_snapshot(Some(&persist_root), "graph", &replacement, TEST_CAP)
        })
        .await
        .unwrap_err();
        assert!(error.contains("size limit"));
        assert_eq!(read_index(Some(&root_str), "graph").await.unwrap(), initial);
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

    #[tokio::test(flavor = "current_thread")]
    async fn durable_reader_rejects_a_declared_allocation_bomb() {
        let root = test_root();
        let root_str = root.to_string_lossy();
        let path = snapshot_path(Some(&root_str), "graph").unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // MessagePack array32 claiming 2^32-1 items with no body. Structural
        // preflight rejects the declaration before serde can reserve a vector.
        std::fs::write(path, [0xdd, 0xff, 0xff, 0xff, 0xff]).unwrap();

        assert!(read_index(Some(&root_str), "graph")
            .await
            .unwrap_err()
            .contains("invalid"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recompute_is_version_fenced_and_durable() {
        let root = test_root();
        let root_str = root.to_string_lossy();
        persist_index(Some(&root_str), "graph", &stale_index()).unwrap();

        let core = eg_core::graph::GraphCore::new();
        core.add_node("derived".to_string(), derived_properties());
        let view = core.analysis_snapshot();

        let mismatch =
            recompute_materialization(Some(&root_str), "graph", view.clone(), 3, "derived", 2)
                .await
                .unwrap_err();
        assert!(mismatch.contains("authoritative graph version changed"));

        let (materialization, fence_epoch) =
            recompute_materialization(Some(&root_str), "graph", view, 2, "derived", 2)
                .await
                .unwrap();
        assert_eq!(
            materialization.status,
            ProjectedMaterializationStatus::Fresh
        );
        assert_eq!(fence_epoch, 1);
        assert_eq!(
            read_index(Some(&root_str), "graph")
                .await
                .unwrap()
                .status_of("derived"),
            Some(ProjectedMaterializationStatus::Fresh)
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
