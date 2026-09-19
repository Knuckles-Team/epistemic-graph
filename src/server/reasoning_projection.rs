//! Durable, incremental epistemic/causal/materialization projection worker.
//!
//! The authoritative MutationBatch outbox is the scheduling input; its digest-only
//! lease resolves the canonical operation/state record from the same authority.
//! Each lease is applied idempotently, the compact projection is atomically
//! snapshotted, and only then is it acknowledged.
//!
//! The acknowledgement IS the cursor advance. The mutation kernel marks the
//! delivery row delivered and writes the `(scope, consumer)` watermark in one
//! transaction, so this worker never acknowledges and then separately advances or
//! re-reads a watermark: a crash between those two steps is not representable,
//! and the advanced cursor comes back from the acknowledgement itself.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use eg_epistemic::IncrementalReasoningIndex;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::server::state::ServerState;

// The outbox-tailing half of this module. The durable outbox -- its leases, its
// claim budget and its projection cursors -- is a mutation-kernel ledger only the
// redb authority holds, so the `PersistenceBackend` methods that carry it are
// gated on `redb` and so is every item here that calls them. The reader half
// (`read_index`, `materialization_status`, `stale_materializations`,
// `recompute_materialization`) serves from the on-disk projection and is not.

#[cfg(any(feature = "redb", test))]
use eg_epistemic::ProjectionPosition;
#[cfg(feature = "redb")]
use eg_epistemic::{IncrementalDelta, ReasoningProjectionWakeup};
#[cfg(feature = "redb")]
use eg_transaction::OutboxClaimBudget;
#[cfg(feature = "redb")]
use eg_types::mutation_batch::{
    CommittedVersion, MutationBatchStatus, MutationOutboxLease, MutationProjectionCursor,
};
#[cfg(feature = "redb")]
use std::collections::HashSet;
#[cfg(feature = "redb")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The one durable name this worker has on every graph's ledger.
///
/// The retired shard ledger carried two: a `projection` for its
/// `(projection, tenant, graph)` cursor key and a `consumer` for its
/// `(batch, ordinal, consumer)` lease key. Under one scope key both are
/// `(scope, consumer)`, and the lease the kernel hands back carries this exact
/// string, so an acknowledgement could not name a different one even if the
/// second constant had survived.
#[cfg(feature = "redb")]
const CONSUMER: &str = "reasoning-projection-v1";

/// The topic this worker is durably subscribed to.
///
/// Every batch `mutation_batch::compile` builds carries exactly one
/// `engine.projection.rebuild` outbox intent, so this subscription is the whole
/// committed stream rather than a slice of it -- and it is where the filtering
/// the retired path did after claiming now happens, one index scan earlier.
#[cfg(feature = "redb")]
const TOPIC: &str = "engine.projection.rebuild";

/// Rows one sweep may claim across every graph it visits.
///
/// A quarter of it -- the budget's consecutive-claim cap -- is 64, which is
/// exactly the per-graph limit the retired shard claim took.
#[cfg(feature = "redb")]
const CLAIM_SWEEP_LIMIT: u32 = 256;

/// How long a claimed row stays leased to this worker.
#[cfg(feature = "redb")]
const CLAIM_LEASE_MS: u64 = 30_000;

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

/// Start the singleton projection loop after graph recovery.
///
/// A build without the redb authority has no durable outbox to tail at all --
/// the leases and the projection cursors are mutation-kernel ledger rows -- so
/// there is no loop to start. The projection reader below still serves whatever
/// is on disk.
pub fn spawn(state: Arc<RwLock<ServerState>>) {
    #[cfg(feature = "redb")]
    tokio::spawn(projection_loop(state));
    #[cfg(not(feature = "redb"))]
    drop(state);
}

#[cfg(feature = "redb")]
struct ProjectionContext {
    persistence: Arc<dyn crate::server::persistence::PersistenceBackend>,
    persist_dir: Option<String>,
    graphs: Vec<(String, Arc<eg_core::graph::GraphCore>)>,
}

#[cfg(feature = "redb")]
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

#[cfg(feature = "redb")]
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

/// One sweep over every registered graph, under ONE claim budget.
///
/// The budget is the sweep's value, not the call's. A claim is bound to exactly
/// one serving scope by construction, so DESIGN.md's per-tenant weighted round
/// robin and its consecutive-claim cap only mean anything across the scopes a
/// sweep visits; rebuilding the budget per graph would cap nothing and the whole
/// fairness rule would be inert. It is rebuilt per SWEEP rather than kept alive
/// across sweeps because it carries the `now_ms` every lease deadline is measured
/// from, and a stale one would hand out leases that are already expired.
#[cfg(feature = "redb")]
async fn process_graphs(context: &ProjectionContext) -> bool {
    let Ok(mut budget) =
        OutboxClaimBudget::new(CLAIM_SWEEP_LIMIT, CLAIM_LEASE_MS, current_time_ms())
    else {
        return false;
    };
    let mut progressed = false;
    for (graph, core) in &context.graphs {
        // Reuse one budget for the whole sweep. All graph-shard scopes share
        // the reserved ledger tenant, so the budget never observes cross-tenant
        // contention here; its remaining count still bounds total work across
        // every graph without a per-graph reset that could exceed the sweep.
        if process_graph(context, graph, core, &mut budget).await {
            progressed = true;
        }
    }
    progressed
}

/// Graphs whose durable outbox subscription this process has established.
///
/// The subscription itself is the durable fact; this only keeps the worker from
/// opening a write transaction per graph on every poll to re-assert something it
/// already proved. It is deliberately process-local, so an empty memo after a
/// restart costs one idempotent re-subscribe and can never mean a missing
/// subscription.
#[cfg(feature = "redb")]
fn subscribed_graphs() -> &'static RwLock<HashSet<String>> {
    static SUBSCRIBED: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();
    SUBSCRIBED.get_or_init(|| RwLock::new(HashSet::new()))
}

/// Establish this worker's durable subscription on one graph, once.
///
/// A subscription is a real precondition of a claim, not a formality: the kernel
/// refuses to claim for a consumer that has none, and the subscription is what
/// bounds the ordered stream the watermark names. It is asserted here, at the
/// worker's startup on a graph, rather than recovered from a failed claim --
/// a claim that quietly subscribed itself could widen a live projection's stream
/// without anyone having decided to.
#[cfg(feature = "redb")]
async fn ensure_subscription(
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    graph_fname: &str,
) -> bool {
    if subscribed_graphs().read().await.contains(graph_fname) {
        return true;
    }
    if persistence
        .subscribe_mutation_outbox(graph_fname, CONSUMER, TOPIC)
        .await
        .is_err()
    {
        return false;
    }
    subscribed_graphs()
        .write()
        .await
        .insert(graph_fname.to_string());
    true
}

#[cfg(feature = "redb")]
async fn process_graph(
    context: &ProjectionContext,
    graph: &str,
    core: &Arc<eg_core::graph::GraphCore>,
    budget: &mut OutboxClaimBudget,
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
    if !ensure_subscription(&context.persistence, &graph_fname).await {
        return false;
    }
    // A deferred outcome is the queue's own backpressure: a full in-flight set,
    // a spent allowance or an incomplete index backfill claims nothing and
    // leaves every durable intention pending. An empty non-deferred outcome is
    // an idle queue. Neither is projection progress.
    let outcome = match context
        .persistence
        .claim_mutation_outbox(&graph_fname, CONSUMER, budget)
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            // A graph purge retires the durable consumer and cursor together,
            // but this process-local memo and the derived snapshot live outside
            // that transaction.  The exact missing-subscription refusal is the
            // durable proof that a cached name no longer names the incarnation
            // we subscribed to.  Drop both aliases and defer: the next sweep
            // rebuilds from the current GraphCore before re-subscribing.  Other
            // claim errors must never discard a valid projection image.
            if error == "outbox consumer has no durable subscription"
                && reset_retired_projection(context.persist_dir.clone(), graph_fname.clone()).await
            {
                subscribed_graphs().write().await.remove(&graph_fname);
            }
            return false;
        }
    };
    if outcome.is_deferred() {
        return false;
    }
    process_leases(
        &context.persistence,
        context.persist_dir.clone(),
        &graph_fname,
        core.clone(),
        outcome.claims,
    )
    .await
}

/// Remove the name-keyed derived image after the durable ledger proves that
/// this process's cached subscription belongs to a retired graph incarnation.
///
/// The projection is rebuildable from the current authoritative `GraphCore`.
/// Failure leaves the memo in place so the next sweep repeats the exact
/// missing-subscription recovery instead of subscribing while a retired image
/// could still shadow the recreated graph.
#[cfg(feature = "redb")]
async fn reset_retired_projection(persist_dir: Option<String>, graph_fname: String) -> bool {
    run_projection_job(move || remove_projection_snapshot(persist_dir.as_deref(), &graph_fname))
        .await
        .is_ok()
}

#[cfg(feature = "redb")]
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

#[cfg(feature = "redb")]
async fn process_leases(
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    persist_dir: Option<String>,
    graph_fname: &str,
    core: Arc<eg_core::graph::GraphCore>,
    leases: Vec<MutationOutboxLease>,
) -> bool {
    // The durable watermark is the restart/reconciliation boundary, and it is
    // read ONCE per poll rather than once per lease: after the first
    // acknowledgement the watermark is exactly the cursor that acknowledgement
    // returned, written in the same transaction that marked the row delivered.
    // The retired ledger had to acknowledge and then advance separately, so
    // re-reading between the two was the only way to see the gap; there is no
    // longer a gap to see.
    let Ok(mut watermark) = persistence
        .read_mutation_projection_cursor(graph_fname, CONSUMER)
        .await
    else {
        return false;
    };
    let mut progressed = false;
    for lease in leases {
        let Some(advanced) = process_lease(
            persistence,
            persist_dir.clone(),
            graph_fname,
            core.clone(),
            &lease,
            watermark.as_ref(),
        )
        .await
        else {
            break;
        };
        watermark = Some(advanced);
        progressed = true;
    }
    progressed
}

/// Apply one leased event and acknowledge it, returning the watermark the
/// acknowledgement advanced to, or `None` if the worker must stop on this graph.
#[cfg(feature = "redb")]
async fn process_lease(
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    persist_dir: Option<String>,
    graph_fname: &str,
    core: Arc<eg_core::graph::GraphCore>,
    lease: &MutationOutboxLease,
    watermark: Option<&MutationProjectionCursor>,
) -> Option<MutationProjectionCursor> {
    // Projection wake-ups contain only domain-separated identities and closed
    // categorical tags. Bind them to the authoritative operation digest before
    // allowing the side index to advance.
    let wakeup = resolve_projection_wakeup(persistence, graph_fname, lease)
        .await
        .ok()?;
    let (newly_stale, stale_count) = apply_lease_and_publish(
        persist_dir,
        graph_fname.to_string(),
        core,
        lease.clone(),
        wakeup,
        watermark.cloned(),
    )
    .await?;
    crate::metrics::epistemic_materializations_staled(newly_stale as u64);
    crate::metrics::set_epistemic_materializations_stale(stale_count as i64);
    // The acknowledgement IS the cursor advance: the kernel marks the delivery
    // row delivered and writes the `(scope, consumer)` watermark in ONE admitted
    // transaction. A crash between the two is not representable, so there is no
    // second call to make and no watermark left to re-read -- the advanced
    // cursor is the return value.
    // Do not apply later leased rows after an ordering gap
    // (`OUTBOX_ORDER_GAP`) or a lost lease (`STALE_OUTBOX_LEASE`). The lease
    // is deliberately NOT released: its expiry is this worker's backoff,
    // where `outbox_release` would re-offer the row on the next poll and
    // turn a persistent failure into a hot retry loop. The sidecar is at
    // most one event ahead; exact-position replay is harmless.
    persistence
        .ack_mutation_outbox(graph_fname, lease, current_time_ms())
        .await
        .ok()
}

#[cfg(feature = "redb")]
async fn resolve_projection_wakeup(
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    graph_fname: &str,
    lease: &MutationOutboxLease,
) -> Result<ReasoningProjectionWakeup, ()> {
    // The durable subscription bounds every claim to exactly one topic, so a
    // foreign topic here is a corrupt ledger rather than a row to skip. The
    // retired path had to filter after claiming and advanced its cursor over
    // whatever it skipped, which is why it needed a "no wake-up" apply at all.
    if lease.record.intent.topic != TOPIC {
        return Err(());
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
    Ok(wakeup)
}

#[cfg(feature = "redb")]
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

#[cfg(feature = "redb")]
async fn apply_lease_and_publish(
    persist_dir: Option<String>,
    graph_fname: String,
    core: Arc<eg_core::graph::GraphCore>,
    lease: MutationOutboxLease,
    wakeup: ReasoningProjectionWakeup,
    watermark: Option<MutationProjectionCursor>,
) -> Option<(usize, usize)> {
    match run_projection_job(move || {
        let mut index = load_index(persist_dir.as_deref(), &graph_fname)?
            .ok_or_else(|| "reasoning projection is not initialized".to_string())?;
        require_snapshot_not_behind_watermark(&index, watermark.as_ref())?;
        let delta = apply_lease(&mut index, &core, &lease, &wakeup)?;
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

/// Refuse to advance a snapshot the durable watermark has already passed.
///
/// The healthy order is snapshot-then-acknowledge, so an applied projection is
/// equal to or one event AHEAD of the ledger watermark, and re-applying an exact
/// position is idempotent. The converse -- an applied projection BEHIND the
/// watermark -- says the ledger already recorded events this image does not
/// contain, which no crash window between the two can produce and a restored
/// stale snapshot can. It is the direction that silently loses reasoning state,
/// so it fails closed for operator repair rather than being replayed over.
///
/// An index with no position at all is exempt: that is `initialize_index`'s
/// deliberate full rebuild from the live graph after the image went missing, not
/// a stale image, and it is complete as of a version no watermark can predate.
#[cfg(feature = "redb")]
fn require_snapshot_not_behind_watermark(
    index: &IncrementalReasoningIndex,
    watermark: Option<&MutationProjectionCursor>,
) -> Result<(), String> {
    let (Some(watermark), Some(position)) = (watermark, index.position.as_ref()) else {
        return Ok(());
    };
    let CommittedVersion::Graph { target, .. } = watermark.committed_version else {
        return Err("reasoning projection watermark is not graph-authoritative".to_string());
    };
    if position.source_graph_version < target {
        return Err(
            "reasoning projection snapshot is behind its durable projection cursor".to_string(),
        );
    }
    Ok(())
}

#[cfg(feature = "redb")]
fn apply_lease(
    index: &mut IncrementalReasoningIndex,
    core: &eg_core::graph::GraphCore,
    lease: &MutationOutboxLease,
    wakeup: &ReasoningProjectionWakeup,
) -> Result<IncrementalDelta, String> {
    // Fails closed on any non-`Graph` committed version (`Native` or `None`),
    // matching the original `version_scope != Graph` guard exactly while also
    // extracting the committed image version it authorizes below.
    let CommittedVersion::Graph { target, .. } = lease.record.committed_version else {
        return Err("reasoning projection requires a graph-authoritative event".to_string());
    };
    let position = ProjectionPosition {
        batch_id: lease.record.batch_id.clone(),
        ordinal: lease.record.ordinal,
        // The projection describes the source graph AFTER this commit.
        // Its first valid position is target 1, not the bootstrap source 0.
        source_graph_version: target,
    };
    index.apply_wakeup(position, wakeup, &core.analysis_snapshot())
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

fn remove_projection_snapshot(persist_dir: Option<&str>, graph_fname: &str) -> Result<(), String> {
    let Some(path) = snapshot_path(persist_dir, graph_fname) else {
        return Ok(());
    };
    let Some(file) = open_snapshot_file(persist_dir, &path)? else {
        return Ok(());
    };
    drop(file);
    std::fs::remove_file(&path)
        .map_err(|_| "reasoning projection snapshot could not be retired".to_string())?;
    #[cfg(not(windows))]
    {
        let parent = path
            .parent()
            .ok_or_else(|| "reasoning projection snapshot path is invalid".to_string())?;
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| {
                "reasoning projection snapshot directory could not be persisted".to_string()
            })?;
    }
    Ok(())
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

#[cfg(feature = "redb")]
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

    #[cfg(feature = "redb")]
    use crate::server::persistence::PersistenceBackend;

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

    /// The projection cursor is no longer a watermark this worker re-reads after
    /// acknowledging -- the acknowledgement returns it -- so the one thing the
    /// pre-read still decides is whether the on-disk image may be advanced at
    /// all. Shape only: which positions are behind, not any queue threshold.
    #[cfg(feature = "redb")]
    #[test]
    fn an_image_behind_its_durable_cursor_refuses_to_advance() {
        fn watermark(target: u64) -> MutationProjectionCursor {
            MutationProjectionCursor {
                schema_version: eg_types::mutation_batch::MUTATION_BATCH_VERSION,
                projection: CONSUMER.to_string(),
                identity: eg_types::mutation_batch::MutationScopeIdentity::fixed_graph(
                    "graph-shard",
                    "graph",
                    "incarnation",
                )
                .unwrap(),
                batch_id: "acked".to_string(),
                outbox_ordinal: 0,
                committed_version: CommittedVersion::Graph {
                    source: target - 1,
                    target,
                },
                advanced_at_ms: 1,
            }
        }

        // `stale_index` last applied source version 2.
        let index = stale_index();
        assert!(require_snapshot_not_behind_watermark(&index, None).is_ok());
        assert!(require_snapshot_not_behind_watermark(&index, Some(&watermark(2))).is_ok());
        assert!(require_snapshot_not_behind_watermark(&index, Some(&watermark(1))).is_ok());
        let error = require_snapshot_not_behind_watermark(&index, Some(&watermark(3))).unwrap_err();
        assert!(error.contains("behind its durable projection cursor"));

        // A rebuilt-from-graph image carries no position: it is a deliberate
        // full rebuild, not a stale image, so no watermark can be ahead of it.
        let rebuilt = IncrementalReasoningIndex::default();
        assert!(rebuilt.position.is_none());
        assert!(require_snapshot_not_behind_watermark(&rebuilt, Some(&watermark(3))).is_ok());

        // A non-graph watermark is refused for the same reason `apply_lease`
        // refuses a non-graph event: there is no graph version to compare.
        let mut native = watermark(3);
        native.committed_version = CommittedVersion::Native {
            source: 3,
            target: 4,
        };
        assert!(require_snapshot_not_behind_watermark(&index, Some(&native))
            .unwrap_err()
            .contains("not graph-authoritative"));
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

    #[cfg(feature = "redb")]
    struct DeferredProjectionBackend {
        claim_calls: AtomicU64,
        ack_calls: AtomicU64,
        deferred_reason: StdMutex<Option<eg_transaction::OutboxDeferral>>,
    }

    #[cfg(feature = "redb")]
    impl DeferredProjectionBackend {
        fn new() -> Self {
            Self {
                claim_calls: AtomicU64::new(0),
                ack_calls: AtomicU64::new(0),
                deferred_reason: StdMutex::new(None),
            }
        }
    }

    #[cfg(feature = "redb")]
    #[async_trait::async_trait]
    impl crate::server::persistence::PersistenceBackend for DeferredProjectionBackend {
        async fn load_all(
            &self,
            _state: &Arc<RwLock<crate::server::state::ServerState>>,
        ) -> Result<usize, String> {
            Ok(0)
        }

        async fn record_durable(&self, _graph_fname: &str, _method: &Method) -> Result<(), String> {
            Ok(())
        }

        async fn subscribe_mutation_outbox(
            &self,
            _graph_fname: &str,
            _consumer: &str,
            _topic: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn claim_mutation_outbox(
            &self,
            _graph_fname: &str,
            _consumer: &str,
            _budget: &mut eg_transaction::OutboxClaimBudget,
        ) -> Result<eg_transaction::OutboxClaimOutcome, String> {
            self.claim_calls.fetch_add(1, Ordering::Relaxed);
            *self.deferred_reason.lock().unwrap() =
                Some(eg_transaction::OutboxDeferral::BudgetSpent);
            Ok(eg_transaction::OutboxClaimOutcome {
                claims: Vec::new(),
                deferred: Some(eg_transaction::OutboxDeferral::BudgetSpent),
                more_available: true,
                dead_lettered: Vec::new(),
            })
        }

        async fn ack_mutation_outbox(
            &self,
            _graph_fname: &str,
            _lease: &eg_types::mutation_batch::MutationOutboxLease,
            _now_ms: u64,
        ) -> Result<eg_types::mutation_batch::MutationProjectionCursor, String> {
            self.ack_calls.fetch_add(1, Ordering::Relaxed);
            Err("deferred projection must not acknowledge a lease".to_string())
        }

        fn shutdown(&self) {}
    }

    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "current_thread")]
    async fn process_graph_treats_budget_deferred_as_no_progress_without_ack() {
        let root = test_root();
        let root_str = root.to_string_lossy().to_string();
        let graph = format!("deferred-budget-{}", std::process::id());
        let backend = Arc::new(DeferredProjectionBackend::new());
        let persistence: Arc<dyn crate::server::persistence::PersistenceBackend> = backend.clone();
        let core = Arc::new(eg_core::graph::GraphCore::new());
        let context = ProjectionContext {
            persistence,
            persist_dir: Some(root_str.clone()),
            graphs: vec![(graph.clone(), core.clone())],
        };
        let mut budget =
            eg_transaction::OutboxClaimBudget::new(CLAIM_SWEEP_LIMIT, CLAIM_LEASE_MS, 1).unwrap();

        assert!(!process_graph(&context, &graph, &core, &mut budget).await);
        assert_eq!(backend.claim_calls.load(Ordering::Relaxed), 1);
        assert_eq!(backend.ack_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            *backend.deferred_reason.lock().unwrap(),
            Some(eg_transaction::OutboxDeferral::BudgetSpent)
        );
        assert!(load_index(Some(&root_str), &graph).unwrap().is_some());
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "redb")]
    fn projection_budget_batch(graph: &str, row: u64) -> eg_types::mutation_batch::MutationBatch {
        projection_batch(graph, row, format!("projection-budget-{graph}-{row}"))
    }

    #[cfg(feature = "redb")]
    fn projection_batch(
        graph: &str,
        row: u64,
        batch_id: String,
    ) -> eg_types::mutation_batch::MutationBatch {
        use std::collections::BTreeMap;

        use crate::server::mutation_batch::ENGINE_LEDGER_PRINCIPAL;
        use eg_types::contract::{Digest256, MethodId, Nonce};
        use eg_types::mutation_batch::{
            CompiledOperation, CompiledScope, DurabilityDomain, MutationBatch, MutationEnvelope,
            MutationOperation, MutationOutboxIntent, MutationSurface, VersionExpectation,
            BATCH_COMPILED_METHODS, MUTATION_BATCH_VERSION,
        };

        // A CALLER identity. `shard::graph_scope_identity` builds the
        // POST-binding shard scope `(GRAPH_SHARD_TENANT, graph, incarnation)`,
        // and submitting that to `shard::bind_caller_batch` -- whose contract is
        // to bind a caller batch ONTO that scope -- is refused with
        // "'__shard__' is the graph shard's reserved scope tenant and cannot be
        // a caller tenant". Same fix as
        // `resource_reservation_tests::resource_batch` and
        // `work_item_capability`.
        let identity = eg_types::mutation_batch::MutationScopeIdentity::graph(
            eg_types::mutation_batch::ScopeTenantId::new("tenant-a").expect("valid caller tenant"),
            eg_types::mutation_batch::LogicalName::new(graph).expect("valid graph name"),
            eg_types::mutation_batch::IncarnationId::new("incarnation:test:reasoning-projection")
                .expect("valid incarnation"),
        );
        let actor = crate::server::mutation_batch::principal_fingerprint(
            "reasoning-projection-budget-test",
        )
        .expect("test actor fingerprint");
        let method = MethodId::new(BATCH_COMPILED_METHODS).expect("compiled batch method id");
        let operation = MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Graph,
            domain: DurabilityDomain::GraphRows,
            method: Method::AddNode {
                node_id: format!("projection-budget-node-{row}"),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            },
        };
        let payload =
            crate::redb_store::projection_payload_for_operations(std::slice::from_ref(&operation))
                .expect("projection wake-up payload");
        let schema_digest = Digest256::from_bytes([1_u8; 32]);
        let scope_sha256 = identity.identity_digest().to_hex();
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.clone(),
            envelope: MutationEnvelope::for_scope(
                CompiledScope {
                    identity: &identity,
                    actor: &actor,
                    serving_principal: ENGINE_LEDGER_PRINCIPAL,
                    request_id: row + 1,
                    idempotency_key: &batch_id,
                    nonce: Nonce::from_bytes([row as u8; 32]),
                    now_ms: row + 1,
                },
                CompiledOperation {
                    method: method.clone(),
                    method_schema_id: eg_types::mutation_batch::method_schema_id(&method)
                        .expect("compiled batch schema id"),
                    method_schema_digest: schema_digest,
                    canonical_payload_digest: Digest256::from_bytes([2_u8; 32]),
                },
            )
            .expect("test batch envelope"),
            identity,
            placement_epoch: 0,
            version_expectation: VersionExpectation::Graph(row),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![operation],
            outbox: vec![MutationOutboxIntent {
                topic: TOPIC.to_string(),
                key: batch_id.clone(),
                payload,
                headers: BTreeMap::from([
                    ("actor".to_string(), actor),
                    ("scope_sha256".to_string(), scope_sha256),
                ]),
            }],
            created_at_ms: row + 1,
        };
        batch
            .reseal_envelope(schema_digest)
            .expect("test batch envelope reseal");
        batch.validate().expect("test batch validates");
        batch
    }

    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "current_thread")]
    async fn delete_recreate_rebuilds_subscription_and_projection_before_ack() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::redb_backend::RedbBackend;

        let root = test_root();
        let root_str = root.to_string_lossy().to_string();
        let graph = format!("projection-recreate-{}", std::process::id());
        let graph_fname = crate::persist::sanitize(&graph);
        subscribed_graphs().write().await.remove(&graph_fname);

        let backend = Arc::new(
            RedbBackend::open_with_shards(root_str.clone(), 256, 1)
                .expect("open projection recreate backend"),
        );
        let persistence: Arc<dyn crate::server::persistence::PersistenceBackend> = backend.clone();
        let retired_batch =
            projection_batch(&graph_fname, 0, format!("projection-retired-{graph_fname}"));
        persistence
            .commit_mutation_batch(&graph_fname, &retired_batch, None, 1)
            .await
            .expect("commit retired-incarnation projection event");
        let retired_core = Arc::new(eg_core::graph::GraphCore::new());
        retired_core.add_node("retired-derived".to_string(), derived_properties());
        let retired_context = ProjectionContext {
            persistence: persistence.clone(),
            persist_dir: Some(root_str.clone()),
            graphs: vec![(graph.clone(), retired_core.clone())],
        };
        let mut retired_budget =
            OutboxClaimBudget::new(CLAIM_SWEEP_LIMIT, CLAIM_LEASE_MS, current_time_ms()).unwrap();
        assert!(process_graph(&retired_context, &graph, &retired_core, &mut retired_budget).await);
        let first_image = read_index(Some(&root_str), &graph).await.unwrap();
        assert_eq!(
            first_image.position.as_ref().unwrap().source_graph_version,
            1
        );
        // Re-polling an acknowledged first commit cannot advance or replace
        // its durable projection image.
        assert!(!process_graph(&retired_context, &graph, &retired_core, &mut retired_budget).await);
        assert_eq!(
            read_index(Some(&root_str), &graph).await.unwrap(),
            first_image
        );
        assert!(subscribed_graphs().read().await.contains(&graph_fname));
        assert_eq!(
            persistence
                .read_mutation_projection_cursor(&graph_fname, CONSUMER)
                .await
                .unwrap()
                .unwrap()
                .batch_id,
            retired_batch.batch_id
        );

        persistence
            .purge_graph(&graph_fname)
            .await
            .expect("purge retired graph incarnation");
        assert!(persistence
            .read_mutation_projection_cursor(&graph_fname, CONSUMER)
            .await
            .unwrap()
            .is_none());

        let recreated_batch = projection_batch(
            &graph_fname,
            0,
            format!("projection-recreated-{graph_fname}"),
        );
        persistence
            .commit_mutation_batch(&graph_fname, &recreated_batch, None, 2)
            .await
            .expect("commit recreated-incarnation projection event");
        let recreated_core = Arc::new(eg_core::graph::GraphCore::new());
        recreated_core.add_node("fresh-derived".to_string(), derived_properties());
        let recreated_context = ProjectionContext {
            persistence: persistence.clone(),
            persist_dir: Some(root_str.clone()),
            graphs: vec![(graph.clone(), recreated_core.clone())],
        };

        // The first poll sees the durable missing-subscription refusal. It is a
        // failure, not an idle or deferred queue outcome: no cursor advances,
        // and recovery retires both process-local aliases before returning.
        let mut recovery_budget =
            OutboxClaimBudget::new(CLAIM_SWEEP_LIMIT, CLAIM_LEASE_MS, current_time_ms()).unwrap();
        assert!(
            !process_graph(
                &recreated_context,
                &graph,
                &recreated_core,
                &mut recovery_budget
            )
            .await
        );
        assert!(!subscribed_graphs().read().await.contains(&graph_fname));
        assert!(persistence
            .read_mutation_projection_cursor(&graph_fname, CONSUMER)
            .await
            .unwrap()
            .is_none());
        assert!(read_index(Some(&root_str), &graph)
            .await
            .unwrap_err()
            .contains("not initialized"));

        // A later sweep owns a fresh budget, rebuilds from the recreated core,
        // re-subscribes once, and only then acknowledges the new event.
        let mut recreated_budget =
            OutboxClaimBudget::new(CLAIM_SWEEP_LIMIT, CLAIM_LEASE_MS, current_time_ms()).unwrap();
        assert!(
            process_graph(
                &recreated_context,
                &graph,
                &recreated_core,
                &mut recreated_budget
            )
            .await
        );
        let cursor = persistence
            .read_mutation_projection_cursor(&graph_fname, CONSUMER)
            .await
            .unwrap()
            .expect("recreated event is acknowledged");
        assert_eq!(cursor.batch_id, recreated_batch.batch_id);
        let rebuilt = read_index(Some(&root_str), &graph).await.unwrap();
        assert_eq!(rebuilt.status_of("retired-derived"), None);
        assert_eq!(
            rebuilt.status_of("fresh-derived"),
            Some(ProjectedMaterializationStatus::Fresh)
        );

        subscribed_graphs().write().await.remove(&graph_fname);
        backend.shutdown();
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "current_thread")]
    async fn process_graphs_carries_one_claim_budget_across_graph_scopes() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        use crate::server::persistence::redb_backend::RedbBackend;

        let root = test_root();
        let root_str = root.to_string_lossy().to_string();
        let graph_names: Vec<String> = (0..5)
            .map(|index| format!("shared-budget-{}-{index}", std::process::id()))
            .collect();
        let backend = Arc::new(
            RedbBackend::open_with_shards(root_str.clone(), 256, 1)
                .expect("open projection budget backend"),
        );
        let persistence: Arc<dyn crate::server::persistence::PersistenceBackend> = backend.clone();

        for graph in &graph_names {
            for row in 0..64 {
                let batch = projection_budget_batch(graph, row);
                persistence
                    .commit_mutation_batch(graph, &batch, None, row + 1)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("commit projection budget fixture {graph}/{row}: {error}")
                    });
            }
        }

        let graphs = graph_names
            .iter()
            .map(|graph| (graph.clone(), Arc::new(eg_core::graph::GraphCore::new())))
            .collect();
        let context = ProjectionContext {
            persistence: persistence.clone(),
            persist_dir: Some(root_str.clone()),
            graphs,
        };
        assert!(process_graphs(&context).await);

        // Each graph admits at most the 64-row consecutive cap. Reusing the
        // sweep budget lets exactly four graph scopes consume the 256-row
        // allowance; a per-graph reset would incorrectly acknowledge graph 5.
        for (index, graph) in graph_names.iter().enumerate() {
            let cursor = persistence
                .read_mutation_projection_cursor(graph, CONSUMER)
                .await
                .unwrap();
            if index < 4 {
                let cursor = cursor.expect("budget admitted the first four graph scopes");
                assert_eq!(cursor.batch_id, format!("projection-budget-{graph}-63"));
                assert_eq!(cursor.outbox_ordinal, 0);
                let image = read_index(Some(&root_str), graph).await.unwrap();
                assert_eq!(image.position.as_ref().unwrap().source_graph_version, 64);
                assert!(require_snapshot_not_behind_watermark(&image, Some(&cursor)).is_ok());
                assert_eq!(
                    cursor.committed_version,
                    CommittedVersion::Graph {
                        source: 63,
                        target: 64,
                    }
                );
            } else {
                assert!(
                    cursor.is_none(),
                    "a per-graph budget reset would acknowledge the fifth graph"
                );
            }
        }

        backend.shutdown();
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }
}
