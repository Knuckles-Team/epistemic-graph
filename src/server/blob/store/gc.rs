//! Mark-and-sweep garbage collection over manifests, uploads and direct chunk
//! references.
//!
//! One pass reclaims, for the owners its [`SweepRequest`] covers:
//! * every manifest with **no holder** whose latest commit is **past the grace
//!   period** (a manifest without a retention row is never reclaimed);
//! * every upload idle **past the upload TTL**, with the upload row;
//! * every chunk that one of those names, or whose own direct reference was
//!   released, and that is **not reachable**: no surviving manifest names it, no
//!   surviving upload names it, and nothing holds it directly.
//!
//! The walks are generic over the table type, so the sweep (inside its admitted
//! write) and the read-only preview an external chunk backend deletes from
//! decide with one implementation. Working sets are ordered, so a plan does not
//! depend on hash iteration order.

use super::manifest::{decode_manifest, track_gc_digest, validate_digest};
use super::policy::{SweepRequest, SweepStats};
use super::uploads::decode_upload;
use super::{CAS_BLOBS, CAS_RETENTION, CAS_UPLOADS};
use eg_storage::{BlobOwner, BlobSharedWrite};
use eg_transaction::AdmittedOwnerWrite;
use redb::ReadableTable;
use std::collections::BTreeSet;

/// The manifest, upload and retention tables one plan reads.
pub(super) struct SweepTables<'a, B, U, R> {
    pub(super) blobs: &'a B,
    pub(super) uploads: &'a U,
    pub(super) retention: &'a R,
}

/// The shared-service facts a plan needs, from a write or a read.
pub(super) struct SharedFacts<'a> {
    pub(super) refcount: &'a dyn Fn(&str) -> Result<u64, String>,
    pub(super) chunk_present: &'a dyn Fn(&str) -> Result<bool, String>,
    /// Digests whose reference count row reads zero.
    pub(super) zero_refs: BTreeSet<String>,
}

/// Collect the digests of every zero reference count through a row visitor.
pub(super) fn zero_refcounts(
    for_each: impl FnOnce(&mut dyn FnMut(&str, u64) -> Result<(), String>) -> Result<(), String>,
) -> Result<BTreeSet<String>, String> {
    let mut zero = BTreeSet::new();
    for_each(&mut |digest, count| {
        if count == 0 {
            track_gc_digest(&mut zero, digest.to_string())?;
        }
        Ok(())
    })?;
    Ok(zero)
}

/// Everything one pass removes.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct SweepPlan {
    blobs: BTreeSet<String>,
    uploads: BTreeSet<u64>,
    released_refs: BTreeSet<String>,
    chunks: BTreeSet<String>,
}

impl SweepPlan {
    pub(super) fn into_chunks(self) -> BTreeSet<String> {
        self.chunks
    }
}

/// Decide what one pass at `now_ms` reclaims.
pub(super) fn plan_sweep<B, U, R>(
    tables: &SweepTables<'_, B, U, R>,
    shared: &SharedFacts<'_>,
    request: &SweepRequest,
    now_ms: u64,
) -> Result<SweepPlan, String>
where
    B: ReadableTable<&'static str, &'static [u8]>,
    U: ReadableTable<u64, &'static [u8]>,
    R: ReadableTable<&'static str, u64>,
{
    let blobs = dead_manifests(tables, shared, request, now_ms)?;
    let uploads = partition_uploads(tables.uploads, request, now_ms)?;
    let mut live = live_manifest_chunks(tables.blobs, &blobs)?;
    extend_tracked(&mut live, uploads.live_chunks)?;
    let mut candidates = orphan_manifest_chunks(tables.blobs, &blobs)?;
    extend_tracked(&mut candidates, uploads.expired_chunks)?;
    let released_refs = released_direct_refs(tables.blobs, &shared.zero_refs, request)?;
    extend_tracked(&mut candidates, released_refs.iter().cloned())?;
    let chunks = unreachable_chunks(candidates, &live, shared)?;
    Ok(SweepPlan {
        blobs,
        uploads: uploads.expired,
        released_refs,
        chunks,
    })
}

fn extend_tracked(
    set: &mut BTreeSet<String>,
    digests: impl IntoIterator<Item = String>,
) -> Result<(), String> {
    for digest in digests {
        track_gc_digest(set, digest)?;
    }
    Ok(())
}

/// Manifests of covered owners with no holder whose grace has elapsed.
fn dead_manifests<B, U, R>(
    tables: &SweepTables<'_, B, U, R>,
    shared: &SharedFacts<'_>,
    request: &SweepRequest,
    now_ms: u64,
) -> Result<BTreeSet<String>, String>
where
    B: ReadableTable<&'static str, &'static [u8]>,
    R: ReadableTable<&'static str, u64>,
{
    let mut dead = BTreeSet::new();
    for row in tables.blobs.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let digest = key.value();
        validate_digest(digest)?;
        let owner = decode_manifest(value.value())?.owner_scope;
        if request.owner().covers(&owner)
            && (shared.refcount)(digest)? == 0
            && grace_elapsed(tables.retention, digest, request, now_ms)?
        {
            track_gc_digest(&mut dead, digest.to_string())?;
        }
    }
    Ok(dead)
}

fn grace_elapsed<R>(
    retention: &R,
    digest: &str,
    request: &SweepRequest,
    now_ms: u64,
) -> Result<bool, String>
where
    R: ReadableTable<&'static str, u64>,
{
    let committed_at = retention
        .get(digest)
        .map_err(|e| e.to_string())?
        .map(|value| value.value());
    Ok(committed_at.is_some_and(|at| request.policy().grace_elapsed(at, now_ms)))
}

struct UploadPartition {
    expired: BTreeSet<u64>,
    expired_chunks: BTreeSet<String>,
    live_chunks: BTreeSet<String>,
}

/// Split uploads into abandoned (covered owner, idle past the TTL) and live.
fn partition_uploads<U>(
    uploads: &U,
    request: &SweepRequest,
    now_ms: u64,
) -> Result<UploadPartition, String>
where
    U: ReadableTable<u64, &'static [u8]>,
{
    let mut partition = UploadPartition {
        expired: BTreeSet::new(),
        expired_chunks: BTreeSet::new(),
        live_chunks: BTreeSet::new(),
    };
    for row in uploads.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let upload = decode_upload(value.value())?;
        let abandoned = request.owner().covers(upload.owner_scope())
            && request
                .policy()
                .upload_abandoned(upload.last_active_ms(), now_ms);
        let chunks = upload.into_chunks();
        if abandoned {
            partition.expired.insert(key.value());
            extend_tracked(&mut partition.expired_chunks, chunks)?;
        } else {
            extend_tracked(&mut partition.live_chunks, chunks)?;
        }
    }
    Ok(partition)
}

/// Every chunk a manifest outside `dead` still names.
fn live_manifest_chunks<B>(blobs: &B, dead: &BTreeSet<String>) -> Result<BTreeSet<String>, String>
where
    B: ReadableTable<&'static str, &'static [u8]>,
{
    let mut live = BTreeSet::new();
    for row in blobs.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        if !dead.contains(key.value()) {
            extend_tracked(&mut live, decode_manifest(value.value())?.chunks)?;
        }
    }
    Ok(live)
}

/// The chunks named by the manifests in `dead`.
fn orphan_manifest_chunks<B>(blobs: &B, dead: &BTreeSet<String>) -> Result<BTreeSet<String>, String>
where
    B: ReadableTable<&'static str, &'static [u8]>,
{
    let mut named = BTreeSet::new();
    for digest in dead {
        if let Some(value) = blobs.get(digest.as_str()).map_err(|e| e.to_string())? {
            extend_tracked(&mut named, decode_manifest(value.value())?.chunks)?;
        }
    }
    Ok(named)
}

/// Zero reference rows that name no manifest: released direct chunk references.
/// They have no owner, so only an all-owner pass takes them.
fn released_direct_refs<B>(
    blobs: &B,
    zero_refs: &BTreeSet<String>,
    request: &SweepRequest,
) -> Result<BTreeSet<String>, String>
where
    B: ReadableTable<&'static str, &'static [u8]>,
{
    let mut released = BTreeSet::new();
    if !request.owner().covers_direct_chunks() {
        return Ok(released);
    }
    for digest in zero_refs {
        if blobs
            .get(digest.as_str())
            .map_err(|e| e.to_string())?
            .is_none()
        {
            track_gc_digest(&mut released, digest.clone())?;
        }
    }
    Ok(released)
}

/// Candidates that nothing reaches: not named by a surviving manifest or upload,
/// not held directly (a chunk digest with its own holders), and still stored.
fn unreachable_chunks(
    candidates: BTreeSet<String>,
    live: &BTreeSet<String>,
    shared: &SharedFacts<'_>,
) -> Result<BTreeSet<String>, String> {
    let mut unreachable = BTreeSet::new();
    for digest in candidates {
        if !live.contains(&digest)
            && (shared.refcount)(&digest)? == 0
            && (shared.chunk_present)(&digest)?
        {
            unreachable.insert(digest);
        }
    }
    Ok(unreachable)
}

/// Sweep inside an admitted blob write: plan, then apply.
pub(super) fn sweep_rows(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    shared: &BlobSharedWrite<'_>,
    chunk_present: &dyn Fn(&str) -> Result<bool, String>,
    request: &SweepRequest,
    now_ms: u64,
) -> Result<SweepStats, String> {
    let plan = {
        let refcount = |digest: &str| shared.refcount(digest);
        let facts = SharedFacts {
            refcount: &refcount,
            chunk_present,
            zero_refs: zero_refcounts(|visit| shared.for_each_refcount(visit))?,
        };
        let tables = SweepTables {
            blobs: &wtx.open_table(CAS_BLOBS)?,
            uploads: &wtx.open_table(CAS_UPLOADS)?,
            retention: &wtx.open_table(CAS_RETENTION)?,
        };
        plan_sweep(&tables, &facts, request, now_ms)?
    };
    apply_sweep_plan(wtx, shared, &plan)
}

fn apply_sweep_plan(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    shared: &BlobSharedWrite<'_>,
    plan: &SweepPlan,
) -> Result<SweepStats, String> {
    let mut stats = SweepStats::default();
    {
        let mut blobs = wtx.open_table(CAS_BLOBS)?;
        let mut retention = wtx.open_table(CAS_RETENTION)?;
        for digest in &plan.blobs {
            let removed = blobs.remove(digest.as_str()).map_err(|e| e.to_string())?;
            stats.blobs_reclaimed += u64::from(removed.is_some());
            retention
                .remove(digest.as_str())
                .map_err(|e| e.to_string())?;
            shared.remove_refcount(digest)?;
        }
    }
    {
        let mut uploads = wtx.open_table(CAS_UPLOADS)?;
        for cursor in &plan.uploads {
            let removed = uploads.remove(*cursor).map_err(|e| e.to_string())?;
            stats.uploads_expired += u64::from(removed.is_some());
        }
    }
    for digest in &plan.released_refs {
        shared.remove_refcount(digest)?;
    }
    for digest in &plan.chunks {
        stats.chunks_reclaimed += u64::from(shared.remove_chunk(digest)?);
    }
    Ok(stats)
}
