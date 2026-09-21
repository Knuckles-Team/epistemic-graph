//! Content-addressed chunk store (CAS) — CONCEPT:EG-KG.storage.bounded-blob-memory.
//!
//! The bytes tier under the `:Media`/`:Blob` graph shape. A media file is split into
//! chunks; each chunk is stored ONCE keyed by its sha256 (intrinsic dedup), and a blob
//! is a *manifest* (the ordered chunk digests + the total length) keyed by the
//! manifest's own sha256 — so identical content yields an identical blob digest. A
//! graph node references a blob purely by that digest (`content_ref`); the chunk bytes
//! NEVER touch the inline node/edge KV.
//!
//! This is the DAG-low storage core: ONE physical owner file
//! (`{persist_dir}/blob.redb`, `eg_storage::OwnerLayout::Blob`) SEPARATE from the
//! authoritative graph shards, which keeps the manifest/chunk/refcount tables off
//! the hot graph store. Every write is an admitted transaction of the mutation
//! kernel, so durability is the kernel's — commit-before-ack, never a local
//! `Durability` this module sets.
//!
//! ## The [`ChunkStore`] seam
//!
//! Native redb-CAS is the default backend on the Pi/standard build. The SAME trait
//! fronts an S3/MinIO backend behind a SEPARATE `blob-s3` feature, so the lean build
//! links no object-store SDK; native vs S3 changes only the trait method bodies —
//! protocol frames, cursors, manifest shape and graph linkage are identical.
//!
//! ## References, retention and GC (the flagged correctness risk)
//!
//! Content addressing + dedup means one blob can be referenced by N holders across
//! graphs. A reference is a **holder row** (see [`holders`]): taking it twice is one
//! row and releasing it twice removes one, so a retried or replayed reference can
//! neither leak nor drop a count. A [`sweep`](ChunkStore::sweep) reclaims a manifest
//! only when it has no holder AND its latest commit is past the grace period, so an
//! upload between its commit and its first reference survives; it expires uploads
//! idle past their TTL; and it deletes a chunk only when no surviving manifest, no
//! surviving upload and no direct holder still reaches it (see [`gc`]).
//!
//! ## Format
//!
//! `blob.redb` files of the layout before holder rows are refused by name before
//! any writable open (see [`format`]); their data is not migrated.

use crate::mutation_batch::{
    DurabilityDomain, IncarnationId, LogicalName, MutationBatch, MutationCommitPhase,
    MutationScopeIdentity, ScopeTenantId,
};
use eg_storage::{
    BlobOwner, BlobSharedRead, BlobSharedServiceHandle, BlobSharedWrite, CasChunkRows,
    OwnedStoreHandle, PhysicalStoreIdentity, ScopedRead, StorageKernel, StoreOpenOptions,
};
use eg_transaction::{AdmittedOwnerWrite, Begin, MaintenanceBatch, MutationKernel};
use redb::{ReadableTableMetadata, TableDefinition};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

use super::engine_bodies::{EngineBody, StoredEngineBody};

pub(crate) mod format;
mod gc;
mod holders;
mod killpoint;
pub(super) mod manifest;
mod native;
mod policy;
#[cfg(feature = "blob-s3")]
mod sidecar;
#[cfg(test)]
pub(crate) mod test_support;
mod uploads;

pub use holders::{
    HolderChange, HolderId, HolderNamespace, HolderOutcome, HolderReconcile, ReconcileStats,
};
pub use manifest::{
    hex_digest, BlobManifest, CommittedBlob, BLOB_MANIFEST_VERSION, ENGINE_BLOB_OWNER_SCOPE,
};
pub(crate) use manifest::{validate_digest, MAX_BLOB_CHUNK_BYTES};
pub use policy::{
    BlobRetentionPolicy, GcOwnerScope, SweepRequest, SweepStats, DEFAULT_GC_GRACE_MS,
    DEFAULT_UPLOAD_TTL_MS,
};

/// Default chunk size: 2 MiB (in the 1–4 MB band the streaming protocol targets;
/// matches the spike). The pre-EG-071 FIXED stride; content-defined chunking
/// ([`crate::server::blob::cdc`], CONCEPT:EG-KG.storage.backward-manifest-read) now targets this as its AVERAGE.
/// Still the default chunk size of the wire upload cursor (`BlobBegin`).
pub const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// The content-addressed chunk + blob + reference store.
///
/// Streaming contract (bounded memory): callers feed chunks ONE AT A TIME via
/// [`put_chunk`](ChunkStore::put_chunk) as they arrive off the wire, then
/// [`put_manifest`](ChunkStore::put_manifest) assembles the manifest from the
/// accumulated digests. The store never buffers the whole blob — at most one
/// chunk is resident per call. Fetch mirrors it: [`get_chunk`](ChunkStore::get_chunk)
/// pulls one chunk at a time by index off the manifest.
pub trait ChunkStore: Send + Sync {
    /// Store one chunk by its content digest. Returns `(digest, was_new)`;
    /// `was_new == false` means an identical chunk was already present (dedup).
    fn put_chunk(&self, bytes: &[u8]) -> Result<(String, bool), String>;

    /// Read one chunk back by its digest. `Ok(None)` if absent.
    fn get_chunk(&self, digest: &str) -> Result<Option<Vec<u8>>, String>;

    /// Store a blob manifest keyed by `blob_digest` and restart its grace period.
    /// Idempotent in content — re-storing an identical manifest rewrites the same
    /// row (the digest is the manifest hash). Every chunk it names must be stored.
    fn put_manifest(&self, blob_digest: &str, manifest: &BlobManifest) -> Result<(), String>;

    /// Read a blob manifest back by its digest. `Ok(None)` if absent.
    fn get_manifest(&self, blob_digest: &str) -> Result<Option<BlobManifest>, String>;

    /// Take one in-process engine reference (the counted engine holder). The
    /// digest must be a stored manifest or chunk. Returns the new total count.
    fn incref(&self, blob_digest: &str) -> Result<u64, String>;

    /// Give back one in-process engine reference. Refuses underflow of the
    /// engine holder's own count. Returns the new total count.
    fn decref(&self, blob_digest: &str) -> Result<u64, String>;

    /// Current total reference count of a digest (0 if never referenced).
    fn refcount(&self, blob_digest: &str) -> Result<u64, String>;

    /// Mark-and-sweep GC over every owner under the default retention policy.
    fn sweep(&self) -> Result<SweepStats, String>;

    /// Total distinct chunks currently stored (dedup observability).
    fn chunk_count(&self) -> Result<u64, String>;

    /// Total blobs (manifests) currently stored.
    fn blob_count(&self) -> Result<u64, String>;

    /// Universal subordinate-domain version. Backends that cannot atomically
    /// persist native state plus MutationBatch metadata fail closed.
    fn mutation_version(&self, _tenant: &str, _graph: &str) -> Result<u64, String> {
        Err("blob backend does not provide atomic MutationBatch coordination".to_string())
    }

    /// One garbage-collection pass as a caller's MutationBatch, deciding at
    /// `committed_at_ms` under `request`'s owner scope and retention policy.
    fn sweep_batch(
        &self,
        _request: &SweepRequest,
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<SweepStats, String> {
        Err("blob backend does not provide atomic MutationBatch coordination".to_string())
    }

    fn begin_upload_batch(
        &self,
        _cursor: u64,
        _chunk_size: u32,
        _owner_scope: &str,
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<u64, String> {
        Err("blob backend does not provide durable upload coordination".to_string())
    }

    fn put_upload_chunk_batch(
        &self,
        _cursor: u64,
        _bytes: &[u8],
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<(String, u32), String> {
        Err("blob backend does not provide durable upload coordination".to_string())
    }

    fn load_upload(&self, _cursor: u64) -> Result<Option<BlobManifest>, String> {
        Err("blob backend does not provide durable upload coordination".to_string())
    }

    fn commit_upload_batch(
        &self,
        _cursor: u64,
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<String, String> {
        Err("blob backend does not provide durable upload coordination".to_string())
    }

    /// The highest upload cursor id ever begun; a restarted allocator starts above it.
    fn upload_cursor_high_water(&self) -> Result<u64, String> {
        Err("blob backend does not provide durable upload coordination".to_string())
    }

    /// Take or release one named holder's reference, idempotently.
    fn holder_batch(
        &self,
        _change: &HolderChange,
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<HolderOutcome, String> {
        Err("blob backend does not provide holder-scoped references".to_string())
    }

    /// Release the orphaned holders of one namespace.
    fn reconcile_holders_batch(
        &self,
        _request: &HolderReconcile,
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<ReconcileStats, String> {
        Err("blob backend does not provide holder-scoped references".to_string())
    }

    /// Copy a bounded set of connector-pack bodies into the engine-owned CAS
    /// and acquire their idempotent pack holders in one blob-owner write.
    fn put_engine_bodies(
        &self,
        _tenant_id: &str,
        _bodies: &[EngineBody],
        _committed_at_ms: u64,
    ) -> Result<Vec<StoredEngineBody>, String> {
        Err("blob backend does not provide engine-owned body batches".to_string())
    }
}

// ── native redb CAS ─────────────────────────────────────────────────────────

// Owner tables in `blob.redb`, all OFF the inline node/edge KV. `cas_chunks`
// (chunk digest -> bytes) and `cas_refcount` (digest -> total holder count) are
// the shared-service tables reached through `BlobSharedWrite`.
//   cas_blobs     : blob digest -> manifest msgpack
//   cas_uploads   : upload cursor -> durable upload msgpack
//   cas_holders   : (digest, holder) -> holder row msgpack
//   cas_retention : blob digest -> latest manifest commit ms (GC grace clock)
//   cas_counters  : name -> u64 (the upload cursor high-water mark)
const CAS_BLOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");
const CAS_UPLOADS: TableDefinition<u64, &[u8]> = TableDefinition::new("cas_uploads");
const CAS_HOLDERS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("cas_holders");
const CAS_RETENTION: TableDefinition<&str, u64> = TableDefinition::new("cas_retention");
const CAS_COUNTERS: TableDefinition<&str, u64> = TableDefinition::new("cas_counters");

/// Where chunk bytes are authoritative. The object-store backend keeps chunk
/// bytes outside this file, so its sidecar cannot prove a chunk row exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkAuthority {
    /// `cas_chunks` rows in this file are the chunk bytes.
    Local,
    /// Chunk bytes live in an external object store.
    #[cfg(feature = "blob-s3")]
    External,
}

/// Chunks to flush per group commit (CONCEPT:EG-KG.storage.bounded-blob-memory — bounded memory). At the
/// 2 MiB default chunk size this is a ~64 MiB window: at most this many chunk bodies
/// are resident before ONE admitted mutation writes the whole group, so peak RSS is
/// bounded by the GROUP WINDOW, NOT the blob size. (One admitted mutation per chunk
/// bounds RAM too but collapses throughput with an fsync per chunk; group-commit is
/// the cadence `redb_backend.rs` already uses.) Tunable via
/// `EPISTEMIC_GRAPH_BLOB_GROUP_CHUNKS`.
const DEFAULT_GROUP_CHUNKS: usize = 32;

/// Default bound on this store's redb page cache (CONCEPT:EG-KG.storage.bounded-blob-memory).
/// The group window bounds the STAGED chunk bodies, but the CAS stores multi-MB chunk
/// VALUES, so redb's 1 GiB default page cache is what actually lets resident memory
/// track the blob size. Evicting past this cap is the real bound. 128 MiB is about 2x
/// the 64 MiB group window. Tunable via `EPISTEMIC_GRAPH_BLOB_CACHE_BYTES`, validated by
/// [`StoreOpenOptions::with_cache_bytes`].
const DEFAULT_CACHE_BYTES: usize = 128 * 1024 * 1024;

/// The open options every `RedbChunkStore` handle runs under: the page-cache cap above,
/// or the operator's `EPISTEMIC_GRAPH_BLOB_CACHE_BYTES`. A value that does not parse or
/// is outside the kernel's accepted range fails the open rather than silently running
/// uncapped.
fn blob_open_options() -> Result<StoreOpenOptions, String> {
    let cache_bytes = match std::env::var("EPISTEMIC_GRAPH_BLOB_CACHE_BYTES") {
        Ok(raw) => raw
            .trim()
            .parse::<usize>()
            .map_err(|_| "EPISTEMIC_GRAPH_BLOB_CACHE_BYTES must be a byte count".to_string())?,
        Err(_) => DEFAULT_CACHE_BYTES,
    };
    StoreOpenOptions::default().with_cache_bytes(cache_bytes)
}

/// The open chunk group: up to `group` chunk bodies staged in memory, flushed as ONE
/// admitted maintenance mutation. `AdmittedMutation` borrows the mutation kernel and so
/// cannot be parked in a field, hence a buffered group rather than a held-open
/// transaction; N inserts still ride ONE commit, bounding memory (≤ group chunks
/// resident) and fsync rate (one per group). Digest-keyed, preserving the old
/// within-transaction dedup.
struct ChunkBatch {
    /// Chunk bodies staged for the current group, keyed by content digest.
    pending: HashMap<String, Vec<u8>>,
    /// Group window: flush after this many buffered chunks.
    group: usize,
}

/// A bound serving scope. `OwnedStoreHandle` is not `Clone` (it IS a capability), so
/// the cache owns one per scope and hands out `Arc` clones.
type BlobHandle = Arc<OwnedStoreHandle<BlobOwner>>;

/// Operator-facing identity of the ONE physical `blob.redb` owner file — the physical
/// authority boundary, independent of any logical serving scope.
pub(crate) const BLOB_PHYSICAL_STORE: &str = "epistemic-graph:blob";

/// Authenticate and bind ONE logical serving scope on `blob.redb`. The proof bytes are
/// the composition root's; this module supplies only the identity and the layout.
fn bind_scope(kernel: &StorageKernel, scope: &MutationScopeIdentity) -> Result<BlobHandle, String> {
    crate::redb_store::shard::bind_scope::<BlobOwner>(kernel, scope)
}

/// The native blob-domain scope identity for one (tenant, resource) pair.
fn blob_scope_identity(
    tenant: ScopeTenantId,
    resource: &str,
) -> Result<MutationScopeIdentity, String> {
    MutationScopeIdentity::native(
        tenant,
        DurabilityDomain::BlobStore,
        LogicalName::new(resource.to_string())?,
        IncarnationId::new(crate::server::mutation_batch::COMPILED_BATCH_INCARNATION)?,
    )
}

/// The store's BOOTSTRAP and serving scope: what `open` binds and what every
/// cross-scope read and caller-less write runs on. `RedbChunkStore` serves MANY
/// tenant/graph scopes discovered later, one per call (`commit_native_batch`'s
/// `batch.identity`), each bound lazily by `scope_handle`; this reserved resource
/// name is never a real caller's scope. Its `IncarnationId` reuses
/// `COMPILED_BATCH_INCARNATION` rather than a store-local constant because real
/// per-call batches arrive stamped with that SAME constant, and the kernel compares
/// the full identity (incarnation included) when re-binding, failing closed on any
/// mismatch.
fn ledger_bootstrap_identity() -> Result<MutationScopeIdentity, String> {
    blob_scope_identity(ScopeTenantId::system(), "blob-ledger-root")
}

/// The row set one staged chunk group writes, as it appears in the durable batch id:
/// the content digest of the group's own chunk digests, in sorted order. Two groups
/// carrying different chunks are two different maintenance writes.
fn group_subject(group: &HashMap<String, Vec<u8>>) -> String {
    let mut digests: Vec<&str> = group.keys().map(String::as_str).collect();
    digests.sort_unstable();
    let mut hasher = Sha256::new();
    for digest in digests {
        hasher.update(digest.as_bytes());
        hasher.update([0u8]);
    }
    hex::encode(hasher.finalize())
}

/// Native content-addressed store over ONE physical owner file
/// (`{persist_dir}/blob.redb`, `eg_storage::OwnerLayout::Blob`) served through the two
/// kernels. Chunk writes accumulate in memory and flush every [`DEFAULT_GROUP_CHUNKS`]
/// chunks as ONE admitted maintenance mutation, so an acked group survives a crash AND
/// peak memory is bounded by the group window (not the file size). Durability is the
/// KERNEL's — every admitted write is commit-before-ack — so this store neither sets nor
/// can set a redb `Durability`. Manifest/refcount/sweep/read operations flush the open
/// chunk group first (one writer at a time), then run their own admitted write.
pub struct RedbChunkStore {
    kernel: StorageKernel,
    mutations: MutationKernel,
    /// Independently authenticated authority for the physical shared-service
    /// chunk and refcount tables in this blob owner file.
    shared: BlobSharedServiceHandle,
    /// Bootstrap AND serving scope: cross-scope reads and caller-less writes run on it.
    bootstrap: BlobHandle,
    /// Serving scopes, keyed by `identity.binding_digest().to_hex()`.
    scopes: parking_lot::Mutex<HashMap<String, BlobHandle>>,
    batch: parking_lot::Mutex<ChunkBatch>,
    chunks: ChunkAuthority,
}

/// Open (creating if absent) `{persist_dir}/blob.redb` as ONE physical owner file
/// under `eg_storage::OwnerLayout::Blob`. `create_owner` materializes the WHOLE
/// declared census plus the ledger, so CAS rows land in the SAME transaction as the
/// ledger's idempotency/OCC/outbox rows, which is what `commit_native_batch` needs
/// for atomicity. An existing file of the previous blob layout is refused by name
/// first, read-only (see [`format`]).
///
/// The handle runs under [`blob_open_options`]: a capped redb page cache. The
/// kernel migration first opened this file with redb's 1 GiB default because
/// `eg-storage` had no cache injection point then; `StoreOpenOptions` is that
/// injection point, and without it resident memory tracked the blob size (a 256 MB
/// blob grew peak RSS by ~546 MB) despite the group window.
fn open_store(persist_dir: &str, chunks: ChunkAuthority) -> Result<RedbChunkStore, String> {
    std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
    let path = std::path::Path::new(persist_dir).join("blob.redb");
    let physical = PhysicalStoreIdentity::new(BLOB_PHYSICAL_STORE)?;
    let options = blob_open_options()?;
    let kernel = if path.exists() {
        format::refuse_predecessor(&path)?;
        StorageKernel::open_owner_with::<BlobOwner>(&path, physical, None, options)
    } else {
        StorageKernel::create_owner_with::<BlobOwner>(&path, physical, None, options)
    }?;
    let authority = crate::store_authority::process_authority();
    let shared_proof = authority.proof();
    let shared = kernel.authenticate_blob_shared_service(
        authority.as_ref(),
        authority.principal().to_string(),
        &shared_proof,
    )?;
    let (kernel, authority) = kernel.into_read_and_mutation_authority()?;
    let mutations = MutationKernel::new(authority);
    let bootstrap = bind_scope(&kernel, &ledger_bootstrap_identity()?)?;
    mutations.bootstrap_ledger(&bootstrap)?;
    let group = std::env::var("EPISTEMIC_GRAPH_BLOB_GROUP_CHUNKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_GROUP_CHUNKS);
    Ok(RedbChunkStore {
        kernel,
        mutations,
        shared,
        bootstrap,
        scopes: parking_lot::Mutex::new(HashMap::new()),
        batch: parking_lot::Mutex::new(ChunkBatch {
            pending: HashMap::new(),
            group,
        }),
        chunks,
    })
}

/// The shared-service write over an admitted blob write, under the engine principal.
fn shared_write<'a>(
    wtx: &AdmittedOwnerWrite<'a, BlobOwner>,
    service: &BlobSharedServiceHandle,
) -> Result<BlobSharedWrite<'a>, String> {
    wtx.blob_shared_write(service, crate::store_authority::ENGINE_PRINCIPAL)
}

/// Whether a chunk is stored, as far as this file can prove: a local store asks
/// its chunk rows; an external-chunk sidecar cannot, and answers yes.
fn chunk_is_stored(
    chunks: ChunkAuthority,
    shared: &BlobSharedWrite<'_>,
    digest: &str,
) -> Result<bool, String> {
    match chunks {
        ChunkAuthority::Local => shared.chunk_present(digest),
        #[cfg(feature = "blob-s3")]
        ChunkAuthority::External => Ok(true),
    }
}

/// `incref`/`decref` as one admitted owner maintenance mutation (RF-RULING-005 —
/// it carries no caller identity).
fn maintain_counted_reference(
    store: &RedbChunkStore,
    digest: &str,
    delta: i64,
) -> Result<u64, String> {
    store.flush_chunks()?;
    validate_digest(digest)?;
    let at = crate::server::dispatch::authoritative_now_ms();
    store.maintain("blob_adjust_ref_v1", digest, at, |wtx| {
        adjust_counted_reference(wtx, &store.shared, digest, delta, at)
    })
}

impl Drop for RedbChunkStore {
    /// Flush a half-full chunk group on drop so an in-flight upload's staged chunks
    /// aren't discarded (they'd otherwise be re-uploaded; no data loss, but wasteful).
    /// Best-effort — a commit error at drop is ignored.
    fn drop(&mut self) {
        let _ = self.flush_chunks();
    }
}

impl RedbChunkStore {
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        open_store(persist_dir, ChunkAuthority::Local)
    }

    /// Open an in-memory-only CAS backed by a temp dir (tests). The dir is left to
    /// the OS temp reaper; callers that want cleanup pass their own dir to `open`.
    #[cfg(test)]
    pub fn open_temp() -> Result<Self, String> {
        let dir = crate::server::unique_temp_dir("eg-blob-cas");
        Self::open(&dir.to_string_lossy())
    }

    /// Atomically store one content-addressed chunk, take one counted engine
    /// reference to it, and commit the native MutationBatch result in the same redb
    /// transaction: the direct-artifact fast path, whose reference is keyed by the
    /// chunk digest itself and has no manifest row.
    pub fn put_chunk_ref_batch(
        &self,
        bytes: &[u8],
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(String, bool, u64), String> {
        self.flush_chunks()?;
        let digest = manifest::chunk_digest(bytes)?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            let shared =
                wtx.blob_shared_write(&self.shared, crate::store_authority::ENGINE_PRINCIPAL)?;
            let was_new = shared.insert_chunk_if_absent(&digest, bytes)?;
            let refcount =
                adjust_counted_reference(wtx, &self.shared, &digest, 1, committed_at_ms)?;
            Ok((digest.clone(), was_new, refcount))
        })
    }

    /// Move the counted engine reference of `blob_digest` by `delta` as a caller's
    /// MutationBatch (the direct-artifact compensation path).
    pub fn adjust_ref_batch(
        &self,
        blob_digest: &str,
        delta: i64,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<u64, String> {
        self.flush_chunks()?;
        validate_digest(blob_digest)?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            adjust_counted_reference(wtx, &self.shared, blob_digest, delta, committed_at_ms)
        })
    }

    /// The cross-scope owner-row snapshot used for manifests and uploads.
    fn read(&self) -> Result<ScopedRead<'_, BlobOwner>, String> {
        self.kernel.read_scope(&self.bootstrap)
    }

    /// The independently authenticated read used for the physical shared CAS
    /// chunk and refcount tables.
    fn shared_read(&self) -> Result<BlobSharedRead, String> {
        self.kernel
            .read_blob_shared(&self.shared, crate::store_authority::ENGINE_PRINCIPAL)
    }

    /// One (tenant, graph) serving scope: bound on FIRST use, cached for every later
    /// call. Replaces `ensure_scope_bound` — the cache, not a re-bind per call, is now
    /// what makes repeat use idempotent.
    fn scope_handle(&self, identity: &MutationScopeIdentity) -> Result<BlobHandle, String> {
        let key = identity.binding_digest().to_hex();
        if let Some(handle) = self.scopes.lock().get(&key) {
            return Ok(Arc::clone(handle));
        }
        let handle = bind_scope(&self.kernel, identity)?;
        self.scopes.lock().insert(key, Arc::clone(&handle));
        Ok(handle)
    }

    /// Whether `digest` is a committed `cas_chunks` row; with the staged group this is
    /// exactly the old in-transaction `was_new` answer.
    fn chunk_present(&self, digest: &str) -> Result<bool, String> {
        self.shared_read()?.chunk_present(digest)
    }

    /// Write the whole staged chunk group as ONE admitted maintenance mutation and empty
    /// it. A commit failure surfaces here (the group's chunks did NOT land).
    fn commit_group(&self, batch: &mut ChunkBatch) -> Result<(), String> {
        if batch.pending.is_empty() {
            return Ok(());
        }
        let group = std::mem::take(&mut batch.pending);
        let at = crate::server::dispatch::authoritative_now_ms();
        self.maintain("blob_chunk_group_v1", &group_subject(&group), at, |wtx| {
            let rows: Vec<(&str, &[u8])> = group
                .iter()
                .map(|(digest, bytes)| (digest.as_str(), bytes.as_slice()))
                .collect();
            shared_write(wtx, &self.shared)?.insert_chunks_if_absent(&rows)?;
            Ok(())
        })
    }

    /// Commit any staged chunk group. Called before every other operation so exactly one
    /// writer is live and a reader sees every chunk.
    fn flush_chunks(&self) -> Result<(), String> {
        let mut batch = self.batch.lock();
        self.commit_group(&mut batch)
    }

    /// One plain (non-`_batch`) blob write as an owner MAINTENANCE mutation
    /// (RF-RULING-005) on the bootstrap scope: no caller identity, but ledgered, fenced
    /// and version-bumping — no un-ledgered owner-write path exists any more.
    ///
    /// `subject` is the row set the write acts on (a blob digest, a chunk group), and
    /// the version the batch fences on is resolved INSIDE the write transaction by
    /// `admit_current`. Reading it from a snapshot first let two concurrent
    /// `blob_adjust_ref_v1` calls at one observed version build byte-identical batches;
    /// the second then replayed the first's record, LOSING an increment — and a lost
    /// increment is a premature delete of a live chunk.
    fn maintain<T, F>(&self, event: &str, subject: &str, at_ms: u64, apply: F) -> Result<T, String>
    where
        T: serde::Serialize + DeserializeOwned,
        F: FnOnce(&AdmittedOwnerWrite<'_, BlobOwner>) -> Result<T, String>,
    {
        let write = MaintenanceBatch::new(DurabilityDomain::BlobStore, event, subject);
        let bootstrap = self.bootstrap.as_ref();
        let (txn, batch, begun) = self.mutations.admit_current(bootstrap, |version| {
            write.for_scope_version(bootstrap, version)
        })?;
        self.complete_write(bootstrap, txn, &batch, begun, at_ms, apply)
    }

    /// One caller-identified `*_batch` write, admitted under the batch's OWN serving
    /// scope: `batch.identity` is this call's real (tenant, graph) pair, built by the
    /// caller via `CompileBatch`/`compile_opaque_method`, bound on first use.
    fn commit_native_batch<T, F>(
        &self,
        batch: &MutationBatch,
        committed_at_ms: u64,
        apply: F,
    ) -> Result<T, String>
    where
        T: serde::Serialize + DeserializeOwned,
        F: FnOnce(&AdmittedOwnerWrite<'_, BlobOwner>) -> Result<T, String>,
    {
        let owner = self.scope_handle(&batch.identity)?;
        let owner = owner.as_ref();
        let (txn, begun) = self.mutations.admit(owner, batch)?;
        self.complete_write(owner, txn, batch, begun, committed_at_ms, apply)
    }

    /// Stage an ALREADY admitted batch's owner rows, persist the exact verdict as the
    /// replayable result and commit — ONE transaction, so a CAS row and its terminal
    /// MutationBatch metadata can never disagree. Shared by both admission classes;
    /// only the admission itself differs, which is why it is the caller's.
    ///
    /// The four [`MutationCommitPhase`] boundaries are kill points (see
    /// [`killpoint`]); failing at one before commit drops the uncommitted write.
    fn complete_write<T, F>(
        &self,
        owner: &OwnedStoreHandle<BlobOwner>,
        write: eg_transaction::AdmittedMutation<'_, BlobOwner>,
        batch: &MutationBatch,
        begun: Begin,
        at_ms: u64,
        apply: F,
    ) -> Result<T, String>
    where
        T: serde::Serialize + DeserializeOwned,
        F: FnOnce(&AdmittedOwnerWrite<'_, BlobOwner>) -> Result<T, String>,
    {
        let source_version = match begun {
            // Terminally committed already: the recorded verdict IS the answer, and
            // re-applying it would double the effect.
            Begin::Replay(record) => {
                let bytes = record
                    .result_msgpack
                    .as_deref()
                    .ok_or_else(|| "committed blob MutationBatch has no result".to_string())?;
                let replayed = manifest::decode_blob_value(bytes)?;
                self.mutations.commit(write, batch)?;
                return Ok(replayed);
            }
            Begin::Apply { source_version } => source_version,
        };
        killpoint::reach(batch, MutationCommitPhase::BeforeRows)?;
        let result = match stage_owner_rows(&write, owner, batch, apply)? {
            Ok(value) => value,
            Err(error) => {
                write.abort()?;
                return Err(error);
            }
        };
        killpoint::reach(batch, MutationCommitPhase::AfterRowsBeforeMetadata)?;
        let encoded = rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())?;
        self.mutations
            .finish(&write, batch, Some(encoded), at_ms, source_version)?;
        killpoint::reach(batch, MutationCommitPhase::BeforeCommit)?;
        self.mutations.commit(write, batch)?;
        killpoint::reach(batch, MutationCommitPhase::AfterCommitBeforeAck)?;
        Ok(result)
    }
}

/// Apply one write's owner rows, closing the owner capability either way. The
/// outer error is the capability's; the inner one is the apply's refusal, which
/// the caller answers by aborting the whole write.
fn stage_owner_rows<T, F>(
    write: &eg_transaction::AdmittedMutation<'_, BlobOwner>,
    owner: &OwnedStoreHandle<BlobOwner>,
    batch: &MutationBatch,
    apply: F,
) -> Result<Result<T, String>, String>
where
    F: FnOnce(&AdmittedOwnerWrite<'_, BlobOwner>) -> Result<T, String>,
{
    let owner_write = write.owner_rows(owner, batch)?;
    let staged = apply(&owner_write);
    // Dropping the owner capability unfinished poisons the write; always close it.
    owner_write.finish_owner()?;
    Ok(staged)
}

/// Move the counted engine holder of `digest` by `delta` and the digest's total
/// reference count with it, in the caller's admitted write. With
/// [`holders::apply_holder_change`] this is the only writer of `cas_refcount`, so the
/// count always equals the sum of the digest's holder rows.
fn adjust_counted_reference(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    service: &BlobSharedServiceHandle,
    digest: &str,
    delta: i64,
    at_ms: u64,
) -> Result<u64, String> {
    let shared = shared_write(wtx, service)?;
    holders::adjust_counted_holder_row(wtx, &shared, digest, delta, at_ms)?;
    shared.adjust_refcount(digest, delta)
}

/// Per-test peak-RSS window shared by the blob bounded-memory tests (see its module doc).
#[cfg(test)]
pub(super) mod peak_rss;

#[cfg(test)]
mod tests;
