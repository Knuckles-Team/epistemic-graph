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
//! ## Refcount GC (the flagged correctness risk)
//!
//! Content addressing + dedup means one blob can be referenced by N `:Media` nodes
//! across graphs. So a blob carries a refcount: referencing a blob [`incref`]s it,
//! removing a reference [`decref`]s it, and a [`sweep`] reclaims every blob whose
//! refcount has fallen to zero — deleting its manifest AND every chunk that no
//! *surviving* blob still lists. Deleting one `:Media` therefore never unlinks chunks
//! another blob shares (proven by the GC tests). The chunk liveness set is recomputed
//! from the surviving manifests at sweep time, so a chunk shared by a live blob is kept.

use crate::mutation_batch::{
    IncarnationId, LogicalName, MutationBatch, MutationDomain, MutationOperation,
    MutationRequestContext, MutationScopeIdentity, MutationSurface, TenantId, VersionExpectation,
    MUTATION_BATCH_VERSION,
};
use eg_storage::{BlobOwner, OwnedStoreHandle, PhysicalStoreIdentity, ScopedRead, StorageKernelV1};
use eg_transaction::{AdmittedOwnerWrite, Begin, MutationKernelV1};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Default chunk size: 2 MiB (in the 1–4 MB band the streaming protocol targets;
/// matches the spike). The pre-EG-071 FIXED stride; content-defined chunking
/// ([`crate::server::blob::cdc`], CONCEPT:EG-KG.storage.backward-manifest-read) now targets this as its AVERAGE.
/// Still the default chunk size of the wire upload cursor (`BlobBegin`).
pub const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;
pub(crate) const MAX_BLOB_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const MAX_BLOB_MANIFEST_BYTES: usize = 128 * 1024 * 1024;
const MAX_BLOB_MANIFEST_ITEMS: usize = 3_000_000;
const MAX_BLOB_CHUNKS: usize = 1_000_000;
const MAX_BLOB_GC_TRACKED_DIGESTS: usize = 1_000_000;

pub(super) fn validate_digest(digest: &str) -> Result<(), String> {
    if digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("blob digest is invalid".to_string())
    }
}

fn track_gc_digest(set: &mut HashSet<String>, digest: String) -> Result<(), String> {
    set.insert(digest);
    if set.len() > MAX_BLOB_GC_TRACKED_DIGESTS {
        Err("blob garbage collection exceeds resource limits".to_string())
    } else {
        Ok(())
    }
}

/// Bound one chunk body and return its content address.
fn chunk_digest(bytes: &[u8]) -> Result<String, String> {
    if bytes.len() > MAX_BLOB_CHUNK_BYTES {
        return Err("blob chunk exceeds resource limits".to_string());
    }
    Ok(hex_digest(bytes))
}

fn blob_msgpack_limits() -> eg_types::msgpack::MsgpackLimits {
    eg_types::msgpack::MsgpackLimits::new(
        MAX_BLOB_MANIFEST_BYTES,
        MAX_BLOB_MANIFEST_ITEMS,
        eg_types::msgpack::DEFAULT_MAX_DEPTH,
    )
}

fn decode_blob_value<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(bytes, blob_msgpack_limits())
        .map_err(|_| "blob metadata is invalid or exceeds resource limits".to_string())
}

fn decode_manifest(bytes: &[u8]) -> Result<BlobManifest, String> {
    let manifest: BlobManifest = decode_blob_value(bytes)?;
    manifest.validate()?;
    Ok(manifest)
}

/// Encode + bound-check a manifest and prove `blob_digest` IS its content address.
fn encode_manifest(blob_digest: &str, manifest: &BlobManifest) -> Result<Vec<u8>, String> {
    manifest.validate()?;
    let bytes = rmp_serde::to_vec_named(manifest).map_err(|e| e.to_string())?;
    eg_types::msgpack::validate_single_value(&bytes, blob_msgpack_limits())
        .map_err(|_| "blob manifest exceeds resource limits".to_string())?;
    validate_digest(blob_digest)?;
    if blob_digest != hex_digest(&bytes) {
        return Err("blob manifest digest does not match its content".to_string());
    }
    Ok(bytes)
}

/// Saturating signed adjustment of a reference count (plain + `_batch` paths).
fn apply_delta(current: u64, delta: i64) -> u64 {
    match u64::try_from(delta) {
        Ok(up) => current.saturating_add(up),
        Err(_) => current.saturating_sub(delta.unsigned_abs()),
    }
}

/// Current manifest of a content-addressed blob: the ordered chunk digests,
/// exact per-chunk lengths, total length, and opaque owner. Serialized to
/// MessagePack; the blob digest is the SHA-256 of those bytes.
///
/// Only this explicitly versioned shape is accepted. Retired fixed-stride or
/// unowned manifests must be migrated offline before the engine starts; the live
/// serving path never guesses missing boundaries or authority.
pub const BLOB_MANIFEST_VERSION: u16 = 2;
pub const ENGINE_BLOB_OWNER_SCOPE: &str = "engine-internal";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobManifest {
    pub schema_version: u16,
    /// Verified tenant+principal ownership, or the reserved engine-internal scope.
    pub owner_scope: String,
    /// Hex sha256 chunk digests, in file order.
    pub chunks: Vec<String>,
    /// Exact per-chunk byte length, parallel to `chunks`.
    pub chunk_lens: Vec<u32>,
    /// Total length of the assembled blob in bytes.
    pub len: u64,
    /// Requested upload/chunker size. Zero denotes the native content-defined
    /// chunker default; boundaries are always read from `chunk_lens`.
    pub chunk_size: u32,
}

impl BlobManifest {
    pub(super) fn validate(&self) -> Result<(), String> {
        if self.schema_version != BLOB_MANIFEST_VERSION
            || self.owner_scope.is_empty()
            || self.owner_scope.len() > 256
            || self.chunks.len() > MAX_BLOB_CHUNKS
            || self.chunks.iter().any(|digest| {
                digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            return Err("blob manifest is invalid or exceeds resource limits".to_string());
        }
        if self.chunks.is_empty() {
            return if self.len == 0 && self.chunk_lens.is_empty() {
                Ok(())
            } else {
                Err("blob manifest is inconsistent".to_string())
            };
        }
        if self.chunk_lens.len() != self.chunks.len()
            || self.chunk_size as usize > MAX_BLOB_CHUNK_BYTES
            || self
                .chunk_lens
                .iter()
                .any(|length| *length == 0 || *length as usize > MAX_BLOB_CHUNK_BYTES)
        {
            return Err("blob manifest is inconsistent".to_string());
        }
        let total = self
            .chunk_lens
            .iter()
            .try_fold(0u64, |sum, length| sum.checked_add(*length as u64));
        if total != Some(self.len) {
            return Err("blob manifest is inconsistent".to_string());
        }
        Ok(())
    }

    /// Exact per-chunk byte lengths, in file order.
    pub fn chunk_lengths(&self) -> Vec<u32> {
        self.chunk_lens.clone()
    }

    /// Per-chunk `(offset, len)` boundaries, in file order — the running prefix sum
    /// of [`chunk_lengths`](Self::chunk_lengths). For random-access/seek over a blob
    /// (CONCEPT:EG-KG.storage.backward-manifest-read variable boundaries).
    pub fn chunk_offsets(&self) -> Vec<(u64, u32)> {
        let mut out = Vec::with_capacity(self.chunks.len());
        let mut off = 0u64;
        for l in self.chunk_lengths() {
            out.push((off, l));
            off += l as u64;
        }
        out
    }
}

/// Hex sha256 of `bytes` — the content address of a chunk or a manifest.
pub fn hex_digest(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Outcome of committing an upload cursor: the content address of the whole blob
/// plus the manifest needed to build the `:Blob` graph shape.
#[derive(Debug, Clone)]
pub struct CommittedBlob {
    pub digest: String,
    pub manifest: BlobManifest,
}

/// The content-addressed chunk + blob + refcount store.
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

    /// Store a blob manifest keyed by `blob_digest`. Idempotent — re-storing an
    /// identical manifest is a no-op (the digest is the manifest hash).
    fn put_manifest(&self, blob_digest: &str, manifest: &BlobManifest) -> Result<(), String>;

    /// Read a blob manifest back by its digest. `Ok(None)` if absent.
    fn get_manifest(&self, blob_digest: &str) -> Result<Option<BlobManifest>, String>;

    /// Increment a blob's reference count (a `:Media` node now references it).
    /// Creates the entry at 1 on first reference. Returns the new count.
    fn incref(&self, blob_digest: &str) -> Result<u64, String>;

    /// Decrement a blob's reference count (a `:Media` reference was removed).
    /// Saturates at 0; a blob at 0 is eligible for the next [`sweep`](ChunkStore::sweep).
    /// Returns the new count.
    fn decref(&self, blob_digest: &str) -> Result<u64, String>;

    /// Current reference count of a blob (0 if never referenced).
    fn refcount(&self, blob_digest: &str) -> Result<u64, String>;

    /// Mark-and-sweep GC. Deletes every blob whose refcount is 0 (its manifest +
    /// its refcount entry) and every chunk no *surviving* blob still lists.
    /// Returns `(blobs_reclaimed, chunks_reclaimed)`. Safe to run concurrently
    /// with reads of live blobs — only zero-ref blobs and their now-orphan chunks
    /// are touched.
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

    fn commit_cursor_batch(
        &self,
        _batch: &MutationBatch,
        _cursor: u64,
        _committed_at_ms: u64,
    ) -> Result<u64, String> {
        Err("blob backend does not provide atomic MutationBatch coordination".to_string())
    }

    fn put_chunk_batch(
        &self,
        _bytes: &[u8],
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<(String, bool), String> {
        Err("blob backend does not provide atomic MutationBatch coordination".to_string())
    }

    /// Atomically store one content-addressed chunk, acquire its reference, and
    /// commit the native MutationBatch result in the same redb transaction.
    fn put_chunk_ref_batch(
        &self,
        _bytes: &[u8],
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<(String, bool, u64), String> {
        Err("blob backend does not provide atomic chunk/reference coordination".to_string())
    }

    fn put_manifest_batch(
        &self,
        _blob_digest: &str,
        _manifest: &BlobManifest,
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<String, String> {
        Err("blob backend does not provide atomic MutationBatch coordination".to_string())
    }

    fn adjust_ref_batch(
        &self,
        _blob_digest: &str,
        _delta: i64,
        _batch: &MutationBatch,
        _committed_at_ms: u64,
    ) -> Result<u64, String> {
        Err("blob backend does not provide atomic MutationBatch coordination".to_string())
    }

    fn sweep_batch(
        &self,
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
}

/// What a [`ChunkStore::sweep`] reclaimed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepStats {
    pub blobs_reclaimed: u64,
    pub chunks_reclaimed: u64,
}

// ── native redb CAS ─────────────────────────────────────────────────────────

use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};

// Three tables, all in `blob.redb`, all OFF the inline node/edge KV:
//   cas_chunks   : chunk_digest(hex) -> chunk bytes        (DEDUP happens here)
//   cas_blobs    : blob_digest(hex)  -> manifest msgpack
//   cas_refcount : blob_digest(hex)  -> u64 reference count (GC drives off this)
const CAS_CHUNKS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_chunks");
const CAS_BLOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");
const CAS_REFCOUNT: TableDefinition<&str, u64> = TableDefinition::new("cas_refcount");
const CAS_UPLOADS: TableDefinition<u64, &[u8]> = TableDefinition::new("cas_uploads");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableUpload {
    owner_scope: String,
    chunk_digests: Vec<String>,
    chunk_lens: Vec<u32>,
    len: u64,
    chunk_size: u32,
    applied_batches: HashSet<String>,
}

impl DurableUpload {
    fn manifest(&self) -> BlobManifest {
        BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: self.owner_scope.clone(),
            chunks: self.chunk_digests.clone(),
            chunk_lens: self.chunk_lens.clone(),
            len: self.len,
            chunk_size: self.chunk_size,
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.chunk_size == 0
            || self.chunk_size as usize > MAX_BLOB_CHUNK_BYTES
            || self.applied_batches.len() > MAX_BLOB_CHUNKS
            || self
                .applied_batches
                .iter()
                .any(|batch_id| batch_id.is_empty() || batch_id.len() > 1024)
        {
            return Err("blob upload exceeds resource limits".to_string());
        }
        self.manifest().validate()
    }
}

fn decode_upload(bytes: &[u8]) -> Result<DurableUpload, String> {
    let upload: DurableUpload = decode_blob_value(bytes)?;
    upload.validate()?;
    Ok(upload)
}

/// Chunks to flush per group commit (CONCEPT:EG-KG.storage.bounded-blob-memory — bounded memory). At the
/// 2 MiB default chunk size this is a ~64 MiB window: at most this many chunk bodies
/// are resident before ONE admitted mutation writes the whole group, so peak RSS is
/// bounded by the GROUP WINDOW, NOT the blob size. (One admitted mutation per chunk
/// bounds RAM too but collapses throughput with an fsync per chunk; group-commit is
/// the cadence `redb_backend.rs` already uses.) Tunable via
/// `EPISTEMIC_GRAPH_BLOB_GROUP_CHUNKS`.
const DEFAULT_GROUP_CHUNKS: usize = 32;

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
fn bind_scope(
    kernel: &StorageKernelV1,
    scope: &MutationScopeIdentity,
) -> Result<BlobHandle, String> {
    let authority = crate::store_authority::process_authority();
    let grant = kernel.authenticate_scope::<BlobOwner>(
        authority.as_ref(),
        scope.clone(),
        authority.principal().to_string(),
        &authority.proof(),
    )?;
    kernel.bind_serving_scope(grant, 0).map(Arc::new)
}

/// The native blob-domain scope identity for one (tenant, resource) pair.
fn blob_scope_identity(tenant: TenantId, resource: &str) -> Result<MutationScopeIdentity, String> {
    MutationScopeIdentity::native(
        tenant,
        MutationDomain::BlobStore,
        LogicalName::new(resource.to_string())?,
        IncarnationId::new(crate::server::mutation_batch::COMPILED_BATCH_INCARNATION)?,
    )
}

/// The batch for one plain (non-`_batch`) blob write. `batch_id` is `(event, scope
/// version)`: exactly one batch commits per version, so it is unique per attempt and
/// stable across a crash-retry of it — a retry replays, it does not conflict.
fn maintenance_batch(
    owner: &OwnedStoreHandle<BlobOwner>,
    event: &str,
    expected_version: u64,
) -> Result<MutationBatch, String> {
    let batch_id = format!("{event}:v{expected_version}");
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: MutationRequestContext {
            request_id: 0,
            principal: owner.principal().to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // A maintenance mutation claims no capability (a plain `Native`-versioned
            // write, never the reserved-system `Unversioned` path).
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: owner.identity().clone(),
        placement_epoch: 0,
        idempotency_key: batch_id.clone(),
        version_expectation: VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: MutationDomain::BlobStore,
            method: crate::protocol::Method::ApplyMutation {
                event_type: event.to_string(),
                query: batch_id,
            },
        }],
        outbox: Vec::new(),
        created_at_ms: 0,
    };
    batch.validate()?;
    Ok(batch)
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
    kernel: StorageKernelV1,
    mutations: MutationKernelV1,
    /// Bootstrap AND serving scope: cross-scope reads and caller-less writes run on it.
    bootstrap: BlobHandle,
    /// Serving scopes, keyed by `identity.binding_digest().to_hex()`.
    scopes: parking_lot::Mutex<HashMap<String, BlobHandle>>,
    batch: parking_lot::Mutex<ChunkBatch>,
}

impl RedbChunkStore {
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
        blob_scope_identity(TenantId::system(), "blob-ledger-root")
    }

    /// The native scope identity for one (tenant, graph) pair's blob rows. `graph` is
    /// the `resource` name, matching `commit_native_batch`'s callers, which route by
    /// `(tenant, graph)` (e.g. `mutation_version`).
    fn scope_identity(&self, tenant: &str, graph: &str) -> Result<MutationScopeIdentity, String> {
        blob_scope_identity(TenantId::new(tenant.to_string())?, graph)
    }

    /// Open (creating if absent) `{persist_dir}/blob.redb` as ONE physical owner file
    /// under `eg_storage::OwnerLayout::Blob`. `create_owner` materializes the WHOLE
    /// declared census — `cas_chunks`, `cas_blobs`, `cas_refcount`, `cas_uploads` plus
    /// the ledger — so the old four-table bootstrap closure is gone, and CAS rows still
    /// land in the SAME transaction as the ledger's idempotency/OCC/outbox rows, which
    /// is what `commit_native_batch` needs for atomicity.
    ///
    /// NOTE (flagged, not resolved here): the previous `Database::builder()
    /// .set_cache_size(EPISTEMIC_GRAPH_BLOB_CACHE_BYTES)` tuning is DROPPED by this
    /// migration — `eg_storage::StorageKernelV1::{create_owner, open_owner}` open the
    /// file themselves with no cache-size injection point, and this store has no other
    /// path to the database beneath them. That cap existed to bound RSS against this
    /// store's multi-MB chunk values; the follow-up belongs in `eg-storage`.
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
        let path = std::path::Path::new(persist_dir).join("blob.redb");
        let physical = PhysicalStoreIdentity::new(BLOB_PHYSICAL_STORE)?;
        let kernel = if path.exists() {
            StorageKernelV1::open_owner::<BlobOwner>(&path, physical, None)
        } else {
            StorageKernelV1::create_owner::<BlobOwner>(&path, physical, None)
        }?;
        let (kernel, authority) = kernel.into_read_and_mutation_authority()?;
        let mutations = MutationKernelV1::new(authority);
        let bootstrap = bind_scope(&kernel, &Self::ledger_bootstrap_identity()?)?;
        mutations.bootstrap_ledger(&bootstrap)?;
        let group = std::env::var("EPISTEMIC_GRAPH_BLOB_GROUP_CHUNKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_GROUP_CHUNKS);
        Ok(Self {
            kernel,
            mutations,
            bootstrap,
            scopes: parking_lot::Mutex::new(HashMap::new()),
            batch: parking_lot::Mutex::new(ChunkBatch {
                pending: HashMap::new(),
                group,
            }),
        })
    }

    /// Open an in-memory-only CAS backed by a temp dir (tests). The dir is left to
    /// the OS temp reaper; callers that want cleanup pass their own dir to `open`.
    #[cfg(test)]
    pub fn open_temp() -> Result<Self, String> {
        let dir = crate::server::unique_temp_dir("eg-blob-cas");
        Self::open(&dir.to_string_lossy())
    }
}

impl ChunkStore for RedbChunkStore {
    fn put_chunk(&self, bytes: &[u8]) -> Result<(String, bool), String> {
        let digest = chunk_digest(bytes)?;
        let mut batch = self.batch.lock();
        // Dedup: already staged in this window, or already committed to `cas_chunks`.
        if batch.pending.contains_key(&digest) || self.chunk_present(&digest)? {
            return Ok((digest, false));
        }
        batch.pending.insert(digest.clone(), bytes.to_vec());
        // Group-commit boundary: flush every `group` staged chunks so the resident
        // chunk bodies (peak RAM) never exceed the group window — independent of the
        // blob size. One admitted mutation amortizes the whole group.
        if batch.pending.len() >= batch.group {
            self.commit_group(&mut batch)?;
        }
        Ok((digest, true))
    }

    fn get_chunk(&self, digest: &str) -> Result<Option<Vec<u8>>, String> {
        // A read must see all committed chunks: flush the open group first so a
        // just-uploaded chunk is durable + visible (a staged group is not yet a row).
        self.flush_chunks()?;
        validate_digest(digest)?;
        // `cas_chunks`/`cas_refcount` are contracted `TableScope::SharedService`, but
        // `eg-storage`'s `blob_shared.rs` authority for them exposes only a row count
        // and NO write surface, so it cannot serve this store. Both are in
        // `owner_table_names(OwnerLayout::Blob)`, so they go through the owner-read/
        // owner-write path the kernel itself permits; if that confinement is later
        // enforced this store needs a `BlobSharedWrite` on the same admitted txn.
        let read = self.read()?;
        let t = read.open_owner_table(CAS_CHUNKS)?;
        t.get(digest)
            .map_err(|e| e.to_string())?
            .map(|g| {
                if g.value().len() > MAX_BLOB_CHUNK_BYTES {
                    Err("stored blob chunk exceeds resource limits".to_string())
                } else {
                    Ok(g.value().to_vec())
                }
            })
            .transpose()
    }

    fn put_manifest(&self, blob_digest: &str, manifest: &BlobManifest) -> Result<(), String> {
        // BlobCommit lands here: flush the upload's final partial chunk group first
        // (the manifest's chunks must all be durable before the manifest references
        // them), then write the manifest as its own admitted maintenance mutation.
        self.flush_chunks()?;
        let bytes = encode_manifest(blob_digest, manifest)?;
        self.maintain("blob_put_manifest_v1", |owner_write| {
            let mut t = owner_write.open_table(CAS_BLOBS)?;
            t.insert(blob_digest, bytes.as_slice())
                .map_err(|e| e.to_string())?;
            Ok(())
        })
    }

    fn get_manifest(&self, blob_digest: &str) -> Result<Option<BlobManifest>, String> {
        self.flush_chunks()?;
        validate_digest(blob_digest)?;
        let read = self.read()?;
        let t = read.open_owner_table(CAS_BLOBS)?;
        match t.get(blob_digest).map_err(|e| e.to_string())? {
            Some(g) => Ok(Some(decode_manifest(g.value())?)),
            None => Ok(None),
        }
    }

    fn incref(&self, blob_digest: &str) -> Result<u64, String> {
        self.adjust_ref(blob_digest, 1)
    }

    fn decref(&self, blob_digest: &str) -> Result<u64, String> {
        self.adjust_ref(blob_digest, -1)
    }

    fn refcount(&self, blob_digest: &str) -> Result<u64, String> {
        self.flush_chunks()?;
        validate_digest(blob_digest)?;
        let read = self.read()?;
        Ok(read
            .open_owner_table(CAS_REFCOUNT)?
            .get(blob_digest)
            .map_err(|e| e.to_string())?
            .map(|g| g.value())
            .unwrap_or(0))
    }

    fn sweep(&self) -> Result<SweepStats, String> {
        self.flush_chunks()?;
        self.maintain("blob_sweep_v1", sweep_rows)
    }

    fn chunk_count(&self) -> Result<u64, String> {
        self.flush_chunks()?;
        let read = self.read()?;
        read.open_owner_table(CAS_CHUNKS)?
            .len()
            .map_err(|e| e.to_string())
    }

    fn blob_count(&self) -> Result<u64, String> {
        self.flush_chunks()?;
        let read = self.read()?;
        read.open_owner_table(CAS_BLOBS)?
            .len()
            .map_err(|e| e.to_string())
    }

    fn mutation_version(&self, tenant: &str, graph: &str) -> Result<u64, String> {
        let owner = self.scope_handle(&self.scope_identity(tenant, graph)?)?;
        eg_transaction::version(&self.kernel.read_scope(&owner)?)
    }

    fn commit_cursor_batch(
        &self,
        batch: &MutationBatch,
        cursor: u64,
        committed_at_ms: u64,
    ) -> Result<u64, String> {
        self.commit_native_batch(batch, committed_at_ms, |_| Ok(cursor))
    }

    fn put_chunk_batch(
        &self,
        bytes: &[u8],
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(String, bool), String> {
        self.flush_chunks()?;
        let digest = chunk_digest(bytes)?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            let was_new = insert_chunk_row(wtx, &digest, bytes)?;
            Ok((digest.clone(), was_new))
        })
    }

    fn put_chunk_ref_batch(
        &self,
        bytes: &[u8],
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(String, bool, u64), String> {
        self.flush_chunks()?;
        let digest = chunk_digest(bytes)?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            let was_new = insert_chunk_row(wtx, &digest, bytes)?;
            let refcount = update_refcount(wtx, &digest, |current| {
                current
                    .checked_add(1)
                    .ok_or_else(|| "blob reference count overflow".to_string())
            })?;
            Ok((digest.clone(), was_new, refcount))
        })
    }

    fn put_manifest_batch(
        &self,
        blob_digest: &str,
        manifest: &BlobManifest,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<String, String> {
        self.flush_chunks()?;
        let bytes = encode_manifest(blob_digest, manifest)?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            let mut table = wtx.open_table(CAS_BLOBS)?;
            table
                .insert(blob_digest, bytes.as_slice())
                .map_err(|e| e.to_string())?;
            Ok(blob_digest.to_string())
        })
    }

    fn adjust_ref_batch(
        &self,
        blob_digest: &str,
        delta: i64,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<u64, String> {
        self.flush_chunks()?;
        validate_digest(blob_digest)?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            update_refcount(wtx, blob_digest, |current| Ok(apply_delta(current, delta)))
        })
    }

    fn sweep_batch(
        &self,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<SweepStats, String> {
        self.flush_chunks()?;
        self.commit_native_batch(batch, committed_at_ms, sweep_rows)
    }

    fn begin_upload_batch(
        &self,
        cursor: u64,
        chunk_size: u32,
        owner_scope: &str,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<u64, String> {
        self.flush_chunks()?;
        if chunk_size == 0 || chunk_size as usize > MAX_BLOB_CHUNK_BYTES {
            return Err("blob chunk size exceeds resource limits".to_string());
        }
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            let mut uploads = wtx.open_table(CAS_UPLOADS)?;
            if uploads.get(cursor).map_err(|e| e.to_string())?.is_none() {
                let upload = DurableUpload {
                    owner_scope: owner_scope.to_string(),
                    chunk_digests: Vec::new(),
                    chunk_lens: Vec::new(),
                    len: 0,
                    chunk_size,
                    applied_batches: HashSet::new(),
                };
                upload.validate()?;
                let bytes = rmp_serde::to_vec_named(&upload).map_err(|e| e.to_string())?;
                eg_types::msgpack::validate_single_value(&bytes, blob_msgpack_limits())
                    .map_err(|_| "blob upload exceeds resource limits".to_string())?;
                uploads
                    .insert(cursor, bytes.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            Ok(cursor)
        })
    }

    fn put_upload_chunk_batch(
        &self,
        cursor: u64,
        bytes: &[u8],
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(String, u32), String> {
        self.flush_chunks()?;
        let digest = chunk_digest(bytes)?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            insert_chunk_row(wtx, &digest, bytes)?;
            let mut uploads = wtx.open_table(CAS_UPLOADS)?;
            let row = uploads
                .get(cursor)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "unknown durable upload cursor".to_string())?;
            let mut upload = decode_upload(row.value())?;
            drop(row);
            if upload.applied_batches.insert(batch.batch_id.clone()) {
                if upload.chunk_digests.len() >= MAX_BLOB_CHUNKS {
                    return Err("blob upload exceeds resource limits".to_string());
                }
                upload.chunk_digests.push(digest.clone());
                upload.chunk_lens.push(bytes.len() as u32);
                upload.len = upload
                    .len
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| "blob upload exceeds resource limits".to_string())?;
                upload.validate()?;
                let encoded = rmp_serde::to_vec_named(&upload).map_err(|e| e.to_string())?;
                eg_types::msgpack::validate_single_value(&encoded, blob_msgpack_limits())
                    .map_err(|_| "blob upload exceeds resource limits".to_string())?;
                uploads
                    .insert(cursor, encoded.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            Ok((digest.clone(), upload.chunk_digests.len() as u32))
        })
    }

    fn load_upload(&self, cursor: u64) -> Result<Option<BlobManifest>, String> {
        self.flush_chunks()?;
        let read = self.read()?;
        let uploads = read.open_owner_table(CAS_UPLOADS)?;
        let manifest = uploads
            .get(cursor)
            .map_err(|e| e.to_string())?
            .map(|row| decode_upload(row.value()).map(|upload| upload.manifest()))
            .transpose()?;
        Ok(manifest)
    }

    fn commit_upload_batch(
        &self,
        cursor: u64,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<String, String> {
        self.flush_chunks()?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            let upload = {
                let uploads = wtx.open_table(CAS_UPLOADS)?;
                let row = uploads
                    .get(cursor)
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| "unknown durable upload cursor".to_string())?;
                decode_upload(row.value())?
            };
            let manifest = upload.manifest();
            manifest.validate()?;
            let encoded = rmp_serde::to_vec_named(&manifest).map_err(|e| e.to_string())?;
            eg_types::msgpack::validate_single_value(&encoded, blob_msgpack_limits())
                .map_err(|_| "blob manifest exceeds resource limits".to_string())?;
            let digest = hex_digest(&encoded);
            {
                let mut blobs = wtx.open_table(CAS_BLOBS)?;
                blobs
                    .insert(digest.as_str(), encoded.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            {
                let mut uploads = wtx.open_table(CAS_UPLOADS)?;
                uploads.remove(cursor).map_err(|e| e.to_string())?;
            }
            Ok(digest)
        })
    }
}

/// Insert `bytes` at `digest` in `cas_chunks` unless already there; reports whether it
/// was new — the dedup answer every chunk write path shares.
fn insert_chunk_row(
    owner_write: &AdmittedOwnerWrite<'_, BlobOwner>,
    digest: &str,
    bytes: &[u8],
) -> Result<bool, String> {
    let mut chunks = owner_write.open_table(CAS_CHUNKS)?;
    let absent = chunks.get(digest).map_err(|e| e.to_string())?.is_none();
    if absent {
        chunks.insert(digest, bytes).map_err(|e| e.to_string())?;
    }
    Ok(absent)
}

/// Read `digest`'s reference count (0 when absent), compute the next value with `next`
/// and store it, in the same admitted transaction.
fn update_refcount<F>(
    owner_write: &AdmittedOwnerWrite<'_, BlobOwner>,
    digest: &str,
    next: F,
) -> Result<u64, String>
where
    F: FnOnce(u64) -> Result<u64, String>,
{
    let mut refs = owner_write.open_table(CAS_REFCOUNT)?;
    let current = refs
        .get(digest)
        .map_err(|e| e.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    let updated = next(current)?;
    refs.insert(digest, updated).map_err(|e| e.to_string())?;
    Ok(updated)
}

fn sweep_rows(wtx: &AdmittedOwnerWrite<'_, BlobOwner>) -> Result<SweepStats, String> {
    let dead: Vec<String> = {
        let refs = wtx.open_table(CAS_REFCOUNT)?;
        let mut dead = Vec::new();
        for row in refs.iter().map_err(|e| e.to_string())? {
            let (key, value) = row.map_err(|e| e.to_string())?;
            validate_digest(key.value())?;
            if value.value() == 0 {
                if dead.len() >= MAX_BLOB_GC_TRACKED_DIGESTS {
                    return Err("blob garbage collection exceeds resource limits".to_string());
                }
                dead.push(key.value().to_string());
            }
        }
        dead
    };
    let orphan_manifests: Vec<String> = {
        let blobs = wtx.open_table(CAS_BLOBS)?;
        let refs = wtx.open_table(CAS_REFCOUNT)?;
        let mut orphans = Vec::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let digest = key.value();
            validate_digest(digest)?;
            if refs.get(digest).map_err(|e| e.to_string())?.is_none() {
                if orphans.len() >= MAX_BLOB_GC_TRACKED_DIGESTS {
                    return Err("blob garbage collection exceeds resource limits".to_string());
                }
                orphans.push(digest.to_string());
            }
        }
        orphans
    };
    let mut to_delete: HashSet<String> = dead.into_iter().collect();
    to_delete.extend(orphan_manifests);
    if to_delete.len() > MAX_BLOB_GC_TRACKED_DIGESTS {
        return Err("blob garbage collection exceeds resource limits".to_string());
    }
    let live_chunks: HashSet<String> = {
        let blobs = wtx.open_table(CAS_BLOBS)?;
        let mut live = HashSet::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (key, value) = row.map_err(|e| e.to_string())?;
            if to_delete.contains(key.value()) {
                continue;
            }
            let manifest = decode_manifest(value.value())?;
            for chunk in manifest.chunks {
                track_gc_digest(&mut live, chunk)?;
            }
        }
        live
    };
    let mut orphan_chunks: HashSet<String> = {
        let blobs = wtx.open_table(CAS_BLOBS)?;
        let mut orphans = HashSet::new();
        for digest in &to_delete {
            if let Some(value) = blobs.get(digest.as_str()).map_err(|e| e.to_string())? {
                let manifest = decode_manifest(value.value())?;
                for chunk in manifest.chunks {
                    if !live_chunks.contains(&chunk) {
                        track_gc_digest(&mut orphans, chunk)?;
                    }
                }
            }
        }
        orphans
    };
    // `put_chunk_ref_batch` is the direct-artifact fast path: its refcount key is the
    // chunk digest itself and deliberately has no manifest row, so a zero-ref direct
    // chunk cannot be found by walking dead manifests. Reclaim it here unless a
    // surviving manifest still names it — leak-free compensation/restart replay
    // without endangering a chunk still reachable from a live blob.
    {
        let chunks = wtx.open_table(CAS_CHUNKS)?;
        for digest in &to_delete {
            if !live_chunks.contains(digest)
                && chunks
                    .get(digest.as_str())
                    .map_err(|e| e.to_string())?
                    .is_some()
            {
                track_gc_digest(&mut orphan_chunks, digest.clone())?;
            }
        }
    }
    let mut stats = SweepStats::default();
    {
        let mut blobs = wtx.open_table(CAS_BLOBS)?;
        let mut refs = wtx.open_table(CAS_REFCOUNT)?;
        for digest in &to_delete {
            if blobs
                .remove(digest.as_str())
                .map_err(|e| e.to_string())?
                .is_some()
            {
                stats.blobs_reclaimed += 1;
            }
            refs.remove(digest.as_str()).map_err(|e| e.to_string())?;
        }
    }
    {
        let mut chunks = wtx.open_table(CAS_CHUNKS)?;
        for digest in &orphan_chunks {
            if chunks
                .remove(digest.as_str())
                .map_err(|e| e.to_string())?
                .is_some()
            {
                stats.chunks_reclaimed += 1;
            }
        }
    }
    Ok(stats)
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
    /// The cross-scope snapshot read every getter runs on: a kernel-issued scoped read
    /// on the bootstrap scope over this layout's four owner tables.
    fn read(&self) -> Result<ScopedRead<'_, BlobOwner>, String> {
        self.kernel.read_scope(&self.bootstrap)
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
        let read = self.read()?;
        Ok(read
            .open_owner_table(CAS_CHUNKS)?
            .get(digest)
            .map_err(|e| e.to_string())?
            .is_some())
    }

    /// Write the whole staged chunk group as ONE admitted maintenance mutation and empty
    /// it. A commit failure surfaces here (the group's chunks did NOT land).
    fn commit_group(&self, batch: &mut ChunkBatch) -> Result<(), String> {
        if batch.pending.is_empty() {
            return Ok(());
        }
        let group = std::mem::take(&mut batch.pending);
        self.maintain("blob_chunk_group_v1", |owner_write| {
            let mut table = owner_write.open_table(CAS_CHUNKS)?;
            for (digest, bytes) in &group {
                table
                    .insert(digest.as_str(), bytes.as_slice())
                    .map_err(|e| e.to_string())?;
            }
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
    fn maintain<T, F>(&self, event: &str, apply: F) -> Result<T, String>
    where
        T: serde::Serialize + DeserializeOwned,
        F: FnOnce(&AdmittedOwnerWrite<'_, BlobOwner>) -> Result<T, String>,
    {
        let version = eg_transaction::version(&self.read()?)?;
        let batch = maintenance_batch(&self.bootstrap, event, version)?;
        let now = crate::server::dispatch::authoritative_now_ms();
        self.apply_write(&self.bootstrap, &batch, now, true, apply)
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
        self.apply_write(&owner, batch, committed_at_ms, false, apply)
    }

    /// Admit `batch`, stage its owner rows, persist the exact verdict as the replayable
    /// result and commit — ONE transaction, so a CAS row and its terminal
    /// MutationBatch metadata can never disagree.
    fn apply_write<T, F>(
        &self,
        owner: &OwnedStoreHandle<BlobOwner>,
        batch: &MutationBatch,
        at_ms: u64,
        maintenance: bool,
        apply: F,
    ) -> Result<T, String>
    where
        T: serde::Serialize + DeserializeOwned,
        F: FnOnce(&AdmittedOwnerWrite<'_, BlobOwner>) -> Result<T, String>,
    {
        let (write, begun) = if maintenance {
            self.mutations.admit_maintenance(owner, batch)
        } else {
            self.mutations.admit(owner, batch)
        }?;
        let source_version = match begun {
            // Terminally committed already: the recorded verdict IS the answer, and
            // re-applying it would double the effect.
            Begin::Replay(record) => {
                let bytes = record
                    .result_msgpack
                    .as_deref()
                    .ok_or_else(|| "committed blob MutationBatch has no result".to_string())?;
                let replayed = decode_blob_value(bytes)?;
                write.abort()?;
                return Ok(replayed);
            }
            Begin::Apply { source_version } => source_version,
        };
        let owner_write = write.owner_rows(owner, batch)?;
        let staged = apply(&owner_write);
        // Dropping the owner capability unfinished poisons the write; always close it.
        owner_write.finish_owner()?;
        let result = match staged {
            Ok(value) => value,
            Err(error) => {
                write.abort()?;
                return Err(error);
            }
        };
        let encoded = rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())?;
        self.mutations
            .finish(&write, batch, Some(encoded), at_ms, source_version)?;
        self.mutations.commit(write, batch)?;
        Ok(result)
    }

    #[cfg(feature = "blob-s3")]
    pub(crate) fn record_native_batch_result<T>(
        &self,
        batch: &MutationBatch,
        result: T,
        committed_at_ms: u64,
    ) -> Result<T, String>
    where
        T: serde::Serialize + DeserializeOwned,
    {
        self.commit_native_batch(batch, committed_at_ms, |_| Ok(result))
    }

    #[cfg(feature = "blob-s3")]
    pub(crate) fn sweep_batch_with_external_chunks(
        &self,
        batch: &MutationBatch,
        committed_at_ms: u64,
        external_chunks: u64,
    ) -> Result<SweepStats, String> {
        self.flush_chunks()?;
        self.commit_native_batch(batch, committed_at_ms, |wtx| {
            let mut stats = sweep_rows(wtx)?;
            stats.chunks_reclaimed = external_chunks;
            Ok(stats)
        })
    }

    /// Chunks that the NEXT [`sweep`](ChunkStore::sweep) would orphan: chunks
    /// referenced ONLY by dead blobs (refcount 0 or no refcount entry) and by no
    /// surviving manifest. Used by the S3 backend to delete the matching objects
    /// before the sidecar sweep drops the manifests. Read-only.
    #[cfg(feature = "blob-s3")]
    pub(crate) fn orphan_chunks_preview(&self) -> Result<HashSet<String>, String> {
        self.flush_chunks()?;
        let read = self.read()?;
        let blobs = read.open_owner_table(CAS_BLOBS)?;
        let refs = read.open_owner_table(CAS_REFCOUNT)?;
        let mut to_delete: HashSet<String> = HashSet::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (k, _) = row.map_err(|e| e.to_string())?;
            let d = k.value();
            validate_digest(d)?;
            let dead = refs
                .get(d)
                .map_err(|e| e.to_string())?
                .map(|g| g.value() == 0)
                .unwrap_or(true);
            if dead {
                track_gc_digest(&mut to_delete, d.to_string())?;
            }
        }
        let mut live_chunks: HashSet<String> = HashSet::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            if to_delete.contains(k.value()) {
                continue;
            }
            let m = decode_manifest(v.value())?;
            for chunk in m.chunks {
                track_gc_digest(&mut live_chunks, chunk)?;
            }
        }
        let mut orphans = HashSet::new();
        for digest in &to_delete {
            if let Some(v) = blobs.get(digest.as_str()).map_err(|e| e.to_string())? {
                let m = decode_manifest(v.value())?;
                for c in m.chunks {
                    if !live_chunks.contains(&c) {
                        track_gc_digest(&mut orphans, c)?;
                    }
                }
            }
        }
        Ok(orphans)
    }

    /// Count of distinct chunk digests referenced by all surviving manifests
    /// (dedup observability when chunk bytes live off-redb, e.g. in S3).
    #[cfg(feature = "blob-s3")]
    pub(crate) fn distinct_referenced_chunks(&self) -> Result<u64, String> {
        self.flush_chunks()?;
        let read = self.read()?;
        let blobs = read.open_owner_table(CAS_BLOBS)?;
        let mut seen: HashSet<String> = HashSet::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (_, v) = row.map_err(|e| e.to_string())?;
            let m = decode_manifest(v.value())?;
            for chunk in m.chunks {
                track_gc_digest(&mut seen, chunk)?;
            }
        }
        Ok(seen.len() as u64)
    }

    /// Adjust a blob's refcount by `delta`, saturating at 0, as ONE admitted owner
    /// maintenance mutation (RF-RULING-005 — it carries no caller identity).
    fn adjust_ref(&self, blob_digest: &str, delta: i64) -> Result<u64, String> {
        self.flush_chunks()?;
        validate_digest(blob_digest)?;
        self.maintain("blob_adjust_ref_v1", |owner_write| {
            update_refcount(owner_write, blob_digest, |cur| Ok(apply_delta(cur, delta)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coordinator_batch(
        id: &str,
        request_id: u64,
        expected: u64,
        event_type: &str,
    ) -> MutationBatch {
        crate::server::mutation_batch::compile_opaque_method(
            crate::server::mutation_batch::CompileBatch {
                batch_id: id,
                request_id,
                principal: Some("system"),
                tenant: "tenant-opaque",
                graph: "scope-opaque",
                placement_epoch: 0,
                idempotency_key: id,
                expected_graph_version: Some(expected),
                fencing_token: None,
                created_at_ms: request_id,
                default_surface: crate::mutation_batch::MutationSurface::Other,
                authoritative_state: None,
            },
            &crate::protocol::Method::ApplyMutation {
                event_type: event_type.to_string(),
                query: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            },
            crate::mutation_batch::MutationSurface::Other,
            crate::mutation_batch::MutationDomain::BlobStore,
            "blob_coordinator_test",
        )
        .unwrap()
    }

    #[test]
    fn direct_ref_acquire_compensation_and_gc_are_restart_replay_safe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let body = b"opaque-result";
        let digest = hex_digest(body);
        {
            let store = RedbChunkStore::open(&path).unwrap();
            let acquire = coordinator_batch("acquire", 1, 0, "blob_direct_ref_acquire_v1");
            assert_eq!(
                store.put_chunk_ref_batch(body, &acquire, 1).unwrap(),
                (digest.clone(), true, 1)
            );
            assert_eq!(
                store.put_chunk_ref_batch(body, &acquire, 1).unwrap(),
                (digest.clone(), true, 1),
                "acknowledgement-lost acquire must replay without another ref"
            );
            let release = coordinator_batch("release", 2, 1, "blob_direct_ref_release_v1");
            assert_eq!(store.adjust_ref_batch(&digest, -1, &release, 2).unwrap(), 0);
            assert_eq!(
                store.adjust_ref_batch(&digest, -1, &release, 2).unwrap(),
                0,
                "acknowledgement-lost compensation must not underflow"
            );
        }
        let store = RedbChunkStore::open(&path).unwrap();
        assert_eq!(store.refcount(&digest).unwrap(), 0);
        let swept = store.sweep().unwrap();
        assert_eq!(swept.chunks_reclaimed, 1);
        assert!(store.get_chunk(&digest).unwrap().is_none());
    }

    #[test]
    fn stored_manifest_decoder_rejects_allocation_bombs_and_inconsistent_metadata() {
        let allocation_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_manifest(&allocation_bomb).is_err());

        let inconsistent = BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
            chunks: vec!["0".repeat(64)],
            chunk_lens: vec![1],
            len: 2,
            chunk_size: 1,
        };
        let encoded = rmp_serde::to_vec_named(&inconsistent).unwrap();
        assert!(decode_manifest(&encoded).is_err());
    }

    #[test]
    fn manifest_key_must_match_content_digest() {
        let store = RedbChunkStore::open_temp().unwrap();
        let manifest = BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
            chunks: Vec::new(),
            chunk_lens: Vec::new(),
            len: 0,
            chunk_size: 0,
        };
        assert!(store.put_manifest(&"0".repeat(64), &manifest).is_err());
    }

    fn chunked(store: &dyn ChunkStore, data: &[u8], chunk_size: usize) -> CommittedBlob {
        // Stream the data through the store one chunk at a time (bounded memory),
        // exactly as the protocol cursor does.
        let mut chunks = Vec::new();
        let mut chunk_lens = Vec::new();
        for part in data.chunks(chunk_size) {
            let (digest, _was_new) = store.put_chunk(part).unwrap();
            chunks.push(digest);
            chunk_lens.push(part.len() as u32);
        }
        let manifest = BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
            chunks,
            chunk_lens,
            len: data.len() as u64,
            chunk_size: chunk_size as u32,
        };
        let bytes = rmp_serde::to_vec_named(&manifest).unwrap();
        let digest = hex_digest(&bytes);
        store.put_manifest(&digest, &manifest).unwrap();
        CommittedBlob { digest, manifest }
    }

    fn reassemble(store: &dyn ChunkStore, manifest: &BlobManifest) -> Vec<u8> {
        let mut out = Vec::new();
        for d in &manifest.chunks {
            out.extend(store.get_chunk(d).unwrap().unwrap());
        }
        out
    }

    #[test]
    fn roundtrip_integrity_and_content_address_is_stable() {
        let store = RedbChunkStore::open_temp().unwrap();
        let data: Vec<u8> = (0u32..500_000).map(|i| (i % 251) as u8).collect();
        let blob = chunked(&store, &data, 4096);
        assert_eq!(reassemble(&store, &blob.manifest), data);
        // Re-committing identical content yields the SAME blob digest.
        let blob2 = chunked(&store, &data, 4096);
        assert_eq!(blob.digest, blob2.digest);
    }

    #[test]
    fn identical_content_dedups_chunks() {
        let store = RedbChunkStore::open_temp().unwrap();
        let data: Vec<u8> = (0u32..400_000).map(|i| (i % 97) as u8).collect();
        let b1 = chunked(&store, &data, 8192);
        let after_first = store.chunk_count().unwrap();
        let b2 = chunked(&store, &data, 8192);
        let after_second = store.chunk_count().unwrap();
        // The second upload of identical content stored ZERO new chunks.
        assert_eq!(b1.digest, b2.digest);
        assert_eq!(after_first, after_second);
        assert!(after_first > 0);
    }

    #[test]
    fn gc_reclaims_orphan_keeps_referenced_shared() {
        let store = RedbChunkStore::open_temp().unwrap();
        // Two distinct blobs that SHARE a chunk (the first 8KB), plus a chunk
        // unique to each.
        let shared: Vec<u8> = vec![0xAB; 8192];
        let only_a: Vec<u8> = vec![0x11; 8192];
        let only_b: Vec<u8> = vec![0x22; 8192];

        let mut data_a = shared.clone();
        data_a.extend(&only_a);
        let mut data_b = shared.clone();
        data_b.extend(&only_b);

        let a = chunked(&store, &data_a, 8192);
        let b = chunked(&store, &data_b, 8192);
        // 3 distinct chunks: shared, only_a, only_b.
        assert_eq!(store.chunk_count().unwrap(), 3);

        // Reference both blobs (each from one :Media node).
        store.incref(&a.digest).unwrap();
        store.incref(&b.digest).unwrap();
        assert_eq!(store.refcount(&a.digest).unwrap(), 1);

        // Remove the reference to A only.
        assert_eq!(store.decref(&a.digest).unwrap(), 0);
        let stats = store.sweep().unwrap();
        // A's manifest is reclaimed; the shared chunk is KEPT (B still lists it),
        // so only A's unique chunk is reclaimed.
        assert_eq!(stats.blobs_reclaimed, 1);
        assert_eq!(stats.chunks_reclaimed, 1);
        assert!(store.get_manifest(&a.digest).unwrap().is_none());
        assert!(store.get_manifest(&b.digest).unwrap().is_some());
        // B is still fully reassemblable (shared chunk survived).
        assert_eq!(reassemble(&store, &b.manifest), data_b);
        assert_eq!(store.chunk_count().unwrap(), 2); // shared + only_b

        // Now drop B too → everything is reclaimed.
        assert_eq!(store.decref(&b.digest).unwrap(), 0);
        let stats = store.sweep().unwrap();
        assert_eq!(stats.blobs_reclaimed, 1);
        assert_eq!(stats.chunks_reclaimed, 2); // shared + only_b now orphan
        assert_eq!(store.chunk_count().unwrap(), 0);
        assert_eq!(store.blob_count().unwrap(), 0);
    }

    #[test]
    fn shared_blob_survives_until_last_reference_drops() {
        let store = RedbChunkStore::open_temp().unwrap();
        // Two DISTINCT chunks (so chunk_reclaimed is unambiguous, not deduped to 1).
        let mut data: Vec<u8> = vec![0x5A; 8192];
        data.extend(std::iter::repeat_n(0x6B, 8192));
        let blob = chunked(&store, &data, 8192);
        // Two :Media nodes reference the SAME blob (dedup at the blob level).
        store.incref(&blob.digest).unwrap();
        assert_eq!(store.incref(&blob.digest).unwrap(), 2);

        // First reference removed → still referenced → sweep keeps it.
        assert_eq!(store.decref(&blob.digest).unwrap(), 1);
        let stats = store.sweep().unwrap();
        assert_eq!(stats.blobs_reclaimed, 0);
        assert!(store.get_manifest(&blob.digest).unwrap().is_some());

        // Last reference removed → sweep reclaims it.
        assert_eq!(store.decref(&blob.digest).unwrap(), 0);
        let stats = store.sweep().unwrap();
        assert_eq!(stats.blobs_reclaimed, 1);
        assert_eq!(stats.chunks_reclaimed, 2);
    }

    /// Peak RSS of this process, MB (Linux VmHWM). Used to assert bounded memory.
    fn peak_rss_mb() -> u64 {
        let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                if let Some(kb) = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    return kb / 1024;
                }
            }
        }
        0
    }

    /// Fill a chunk with offset-seeded pseudo-random bytes (xorshift) so chunks are
    /// DISTINCT (worst case for dedup — proves real storage) without holding the file.
    fn fill_chunk(buf: &mut [u8], idx: u64) {
        let mut x = (idx + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        for b in buf.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = (x & 0xFF) as u8;
        }
    }

    /// BOUNDED MEMORY (the flagged correctness risk). Streams a LARGE blob through the
    /// group-commit CAS one chunk at a time — generated, stored, dropped — then streams
    /// it back re-hashing chunk-by-chunk, retaining NEITHER the whole blob NOR all
    /// chunks. Peak RSS must stay near the group window (≈64 MiB at 32× 2 MiB),
    /// INDEPENDENT of the blob size: a regression that stopped bounding the window
    /// would make RSS track the file size and trip the assert. Default 256 MB (4× the
    /// window); `EG_BLOB_RSS_MB` runs 1GB+.
    // Measures WHOLE-PROCESS RSS, so it is only meaningful run in isolation: under the
    // parallel test harness a sibling 256MB-blob test inflates the shared RSS and trips
    // the bound (~347MB observed vs the 320 cap). Ignored by default; the CI
    // "memory-regression" step runs it serially (`--ignored --test-threads=1`).
    #[test]
    #[ignore = "process-global RSS; run isolated via `--ignored --test-threads=1`"]
    fn bounded_memory_large_blob_group_commit() {
        let total_mb: u64 = std::env::var("EG_BLOB_RSS_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256);
        let chunk_size = 2 * 1024 * 1024usize; // 2 MiB
        let n_chunks = (total_mb * 1024 * 1024).div_ceil(chunk_size as u64);
        let store = RedbChunkStore::open_temp().unwrap();

        let rss_before = peak_rss_mb();

        // Upload streaming — one chunk resident at a time.
        let mut digests = Vec::with_capacity(n_chunks as usize);
        let mut src_hash = Sha256::new();
        for i in 0..n_chunks {
            let mut buf = vec![0u8; chunk_size];
            fill_chunk(&mut buf, i);
            src_hash.update(&buf);
            let (digest, _was_new) = store.put_chunk(&buf).unwrap();
            digests.push(digest);
            // buf dropped here — never accumulated.
        }
        let chunk_lens = vec![chunk_size as u32; n_chunks as usize];
        let manifest = BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
            chunks: digests,
            chunk_lens,
            len: n_chunks * chunk_size as u64,
            chunk_size: chunk_size as u32,
        };
        let mbytes = rmp_serde::to_vec_named(&manifest).unwrap();
        let blob_digest = hex_digest(&mbytes);
        store.put_manifest(&blob_digest, &manifest).unwrap();

        // Download streaming — re-hash chunk-by-chunk, retain nothing.
        let got = store.get_manifest(&blob_digest).unwrap().unwrap();
        let mut dl_hash = Sha256::new();
        for d in &got.chunks {
            let bytes = store.get_chunk(d).unwrap().unwrap();
            dl_hash.update(&bytes);
        }
        assert_eq!(
            hex::encode(src_hash.finalize()),
            hex::encode(dl_hash.finalize()),
            "round-trip integrity over the streamed large blob"
        );

        // Bounded memory: peak RSS GROWTH over the run must be a small multiple of the
        // group window (32×2MiB = 64MiB) plus the redb page cache and transient
        // buffers — NOT the blob size. A regression that stopped bounding the staged
        // group would track the file size (1GB blob → ~1GB RSS) and blow past this.
        let peak = peak_rss_mb();
        let growth = peak.saturating_sub(rss_before);
        assert!(
            growth < 320,
            "peak RSS growth {growth}MB for a {total_mb}MB blob must be bounded by the \
             group window plus the page cache, not the file size"
        );
    }

    #[test]
    fn sweep_reclaims_unreferenced_uploaded_blob() {
        // A blob that was uploaded+committed but never referenced by a node (an
        // abandoned upload) is reclaimed on sweep — no refcount entry == dead.
        let store = RedbChunkStore::open_temp().unwrap();
        let data: Vec<u8> = vec![0x7F; 4096];
        let blob = chunked(&store, &data, 4096);
        assert_eq!(store.blob_count().unwrap(), 1);
        let stats = store.sweep().unwrap();
        assert_eq!(stats.blobs_reclaimed, 1);
        assert_eq!(stats.chunks_reclaimed, 1);
        assert!(store.get_manifest(&blob.digest).unwrap().is_none());
    }
}
