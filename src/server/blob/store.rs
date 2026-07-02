//! Content-addressed chunk store (CAS) — CONCEPT:KG-2.206.
//!
//! The bytes tier under the `:Media`/`:Blob` graph shape. A media file is split
//! into fixed-size chunks; each chunk is stored ONCE keyed by its sha256
//! (intrinsic dedup), and a blob is a *manifest* (the ordered chunk digests + the
//! total length) keyed by the manifest's own sha256 — so identical content yields
//! an identical blob digest. A graph node references a blob purely by that digest
//! (`content_ref`); the chunk bytes NEVER touch the inline node/edge KV.
//!
//! This is the DAG-low storage core. It is a self-contained redb database
//! (`{persist_dir}/blob.redb`) SEPARATE from the graph store's `graph.redb` —
//! redb takes an exclusive per-process file lock, so the CAS cannot share the
//! graph DB; a separate file is the clean seam and keeps the manifest/chunk/
//! refcount tables off the hot graph store.
//!
//! ## The [`ChunkStore`] seam
//!
//! Native redb-CAS is the default backend on the Pi/standard build (no object
//! store SDK). The SAME trait fronts an S3/MinIO backend behind a SEPARATE
//! `blob-s3` feature, so the lean build links no object-store SDK. Native-redb vs
//! S3 changes only the body of the trait methods; the protocol frames, cursors,
//! manifest shape and graph linkage are identical.
//!
//! ## Refcount GC (the flagged correctness risk)
//!
//! Content addressing + dedup means one blob can be referenced by N `:Media`
//! nodes across graphs. So a blob carries a refcount: referencing a blob
//! [`incref`]s it, removing a reference [`decref`]s it, and a [`sweep`] reclaims
//! every blob whose refcount has fallen to zero — deleting its manifest AND every
//! chunk that no *surviving* blob still lists. Deleting one `:Media` therefore
//! never unlinks chunks another blob shares (proven by the GC tests). The chunk
//! liveness set is recomputed from the surviving manifests at sweep time
//! (mark-and-sweep), so a chunk shared by a live blob is always kept.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;

/// Default chunk size: 2 MiB (in the 1–4 MB band the streaming protocol targets;
/// matches the spike). The pre-EG-071 FIXED stride; content-defined chunking
/// ([`crate::server::blob::cdc`], CONCEPT:EG-071) now targets this as its AVERAGE.
/// Still the default chunk size of the wire upload cursor (`BlobBegin`).
pub const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// Manifest of a content-addressed blob: the ordered chunk digests + per-chunk
/// lengths + total length. Serialized to MessagePack; the blob digest is the sha256
/// of those bytes.
///
/// ## On-disk format & backward read (CONCEPT:EG-071)
///
/// EG-071 switched the splitter from a fixed 2 MiB stride to content-defined
/// (Gear/FastCDC) chunking, so chunks are now VARIABLE length. `chunk_lens` records
/// each chunk's byte length (offsets are its running prefix sum). It is
/// `#[serde(default)]`, so a PRE-EG-071 manifest — written with only `chunks`/`len`/
/// fixed `chunk_size` — still deserializes: it lands with an empty `chunk_lens`, and
/// [`chunk_lengths`](Self::chunk_lengths) reconstructs the boundaries from the fixed
/// `chunk_size`. We chose additive `#[serde(default)]` over a versioned envelope
/// because reconstruction (concatenating the stored chunk bytes in order) never
/// needs the lengths — the chunk bytes are self-describing — so OLD blobs remain
/// byte-for-byte reconstructable with zero migration; the lengths are metadata for
/// indexing/seeking and dedup observability. `chunk_size` is `0` for content-defined
/// manifests and non-zero only for legacy fixed-stride ones (the reconstruction key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobManifest {
    /// Hex sha256 chunk digests, in file order.
    pub chunks: Vec<String>,
    /// Per-chunk byte length, parallel to `chunks` (CONCEPT:EG-071). Empty in a
    /// pre-EG-071 fixed-stride manifest — see [`chunk_lengths`](Self::chunk_lengths).
    #[serde(default)]
    pub chunk_lens: Vec<u32>,
    /// Total length of the assembled blob in bytes.
    pub len: u64,
    /// Legacy fixed chunk size: `0` for content-defined (EG-071) manifests; the
    /// fixed stride for pre-EG-071 manifests, where it reconstructs `chunk_lens`.
    pub chunk_size: u32,
}

impl BlobManifest {
    /// Per-chunk byte lengths, in file order. Returns the recorded `chunk_lens` for
    /// content-defined (EG-071) manifests; for a legacy fixed-stride manifest (empty
    /// `chunk_lens`, non-zero `chunk_size`) it RECONSTRUCTS them — every chunk is
    /// `chunk_size` except a possibly-shorter final one. CONCEPT:EG-071 backward read.
    pub fn chunk_lengths(&self) -> Vec<u32> {
        if !self.chunk_lens.is_empty() || self.chunks.is_empty() {
            return self.chunk_lens.clone();
        }
        // Legacy fixed-stride reconstruction.
        let n = self.chunks.len();
        let cs = self.chunk_size as u64;
        if cs == 0 {
            return Vec::new();
        }
        let mut lens = Vec::with_capacity(n);
        let mut remaining = self.len;
        for _ in 0..n {
            let l = remaining.min(cs);
            lens.push(l as u32);
            remaining = remaining.saturating_sub(l);
        }
        lens
    }

    /// Per-chunk `(offset, len)` boundaries, in file order — the running prefix sum
    /// of [`chunk_lengths`](Self::chunk_lengths). For random-access/seek over a blob
    /// (CONCEPT:EG-071 variable boundaries).
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
}

/// What a [`ChunkStore::sweep`] reclaimed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepStats {
    pub blobs_reclaimed: u64,
    pub chunks_reclaimed: u64,
}

// ── native redb CAS ─────────────────────────────────────────────────────────

use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};

// Three tables, all in `blob.redb`, all OFF the inline node/edge KV:
//   cas_chunks   : chunk_digest(hex) -> chunk bytes        (DEDUP happens here)
//   cas_blobs    : blob_digest(hex)  -> manifest msgpack
//   cas_refcount : blob_digest(hex)  -> u64 reference count (GC drives off this)
const CAS_CHUNKS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_chunks");
const CAS_BLOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_blobs");
const CAS_REFCOUNT: TableDefinition<&str, u64> = TableDefinition::new("cas_refcount");

/// Chunks to flush per group commit (CONCEPT:KG-2.206 — bounded memory). At the
/// 2 MiB default chunk size this is a ~64 MiB write window: redb buffers at most
/// this many dirty chunk pages before forcing an `Immediate` commit, so peak RSS is
/// bounded by the GROUP WINDOW, NOT the blob size. (Committing each chunk
/// `Durability::None` would make redb buffer EVERY dirty page until a later flush —
/// RSS would track the whole file; per-chunk `Immediate` bounds RAM but collapses
/// throughput with an fsync per chunk. Group-commit is the resolution the engine's
/// `redb_backend.rs` already uses, inherited here.) Tunable via
/// `EPISTEMIC_GRAPH_BLOB_GROUP_CHUNKS`.
const DEFAULT_GROUP_CHUNKS: usize = 32;

/// An open chunk-write transaction accumulating up to `group` chunk inserts before
/// a forced group commit. Holds the redb `WriteTransaction` between puts so N chunk
/// inserts ride ONE `Immediate` fsync — group-commit, bounding both memory (≤ group
/// chunks resident) and fsync rate (one per group), exactly like `redb_backend.rs`.
struct ChunkBatch {
    /// `Some` while a group is open (chunks buffered, not yet committed).
    txn: Option<redb::WriteTransaction>,
    /// Chunks inserted into the currently-open `txn` (forces a commit at `group`).
    pending: usize,
    /// Group window: flush after this many buffered chunk inserts.
    group: usize,
}

/// Native redb content-addressed store. One redb `Database` per CAS, reusing the
/// engine's existing redb infrastructure (`Database` + `TableDefinition` +
/// off-reactor GROUP-COMMIT, the same cadence as `redb_backend.rs`). Chunk writes
/// accumulate in one open `WriteTransaction` flushed every [`DEFAULT_GROUP_CHUNKS`]
/// inserts with `Durability::Immediate`, so an acked group survives a crash AND peak
/// memory is bounded by the group window (not the file size). Manifest/refcount/
/// sweep/read operations flush the open chunk batch first (redb permits one writer
/// at a time), then run their own durable transaction.
pub struct RedbChunkStore {
    db: Arc<Database>,
    batch: parking_lot::Mutex<ChunkBatch>,
}

impl RedbChunkStore {
    /// Open (or create) `{persist_dir}/blob.redb` and ensure the CAS tables exist.
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
        let db_path = std::path::Path::new(persist_dir).join("blob.redb");
        // CAP redb's page cache (default 1 GiB!) — the CAS stores multi-MB chunk
        // VALUES, so the default cache lets RSS track the whole blob size even WITH
        // group-commit (the page cache, not the open txn, is what balloons). A small
        // cap is the real bound: redb evicts cached pages past it, so peak RSS stays
        // near the cap regardless of blob size. 128 MiB default (≈ 2× the 64 MiB
        // group window); tunable via `EPISTEMIC_GRAPH_BLOB_CACHE_BYTES`.
        let cache_bytes = std::env::var("EPISTEMIC_GRAPH_BLOB_CACHE_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(128 * 1024 * 1024usize);
        let db = Database::builder()
            .set_cache_size(cache_bytes)
            .create(&db_path)
            .map_err(|e| e.to_string())?;
        // Create all tables so a read txn against a fresh DB doesn't error.
        let wtx = db.begin_write().map_err(|e| e.to_string())?;
        wtx.open_table(CAS_CHUNKS).map_err(|e| e.to_string())?;
        wtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
        wtx.open_table(CAS_REFCOUNT).map_err(|e| e.to_string())?;
        wtx.commit().map_err(|e| e.to_string())?;
        let group = std::env::var("EPISTEMIC_GRAPH_BLOB_GROUP_CHUNKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_GROUP_CHUNKS);
        Ok(Self {
            db: Arc::new(db),
            batch: parking_lot::Mutex::new(ChunkBatch {
                txn: None,
                pending: 0,
                group,
            }),
        })
    }

    /// Open an in-memory-only CAS backed by a temp dir (tests). The dir is left to
    /// the OS temp reaper; callers that want cleanup pass their own dir to `open`.
    #[cfg(test)]
    pub fn open_temp() -> Result<Self, String> {
        let dir = std::env::temp_dir().join(format!(
            "eg-blob-cas-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        Self::open(&dir.to_string_lossy())
    }
}

impl ChunkStore for RedbChunkStore {
    fn put_chunk(&self, bytes: &[u8]) -> Result<(String, bool), String> {
        let digest = hex_digest(bytes);
        let mut batch = self.batch.lock();
        // Open the group's write txn lazily; reuse it across chunks in the window.
        if batch.txn.is_none() {
            let mut wtx = self.db.begin_write().map_err(|e| e.to_string())?;
            wtx.set_durability(Durability::Immediate)
                .map_err(|e| e.to_string())?;
            batch.txn = Some(wtx);
            batch.pending = 0;
        }
        let was_new = {
            let wtx = batch.txn.as_ref().expect("txn just opened");
            let mut t = wtx.open_table(CAS_CHUNKS).map_err(|e| e.to_string())?;
            if t.get(digest.as_str()).map_err(|e| e.to_string())?.is_some() {
                false
            } else {
                t.insert(digest.as_str(), bytes)
                    .map_err(|e| e.to_string())?;
                true
            }
        };
        if was_new {
            batch.pending += 1;
        }
        // Group-commit boundary: flush every `group` buffered chunks so redb's
        // dirty-page set (peak RAM) never exceeds the group window — independent of
        // the blob size. One `Immediate` fsync amortizes the whole group.
        if batch.pending >= batch.group {
            if let Some(wtx) = batch.txn.take() {
                wtx.commit().map_err(|e| e.to_string())?;
            }
            batch.pending = 0;
        }
        Ok((digest, was_new))
    }

    fn get_chunk(&self, digest: &str) -> Result<Option<Vec<u8>>, String> {
        // A read must see all committed chunks: flush the open group first so a
        // just-uploaded chunk is durable + visible (redb reads don't see an
        // uncommitted write txn).
        self.flush_chunks()?;
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let t = rtx.open_table(CAS_CHUNKS).map_err(|e| e.to_string())?;
        Ok(t.get(digest)
            .map_err(|e| e.to_string())?
            .map(|g| g.value().to_vec()))
    }

    fn put_manifest(&self, blob_digest: &str, manifest: &BlobManifest) -> Result<(), String> {
        // BlobCommit lands here: flush the upload's final partial chunk group first
        // (the manifest's chunks must all be durable before the manifest references
        // them), then write the manifest in its own durable txn.
        self.flush_chunks()?;
        let bytes = rmp_serde::to_vec_named(manifest).map_err(|e| e.to_string())?;
        let mut wtx = self.db.begin_write().map_err(|e| e.to_string())?;
        wtx.set_durability(Durability::Immediate)
            .map_err(|e| e.to_string())?;
        {
            let mut t = wtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
            t.insert(blob_digest, bytes.as_slice())
                .map_err(|e| e.to_string())?;
        }
        wtx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn get_manifest(&self, blob_digest: &str) -> Result<Option<BlobManifest>, String> {
        self.flush_chunks()?;
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let t = rtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
        match t.get(blob_digest).map_err(|e| e.to_string())? {
            Some(g) => Ok(Some(
                rmp_serde::from_slice(g.value()).map_err(|e| e.to_string())?,
            )),
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
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let t = rtx.open_table(CAS_REFCOUNT).map_err(|e| e.to_string())?;
        Ok(t.get(blob_digest)
            .map_err(|e| e.to_string())?
            .map(|g| g.value())
            .unwrap_or(0))
    }

    fn sweep(&self) -> Result<SweepStats, String> {
        self.flush_chunks()?;
        let mut wtx = self.db.begin_write().map_err(|e| e.to_string())?;
        wtx.set_durability(Durability::Immediate)
            .map_err(|e| e.to_string())?;
        let mut stats = SweepStats::default();
        {
            // Phase 1 (MARK dead): collect every blob with refcount 0.
            let dead: Vec<String> = {
                let refs = wtx.open_table(CAS_REFCOUNT).map_err(|e| e.to_string())?;
                let mut dead = Vec::new();
                for row in refs.iter().map_err(|e| e.to_string())? {
                    let (k, v) = row.map_err(|e| e.to_string())?;
                    if v.value() == 0 {
                        dead.push(k.value().to_string());
                    }
                }
                dead
            };
            // Also treat a manifest with NO refcount entry as dead (an
            // uncommitted/abandoned upload that was never referenced).
            let orphan_manifests: Vec<String> = {
                let blobs = wtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
                let refs = wtx.open_table(CAS_REFCOUNT).map_err(|e| e.to_string())?;
                let mut orphans = Vec::new();
                for row in blobs.iter().map_err(|e| e.to_string())? {
                    let (k, _) = row.map_err(|e| e.to_string())?;
                    let d = k.value();
                    if refs.get(d).map_err(|e| e.to_string())?.is_none() {
                        orphans.push(d.to_string());
                    }
                }
                orphans
            };
            let mut to_delete: HashSet<String> = dead.into_iter().collect();
            to_delete.extend(orphan_manifests);

            // Phase 2 (SWEEP): recompute the LIVE chunk set from the manifests that
            // SURVIVE (not in to_delete), so any chunk a live blob still lists is
            // kept even if a dead blob also listed it.
            let live_chunks: HashSet<String> = {
                let blobs = wtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
                let mut live = HashSet::new();
                for row in blobs.iter().map_err(|e| e.to_string())? {
                    let (k, v) = row.map_err(|e| e.to_string())?;
                    if to_delete.contains(k.value()) {
                        continue;
                    }
                    let m: BlobManifest =
                        rmp_serde::from_slice(v.value()).map_err(|e| e.to_string())?;
                    live.extend(m.chunks);
                }
                live
            };

            // Collect the chunks the dying blobs reference, minus the live set =
            // the orphan chunks to reclaim.
            let orphan_chunks: HashSet<String> = {
                let blobs = wtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
                let mut orphans = HashSet::new();
                for digest in &to_delete {
                    if let Some(v) = blobs.get(digest.as_str()).map_err(|e| e.to_string())? {
                        let m: BlobManifest =
                            rmp_serde::from_slice(v.value()).map_err(|e| e.to_string())?;
                        for c in m.chunks {
                            if !live_chunks.contains(&c) {
                                orphans.insert(c);
                            }
                        }
                    }
                }
                orphans
            };

            // Phase 3 (DELETE): remove dead manifests + their refcount entries +
            // the orphan chunks.
            {
                let mut blobs = wtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
                let mut refs = wtx.open_table(CAS_REFCOUNT).map_err(|e| e.to_string())?;
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
                let mut chunks = wtx.open_table(CAS_CHUNKS).map_err(|e| e.to_string())?;
                for c in &orphan_chunks {
                    if chunks
                        .remove(c.as_str())
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        stats.chunks_reclaimed += 1;
                    }
                }
            }
        }
        wtx.commit().map_err(|e| e.to_string())?;
        Ok(stats)
    }

    fn chunk_count(&self) -> Result<u64, String> {
        self.flush_chunks()?;
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let t = rtx.open_table(CAS_CHUNKS).map_err(|e| e.to_string())?;
        t.len().map_err(|e| e.to_string())
    }

    fn blob_count(&self) -> Result<u64, String> {
        self.flush_chunks()?;
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let t = rtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
        t.len().map_err(|e| e.to_string())
    }
}

impl Drop for RedbChunkStore {
    /// Flush a half-full chunk group on drop so an in-flight upload's chunks aren't
    /// rolled back with the open `WriteTransaction` (they'd otherwise be re-uploaded;
    /// no data loss, but wasteful). Best-effort — a commit error at drop is ignored.
    fn drop(&mut self) {
        let _ = self.flush_chunks();
    }
}

impl RedbChunkStore {
    /// Commit any open chunk-write group durably, closing the txn. Called before any
    /// other operation (read or a non-chunk write) since redb permits exactly one
    /// open write txn at a time, and so a reader sees the latest chunks. A `commit`
    /// failure surfaces here (the group's chunks did NOT land).
    fn flush_chunks(&self) -> Result<(), String> {
        let mut batch = self.batch.lock();
        if let Some(wtx) = batch.txn.take() {
            wtx.commit().map_err(|e| e.to_string())?;
        }
        batch.pending = 0;
        Ok(())
    }

    /// Chunks that the NEXT [`sweep`](ChunkStore::sweep) would orphan: chunks
    /// referenced ONLY by dead blobs (refcount 0 or no refcount entry) and by no
    /// surviving manifest. Used by the S3 backend to delete the matching objects
    /// before the sidecar sweep drops the manifests. Read-only.
    #[cfg(feature = "blob-s3")]
    pub(crate) fn orphan_chunks_preview(&self) -> Result<HashSet<String>, String> {
        self.flush_chunks()?;
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let blobs = rtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
        let refs = rtx.open_table(CAS_REFCOUNT).map_err(|e| e.to_string())?;
        let mut to_delete: HashSet<String> = HashSet::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (k, _) = row.map_err(|e| e.to_string())?;
            let d = k.value();
            let dead = refs
                .get(d)
                .map_err(|e| e.to_string())?
                .map(|g| g.value() == 0)
                .unwrap_or(true);
            if dead {
                to_delete.insert(d.to_string());
            }
        }
        let mut live_chunks: HashSet<String> = HashSet::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            if to_delete.contains(k.value()) {
                continue;
            }
            let m: BlobManifest = rmp_serde::from_slice(v.value()).map_err(|e| e.to_string())?;
            live_chunks.extend(m.chunks);
        }
        let mut orphans = HashSet::new();
        for digest in &to_delete {
            if let Some(v) = blobs.get(digest.as_str()).map_err(|e| e.to_string())? {
                let m: BlobManifest =
                    rmp_serde::from_slice(v.value()).map_err(|e| e.to_string())?;
                for c in m.chunks {
                    if !live_chunks.contains(&c) {
                        orphans.insert(c);
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
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let blobs = rtx.open_table(CAS_BLOBS).map_err(|e| e.to_string())?;
        let mut seen: HashSet<String> = HashSet::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (_, v) = row.map_err(|e| e.to_string())?;
            let m: BlobManifest = rmp_serde::from_slice(v.value()).map_err(|e| e.to_string())?;
            seen.extend(m.chunks);
        }
        Ok(seen.len() as u64)
    }

    /// Adjust a blob's refcount by `delta`, saturating at 0. Durable per call.
    fn adjust_ref(&self, blob_digest: &str, delta: i64) -> Result<u64, String> {
        self.flush_chunks()?;
        let mut wtx = self.db.begin_write().map_err(|e| e.to_string())?;
        wtx.set_durability(Durability::Immediate)
            .map_err(|e| e.to_string())?;
        let new = {
            let mut t = wtx.open_table(CAS_REFCOUNT).map_err(|e| e.to_string())?;
            let cur = t
                .get(blob_digest)
                .map_err(|e| e.to_string())?
                .map(|g| g.value())
                .unwrap_or(0);
            let new = if delta >= 0 {
                cur.saturating_add(delta as u64)
            } else {
                cur.saturating_sub((-delta) as u64)
            };
            t.insert(blob_digest, new).map_err(|e| e.to_string())?;
            new
        };
        wtx.commit().map_err(|e| e.to_string())?;
        Ok(new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// BOUNDED MEMORY (the flagged correctness risk). Streams a LARGE blob through
    /// the group-commit CAS one chunk at a time — generated, stored, and dropped —
    /// then streams it back re-hashing chunk-by-chunk, retaining NEITHER the whole
    /// blob NOR all chunks. Peak RSS must stay near the group window (≈64 MiB at 32×
    /// 2 MiB), INDEPENDENT of the blob size: a regression to per-chunk
    /// `Durability::None` (redb buffering every dirty page) would make RSS track the
    /// file size and trip the assert. Default 256 MB (4× the window, enough to catch
    /// the bug per the spike — 64/256 MB passes did NOT trip the None-leak only
    /// because the assert is window-relative here); `EG_BLOB_RSS_MB` runs 1GB+.
    #[test]
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

        // Bounded memory: peak RSS GROWTH over the run must be a small multiple of
        // the group window, NOT the blob size. Window = 32×2MiB = 64MiB; allow a
        // generous 256MB headroom for redb page cache + the test binary, but a
        // file-size leak (256MB..1GB+) blows past it.
        let peak = peak_rss_mb();
        let growth = peak.saturating_sub(rss_before);
        // Bound = the capped redb page cache (128 MiB default) + headroom for redb's
        // mmap working set and transient buffers — NOT the blob size. A regression
        // that uncaps the cache or buffers per-chunk-None would track the file size
        // (1GB blob → ~1GB RSS) and blow past this; the cap holds it flat.
        assert!(
            growth < 320,
            "peak RSS growth {growth}MB for a {total_mb}MB blob must be bounded by the \
             capped page cache (~128MiB), not the file size — an uncapped-cache or \
             per-chunk-None regression would track the file size"
        );
    }

    /// CONCEPT:EG-071 backward read — a PRE-EG-071 manifest (serialized with only
    /// `chunks`/`len`/fixed `chunk_size`, no `chunk_lens`) still deserializes, its
    /// chunk lengths reconstruct from the fixed stride, and the blob reassembles
    /// byte-for-byte (the chunk bytes are self-describing, so no migration is needed).
    #[test]
    fn legacy_fixed_size_manifest_reads_back() {
        let store = RedbChunkStore::open_temp().unwrap();
        let data: Vec<u8> = (0u32..10_000).map(|i| (i % 251) as u8).collect();
        let chunk_size = 4096usize;

        // Store the chunks exactly as the old fixed splitter would have.
        let mut chunks = Vec::new();
        for part in data.chunks(chunk_size) {
            chunks.push(store.put_chunk(part).unwrap().0);
        }

        // Serialize an OLD-SHAPE manifest (no `chunk_lens` field at all) and persist
        // it under its own digest, exactly as a pre-EG-071 build wrote it.
        #[derive(Serialize)]
        struct LegacyManifest {
            chunks: Vec<String>,
            len: u64,
            chunk_size: u32,
        }
        let legacy = LegacyManifest {
            chunks: chunks.clone(),
            len: data.len() as u64,
            chunk_size: chunk_size as u32,
        };
        let bytes = rmp_serde::to_vec_named(&legacy).unwrap();
        let blob_digest = hex_digest(&bytes);
        {
            // Write the legacy bytes straight into the CAS_BLOBS table.
            let wtx = store.db.begin_write().unwrap();
            {
                let mut t = wtx.open_table(CAS_BLOBS).unwrap();
                t.insert(blob_digest.as_str(), bytes.as_slice()).unwrap();
            }
            wtx.commit().unwrap();
        }

        // It deserializes into the new BlobManifest with an empty chunk_lens.
        let m = store.get_manifest(&blob_digest).unwrap().unwrap();
        assert!(m.chunk_lens.is_empty(), "legacy manifest has no chunk_lens");
        assert_eq!(m.chunk_size, chunk_size as u32);

        // Reconstructed lengths: full strides then a short final chunk.
        let lens = m.chunk_lengths();
        assert_eq!(
            lens.iter().map(|&l| l as u64).sum::<u64>(),
            data.len() as u64
        );
        assert_eq!(*lens.first().unwrap(), chunk_size as u32);
        assert!(*lens.last().unwrap() <= chunk_size as u32);
        // Offsets are the running prefix sum.
        assert_eq!(m.chunk_offsets().first().unwrap().0, 0);

        // And the blob still reassembles byte-for-byte.
        assert_eq!(reassemble(&store, &m), data);
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
