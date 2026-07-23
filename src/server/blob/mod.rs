//! Streamed, content-addressed BLOB substrate (CONCEPT:EG-KG.storage.blob-namespace).
//!
//! The bytes tier under multimodal media (image/video/audio/file). A blob is
//! transferred as MANY ordinary one-Response-per-Request frames sharing a
//! server-side cursor — NOT a side-channel socket, NOT gRPC. The whole file is
//! never resident on either side; at most one chunk is in flight.
//!
//! Layout:
//! * [`store`] — the DAG-low content-addressed store: a `ChunkStore` trait + a
//!   native redb implementation (default) + refcount mark-and-sweep GC.
//! * [`s3`] (feature `blob-s3`) — an object-store backend behind the SAME trait.
//! * this module — the server-side cursor state ([`BlobCursors`]) that the
//!   protocol handler drives, with a TTL reaper mirroring the OCC-txn `open_txns`
//!   pattern.

pub mod store;

// Content-defined chunking (CONCEPT:EG-KG.storage.backward-manifest-read): the Gear/FastCDC rolling-hash splitter
// that replaces fixed-stride chunking, so the sha256 CAS dedups edited copies.
pub mod cdc;

// In-process bounded-memory streaming facade over the CAS (CONCEPT:EG-KG.sharding.m3-r4, M3 R4):
// stream a multi-GB blob between an arbitrary `Read`/`Write` and the CAS without
// buffering the whole blob. The in-process twin of the wire upload/fetch cursor.
pub mod stream;

#[cfg(feature = "blob-s3")]
pub mod s3;

// A real, engine-backed `eg_alignment::EvidenceResolver` (L21, CONCEPT:EG-P1-3
// follow-up): resolves a located `EvidenceLocus` through a `GraphView` snapshot's
// own stored `blob_ref` property, then reads the actual bytes back out of THIS
// module's `ChunkStore` — see `cas_resolver`'s module docs for why it lives here
// rather than inside `eg-alignment` itself. Behind the `alignment` feature (implies
// `blob`); a build without it compiles none of it.
#[cfg(feature = "alignment")]
pub mod cas_resolver;

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use store::{
    BlobManifest, ChunkStore, RedbChunkStore, SweepStats, BLOB_MANIFEST_VERSION,
    DEFAULT_CHUNK_SIZE, ENGINE_BLOB_OWNER_SCOPE,
};

/// An open UPLOAD cursor. It does NOT buffer the whole file: each arriving chunk is
/// hashed + written to the CAS immediately and only its digest is appended here, so
/// the resident memory for an upload is ~(len/chunk_size)·~70 bytes of digests —
/// kilobytes, not gigabytes.
pub struct UploadCursor {
    /// Verified tenant+principal owner of the cursor.
    pub owner_scope: String,
    pub chunk_digests: Vec<String>,
    /// Per-chunk byte lengths, parallel to `chunk_digests` (CONCEPT:EG-KG.storage.backward-manifest-read): the
    /// variable content-defined boundaries the manifest records. Chunk boundaries
    /// arrive pre-cut on the wire (the client chunks — `BlobChunkPut` carries one
    /// chunk per frame), so the server records whatever lengths it is fed; the
    /// in-process splitter ([`stream::stream_blob_put`]) cuts them with the
    /// [`cdc::Chunker`].
    pub chunk_lens: Vec<u32>,
    pub len: u64,
    pub chunk_size: u32,
    /// Last-activity wall-clock ms, for the idle TTL reaper.
    pub last_active_ms: u64,
    /// Opaque MutationBatch ids already projected into this cursor. This makes an
    /// ack-lost `BlobChunkPut` replay idempotent without storing payload or caller
    /// identity in memory.
    pub applied_batches: std::collections::HashSet<String>,
}

/// An open FETCH cursor: just the manifest. Chunks are pulled from the CAS one at a
/// time on demand — never materialized together.
pub struct FetchCursor {
    pub manifest: BlobManifest,
    pub last_active_ms: u64,
}

/// Wall-clock milliseconds since the epoch (monotonic enough for TTL; tolerates
/// clock skew the same way the OCC-txn sweep does).
pub fn now_ms() -> u64 {
    crate::server::dispatch::authoritative_now_ms()
}

/// Server-side blob cursor state, held on [`ServerState`](crate::server::ServerState).
/// Keyed by a server-issued u64 cursor id (NOT by connection — the protocol is
/// stateless and threads the cursor in the request body, exactly like `txn_id`).
pub struct BlobCursors {
    pub store: Arc<dyn ChunkStore>,
    uploads: DashMap<u64, UploadCursor>,
    fetches: DashMap<u64, FetchCursor>,
    next_id: AtomicU64,
}

impl BlobCursors {
    pub fn new(store: Arc<dyn ChunkStore>) -> Self {
        Self {
            store,
            uploads: DashMap::new(),
            fetches: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    fn alloc(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Reserve an id without publishing cursor state. The handler first commits
    /// this id as the native batch result, then restores the owned upload row.
    pub fn allocate_upload_id(&self) -> u64 {
        self.alloc()
    }

    /// Rebuild the in-memory serving projection from the authoritative durable
    /// upload row after startup or an ack-lost retry.
    pub fn restore_upload_manifest(&self, id: u64, manifest: BlobManifest) -> Result<(), String> {
        manifest.validate()?;
        self.next_id
            .fetch_max(id.saturating_add(1), Ordering::Relaxed);
        self.uploads.insert(
            id,
            UploadCursor {
                owner_scope: manifest.owner_scope.clone(),
                chunk_digests: manifest.chunks,
                chunk_lens: manifest.chunk_lens,
                len: manifest.len,
                chunk_size: manifest.chunk_size,
                last_active_ms: now_ms(),
                applied_batches: std::collections::HashSet::new(),
            },
        );
        Ok(())
    }

    pub fn authorize_upload(&self, cursor: u64, owner_scope: &str) -> Result<(), String> {
        let upload = self
            .uploads
            .get(&cursor)
            .ok_or_else(|| "unknown upload cursor".to_string())?;
        if !owner_scope.is_empty() && upload.owner_scope == owner_scope {
            Ok(())
        } else {
            Err("unknown upload cursor".to_string())
        }
    }

    /// Idempotent serving-projection update for one durably committed chunk batch.
    pub fn push_chunk_once(
        &self,
        cursor: u64,
        batch_id: &str,
        digest: String,
        added_len: u64,
    ) -> Result<u32, String> {
        let mut up = self
            .uploads
            .get_mut(&cursor)
            .ok_or_else(|| "unknown upload cursor".to_string())?;
        if !up.applied_batches.insert(batch_id.to_string()) {
            return Ok(up.chunk_digests.len() as u32);
        }
        up.len += added_len;
        up.chunk_digests.push(digest);
        up.chunk_lens.push(added_len as u32);
        up.last_active_ms = now_ms();
        Ok(up.chunk_digests.len() as u32)
    }

    /// Snapshot a manifest without consuming the upload cursor. Consumption is a
    /// serving projection performed only after the manifest batch commits.
    pub fn snapshot_upload(&self, cursor: u64) -> Result<BlobManifest, String> {
        let up = self
            .uploads
            .get(&cursor)
            .ok_or_else(|| "unknown upload cursor".to_string())?;
        Ok(BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: up.owner_scope.clone(),
            chunks: up.chunk_digests.clone(),
            chunk_lens: up.chunk_lens.clone(),
            len: up.len,
            chunk_size: up.chunk_size,
        })
    }

    pub fn finish_upload(&self, cursor: u64) {
        self.uploads.remove(&cursor);
    }

    /// Open a fetch cursor over a stored manifest; returns `(cursor, n_chunks)`.
    pub fn open_fetch(&self, manifest: BlobManifest) -> (u64, u32) {
        let n = manifest.chunks.len() as u32;
        let id = self.alloc();
        self.fetches.insert(
            id,
            FetchCursor {
                manifest,
                last_active_ms: now_ms(),
            },
        );
        (id, n)
    }

    pub fn authorize_fetch(&self, cursor: u64, owner_scope: &str) -> Result<(), String> {
        let fetch = self
            .fetches
            .get(&cursor)
            .ok_or_else(|| "unknown fetch cursor".to_string())?;
        if !owner_scope.is_empty() && fetch.manifest.owner_scope == owner_scope {
            Ok(())
        } else {
            Err("unknown fetch cursor".to_string())
        }
    }

    /// The chunk digest at `idx` of an open fetch cursor.
    pub fn fetch_chunk_digest(&self, cursor: u64, idx: u32) -> Result<String, String> {
        let mut f = self
            .fetches
            .get_mut(&cursor)
            .ok_or_else(|| "unknown fetch cursor".to_string())?;
        let digest = f
            .manifest
            .chunks
            .get(idx as usize)
            .cloned()
            .ok_or_else(|| "chunk idx out of range".to_string())?;
        f.last_active_ms = now_ms();
        Ok(digest)
    }

    /// Drop a fetch cursor (client done streaming down). Idempotent.
    pub fn close_fetch(&self, cursor: u64) {
        self.fetches.remove(&cursor);
    }

    /// Reap upload + fetch cursors idle past `ttl_secs`. Returns the count reaped.
    /// Mirrors `sweep_expired_txns` — frees abandoned cursor memory (an abandoned
    /// upload has its chunks in the CAS as orphans, reclaimed by the blob GC sweep).
    pub fn reap_idle(&self, ttl_secs: u64, now: u64) -> usize {
        let ttl_ms = ttl_secs.saturating_mul(1000);
        let mut reaped = 0;
        let expired_up: Vec<u64> = self
            .uploads
            .iter()
            .filter(|e| now.saturating_sub(e.value().last_active_ms) >= ttl_ms)
            .map(|e| *e.key())
            .collect();
        for id in expired_up {
            self.uploads.remove(&id);
            reaped += 1;
        }
        let expired_fetch: Vec<u64> = self
            .fetches
            .iter()
            .filter(|e| now.saturating_sub(e.value().last_active_ms) >= ttl_ms)
            .map(|e| *e.key())
            .collect();
        for id in expired_fetch {
            self.fetches.remove(&id);
            reaped += 1;
        }
        reaped
    }

    /// Count of currently-open cursors (uploads + fetches) — observability/tests.
    pub fn open_count(&self) -> usize {
        self.uploads.len() + self.fetches.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursors() -> BlobCursors {
        BlobCursors::new(Arc::new(RedbChunkStore::open_temp().unwrap()))
    }

    #[test]
    fn upload_then_fetch_cursor_lifecycle() {
        let c = cursors();
        let up = c.allocate_upload_id();
        c.restore_upload_manifest(
            up,
            BlobManifest {
                schema_version: BLOB_MANIFEST_VERSION,
                owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
                chunks: Vec::new(),
                chunk_lens: Vec::new(),
                len: 0,
                chunk_size: 8,
            },
        )
        .unwrap();
        // Stream three tiny chunks through the store + cursor.
        for (ordinal, part) in [b"aaaa".as_slice(), b"bbbb", b"cc"].into_iter().enumerate() {
            let (digest, _) = c.store.put_chunk(part).unwrap();
            c.push_chunk_once(up, &format!("batch-{ordinal}"), digest, part.len() as u64)
                .unwrap();
        }
        let manifest = c.snapshot_upload(up).unwrap();
        c.finish_upload(up);
        assert_eq!(manifest.len, 10);
        assert_eq!(manifest.chunks.len(), 3);
        assert!(
            c.uploads.get(&up).is_none(),
            "upload cursor dropped on commit"
        );

        let (fc, n) = c.open_fetch(manifest);
        assert_eq!(n, 3);
        let d0 = c.fetch_chunk_digest(fc, 0).unwrap();
        assert_eq!(c.store.get_chunk(&d0).unwrap().unwrap(), b"aaaa");
        assert!(c.fetch_chunk_digest(fc, 9).is_err(), "out of range");
        c.close_fetch(fc);
        assert_eq!(c.open_count(), 0);
    }

    #[test]
    fn reaper_reclaims_idle_cursors() {
        let c = cursors();
        let up = c.allocate_upload_id();
        let manifest = BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
            chunks: vec![],
            chunk_lens: vec![],
            len: 0,
            chunk_size: 8,
        };
        c.restore_upload_manifest(up, manifest.clone()).unwrap();
        let (_fc, _) = c.open_fetch(manifest);
        assert_eq!(c.open_count(), 2);
        // A reap with a far-future "now" (TTL elapsed) reclaims both.
        let reaped = c.reap_idle(300, now_ms() + 1_000_000);
        assert_eq!(reaped, 2);
        assert_eq!(c.open_count(), 0);
    }

    #[test]
    fn unknown_cursor_errs() {
        let c = cursors();
        assert!(c.push_chunk_once(999, "batch", "x".into(), 1).is_err());
        assert!(c.snapshot_upload(999).is_err());
        assert!(c.fetch_chunk_digest(999, 0).is_err());
    }

    #[test]
    fn upload_and_fetch_cursors_are_owner_bound() {
        let c = cursors();
        c.restore_upload_manifest(
            41,
            BlobManifest {
                schema_version: BLOB_MANIFEST_VERSION,
                owner_scope: "tenant-a/alice".to_string(),
                chunks: Vec::new(),
                chunk_lens: Vec::new(),
                len: 0,
                chunk_size: 8,
            },
        )
        .unwrap();
        assert!(c.authorize_upload(41, "tenant-a/alice").is_ok());
        assert!(c.authorize_upload(41, "tenant-a/bob").is_err());
        assert!(c.authorize_upload(41, "tenant-b/alice").is_err());

        let manifest = BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: "tenant-a/alice".to_string(),
            chunks: Vec::new(),
            chunk_lens: Vec::new(),
            len: 0,
            chunk_size: 0,
        };
        let (fetch, _) = c.open_fetch(manifest);
        assert!(c.authorize_fetch(fetch, "tenant-a/alice").is_ok());
        assert!(c.authorize_fetch(fetch, "tenant-a/bob").is_err());
        assert!(c.authorize_fetch(fetch, "tenant-b/alice").is_err());
    }
}
