//! Streamed, content-addressed BLOB substrate (CONCEPT:KG-2.206).
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

#[cfg(feature = "blob-s3")]
pub mod s3;

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub use store::{BlobManifest, ChunkStore, RedbChunkStore, SweepStats, DEFAULT_CHUNK_SIZE};

/// An open UPLOAD cursor. It does NOT buffer the whole file: each arriving chunk is
/// hashed + written to the CAS immediately and only its digest is appended here, so
/// the resident memory for an upload is ~(len/chunk_size)·~70 bytes of digests —
/// kilobytes, not gigabytes.
pub struct UploadCursor {
    pub chunk_digests: Vec<String>,
    pub len: u64,
    pub chunk_size: u32,
    /// Last-activity wall-clock ms, for the idle TTL reaper.
    pub last_active_ms: u64,
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
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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

    /// Open an upload cursor; returns its id.
    pub fn open_upload(&self, chunk_size: u32) -> u64 {
        let id = self.alloc();
        self.uploads.insert(
            id,
            UploadCursor {
                chunk_digests: Vec::new(),
                len: 0,
                chunk_size,
                last_active_ms: now_ms(),
            },
        );
        id
    }

    /// Append a stored chunk's digest to an open upload cursor. Returns the running
    /// chunk count, or `Err` if the cursor is unknown.
    pub fn push_chunk(&self, cursor: u64, digest: String, added_len: u64) -> Result<u32, String> {
        let mut up = self
            .uploads
            .get_mut(&cursor)
            .ok_or_else(|| "unknown upload cursor".to_string())?;
        up.len += added_len;
        up.chunk_digests.push(digest);
        up.last_active_ms = now_ms();
        Ok(up.chunk_digests.len() as u32)
    }

    /// Finalize an upload cursor → its [`BlobManifest`]. Drops the cursor.
    pub fn take_upload(&self, cursor: u64) -> Result<BlobManifest, String> {
        let (_, up) = self
            .uploads
            .remove(&cursor)
            .ok_or_else(|| "unknown upload cursor".to_string())?;
        Ok(BlobManifest {
            chunks: up.chunk_digests,
            len: up.len,
            chunk_size: up.chunk_size,
        })
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
        let up = c.open_upload(8);
        // Stream three tiny chunks through the store + cursor.
        for part in [b"aaaa".as_slice(), b"bbbb", b"cc"] {
            let (digest, _) = c.store.put_chunk(part).unwrap();
            c.push_chunk(up, digest, part.len() as u64).unwrap();
        }
        let manifest = c.take_upload(up).unwrap();
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
        let up = c.open_upload(8);
        let (_, manifest) = (
            up,
            BlobManifest {
                chunks: vec![],
                len: 0,
                chunk_size: 8,
            },
        );
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
        assert!(c.push_chunk(999, "x".into(), 1).is_err());
        assert!(c.take_upload(999).is_err());
        assert!(c.fetch_chunk_digest(999, 0).is_err());
    }
}
