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

pub mod engine_bodies;
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
    BlobManifest, BlobRetentionPolicy, ChunkStore, GcOwnerScope, RedbChunkStore, SweepRequest,
    SweepStats, BLOB_MANIFEST_VERSION, DEFAULT_CHUNK_SIZE, ENGINE_BLOB_OWNER_SCOPE,
};

/// The serving projection of one durable upload: who may append to it and when it
/// was last touched. The chunk list itself is only in the durable `cas_uploads`
/// row, so nothing here can disagree with what a commit assembles.
pub struct UploadCursor {
    /// Verified tenant+principal owner of the cursor.
    pub owner_scope: String,
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
    crate::server::dispatch::authoritative_now_ms()
}

/// Server-side blob cursor state, held on [`ServerState`](crate::server::ServerState).
/// Keyed by a server-issued u64 cursor id (NOT by connection — the protocol is
/// stateless and threads the cursor in the request body, exactly like `txn_id`).
///
/// Upload cursor ids are never reused, across restarts included: the first
/// allocation seeds the counter above the store's durable high-water mark, and
/// every durable begin raises that mark in its own transaction.
pub struct BlobCursors {
    pub store: Arc<dyn ChunkStore>,
    uploads: DashMap<u64, UploadCursor>,
    fetches: DashMap<u64, FetchCursor>,
    next_id: AtomicU64,
    /// Whether `next_id` has been raised above the durable high-water mark.
    upload_ids_seeded: parking_lot::Mutex<bool>,
    retention: BlobRetentionPolicy,
}

/// The upload-id seeding state and the retention policy every fresh
/// [`BlobCursors`] starts from, factored out so its constructor stays a plain
/// field list.
fn fresh_retention_state() -> (parking_lot::Mutex<bool>, BlobRetentionPolicy) {
    (
        parking_lot::Mutex::new(false),
        BlobRetentionPolicy::default(),
    )
}

impl BlobCursors {
    pub fn new(store: Arc<dyn ChunkStore>) -> Self {
        let (upload_ids_seeded, retention) = fresh_retention_state();
        Self {
            store,
            uploads: DashMap::new(),
            fetches: DashMap::new(),
            next_id: AtomicU64::new(1),
            upload_ids_seeded,
            retention,
        }
    }

    /// Serve garbage collection under `retention` instead of the default policy.
    pub fn with_retention(mut self, retention: BlobRetentionPolicy) -> Self {
        self.retention = retention;
        self
    }

    /// The retention policy `BlobGc` sweeps under.
    pub fn retention(&self) -> BlobRetentionPolicy {
        self.retention
    }

    /// Reserve a never-used upload cursor id without publishing cursor state. The
    /// handler first commits this id as the native batch result, then restores the
    /// owned upload row.
    pub fn allocate_upload_id(&self) -> Result<u64, String> {
        self.seed_upload_ids_once()?;
        Ok(self.next_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Raise `next_id` above the store's durable high-water mark on the FIRST
    /// call only, so a restarted allocator never proposes an id any process
    /// ever began (see [`allocate_upload_id`](Self::allocate_upload_id)).
    fn seed_upload_ids_once(&self) -> Result<(), String> {
        let mut seeded = self.upload_ids_seeded.lock();
        if *seeded {
            return Ok(());
        }
        let high_water = self.store.upload_cursor_high_water()?;
        self.next_id
            .fetch_max(high_water.saturating_add(1), Ordering::SeqCst);
        *seeded = true;
        Ok(())
    }

    /// Rebuild the in-memory serving projection from the authoritative durable
    /// upload row after startup or an ack-lost retry.
    pub fn restore_upload_manifest(&self, id: u64, manifest: BlobManifest) -> Result<(), String> {
        manifest.validate()?;
        self.next_id
            .fetch_max(id.saturating_add(1), Ordering::SeqCst);
        self.uploads.insert(
            id,
            UploadCursor {
                owner_scope: manifest.owner_scope,
                last_active_ms: now_ms(),
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

    pub fn finish_upload(&self, cursor: u64) {
        self.uploads.remove(&cursor);
    }

    /// Open a fetch cursor over a stored manifest; returns `(cursor, n_chunks)`.
    pub fn open_fetch(&self, manifest: BlobManifest) -> (u64, u32) {
        let n = manifest.chunks.len() as u32;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
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
    /// Mirrors `sweep_expired_txns` — frees abandoned cursor memory only; the
    /// durable upload row stays until `BlobGc` expires it past the upload TTL.
    pub fn reap_idle(&self, ttl_secs: u64, now: u64) -> usize {
        let ttl_ms = ttl_secs.saturating_mul(1000);
        reap_expired(&self.uploads, |upload| upload.last_active_ms, now, ttl_ms)
            + reap_expired(&self.fetches, |fetch| fetch.last_active_ms, now, ttl_ms)
    }
}

/// Remove every cursor of `cursors` idle for at least `ttl_ms` at `now`.
fn reap_expired<V>(
    cursors: &DashMap<u64, V>,
    last_active_ms: impl Fn(&V) -> u64,
    now: u64,
    ttl_ms: u64,
) -> usize {
    let expired: Vec<u64> = cursors
        .iter()
        .filter(|entry| now.saturating_sub(last_active_ms(entry.value())) >= ttl_ms)
        .map(|entry| *entry.key())
        .collect();
    for id in &expired {
        cursors.remove(id);
    }
    expired.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursors_over(store: Arc<RedbChunkStore>) -> BlobCursors {
        BlobCursors::new(store)
    }

    fn cursors() -> BlobCursors {
        cursors_over(Arc::new(RedbChunkStore::open_temp().unwrap()))
    }

    fn empty_manifest(owner_scope: &str, chunk_size: u32) -> BlobManifest {
        BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: owner_scope.to_string(),
            chunks: Vec::new(),
            chunk_lens: Vec::new(),
            len: 0,
            chunk_size,
        }
    }

    fn open_counts(c: &BlobCursors) -> usize {
        c.uploads.len() + c.fetches.len()
    }

    #[test]
    fn fetch_cursor_lifecycle() {
        let c = cursors();
        // Stream three tiny chunks straight into the store, exactly as the wire
        // upload path would, then fetch them back through a cursor.
        let mut chunks = Vec::new();
        let mut chunk_lens = Vec::new();
        for part in [b"aaaa".as_slice(), b"bbbb", b"cc"] {
            let (digest, _) = c.store.put_chunk(part).unwrap();
            chunks.push(digest);
            chunk_lens.push(part.len() as u32);
        }
        let manifest = BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
            chunks,
            chunk_lens,
            len: 10,
            chunk_size: 8,
        };
        let (fc, n) = c.open_fetch(manifest);
        assert_eq!(n, 3);
        let d0 = c.fetch_chunk_digest(fc, 0).unwrap();
        assert_eq!(c.store.get_chunk(&d0).unwrap().unwrap(), b"aaaa");
        assert!(c.fetch_chunk_digest(fc, 9).is_err(), "out of range");
        c.close_fetch(fc);
        assert_eq!(open_counts(&c), 0);
    }

    #[test]
    fn reaper_reclaims_idle_cursors() {
        let c = cursors();
        let up = c.allocate_upload_id().unwrap();
        let manifest = empty_manifest(ENGINE_BLOB_OWNER_SCOPE, 8);
        c.restore_upload_manifest(up, manifest.clone()).unwrap();
        let (_fc, _) = c.open_fetch(manifest);
        assert_eq!(open_counts(&c), 2);
        // A reap with a far-future "now" (TTL elapsed) reclaims both.
        let reaped = c.reap_idle(300, now_ms() + 1_000_000);
        assert_eq!(reaped, 2);
        assert_eq!(open_counts(&c), 0);
    }

    #[test]
    fn unknown_cursor_errs() {
        let c = cursors();
        assert!(c.authorize_upload(999, "anyone").is_err());
        assert!(c.fetch_chunk_digest(999, 0).is_err());
        assert!(c.authorize_fetch(999, "anyone").is_err());
    }

    #[test]
    fn upload_and_fetch_cursors_are_owner_bound() {
        let c = cursors();
        c.restore_upload_manifest(41, empty_manifest("tenant-a/alice", 8))
            .unwrap();
        assert!(c.authorize_upload(41, "tenant-a/alice").is_ok());
        assert!(c.authorize_upload(41, "tenant-a/bob").is_err());
        assert!(c.authorize_upload(41, "tenant-b/alice").is_err());

        let (fetch, _) = c.open_fetch(empty_manifest("tenant-a/alice", 0));
        assert!(c.authorize_fetch(fetch, "tenant-a/alice").is_ok());
        assert!(c.authorize_fetch(fetch, "tenant-a/bob").is_err());
        assert!(c.authorize_fetch(fetch, "tenant-b/alice").is_err());
    }

    /// X5 restart safety, the `BlobCursors` half: a fresh allocator over a store
    /// that already has upload rows begun (a simulated restart, since the durable
    /// high-water mark is exactly what a real restart reads back) never proposes
    /// an id at or below one the store has ever begun, even before this process's
    /// own in-memory counter has ever allocated one.
    #[test]
    fn a_fresh_allocator_over_a_restarted_store_never_proposes_a_used_id() {
        let store = Arc::new(RedbChunkStore::open_temp().unwrap());
        let batch = |id: &str| crate::server::blob::store::test_support::fresh(store.as_ref(), id);
        store
            .begin_upload_batch(1, 8, "owner-a", &batch("begin-1"), 1)
            .unwrap();
        store
            .begin_upload_batch(7, 8, "owner-a", &batch("begin-7"), 2)
            .unwrap();
        assert_eq!(store.upload_cursor_high_water().unwrap(), 7);

        // A brand-new `BlobCursors` (what a restarted process constructs) has
        // never allocated anything itself, yet must not propose 1..=7.
        let restarted = cursors_over(Arc::clone(&store));
        let proposed = restarted.allocate_upload_id().unwrap();
        assert!(
            proposed > 7,
            "proposed {proposed} collides with a surviving upload"
        );

        // The seeding happens once: a second allocation just increments.
        let next = restarted.allocate_upload_id().unwrap();
        assert_eq!(next, proposed + 1);
    }

    /// `restore_upload_manifest` also raises the in-memory counter directly (the
    /// ack-lost-retry path, which restores a specific id before the allocator
    /// would otherwise have reached it).
    #[test]
    fn restoring_an_upload_raises_the_allocator_past_its_id() {
        let c = cursors();
        c.restore_upload_manifest(50, empty_manifest(ENGINE_BLOB_OWNER_SCOPE, 8))
            .unwrap();
        assert!(c.next_id.load(Ordering::SeqCst) > 50);
    }
}
