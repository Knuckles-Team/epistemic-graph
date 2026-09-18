//! The redb store as the object-store backend's sidecar (feature `blob-s3`):
//! manifests, holders, uploads and the ledger stay in `blob.redb`, while chunk
//! bytes are authoritative in the object store.

use super::gc::{plan_sweep, zero_refcounts, SharedFacts, SweepTables};
use super::manifest::{decode_manifest, track_gc_digest};
use super::{
    open_store, ChunkAuthority, ChunkStore, RedbChunkStore, SweepRequest, SweepStats, CAS_BLOBS,
    CAS_RETENTION, CAS_UPLOADS,
};
use crate::mutation_batch::MutationBatch;
use redb::ReadableTable;
use std::collections::BTreeSet;

impl RedbChunkStore {
    /// Open `{persist_dir}/blob.redb` as a sidecar whose chunk bytes live elsewhere.
    pub(crate) fn open_external_chunk_sidecar(persist_dir: &str) -> Result<Self, String> {
        open_store(persist_dir, ChunkAuthority::External)
    }

    /// The sidecar sweep after the object store deleted `external_chunks` objects.
    pub(crate) fn sweep_batch_with_external_chunks(
        &self,
        request: &SweepRequest,
        batch: &MutationBatch,
        committed_at_ms: u64,
        external_chunks: u64,
    ) -> Result<SweepStats, String> {
        let mut stats = self.sweep_batch(request, batch, committed_at_ms)?;
        stats.chunks_reclaimed = external_chunks;
        Ok(stats)
    }

    /// Chunks that a sweep under `request` at `now_ms` would reclaim, decided by the
    /// same plan the sweep runs. Used by the object-store backend to delete the
    /// matching objects before the sidecar sweep drops their manifests. Read-only.
    pub(crate) fn sweep_preview_chunks(
        &self,
        request: &SweepRequest,
        now_ms: u64,
    ) -> Result<BTreeSet<String>, String> {
        self.flush_chunks()?;
        let read = self.read()?;
        let shared = self.shared_read()?;
        let refcount = |digest: &str| shared.refcount(digest);
        let facts = SharedFacts {
            refcount: &refcount,
            chunk_present: &|_: &str| -> Result<bool, String> { Ok(true) },
            zero_refs: zero_refcounts(|visit| shared.for_each_refcount(visit))?,
        };
        let tables = SweepTables {
            blobs: &read.open_owner_table(CAS_BLOBS)?,
            uploads: &read.open_owner_table(CAS_UPLOADS)?,
            retention: &read.open_owner_table(CAS_RETENTION)?,
        };
        plan_sweep(&tables, &facts, request, now_ms).map(|plan| plan.into_chunks())
    }

    /// Count of distinct chunk digests referenced by all surviving manifests
    /// (dedup observability when chunk bytes live off-redb, e.g. in S3).
    pub(crate) fn distinct_referenced_chunks(&self) -> Result<u64, String> {
        self.flush_chunks()?;
        let read = self.read()?;
        let blobs = read.open_owner_table(CAS_BLOBS)?;
        let mut seen = BTreeSet::new();
        for row in blobs.iter().map_err(|e| e.to_string())? {
            let (_, value) = row.map_err(|e| e.to_string())?;
            for chunk in decode_manifest(value.value())?.chunks {
                track_gc_digest(&mut seen, chunk)?;
            }
        }
        Ok(seen.len() as u64)
    }
}
