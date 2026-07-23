//! S3 / MinIO content-addressed chunk backend (CONCEPT:EG-KG.storage.blob-namespace) — feature `blob-s3`.
//!
//! The fleet/billions-scale answer behind the SAME [`ChunkStore`] trait: chunk
//! bytes go to an object store (native multipart, lifecycle, replication, erasure
//! coding, virtually unbounded), and a small redb sidecar keeps ONLY the manifests,
//! refcounts, and a chunk-presence index. The protocol frames, cursors, manifest
//! shape and graph linkage are byte-identical to the native redb backend; only the
//! body of the chunk methods changes.
//!
//! This pulls the `object_store` SDK, so it lives behind a SEPARATE `blob-s3`
//! feature (NOT folded into `blob`/`node`/`cluster`/`full`/`pi`): the Pi/default
//! build links no object-store SDK. The `blob` feature alone gives the native
//! redb-CAS; `blob-s3` is an explicit opt-in for object-store deployments.
//!
//! Configuration (read at open): `EPISTEMIC_GRAPH_BLOB_S3_BUCKET` (required),
//! `EPISTEMIC_GRAPH_BLOB_S3_PREFIX` (default `cas/chunks`). Endpoint/credentials
//! come from the standard AWS env (`AWS_ENDPOINT`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`,
//! `AWS_SECRET_ACCESS_KEY`) — set `AWS_ENDPOINT` to a MinIO URL for MinIO.

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::runtime::Runtime;

use super::store::{BlobManifest, ChunkStore, RedbChunkStore, SweepStats};
use crate::mutation_batch::MutationBatch;

/// Object-store-backed CAS. Chunk bytes live in S3/MinIO keyed by their digest;
/// the manifests + refcounts live in a local redb sidecar (reusing
/// [`RedbChunkStore`] for everything except the chunk bytes).
pub struct S3ChunkStore {
    store: Box<dyn ObjectStore>,
    prefix: String,
    /// Sidecar: manifests + refcounts (and the dedup presence check is an S3 HEAD).
    sidecar: RedbChunkStore,
    /// A dedicated current-thread runtime to drive the async object_store calls
    /// from this sync trait without requiring an ambient tokio runtime.
    rt: Runtime,
}

impl S3ChunkStore {
    /// Open the object-store CAS. `persist_dir` holds the redb sidecar (manifests +
    /// refcounts); the chunk bytes go to the configured bucket.
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        let bucket = std::env::var("EPISTEMIC_GRAPH_BLOB_S3_BUCKET")
            .map_err(|_| "EPISTEMIC_GRAPH_BLOB_S3_BUCKET is required for the blob-s3 backend")?;
        let prefix =
            std::env::var("EPISTEMIC_GRAPH_BLOB_S3_PREFIX").unwrap_or_else(|_| "cas/chunks".into());
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| e.to_string())?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            store: Box::new(store),
            prefix,
            sidecar: RedbChunkStore::open(persist_dir)?,
            rt,
        })
    }

    fn chunk_path(&self, digest: &str) -> ObjPath {
        // Sharded prefix keeps the object listing tractable: cas/chunks/ab/cd/<digest>.
        let (a, b) = (&digest[0..2], &digest[2..4]);
        ObjPath::from(format!("{}/{}/{}/{}", self.prefix, a, b, digest))
    }
}

impl ChunkStore for S3ChunkStore {
    fn put_chunk(&self, bytes: &[u8]) -> Result<(String, bool), String> {
        if bytes.len() > super::store::MAX_BLOB_CHUNK_BYTES {
            return Err("blob chunk exceeds resource limits".to_string());
        }
        let digest = super::store::hex_digest(bytes);
        let path = self.chunk_path(&digest);
        self.rt.block_on(async {
            // Dedup: a HEAD on the content-addressed key. Present ⇒ skip the PUT.
            match self.store.head(&path).await {
                Ok(_) => Ok((digest.clone(), false)),
                Err(object_store::Error::NotFound { .. }) => {
                    self.store
                        .put(&path, bytes.to_vec().into())
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok((digest.clone(), true))
                }
                Err(e) => Err(e.to_string()),
            }
        })
    }

    fn get_chunk(&self, digest: &str) -> Result<Option<Vec<u8>>, String> {
        super::store::validate_digest(digest)?;
        let path = self.chunk_path(digest);
        self.rt.block_on(async {
            match self.store.head(&path).await {
                Ok(meta) if meta.size > super::store::MAX_BLOB_CHUNK_BYTES as u64 => {
                    return Err("stored blob chunk exceeds resource limits".to_string());
                }
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error.to_string()),
            }
            match self.store.get(&path).await {
                Ok(r) => {
                    let bytes = r.bytes().await.map_err(|e| e.to_string())?;
                    if bytes.len() > super::store::MAX_BLOB_CHUNK_BYTES {
                        return Err("stored blob chunk exceeds resource limits".to_string());
                    }
                    Ok(Some(bytes.to_vec()))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.to_string()),
            }
        })
    }

    // Manifests + refcounts + GC bookkeeping live in the redb sidecar.
    fn put_manifest(&self, blob_digest: &str, manifest: &BlobManifest) -> Result<(), String> {
        self.sidecar.put_manifest(blob_digest, manifest)
    }

    fn get_manifest(&self, blob_digest: &str) -> Result<Option<BlobManifest>, String> {
        self.sidecar.get_manifest(blob_digest)
    }

    fn incref(&self, blob_digest: &str) -> Result<u64, String> {
        self.sidecar.incref(blob_digest)
    }

    fn decref(&self, blob_digest: &str) -> Result<u64, String> {
        self.sidecar.decref(blob_digest)
    }

    fn refcount(&self, blob_digest: &str) -> Result<u64, String> {
        self.sidecar.refcount(blob_digest)
    }

    fn sweep(&self) -> Result<SweepStats, String> {
        // Determine which chunks the sidecar GC would orphan, delete those objects
        // from S3, then let the sidecar drop the manifests/refcounts. We compute the
        // orphan object set BEFORE the sidecar sweep removes the manifests.
        let orphans = self.sidecar.orphan_chunks_preview()?;
        for digest in &orphans {
            let path = self.chunk_path(digest);
            self.rt.block_on(async {
                match self.store.delete(&path).await {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(e.to_string()),
                }
            })?;
        }
        // The sidecar sweep reclaims manifests + refcounts. Its `chunks_reclaimed`
        // counts sidecar chunk rows (there are none in S3 mode), so report the S3
        // object deletions instead.
        let mut stats = self.sidecar.sweep()?;
        stats.chunks_reclaimed = orphans.len() as u64;
        Ok(stats)
    }

    fn chunk_count(&self) -> Result<u64, String> {
        // Object count is an O(N) S3 list; report the sidecar's manifest-derived
        // distinct-chunk estimate instead, which is what observability needs.
        self.sidecar.distinct_referenced_chunks()
    }

    fn blob_count(&self) -> Result<u64, String> {
        self.sidecar.blob_count()
    }

    fn mutation_version(&self, tenant: &str, graph: &str) -> Result<u64, String> {
        self.sidecar.mutation_version(tenant, graph)
    }

    fn commit_cursor_batch(
        &self,
        batch: &MutationBatch,
        cursor: u64,
        committed_at_ms: u64,
    ) -> Result<u64, String> {
        self.sidecar
            .commit_cursor_batch(batch, cursor, committed_at_ms)
    }

    fn put_chunk_batch(
        &self,
        bytes: &[u8],
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(String, bool), String> {
        // Content-addressed PUT is intrinsically retry-idempotent. The sidecar's
        // terminal batch/outbox record is the authority: a crash after PUT but
        // before sidecar commit retries the same digest, then commits once.
        let result = self.put_chunk(bytes)?;
        self.sidecar
            .record_native_batch_result(batch, result, committed_at_ms)
    }

    fn put_manifest_batch(
        &self,
        blob_digest: &str,
        manifest: &BlobManifest,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<String, String> {
        self.sidecar
            .put_manifest_batch(blob_digest, manifest, batch, committed_at_ms)
    }

    fn adjust_ref_batch(
        &self,
        blob_digest: &str,
        delta: i64,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<u64, String> {
        self.sidecar
            .adjust_ref_batch(blob_digest, delta, batch, committed_at_ms)
    }

    fn sweep_batch(
        &self,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<SweepStats, String> {
        let orphans = self.sidecar.orphan_chunks_preview()?;
        for digest in &orphans {
            let path = self.chunk_path(digest);
            self.rt.block_on(async {
                match self.store.delete(&path).await {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(error) => Err(error.to_string()),
                }
            })?;
        }
        self.sidecar
            .sweep_batch_with_external_chunks(batch, committed_at_ms, orphans.len() as u64)
    }

    fn begin_upload_batch(
        &self,
        cursor: u64,
        chunk_size: u32,
        owner_scope: &str,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<u64, String> {
        self.sidecar
            .begin_upload_batch(cursor, chunk_size, owner_scope, batch, committed_at_ms)
    }

    fn put_upload_chunk_batch(
        &self,
        cursor: u64,
        bytes: &[u8],
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(String, u32), String> {
        // The local sidecar retains the authoritative chunk until the batch and
        // durable upload cursor commit. S3 is an idempotent content-addressed
        // projection, so a crash can only leave an extra recoverable object.
        let _ = self.put_chunk(bytes)?;
        self.sidecar
            .put_upload_chunk_batch(cursor, bytes, batch, committed_at_ms)
    }

    fn load_upload(&self, cursor: u64) -> Result<Option<BlobManifest>, String> {
        self.sidecar.load_upload(cursor)
    }

    fn commit_upload_batch(
        &self,
        cursor: u64,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<String, String> {
        self.sidecar
            .commit_upload_batch(cursor, batch, committed_at_ms)
    }
}
