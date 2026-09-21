//! Native redb implementation of the [`super::ChunkStore`] seam.
//!
//! The owner/kernel state and public trait live in the parent module.  Operation
//! families are kept in the sibling modules below; this file is only the thin
//! trait-object adapter required by the public `ChunkStore` contract.  The macro
//! keeps the trait method set together while the implementation bodies remain
//! in focused modules (a trait implementation cannot be split into multiple
//! Rust `impl Trait for Type` blocks).

mod chunks;
mod engine;
mod holders;
mod manifests;
mod uploads;

use super::*;

macro_rules! impl_native_chunk_store {
    ($($method:item)*) => {
        impl ChunkStore for RedbChunkStore {
            $($method)*
        }
    };
}

impl_native_chunk_store! {
    fn put_chunk(&self, bytes: &[u8]) -> Result<(String, bool), String> {
        chunks::put_chunk(self, bytes)
    }

    fn get_chunk(&self, digest: &str) -> Result<Option<Vec<u8>>, String> {
        chunks::get_chunk(self, digest)
    }

    fn put_manifest(&self, blob_digest: &str, manifest: &BlobManifest) -> Result<(), String> {
        manifests::put_manifest(self, blob_digest, manifest)
    }

    fn get_manifest(&self, blob_digest: &str) -> Result<Option<BlobManifest>, String> {
        manifests::get_manifest(self, blob_digest)
    }

    fn incref(&self, blob_digest: &str) -> Result<u64, String> {
        manifests::adjust_reference(self, blob_digest, 1)
    }

    fn decref(&self, blob_digest: &str) -> Result<u64, String> {
        manifests::adjust_reference(self, blob_digest, -1)
    }

    fn refcount(&self, blob_digest: &str) -> Result<u64, String> {
        manifests::refcount(self, blob_digest)
    }

    fn sweep(&self) -> Result<SweepStats, String> {
        manifests::sweep(self)
    }

    fn chunk_count(&self) -> Result<u64, String> {
        manifests::chunk_count(self)
    }

    fn blob_count(&self) -> Result<u64, String> {
        manifests::blob_count(self)
    }

    fn put_engine_bodies(
        &self,
        tenant_id: &str,
        bodies: &[EngineBody],
        committed_at_ms: u64,
    ) -> Result<Vec<StoredEngineBody>, String> {
        engine::put_engine_bodies(self, tenant_id, bodies, committed_at_ms)
    }

    fn mutation_version(&self, tenant: &str, graph: &str) -> Result<u64, String> {
        uploads::mutation_version(self, tenant, graph)
    }

    fn sweep_batch(
        &self,
        request: &SweepRequest,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<SweepStats, String> {
        uploads::sweep_batch(self, request, batch, committed_at_ms)
    }

    fn begin_upload_batch(
        &self,
        cursor: u64,
        chunk_size: u32,
        owner_scope: &str,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<u64, String> {
        uploads::begin_upload_batch(
            self,
            cursor,
            chunk_size,
            owner_scope,
            batch,
            committed_at_ms,
        )
    }

    fn put_upload_chunk_batch(
        &self,
        cursor: u64,
        bytes: &[u8],
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<(String, u32), String> {
        uploads::put_upload_chunk_batch(self, cursor, bytes, batch, committed_at_ms)
    }

    fn load_upload(&self, cursor: u64) -> Result<Option<BlobManifest>, String> {
        uploads::load_upload(self, cursor)
    }

    fn commit_upload_batch(
        &self,
        cursor: u64,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<String, String> {
        uploads::commit_upload_batch(self, cursor, batch, committed_at_ms)
    }

    fn upload_cursor_high_water(&self) -> Result<u64, String> {
        uploads::upload_cursor_high_water(self)
    }

    fn holder_batch(
        &self,
        change: &HolderChange,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<HolderOutcome, String> {
        holders::holder_batch(self, change, batch, committed_at_ms)
    }

    fn reconcile_holders_batch(
        &self,
        request: &HolderReconcile,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<ReconcileStats, String> {
        holders::reconcile_holders_batch(self, request, batch, committed_at_ms)
    }
}
