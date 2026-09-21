//! Durable upload and caller-scoped mutation operations for the native CAS.

use super::super::*;

pub(super) fn mutation_version(
    store: &RedbChunkStore,
    tenant: &str,
    graph: &str,
) -> Result<u64, String> {
    let identity = blob_scope_identity(ScopeTenantId::new(tenant.to_string())?, graph)?;
    let owner = store.scope_handle(&identity)?;
    eg_transaction::version(&store.kernel.read_scope(&owner)?)
}

pub(super) fn sweep_batch(
    store: &RedbChunkStore,
    request: &SweepRequest,
    batch: &MutationBatch,
    committed_at_ms: u64,
) -> Result<SweepStats, String> {
    store.flush_chunks()?;
    store.commit_native_batch(batch, committed_at_ms, |wtx| {
        super::manifests::sweep_in_for_batch(store, wtx, request, committed_at_ms)
    })
}

pub(super) fn begin_upload_batch(
    store: &RedbChunkStore,
    cursor: u64,
    chunk_size: u32,
    owner_scope: &str,
    batch: &MutationBatch,
    committed_at_ms: u64,
) -> Result<u64, String> {
    store.flush_chunks()?;
    if chunk_size == 0 || chunk_size as usize > MAX_BLOB_CHUNK_BYTES {
        return Err("blob chunk size exceeds resource limits".to_string());
    }
    store.commit_native_batch(batch, committed_at_ms, |wtx| {
        super::super::uploads::begin_upload(wtx, cursor, chunk_size, owner_scope, committed_at_ms)
    })
}

pub(super) fn put_upload_chunk_batch(
    store: &RedbChunkStore,
    cursor: u64,
    bytes: &[u8],
    batch: &MutationBatch,
    committed_at_ms: u64,
) -> Result<(String, u32), String> {
    store.flush_chunks()?;
    let digest = manifest::chunk_digest(bytes)?;
    store.commit_native_batch(batch, committed_at_ms, |wtx| {
        let shared = shared_write(wtx, &store.shared)?;
        let chunk = (digest.as_str(), bytes);
        super::super::uploads::put_upload_chunk(
            wtx,
            &shared,
            cursor,
            chunk,
            &batch.batch_id,
            committed_at_ms,
        )
    })
}

pub(super) fn load_upload(
    store: &RedbChunkStore,
    cursor: u64,
) -> Result<Option<BlobManifest>, String> {
    store.flush_chunks()?;
    let read = store.read()?;
    let table = read.open_owner_table(CAS_UPLOADS)?;
    let row = table.get(cursor).map_err(|e| e.to_string())?;
    // Bind before returning: the guard borrows `table` (E0597 otherwise).
    row.map(|value| {
        super::super::uploads::decode_upload(value.value()).map(|upload| upload.manifest())
    })
    .transpose()
}

pub(super) fn commit_upload_batch(
    store: &RedbChunkStore,
    cursor: u64,
    batch: &MutationBatch,
    committed_at_ms: u64,
) -> Result<String, String> {
    store.flush_chunks()?;
    store.commit_native_batch(batch, committed_at_ms, |wtx| {
        let shared = shared_write(wtx, &store.shared)?;
        let stored = |chunk: &str| chunk_is_stored(store.chunks, &shared, chunk);
        super::super::uploads::commit_upload(wtx, &stored, cursor, committed_at_ms)
    })
}

pub(super) fn upload_cursor_high_water(store: &RedbChunkStore) -> Result<u64, String> {
    store.flush_chunks()?;
    let read = store.read()?;
    super::super::uploads::upload_cursor_high_water(&read.open_owner_table(CAS_COUNTERS)?)
}
