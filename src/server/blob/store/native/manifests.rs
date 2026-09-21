//! Manifest, reference-count and GC operations for the native CAS.

use super::super::*;

pub(super) fn put_manifest(
    store: &RedbChunkStore,
    blob_digest: &str,
    manifest: &BlobManifest,
) -> Result<(), String> {
    // BlobCommit lands here: flush the upload's final partial chunk group first
    // (the manifest's chunks must all be durable before the manifest references
    // them), then write the manifest as its own admitted maintenance mutation.
    store.flush_chunks()?;
    manifest::encode_manifest(blob_digest, manifest)?;
    let at = crate::server::dispatch::authoritative_now_ms();
    store.maintain("blob_put_manifest_v1", blob_digest, at, |wtx| {
        let shared = shared_write(wtx, &store.shared)?;
        let stored = |chunk: &str| chunk_is_stored(store.chunks, &shared, chunk);
        manifest::record_manifest(wtx, &stored, blob_digest, manifest, at)
    })
}

pub(super) fn get_manifest(
    store: &RedbChunkStore,
    blob_digest: &str,
) -> Result<Option<BlobManifest>, String> {
    store.flush_chunks()?;
    validate_digest(blob_digest)?;
    let read = store.read()?;
    let table = read.open_owner_table(CAS_BLOBS)?;
    let row = table.get(blob_digest).map_err(|e| e.to_string())?;
    // Bind before returning: the guard borrows `table` (E0597 otherwise).
    row.map(|guard| manifest::decode_manifest(guard.value()))
        .transpose()
}

pub(super) fn adjust_reference(
    store: &RedbChunkStore,
    blob_digest: &str,
    delta: i64,
) -> Result<u64, String> {
    maintain_counted_reference(store, blob_digest, delta)
}

pub(super) fn refcount(store: &RedbChunkStore, blob_digest: &str) -> Result<u64, String> {
    store.flush_chunks()?;
    store.shared_read()?.refcount(blob_digest)
}

pub(super) fn sweep(store: &RedbChunkStore) -> Result<SweepStats, String> {
    store.flush_chunks()?;
    let at = crate::server::dispatch::authoritative_now_ms();
    store.maintain("blob_sweep_v1", "all", at, |wtx| {
        sweep_in_for_batch(store, wtx, &SweepRequest::default(), at)
    })
}

pub(super) fn chunk_count(store: &RedbChunkStore) -> Result<u64, String> {
    store.flush_chunks()?;
    store.shared_read()?.table_rows::<CasChunkRows>()
}

pub(super) fn blob_count(store: &RedbChunkStore) -> Result<u64, String> {
    store.flush_chunks()?;
    let read = store.read()?;
    read.open_owner_table(CAS_BLOBS)?
        .len()
        .map_err(|e| e.to_string())
}

pub(super) fn sweep_in_for_batch(
    store: &RedbChunkStore,
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    request: &SweepRequest,
    at_ms: u64,
) -> Result<SweepStats, String> {
    let shared = shared_write(wtx, &store.shared)?;
    let stored = |chunk: &str| chunk_is_stored(store.chunks, &shared, chunk);
    gc::sweep_rows(wtx, &shared, &stored, request, at_ms)
}
