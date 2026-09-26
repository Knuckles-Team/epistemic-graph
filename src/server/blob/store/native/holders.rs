//! Named holder mutations for the native CAS.

use super::super::*;

pub(super) fn has_named_holder(
    store: &RedbChunkStore,
    digest: &str,
    holder: &str,
) -> Result<bool, String> {
    validate_digest(digest)?;
    let holder = HolderId::new(holder)?;
    let read = store.read()?;
    let table = read.open_owner_table(CAS_HOLDERS)?;
    let found = table
        .get((digest, holder.as_str()))
        .map(|row| row.is_some())
        .map_err(|error| error.to_string())?;
    Ok(found)
}

pub(super) fn holder_batch(
    store: &RedbChunkStore,
    change: &HolderChange,
    batch: &MutationBatch,
    committed_at_ms: u64,
) -> Result<HolderOutcome, String> {
    store.flush_chunks()?;
    store.commit_native_batch(batch, committed_at_ms, |wtx| {
        let shared = shared_write(wtx, &store.shared)?;
        super::super::holders::apply_holder_change(wtx, &shared, change, committed_at_ms)
    })
}

pub(super) fn reconcile_holders_batch(
    store: &RedbChunkStore,
    request: &HolderReconcile,
    batch: &MutationBatch,
    committed_at_ms: u64,
) -> Result<ReconcileStats, String> {
    store.flush_chunks()?;
    store.commit_native_batch(batch, committed_at_ms, |wtx| {
        super::super::holders::reconcile_holders(wtx, &shared_write(wtx, &store.shared)?, request)
    })
}
