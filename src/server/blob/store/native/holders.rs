//! Named holder mutations for the native CAS.

use super::super::*;

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
