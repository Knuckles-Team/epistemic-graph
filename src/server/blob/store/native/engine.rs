//! Connector-pack body writes into the engine-owned native CAS.

use super::super::*;
use crate::server::blob::engine_bodies::{EngineBody, PlannedEngineBody, StoredEngineBody};

pub(super) fn put_engine_bodies(
    store: &RedbChunkStore,
    tenant_id: &str,
    bodies: &[EngineBody],
    committed_at_ms: u64,
) -> Result<Vec<StoredEngineBody>, String> {
    store.flush_chunks()?;
    let planned = super::super::super::engine_bodies::plan_engine_bodies(tenant_id, bodies)?;
    let subject = super::super::super::engine_bodies::batch_subject(&planned);
    store.maintain(
        "connector_pack_engine_bodies",
        &subject,
        committed_at_ms,
        |wtx| put_engine_bodies_in(store, wtx, &planned, committed_at_ms),
    )
}

fn put_engine_bodies_in(
    store: &RedbChunkStore,
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    planned: &[PlannedEngineBody],
    committed_at_ms: u64,
) -> Result<Vec<StoredEngineBody>, String> {
    let shared = shared_write(wtx, &store.shared)?;
    let chunk_rows = engine_body_chunk_rows(planned);
    shared.insert_chunks_if_absent(&chunk_rows)?;
    let stored = |chunk: &str| chunk_is_stored(store.chunks, &shared, chunk);
    for body in planned {
        manifest::record_manifest(
            wtx,
            &stored,
            &body.stored.manifest_digest,
            &body.manifest,
            committed_at_ms,
        )?;
        let holder = HolderId::new(&body.stored.holder_id)?;
        let change = HolderChange::acquire(
            &body.stored.manifest_digest,
            holder,
            &body.manifest.owner_scope,
        )?;
        holders::apply_holder_change(wtx, &shared, &change, committed_at_ms)?;
    }
    Ok(planned.iter().map(|body| body.stored.clone()).collect())
}

fn engine_body_chunk_rows(planned: &[PlannedEngineBody]) -> Vec<(&str, &[u8])> {
    planned
        .iter()
        .filter_map(|body| {
            body.chunk_digest
                .as_deref()
                .map(|digest| (digest, body.body.as_slice()))
        })
        .collect()
}
