//! Served EG-SCM source changesets: submit, seal a gate verdict, land, revert
//! and read. arbora serves `source.commit`, `source.land` and
//! `source.revert` over these operations; the engine owns every record.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{Response, ResultPayload};
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::state::ServerState;
use eg_types::result_contract::storage as results;
use eg_types::source_changeset::{
    propose, referenced_blobs, GateVerdict, SourceChangesetOp, SourceChangesetRecord,
    SourceRefusal,
};

pub(crate) async fn handle_source_changeset(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: SourceChangesetOp,
) -> Response {
    if op.tenant_id() != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: source changeset tenant must match verified request tenant",
        );
    }
    let store = {
        let mut guard = state.write().await;
        match guard.ensure_agent_library() {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        }
    };
    let caller = Caller {
        principal: verified.principal_persistence_id(),
        agent_id: verified.agent_id().to_owned(),
        // An agent acts when its verified agent identity differs from the
        // principal it acts for; a person acting directly carries no run.
        is_agent: verified.agent_id() != verified.principal(),
        now_ms: crate::server::dispatch::authoritative_now_ms(),
    };
    let result = match op {
        SourceChangesetOp::Submit { submission } => {
            match propose(
                *submission,
                &caller.principal,
                &caller.agent_id,
                caller.is_agent,
                caller.now_ms,
            ) {
                Ok(record) => match check_blobs(state, verified, &record).await {
                    Ok(()) => store
                        .submit_source_changeset(record)
                        .and_then(ResultPayload::of::<results::SourceChangesetSubmit>),
                    Err(error) => Err(error),
                },
                Err(refusal) => Err(refusal.to_string()),
            }
        }
        op => dispatch_recorded(&store, &caller, op),
    };
    match result {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => Response::err(req_id, error),
    }
}

struct Caller {
    principal: String,
    agent_id: String,
    is_agent: bool,
    now_ms: u64,
}

fn dispatch_recorded(
    store: &AgentLibraryStore,
    caller: &Caller,
    op: SourceChangesetOp,
) -> Result<ResultPayload, String> {
    match op {
        SourceChangesetOp::Submit { .. } => Err("source submit routed twice".to_string()),
        SourceChangesetOp::RecordVerdict {
            tenant_id,
            changeset_id,
            gate_id,
            input_digest,
            verdict,
            evidence_digest,
            duration_ms,
        } => {
            let verdict = GateVerdict {
                gate_id,
                input_digest,
                verdict,
                evidence_digest,
                duration_ms,
                recorded_by: caller.principal.clone(),
                recorded_at_ms: caller.now_ms,
            };
            store
                .record_source_verdict(&tenant_id, &changeset_id, verdict)
                .and_then(ResultPayload::of::<results::SourceChangesetRecordVerdict>)
        }
        SourceChangesetOp::Land {
            tenant_id,
            changeset_id,
        } => store
            .land_source_changeset(&tenant_id, &changeset_id, caller.now_ms)
            .and_then(ResultPayload::of::<results::SourceChangesetLand>),
        SourceChangesetOp::Revert {
            tenant_id,
            changeset_id,
            message,
        } => store
            .revert_source_changeset(
                &tenant_id,
                &changeset_id,
                message,
                &caller.principal,
                &caller.agent_id,
                caller.now_ms,
            )
            .and_then(ResultPayload::of::<results::SourceChangesetRevert>),
        SourceChangesetOp::Get {
            tenant_id,
            changeset_id,
        } => store
            .source_changeset(&tenant_id, &changeset_id)
            .and_then(ResultPayload::of::<results::SourceChangesetGet>),
        SourceChangesetOp::Head {
            tenant_id,
            repo,
            branch,
        } => store
            .source_branch_head(&tenant_id, &repo, &branch)
            .and_then(ResultPayload::of::<results::SourceChangesetHead>),
    }
}

/// Every digest a changeset points at must be readable from the blob store,
/// and every new file version must be one the caller uploaded.
#[cfg(feature = "blob")]
async fn check_blobs(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    record: &SourceChangesetRecord,
) -> Result<(), String> {
    use crate::server::blob::ChunkStore;
    let cursors = state.read().await.blob.clone();
    let Some(cursors) = cursors else {
        return Err("UNAVAILABLE: source changesets require the blob substrate".to_string());
    };
    let carrier = crate::server::access::CarrierAuthority::from_verified(verified)?;
    let owner_scope = carrier.owner_scope();
    let new_blobs: std::collections::BTreeSet<&str> = record
        .changeset
        .changed_paths
        .iter()
        .filter_map(|path| path.new_blob.as_deref())
        .collect();
    for digest in referenced_blobs(&record.changeset) {
        let manifest = cursors.store.get_manifest(digest)?;
        let readable = match manifest {
            Some(manifest) => !new_blobs.contains(digest) || manifest.owner_scope == owner_scope,
            None => false,
        };
        if !readable {
            return Err(SourceRefusal::BlobUnreadable(digest.to_owned()).to_string());
        }
    }
    Ok(())
}

#[cfg(not(feature = "blob"))]
async fn check_blobs(
    _state: &Arc<RwLock<ServerState>>,
    _verified: &VerifiedRequestContext,
    _record: &SourceChangesetRecord,
) -> Result<(), String> {
    Err("UNAVAILABLE: source changesets require the blob substrate".to_string())
}

#[cfg(all(test, feature = "blob"))]
#[path = "source_changeset_tests.rs"]
mod tests;
