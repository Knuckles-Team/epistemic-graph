//! Authenticated D18 write-back record handling.
//!
//! This handler never performs source I/O. It binds every durable row to the
//! verified tenant, actor and request idempotency key, then calls the existing
//! tenant-scoped ControlRedb owner.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{Response, ResultPayload};
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::state::ServerState;
use eg_types::write_back::{SourceChangeSet, WriteBackOp};

pub(crate) async fn handle_write_back(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: WriteBackOp,
) -> Response {
    if op.tenant_id() != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: write-back tenant must match verified request tenant",
        );
    }
    let store = {
        let mut guard = state.write().await;
        match guard.ensure_agent_library() {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        }
    };
    let actor = verified.principal_persistence_id();
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let result = dispatch_write_back(
        store.as_ref(),
        verified.idempotency_key(),
        actor,
        now_ms,
        op,
    );
    match result {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => Response::err(req_id, error),
    }
}

fn dispatch_write_back(
    store: &AgentLibraryStore,
    idempotency_key: &str,
    actor: String,
    now_ms: u64,
    op: WriteBackOp,
) -> Result<ResultPayload, String> {
    match op {
        WriteBackOp::Create { change_set } => {
            create_change_set(store, idempotency_key, &actor, now_ms, *change_set)
        }
        WriteBackOp::Get {
            tenant_id,
            change_set_id,
        } => get_change_set(store, &tenant_id, &change_set_id),
        WriteBackOp::RecordAttempt { attempt } => {
            record_attempt(store, idempotency_key, actor, now_ms, *attempt)
        }
        WriteBackOp::RecordReconciliation { observation } => {
            record_reconciliation(store, idempotency_key, actor, now_ms, *observation)
        }
        WriteBackOp::Receipts {
            tenant_id,
            change_set_id,
            after_sequence,
            limit,
        } => get_receipts(store, &tenant_id, &change_set_id, after_sequence, limit),
    }
}

fn create_change_set(
    store: &AgentLibraryStore,
    idempotency_key: &str,
    actor: &str,
    now_ms: u64,
    change_set: SourceChangeSet,
) -> Result<ResultPayload, String> {
    if change_set.actor != actor {
        return Err(
            "ACCESS_DENIED: change-set actor must match verified request principal".to_string(),
        );
    }
    if change_set.idempotency_key != idempotency_key {
        return Err(
            "IDEMPOTENCY_CONFLICT: change-set key must match verified request key".to_string(),
        );
    }
    if now_ms >= change_set.expires_at_ms {
        return Err("write-back change set has expired".to_string());
    }
    store
        .create_change_set(change_set)
        .and_then(ResultPayload::of::<eg_types::result_contract::storage::WriteBackCreate>)
}

fn get_change_set(
    store: &AgentLibraryStore,
    tenant_id: &str,
    change_set_id: &str,
) -> Result<ResultPayload, String> {
    store
        .write_back_change_set(tenant_id, change_set_id)
        .and_then(ResultPayload::of::<eg_types::result_contract::storage::WriteBackGet>)
}

fn record_attempt(
    store: &AgentLibraryStore,
    idempotency_key: &str,
    actor: String,
    now_ms: u64,
    attempt: eg_types::write_back::WriteBackAttempt,
) -> Result<ResultPayload, String> {
    if attempt.idempotency_key != idempotency_key {
        return Err(
            "IDEMPOTENCY_CONFLICT: attempt key must match verified request key".to_string(),
        );
    }
    store
        .record_write_back_attempt(attempt, actor, now_ms)
        .and_then(ResultPayload::of::<eg_types::result_contract::storage::WriteBackRecordAttempt>)
}

fn record_reconciliation(
    store: &AgentLibraryStore,
    idempotency_key: &str,
    actor: String,
    now_ms: u64,
    observation: eg_types::write_back::ReconciliationObservation,
) -> Result<ResultPayload, String> {
    if observation.idempotency_key != idempotency_key {
        return Err(
            "IDEMPOTENCY_CONFLICT: reconciliation key must match verified request key".to_string(),
        );
    }
    store
        .record_write_back_reconciliation(observation, actor, now_ms)
        .and_then(
            ResultPayload::of::<eg_types::result_contract::storage::WriteBackRecordReconciliation>,
        )
}

fn get_receipts(
    store: &AgentLibraryStore,
    tenant_id: &str,
    change_set_id: &str,
    after_sequence: u64,
    limit: u16,
) -> Result<ResultPayload, String> {
    store
        .write_back_receipts(tenant_id, change_set_id, after_sequence, limit)
        .and_then(ResultPayload::of::<eg_types::result_contract::storage::WriteBackReceipts>)
}
