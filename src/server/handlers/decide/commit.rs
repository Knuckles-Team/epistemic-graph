//! `Method::DecisionCommit`: make one assembly record durable.
//!
//! The record arriving on the wire is never trusted (DECIDE-LAYER-DESIGN
//! §4.2). In order: the version and size are checked; the engine re-derives
//! the record from its own stored inputs and compares bytes; the certificate
//! is verified independently; then, inside ONE Agent Library write, every
//! candidate's stored facts are compared with the revision they name and the
//! scope's catalog digest is compared-and-set. Only then is the record stored,
//! as a `DecisionRecord` component plus its verbatim body. A repeat of the
//! same record is an idempotent replay.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;
use eg_types::decision::DecisionCommitRequest;

/// Commit one `DecisionRecord` as a component revision, idempotently.
#[cfg(feature = "decide")]
pub(crate) async fn handle_decision_commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: DecisionCommitRequest,
) -> Response {
    match commit(state, req_id, verified, request).await {
        Ok(result) => Response::ok(
            req_id,
            crate::protocol::ResultPayload::of_ref::<
                eg_types::result_contract::storage::DecisionCommit,
            >(&result),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "decide")]
async fn commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: DecisionCommitRequest,
) -> Result<eg_types::decision::DecisionCommitResult, String> {
    use eg_types::decision::{DecisionErrorCode, MAX_DECISION_RECORD_BYTES};

    let DecisionCommitRequest {
        context,
        record,
        expected_catalog_digest,
    } = request;
    if record.tenant_id != verified.tenant() {
        return Err(
            "ACCESS_DENIED: the record tenant must match the verified request tenant".into(),
        );
    }
    let bytes = eg_compute::assemble::encoded_len(&record);
    if bytes > MAX_DECISION_RECORD_BYTES {
        return Err(format!(
            "{}: {bytes} bytes exceed {MAX_DECISION_RECORD_BYTES}",
            DecisionErrorCode::RecordTooLarge.as_str()
        ));
    }
    let checked = record.clone();
    tokio::task::spawn_blocking(move || eg_compute::assemble::replay_check(&checked))
        .await
        .map_err(|error| format!("decision replay task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    super::shape::check_decision_shape(&record)?;
    let store = state.write().await.ensure_agent_library()?;
    let context = crate::server::handlers::admin::bind_agent_library_context(
        &store,
        req_id,
        verified,
        context,
        "decision:commit",
        true,
    )?;
    let written = store.commit_decision_record(context, &record, &expected_catalog_digest)?;
    super::assemble::audit_line("decision-commit", &record, verified);
    Ok(eg_types::decision::DecisionCommitResult {
        schema_version: eg_types::decision::DECISION_COMMIT_RESULT_SCHEMA_VERSION,
        record_id: record.record_id,
        component: written.result,
        replayed: written.replayed,
    })
}

/// A build without the Decide layer still serves the method: it refuses by
/// name.
#[cfg(not(feature = "decide"))]
pub(crate) async fn handle_decision_commit(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _verified: &VerifiedRequestContext,
    _request: DecisionCommitRequest,
) -> Response {
    Response::err(req_id, "DecisionCommit requires the `decide` feature")
}
