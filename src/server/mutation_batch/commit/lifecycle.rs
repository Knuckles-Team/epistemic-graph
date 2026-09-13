use std::sync::Arc;

use crate::mutation_batch::MutationSurface;
use crate::protocol::{Method, ResultPayload};
use crate::server::mutation::LifecycleAttempt;
use crate::server::persistence::PersistenceBackend;
use eg_types::contract::Nonce;

use super::super::compile::{compile_methods, CompileBatch};
use super::super::digest::lifecycle_batch_id;

pub(crate) struct LifecycleCommitRequest<'a> {
    pub(crate) persistence: &'a Arc<dyn PersistenceBackend>,
    pub(crate) action: &'a str,
    pub(crate) request_id: u64,
    attempt_nonce: Option<Nonce>,
    pub(crate) principal: Option<&'a str>,
    pub(crate) idempotency_key: &'a str,
    pub(crate) graph: &'a str,
    pub(crate) method: Method,
    pub(crate) result: &'a ResultPayload,
}

impl<'a> LifecycleCommitRequest<'a> {
    pub(crate) fn new(
        persistence: &'a Arc<dyn PersistenceBackend>,
        action: &'a str,
        request_id: u64,
        principal: Option<&'a str>,
        idempotency_key: &'a str,
        graph: &'a str,
        method: Method,
        result: &'a ResultPayload,
    ) -> Self {
        Self {
            persistence,
            action,
            request_id,
            attempt_nonce: None,
            principal,
            idempotency_key,
            graph,
            method,
            result,
        }
    }

    pub(crate) fn with_attempt_nonce(mut self, attempt_nonce: Option<Nonce>) -> Self {
        self.attempt_nonce = attempt_nonce;
        self
    }
}

/// Commit one CreateGraph/DeleteGraph batch before the caller mutates the in-RAM
/// registry. The redb kernel applies graph_meta/purge, status, idempotency and
/// outbox atomically; `replayed` lets the caller finish a post-commit RAM publish.
pub(crate) async fn commit_lifecycle(
    request: LifecycleCommitRequest<'_>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let LifecycleCommitRequest {
        persistence,
        action,
        request_id,
        attempt_nonce,
        principal,
        idempotency_key,
        graph,
        method,
        result,
    } = request;
    let batch_id = lifecycle_batch_id(action, graph, principal, idempotency_key);
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let fname = crate::persist::sanitize(graph);
    // v1's `VersionExpectation` has no "unversioned" arm available to an ordinary
    // tenant, so this can no longer pass `None` for "don't care". A not-yet-created
    // graph has no MUTATION_GRAPH_VERSION row -- `read_current_mutation_graph_version`
    // (`src/redb_store.rs`) treats that as `INITIAL_GRAPH_VERSION` (0), which is
    // exactly the correct expectation for CreateGraph; DeleteGraph reads the
    // graph's real current version.
    let expected_graph_version = persistence
        .read_mutation_graph_version(&fname)
        .await?
        .unwrap_or(0);
    let batch = compile_methods(
        CompileBatch {
            batch_id: &batch_id,
            request_id,
            attempt_nonce,
            principal,
            tenant: graph,
            graph,
            placement_epoch: 0,
            idempotency_key,
            expected_graph_version: Some(expected_graph_version),
            fencing_token: None,
            created_at_ms,
            default_surface: MutationSurface::Lifecycle,
            authoritative_state: None,
        },
        vec![method],
    )?;
    let encoded_result = rmp_serde::to_vec_named(result).map_err(|e| e.to_string())?;
    persistence
        .commit_mutation_batch(&fname, &batch, Some(&encoded_result), created_at_ms)
        .await
}

/// Did this exact lifecycle request already reach its durable commit point? Used
/// when the registry already reflects Create (or no longer reflects Delete) so a
/// network retry returns the committed outcome instead of creating a second batch.
pub(crate) async fn lifecycle_was_committed(
    persistence: &Arc<dyn PersistenceBackend>,
    attempt: LifecycleAttempt<'_>,
    method: Method,
    result: &ResultPayload,
) -> Result<bool, String> {
    let LifecycleAttempt {
        action,
        graph,
        request_id,
        attempt_nonce,
        principal,
        idempotency_key,
    } = attempt;
    let fname = crate::persist::sanitize(graph);
    let batch_id = lifecycle_batch_id(action, graph, principal, idempotency_key);
    if persistence
        .read_mutation_batch(&fname, &batch_id)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    let committed = commit_lifecycle(
        LifecycleCommitRequest::new(
            persistence,
            action,
            request_id,
            principal,
            idempotency_key,
            graph,
            method,
            result,
        )
        .with_attempt_nonce(attempt_nonce),
    )
    .await?;
    if !committed.replayed {
        return Err("lifecycle replay probe unexpectedly committed fresh work".to_string());
    }
    Ok(persistence
        .read_mutation_lifecycle_head(&fname)
        .await?
        .as_deref()
        == Some(committed.record.batch.batch_id.as_str()))
}
