use std::sync::Arc;

use crate::mutation_batch::MutationSurface;
use crate::protocol::{Method, ResultPayload};
use crate::server::mutation::LifecycleAttempt;
use crate::server::persistence::PersistenceBackend;
use eg_types::contract::Nonce;

use super::super::compile::{compile_methods, CompileBatch};
use super::super::digest::{lifecycle_batch_id, principal_fingerprint};

pub(crate) struct LifecycleCommitRequest<'a> {
    pub(crate) persistence: &'a Arc<dyn PersistenceBackend>,
    pub(crate) action: &'a str,
    pub(crate) request_id: u64,
    attempt_nonce: Option<Nonce>,
    /// Verified carrier tenant scope (EH-375), set by [`Self::with_tenant_scope`].
    /// `None` fails the commit closed: a lifecycle batch id is never tenant-less.
    tenant_scope: Option<&'a str>,
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
        origin: super::CommitOrigin<'a>,
        idempotency_key: &'a str,
        graph: &'a str,
        method: Method,
        result: &'a ResultPayload,
    ) -> Self {
        Self {
            persistence,
            action,
            request_id: origin.request_id,
            attempt_nonce: None,
            tenant_scope: None,
            principal: origin.principal,
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

    /// Bind the verified carrier tenant scope the durable batch id is keyed on.
    pub(crate) fn with_tenant_scope(mut self, tenant_scope: &'a str) -> Self {
        self.tenant_scope = Some(tenant_scope);
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
        tenant_scope,
        principal,
        idempotency_key,
        graph,
        method,
        result,
    } = request;
    let tenant_scope = tenant_scope
        .ok_or_else(|| "lifecycle commit requires a verified tenant scope".to_string())?;
    let batch_id = lifecycle_batch_id(action, tenant_scope, graph, principal, idempotency_key);
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
    let committed = persistence
        .commit_mutation_batch(&fname, &batch, Some(&encoded_result), created_at_ms)
        .await?;
    validate_lifecycle_replay_result(&committed, &encoded_result)?;
    Ok(committed)
}

fn validate_lifecycle_replay_result(
    committed: &crate::mutation_batch::MutationBatchCommit,
    encoded_result: &[u8],
) -> Result<(), String> {
    if committed.replayed && committed.record.result_msgpack.as_deref() != Some(encoded_result) {
        return Err("lifecycle receipt has a conflicting terminal result".to_string());
    }
    Ok(())
}

/// Did this exact lifecycle request already reach its durable commit point? Used
/// when the registry already reflects Create (or no longer reflects Delete) so a
/// network retry returns the committed outcome instead of creating a second batch.
///
/// The answer compares the retained lifecycle IDENTITY of the stored receipt --
/// graph, graph type, principal, tenant scope and idempotency key -- and never
/// recompiles the batch. A recompiled batch carries build-dependent bytes (the
/// contract catalog digest, method schema and policy digests, encoder output), so
/// comparing its digest with a receipt an earlier release wrote refused every
/// retry after an upgrade (#709). The tenant scope, action, graph, principal and
/// key address the receipt through [`lifecycle_batch_id`]; the receipt's own
/// operation, actor attribution and idempotency key are then checked against the
/// request, and any disagreement is a conflict, not a replay.
pub(crate) async fn lifecycle_was_committed(
    persistence: &Arc<dyn PersistenceBackend>,
    attempt: LifecycleAttempt<'_>,
    method: Method,
) -> Result<bool, String> {
    let LifecycleAttempt {
        action,
        tenant_scope,
        graph,
        principal,
        idempotency_key,
        request_id: _,
    } = attempt;
    let fname = crate::persist::sanitize(graph);
    let batch_id = lifecycle_batch_id(action, tenant_scope, graph, principal, idempotency_key);
    let Some(record) = persistence.read_mutation_batch(&fname, &batch_id).await? else {
        return Ok(false);
    };
    if record.status != eg_types::mutation_batch::MutationBatchStatus::Committed {
        return Ok(false);
    }
    lifecycle_identity_matches(&record, &batch_id, principal, idempotency_key, &method)?;
    // The stored receipt IS the answer. Stale retries across a delete/recreate are
    // fenced by the kernel: purging a graph retires its scope binding and its
    // receipts, so an old incarnation's batch is no longer readable above
    // (`store_cleanup::purge_graph_rows`, RF-RULING-004).
    Ok(true)
}

/// Refuse a stored lifecycle receipt whose retained identity differs from the
/// request. Build-dependent envelope bytes are deliberately not compared.
pub(crate) fn lifecycle_identity_matches(
    record: &crate::mutation_batch::MutationBatchRecord,
    batch_id: &str,
    principal: Option<&str>,
    idempotency_key: &str,
    method: &Method,
) -> Result<(), String> {
    const CONFLICT: &str = "lifecycle receipt has a conflicting identity";
    if record.batch.batch_id != batch_id {
        return Err(format!("{CONFLICT}: batch id"));
    }
    let principal =
        principal.ok_or_else(|| "lifecycle replay requires a verified principal".to_string())?;
    if record.committing_actor()? != principal_fingerprint(principal)? {
        return Err(format!("{CONFLICT}: principal"));
    }
    let stored_key = record
        .batch
        .envelope
        .operation()
        .and_then(|operation| operation.authority.idempotency_key.as_ref())
        .map(|key| key.as_str());
    if stored_key != Some(idempotency_key) {
        return Err(format!("{CONFLICT}: idempotency key"));
    }
    let [operation] = record.batch.operations.as_slice() else {
        return Err(format!("{CONFLICT}: operation count"));
    };
    let same_target = match (&operation.method, method) {
        (
            Method::CreateGraph {
                graph_name: stored_graph,
                graph_type: stored_type,
            },
            Method::CreateGraph {
                graph_name,
                graph_type,
            },
        ) => stored_graph == graph_name && stored_type == graph_type,
        (
            Method::DeleteGraph {
                graph_name: stored_graph,
            },
            Method::DeleteGraph { graph_name },
        ) => stored_graph == graph_name,
        _ => false,
    };
    if !same_target {
        return Err(format!("{CONFLICT}: graph or graph type"));
    }
    Ok(())
}
