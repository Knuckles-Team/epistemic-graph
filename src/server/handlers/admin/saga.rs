//! Durable coordinator saga helpers shared by admin and transaction handlers.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::mutation_batch::{
    DurabilityDomain, MutationBatch, MutationBatchCommit, MutationBatchRecord,
    MutationScopeIdentity, MutationSurface,
};
use crate::protocol::{Method, Response};
use crate::server::access::CarrierAuthority;
use crate::server::state::ServerState;
use eg_types::contract::Nonce;

// This authority exists only while polling one authenticated request. It is
// neither process-global state nor inherited by independently spawned tasks.
tokio::task_local! {
    static AUTHENTICATED_SAGA_AUTHORITY: CarrierAuthority;
}

pub(crate) async fn scope_admin_saga_authority<F: std::future::Future>(
    authority: CarrierAuthority,
    request: F,
) -> F::Output {
    AUTHENTICATED_SAGA_AUTHORITY.scope(authority, request).await
}

pub(crate) fn current_admin_saga_authority() -> Result<CarrierAuthority, String> {
    AUTHENTICATED_SAGA_AUTHORITY
        .try_with(Clone::clone)
        .map_err(|_| "admin saga requires authenticated request authority".to_string())
}

#[cfg(feature = "redb")]
pub(crate) struct AdminSaga {
    pub(crate) batch: MutationBatch,
    pub(crate) created_at_ms: u64,
    pub(crate) replayed: Option<crate::protocol::ResultPayload>,
    /// A durable operation can be Prepared (resume required) or Execute (first
    /// attempt); both have no replay payload, so keep that distinction explicit.
    pub(crate) prepared: bool,
}

#[cfg(feature = "redb")]
fn validate_admin_attempt_nonce(
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
) -> Result<(), String> {
    if attempt_nonce.is_some() && attempt_nonce != authority.attempt_nonce() {
        Err("admin saga nonce does not match authenticated request".to_string())
    } else {
        Ok(())
    }
}

#[cfg(feature = "redb")]
fn require_admin_saga_execution(saga: AdminSaga, refusal: &str) -> Result<AdminSaga, String> {
    if saga.prepared {
        Err(refusal.to_string())
    } else {
        Ok(saga)
    }
}

#[cfg(feature = "redb")]
fn resolve_admin_saga_step(
    identity: &MutationScopeIdentity,
    step: eg_transaction::SagaBegin,
) -> Result<(Option<crate::protocol::ResultPayload>, bool), String> {
    match step {
        eg_transaction::SagaBegin::Committed(record) => {
            let (_, result) = decode_admin_commit(record, identity, true)?;
            Ok((Some(result), false))
        }
        eg_transaction::SagaBegin::Execute => Ok((None, false)),
        eg_transaction::SagaBegin::Resume(_) => Ok((None, true)),
    }
}

#[cfg(feature = "redb")]
pub(crate) fn begin_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    domain: DurabilityDomain,
) -> Result<AdminSaga, String> {
    begin_admin_saga_with_nonce(backend, req_id, caller, method, domain, None)
}

#[cfg(feature = "redb")]
pub(crate) fn begin_admin_saga_with_nonce(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    domain: DurabilityDomain,
    attempt_nonce: Option<Nonce>,
) -> Result<AdminSaga, String> {
    let authority = current_admin_saga_authority()?;
    let _ = caller; // The verified scope is the sole durable actor authority.
    validate_admin_attempt_nonce(&authority, attempt_nonce)?;
    let saga = begin_authenticated_admin_saga(
        backend,
        req_id,
        &authority,
        method,
        domain,
        authority.attempt_nonce(),
    )?;
    require_admin_saga_execution(
        saga,
        "admin saga is Prepared; refusing to re-execute its mutation",
    )
}

/// Begin a session-control saga under the authenticated carrier's stable
/// tenant/principal/idempotency scope. The request id remains provenance only;
/// it must never select a replay row.
#[cfg(feature = "redb")]
pub(crate) fn begin_authenticated_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
    domain: DurabilityDomain,
    attempt_nonce: Option<Nonce>,
) -> Result<AdminSaga, String> {
    let stable_id = authenticated_admin_saga_id(authority);
    let saga = begin_named_admin_saga_with_nonce(
        backend,
        req_id,
        Some(authority.actor_scope()),
        method,
        domain,
        &stable_id,
        attempt_nonce,
    )?;
    if let Some(result) = saga.replayed.as_ref() {
        validate_admin_method_result(method, result)?;
    }
    Ok(saga)
}

#[cfg(feature = "redb")]
fn authenticated_admin_saga_id(authority: &CarrierAuthority) -> String {
    // Payload belongs to the operation digest, not its lookup key: changing the
    // method under one authenticated idempotency key must conflict, not execute.
    authority.namespace("cluster-admin-authenticated", authority.idempotency_key())
}

/// Check concrete replay bodies while the original method is available, before
/// opaque durable encoding erases its discriminant.
#[cfg(feature = "redb")]
pub(crate) fn validate_admin_method_result(
    method: &Method,
    result: &crate::protocol::ResultPayload,
) -> Result<(), String> {
    use crate::protocol::ResultPayload;
    match method {
        Method::Reshard { .. } => {
            validate_admin_json::<eg_types::result_contract::cluster::ShardReshardReport>(result)
        }
        Method::RebalanceExecute { .. } => {
            validate_admin_json::<eg_types::result_contract::cluster::RebalanceExecution>(result)
        }
        Method::Restore { .. } => {
            validate_admin_json::<eg_types::storage_wire::RestoreReceipt>(result)
        }
        Method::MultiGraphBatchUpdate { .. } => validate_admin_json::<
            eg_types::result_contract::transactions::MultiGraphBatchReport,
        >(result),
        Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. } => {
            require_admin_result(matches!(result, ResultPayload::Bool(_)))
        }
        #[cfg(feature = "compute-dist")]
        Method::CreateMatView { .. } | Method::RefreshMatView { .. } => {
            require_admin_result(matches!(result, ResultPayload::Count(_)))
        }
        #[cfg(feature = "matview")]
        Method::PlanMatViewDefine { .. } | Method::PlanMatViewRefresh { .. } => {
            require_admin_result(matches!(result, ResultPayload::Count(_)))
        }
        #[cfg(feature = "matview")]
        Method::PlanMatViewDrop { .. } => {
            require_admin_result(matches!(result, ResultPayload::Bool(_)))
        }
        // Session-control and txn lifecycle boundaries retain their method and
        // validate their own typed contract before returning a replay.
        _ => Ok(()),
    }
}

#[cfg(feature = "redb")]
fn require_admin_result(valid: bool) -> Result<(), String> {
    if valid {
        Ok(())
    } else {
        Err("admin saga replay has the wrong result type".to_string())
    }
}

#[cfg(feature = "redb")]
fn validate_admin_json<T: serde::de::DeserializeOwned>(
    result: &crate::protocol::ResultPayload,
) -> Result<(), String> {
    let crate::protocol::ResultPayload::Json(value) = result else {
        return Err("admin saga replay requires a JSON body".to_string());
    };
    serde_json::from_value::<T>(value.clone())
        .map(|_| ())
        .map_err(|_| "admin saga replay has an invalid typed JSON body".to_string())
}

// `admin_saga_request_stamp` is DELETED, not moved.
//
// It replayed the ORIGINAL attempt's `request_id`, `created_at_ms` and observed
// OCC version back into a rebuilt batch, and its own doc comment said exactly
// why: "Re-deriving them live on every call makes a legitimate replay's
// freshly-compiled batch byte-diverge ... spuriously failing closed with
// IDEMPOTENCY_CONFLICT." That was true while the kernel decided replay by
// WHOLE-BATCH byte identity. `OperationReplayIdentity` structurally excludes all
// three -- it has no timestamp field, no request id, and this slice's canonical
// payload digest excludes the OCC expectation -- so re-deriving them live is now
// correct and the workaround is not merely unnecessary but wrong: reusing a
// stale OCC observation would make the retry claim a version it never observed.

#[cfg(feature = "redb")]
pub(crate) fn begin_named_admin_saga_with_nonce(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    domain: DurabilityDomain,
    batch_id: &str,
    attempt_nonce: Option<Nonce>,
) -> Result<AdminSaga, String> {
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let now = crate::server::dispatch::authoritative_now_ms();
    // Live values on every attempt: a retry legitimately observes a later OCC
    // version, a new dispatch request id and a later clock, and the stable
    // operation identity excludes all three.
    let expected = eg_transaction::version(&backend.admin_mutations_read()?)?;
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: req_id,
            attempt_nonce,
            principal: caller,
            tenant: "native",
            graph: "cluster-admin",
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        method,
        MutationSurface::Other,
        domain,
        "cluster_admin_operation",
    )?;
    let (replayed, prepared) =
        resolve_admin_saga_step(&identity, backend.admin_saga_step(&batch, now, None)?)?;
    Ok(AdminSaga {
        batch,
        created_at_ms: now,
        replayed,
        prepared,
    })
}

/// Begin or resume a digest-only coordinator and atomically attach opaque private
/// recovery bytes.  The caller must supply authenticated ciphertext whose plaintext
/// SHA-256 is `payload_digest`; neither the canonical batch nor its outbox contains
/// the private body.
/// The sealed private-payload fields for the named private-payload saga,
/// bundled so the function stays under the clippy argument-count ceiling.
#[cfg(feature = "redb")]
pub(crate) struct AdminSagaPayload<'a> {
    pub(crate) domain: DurabilityDomain,
    pub(crate) batch_id: &'a str,
    pub(crate) event_type: &'a str,
    pub(crate) payload_digest: &'a str,
    pub(crate) encrypted_payload: &'a [u8],
}

#[cfg(feature = "redb")]
pub(crate) fn begin_named_admin_saga_with_private_payload_and_nonce(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    attempt_nonce: Option<Nonce>,
    payload: AdminSagaPayload<'_>,
) -> Result<AdminSaga, String> {
    let AdminSagaPayload {
        domain,
        batch_id,
        event_type,
        payload_digest,
        encrypted_payload,
    } = payload;
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let now = crate::server::dispatch::authoritative_now_ms();
    // Live values on every attempt: a retry legitimately observes a later OCC
    // version, a new dispatch request id and a later clock, and the stable
    // operation identity excludes all three.
    let expected = eg_transaction::version(&backend.admin_mutations_read()?)?;
    let batch = crate::server::mutation_batch::compile_opaque_digest(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: req_id,
            attempt_nonce,
            principal: caller,
            tenant: "native",
            graph: "cluster-admin",
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        payload_digest,
        MutationSurface::Transaction,
        domain,
        event_type,
    )?;
    let (replayed, prepared) = resolve_admin_saga_step(
        &identity,
        backend.admin_saga_step(&batch, now, Some(encrypted_payload))?,
    )?;
    Ok(AdminSaga {
        batch,
        created_at_ms: now,
        replayed,
        prepared,
    })
}

/// Re-open the exact durable coordinator batch without reconstructing its original
/// payload-bearing operation.  This is the crash-recovery path after ephemeral
/// staging has disappeared.
#[cfg(feature = "redb")]
pub(crate) fn resume_named_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch_id: &str,
    caller: Option<&str>,
) -> Result<Option<AdminSaga>, String> {
    let expected_principal = crate::server::mutation_batch::principal_fingerprint(
        caller.ok_or_else(|| "coordinator recovery requires a verified principal".to_string())?,
    )?;
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let Some(record) = eg_transaction::read_ledger(&backend.admin_mutations_read()?, batch_id)?
    else {
        return Ok(None);
    };
    validate_admin_record(&record, &identity)?;
    validate_admin_lookup_key(&record, batch_id)?;
    // The caller lives in the outbox `actor` header, never in
    // `context.principal` -- which is now the committing ledger's serving
    // principal on every domain (RF-RULING-004 application note). A batch with
    // no header is refused rather than matched.
    if record.committing_actor()? != expected_principal {
        return Err("coordinator receipt does not match caller scope".to_string());
    }
    let replayed = match record.status {
        crate::mutation_batch::MutationBatchStatus::Prepared => None,
        crate::mutation_batch::MutationBatchStatus::Committed => {
            let (record, result) = decode_admin_commit(record, &identity, true)?;
            return Ok(Some(AdminSaga {
                batch: record.batch,
                created_at_ms: record.committed_at_ms,
                replayed: Some(result),
                prepared: false,
            }));
        }
        crate::mutation_batch::MutationBatchStatus::Aborted => {
            return Err("coordinator receipt was aborted".to_string())
        }
    };
    Ok(Some(AdminSaga {
        batch: record.batch,
        created_at_ms: record.committed_at_ms,
        replayed,
        prepared: true,
    }))
}

#[cfg(feature = "redb")]
pub(crate) fn finish_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch: MutationBatch,
    committed_at_ms: u64,
    result: crate::protocol::ResultPayload,
) -> Result<crate::protocol::ResultPayload, String> {
    validate_admin_result(&batch, &result)?;
    let encoded = rmp_serde::to_vec_named(&result).map_err(|error| error.to_string())?;
    let (record, replayed) = backend.admin_saga_end(&batch, encoded, committed_at_ms)?;
    let (_, durable_result) = decode_admin_commit(record, &batch.identity, replayed)?;
    Ok(durable_result)
}

#[cfg(feature = "redb")]
fn validate_admin_record(
    record: &MutationBatchRecord,
    expected_identity: &MutationScopeIdentity,
) -> Result<(), String> {
    record.validate()?;
    if &record.identity != expected_identity {
        return Err("admin saga receipt does not match its requested scope".to_string());
    }
    Ok(())
}

#[cfg(feature = "redb")]
fn validate_admin_lookup_key(record: &MutationBatchRecord, batch_id: &str) -> Result<(), String> {
    if record.batch.batch_id != batch_id || record.batch.idempotency_key() != batch_id {
        return Err("coordinator receipt identity is corrupt".to_string());
    }
    Ok(())
}

#[cfg(feature = "redb")]
fn decode_admin_commit(
    record: MutationBatchRecord,
    expected_identity: &MutationScopeIdentity,
    replayed: bool,
) -> Result<(MutationBatchRecord, crate::protocol::ResultPayload), String> {
    let commit = MutationBatchCommit {
        record,
        identity: expected_identity.clone(),
        replayed,
    };
    commit.validate()?;
    let bytes = commit
        .record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed admin saga has no result".to_string())?;
    let result = rmp_serde::from_slice(bytes).map_err(|error| error.to_string())?;
    validate_admin_result(&commit.record.batch, &result)?;
    Ok((commit.record, result))
}

/// Validate the result shape at the durable coordinator boundary for methods
/// whose replay callers have a fixed wire contract.  The coordinator stores
/// the outer `ResultPayload`; these checks therefore inspect that payload
/// directly and deliberately do not route it through `ResultPayload::of_receipt`,
/// which is reserved for stores that persist a method body without the outer
/// envelope.
#[cfg(feature = "redb")]
fn validate_admin_result(
    batch: &MutationBatch,
    result: &crate::protocol::ResultPayload,
) -> Result<(), String> {
    let Some(method) = batch.operations.first().map(|operation| &operation.method) else {
        return Ok(());
    };
    // `begin_admin_saga*` intentionally lowers the caller's original method to
    // an opaque `ApplyMutation` digest before it reaches this coordinator.  The
    // digest does not retain the original result contract, so only the private
    // transaction-recovery event has a shape that can be validated here.  The
    // session-control boundary validates its original method before admission
    // and again when it serves a replay.
    let expected = match method {
        Method::ApplyMutation { event_type, .. } if event_type == "transaction_recovery_plan" => {
            Some("Bool")
        }
        _ => None,
    };
    let Some(expected) = expected else {
        return Ok(());
    };
    let matches = match expected {
        "Bool" => matches!(result, crate::protocol::ResultPayload::Bool(_)),
        "Count" => matches!(result, crate::protocol::ResultPayload::Count(_)),
        "String" => matches!(result, crate::protocol::ResultPayload::String(_)),
        "Json" => matches!(result, crate::protocol::ResultPayload::Json(_)),
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(format!(
            "admin saga result for {} has the wrong payload type; expected {expected}",
            method.tag_name()
        ))
    }
}

#[cfg(feature = "redb")]
fn catalog_saga_replay_response(
    req_id: u64,
    method: &Method,
    saga: &AdminSaga,
) -> Option<Response> {
    if saga.prepared {
        return Some(Response::err(
            req_id,
            "catalog saga is Prepared; refusing to re-execute its mutation",
        ));
    }
    let Some(result) = saga.replayed.clone() else {
        return None;
    };
    if !matches!(result, crate::protocol::ResultPayload::Bool(_)) {
        return Some(Response::err(
            req_id,
            format!(
                "catalog saga replay for {} has the wrong payload type; expected Bool",
                method.tag_name()
            ),
        ));
    }
    Some(Response::ok(req_id, result))
}

#[cfg(feature = "redb")]
pub(crate) fn catalog_saga<M>(
    req_id: u64,
    caller: Option<&str>,
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    method: &Method,
    attempt_nonce: Option<Nonce>,
    apply: impl FnOnce(&crate::server::persistence::tenant_catalog::TenantCatalog) -> Result<(), String>,
) -> Response
where
    M: eg_types::result_contract::MethodResult<
        Body = bool,
        Encoding = eg_types::result_contract::encoding::Bool,
    >,
{
    let saga = match begin_admin_saga_with_nonce(
        backend,
        req_id,
        caller,
        method,
        DurabilityDomain::ControlPlane,
        attempt_nonce,
    ) {
        Ok(saga) => saga,
        Err(error) => return Response::err(req_id, error),
    };
    if let Some(response) = catalog_saga_replay_response(req_id, method, &saga) {
        return response;
    }
    let Some(catalog) = backend.catalog() else {
        return no_catalog(req_id);
    };
    if let Err(error) = apply(&catalog) {
        return Response::err(req_id, format!("catalog write failed: {error}"));
    }
    match finish_admin_saga(
        backend,
        saga.batch,
        saga.created_at_ms,
        crate::protocol::ResultPayload::scalar::<M>(true),
    ) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
pub(crate) fn reshard_report(
    report: &crate::server::persistence::online_reshard::ReshardReport,
) -> eg_types::result_contract::cluster::ShardReshardReport {
    eg_types::result_contract::cluster::ShardReshardReport {
        graph: report.graph.clone(),
        from_shard: report.from_shard as u64,
        to_shard: report.to_shard as u64,
        nodes: report.nodes,
        edges: report.edges,
        ledger: report.ledger,
        semantic: report.semantic,
        audit: report.audit,
        delta_nodes: report.delta_nodes,
        delta_edges: report.delta_edges,
        no_op: report.no_op,
    }
}

#[cfg(feature = "redb")]
pub(crate) fn rebalance_plan_report(
    plan: &crate::server::persistence::rebalance::RebalancePlan,
    shards: &[crate::server::persistence::rebalance::ShardLoad],
) -> eg_types::result_contract::cluster::RebalancePlanReport {
    eg_types::result_contract::cluster::RebalancePlanReport {
        moves: plan
            .moves
            .iter()
            .map(
                |planned| eg_types::result_contract::cluster::RebalanceMove {
                    graph: planned.graph.clone(),
                    from_shard: planned.from_shard,
                    to_shard: planned.to_shard,
                },
            )
            .collect(),
        shards: shards
            .iter()
            .map(
                |shard| eg_types::result_contract::cluster::ShardLoadSummary {
                    shard: shard.shard,
                    total: shard.total(),
                    graphs: shard.graphs.len() as u64,
                },
            )
            .collect(),
    }
}

#[cfg(feature = "redb")]
pub(crate) fn no_catalog(req_id: u64) -> Response {
    Response::err(
        req_id,
        "no tenant catalog attached (set EPISTEMIC_GRAPH_TENANT_CATALOG=1 and restart)",
    )
}

#[cfg(feature = "redb")]
pub(crate) fn rebalance_opts(
    tolerance: Option<f64>,
    max_moves: Option<usize>,
) -> crate::server::persistence::rebalance::RebalanceOptions {
    let mut opts = crate::server::persistence::rebalance::RebalanceOptions::default();
    if let Some(t) = tolerance {
        opts.tolerance = t;
    }
    if let Some(m) = max_moves {
        opts.max_moves = m;
    }
    opts
}

/// Live per-graph load `(sanitized_fname, resident_node_count)` over the registry + the
/// shard count K (CONCEPT:EG-KG.sharding.even-load-rebalance integration). Resident node count is the KG-2.51 per-graph
/// size dimension — a cheap, available balance metric. `__commons__` is included like any
/// other graph. Returns `(loads, k)`.
#[cfg(feature = "redb")]
pub(crate) async fn live_graph_loads(
    state: &Arc<RwLock<ServerState>>,
) -> (Vec<(String, u64)>, usize) {
    let s = state.read().await;
    let loads: Vec<(String, u64)> = s
        .registry
        .all_entries()
        .iter()
        .map(|e| {
            (
                crate::persist::sanitize(&e.name),
                e.core.node_count() as u64,
            )
        })
        .collect();
    let k = s
        .persistence
        .as_ref()
        .and_then(|p| p.as_redb())
        .map(|r| r.shard_count())
        .unwrap_or(1);
    (loads, k)
}

#[cfg(test)]
#[path = "saga_tests.rs"]
mod saga_tests;
