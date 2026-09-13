//! Durable coordinator saga helpers shared by admin and transaction handlers.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::mutation_batch::{
    DurabilityDomain, MutationBatch, MutationBatchCommit, MutationBatchRecord,
    MutationScopeIdentity, MutationSurface,
};
use crate::protocol::{Method, Response};
use crate::server::state::ServerState;
use eg_types::contract::Nonce;

#[cfg(feature = "redb")]
pub(crate) struct AdminSaga {
    pub(crate) batch: MutationBatch,
    pub(crate) created_at_ms: u64,
    pub(crate) replayed: Option<crate::protocol::ResultPayload>,
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
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "cluster-admin",
        "cluster-admin",
        req_id,
        method,
    );
    begin_named_admin_saga_with_nonce(
        backend,
        req_id,
        caller,
        method,
        domain,
        &batch_id,
        attempt_nonce,
    )
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
    let replayed = match backend.admin_saga_step(&batch, now, None)? {
        eg_transaction::SagaBegin::Committed(record) => {
            let (_, result) = decode_admin_commit(record, &identity, true)?;
            Some(result)
        }
        eg_transaction::SagaBegin::Execute | eg_transaction::SagaBegin::Resume(_) => None,
    };
    Ok(AdminSaga {
        batch,
        created_at_ms: now,
        replayed,
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
    let replayed = match backend.admin_saga_step(&batch, now, Some(encrypted_payload))? {
        eg_transaction::SagaBegin::Committed(record) => {
            let (_, result) = decode_admin_commit(record, &identity, true)?;
            Some(result)
        }
        eg_transaction::SagaBegin::Execute | eg_transaction::SagaBegin::Resume(_) => None,
    };
    Ok(AdminSaga {
        batch,
        created_at_ms: now,
        replayed,
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
    }))
}

#[cfg(feature = "redb")]
pub(crate) fn finish_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch: MutationBatch,
    committed_at_ms: u64,
    result: crate::protocol::ResultPayload,
) -> Result<crate::protocol::ResultPayload, String> {
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
    Ok((commit.record, result))
}

#[cfg(feature = "redb")]
pub(crate) fn catalog_saga(
    req_id: u64,
    caller: Option<&str>,
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    method: &Method,
    attempt_nonce: Option<Nonce>,
    apply: impl FnOnce(&crate::server::persistence::tenant_catalog::TenantCatalog) -> Result<(), String>,
) -> Response {
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
    if let Some(result) = saga.replayed {
        return Response::ok(req_id, result);
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
        crate::protocol::ResultPayload::Bool(true),
    ) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
pub(crate) fn report_json(
    report: &crate::server::persistence::online_reshard::ReshardReport,
) -> serde_json::Value {
    serde_json::json!({
        "graph": report.graph,
        "from_shard": report.from_shard,
        "to_shard": report.to_shard,
        "nodes": report.nodes,
        "edges": report.edges,
        "ledger": report.ledger,
        "semantic": report.semantic,
        "audit": report.audit,
        "delta_nodes": report.delta_nodes,
        "delta_edges": report.delta_edges,
        "no_op": report.no_op,
    })
}

#[cfg(feature = "redb")]
pub(crate) fn plan_json(
    plan: &crate::server::persistence::rebalance::RebalancePlan,
    shards: &[crate::server::persistence::rebalance::ShardLoad],
) -> serde_json::Value {
    let moves: Vec<serde_json::Value> = plan
        .moves
        .iter()
        .map(|m| {
            serde_json::json!({
                "graph": m.graph,
                "from_shard": m.from_shard,
                "to_shard": m.to_shard,
            })
        })
        .collect();
    let loads: Vec<serde_json::Value> = shards
        .iter()
        .map(
            |s| serde_json::json!({"shard": s.shard, "total": s.total(), "graphs": s.graphs.len()}),
        )
        .collect();
    serde_json::json!({"moves": moves, "shards": loads})
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
