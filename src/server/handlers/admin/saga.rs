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
    pub(crate) batch: AdminSagaBatch,
    pub(crate) created_at_ms: u64,
    pub(crate) replayed: Option<crate::protocol::ResultPayload>,
    pub(crate) prepared: bool,
}

#[cfg(feature = "redb")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminSagaResultContract {
    Bool,
    Count,
    Text,
    SparqlRecoveryOutcome,
    ShardReshardReport,
    RebalanceExecution,
    RestoreReceipt,
    MultiGraphBatchReport,
    ChannelCreated,
    ChannelDeparture,
    BeliefMaterialization,
}

#[cfg(feature = "redb")]
impl AdminSagaResultContract {
    fn for_method(method: &Method) -> Result<Self, String> {
        if let Some(contract) = Self::for_core_method(method) {
            return Ok(contract);
        }
        if let Some(contract) = Self::for_knowledge_method(method) {
            return Ok(contract);
        }
        if let Some(contract) = Self::for_streaming_method(method) {
            return Ok(contract);
        }
        if let Some(contract) = Self::for_extension_method(method) {
            return Ok(contract);
        }
        Err(format!(
            "admin saga for {} has no declared result contract",
            method.tag_name()
        ))
    }

    fn for_core_method(method: &Method) -> Option<Self> {
        match method {
            Method::Reshard { .. } => Some(Self::ShardReshardReport),
            Method::RebalanceExecute { .. } => Some(Self::RebalanceExecution),
            Method::Restore { .. } => Some(Self::RestoreReceipt),
            Method::MultiGraphBatchUpdate { .. } => Some(Self::MultiGraphBatchReport),
            Method::CatalogAssign { .. }
            | Method::CatalogReassign { .. }
            | Method::CatalogRemove { .. }
            | Method::TxnAddNode { .. }
            | Method::TxnRemoveNode { .. }
            | Method::TxnAddEdge { .. }
            | Method::TxnRemoveEdge { .. }
            | Method::TxnCas { .. }
            | Method::TxnAddEmbedding { .. }
            | Method::TxnBlobRef { .. }
            | Method::TxnAddMeasurement { .. }
            | Method::Rollback { .. } => Some(Self::Bool),
            Method::BeginTxn { .. } | Method::JoinChannel { .. } | Method::SendMessage { .. } => {
                Some(Self::Text)
            }
            Method::CreateChannel { .. } => Some(Self::ChannelCreated),
            Method::LeaveChannel { .. } | Method::CloseChannel { .. } => {
                Some(Self::ChannelDeparture)
            }
            _ => None,
        }
    }

    fn for_knowledge_method(method: &Method) -> Option<Self> {
        match method {
            #[cfg(feature = "owl")]
            Method::TxnAxiom { .. } => Some(Self::Bool),
            #[cfg(feature = "sparql")]
            Method::TxnConstruct { .. } => Some(Self::Bool),
            #[cfg(feature = "query")]
            Method::TxnPlanWriteback { .. } => Some(Self::Bool),
            #[cfg(feature = "epistemic")]
            Method::TxnMaterializeBelief { .. } => Some(Self::BeliefMaterialization),
            _ => None,
        }
    }

    fn for_streaming_method(method: &Method) -> Option<Self> {
        match method {
            #[cfg(feature = "streaming")]
            Method::RegisterContinuousQuery { .. } | Method::RegisterTrigger { .. } => {
                Some(Self::Text)
            }
            #[cfg(feature = "streaming")]
            Method::DropContinuousQuery { .. } | Method::DropTrigger { .. } => Some(Self::Bool),
            #[cfg(all(feature = "streaming", feature = "stream"))]
            Method::CepSubscribe { .. } => Some(Self::Count),
            #[cfg(all(feature = "streaming", feature = "stream"))]
            Method::CepUnsubscribe { .. } => Some(Self::Bool),
            _ => None,
        }
    }

    fn for_extension_method(method: &Method) -> Option<Self> {
        match method {
            #[cfg(feature = "wasm-udf")]
            Method::RegisterUdf { .. } => Some(Self::Text),
            #[cfg(feature = "federation")]
            Method::RegisterForeignSource { .. } => Some(Self::Text),
            #[cfg(feature = "compute-dist")]
            Method::CreateMatView { .. } | Method::RefreshMatView { .. } => Some(Self::Count),
            #[cfg(feature = "matview")]
            Method::PlanMatViewDefine { .. } | Method::PlanMatViewRefresh { .. } => {
                Some(Self::Count)
            }
            #[cfg(feature = "matview")]
            Method::PlanMatViewDrop { .. } => Some(Self::Bool),
            _ => None,
        }
    }

    fn for_private_event(event_type: &str) -> Result<Self, String> {
        match event_type {
            "transaction_recovery_plan" | "sparql_http_compensation_v1" => Ok(Self::Bool),
            "sparql_http_recovery_plan_v1" => Ok(Self::SparqlRecoveryOutcome),
            _ => Err(format!(
                "private admin saga event {event_type:?} has no declared result contract"
            )),
        }
    }

    fn validate(self, result: &crate::protocol::ResultPayload) -> Result<(), String> {
        use crate::protocol::ResultPayload;
        match self {
            Self::Bool => matches!(result, ResultPayload::Bool(_))
                .then_some(())
                .ok_or_else(|| "admin saga replay has the wrong result type".to_string()),
            Self::Count => matches!(result, ResultPayload::Count(_))
                .then_some(())
                .ok_or_else(|| "admin saga replay has the wrong result type".to_string()),
            Self::Text => matches!(result, ResultPayload::String(_))
                .then_some(())
                .ok_or_else(|| "admin saga replay has the wrong result type".to_string()),
            Self::SparqlRecoveryOutcome => validate_sparql_recovery_outcome(result),
            Self::ShardReshardReport => validate_admin_json::<
                eg_types::result_contract::cluster::ShardReshardReport,
            >(result),
            Self::RebalanceExecution => validate_admin_json::<
                eg_types::result_contract::cluster::RebalanceExecution,
            >(result),
            Self::RestoreReceipt => {
                validate_admin_json::<eg_types::storage_wire::RestoreReceipt>(result)
            }
            Self::MultiGraphBatchReport => validate_admin_json::<
                eg_types::result_contract::transactions::MultiGraphBatchReport,
            >(result),
            Self::ChannelCreated => {
                validate_admin_json::<eg_types::messaging_wire::ChannelCreated>(result)
            }
            Self::ChannelDeparture => {
                validate_admin_json::<eg_types::messaging_wire::ChannelDeparture>(result)
            }
            Self::BeliefMaterialization => validate_admin_json::<
                eg_types::result_contract::transactions::BeliefMaterialization,
            >(result),
        }
    }

    fn public_discriminator(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Count => "count",
            Self::Text => "text",
            Self::ShardReshardReport => "shard-reshard-report-v1",
            Self::RebalanceExecution => "rebalance-execution-v1",
            Self::RestoreReceipt => "restore-receipt-v1",
            Self::MultiGraphBatchReport => "multi-graph-batch-report-v1",
            Self::ChannelCreated => "channel-created-v1",
            Self::ChannelDeparture => "channel-departure-v1",
            Self::BeliefMaterialization => "belief-materialization-v1",
            Self::SparqlRecoveryOutcome => unreachable!("private contract used by public saga"),
        }
    }

    fn for_public_discriminator(discriminator: &str) -> Result<Self, String> {
        Self::for_public_scalar_discriminator(discriminator)
            .or_else(|| Self::for_public_structured_discriminator(discriminator))
            .ok_or_else(|| {
                format!(
                    "public admin saga has unknown result-contract discriminator {discriminator:?}"
                )
            })
    }

    fn for_public_scalar_discriminator(discriminator: &str) -> Option<Self> {
        match discriminator {
            "bool" => Some(Self::Bool),
            "count" => Some(Self::Count),
            "text" => Some(Self::Text),
            _ => None,
        }
    }

    fn for_public_structured_discriminator(discriminator: &str) -> Option<Self> {
        match discriminator {
            "shard-reshard-report-v1" => Some(Self::ShardReshardReport),
            "rebalance-execution-v1" => Some(Self::RebalanceExecution),
            "restore-receipt-v1" => Some(Self::RestoreReceipt),
            "multi-graph-batch-report-v1" => Some(Self::MultiGraphBatchReport),
            "channel-created-v1" => Some(Self::ChannelCreated),
            "channel-departure-v1" => Some(Self::ChannelDeparture),
            "belief-materialization-v1" => Some(Self::BeliefMaterialization),
            _ => None,
        }
    }
}

#[cfg(feature = "redb")]
const PUBLIC_ADMIN_EVENT_PREFIX: &str = "cluster_admin_operation/";

#[cfg(feature = "redb")]
fn validate_sparql_recovery_outcome(result: &crate::protocol::ResultPayload) -> Result<(), String> {
    let crate::protocol::ResultPayload::Json(value) = result else {
        return Err("SPARQL recovery replay requires a JSON body".to_string());
    };
    let Some(body) = value.as_object() else {
        return Err("SPARQL recovery replay has an invalid typed JSON body".to_string());
    };
    // All five report fields are required, so a successful decode plus exact
    // object length also excludes unknown fields.
    let success = body.len() == 5
        && serde_json::from_value::<eg_types::result_contract::transactions::SparqlUpdateReport>(
            value.clone(),
        )
        .is_ok();
    let compensated = body.len() == 3
        && body.get("outcome").and_then(serde_json::Value::as_str) == Some("compensated")
        && body
            .get("updated_graphs")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && body
            .get("created_graphs")
            .and_then(serde_json::Value::as_u64)
            == Some(0);
    (success || compensated)
        .then_some(())
        .ok_or_else(|| "SPARQL recovery replay has an invalid typed JSON body".to_string())
}

#[cfg(feature = "redb")]
pub(crate) struct AdminSagaBatch {
    durable: MutationBatch,
    result_contract: Option<AdminSagaResultContract>,
}

#[cfg(feature = "redb")]
impl std::ops::Deref for AdminSagaBatch {
    type Target = MutationBatch;

    fn deref(&self) -> &Self::Target {
        &self.durable
    }
}

#[cfg(feature = "redb")]
impl Clone for AdminSagaBatch {
    fn clone(&self) -> Self {
        Self {
            durable: self.durable.clone(),
            result_contract: self.result_contract,
        }
    }
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
    batch: &mut MutationBatch,
    step: eg_transaction::SagaBegin,
    expected_contract: AdminSagaResultContract,
) -> Result<(Option<crate::protocol::ResultPayload>, bool), String> {
    match step {
        eg_transaction::SagaBegin::Committed(record) => {
            let (record, result) = decode_admin_commit(record, identity, true)?;
            expected_contract.validate(&result)?;
            *batch = record.batch;
            Ok((Some(result), false))
        }
        eg_transaction::SagaBegin::Execute => Ok((None, false)),
        eg_transaction::SagaBegin::Resume(record) => {
            *batch = record.batch;
            Ok((None, true))
        }
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

#[cfg(feature = "redb")]
pub(crate) fn begin_authenticated_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
    domain: DurabilityDomain,
    attempt_nonce: Option<Nonce>,
) -> Result<AdminSaga, String> {
    let stable_id = authority.namespace("cluster-admin-authenticated", authority.idempotency_key());
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
        AdminSagaResultContract::for_method(method)?.validate(result)?;
    }
    Ok(saga)
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

#[cfg(feature = "redb")]
fn public_admin_event_type(
    existing: Option<&MutationBatchRecord>,
    result_contract: AdminSagaResultContract,
) -> String {
    let legacy = existing.is_some_and(|record| {
        matches!(&record.batch.operations[..], [operation]
            if matches!(&operation.method, Method::ApplyMutation { event_type, .. }
                if event_type == "cluster_admin_operation"))
    });
    if legacy {
        "cluster_admin_operation".to_string()
    } else {
        format!(
            "{PUBLIC_ADMIN_EVENT_PREFIX}{}",
            result_contract.public_discriminator()
        )
    }
}

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
    let result_contract = AdminSagaResultContract::for_method(method)?;
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let now = crate::server::dispatch::authoritative_now_ms();
    let read = backend.admin_mutations_read()?;
    let expected = eg_transaction::version(&read)?;
    let existing = eg_transaction::read_ledger(&read, batch_id)?;
    let event_type = public_admin_event_type(existing.as_ref(), result_contract);
    let mut batch = crate::server::mutation_batch::compile_opaque_method(
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
        &event_type,
    )?;
    let step = backend.admin_saga_step(&batch, now, None)?;
    let (replayed, prepared) =
        resolve_admin_saga_step(&identity, &mut batch, step, result_contract)?;
    Ok(AdminSaga {
        batch: AdminSagaBatch {
            durable: batch,
            result_contract: Some(result_contract),
        },
        created_at_ms: now,
        replayed,
        prepared,
    })
}

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
    let result_contract = AdminSagaResultContract::for_private_event(event_type)?;
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let now = crate::server::dispatch::authoritative_now_ms();
    let expected = eg_transaction::version(&backend.admin_mutations_read()?)?;
    let mut batch = crate::server::mutation_batch::compile_opaque_digest(
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
    let step = backend.admin_saga_step(&batch, now, Some(encrypted_payload))?;
    let (replayed, prepared) =
        resolve_admin_saga_step(&identity, &mut batch, step, result_contract)?;
    Ok(AdminSaga {
        batch: AdminSagaBatch {
            durable: batch,
            result_contract: Some(result_contract),
        },
        created_at_ms: now,
        replayed,
        prepared,
    })
}

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
    let result_contract = recovered_admin_result_contract(&record.batch)?;
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
            return resume_committed_admin_saga(record, &identity, result_contract).map(Some);
        }
        crate::mutation_batch::MutationBatchStatus::Aborted => {
            return Err("coordinator receipt was aborted".to_string())
        }
    };
    Ok(Some(AdminSaga {
        batch: AdminSagaBatch {
            durable: record.batch,
            result_contract: Some(result_contract),
        },
        created_at_ms: record.committed_at_ms,
        replayed,
        prepared: true,
    }))
}

#[cfg(feature = "redb")]
fn resume_committed_admin_saga(
    record: MutationBatchRecord,
    identity: &MutationScopeIdentity,
    result_contract: AdminSagaResultContract,
) -> Result<AdminSaga, String> {
    let (record, result) = decode_admin_commit(record, identity, true)?;
    result_contract.validate(&result)?;
    Ok(AdminSaga {
        batch: AdminSagaBatch {
            durable: record.batch,
            result_contract: Some(result_contract),
        },
        created_at_ms: record.committed_at_ms,
        replayed: Some(result),
        prepared: false,
    })
}

#[cfg(feature = "redb")]
fn recovered_admin_result_contract(
    batch: &MutationBatch,
) -> Result<AdminSagaResultContract, String> {
    let [operation] = batch.operations.as_slice() else {
        return Err("coordinator receipt has an invalid operation inventory".to_string());
    };
    let Method::ApplyMutation { event_type, .. } = &operation.method else {
        return Err("coordinator receipt has an invalid durable operation".to_string());
    };
    if let Some(discriminator) = event_type.strip_prefix(PUBLIC_ADMIN_EVENT_PREFIX) {
        AdminSagaResultContract::for_public_discriminator(discriminator)
    } else if event_type == "cluster_admin_operation" {
        Err("public admin saga recovery is missing its result-contract discriminator".to_string())
    } else {
        AdminSagaResultContract::for_private_event(event_type)
    }
}

#[cfg(feature = "redb")]
pub(crate) fn finish_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch: AdminSagaBatch,
    committed_at_ms: u64,
    result: crate::protocol::ResultPayload,
) -> Result<crate::protocol::ResultPayload, String> {
    let result_contract = batch.result_contract.ok_or_else(|| {
        "admin saga recovery requires an explicit original result contract".to_string()
    })?;
    result_contract.validate(&result)?;
    let encoded = rmp_serde::to_vec_named(&result).map_err(|error| error.to_string())?;
    let (record, replayed) = backend.admin_saga_end(&batch.durable, encoded, committed_at_ms)?;
    let (_, durable_result) = decode_admin_commit(record, &batch.identity, replayed)?;
    result_contract.validate(&durable_result)?;
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
    if let Err(error) = saga
        .batch
        .result_contract
        .expect("method-bearing catalog saga has a result contract")
        .validate(&result)
    {
        return Some(Response::err(
            req_id,
            format!(
                "catalog saga replay for {} violates its durable result contract: {error}",
                method.tag_name(),
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
