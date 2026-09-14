//! Volatile transaction lifecycle admission and typed replay.

use super::*;

/// Every mutating transaction-family method bypasses the transport replay
/// ledger because its effect belongs to the durable kernel.  Begin/stage/
/// rollback used to be the hole in that rule: they changed in-memory
/// `open_txns` state without ever presenting the verified nonce to the kernel.
/// Use the existing named admin saga as the one admission/receipt authority for
/// those lifecycle steps; this helper does not introduce a second replay table.
pub(in crate::server::handlers::txn) fn is_txn_lifecycle_method(method: &Method) -> bool {
    matches!(method, Method::BeginTxn { .. })
        || (method_txn_id(method).is_some() && !matches!(method, Method::Commit { .. }))
}

/// A terminal lifecycle receipt is useful only while the volatile transaction
/// handle it describes is still present.  The durable saga may outlive the
/// process, but it cannot recreate `open_txns`; returning its old success after
/// restart would hand the caller a dead Begin handle or claim a Stage/Rollback
/// succeeded before the next request fails with `unknown transaction`.
pub(in crate::server::handlers::txn) fn validate_txn_lifecycle_result(
    method: &Method,
    result: &ResultPayload,
) -> Result<(), String> {
    let expected = match method {
        Method::BeginTxn { .. } => "String",
        Method::TxnAddNode { .. }
        | Method::TxnRemoveNode { .. }
        | Method::TxnAddEdge { .. }
        | Method::TxnRemoveEdge { .. }
        | Method::TxnCas { .. }
        | Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. }
        | Method::Rollback { .. } => "Bool",
        Method::TxnAddMeasurement { .. } => "Bool",
        #[cfg(feature = "owl")]
        Method::TxnAxiom { .. } => "Bool",
        #[cfg(feature = "sparql")]
        Method::TxnConstruct { .. } => "Bool",
        #[cfg(feature = "query")]
        Method::TxnPlanWriteback { .. } => "Bool",
        #[cfg(feature = "epistemic")]
        Method::TxnMaterializeBelief { .. } => "BeliefMaterialization",
        _ => {
            return Err(format!(
                "transaction lifecycle receipt is not valid for {}",
                method.tag_name()
            ));
        }
    };
    let matches = matches_lifecycle_result(expected, result);
    if matches {
        Ok(())
    } else {
        Err(format!(
            "transaction lifecycle replay for {} has the wrong payload type; expected {expected}",
            method.tag_name()
        ))
    }
}

fn matches_lifecycle_result(expected: &str, result: &ResultPayload) -> bool {
    match expected {
        "String" => matches!(result, ResultPayload::String(_)),
        "Bool" => matches!(result, ResultPayload::Bool(_)),
        "BeliefMaterialization" => matches!(
            result,
            ResultPayload::Json(value)
                if serde_json::from_value::<
                    eg_types::result_contract::transactions::BeliefMaterialization,
                >(value.clone())
                .is_ok()
        ),
        _ => false,
    }
}

pub(in crate::server::handlers::txn) async fn validate_txn_lifecycle_replay(
    state: &Arc<RwLock<ServerState>>,
    method: &Method,
    owner: &str,
    result: &ResultPayload,
) -> Result<(), String> {
    validate_txn_lifecycle_result(method, result)?;
    let txn_id = if matches!(method, Method::BeginTxn { .. }) {
        match result {
            ResultPayload::String(txn_id) => txn_id.as_str(),
            _ => {
                return Err(
                    "transaction lifecycle receipt has the wrong BeginTxn result".to_string(),
                );
            }
        }
    } else {
        method_txn_id(method).ok_or_else(|| {
            "transaction lifecycle receipt has no volatile transaction handle".to_string()
        })?
    };
    let s = state.read().await;
    let Some(entry) = s.open_txns.get(txn_id) else {
        return Err(
            "transaction lifecycle receipt is terminal but volatile staging state is unavailable; \
             refusing to return a stale success"
                .to_string(),
        );
    };
    if entry.value().lock().agent != owner {
        return Err("transaction lifecycle receipt does not match caller scope".to_string());
    }
    Ok(())
}

pub(in crate::server::handlers::txn) fn txn_lifecycle_batch_id(
    authority: &CarrierAuthority,
    method: &Method,
) -> String {
    // Keep one envelope idempotency key reusable across different transaction
    // operations by including the operation family in the opaque coordinator
    // input.  The full method body remains in the kernel operation digest, so a
    // changed txn id, graph, or payload still conflicts under the same key.
    let operation_key = format!("{}:{}", method.tag_name(), authority.idempotency_key());
    crate::server::mutation_batch::opaque_coordinator_key(
        "transaction-lifecycle",
        authority.owner_scope(),
        &operation_key,
    )
}

#[cfg(feature = "redb")]
pub(crate) struct TxnLifecycleReceipt {
    pub(in crate::server::handlers::txn) backend:
        Arc<dyn crate::server::persistence::PersistenceBackend>,
    pub(in crate::server::handlers::txn) saga: crate::server::handlers::admin::AdminSaga,
}

#[cfg(not(feature = "redb"))]
pub(crate) struct TxnLifecycleReceipt;

#[cfg(feature = "redb")]
pub(in crate::server::handlers::txn) async fn begin_txn_lifecycle_receipt(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: &str,
    authority: &CarrierAuthority,
    method: &Method,
) -> Result<TxnLifecycleReceipt, String> {
    let _ = caller;
    let caller = authority.actor_scope();
    let backend = state.read().await.persistence.clone().ok_or_else(|| {
        "transaction lifecycle requires an authoritative MutationBatch backend".to_string()
    })?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "transaction lifecycle requires durable redb".to_string())?;
    let batch_id = txn_lifecycle_batch_id(authority, method);
    // A lifecycle saga is only the replay authority; its effect still lives in
    // the volatile transaction registry.  If a prior attempt prepared that
    // saga and then died before terminalization, executing the method again
    // would duplicate Begin/Stage/Rollback (or silently operate on a different
    // handle after restart).  Probe the same durable coordinator before
    // admission so a Prepared receipt becomes an explicit lost-staging
    // refusal.  The real admission below still consumes the exact nonce and
    // therefore preserves the kernel's REPLAY_NONCE_CONSUMED / conflict
    // decisions for retries.
    let prepared =
        crate::server::handlers::admin::resume_named_admin_saga(redb, &batch_id, Some(caller))?
            .is_some_and(|saga| saga.prepared);
    let saga = crate::server::handlers::admin::begin_named_admin_saga_with_nonce(
        redb,
        req_id,
        Some(caller),
        method,
        crate::mutation_batch::DurabilityDomain::ControlPlane,
        &batch_id,
        authority.attempt_nonce(),
    )?;
    if prepared || saga.prepared {
        return Err(
            "transaction lifecycle receipt is Prepared but volatile staging state is unavailable; \
             refusing to re-execute an ambiguous lifecycle operation"
                .to_string(),
        );
    }
    Ok(TxnLifecycleReceipt { backend, saga })
}

#[cfg(not(feature = "redb"))]
pub(in crate::server::handlers::txn) async fn begin_txn_lifecycle_receipt(
    _state: &Arc<RwLock<ServerState>>,
    _req_id: u64,
    _caller: &str,
    _authority: &CarrierAuthority,
    _method: &Method,
) -> Result<TxnLifecycleReceipt, String> {
    Err("transaction lifecycle requires the redb MutationBatch coordinator".to_string())
}

#[cfg(feature = "redb")]
pub(in crate::server::handlers::txn) fn finish_txn_lifecycle_receipt(
    receipt: TxnLifecycleReceipt,
    result: ResultPayload,
    method: Option<&Method>,
) -> Result<ResultPayload, String> {
    if let Some(method) = method {
        validate_txn_lifecycle_result(method, &result)?;
    }
    finish_saga_via_redb(
        &receipt.backend,
        receipt.saga,
        result,
        "transaction lifecycle lost its redb coordinator",
    )
}

#[cfg(not(feature = "redb"))]
pub(in crate::server::handlers::txn) fn finish_txn_lifecycle_receipt(
    _receipt: TxnLifecycleReceipt,
    _result: ResultPayload,
    _method: Option<&Method>,
) -> Result<ResultPayload, String> {
    Err("transaction lifecycle requires the redb MutationBatch coordinator".to_string())
}

/// Abort after a volatile lifecycle effect and before its durable terminal
/// receipt is written.  This is an explicit fault-window hook for restart
/// testing; it is inert unless a request id is armed in the environment.
/// Keeping the hook at this boundary exercises the real signed dispatch path
/// without creating another replay or idempotency authority.
pub(crate) fn fault_after_txn_lifecycle_effect(req_id: u64) {
    let Ok(armed) = std::env::var("EPISTEMIC_GRAPH_LIFECYCLE_EFFECT_FAULT_REQUEST_ID") else {
        return;
    };
    if armed.parse::<u64>().ok() == Some(req_id) {
        eprintln!("EPISTEMIC_GRAPH_LIFECYCLE_EFFECT_FAULT_REQUEST_ID armed for request {req_id}");
        std::process::abort();
    }
}

/// Admission result for a GraphQL-native begin/stage/read/rollback operation.
///
/// These operations mutate the process registry, but their replay identity and
/// terminal result belong to the same named admin saga used by the native
/// `BeginTxn`/`Txn*`/`Rollback` lifecycle.  Keeping the receipt behind this
/// facade lets the GraphQL handler execute its existing registry primitive after
/// admission without creating a second replay ledger or coordinator.
#[cfg(feature = "graphql")]
pub(crate) enum GraphQlLifecycleAdmission {
    Replayed(ResultPayload),
    /// Boxed because the receipt is ~450 bytes against the replayed payload's
    /// much smaller one, so unboxed both arms paid the receipt's size. Safe
    /// here for the same reason as elsewhere in this module: this enum is the
    /// in-process return of one admission call, derives no `Serialize`, and
    /// never crosses the wire or a durable boundary -- the receipt it carries
    /// has its own durable representation, which boxing does not touch.
    Execute(Box<TxnLifecycleReceipt>),
}

/// Consume the verified carrier's nonce and stable key for one native GraphQL
/// staging operation.  A fresh nonce with the same operation identity returns
/// `Replayed`; a reused nonce or changed method body is rejected by the kernel.
#[cfg(feature = "graphql")]
pub(crate) async fn begin_graphql_lifecycle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: &str,
    authority: &CarrierAuthority,
    method: &Method,
) -> Result<GraphQlLifecycleAdmission, String> {
    let receipt = begin_txn_lifecycle_receipt(state, req_id, caller, authority, method).await?;
    #[cfg(feature = "redb")]
    if let Some(result) = receipt.saga.replayed.clone() {
        return Ok(GraphQlLifecycleAdmission::Replayed(result));
    }
    Ok(GraphQlLifecycleAdmission::Execute(Box::new(receipt)))
}

/// Terminalize a native GraphQL staging operation through the same durable
/// lifecycle receipt used by the ordinary transaction-family methods.
#[cfg(feature = "graphql")]
pub(crate) fn finish_graphql_lifecycle(
    receipt: TxnLifecycleReceipt,
    result: ResultPayload,
) -> Result<ResultPayload, String> {
    finish_txn_lifecycle_receipt(receipt, result, None)
}
