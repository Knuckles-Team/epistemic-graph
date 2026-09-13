#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
use super::request_boundary::dispatch_with_context;
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
use super::*;

#[cfg(all(feature = "redb", feature = "security"))]
const MAX_PRIVATE_COORDINATOR_PLAN_BYTES: usize = 256 * 1024 * 1024;

#[cfg(all(feature = "redb", feature = "security"))]
fn seal_private_coordinator_plan<T: serde::Serialize>(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    plan: &T,
) -> Result<(String, Vec<u8>), String> {
    let cipher = backend.transaction_recovery_cipher().ok_or_else(|| {
        format!(
            "coordinator recovery requires {} to be configured",
            crate::crypto::ENCRYPTION_KEY_ENV
        )
    })?;
    seal_private_coordinator_plan_with_cipher(&cipher, plan)
}

#[cfg(all(feature = "redb", feature = "security"))]
fn seal_private_coordinator_plan_with_cipher<T: serde::Serialize>(
    cipher: &crate::crypto::ValueCipher,
    plan: &T,
) -> Result<(String, Vec<u8>), String> {
    use sha2::{Digest, Sha256};
    let plaintext = rmp_serde::to_vec_named(plan).map_err(|error| error.to_string())?;
    if plaintext.is_empty() || plaintext.len() > MAX_PRIVATE_COORDINATOR_PLAN_BYTES {
        return Err("coordinator recovery plan exceeds resource limits".to_string());
    }
    let digest = hex::encode(Sha256::digest(&plaintext));
    Ok((digest, cipher.seal(&plaintext)))
}

#[cfg(all(feature = "redb", feature = "security"))]
fn private_coordinator_plan_digest(
    batch: &crate::mutation_batch::MutationBatch,
    event_type: &str,
) -> Result<String, String> {
    let [operation] = batch.operations.as_slice() else {
        return Err("coordinator parent has an invalid operation inventory".to_string());
    };
    let Method::ApplyMutation {
        event_type: observed,
        query,
    } = &operation.method
    else {
        return Err("coordinator parent has an invalid operation".to_string());
    };
    let digest = query
        .strip_prefix("sha256:")
        .filter(|digest| digest.len() == 64)
        .ok_or_else(|| "coordinator parent has an invalid plan digest".to_string())?;
    if observed != event_type
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("coordinator parent has an invalid plan binding".to_string());
    }
    Ok(digest.to_string())
}

#[cfg(all(feature = "redb", feature = "security"))]
fn open_private_coordinator_plan<T: serde::de::DeserializeOwned>(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch: &crate::mutation_batch::MutationBatch,
    event_type: &str,
    encrypted: &[u8],
) -> Result<T, String> {
    let cipher = backend.transaction_recovery_cipher().ok_or_else(|| {
        format!(
            "coordinator recovery requires {} to be configured",
            crate::crypto::ENCRYPTION_KEY_ENV
        )
    })?;
    open_private_coordinator_plan_with_cipher(&cipher, batch, event_type, encrypted)
}

#[cfg(all(feature = "redb", feature = "security"))]
fn open_private_coordinator_plan_with_cipher<T: serde::de::DeserializeOwned>(
    cipher: &crate::crypto::ValueCipher,
    batch: &crate::mutation_batch::MutationBatch,
    event_type: &str,
    encrypted: &[u8],
) -> Result<T, String> {
    use sha2::{Digest, Sha256};
    if !crate::crypto::is_sealed(encrypted) {
        return Err("coordinator recovery plan is not authenticated ciphertext".to_string());
    }
    let plaintext = cipher
        .unseal(encrypted)
        .map_err(|_| "coordinator recovery plan authentication failed".to_string())?;
    if plaintext.is_empty() || plaintext.len() > MAX_PRIVATE_COORDINATOR_PLAN_BYTES {
        return Err("coordinator recovery plan exceeds resource limits".to_string());
    }
    let observed = hex::encode(Sha256::digest(&plaintext));
    if observed != private_coordinator_plan_digest(batch, event_type)? {
        return Err("coordinator recovery plan digest mismatch".to_string());
    }
    eg_types::msgpack::decode_bounded(
        &plaintext,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_PRIVATE_COORDINATOR_PLAN_BYTES,
            4_000_000,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "coordinator recovery plan is invalid".to_string())
}

#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
const SPARQL_RECOVERY_EVENT: &str = "sparql_http_recovery_plan_v1";
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
const SPARQL_COMPENSATION_EVENT: &str = "sparql_http_compensation_v1";

#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SparqlRecoveryPlan {
    schema_version: u8,
    graphs: Vec<crate::server::sparql_http::PlannedGraphUpdate>,
    /// The planner's operation counts, sealed with the plan so a resumed saga answers
    /// the same `ApplyMutation` report as the attempt that planned it. A plan sealed
    /// before this field existed decodes with zero counts.
    #[serde(default)]
    counts: eg_rdf::update::UpdateReport,
}

#[cfg(all(feature = "redb", feature = "security", feature = "sparql-http"))]
fn coordinator_result_is_compensated(result: &ResultPayload) -> bool {
    matches!(result, ResultPayload::Json(value)
        if value.get("outcome").and_then(serde_json::Value::as_str) == Some("compensated"))
}

#[cfg(all(
    feature = "sparql-http",
    feature = "redb",
    feature = "security",
    feature = "raft"
))]
async fn clear_coordinated_graph_decision(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    coordinator_id: &str,
) -> Result<(), String> {
    backend.xshard_decision_clear(coordinator_id).await
}

#[cfg(all(
    feature = "sparql-http",
    feature = "redb",
    feature = "security",
    not(feature = "raft")
))]
async fn clear_coordinated_graph_decision(
    _backend: &crate::server::persistence::redb_backend::RedbBackend,
    _coordinator_id: &str,
) -> Result<(), String> {
    Ok(())
}

/// Everything the SPARQL-HTTP update coordinator's phases share: the verified
/// caller, the durable redb backend, and the two saga ids (the parent that
/// carries the sealed plan and the compensation marker that makes a restart
/// choose one direction forever). Bundled so each phase helper stays inside the
/// parameter cap.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
struct SparqlUpdateCoordination<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &'a VerifiedRequestContext,
    verified_actor: &'a str,
    redb: &'a crate::server::persistence::redb_backend::RedbBackend,
    parent_id: &'a str,
    compensation_id: &'a str,
}

/// A resumed saga that already carries its committed result: clear BOTH
/// retained decisions, then replay the recorded outcome verbatim — a compensated
/// outcome still answers as the compensation error, never as success.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn replay_sparql_update(
    coord: &SparqlUpdateCoordination<'_>,
    result: ResultPayload,
) -> Response {
    if let Err(error) = clear_coordinated_graph_decision(coord.redb, coord.parent_id).await {
        return Response::err(coord.req_id, error);
    }
    if let Err(error) = clear_coordinated_graph_decision(coord.redb, coord.compensation_id).await {
        return Response::err(coord.req_id, error);
    }
    if coordinator_result_is_compensated(&result) {
        Response::err(coord.req_id, "SPARQL update was durably compensated")
    } else {
        Response::ok(coord.req_id, result)
    }
}

/// Recover the sealed plan of a saga that was already begun by an earlier
/// attempt. `Err` is this request's final response — either the replayed
/// outcome or a recovery failure.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn resume_sparql_update_plan(
    coord: &SparqlUpdateCoordination<'_>,
    saga: handlers::admin::AdminSaga,
) -> Result<(handlers::admin::AdminSaga, SparqlRecoveryPlan), Response> {
    if let Some(result) = saga.replayed.clone() {
        return Err(replay_sparql_update(coord, result).await);
    }
    let read = match coord.redb.admin_mutations_read() {
        Ok(read) => read,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    let encrypted = match eg_transaction::read_private_payload(&read, coord.parent_id) {
        Ok(Some(value)) => value,
        Ok(None) => {
            return Err(Response::err(
                coord.req_id,
                "SPARQL recovery plan is missing",
            ))
        }
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    let plan = match open_private_coordinator_plan(
        coord.redb,
        &saga.batch,
        SPARQL_RECOVERY_EVENT,
        &encrypted,
    ) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    Ok((saga, plan))
}

/// The complete graph set this update may address. An update whose graph is a
/// variable can reach every resident graph, so the registry is folded in and the
/// set deduplicated.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn resolve_sparql_update_graphs(
    coord: &SparqlUpdateCoordination<'_>,
    query: &str,
    default_graph: &str,
) -> Result<Vec<String>, Response> {
    let mut graphs = match crate::server::sparql_http::update_graphs(query, default_graph) {
        Ok(graphs) => graphs,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    if crate::server::sparql_http::update_uses_variable_graph(query) {
        graphs.extend(
            coord
                .state
                .read()
                .await
                .registry
                .list()
                .into_iter()
                .map(|(name, _)| name),
        );
        graphs.sort();
        graphs.dedup();
    }
    Ok(graphs)
}

/// Write access is checked against every graph that ALREADY exists, under one
/// read lock; the names that do not yet exist are returned so the caller can
/// gate creation separately.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn check_sparql_update_graph_access(
    coord: &SparqlUpdateCoordination<'_>,
    graphs: &[String],
) -> Result<Vec<String>, Response> {
    let current = timed_read(coord.state).await;
    for graph in graphs {
        let Some(entry) = current.registry.get(graph) else {
            continue;
        };
        if let Err(error) = check_graph_access(
            &current.isolation,
            Some(coord.verified_actor),
            graph,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Write,
        ) {
            return Err(Response::err(coord.req_id, error));
        }
    }
    Ok(graphs
        .iter()
        .filter(|graph| !current.registry.exists(graph))
        .cloned()
        .collect::<Vec<_>>())
}

/// Seal a plan as authenticated ciphertext and open the named saga that binds
/// only its digest. Shared by the parent plan and the compensation marker.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
fn begin_sealed_sparql_saga(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlan,
    batch_id: &str,
    event_type: &str,
) -> Result<handlers::admin::AdminSaga, Response> {
    let (digest, encrypted) = match seal_private_coordinator_plan(coord.redb, plan) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    handlers::admin::begin_named_admin_saga_with_private_payload_and_nonce(
        coord.redb,
        coord.req_id,
        Some(coord.verified_actor),
        coord.verified_context.attempt_nonce(),
        handlers::admin::AdminSagaPayload {
            domain: crate::mutation_batch::DurabilityDomain::MultiGraph,
            batch_id,
            event_type,
            payload_digest: &digest,
            encrypted_payload: &encrypted,
        },
    )
    .map_err(|error| Response::err(coord.req_id, error))
}

/// First attempt: resolve the graph set, gate access, plan the before/after
/// images, and seal them into a fresh parent saga.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn build_sparql_update_plan(
    coord: &SparqlUpdateCoordination<'_>,
    query: &str,
    default_graph: &str,
) -> Result<(handlers::admin::AdminSaga, SparqlRecoveryPlan), Response> {
    let graphs = resolve_sparql_update_graphs(coord, query, default_graph).await?;
    let missing = check_sparql_update_graph_access(coord, &graphs).await?;
    if !missing.is_empty() && !coord.verified_context.allows_method("graph:admin", true) {
        return Err(Response::err(
            coord.req_id,
            "ACCESS_DENIED: SPARQL graph creation requires graph:admin",
        ));
    }
    let (planned, counts) =
        match crate::server::sparql_http::plan_update(coord.state, query, default_graph, &graphs)
            .await
        {
            Ok(planned) => planned,
            Err(error) => return Err(Response::err(coord.req_id, error)),
        };
    let plan = SparqlRecoveryPlan {
        schema_version: 1,
        graphs: planned,
        counts,
    };
    let saga = begin_sealed_sparql_saga(coord, &plan, coord.parent_id, SPARQL_RECOVERY_EVENT)?;
    Ok((saga, plan))
}

/// Resolve this request's parent saga and its sealed plan: resume the one an
/// earlier attempt began, or build a new one.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn stage_sparql_update(
    coord: &SparqlUpdateCoordination<'_>,
    query: &str,
    default_graph: &str,
) -> Result<(handlers::admin::AdminSaga, SparqlRecoveryPlan), Response> {
    let resumed = match handlers::admin::resume_named_admin_saga(
        coord.redb,
        coord.parent_id,
        Some(coord.verified_actor),
    ) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    let (saga, plan) = match resumed {
        Some(saga) => resume_sparql_update_plan(coord, saga).await?,
        None => build_sparql_update_plan(coord, query, default_graph).await?,
    };
    if plan.schema_version != 1 {
        return Err(Response::err(
            coord.req_id,
            "unsupported SPARQL recovery plan",
        ));
    }
    Ok((saga, plan))
}

/// Create every graph the plan introduces that is not already resident, through
/// the ordinary dispatch path so lifecycle stays durable and authorized.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn create_missing_sparql_graphs(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlan,
) -> Result<(), Response> {
    for update in plan.graphs.iter().filter(|update| !update.existed_before) {
        if timed_read(coord.state).await.registry.exists(&update.graph) {
            continue;
        }
        let request = Request {
            id: coord.req_id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(coord.verified_actor.to_string()),
            method: Method::CreateGraph {
                graph_name: update.graph.clone(),
                graph_type: update.graph_type,
            },
        };
        let response = Box::pin(dispatch_with_context(
            coord.state,
            request,
            Some(VerifiedRequestContext::clone(coord.verified_context)),
        ))
        .await;
        if let Some(error) = response.error {
            return Err(Response::err(coord.req_id, error));
        }
    }
    Ok(())
}

#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
fn sparql_forward_methods(
    plan: &SparqlRecoveryPlan,
) -> Vec<(String, crate::protocol::GraphType, Vec<Method>)> {
    plan.graphs
        .iter()
        .map(|update| {
            (
                update.graph.clone(),
                update.graph_type,
                vec![Method::FromMsgpack {
                    msgpack: update.after_msgpack.clone(),
                }],
            )
        })
        .collect()
}

#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
fn sparql_rollback_methods(
    plan: &SparqlRecoveryPlan,
) -> Vec<(String, crate::protocol::GraphType, Vec<Method>)> {
    plan.graphs
        .iter()
        .filter(|update| update.existed_before)
        .map(|update| {
            (
                update.graph.clone(),
                update.graph_type,
                vec![Method::FromMsgpack {
                    msgpack: update.before_msgpack.clone(),
                }],
            )
        })
        .collect::<Vec<_>>()
}

/// Close the parent saga on the roll-forward path and clear its retained
/// decision.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn finish_sparql_commit(
    coord: &SparqlUpdateCoordination<'_>,
    saga: handlers::admin::AdminSaga,
    plan: &SparqlRecoveryPlan,
) -> Response {
    let committed = match finish_sparql_parent_saga(coord.redb, saga, plan) {
        Ok(committed) => committed,
        Err(error) => return Response::err(coord.req_id, error),
    };
    match clear_coordinated_graph_decision(coord.redb, coord.parent_id).await {
        Ok(_) => Response::ok(coord.req_id, committed),
        Err(error) => Response::err(coord.req_id, error),
    }
}

/// Close the parent saga over the committed update's declared `ApplyMutation` report.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
fn finish_sparql_parent_saga(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    saga: handlers::admin::AdminSaga,
    plan: &SparqlRecoveryPlan,
) -> Result<ResultPayload, String> {
    let report = eg_types::result_contract::transactions::SparqlUpdateReport {
        operations: plan.counts.operations as u64,
        inserted: plan.counts.inserted as u64,
        deleted: plan.counts.deleted as u64,
        updated_graphs: plan.graphs.len() as u64,
        created_graphs: plan
            .graphs
            .iter()
            .filter(|graph| !graph.existed_before)
            .count() as u64,
    };
    let result = ResultPayload::of::<eg_types::result_contract::graph::ApplyMutation>(report)?;
    handlers::admin::finish_admin_saga(redb, saga.batch, saga.created_at_ms, result)
}

/// What the roll-forward attempt decided.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
enum SparqlForwardOutcome {
    /// This request is finished; the response is final.
    Settled(Box<Response>),
    /// The forward commit did not take. Compensate, carrying the parent saga
    /// and the durable compensation marker that pins the direction.
    Compensate(Box<(handlers::admin::AdminSaga, handlers::admin::AdminSaga)>),
}

/// Roll forward: create the plan's new graphs, commit every after-image, and on
/// success close the parent saga. Anything else opens the compensation marker.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn try_sparql_forward_commit(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlan,
    saga: handlers::admin::AdminSaga,
) -> SparqlForwardOutcome {
    if let Err(response) = create_missing_sparql_graphs(coord, plan).await {
        return SparqlForwardOutcome::Settled(Box::new(response));
    }
    let forward = handlers::txn::commit_coordinated_graph_methods_with_nonce(
        coord.state,
        coord.req_id,
        Some(coord.verified_actor),
        coord.parent_id,
        sparql_forward_methods(plan),
        coord.verified_context.attempt_nonce(),
    )
    .await;
    if forward.error.is_none() && !matches!(forward.result, Some(ResultPayload::Bool(false))) {
        return SparqlForwardOutcome::Settled(Box::new(
            finish_sparql_commit(coord, saga, plan).await,
        ));
    }
    let marker_plan = SparqlRecoveryPlan {
        schema_version: 1,
        graphs: Vec::new(),
        counts: eg_rdf::update::UpdateReport::default(),
    };
    match begin_sealed_sparql_saga(
        coord,
        &marker_plan,
        coord.compensation_id,
        SPARQL_COMPENSATION_EVENT,
    ) {
        Ok(marker) => SparqlForwardOutcome::Compensate(Box::new((saga, marker))),
        Err(response) => SparqlForwardOutcome::Settled(Box::new(response)),
    }
}

/// Restore every before-image of a graph that already existed.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn apply_sparql_rollback(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlan,
) -> Result<(), Response> {
    let rollback_methods = sparql_rollback_methods(plan);
    if rollback_methods.is_empty() {
        return Ok(());
    }
    let rollback = handlers::txn::commit_coordinated_graph_methods_with_nonce(
        coord.state,
        coord.req_id,
        Some(coord.verified_actor),
        coord.compensation_id,
        rollback_methods,
        coord.verified_context.attempt_nonce(),
    )
    .await;
    if let Some(error) = rollback.error {
        return Err(Response::err(
            coord.req_id,
            format!("SPARQL compensation pending: {error}"),
        ));
    }
    if matches!(rollback.result, Some(ResultPayload::Bool(false))) {
        return Err(Response::err(
            coord.req_id,
            "SPARQL compensation decision aborted",
        ));
    }
    Ok(())
}

/// Drop every graph the plan created, in REVERSE plan order.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn delete_created_sparql_graphs(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlan,
) -> Result<(), Response> {
    for update in plan
        .graphs
        .iter()
        .rev()
        .filter(|update| !update.existed_before)
    {
        if !timed_read(coord.state).await.registry.exists(&update.graph) {
            continue;
        }
        let request = Request {
            id: coord.req_id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(coord.verified_actor.to_string()),
            method: Method::DeleteGraph {
                graph_name: update.graph.clone(),
            },
        };
        let response = Box::pin(dispatch_with_context(
            coord.state,
            request,
            Some(VerifiedRequestContext::clone(coord.verified_context)),
        ))
        .await;
        if let Some(error) = response.error {
            return Err(Response::err(
                coord.req_id,
                format!("SPARQL compensation pending: {error}"),
            ));
        }
    }
    Ok(())
}

/// Close the compensation marker, then the parent saga, then clear both
/// retained decisions. The request answers as a durable compensation.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn finish_sparql_compensation(
    coord: &SparqlUpdateCoordination<'_>,
    saga: handlers::admin::AdminSaga,
    compensation_saga: Option<handlers::admin::AdminSaga>,
) -> Response {
    if let Some(marker) = compensation_saga {
        if let Err(error) = handlers::admin::finish_admin_saga(
            coord.redb,
            marker.batch,
            marker.created_at_ms,
            ResultPayload::Bool(true),
        ) {
            return Response::err(coord.req_id, error);
        }
    }
    let result = ResultPayload::Json(serde_json::json!({
        "outcome": "compensated",
        "updated_graphs": 0,
        "created_graphs": 0,
    }));
    if let Err(error) =
        handlers::admin::finish_admin_saga(coord.redb, saga.batch, saga.created_at_ms, result)
    {
        return Response::err(coord.req_id, error);
    }
    if let Err(error) = clear_coordinated_graph_decision(coord.redb, coord.parent_id).await {
        return Response::err(coord.req_id, error);
    }
    if let Err(error) = clear_coordinated_graph_decision(coord.redb, coord.compensation_id).await {
        return Response::err(coord.req_id, error);
    }
    Response::err(coord.req_id, "SPARQL update was durably compensated")
}

/// Drive the staged saga: roll forward once, and otherwise compensate. A
/// compensation marker that ALREADY exists pins the direction — the forward
/// attempt is skipped entirely, so a restart can never alternate.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn run_coordinated_sparql_http_update(
    coord: &SparqlUpdateCoordination<'_>,
    query: &str,
    default_graph: &str,
) -> Response {
    let (saga, plan) = match stage_sparql_update(coord, query, default_graph).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let existing_marker = match handlers::admin::resume_named_admin_saga(
        coord.redb,
        coord.compensation_id,
        Some(coord.verified_actor),
    ) {
        Ok(value) => value,
        Err(error) => return Response::err(coord.req_id, error),
    };
    let (saga, compensation_saga) = match existing_marker {
        Some(marker) => (saga, Some(marker)),
        None => match try_sparql_forward_commit(coord, &plan, saga).await {
            SparqlForwardOutcome::Settled(response) => return *response,
            SparqlForwardOutcome::Compensate(pair) => {
                let (saga, marker) = *pair;
                (saga, Some(marker))
            }
        },
    };
    if let Err(response) = apply_sparql_rollback(coord, &plan).await {
        return response;
    }
    if let Err(response) = delete_created_sparql_graphs(coord, &plan).await {
        return response;
    }
    finish_sparql_compensation(coord, saga, compensation_saga).await
}

/// Coordinate one signed SPARQL HTTP update over detached graph images. The
/// complete before/after plan is authenticated ciphertext bound to a digest-only
/// parent before lifecycle or graph state changes. Clustered graph spans use the
/// retained-decision cross-shard 2PC authority; local spans use its deterministic
/// child MutationBatches. A durable compensation marker makes restart choose one
/// direction forever, so a crash cannot alternate roll-forward and rollback.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
pub(super) async fn coordinated_sparql_http_update(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    default_graph: &str,
    query: String,
) -> Response {
    let verified_actor = verified_context.agent_id().trim();
    if verified_actor.is_empty() {
        return Response::err(req_id, "ACCESS_DENIED: SPARQL update has no verified actor");
    }
    let parent_method = Method::ApplyMutation {
        event_type: crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT.to_string(),
        query: query.clone(),
    };
    let backend = timed_read(state).await.persistence.clone();
    let Some(backend) = backend else {
        return Response::err(req_id, "SPARQL HTTP update requires durable persistence");
    };
    let Some(redb) = backend.as_redb() else {
        return Response::err(req_id, "SPARQL HTTP update requires durable redb");
    };
    let parent_id = crate::server::mutation_batch::opaque_request_key(
        "sparql-http-parent",
        default_graph,
        req_id,
        &parent_method,
    );
    let compensation_id = crate::server::mutation_batch::opaque_coordinator_key(
        "sparql-http-compensation",
        default_graph,
        &parent_id,
    );
    let coord = SparqlUpdateCoordination {
        state,
        req_id,
        verified_context,
        verified_actor,
        redb,
        parent_id: &parent_id,
        compensation_id: &compensation_id,
    };
    run_coordinated_sparql_http_update(&coord, &query, default_graph).await
}

#[cfg(all(
    feature = "sparql-http",
    not(all(feature = "redb", feature = "security"))
))]
pub(super) async fn coordinated_sparql_http_update(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _verified_context: &VerifiedRequestContext,
    _default_graph: &str,
    _query: String,
) -> Response {
    Response::err(
        req_id,
        "SPARQL HTTP update requires a build with durable redb support",
    )
}

#[cfg(all(test, feature = "redb", feature = "security", feature = "sparql-http"))]
mod coordinator_restart_tests {
    use super::*;
    use crate::mutation_batch::{DurabilityDomain, MutationBatchStatus, MutationSurface};
    use eg_transaction::{read_ledger, read_private_payload};

    /// The coordinator owner file is ledger-only: receipts and sealed payloads.
    type Owner = eg_storage::LedgerOnlyOwner;

    /// The ONE physical owner file each test opens, and the principal + proof
    /// bytes this module's grant authority accepts for it.
    const TEST_PHYSICAL_STORE: &str = "epistemic-graph:sparql-coordinator-test";
    const TEST_PRINCIPAL: &str = "principal:epistemic-graph:sparql-coordinator-test";
    const TEST_PROOF: &[u8] = b"sparql-coordinator-test-scope-grant";

    /// Test-local [`eg_storage::PrivatePayloadIntegrity`], mirroring
    /// `persistence::redb_backend::TxnRecoveryPrivateIntegrity` exactly (same
    /// unseal-then-compare-SHA-256 check) but keyed off the test's own in-memory
    /// cipher. These tests seal and read real private (encrypted) coordinator
    /// recovery payloads, so — exactly like production — the store needs a real
    /// authority; `None` here would not be a stub, it would silently disable the
    /// authentication these tests exist to exercise.
    struct TestPrivateIntegrity(crate::crypto::ValueCipher);

    impl eg_storage::PrivatePayloadIntegrity for TestPrivateIntegrity {
        fn authenticate(
            &self,
            sealed: &[u8],
            expected_plaintext_digest: &str,
        ) -> Result<(), String> {
            crate::server::persistence::redb_backend::authenticate_private_payload(
                &self.0,
                sealed,
                expected_plaintext_digest,
            )
        }
    }

    /// This module's composition root for scope grants (RF-RULING-004): these
    /// tests have no engine root, so they decide their own. Not a permissive
    /// stub — it is built for ONE layout and checks that layout, the scope
    /// tenant, the principal AND the proof bytes, so any other layout, tenant,
    /// principal or proof still fails closed.
    struct TestScopeVerifier;

    impl eg_storage::ScopeGrantVerifier for TestScopeVerifier {
        fn verify(
            &self,
            _physical: &eg_storage::PhysicalStoreIdentity,
            layout: eg_storage::OwnerLayout,
            identity: &eg_types::MutationScopeIdentity,
            principal: &str,
            proof: &[u8],
        ) -> Result<(), String> {
            if layout != eg_storage::OwnerLayout::LedgerOnly
                || identity.tenant().as_str() != "native"
                || principal != TEST_PRINCIPAL
                || proof != TEST_PROOF
            {
                return Err("sparql coordinator test scope grant rejected".to_string());
            }
            Ok(())
        }
    }

    /// Open (creating when absent) the coordinator owner file at `path` exactly
    /// as a composition root does: the storage kernel, the one mutation kernel
    /// it issues once, and the single authenticated, ledger-bootstrapped scope.
    fn open_coordinator_store(
        path: &std::path::Path,
        identity: &eg_types::MutationScopeIdentity,
        cipher: &crate::crypto::ValueCipher,
    ) -> (
        eg_storage::StorageKernel,
        eg_transaction::MutationKernel,
        eg_storage::OwnedStoreHandle<Owner>,
    ) {
        let physical = eg_storage::PhysicalStoreIdentity::new(TEST_PHYSICAL_STORE).unwrap();
        let integrity: Option<Arc<dyn eg_storage::PrivatePayloadIntegrity>> =
            Some(Arc::new(TestPrivateIntegrity(cipher.clone())));
        let kernel = if path.exists() {
            eg_storage::StorageKernel::open_owner::<Owner>(path, physical, integrity)
        } else {
            eg_storage::StorageKernel::create_owner::<Owner>(path, physical, integrity)
        }
        .unwrap();
        let (kernel, authority) = kernel.into_read_and_mutation_authority().unwrap();
        let mutations = eg_transaction::MutationKernel::new(authority);
        let grant = kernel
            .authenticate_scope::<Owner>(
                &TestScopeVerifier,
                identity.clone(),
                TEST_PRINCIPAL.to_string(),
                TEST_PROOF,
            )
            .unwrap();
        let owner = kernel.bind_serving_scope(grant, 0).unwrap();
        mutations.bootstrap_ledger(&owner).unwrap();
        (kernel, mutations, owner)
    }

    fn parent_batch(id: &str, digest: &str, event_type: &str) -> eg_types::MutationBatch {
        crate::server::mutation_batch::compile_opaque_digest(
            crate::server::mutation_batch::CompileBatch {
                batch_id: id,
                request_id: 41,
                attempt_nonce: None,
                principal: Some("system"),
                tenant: "native",
                graph: "cluster-admin",
                placement_epoch: 0,
                idempotency_key: id,
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 1,
                default_surface: MutationSurface::Other,
                authoritative_state: None,
            },
            digest,
            MutationSurface::Other,
            DurabilityDomain::MultiGraph,
            event_type,
        )
        .unwrap()
    }

    #[test]
    fn encrypted_sparql_preimages_survive_process_restart_and_tamper_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordinator.redb");
        let cipher = crate::crypto::ValueCipher::from_key_material(b"restart-test-key");
        let plan = SparqlRecoveryPlan {
            schema_version: 1,
            graphs: vec![crate::server::sparql_http::PlannedGraphUpdate {
                graph: "graph-opaque".to_string(),
                graph_type: crate::protocol::GraphType::Global,
                existed_before: true,
                before_msgpack: vec![0x91, 0x01],
                after_msgpack: vec![0x91, 0x02],
            }],
            counts: eg_rdf::update::UpdateReport::default(),
        };
        let (digest, encrypted) =
            seal_private_coordinator_plan_with_cipher(&cipher, &plan).unwrap();
        let batch = parent_batch("sparql-parent", &digest, SPARQL_RECOVERY_EVENT);
        let identity = batch.identity.clone();
        {
            let (_kernel, mutations, owner) = open_coordinator_store(&path, &identity, &cipher);
            let begun = mutations.saga_step(&owner, &batch, 1, Some(&encrypted));
            assert!(matches!(begun.unwrap(), eg_transaction::SagaBegin::Execute));
        }
        let (kernel, _mutations, owner) = open_coordinator_store(&path, &identity, &cipher);
        let read = kernel.read_scope(&owner).unwrap();
        let record = read_ledger(&read, &batch.batch_id).unwrap().unwrap();
        assert_eq!(record.status, MutationBatchStatus::Prepared);
        let recovered = read_private_payload(&read, &batch.batch_id)
            .unwrap()
            .unwrap();
        let opened: SparqlRecoveryPlan = open_private_coordinator_plan_with_cipher(
            &cipher,
            &record.batch,
            SPARQL_RECOVERY_EVENT,
            &recovered,
        )
        .unwrap();
        assert_eq!(opened.graphs[0].before_msgpack, vec![0x91, 0x01]);
        let mut tampered = recovered;
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(
            open_private_coordinator_plan_with_cipher::<SparqlRecoveryPlan>(
                &cipher,
                &record.batch,
                SPARQL_RECOVERY_EVENT,
                &tampered,
            )
            .is_err()
        );
    }

    #[test]
    fn durable_compensation_marker_fixes_restart_direction_and_erases_its_plan() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compensation.redb");
        let cipher = crate::crypto::ValueCipher::from_key_material(b"compensation-test-key");
        let parent_plan = SparqlRecoveryPlan {
            schema_version: 1,
            graphs: Vec::new(),
            counts: eg_rdf::update::UpdateReport::default(),
        };
        let (parent_digest, parent_encrypted) =
            seal_private_coordinator_plan_with_cipher(&cipher, &parent_plan).unwrap();
        let parent = parent_batch("sparql-parent", &parent_digest, SPARQL_RECOVERY_EVENT);
        let (marker_digest, marker_encrypted) =
            seal_private_coordinator_plan_with_cipher(&cipher, &parent_plan).unwrap();
        let marker = parent_batch(
            "sparql-compensation",
            &marker_digest,
            SPARQL_COMPENSATION_EVENT,
        );
        let identity = parent.identity.clone();
        {
            let (_kernel, mutations, owner) = open_coordinator_store(&path, &identity, &cipher);
            mutations
                .saga_step(&owner, &parent, 1, Some(&parent_encrypted))
                .unwrap();
            mutations
                .saga_step(&owner, &marker, 2, Some(&marker_encrypted))
                .unwrap();
            let result = rmp_serde::to_vec_named(&ResultPayload::Bool(true)).unwrap();
            mutations.saga_end(&owner, &marker, result, 3).unwrap();
        }
        let (kernel, _mutations, owner) = open_coordinator_store(&path, &identity, &cipher);
        let read = kernel.read_scope(&owner).unwrap();
        let status = |id: &str| read_ledger(&read, id).unwrap().unwrap().status;
        assert_eq!(status(&parent.batch_id), MutationBatchStatus::Prepared);
        assert_eq!(status(&marker.batch_id), MutationBatchStatus::Committed);
        let sealed = |id: &str| read_private_payload(&read, id).unwrap();
        assert!(sealed(&parent.batch_id).is_some());
        assert!(sealed(&marker.batch_id).is_none());
    }
}
