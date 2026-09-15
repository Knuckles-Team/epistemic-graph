use super::*;
#[cfg(feature = "graphql")]
use crate::server::handlers::txn;

/// The GraphQL WRITE surface — a `mutation { … }` document, one of three shapes
/// (`commitTransaction`, another cross-modal staging verb, or an ordinary
/// mutation) resolved by `classify_crossmodal`. Pure extract-method out of
/// `handle_graphql`'s `graphql_is_mutation` branch, no behaviour change: every
/// original `return Ok(resp)`/`return Ok(Response::err(...))` in that branch is
/// now this function's own return (same `Result<Response, Method>` shape).
#[cfg(feature = "graphql")]
pub(crate) async fn handle_graphql_mutation(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    read_authority: Option<&GraphReadAuthority>,
    core: &Arc<GraphCore>,
    query: String,
    variables: Option<serde_json::Value>,
) -> Result<Response, Method> {
    let carrier = match read_authority.and_then(GraphReadAuthority::carrier) {
        Some(carrier) => carrier,
        None => {
            crate::metrics::access_denied();
            return Ok(Response::err(
                req_id,
                "ACCESS_DENIED: GraphQL mutation requires verified tenant+actor authority",
            ));
        }
    };
    // Cross-modal transaction routing (CONCEPT:EG-KG.query.eg-9/419). A GraphQL mutation
    // is one of three shapes: a `commitTransaction` — landed DURABLY via
    // `commit_cross_modal_txn` (ONE redb WriteTransaction across graph + vector
    // + tsdb + axioms), exactly as pgwire's commit path; a begin/stage/read/
    // rollback cross-modal verb — run in-memory over the process-wide
    // `CrossModalTxnRegistry` (staging + read-your-own-writes, no durable side
    // effect until commit); or an ordinary mutation — the native `execute_mutation`
    // write path. `classify_crossmodal` picks the route with ONE parse.
    match eg_graphql::classify_crossmodal(&query) {
        eg_graphql::CrossModalRoute::Commit(txn_id) => {
            handle_graphql_commit_txn(state, req_id, graph_name, core, &txn_id, carrier).await
        }
        eg_graphql::CrossModalRoute::Staging => {
            handle_graphql_staging_mutation(
                state,
                req_id,
                read_authority,
                core,
                carrier,
                query,
                variables,
            )
            .await
        }
        eg_graphql::CrossModalRoute::Invalid(message) => Ok(Response::err(req_id, message)),
        eg_graphql::CrossModalRoute::NotCrossModal => {
            handle_graphql_plain_mutation(req_id, core, query).await
        }
    }
}

/// The `CrossModalRoute::Commit` arm of [`handle_graphql_mutation`]: land the
/// staged cross-modal txn DURABLY via `commit_cross_modal_txn` (ONE redb
/// WriteTransaction across graph + vector + tsdb + axioms), exactly as pgwire's
/// commit path.
#[cfg(feature = "graphql")]
pub(crate) async fn handle_graphql_commit_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    txn_id: &str,
    carrier: &crate::server::access::CarrierAuthority,
) -> Result<Response, Method> {
    let committed = txn::commit_graphql_cross_modal(
        state,
        req_id,
        graph_name,
        core,
        graphql_crossmodal_registry(),
        txn_id,
        carrier,
    )
    .await;
    let resp = match committed {
        Ok(committed) => dynamic_response::<query_results::GraphQl, _>(
            req_id,
            &serde_json::json!({
                "data": {"commitTransaction": {"committed": committed}}
            }),
        ),
        Err(msg) => Response::err(req_id, format!("GraphQL commitTransaction error: {msg}")),
    };
    Ok(resp)
}

/// The `CrossModalRoute::Staging` arm of [`handle_graphql_mutation`]: a
/// begin/stage/read/rollback cross-modal verb, run in-memory over the
/// process-wide `CrossModalTxnRegistry` (staging + read-your-own-writes, no
/// durable side effect until commit).
#[cfg(feature = "graphql")]
pub(crate) async fn handle_graphql_staging_mutation(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    read_authority: Option<&GraphReadAuthority>,
    core: &Arc<GraphCore>,
    carrier: &crate::server::access::CarrierAuthority,
    query: String,
    variables: Option<serde_json::Value>,
) -> Result<Response, Method> {
    // The GraphQL registry remains the staging implementation, but every
    // begin/stage/read/rollback request first enters the existing transaction
    // lifecycle saga.  Its method body includes variables so a changed signed
    // request cannot reuse the same stable key for a different operation.
    let method = Method::GraphQl {
        query: query.clone(),
        variables,
    };
    let admission =
        match begin_graphql_staging_admission(state, req_id, carrier, &method, &query).await {
            Ok(admission) => admission,
            Err(response) => return Ok(response),
        };
    let receipt = match admission {
        GraphQlStagingAdmission::Replayed(result) => return Ok(Response::ok(req_id, result)),
        GraphQlStagingAdmission::Execute(receipt) => receipt,
    };
    let core_w = read_authority
        .expect("GraphQL mutation authority checked above")
        .project_core(core);
    let owner_scope = carrier.owner_scope().to_string();
    let reg = graphql_crossmodal_registry();
    let value = match execute_graphql_staging(req_id, core_w, reg, owner_scope, query).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let result = match graphql_staging_payload(req_id, &value) {
        Ok(result) => result,
        Err(response) => return Ok(response),
    };
    txn::fault_after_txn_lifecycle_effect(req_id);
    Ok(finish_graphql_staging(req_id, receipt, result))
}

#[cfg(feature = "graphql")]
fn finish_graphql_staging(
    req_id: u64,
    receipt: txn::TxnLifecycleReceipt,
    result: ResultPayload,
) -> Response {
    match txn::finish_graphql_lifecycle(receipt, result) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "graphql")]
enum GraphQlStagingAdmission {
    Replayed(ResultPayload),
    Execute(txn::TxnLifecycleReceipt),
}

#[cfg(feature = "graphql")]
async fn begin_graphql_staging_admission(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    carrier: &crate::server::access::CarrierAuthority,
    method: &Method,
    query: &str,
) -> Result<GraphQlStagingAdmission, Response> {
    let admission =
        txn::begin_graphql_lifecycle(state, req_id, carrier.agent_id(), carrier, method)
            .await
            .map_err(|error| Response::err(req_id, error))?;
    match admission {
        txn::GraphQlLifecycleAdmission::Replayed(result) => {
            if !graphql_lifecycle_replay_is_live(
                carrier.owner_scope(),
                query,
                &result,
                graphql_crossmodal_registry(),
            ) {
                return Err(Response::err(
                    req_id,
                    "GraphQL lifecycle receipt is terminal but volatile staging state is unavailable; refusing to return a stale success",
                ));
            }
            Ok(GraphQlStagingAdmission::Replayed(result))
        }
        txn::GraphQlLifecycleAdmission::Execute(receipt) => {
            Ok(GraphQlStagingAdmission::Execute(*receipt))
        }
    }
}

#[cfg(feature = "graphql")]
async fn execute_graphql_staging(
    req_id: u64,
    core: Arc<GraphCore>,
    registry: &'static eg_graphql::CrossModalTxnRegistry,
    owner_scope: String,
    query: String,
) -> Result<serde_json::Value, Response> {
    match compute_off_lock(req_id, move || {
        eg_graphql::execute_crossmodal(&core, registry, &owner_scope, &query)
    })
    .await
    {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(msg)) => Err(Response::err(
            req_id,
            format!("GraphQL cross-modal error: {msg}"),
        )),
        Err(response) => Err(response),
    }
}

#[cfg(feature = "graphql")]
fn graphql_staging_payload(
    req_id: u64,
    value: &serde_json::Value,
) -> Result<ResultPayload, Response> {
    ResultPayload::of_dynamic::<query_results::GraphQl, _>(value)
        .map_err(|error| Response::err(req_id, error))
}

/// The `CrossModalRoute::NotCrossModal` arm of [`handle_graphql_mutation`]: an
/// ordinary mutation, the native `execute_mutation` write path.
#[cfg(feature = "graphql")]
pub(crate) async fn handle_graphql_plain_mutation(
    req_id: u64,
    core: &Arc<GraphCore>,
    query: String,
) -> Result<Response, Method> {
    let core_w = core.clone();
    let resp = match compute_off_lock(req_id, move || {
        eg_graphql::execute_mutation(&core_w, &query)
    })
    .await
    {
        Ok(Ok(value)) => dynamic_response::<query_results::GraphQl, _>(req_id, &value),
        Ok(Err(msg)) => Response::err(req_id, format!("GraphQL mutation error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}

#[cfg(feature = "graphql")]
pub(crate) async fn handle_graphql(
    ctx: &QueryHandlerCtx<'_>,
    query: String,
    variables: Option<serde_json::Value>,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let read_authority = ctx.read_authority;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    // GraphQL WRITE surface (CONCEPT:EG-KG.query.mutation/EG-023): a `mutation { … }` document
    // maps onto eg-core's native write ops over the LIVE `GraphCore` via
    // `execute_mutation` (which bumps the OCC version / `mark_dirty` once it
    // lands). NOT cached (it is a write) and NOT RLS pre-filtered (writes are
    // graph-ACL-gated in `dispatch_graph_op` — this method classified Write).
    if crate::server::access::graphql_is_mutation(&query) {
        return handle_graphql_mutation(
            state,
            req_id,
            graph_name,
            read_authority,
            &core,
            query,
            variables,
        )
        .await;
    }
    // A `subscription { … }` is a read-only POLL of the current matches (a full
    // push transport is a documented eg-graphql deferral); a `query { … }` is the
    // ordinary read. Both run over the SAME RLS-filtered off-lock snapshot below.
    let is_subscription = matches!(
        eg_graphql::parse_operation(&query),
        Ok(eg_graphql::Operation::Subscription(_))
    );
    // GraphQL READ surface (CONCEPT:EG-KG.query.sparql-completeness): compile the GraphQL query to
    // scans + BFS over the SAME off-lock snapshot the Cypher path uses, via the
    // pure-Rust eg-graphql resolver (NO async-graphql / DataFusion). The result
    // is the GraphQL `{"data": …}` JSON, returned via `ResultPayload::Raw`.
    //
    // GraphQL runs under the SAME version-keyed, RLS-aware result cache the
    // SQL/Cypher/SPARQL paths do (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): the cache KEY
    // folds in the caller's RLS context so agent A's filtered `{data}` is NEVER
    // served to agent B for the same GraphQL query text, and the snapshot is
    // RLS-FILTERED to the caller's visible rows BEFORE the resolver runs — a
    // GraphQL read cannot leak rows across agents any more than a Cypher read.
    // Bind the request's GraphQL `$variables` (task #23): a `query { … }` runs
    // through `execute_with_variables` so `$var` args + `@skip`/`@include`
    // resolve (CONCEPT:EG-KG.query.fragments-variables-directives); absent ⇒ an empty object, byte-identical to the
    // no-vars path. (A `subscription { … }` stays a poll of the current matches.)
    let vars = variables.unwrap_or_else(|| serde_json::json!({}));
    #[cfg(feature = "result-cache")]
    let (snap, version, hash) = {
        // Fold the bound variables INTO the cache key: the same query text with
        // different `$variables` can produce different `{data}`, so the key must
        // distinguish them or a variables-bound read would serve a stale result.
        // An empty `{}` serializes to `{}` — byte-stable for the no-vars path.
        let mut key_payload = query.as_bytes().to_vec();
        key_payload.push(0);
        key_payload.extend_from_slice(&serde_json::to_vec(&vars).unwrap_or_default());
        let hash = rls_cache_hash(
            "graphql",
            &key_payload,
            #[cfg(feature = "security")]
            caller,
            #[cfg(feature = "security")]
            rls,
        );
        // perf/row-visibility-index (B-sweep): probe the whole-RESULT
        // cache FIRST via a cheap `core.version()` read — no snapshot at
        // all on a hit (mirrors `Method::CypherQuery`'s exact two-tier
        // shape; the previous code here built+filtered a snapshot before
        // ever checking for a hit). Only a genuine MISS reaches the
        // per-(actor,version) `FilteredViewCache` probe-then-build below.
        if let Some(bytes) = core.result_cache().get(hash, core.version()) {
            return Ok(Response::ok(
                req_id,
                ResultPayload::of_encoded::<query_results::GraphQl>(bytes),
            ));
        }
        #[cfg(feature = "security")]
        let (snap, version) = versioned_rls_snapshot(&core, caller, rls);
        #[cfg(not(feature = "security"))]
        let (snap, version) = core.analysis_snapshot_versioned();
        (snap, version, hash)
    };
    #[cfg(not(feature = "result-cache"))]
    let snap = rls_snapshot(
        &core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    let resp = match compute_off_lock(req_id, move || {
        if is_subscription {
            eg_graphql::subscribe(&snap, &query)
        } else {
            eg_graphql::execute_with_variables(&snap, &query, &vars)
        }
    })
    .await
    {
        Ok(Ok(value)) => match ResultPayload::of_dynamic::<query_results::GraphQl, _>(&value) {
            Ok(payload) => {
                #[cfg(feature = "result-cache")]
                eg_core::result_cache::cache_result(core.result_cache(), hash, version, &payload);
                Response::ok(req_id, payload)
            }
            Err(error) => Response::err(req_id, error),
        },
        Ok(Err(msg)) => Response::err(req_id, format!("GraphQL error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}
