use super::*;

/// Verify that Cypher's explicit wire mode agrees with the native parser.
///
/// The mode is an authorization and durability claim, not a parser hint. Callers
/// must reject a mismatch before choosing a read lane or MutationBatch path.
#[cfg(feature = "cypher")]
pub(crate) fn validate_cypher_mode(method: &Method) -> Result<(), String> {
    let Method::CypherQuery { query, mode } = method else {
        return Ok(());
    };
    let parsed_mode = match eg_query::classify_cypher(query) {
        Ok(eg_query::CypherStatementKind::Read) => crate::protocol::CypherMode::Read,
        Ok(eg_query::CypherStatementKind::Write) => crate::protocol::CypherMode::Write,
        Err(message) => return Err(format!("Cypher error: {message}")),
    };
    if &parsed_mode != mode {
        return Err("Cypher error: declared mode does not match the parsed statement".to_string());
    }
    Ok(())
}

/// Process-wide GraphQL cross-modal transaction registry (CONCEPT:EG-KG.query.facade-reconcile-hook). Holds staged
/// multi-request cross-modal txns (`beginTransaction` … `stage*` … `commitTransaction`)
/// across GraphQL requests — GraphQL over the RPC transport has no per-connection session,
/// and txn ids are process-unique, so ONE shared registry is the carrier (the `OnceLock`
/// idiom the UQL text-embedder seam uses).
#[cfg(feature = "graphql")]
pub(crate) fn graphql_crossmodal_registry() -> &'static eg_graphql::CrossModalTxnRegistry {
    use std::sync::OnceLock;
    static REG: OnceLock<eg_graphql::CrossModalTxnRegistry> = OnceLock::new();
    REG.get_or_init(eg_graphql::CrossModalTxnRegistry::new)
}

#[cfg(feature = "graphql")]
pub(crate) fn graphql_field_txn_id(field: &Field) -> Option<String> {
    field.args.iter().find_map(|(name, value)| {
        (name == "txnId").then(|| match value {
            GqlValue::Str(txn_id) => Some(txn_id.clone()),
            _ => None,
        })?
    })
}

#[cfg(feature = "graphql")]
pub(crate) fn graphql_query_txn_ids(query: &str) -> Option<(bool, Vec<String>)> {
    let operation = eg_graphql::parse_operation(query).ok()?;
    let eg_graphql::Operation::Mutation(mutation) = operation else {
        return None;
    };
    let has_begin = mutation
        .roots
        .iter()
        .any(|field| field.name == "beginTransaction");
    let ids = mutation
        .roots
        .iter()
        .filter_map(graphql_field_txn_id)
        .collect();
    Some((has_begin, ids))
}

#[cfg(feature = "graphql")]
pub(crate) fn graphql_result_txn_ids(value: &serde_json::Value) -> Vec<String> {
    value
        .get("data")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|fields| fields.values())
        .filter_map(|field| field.get("txnId"))
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

/// Store `payload` in the version-keyed result cache under `hash`/`version`,
/// then wrap it as a successful [`Response`]. Cypher's cached read reaches
/// this exact store-then-respond step right after its own execution+encode
/// path (CONCEPT:EG-KG.coordination.distributed-cache-coherence); one owner keeps the cache write
/// and the response construction from drifting apart.
#[cfg(feature = "result-cache")]
pub(crate) fn cache_and_respond(
    core: &Arc<GraphCore>,
    req_id: u64,
    hash: u128,
    version: u64,
    payload: ResultPayload,
) -> Response {
    eg_core::result_cache::cache_result(core.result_cache(), hash, version, &payload);
    Response::ok(req_id, payload)
}

#[cfg(feature = "graphql")]
pub(crate) fn graphql_lifecycle_replay_is_live(
    owner_scope: &str,
    query: &str,
    result: &ResultPayload,
    registry: &eg_graphql::CrossModalTxnRegistry,
) -> bool {
    let Some((has_begin, mut txn_ids)) = graphql_query_txn_ids(query) else {
        return false;
    };
    if has_begin {
        let ResultPayload::Raw(bytes) = result else {
            return false;
        };
        let value = match rmp_serde::from_slice::<serde_json::Value>(bytes) {
            Ok(value) => value,
            Err(_) => return false,
        };
        txn_ids.extend(graphql_result_txn_ids(&value));
    }
    !txn_ids.is_empty()
        && txn_ids
            .iter()
            .all(|txn_id| registry.contains_handle(owner_scope, txn_id))
}

#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) fn plan_needs_tsdb(ops: &[eg_plan::Op]) -> bool {
    ops.iter().any(|op| match op {
        eg_plan::Op::TsScan { .. } => true,
        #[cfg(feature = "text")]
        eg_plan::Op::FuseRrf { branches, .. } => {
            branches.iter().any(|branch| plan_needs_tsdb(branch))
        }
        _ => false,
    })
}

/// Resolve an actor-owned storage namespace before a served plan can touch the
/// committed TSDB. Graph ACL/RLS actor identity alone is not a tenant carrier.
/// `pub(crate)`: also the single source of truth `handlers::mining`'s plan-sourced
/// `TsScan` leg reuses (CONCEPT:EG-KG.mining.tsdb-typed-absent) rather than
/// re-deriving the same tenant/namespace scope a second time.
#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) fn served_tsdb_scope(
    plan: &eg_plan::Plan,
    graph: &str,
    read_authority: Option<&GraphReadAuthority>,
) -> Result<Option<(String, String)>, String> {
    if !plan_needs_tsdb(&plan.ops) {
        return Ok(None);
    }
    let carrier = read_authority
        .and_then(GraphReadAuthority::carrier)
        .ok_or_else(|| {
            crate::metrics::access_denied();
            "ACCESS_DENIED: TsScan requires a verified tenant+actor carrier".to_string()
        })?;
    Ok(Some((
        carrier.tenant_scope().to_string(),
        carrier.namespace("timeseries-graph", graph),
    )))
}

/// Bundled context every extracted per-method arm of `try_handle` below needs, so
/// each stays a plain function of its own destructured request fields plus this
/// one shared bundle (CONCEPT:EG-KG.query.dispatch-convention). Pure grouping,
/// mirroring `TryHandleContext` plus the pieces the arm bodies additionally close
/// over (`state`, `core`, and -- security builds only -- `rls`): no field renamed
/// or dropped from what `try_handle` already threaded through by hand before this
/// extraction, no behaviour change.
pub(crate) struct QueryHandlerCtx<'a> {
    pub(crate) state: &'a Arc<RwLock<ServerState>>,
    pub(crate) req_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) read_authority: Option<&'a GraphReadAuthority>,
    pub(crate) caller: &'a str,
    pub(crate) core: &'a Arc<GraphCore>,
    #[cfg(feature = "security")]
    pub(crate) rls: &'a Arc<crate::isolation::IsolationLayer>,
}
