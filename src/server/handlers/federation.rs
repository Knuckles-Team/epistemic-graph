//! Query-federation handlers (CONCEPT:MEDULLA-KG.query.query-federation, Lane P).
//!
//! `RegisterForeignSource` records a named EXTERNAL source (a remote medulla
//! engine or an HTTP/JSON API — [`medulla_types::wire::ForeignSourceSpec`]) in the
//! owner-scoped [`crate::server::foreign_catalog::ForeignSourceCatalog`] on
//! `ServerState`, under the caller's VERIFIED owner — tenant+principal (EH-373). The actual
//! cross-engine / HTTP fetch is driven by the unified-query handlers: an inline-spec
//! `Op::ForeignScan` resolves itself, and a `Named` `Op::ForeignScan` / an `Op::Foreign`
//! (the UQL `FOREIGN "<name>"` marker) resolves through the registry
//! `ForeignSourceCatalog::registry_for` builds from the caller's own entries only
//! (CONCEPT:MEDULLA-KG.query.closure-backed-source). One principal can therefore neither use
//! nor overwrite another principal's registration, even under the same name (one engine
//! is bound to one tenant; the tenant stays inside the owner key). Rows a foreign
//! source returns are not RLS-filtered. A lightweight, non-blocking insert — no
//! off-reactor work needed.

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::OwnerScopedCall;
use medulla_types::foreign_read::{
    ForeignReadAbortRequest, ForeignReadChunkRequest, ForeignReadCompleteRequest,
    ForeignReadOpenRequest, FOREIGN_READ_SCHEMA_VERSION,
};

/// Try to handle a federation method. `Ok(resp)` = handled; `Err(method)` = not mine.
pub(crate) async fn try_handle(
    call: OwnerScopedCall<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = call.req_id;
    match method {
        Method::RegisterForeignSource { name, source } => {
            let owner = match call.owner("RegisterForeignSource") {
                Ok(owner) => owner,
                Err(refusal) => return Ok(refusal),
            };
            if let Err(error) = medulla_plan::federation::validate_column_mapping(&source) {
                return Ok(Response::err(req_id, error));
            }
            // EH-378: provision the source's share role (assigned to nobody) under the
            // same write lock as the registration, so a registered source always has one.
            let mut s = call.state.write().await;
            if let Err(error) = crate::server::foreign_share::provision_share_role(
                &mut s.isolation,
                owner.agent_id(),
                &name,
            ) {
                return Ok(Response::err(req_id, error));
            }
            if let Err(error) = s.foreign_sources.register(&owner, name.clone(), source) {
                return Ok(Response::err(req_id, error));
            }
            Ok(Response::ok(
                req_id,
                ResultPayload::scalar::<medulla_types::result_contract::cluster::RegisterForeignSource>(
                    name,
                ),
            ))
        }
        Method::QueryForeignColumns {
            name,
            columns,
            predicates,
        } => Ok(query_columns(call, name, columns, predicates).await),
        Method::ForeignReadOpen { request } => Ok(open_foreign_read(call, request).await),
        Method::ForeignReadChunk { request } => Ok(chunk_foreign_read(call, request).await),
        Method::ForeignReadComplete { request } => Ok(complete_foreign_read(call, request).await),
        Method::ForeignReadAbort { request } => Ok(abort_foreign_read(call, request).await),
        other => Err(other),
    }
}

async fn open_foreign_read(call: OwnerScopedCall<'_>, request: ForeignReadOpenRequest) -> Response {
    let req_id = call.req_id;
    let owner = match call.owner("ForeignReadOpen") {
        Ok(owner) => owner,
        Err(refusal) => return refusal,
    };
    if !owner.can_read() {
        crate::metrics::access_denied();
        return Response::err(req_id, "ACCESS_DENIED: ForeignReadOpen requires kg:read");
    }
    if request.schema_version != FOREIGN_READ_SCHEMA_VERSION {
        return Response::err(
            req_id,
            "FOREIGN_READ_SCHEMA_MISMATCH: unsupported schema version",
        );
    }
    if let Err(error) = request.limits.validate() {
        return Response::err(req_id, error);
    }
    let store = {
        let state = call.state.read().await;
        state.agent_library.clone()
    };
    let Some(store) = store else {
        return Response::err(
            req_id,
            "SOURCE_APPROVAL_STORE_UNAVAILABLE: foreign read requires the Agent Library",
        );
    };
    let ForeignReadOpenRequest {
        source_name,
        approval,
        plan_digest,
        query_digest,
        limits,
        ..
    } = request;
    let (execution, expires_at_ms, source_identity) = {
        let state = call.state.read().await;
        match state.foreign_reads.open(
            &owner,
            source_name.clone(),
            approval.clone(),
            plan_digest,
            query_digest,
            limits,
        ) {
            Ok(opened) => opened,
            Err(error) => return Response::err(req_id, error),
        }
    };
    let binding = {
        let state = call.state.read().await;
        let page_source = match state.foreign_reads.page_source(execution.execution_id) {
            Ok(source) => source,
            Err(error) => {
                state.foreign_reads.remove(execution.execution_id);
                return Response::err(req_id, error);
            }
        };
        state.foreign_sources.bind_registered_for(
            &owner,
            &store,
            &state.isolation,
            &approval,
            &source_name,
            page_source,
        )
    };
    if let Err(error) = binding {
        let state = call.state.read().await;
        state.foreign_reads.remove(execution.execution_id);
        return Response::err(req_id, error);
    }
    let result = medulla_types::foreign_read::ForeignReadOpenResult {
        schema_version: FOREIGN_READ_SCHEMA_VERSION,
        execution,
        expires_at_ms,
        next_sequence: 0,
        source_identity,
    };
    Response::ok(
        req_id,
        ResultPayload::of::<medulla_types::result_contract::query::ForeignReadOpen>(result),
    )
}

async fn chunk_foreign_read(
    call: OwnerScopedCall<'_>,
    request: ForeignReadChunkRequest,
) -> Response {
    let req_id = call.req_id;
    let owner = match call.owner("ForeignReadChunk") {
        Ok(owner) => owner,
        Err(refusal) => return refusal,
    };
    if !owner.can_read() {
        crate::metrics::access_denied();
        return Response::err(req_id, "ACCESS_DENIED: ForeignReadChunk requires kg:read");
    }
    let result = {
        let state = call.state.read().await;
        let Some(store) = state.agent_library.as_deref() else {
            return Response::err(
                req_id,
                "SOURCE_APPROVAL_STORE_UNAVAILABLE: foreign read requires the Agent Library",
            );
        };
        state
            .foreign_reads
            .append(&owner, store, &state.foreign_sources, request)
    };
    match result {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of::<medulla_types::result_contract::query::ForeignReadChunk>(result),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

async fn complete_foreign_read(
    call: OwnerScopedCall<'_>,
    request: ForeignReadCompleteRequest,
) -> Response {
    let req_id = call.req_id;
    let owner = match call.owner("ForeignReadComplete") {
        Ok(owner) => owner,
        Err(refusal) => return refusal,
    };
    if !owner.can_read() {
        crate::metrics::access_denied();
        return Response::err(
            req_id,
            "ACCESS_DENIED: ForeignReadComplete requires kg:read",
        );
    }
    let result = {
        let state = call.state.read().await;
        let Some(store) = state.agent_library.as_deref() else {
            return Response::err(
                req_id,
                "SOURCE_APPROVAL_STORE_UNAVAILABLE: foreign read requires the Agent Library",
            );
        };
        state
            .foreign_reads
            .complete(&owner, store, &state.foreign_sources, request)
    };
    match result {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of::<medulla_types::result_contract::query::ForeignReadComplete>(result),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

async fn abort_foreign_read(
    call: OwnerScopedCall<'_>,
    request: ForeignReadAbortRequest,
) -> Response {
    let req_id = call.req_id;
    let owner = match call.owner("ForeignReadAbort") {
        Ok(owner) => owner,
        Err(refusal) => return refusal,
    };
    if !owner.can_read() {
        crate::metrics::access_denied();
        return Response::err(req_id, "ACCESS_DENIED: ForeignReadAbort requires kg:read");
    }
    let result = {
        let state = call.state.read().await;
        let Some(store) = state.agent_library.as_deref() else {
            return Response::err(
                req_id,
                "SOURCE_APPROVAL_STORE_UNAVAILABLE: foreign read requires the Agent Library",
            );
        };
        state
            .foreign_reads
            .abort(&owner, store, &state.foreign_sources, request)
    };
    match result {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of::<medulla_types::result_contract::query::ForeignReadAbort>(result),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

/// Build the caller's registry while the catalog is locked, then release the
/// lock before the blocking SQL fetch. Only the verified carrier chooses scope.
async fn query_columns(
    call: OwnerScopedCall<'_>,
    name: String,
    columns: Vec<String>,
    predicates: Vec<medulla_types::wire::ForeignColumnPredicate>,
) -> Response {
    let req_id = call.req_id;
    let owner = match call.owner("QueryForeignColumns") {
        Ok(owner) => owner,
        Err(refusal) => return refusal,
    };
    if !owner.can_read() {
        crate::metrics::access_denied();
        return Response::err(
            req_id,
            "ACCESS_DENIED: QueryForeignColumns requires kg:read",
        );
    }
    if columns.len() > 128 || predicates.len() > 128 {
        return Response::err(
            req_id,
            "INVALID_ARGUMENT: foreign column request exceeds 128 fields or predicates",
        );
    }
    let registry = {
        let state = call.state.read().await;
        state.foreign_sources.registry_for(&owner, &state.isolation)
    };
    let result = tokio::task::spawn_blocking(move || {
        use medulla_plan::federation_opt::{ColumnPredicate, Comparison, FederationSession};
        use medulla_types::wire::ForeignColumnComparison as WireComparison;

        let predicates = predicates
            .into_iter()
            .map(|predicate| ColumnPredicate {
                column: predicate.column,
                comparison: match predicate.comparison {
                    WireComparison::Eq => Comparison::Eq,
                    WireComparison::Ne => Comparison::Ne,
                    WireComparison::Lt => Comparison::Lt,
                    WireComparison::Le => Comparison::Le,
                    WireComparison::Gt => Comparison::Gt,
                    WireComparison::Ge => Comparison::Ge,
                },
                value: predicate.value,
            })
            .collect::<Vec<_>>();
        let session = FederationSession::from_env();
        registry
            .registry()
            .query_columns(&name, &columns, &predicates, &session)
            .map(|rows| {
                rows.rows()
                    .iter()
                    .map(|row| medulla_types::wire::ForeignColumnRow {
                        id: row.id.clone(),
                        score: row.score,
                        columns: row.columns.clone(),
                    })
                    .collect::<Vec<_>>()
            })
    })
    .await;
    match result {
        Ok(Ok(rows)) => Response::ok(
            req_id,
            ResultPayload::of::<medulla_types::result_contract::query::QueryForeignColumns>(rows),
        ),
        Ok(Err(error)) => Response::err(req_id, column_query_error(&error)),
        Err(_) => Response::err(req_id, "INTERNAL: foreign column worker failed"),
    }
}

/// Map connector and planner prose to the codes declared for this read method.
/// Do not promote a source-supplied prefix into an arbitrary wire code.
fn column_query_error(error: &str) -> String {
    if error.starts_with(medulla_plan::federation_opt::BUDGET_EXCEEDED) {
        if error.contains(":wall_ms:") {
            return "ENGINE_DEADLINE_EXCEEDED: foreign column read exceeded time budget".into();
        }
        return "ENGINE_RESOURCE_EXHAUSTED: foreign column read exceeded resource budget".into();
    }
    if error.starts_with(medulla_plan::federation_opt::RESULT_INCOMPLETE) {
        return "ENGINE_RESOURCE_EXHAUSTED: foreign column result incomplete".into();
    }
    if error.contains("HTTP column projection exceeds limit") {
        return "ENGINE_RESOURCE_EXHAUSTED: foreign column projection exceeded size limit".into();
    }
    if [
        "not exposed",
        "column mapping",
        "column query requires a registered source",
        "SQL query is not read-only",
        "SQL column predicates require string values",
        "unsupported SQL connection scheme",
    ]
    .iter()
    .any(|message| error.contains(message))
        || error.starts_with(medulla_plan::federation_opt::REQUIRES_KEYS)
    {
        return "INVALID_ARGUMENT: foreign column request is invalid".into();
    }
    "ENGINE_UNAVAILABLE: foreign column source unavailable".into()
}

#[cfg(test)]
mod tests {
    use super::column_query_error;

    #[test]
    fn foreign_column_argument_errors_preserve_matching_and_priority() {
        for message in [
            "not exposed",
            "column mapping",
            "column query requires a registered source",
            "SQL query is not read-only",
            "SQL column predicates require string values",
            "unsupported SQL connection scheme",
        ] {
            assert_eq!(
                column_query_error(&format!("federation: {message}: detail")),
                "INVALID_ARGUMENT: foreign column request is invalid"
            );
            assert_eq!(
                column_query_error(&format!("FEDERATION_BUDGET_EXCEEDED:rows: {message}")),
                "ENGINE_RESOURCE_EXHAUSTED: foreign column read exceeded resource budget"
            );
        }
        assert_eq!(
            column_query_error("FEDERATION_SOURCE_REQUIRES_KEYS: detail"),
            "INVALID_ARGUMENT: foreign column request is invalid"
        );
        assert_eq!(
            column_query_error("backend: FEDERATION_SOURCE_REQUIRES_KEYS: detail"),
            "ENGINE_UNAVAILABLE: foreign column source unavailable"
        );
    }

    #[test]
    fn foreign_column_refusals_use_declared_read_codes() {
        assert!(column_query_error("FEDERATION_BUDGET_EXCEEDED:rows: limit")
            .starts_with("ENGINE_RESOURCE_EXHAUSTED:"));
        assert!(
            column_query_error("FEDERATION_BUDGET_EXCEEDED:wall_ms: limit")
                .starts_with("ENGINE_DEADLINE_EXCEEDED:")
        );
        assert!(
            column_query_error("federation: this source has no column mapping")
                .starts_with("INVALID_ARGUMENT:")
        );
        assert!(
            column_query_error("federation: HTTP column projection exceeds limit")
                .starts_with("ENGINE_RESOURCE_EXHAUSTED:")
        );
        let unexpected = column_query_error("UNDECLARED_CODE: secret backend detail");
        assert!(unexpected.starts_with("ENGINE_UNAVAILABLE:"));
        assert!(!unexpected.contains("secret backend detail"));
    }
}
