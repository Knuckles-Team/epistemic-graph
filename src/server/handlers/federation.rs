//! Query-federation handlers (CONCEPT:EG-KG.query.query-federation, Lane P).
//!
//! `RegisterForeignSource` records a named EXTERNAL source (a remote epistemic-graph
//! engine or an HTTP/JSON API — [`eg_types::wire::ForeignSourceSpec`]) in the
//! owner-scoped [`crate::server::foreign_catalog::ForeignSourceCatalog`] on
//! `ServerState`, under the caller's VERIFIED owner — tenant+principal (EH-373). The actual
//! cross-engine / HTTP fetch is driven by the unified-query handlers: an inline-spec
//! `Op::ForeignScan` resolves itself, and a `Named` `Op::ForeignScan` / an `Op::Foreign`
//! (the UQL `FOREIGN "<name>"` marker) resolves through the registry
//! `ForeignSourceCatalog::registry_for` builds from the caller's own entries only
//! (CONCEPT:EG-KG.query.closure-backed-source). One principal can therefore neither use
//! nor overwrite another principal's registration, even under the same name (one engine
//! is bound to one tenant; the tenant stays inside the owner key). Rows a foreign
//! source returns are not RLS-filtered. A lightweight, non-blocking insert — no
//! off-reactor work needed.

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::OwnerScopedCall;

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
            if let Err(error) = eg_plan::federation::validate_column_mapping(&source) {
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
                ResultPayload::scalar::<eg_types::result_contract::cluster::RegisterForeignSource>(
                    name,
                ),
            ))
        }
        Method::QueryForeignColumns {
            name,
            columns,
            predicates,
        } => Ok(query_columns(call, name, columns, predicates).await),
        other => Err(other),
    }
}

/// Build the caller's registry while the catalog is locked, then release the
/// lock before the blocking SQL fetch. Only the verified carrier chooses scope.
async fn query_columns(
    call: OwnerScopedCall<'_>,
    name: String,
    columns: Vec<String>,
    predicates: Vec<eg_types::wire::ForeignColumnPredicate>,
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
        use eg_plan::federation_opt::{ColumnPredicate, Comparison, FederationSession};
        use eg_types::wire::ForeignColumnComparison as WireComparison;

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
                    .map(|row| eg_types::wire::ForeignColumnRow {
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
            ResultPayload::of::<eg_types::result_contract::query::QueryForeignColumns>(rows),
        ),
        Ok(Err(error)) => Response::err(req_id, column_query_error(&error)),
        Err(_) => Response::err(req_id, "INTERNAL: foreign column worker failed"),
    }
}

/// Map connector and planner prose to the codes declared for this read method.
/// Do not promote a source-supplied prefix into an arbitrary wire code.
fn column_query_error(error: &str) -> String {
    if error.starts_with(eg_plan::federation_opt::BUDGET_EXCEEDED) {
        if error.contains(":wall_ms:") {
            return "ENGINE_DEADLINE_EXCEEDED: foreign column read exceeded time budget".into();
        }
        return "ENGINE_RESOURCE_EXHAUSTED: foreign column read exceeded resource budget".into();
    }
    if error.starts_with(eg_plan::federation_opt::RESULT_INCOMPLETE) {
        return "ENGINE_RESOURCE_EXHAUSTED: foreign column result incomplete".into();
    }
    if error.contains("HTTP column projection exceeds limit") {
        return "ENGINE_RESOURCE_EXHAUSTED: foreign column projection exceeded size limit".into();
    }
    if error.contains("not exposed")
        || error.contains("column mapping")
        || error.contains("column query requires a registered source")
        || error.contains("SQL query is not read-only")
        || error.contains("SQL column predicates require string values")
        || error.contains("unsupported SQL connection scheme")
        || error.starts_with(eg_plan::federation_opt::REQUIRES_KEYS)
    {
        return "INVALID_ARGUMENT: foreign column request is invalid".into();
    }
    "ENGINE_UNAVAILABLE: foreign column source unavailable".into()
}

#[cfg(test)]
mod tests {
    use super::column_query_error;

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
