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
use eg_types::wire::{ForeignColumnComparison, ForeignColumnPredicate};

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
            #[cfg(feature = "federation-sql")]
            let probe_spec = source.clone();
            s.foreign_sources.register(&owner, name.clone(), source);
            // Advisory and bounded: registration succeeds even if the catalog is
            // unavailable, and no probe can hold the ServerState write lock.
            #[cfg(feature = "federation-sql")]
            {
                drop(s);
                static SLOTS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
                    std::sync::OnceLock::new();
                let slots =
                    SLOTS.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(4)));
                if let Ok(permit) = slots.clone().try_acquire_owned() {
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Some(rows) =
                            eg_plan::federation_opt::probe_postgres_rows(&probe_spec).await
                        {
                            let fingerprint =
                                eg_plan::federation_opt::source_fingerprint(&probe_spec);
                            eg_plan::federation_opt::observe_catalog_estimate(fingerprint, rows);
                        }
                    });
                }
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

/// Resolve the caller's authorized registry while holding the state read lock,
/// then perform the potentially blocking foreign fetch off the Tokio reactor.
async fn query_columns(
    call: OwnerScopedCall<'_>,
    name: String,
    columns: Vec<String>,
    predicates: Vec<ForeignColumnPredicate>,
) -> Response {
    let req_id = call.req_id;
    let owner = match call.owner("QueryForeignColumns") {
        Ok(owner) => owner,
        Err(refusal) => return refusal,
    };
    if predicates
        .iter()
        .any(|predicate| predicate.value.is_array() || predicate.value.is_object())
    {
        return Response::err(req_id, "federation: column predicates require JSON scalars");
    }
    let registry = {
        let state = call.state.read().await;
        state.foreign_sources.registry_for(&owner, &state.isolation)
    };
    let predicates: Vec<eg_plan::federation_opt::ColumnPredicate> = predicates
        .into_iter()
        .map(|p| eg_plan::federation_opt::ColumnPredicate {
            column: p.column,
            comparison: match p.comparison {
                ForeignColumnComparison::Eq => eg_plan::federation_opt::Comparison::Eq,
                ForeignColumnComparison::Ne => eg_plan::federation_opt::Comparison::Ne,
                ForeignColumnComparison::Lt => eg_plan::federation_opt::Comparison::Lt,
                ForeignColumnComparison::Le => eg_plan::federation_opt::Comparison::Le,
                ForeignColumnComparison::Gt => eg_plan::federation_opt::Comparison::Gt,
                ForeignColumnComparison::Ge => eg_plan::federation_opt::Comparison::Ge,
            },
            value: p.value,
        })
        .collect();
    let fetched = tokio::task::spawn_blocking(move || {
        let session = eg_plan::federation_opt::FederationSession::from_env();
        registry
            .registry()
            .query_columns(&name, &columns, &predicates, &session)
    })
    .await;
    match fetched {
        Ok(Ok(rows)) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::cluster::QueryForeignColumns>(
                eg_types::result_contract::cluster::ForeignColumnRows {
                    rows: rows
                        .rows()
                        .iter()
                        .map(|row| eg_types::result_contract::cluster::ForeignColumnRow {
                            id: row.id.clone(),
                            score: row.score,
                            columns: row.columns.clone(),
                        })
                        .collect(),
                },
            ),
        ),
        Ok(Err(error)) => Response::err(req_id, error),
        Err(join) => Response::err(req_id, format!("QueryForeignColumns task error: {join}")),
    }
}
