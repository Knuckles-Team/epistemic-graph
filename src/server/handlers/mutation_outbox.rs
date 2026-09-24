//! `Method::MutationOutbox`: the operator view of one owner's outbox (X10,
//! PX10b item 8).
//!
//! `status` and `dead_letters` read one snapshot of the named owner's scope;
//! `rewind` re-delivers a consumer's stream in order from a position through
//! bounded kernel transactions. The action is `admin:outbox`, which the
//! request boundary gates behind the RBAC admin capability, and the method is
//! local-only: delivery rows are the local owner's, never replicated.
//!
//! The owner resolution below is the whole routing decision and it is
//! EXHAUSTIVE over [`NativeOutboxStore`]: an outbox nobody named is an outbox
//! nobody monitors, so a new durable owner must be given a resolver here. A
//! native target names the scope its store keys outboxes by -- the tenant for
//! the Agent Library and jobs, a binding for a semantic index, a resource for
//! the tenant's shared SQL catalog.

use std::sync::Arc;

use tokio::sync::RwLock;

use eg_types::mutation_outbox::MutationOutboxOp;
#[cfg(feature = "redb")]
use eg_types::mutation_outbox::{NativeOutboxScope, NativeOutboxStore, OutboxTarget};

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

/// Read one consumer's standing, its dead letters, or rewind its cursor.
pub(crate) async fn handle_mutation_outbox(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: MutationOutboxOp,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, op);
        Response::err(req_id, "MutationOutbox requires the redb feature")
    }
    #[cfg(feature = "redb")]
    {
        match serve(state, verified, op).await {
            Ok(answer) => answer.into_response(req_id),
            Err(error) => Response::err(req_id, error),
        }
    }
}

#[cfg(feature = "redb")]
use crate::server::outbox_operator::{
    position, OutboxView, OutboxViewAnswer, OutboxWrite, OutboxWriteReply,
};

/// The label a failed blocking store task names.
#[cfg(feature = "redb")]
const BLOCKING_TASK: &str = "mutation outbox";

/// Everything one operation can answer.
#[cfg(feature = "redb")]
enum Answer {
    View(OutboxViewAnswer),
    Write(OutboxWriteReply),
}

#[cfg(feature = "redb")]
impl Answer {
    fn into_response(self, req_id: u64) -> Response {
        use crate::protocol::ResultPayload;
        use eg_types::result_contract::transactions as results;

        match self {
            Answer::View(OutboxViewAnswer::Status(view)) => Response::ok(
                req_id,
                ResultPayload::of_ref::<results::MutationOutboxStatus>(&view),
            ),
            Answer::View(OutboxViewAnswer::DeadLetters(page)) => Response::ok(
                req_id,
                ResultPayload::of_ref::<results::MutationOutboxDeadLetters>(&page),
            ),
            Answer::Write(OutboxWriteReply::Rewound(receipt)) => Response::ok(
                req_id,
                ResultPayload::of_ref::<results::MutationOutboxRewind>(&receipt),
            ),
            Answer::Write(OutboxWriteReply::Rejected) => Response::err(
                req_id,
                "CORRUPT_OUTBOX: an operator request produced a consumer reject",
            ),
        }
    }
}

#[cfg(feature = "redb")]
async fn serve(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    op: MutationOutboxOp,
) -> Result<Answer, String> {
    op.validate()?;
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let owner = resolve(state, verified, op.target()).await?;
    match op {
        MutationOutboxOp::Status { consumer, .. } => owner
            .view(OutboxView::Status { consumer })
            .await
            .map(Answer::View),
        MutationOutboxOp::DeadLetters {
            consumer,
            after,
            limit,
            ..
        } => owner
            .view(OutboxView::DeadLetters {
                consumer,
                after: after.as_ref().map(position),
                limit,
            })
            .await
            .map(Answer::View),
        MutationOutboxOp::Rewind { consumer, to, .. } => owner
            .write(OutboxWrite::Rewind {
                consumer,
                to,
                now_ms,
            })
            .await
            .map(Answer::Write),
    }
}

/// The owner one target names, resolved to the store that holds its outbox.
#[cfg(feature = "redb")]
enum Owner {
    Graph {
        persistence: Arc<dyn crate::server::persistence::PersistenceBackend>,
        graph_fname: String,
    },
    AgentLibrary {
        store: Arc<crate::server::persistence::agent_library::AgentLibraryStore>,
        tenant_id: String,
    },
    #[cfg(feature = "jobs")]
    Jobs {
        store: Arc<eg_jobs::store::JobStore>,
    },
    #[cfg(feature = "ann-redb")]
    SemanticBinding {
        service: Arc<eg_core::compute::semantic_index_service::SemanticIndexService>,
    },
    #[cfg(feature = "query")]
    SqlResource {
        store: eg_query::TableStore,
        tenant_scope: String,
        resource: String,
    },
}

#[cfg(feature = "redb")]
async fn resolve(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    target: &OutboxTarget,
) -> Result<Owner, String> {
    match target {
        OutboxTarget::Graph { graph } => {
            let persistence = state.read().await.persistence.clone().ok_or_else(|| {
                "OUTBOX_OWNER_UNAVAILABLE: no durable persistence backend".to_string()
            })?;
            Ok(Owner::Graph {
                persistence,
                graph_fname: crate::persist::sanitize(graph),
            })
        }
        OutboxTarget::NativeStore {
            store,
            tenant_id,
            scope,
        } => {
            if tenant_id != verified.tenant() {
                return Err(
                    "ACCESS_DENIED: outbox tenant must match verified request tenant".to_string(),
                );
            }
            resolve_native(state, verified, *store, scope).await
        }
    }
}

/// Resolve a native store's outbox from the scope the store keys it by. The
/// pairing of store and scope kind was checked by `OutboxTarget::validate`.
#[cfg(feature = "redb")]
async fn resolve_native(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    store: NativeOutboxStore,
    scope: &NativeOutboxScope,
) -> Result<Owner, String> {
    match (store, scope) {
        (NativeOutboxStore::AgentLibrary, _) => Ok(Owner::AgentLibrary {
            store: state.write().await.ensure_agent_library()?,
            tenant_id: verified.tenant().to_string(),
        }),
        (NativeOutboxStore::Jobs, _) => jobs_owner(state).await,
        (NativeOutboxStore::SemanticIndex, NativeOutboxScope::SemanticBinding { binding_id }) => {
            semantic_owner(state, verified, binding_id).await
        }
        (NativeOutboxStore::SqlCatalog, NativeOutboxScope::SqlResource { resource }) => {
            sql_owner(state, verified, resource).await
        }
        (NativeOutboxStore::SemanticIndex | NativeOutboxStore::SqlCatalog, _) => {
            Err("OUTBOX_SCOPE_MISMATCH: the store needs its own scope selector".to_string())
        }
    }
}

#[cfg(feature = "redb")]
async fn persist_dir(state: &Arc<RwLock<ServerState>>) -> Result<std::path::PathBuf, String> {
    state
        .read()
        .await
        .persist_dir
        .as_deref()
        .map(std::path::PathBuf::from)
        .ok_or_else(|| "OUTBOX_OWNER_UNAVAILABLE: no configured persist directory".to_string())
}

#[cfg(all(feature = "redb", feature = "ann-redb"))]
async fn semantic_owner(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    binding_id: &str,
) -> Result<Owner, String> {
    let authority = crate::server::access::CarrierAuthority::from_verified(verified)?;
    let service = crate::server::semantic_index::existing_semantic_service(
        &persist_dir(state).await?,
        authority.tenant_scope(),
        binding_id,
    )?;
    Ok(Owner::SemanticBinding { service })
}

#[cfg(all(feature = "redb", not(feature = "ann-redb")))]
async fn semantic_owner(
    _state: &Arc<RwLock<ServerState>>,
    _verified: &VerifiedRequestContext,
    _binding_id: &str,
) -> Result<Owner, String> {
    Err("OUTBOX_OWNER_UNAVAILABLE: the semantic index is not in this build".to_string())
}

#[cfg(all(feature = "redb", feature = "query"))]
async fn sql_owner(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    resource: &str,
) -> Result<Owner, String> {
    let authority = crate::server::access::CarrierAuthority::from_verified(verified)?;
    let tenant_scope = authority.tenant_scope().to_string();
    let store =
        crate::server::sql_tables::tenant_table_store(&tenant_scope, &persist_dir(state).await?)?;
    Ok(Owner::SqlResource {
        store,
        tenant_scope,
        resource: resource.to_string(),
    })
}

#[cfg(all(feature = "redb", not(feature = "query")))]
async fn sql_owner(
    _state: &Arc<RwLock<ServerState>>,
    _verified: &VerifiedRequestContext,
    _resource: &str,
) -> Result<Owner, String> {
    Err("OUTBOX_OWNER_UNAVAILABLE: the SQL catalog is not in this build".to_string())
}

#[cfg(all(feature = "redb", feature = "jobs"))]
async fn jobs_owner(state: &Arc<RwLock<ServerState>>) -> Result<Owner, String> {
    Ok(Owner::Jobs {
        store: crate::server::handlers::jobs::outbox_job_store(state).await?,
    })
}

#[cfg(all(feature = "redb", not(feature = "jobs")))]
async fn jobs_owner(_state: &Arc<RwLock<ServerState>>) -> Result<Owner, String> {
    Err("OUTBOX_OWNER_UNAVAILABLE: the jobs store is not in this build".to_string())
}

#[cfg(feature = "redb")]
impl Owner {
    async fn view(self, view: OutboxView) -> Result<OutboxViewAnswer, String> {
        match self {
            Owner::Graph {
                persistence,
                graph_fname,
            } => {
                persistence
                    .read_mutation_outbox_view(&graph_fname, view)
                    .await
            }
            Owner::AgentLibrary { store, tenant_id } => {
                let now_ms = crate::server::dispatch::authoritative_now_ms();
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    store.outbox_view(&tenant_id, &view, now_ms)
                })
                .await
            }
            #[cfg(feature = "ann-redb")]
            Owner::SemanticBinding { service } => {
                let now_ms = crate::server::dispatch::authoritative_now_ms();
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    service
                        .outbox_operator_view(&view, now_ms)
                        .map_err(|error| error.to_string())
                })
                .await
            }
            #[cfg(feature = "query")]
            Owner::SqlResource {
                store,
                tenant_scope,
                resource,
            } => {
                let now_ms = crate::server::dispatch::authoritative_now_ms();
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    store.outbox_operator_view(&tenant_scope, &resource, &view, now_ms)
                })
                .await
            }
            #[cfg(feature = "jobs")]
            Owner::Jobs { store } => {
                let now_ms = crate::server::dispatch::authoritative_now_ms();
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    let (mutations, kernel, owner) = store.outbox_owner();
                    let read = kernel.read_scope(owner)?;
                    crate::server::outbox_operator::read_view(mutations, &read, &view, now_ms)
                })
                .await
            }
        }
    }

    async fn write(self, write: OutboxWrite) -> Result<OutboxWriteReply, String> {
        match self {
            Owner::Graph {
                persistence,
                graph_fname,
            } => persistence.write_mutation_outbox(&graph_fname, write).await,
            Owner::AgentLibrary { store, tenant_id } => {
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    store.outbox_write(&tenant_id, write)
                })
                .await
            }
            #[cfg(feature = "ann-redb")]
            Owner::SemanticBinding { service } => {
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    service
                        .outbox_operator_write(write)
                        .map_err(|error| error.to_string())
                })
                .await
            }
            #[cfg(feature = "query")]
            Owner::SqlResource {
                store,
                tenant_scope,
                resource,
            } => {
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    store.outbox_operator_write(&tenant_scope, &resource, write)
                })
                .await
            }
            #[cfg(feature = "jobs")]
            Owner::Jobs { store } => {
                crate::server::dispatch::blocking_task(BLOCKING_TASK, move || {
                    let (mutations, _, owner) = store.outbox_owner();
                    crate::server::outbox_operator::operate(mutations, owner, write)
                })
                .await
            }
        }
    }
}
