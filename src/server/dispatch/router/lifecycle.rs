//! Graph lifecycle and identity helpers for router dispatch.

use super::*;

/// A `CreateGraph` that finds the graph already resident is either a retry of a
/// durably-committed create (idempotent success) or a genuine collision.
async fn reconcile_existing_graph_create(
    backend: &Arc<dyn PersistenceBackend>,
    attempt: crate::server::mutation::LifecycleAttempt<'_>,
    graph_type: crate::protocol::GraphType,
    created_result: ResultPayload,
) -> Response {
    let req_id = attempt.request_id;
    let graph_name = attempt.graph;
    match crate::server::mutation_batch::lifecycle_was_committed(
        backend,
        attempt,
        Method::CreateGraph {
            graph_name: graph_name.to_string(),
            graph_type,
        },
        &created_result,
    )
    .await
    {
        Ok(true) => Response::ok(req_id, created_result),
        Ok(false) => Response::err(req_id, format!("Graph '{graph_name}' already exists")),
        Err(e) => Response::err(
            req_id,
            format!("durable graph-create reconciliation failed: {e}"),
        ),
    }
}

/// The authoritative version the durable lifecycle commit published. Anything
/// other than a positive version means the registry must not publish this
/// incarnation.
async fn read_committed_graph_version(
    backend: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    req_id: u64,
) -> Result<u64, Response> {
    match backend.read_mutation_graph_version(graph_fname).await {
        Ok(Some(version)) if version > 0 => Ok(version),
        Ok(Some(_)) => Err(Response::err(
            req_id,
            "durable graph registration published an invalid zero version",
        )),
        Ok(None) => Err(Response::err(
            req_id,
            "durable graph registration published no authoritative version",
        )),
        Err(error) => Err(Response::err(
            req_id,
            format!("durable graph version read failed: {error}"),
        )),
    }
}

/// The caller identity a graph lifecycle request commits under.
struct GraphLifecycleRequest {
    req_id: u64,
    req_agent_id: Option<String>,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    idempotency_key: String,
}

pub(super) async fn create_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    idempotency_key: String,
    graph_name: String,
    graph_type: crate::protocol::GraphType,
) -> Response {
    let request = GraphLifecycleRequest {
        req_id,
        req_agent_id,
        attempt_nonce,
        idempotency_key,
    };
    match ResultPayload::of::<eg_types::result_contract::cluster::CreateGraph>(
        eg_types::result_contract::cluster::GraphCreated {
            created: graph_name.clone(),
        },
    ) {
        Ok(created_result) => {
            create_declared_graph(state, request, graph_name, graph_type, created_result).await
        }
        Err(error) => Response::err(req_id, error),
    }
}

/// `CreateGraph` once its declared result is encoded: the durable registration,
/// then the resident incarnation.
async fn create_declared_graph(
    state: &Arc<RwLock<ServerState>>,
    request: GraphLifecycleRequest,
    graph_name: String,
    graph_type: crate::protocol::GraphType,
    created_result: ResultPayload,
) -> Response {
    let GraphLifecycleRequest {
        req_id,
        req_agent_id,
        attempt_nonce,
        idempotency_key,
    } = request;
    // Lifecycle shares the same per-graph serialization lane as ordinary
    // MutationBatch/txn writes.  The durable identity must land before the
    // registry publishes this incarnation.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(&graph_name).await;
    let (backend, already_exists) = {
        let s = timed_read(state).await;
        (s.persistence.clone(), s.registry.exists(&graph_name))
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "graph creation requires durable persistence");
    };
    let incarnation_id = crate::server::mutation_batch::lifecycle_batch_id(
        "create",
        &graph_name,
        req_agent_id.as_deref(),
        &idempotency_key,
    );
    if already_exists {
        return reconcile_existing_graph_create(
            &backend,
            crate::server::mutation::LifecycleAttempt {
                action: "create",
                graph: &graph_name,
                request_id: req_id,
                attempt_nonce,
                principal: req_agent_id.as_deref(),
                idempotency_key: &idempotency_key,
            },
            graph_type,
            created_result,
        )
        .await;
    }
    if let Err(e) = crate::server::mutation_batch::commit_lifecycle(
        crate::server::mutation_batch::LifecycleCommitRequest::new(
            &backend,
            "create",
            crate::server::mutation_batch::CommitOrigin {
                request_id: req_id,
                principal: req_agent_id.as_deref(),
            },
            &idempotency_key,
            &graph_name,
            Method::CreateGraph {
                graph_name: graph_name.clone(),
                graph_type,
            },
            &created_result,
        )
        .with_attempt_nonce(attempt_nonce),
    )
    .await
    {
        return Response::err(req_id, format!("durable graph registration failed: {e}"));
    }
    let graph_fname = crate::persist::sanitize(&graph_name);
    let committed_version = match read_committed_graph_version(&backend, &graph_fname, req_id).await
    {
        Ok(version) => version,
        Err(response) => return response,
    };

    let mut s = timed_write(state).await;
    // Bounded hot-context cache admission (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3): a
    // new graph is about to be resident, so make room for it FIRST — evict
    // the coldest resident graph if the finite cap is already reached.
    #[cfg(feature = "redb")]
    {
        let cap = crate::server::persistence::cold_offload::max_resident_graphs();
        let tracker = s.cold_tracker.clone();
        crate::server::persistence::cold_offload::admit_capacity(
            &mut s,
            &tracker,
            &graph_name,
            cap,
        ); // `s`: &mut ServerState via RwLockWriteGuard's DerefMut
    }
    // The creator (when identified) becomes the graph owner, which is
    // what peer-deny / manager-access checks resolve against.
    match s.registry.create_graph_with_incarnation(
        &graph_name,
        graph_type,
        req_agent_id.clone(),
        incarnation_id,
        committed_version,
    ) {
        Ok(()) => {
            crate::metrics::set_graph_size(&graph_name, 0, 0);
            // Close the CreateGraph RBAC-provisioning gap (P0 tenant-graph
            // fix): a tenant graph's `owner` field is otherwise dead under
            // the mandatory `security` build (`check_access` ignores
            // `graph_owner`), so nothing has ever made a freshly-created
            // tenant graph durably readable/writable by an ordinary
            // registered principal. Idempotent; a failure here must never
            // fail graph creation itself (the graph is already durably
            // committed) — it is logged and self-heals on the next
            // `CreateGraph` for a sibling graph of the same tenant, or the
            // deployment-time remediation pass. Security-only (mirrors the
            // `Method::RbacAdmin` precedent above): a non-`security` build
            // decides graph access via `check_access`'s owner-based ACL
            // branch directly, so there is no RBAC store here to provision.
            #[cfg(feature = "security")]
            if let Err(error) = s
                .isolation
                .provision_tenant_graph_access(&graph_name, req_agent_id.as_deref())
            {
                tracing::warn!(
                    graph = %graph_name,
                    %error,
                    "tenant graph RBAC auto-provisioning failed after graph creation \
                     committed; the graph exists but may still be unreadable for \
                     non-System principals until this is retried"
                );
            }
            Response::ok(req_id, created_result)
        }
        Err(e) => Response::err(req_id, e),
    }
}

/// A `DeleteGraph` that finds nothing in the catalog is either a retry of a
/// durably-committed delete (idempotent success) or a genuine miss.
async fn reconcile_missing_graph_delete(
    backend: &Arc<dyn PersistenceBackend>,
    attempt: crate::server::mutation::LifecycleAttempt<'_>,
    deleted_result: ResultPayload,
) -> Response {
    let req_id = attempt.request_id;
    let graph_name = attempt.graph;
    match crate::server::mutation_batch::lifecycle_was_committed(
        backend,
        attempt,
        Method::DeleteGraph {
            graph_name: graph_name.to_string(),
        },
        &deleted_result,
    )
    .await
    {
        Ok(true) => Response::ok(req_id, deleted_result),
        Ok(false) => Response::err(req_id, format!("Graph '{graph_name}' not found")),
        Err(e) => Response::err(
            req_id,
            format!("durable graph-delete reconciliation failed: {e}"),
        ),
    }
}

pub(super) async fn delete_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    idempotency_key: String,
    state_machine_authorized: bool,
    graph_name: &str,
) -> Response {
    let request = GraphLifecycleRequest {
        req_id,
        req_agent_id,
        attempt_nonce,
        idempotency_key,
    };
    match ResultPayload::of::<eg_types::result_contract::cluster::DeleteGraph>(
        eg_types::result_contract::cluster::GraphDeleted {
            deleted: graph_name.to_string(),
        },
    ) {
        Ok(deleted_result) => {
            delete_declared_graph(
                state,
                request,
                state_machine_authorized,
                graph_name,
                deleted_result,
            )
            .await
        }
        Err(error) => Response::err(req_id, error),
    }
}

/// `DeleteGraph` once its declared result is encoded: the access gate, the durable
/// purge, then the in-memory teardown.
async fn delete_declared_graph(
    state: &Arc<RwLock<ServerState>>,
    request: GraphLifecycleRequest,
    state_machine_authorized: bool,
    graph_name: &str,
    deleted_result: ResultPayload,
) -> Response {
    let GraphLifecycleRequest {
        req_id,
        req_agent_id,
        attempt_nonce,
        idempotency_key,
    } = request;
    // Fence gateway/txn writes for this graph across durable purge and RAM
    // teardown.  A retry after a crash at that boundary reconciles from the
    // durable batch record.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    let (backend, exists, denied) = {
        let s = timed_read(state).await;
        let record = s.registry.catalog_record(graph_name);
        // Read-lock half of the access gate; `teardown_deleted_graph_in_memory`
        // re-checks under the write lock after the durable purge commits.
        let denied = if state_machine_authorized {
            None
        } else {
            record.as_ref().and_then(|record| {
                check_graph_access(
                    &s.isolation,
                    req_agent_id.as_deref(),
                    graph_name,
                    record.graph_type,
                    record.owner.as_deref(),
                    AccessLevel::Write,
                )
                .err()
            })
        };
        (s.persistence.clone(), record.is_some(), denied)
    };
    if let Some(denied) = denied {
        return Response::err(req_id, denied);
    }
    let Some(backend) = backend else {
        return Response::err(req_id, "graph deletion requires durable persistence");
    };
    if !exists {
        return reconcile_missing_graph_delete(
            &backend,
            crate::server::mutation::LifecycleAttempt {
                action: "delete",
                graph: graph_name,
                request_id: req_id,
                attempt_nonce,
                principal: req_agent_id.as_deref(),
                idempotency_key: &idempotency_key,
            },
            deleted_result,
        )
        .await;
    }
    if let Err(e) = crate::server::mutation_batch::commit_lifecycle(
        crate::server::mutation_batch::LifecycleCommitRequest::new(
            &backend,
            "delete",
            crate::server::mutation_batch::CommitOrigin {
                request_id: req_id,
                principal: req_agent_id.as_deref(),
            },
            &idempotency_key,
            graph_name,
            Method::DeleteGraph {
                graph_name: graph_name.to_string(),
            },
            &deleted_result,
        )
        .with_attempt_nonce(attempt_nonce),
    )
    .await
    {
        return Response::err(req_id, format!("durable graph purge failed: {e}"));
    }

    teardown_deleted_graph_in_memory(
        state,
        req_id,
        req_agent_id.as_deref(),
        graph_name,
        state_machine_authorized,
        deleted_result,
    )
    .await
}

/// The in-memory teardown half of `DeleteGraph`, after the durable purge has
/// already committed: re-checks graph access under the write lock (the
/// durable-purge check above ran under a read lock, released before the
/// write-lock re-acquire here), removes the registry entry, and forgets every
/// per-graph-NAME-keyed piece of `ServerState` that would otherwise survive
/// a same-name recreate and shadow the new incarnation (write-coalescer
/// cached writer, routed-write-coalescer, per-graph in-flight semaphore,
/// cold-tenant tracker mark) -- see the inline comments at each `.remove()`/
/// `.forget()` call for why each one specifically matters (this is the
/// tenant-churn-corruption fix's own documentation, preserved verbatim).
async fn teardown_deleted_graph_in_memory(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<&str>,
    graph_name: &str,
    state_machine_authorized: bool,
    deleted_result: ResultPayload,
) -> Response {
    let mut s = timed_write(state).await;
    if !state_machine_authorized {
        if let Some(entry) = s.registry.catalog_record(graph_name) {
            if let Err(denied) = check_graph_access(
                &s.isolation,
                req_agent_id,
                graph_name,
                entry.graph_type,
                entry.owner.as_deref(),
                AccessLevel::Write,
            ) {
                return Response::err(req_id, denied);
            }
        }
    }
    match s.registry.delete_graph(graph_name) {
        Ok(()) => {
            crate::metrics::drop_graph(graph_name);
            // In-memory teardown (CONCEPT:EG-KG.backend.many-repeated-create-delete) — distinct from the durable
            // purge below. The registry entry (the live GraphCore) is gone, but
            // per-graph state keyed by NAME elsewhere in ServerState would
            // survive and shadow a same-name recreate. Drop it so the recreate
            // starts truly clean every cycle:
            //  • the write-coalescer's cached writer — its worker owns an
            //    `Arc<GraphCore>` of THIS (deleted) incarnation; left cached,
            //    `writer_for` returns it on recreate (it is name-keyed and
            //    ignores the new core) and routes the new tenant's writes into
            //    the orphaned core — silently dropping them in RAM. THIS is the
            //    tenant-churn corruption.
            //  • the per-graph in-flight semaphore (no data, but bounds an
            //    unbounded entry leak across many churn cycles).
            s.write_coalescer.remove(graph_name);
            // The routed-write-coalescer registry does not hold a stale
            // `Arc<GraphCore>` per writer (each queued job carries its own —
            // see its module docs), so this is resource hygiene only, not a
            // tenant-churn correctness fix like the line above.
            s.routed_write_coalescer.remove(graph_name);
            s.per_graph_inflight.remove(graph_name);
            // Cold-tenant tracker (CONCEPT:EG-KG.backend.r6-feature, R6): forget this graph's access
            // timestamp + offload mark so they don't leak across a same-name recreate.
            #[cfg(feature = "redb")]
            s.cold_tracker.forget(graph_name);
            Response::ok(req_id, deleted_result)
        }
        Err(e) => Response::err(req_id, e),
    }
}

/// Stable wire names for the server index manifest. Kept as their own
/// mappings so the manifest projection below stays a single expression.
pub(super) fn index_kind_label(kind: crate::index::IndexKind) -> &'static str {
    match kind {
        crate::index::IndexKind::Text => "text",
        crate::index::IndexKind::Temporal => "temporal",
        crate::index::IndexKind::DerivedOwl => "derived_owl",
        crate::index::IndexKind::Spatial => "spatial",
        crate::index::IndexKind::Label => "label",
        crate::index::IndexKind::Property => "property",
        crate::index::IndexKind::Ontology => "ontology",
        crate::index::IndexKind::Vector => "vector",
    }
}

pub(super) fn index_validity_label(validity: crate::index::IndexValidity) -> &'static str {
    match validity {
        crate::index::IndexValidity::Building => "building",
        crate::index::IndexValidity::Valid => "valid",
        crate::index::IndexValidity::Stale => "stale",
        crate::index::IndexValidity::Failed => "failed",
    }
}

pub(super) async fn dispatch_get_identity(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    request_graph: &str,
    agent_id: String,
) -> Response {
    match validate_get_identity_graph(request_graph) {
        Ok(()) => {
            let s = timed_read(state).await;
            // `None` = "no identity registered for `agent_id`" (unknown); `Some(identity)`
            // with an empty `roles` Vec = "registered, confirmed to hold no roles". The two
            // MUST stay distinguishable end-to-end — see `IsolationLayer::get_identity` —
            // so a merge-before-register caller can tell "nothing granted yet" from
            // "already confirmed empty", which is the exact ambiguity this RPC exists to
            // eliminate. `serde_json::to_value` preserves that: `None` serializes to JSON
            // `null`, never to an empty object.
            let identity = s.isolation.get_identity(&agent_id);
            drop(s);
            Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::security::GetIdentity>(identity),
            )
        }
        Err(error) => Response::err(req_id, error),
    }
}

/// A unit-returning RBAC admin mutation answers with a fixed acknowledgement
/// string on success and the store's own message on failure; `M` is the op's
/// declared result.
#[cfg(feature = "security")]
fn rbac_admin_ack<M>(req_id: u64, outcome: Result<(), String>, acknowledgement: &str) -> Response
where
    M: eg_types::result_contract::MethodResult<
        Body = String,
        Encoding = eg_types::result_contract::encoding::Text,
    >,
{
    match outcome {
        Ok(()) => Response::ok(
            req_id,
            ResultPayload::scalar::<M>(acknowledgement.to_string()),
        ),
        Err(message) => Response::err(req_id, message),
    }
}

#[cfg(feature = "security")]
pub(super) async fn apply_rbac_admin(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    op: crate::acl::RbacAdminOp,
) -> Response {
    use crate::acl::RbacAdminOp;
    let mut s = timed_write(state).await;
    match op {
        RbacAdminOp::AddRole(role) => rbac_admin_ack::<
            eg_types::result_contract::security::RbacAddRole,
        >(
            req_id, s.isolation.try_add_role(role), "role_added"
        ),
        RbacAdminOp::RemoveRole(name) => {
            rbac_admin_ack::<eg_types::result_contract::security::RbacRemoveRole>(
                req_id,
                s.isolation.try_remove_role(&name),
                "role_removed",
            )
        }
        RbacAdminOp::AddGrant(grant) => rbac_admin_ack::<
            eg_types::result_contract::security::RbacAddGrant,
        >(
            req_id, s.isolation.try_add_grant(grant), "grant_added"
        ),
        RbacAdminOp::RemoveGrant(grant) => match s.isolation.try_remove_grant(&grant) {
            Ok(removed) => Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::security::RbacRemoveGrant>(
                    eg_types::result_contract::security::RbacGrantRemoval { removed },
                ),
            ),
            Err(message) => Response::err(req_id, message),
        },
        RbacAdminOp::List => {
            let policy = s.isolation.rbac();
            Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::security::RbacList>(
                    eg_types::result_contract::security::RbacPolicyListing {
                        roles: policy.roles().cloned().collect(),
                        grants: policy.grants().to_vec(),
                    },
                ),
            )
        }
    }
}
