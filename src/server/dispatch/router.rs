use super::change_envelope::{dispatch_change_envelopes, multi_graph_batch_update};
use super::consensus::{handle_register_server, replicated_identity_bootstrap_authorized};
use super::graph_pipeline::dispatch_graph_op;
#[cfg(feature = "knowledge-batch")]
use super::graph_pipeline::dispatch_knowledge_stream;
#[cfg(feature = "modality-serving")]
use super::graph_pipeline::dispatch_served_modality;
#[cfg(feature = "cost")]
use super::request_boundary::dispatch_resource_stats;
use super::request_boundary::{
    append_native_capacity_ops, append_native_resource_ops, append_native_work_item_ops,
    decode_screen_observation, dispatch_boxed,
};
#[cfg(feature = "ast")]
use super::request_boundary::{ast_input_limits, decode_ast_files, validate_ast_logical_path};
#[cfg(feature = "sparql-http")]
use super::sparql_update::coordinated_sparql_http_update;
use super::*;
mod channels;
mod graph_lifecycle;
mod identity_access;
mod resource_cost;
mod service_control;
mod source_ingest;

use channels::dispatch_channel_methods;
use graph_lifecycle::dispatch_graph_lifecycle_methods;
use identity_access::dispatch_identity_and_access_methods;
use resource_cost::dispatch_resource_cost_methods;
use service_control::dispatch_service_control_methods;
use source_ingest::dispatch_source_ingest_methods;

/// A `CreateGraph` that finds the graph already resident is either a retry of a
/// durably-committed create (idempotent success) or a genuine collision.
async fn reconcile_existing_graph_create(
    backend: &Arc<dyn PersistenceBackend>,
    graph_name: &str,
    req_id: u64,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    principal: Option<&str>,
    idempotency_key: &str,
    graph_type: crate::protocol::GraphType,
    created_result: ResultPayload,
) -> Response {
    match crate::server::mutation_batch::lifecycle_was_committed(
        backend,
        "create",
        graph_name,
        req_id,
        attempt_nonce,
        principal,
        idempotency_key,
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

async fn create_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    idempotency_key: String,
    graph_name: String,
    graph_type: crate::protocol::GraphType,
) -> Response {
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
    let created_result = ResultPayload::Json(serde_json::json!({
        "created": graph_name.clone()
    }));
    let incarnation_id = crate::server::mutation_batch::lifecycle_batch_id(
        "create",
        &graph_name,
        req_agent_id.as_deref(),
        &idempotency_key,
    );
    if already_exists {
        return reconcile_existing_graph_create(
            &backend,
            &graph_name,
            req_id,
            attempt_nonce,
            req_agent_id.as_deref(),
            &idempotency_key,
            graph_type,
            created_result,
        )
        .await;
    }
    if let Err(e) = crate::server::mutation_batch::commit_lifecycle(
        &backend,
        "create",
        req_id,
        attempt_nonce,
        req_agent_id.as_deref(),
        &idempotency_key,
        &graph_name,
        Method::CreateGraph {
            graph_name: graph_name.clone(),
            graph_type,
        },
        &created_result,
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
    graph_name: &str,
    req_id: u64,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    principal: Option<&str>,
    idempotency_key: &str,
    deleted_result: ResultPayload,
) -> Response {
    match crate::server::mutation_batch::lifecycle_was_committed(
        backend,
        "delete",
        graph_name,
        req_id,
        attempt_nonce,
        principal,
        idempotency_key,
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

async fn delete_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    idempotency_key: String,
    state_machine_authorized: bool,
    graph_name: &str,
) -> Response {
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
    let deleted_result = ResultPayload::Json(serde_json::json!({
        "deleted": graph_name
    }));
    if !exists {
        return reconcile_missing_graph_delete(
            &backend,
            graph_name,
            req_id,
            attempt_nonce,
            req_agent_id.as_deref(),
            &idempotency_key,
            deleted_result,
        )
        .await;
    }
    if let Err(e) = crate::server::mutation_batch::commit_lifecycle(
        &backend,
        "delete",
        req_id,
        attempt_nonce,
        req_agent_id.as_deref(),
        &idempotency_key,
        graph_name,
        Method::DeleteGraph {
            graph_name: graph_name.to_string(),
        },
        &deleted_result,
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
fn index_kind_label(kind: crate::index::IndexKind) -> &'static str {
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

fn index_validity_label(validity: crate::index::IndexValidity) -> &'static str {
    match validity {
        crate::index::IndexValidity::Building => "building",
        crate::index::IndexValidity::Valid => "valid",
        crate::index::IndexValidity::Stale => "stale",
        crate::index::IndexValidity::Failed => "failed",
    }
}

async fn dispatch_get_identity(
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
            match serde_json::to_value(&identity) {
                Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                Err(e) => Response::err(req_id, format!("Serialization error: {}", e)),
            }
        }
        Err(error) => Response::err(req_id, error),
    }
}

/// A unit-returning RBAC admin mutation answers with a fixed acknowledgement
/// string on success and the store's own message on failure.
#[cfg(feature = "security")]
fn rbac_admin_ack(req_id: u64, outcome: Result<(), String>, acknowledgement: &str) -> Response {
    match outcome {
        Ok(()) => Response::ok(req_id, ResultPayload::String(acknowledgement.to_string())),
        Err(message) => Response::err(req_id, message),
    }
}

#[cfg(feature = "security")]
async fn apply_rbac_admin(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    op: crate::acl::RbacAdminOp,
) -> Response {
    use crate::acl::RbacAdminOp;
    let mut s = timed_write(state).await;
    match op {
        RbacAdminOp::AddRole(role) => {
            rbac_admin_ack(req_id, s.isolation.try_add_role(role), "role_added")
        }
        RbacAdminOp::RemoveRole(name) => {
            rbac_admin_ack(req_id, s.isolation.try_remove_role(&name), "role_removed")
        }
        RbacAdminOp::AddGrant(grant) => {
            rbac_admin_ack(req_id, s.isolation.try_add_grant(grant), "grant_added")
        }
        RbacAdminOp::RemoveGrant(grant) => match s.isolation.try_remove_grant(&grant) {
            Ok(removed) => Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({ "removed": removed })),
            ),
            Err(message) => Response::err(req_id, message),
        },
        RbacAdminOp::List => {
            let policy = s.isolation.rbac();
            let roles: Vec<_> = policy.roles().cloned().collect();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "roles": roles,
                    "grants": policy.grants(),
                })),
            )
        }
    }
}

/// The request fields every dispatch group needs after `req.method` has been
/// moved out of the `Request`. Keeping the field names (`id`, `graph`,
/// `agent_id`) means each relocated match arm still reads exactly as it did
/// inside `dispatch_inner`.
struct DispatchHeader {
    id: u64,
    graph: String,
    agent_id: Option<String>,
}

/// The authenticated, preamble-resolved context shared by every dispatch group.
/// All fields are shared references or flags, so this is `Copy` and each group
/// call is free.
#[derive(Clone, Copy)]
struct DispatchCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req: &'a DispatchHeader,
    verified_context: &'a VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
}

/// Cluster and shard administration: reshard/catalog/rebalance/backup/restore,
/// the placement catalog, Raft membership, cluster topology discovery and the
/// fleet server registry. All self-routing and cluster-wide, never graph-scoped.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
async fn dispatch_cluster_admin_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {

        // ── M3 catalog-driven resharding admin (CONCEPT:EG-KG.backend.m3-admin-dispatch) ──────
        // The wire surface that drives online resharding (EG-032), the tenant catalog
        // (EG-031) and the rebalance planner (EG-035) + its execution (EG-039). All
        // self-routing service-level ops handled here (not the per-graph chain), so they
        // reach the concrete redb backend via `as_redb`. A non-redb build returns a clean
        // "not available" error from the handler.
                method @ (Method::Reshard { .. }
        | Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. }
        | Method::CatalogList
        | Method::RebalancePlan { .. }
        | Method::RebalanceExecute { .. }
        // ── Online backup / restore + PITR (CONCEPT:EG-KG.sharding.reshard-on-restore) ──────────
        // Routed through the SAME admin handler: self-routing service-level DR ops that
        // reach the concrete redb backend via `as_redb`. Non-redb builds return a clean
        // "not available" error from the handler.
        | Method::Backup { .. }
        | Method::Restore { .. }) => {
            dispatch_boxed(
                async {
    let req_id = req.id;
    let req_agent_id = req.agent_id.clone();
    {
            match handlers::admin::try_handle(
                state,
                req_id,
                req_agent_id.as_deref(),
                verified_context.attempt_nonce(),
                method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is an admin method.
                Err(_) => Response::err(req_id, "admin dispatch routing error"),
            }
        }
}
            )
            .await
        }

        // ── Placement-catalog wire RPC (CONCEPT:EG-KG.sharding.placement-route-rpc, DIST-P2-4) ──
        // Self-routing, NOT graph-scoped (the catalog is cluster-wide, like the M3 admin
        // block above) — exposes the DIST-P2-1 `PlacementCatalog` over the wire so an
        // external caller (`epistemic_graph.client`'s `placement` namespace, AU's
        // `placement_catalog.py`) can consume it instead of guessing independently. A
        // A single-node engine returns an authoritative unplaced route. A configured
        // Raft node without MultiRaft is an invalid cluster and fails closed.
        // The admin mutation (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc, DIST-P2-5) is
        // routed through the SAME handler, self-routing exactly like the M3 admin
        // block above -- see `handlers::placement::try_handle`'s per-variant arms.
                method @ (Method::PlacementRoute { .. } | Method::PlacementAdmin { .. }) => {
            dispatch_boxed(
                async {
    let req_id = req.id;
    {
            match handlers::placement::try_handle(state, req_id, method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a placement method.
                Err(_) => Response::err(req_id, "placement dispatch routing error"),
            }
        }
}
            )
            .await
        }

        // ── Raft cluster membership admin (CONCEPT:EG-KG.storage.kg-kg-2 — cluster_deployment.md §5
        // item 2) ── Self-routing, NOT graph-scoped (cluster-wide, like the M3 admin
        // block above): attaches/promotes a node against `MultiRaft` directly. Gated
        // `admin:cluster` by the SAME scope+admin enforcement every other admin-tier
        // method goes through above (`eg_capabilities::policy`), not a second check
        // here.
                method @ (Method::RaftAddLearner { .. } | Method::RaftChangeMembership { .. }) => {
            dispatch_boxed(
                async {
    let req_id = req.id;
    {
            match handlers::raft_admin::try_handle(state, req_id, method).await {
                Ok(resp) => resp,
                // Unreachable: both variants matched above are raft-admin methods.
                Err(_) => Response::err(req_id, "raft-admin dispatch routing error"),
            }
        }
}
            )
            .await
        }

        // ── Cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1) ──
        // Self-routing, NOT graph-scoped (cluster-wide, like the raft-admin block
        // above): `ClusterMembers` answers from ANY node's local `NodeInfoStore` +
        // live `MultiRaft` membership (no leader redirect, unlike `PlacementRoute` —
        // ADR-1's client resolves via any healthy seed). Node self-reports use a
        // typed internal Raft command and never enter public dispatch.
                method @ Method::ClusterMembers => {
            dispatch_boxed(
                async {
    let req_id = req.id;
    {
            match handlers::topology::try_handle(state, req_id, method, verified_context).await {
                Ok(resp) => resp,
                // Unreachable: the matched variant is the topology method.
                Err(_) => Response::err(req_id, "cluster topology dispatch routing error"),
            }
        }
}
            )
            .await
        }

        // ── Fleet server registry (CONCEPT:EG-KG.sharding.server-registry, W2.5) ──────
        // Self-routing, like `ClusterMembers` above, but for the
        // OPPOSITE reason: those are cluster-wide and NOT graph nodes, while this
        // writes a REAL `:Server` graph node into `__commons__` -- self-routes
        // here (rather than resolving `req.graph`) because a fleet server's
        // registration is a fleet-wide singleton concept, never tenant-scoped,
        // exactly like `ApplyMultisigMutation` self-routes before translating
        // into `Method::ApplyMutation` against `req.graph`. See
        // `handle_register_server`'s doc comment.
                Method::RegisterServer {
            name,
            url,
            resources_json,
            ttl_secs,
        } => {
            dispatch_boxed(
                async {
    let req_id = req.id;
    let req_agent_id = req.agent_id.clone();
    {
            handle_register_server(
                state,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                name,
                url,
                resources_json,
                ttl_secs,
            )
            .await
        }
}
            )
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// RF-020 Agent Library is self-routing and tenant-bound, but it is not part
/// of the cluster-admin backend. Keep its owner opening and typed operations
/// on the authenticated request context before the M3 admin handler resolves a
/// graph backend.
async fn dispatch_agent_library_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::AgentLibrary { op } => {
            #[cfg(feature = "redb")]
            {
                dispatch_boxed(async {
                    handlers::admin::handle_agent_library(state, req.id, verified_context, op).await
                })
                .await
            }
            #[cfg(not(feature = "redb"))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "Agent Library is not available in this build (requires the `redb` feature)",
                )
            }
        }
        Method::AgentGraph { op } => {
            #[cfg(feature = "redb")]
            {
                dispatch_boxed(async {
                    handlers::admin::handle_agent_graph(state, req.id, verified_context, op).await
                })
                .await
            }
            #[cfg(not(feature = "redb"))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "Agent graphs are not available in this build (requires the `redb` feature)",
                )
            }
        }
        Method::AgentComponent { op } => {
            #[cfg(feature = "redb")]
            {
                dispatch_boxed(async {
                    handlers::admin::handle_agent_component(state, req.id, verified_context, op)
                        .await
                })
                .await
            }
            #[cfg(not(feature = "redb"))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "Agent components are not available in this build (requires `redb`)",
                )
            }
        }
        Method::SemanticIndex { op } => {
            #[cfg(all(feature = "ann-redb", feature = "query"))]
            {
                dispatch_boxed(async {
                    handlers::semantic_index::handle_semantic_index(
                        state,
                        req.id,
                        verified_context,
                        op,
                    )
                    .await
                })
                .await
            }
            #[cfg(not(all(feature = "ann-redb", feature = "query")))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "The semantic index is not available in this build (requires `ann-redb` and `query`)",
                )
            }
        }
        Method::AgentTemplate { op } => {
            #[cfg(feature = "redb")]
            {
                dispatch_boxed(async {
                    handlers::admin::handle_agent_template(state, req.id, verified_context, op)
                        .await
                })
                .await
            }
            #[cfg(not(feature = "redb"))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "Agent templates are not available in this build (requires `redb`)",
                )
            }
        }
        other => return ControlFlow::Continue(other),
    })
}

/// Self-routing compute and media surfaces: the durable analytics-job plane,
/// statechart programs, quantum programs, speech recognition and static
/// visualization export. Each runs a program or a model rather than touching
/// the graph directly, so none of them is identity or admin — the pre-domain
/// cut had all five in `dispatch_identity_and_admin_methods`.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
async fn dispatch_compute_and_media_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(any(
        feature = "jobs",
        feature = "statechart",
        feature = "quantum-agent-api",
        feature = "asr-whisper",
        feature = "viz-static-export"
    )))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(any(
        feature = "jobs",
        feature = "statechart",
        feature = "quantum-agent-api",
        feature = "asr-whisper",
        feature = "viz-static-export"
    ))]
    {
        #[allow(unused_variables)]
        let DispatchCtx {
            state,
            req,
            verified_context,
            ..
        } = ctx;
        ControlFlow::Break(match method {
            // ── Durable analytics-job plane (CONCEPT:INT-P2-1, feature `jobs`) ──────────
            // NOT graph-scoped (own `jobs.redb`, keyed by `job_id`) — self-routes here,
            // BEFORE the per-graph `dispatch_graph_op` chain, exactly like `TsAppend`/
            // `Kv*`/`CreateChannel` above. See `handlers/jobs.rs` module docs.
            #[cfg(feature = "jobs")]
            Method::AnalyticsJob { op } => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    {
                        // Worker fencing identity and durable actor attribution come from the
                        // authenticated context, never from the unsigned request envelope's
                        // display/agent field.
                        let carrier = match CarrierAuthority::from_verified(verified_context) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        };
                        handlers::jobs::handle(
                            state,
                            req_id,
                            &carrier,
                            verified_context.attempt_nonce(),
                            verified_context.allows_analytics_worker(),
                            op,
                        )
                        .await
                    }
                })
                .await
            }

            // ── Native statechart engine (CONCEPT:INT-P2-2, feature `statechart`) ───────
            // NOT graph-scoped (own `statecharts.redb`, keyed by def_id/instance_id) —
            // self-routes here, BEFORE the per-graph `dispatch_graph_op` chain, exactly
            // like `AnalyticsJob` above. See `handlers/statechart.rs` module docs.
            #[cfg(feature = "statechart")]
            Method::Statechart { op } => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    {
                        // Durable owner attribution comes from the authenticated context, never
                        // from the unsigned request envelope's display/agent field.
                        let carrier = match CarrierAuthority::from_verified(verified_context) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        };
                        handlers::statechart::handle(state, req_id, &carrier, op).await
                    }
                })
                .await
            }

            // ── Agent-facing quantum control plane (Q8, CONCEPT:EG-KG.compute.quantum-agent-api,
            // feature `quantum-agent-api`) ──────────────────────────────────────────
            // NOT graph-scoped (pure compute -- reads no persisted graph state, writes
            // nothing durable) — self-routes here, BEFORE the per-graph `dispatch_graph_op`
            // chain, exactly like `AnalyticsJob`/`Statechart` above. See
            // `handlers::quantum`'s module docs for the full reachability/exactness/audit
            // contract this closes (program doc: "no job-plane, no wire protocol Method,
            // and no KG concept mapping" — the wire protocol Method half ends here).
            #[cfg(feature = "quantum-agent-api")]
            Method::Quantum { op } => handlers::quantum::handle(req.id, op).await,
            // ── Native ASR provider surface (GOC-33, `OWNER-VOICE-ASR`, feature
            // `asr-whisper`) ─────────────────────────────────────────────────
            // NOT graph-scoped (a transcription reads no persisted graph state and
            // commits no durable asr.result.v1 here) — self-routes here, BEFORE the
            // per-graph `dispatch_graph_op` chain, exactly like `Quantum`/`Viz` above.
            // See `handlers::asr`'s module doc for the authority boundary.
            #[cfg(feature = "asr-whisper")]
            Method::Asr { op } => handlers::asr::handle(req.id, op).await,
            // ── Native visualization render surface (D-VZ-1 lanes V4/V6, feature
            // `viz-static-export`) ──────────────────────────────────────────────
            // NOT graph-scoped (a render builds a FRESH ephemeral per-request
            // ColumnStore, never reads a live GraphCore) — self-routes here, BEFORE
            // the per-graph `dispatch_graph_op` chain, exactly like
            // `AnalyticsJob`/`Statechart` above. See `handlers/viz.rs` module docs.
            // Gated on `viz-static-export` (not bare `viz`, which `eg-types` alone
            // already gates the wire `Method::Viz` variant on) — a deliberate,
            // documented deviation: the handler needs a real ColumnStore + export
            // backend to do anything, which only exist at that tier.
            #[cfg(feature = "viz-static-export")]
            Method::Viz { op } => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    {
                        // No durable owner-scoped state to attribute a render to; the carrier
                        // is still resolved for parity with the sibling self-routed handlers
                        // (see `handlers::viz::handle`'s doc).
                        let carrier = match CarrierAuthority::from_verified(verified_context) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        };
                        handlers::viz::handle(state, req_id, &carrier, op).await
                    }
                })
                .await
            }
            other => return ControlFlow::Continue(other),
        })
    }
}

/// Multi-op OCC transactions and their typed sub-operations.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
async fn dispatch_transaction_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid — multi-op OCC ACID) ──────
        // Stateful + self-routing: a Txn* op targets the graph the txn was opened
        // against (resolved from `open_txns`), NOT necessarily `req.graph`, and
        // BeginTxn carries its own graph. So they are handled here (with `state`)
        // BEFORE the graph-op path — never through `dispatch_graph_op`, whose
        // coalescer/registry-lookup assumes a single `req.graph` target. For
        // BeginTxn the request envelope's `graph` is the default target when the
        // body omits one.
        method @ (Method::BeginTxn { .. }
        | Method::TxnAddNode { .. }
        | Method::TxnRemoveNode { .. }
        | Method::TxnAddEdge { .. }
        | Method::TxnRemoveEdge { .. }
        | Method::TxnCas { .. }
        | Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. }
        | Method::Commit { .. }
        | Method::Rollback { .. }) => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_graph = req.graph.clone();
                {
                    // BeginTxn defaults its target to the request envelope's graph.
                    let method = match method {
                        Method::BeginTxn {
                            graph: None,
                            isolation,
                        } => Method::BeginTxn {
                            graph: Some(req_graph.clone()),
                            isolation,
                        },
                        m => m,
                    };
                    match handlers::txn::try_handle(
                        state,
                        req_id,
                        verified_context.agent_id(),
                        verified_context,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        // Unreachable: every variant matched above is a txn method.
                        Err(_) => Response::err(req_id, "txn dispatch routing error"),
                    }
                }
            })
            .await
        }

        // Extended cross-modal STAGING (CONCEPT:EG-KG.compute.eg-187, closing EG-360/361/362 at RPC) — the tsdb-measurement,
        // OWL-axiom and SPARQL-CONSTRUCT stage methods. `handlers::txn::try_handle` handles
        // them (feature-gated), but they carry their OWN `graph` (like `TxnAddEmbedding`),
        // so they route straight there — NO `BeginTxn` graph-default rewrite. Without these
        // arms the variants fell through to the graph-op "not available" catch-all, so an
        // in-txn measurement/axiom/CONSTRUCT staged fine over pgwire (EG-372, which calls the
        // stage fns directly) but ERRORED over the native RPC surface — a "seamless" leak
        // (docs/north_star.md). Each is `cfg`-gated to match its protocol variant, so a slim
        // build without the feature keeps the prior catch-all behavior.
        #[cfg(feature = "tsdb")]
        method @ Method::TxnAddMeasurement { .. } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    match handlers::txn::try_handle(
                        state,
                        req_id,
                        verified_context.agent_id(),
                        verified_context,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        Err(_) => Response::err(req_id, "txn dispatch routing error"),
                    }
                }
            })
            .await
        }
        #[cfg(feature = "owl")]
        method @ Method::TxnAxiom { .. } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    match handlers::txn::try_handle(
                        state,
                        req_id,
                        verified_context.agent_id(),
                        verified_context,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        Err(_) => Response::err(req_id, "txn dispatch routing error"),
                    }
                }
            })
            .await
        }
        #[cfg(feature = "sparql")]
        method @ Method::TxnConstruct { .. } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    match handlers::txn::try_handle(
                        state,
                        req_id,
                        verified_context.agent_id(),
                        verified_context,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        Err(_) => Response::err(req_id, "txn dispatch routing error"),
                    }
                }
            })
            .await
        }
        // Planner-writeback staging (CONCEPT:EG-KG.query.plan-dag, D7) — carries its OWN
        // `graph` (like `TxnConstruct`), so it routes straight to the txn handler with NO
        // BeginTxn graph-default rewrite. `query`-gated to match its protocol variant.
        #[cfg(feature = "query")]
        method @ Method::TxnPlanWriteback { .. } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    match handlers::txn::try_handle(
                        state,
                        req_id,
                        verified_context.agent_id(),
                        verified_context,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        Err(_) => Response::err(req_id, "txn dispatch routing error"),
                    }
                }
            })
            .await
        }
        // Materialize-belief staging (CONCEPT:EG-KG.epistemic.epistemic-substrate, D5) —
        // carries its OWN `graph` (like `TxnPlanWriteback`), so it routes straight to the
        // txn handler with NO BeginTxn graph-default rewrite. `epistemic`-gated to match
        // its protocol variant.
        #[cfg(feature = "epistemic")]
        method @ Method::TxnMaterializeBelief { .. } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    match handlers::txn::try_handle(
                        state,
                        req_id,
                        verified_context.agent_id(),
                        verified_context,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        Err(_) => Response::err(req_id, "txn dispatch routing error"),
                    }
                }
            })
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// The non-graph durable stores that ride the same connection: the chunked blob
/// store, the KV namespace and SQLite file import/export.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
async fn dispatch_store_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(any(feature = "blob", feature = "kv", feature = "sqlite-file")))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(any(feature = "blob", feature = "kv", feature = "sqlite-file"))]
    {
        #[allow(unused_variables)]
        let DispatchCtx {
            state,
            req,
            verified_context,
            ..
        } = ctx;
        ControlFlow::Break(match method {
            // ── Blob (CONCEPT:EG-KG.storage.blob-namespace) ──────────────────────────────────
            // Content-addressed, NOT graph-scoped: a blob is keyed by digest and may be
            // referenced across graphs, so route at the top level (like txn) before the
            // per-graph chain. The variants only exist with the `blob` feature; without
            // it they aren't in the enum and a slim build can't reach this arm.
            #[cfg(feature = "blob")]
            method @ (Method::BlobBegin { .. }
            | Method::BlobChunkPut { .. }
            | Method::BlobCommit { .. }
            | Method::BlobFetchBegin { .. }
            | Method::BlobChunkGet { .. }
            | Method::BlobFetchEnd { .. }
            | Method::BlobRef { .. }
            | Method::BlobUnref { .. }
            | Method::BlobGc) => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    {
                        let carrier = match CarrierAuthority::from_verified(verified_context) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        };
                        match handlers::blob::try_handle(
                            state,
                            req_id,
                            &carrier,
                            verified_context.attempt_nonce(),
                            method,
                        )
                        .await
                        {
                            Ok(resp) => resp,
                            // Unreachable: every variant matched above is a blob method.
                            Err(_) => Response::err(req_id, "blob dispatch routing error"),
                        }
                    }
                })
                .await
            }

            // ── Key→Value (CONCEPT:EG-KG.storage.namespaced-kv-surface) ───────────────────────────────
            // Namespaced KV, NOT graph-scoped: a pair is keyed by (namespace, key) and
            // lives off the node/edge graph, so route at the top level (like blob/txn)
            // before the per-graph chain. The variants only exist with the `kv` feature;
            // without it they aren't in the enum and a slim build can't reach this arm.
            #[cfg(feature = "kv")]
            method @ (Method::KvGet { .. }
            | Method::KvPut { .. }
            | Method::KvDelete { .. }
            | Method::KvScan { .. }
            | Method::KvCas { .. }) => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    {
                        let carrier = match CarrierAuthority::from_verified(verified_context) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        };
                        match crate::server::kv::try_handle(state, req_id, &carrier, method).await {
                            Ok(resp) => resp,
                            // Unreachable: every variant matched above is a kv method.
                            Err(_) => Response::err(req_id, "kv dispatch routing error"),
                        }
                    }
                })
                .await
            }

            // ── SQLite `.db` file import/export (CONCEPT:EG-KG.query.eg-feature/EG-332) ──
            // File-scoped, NOT graph-scoped: both ops target a filesystem `path` and move
            // rows through the verified caller's owner-scoped user-table store (behind `query`), so they
            // self-route here (like the Blob*/Kv* ops) BEFORE the per-graph chain. Gated
            // `sqlite-file` (which pulls the bundled C sqlite kept OUT of pi); a build
            // without it never has the variants in the enum, so this arm can't be reached.
            #[cfg(feature = "sqlite-file")]
            method @ (Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. }) => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    {
                        let carrier = match CarrierAuthority::from_verified(verified_context) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        };
                        match handlers::sqlite_file::try_handle(
                            state,
                            req_id,
                            &carrier,
                            verified_context.attempt_nonce(),
                            method,
                        )
                        .await
                        {
                            Ok(resp) => resp,
                            // Unreachable: both variants matched above are sqlite-file methods.
                            Err(_) => Response::err(req_id, "sqlite-file dispatch routing error"),
                        }
                    }
                })
                .await
            }
            other => return ControlFlow::Continue(other),
        })
    }
}

/// The reactive subscription plane: CDC tailing, continuous queries, watches,
/// triggers and live CEP standing queries.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
async fn dispatch_streaming_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(feature = "streaming"))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(feature = "streaming")]
    {
        #[allow(unused_variables)]
        let DispatchCtx {
            state,
            req,
            verified_context,
            ..
        } = ctx;
        ControlFlow::Break(match method {
            // ── Streaming / CDC / subscriptions (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230) ───
            // The reactive READ + REGISTER surface over the CDC hub on `state` (the WRITE
            // side — emitting changes — lives in the dispatch_graph_op write-side-effect
            // block). These are NOT graph-mutating (CdcRead/Watch/FiredTriggers tail a
            // cursor; Register*/Drop* manage hub registrations), so they self-route here
            // BEFORE the per-graph chain, like tsdb/blob. Gated `streaming`: in a slim
            // build the arm is absent and the variants fall to the graph_ops not-built
            // catch-all (never a panic, never a mis-route).
            #[cfg(feature = "streaming")]
            method @ (Method::CdcRead { .. }
            | Method::RegisterContinuousQuery { .. }
            | Method::ReadContinuousQuery { .. }
            | Method::DropContinuousQuery { .. }
            | Method::Watch { .. }
            | Method::RegisterTrigger { .. }
            | Method::DropTrigger { .. }
            | Method::ListTriggers { .. }
            | Method::FiredTriggers { .. }) => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    {
                        let carrier = match CarrierAuthority::from_verified(verified_context) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        };
                        let read_authority = {
                            let s = timed_read(state).await;
                            match GraphReadAuthority::from_verified(verified_context, &s.isolation)
                            {
                                Ok(authority) => authority,
                                Err(denied) => return Response::err(req_id, denied),
                            }
                        };
                        match handlers::streaming::try_handle(
                            state,
                            req_id,
                            &carrier,
                            &read_authority,
                            method,
                        )
                        .await
                        {
                            Ok(resp) => resp,
                            // Unreachable: every variant matched above is a streaming method.
                            Err(_) => Response::err(req_id, "streaming dispatch routing error"),
                        }
                    }
                })
                .await
            }

            // ── Live CEP standing queries (CONCEPT:EG-KG.query.protocol-types) ───────────────
            // The PUSH half of the event-stream + CEP modality: register a CEP pattern once
            // (CepSubscribe), then long-poll the matches it detects as CDC changes flow
            // (CepPoll). The engine is fed by the CDC hub (the write side lives in the
            // dispatch write-side-effect block via `CepSurface::feed_change`); this is the
            // register + poll surface over it. NOT graph-mutating, so it self-routes here
            // BEFORE the per-graph chain (like the streaming/tsdb/blob surfaces). Gated
            // `all(streaming, stream)`: the CDC feed AND the live NFA engine. A build missing
            // either (e.g. `pi` — streaming, no stream) omits this arm; the `Cep*` variants
            // (gated `streaming`) then fall to the graph_ops not-available catch-all.
            #[cfg(all(feature = "streaming", feature = "stream"))]
            method @ (Method::CepSubscribe { .. }
            | Method::CepPoll { .. }
            | Method::CepUnsubscribe { .. }) => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    {
                        let carrier = match CarrierAuthority::from_verified(verified_context) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        };
                        match crate::server::cep::try_handle(state, req_id, &carrier, method).await
                        {
                            Ok(resp) => resp,
                            // Unreachable: every variant matched above is a CEP method.
                            Err(_) => Response::err(req_id, "cep dispatch routing error"),
                        }
                    }
                })
                .await
            }
            other => return ControlFlow::Continue(other),
        })
    }
}

/// The two governed stream WRITE surfaces — served-modality results and the
/// knowledge batch stream. Both go through an `authorize_and_route_*` admission
/// step before reaching the target graph, which is what separates them from the
/// read-side subscription plane above.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
async fn dispatch_governed_stream_write_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(any(feature = "modality-serving", feature = "knowledge-batch")))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(any(feature = "modality-serving", feature = "knowledge-batch"))]
    {
        #[allow(unused_variables)]
        let DispatchCtx {
            state,
            req,
            verified_context,
            ..
        } = ctx;
        ControlFlow::Break(match method {
            #[cfg(feature = "modality-serving")]
            Method::ServedModality { op } => {
                dispatch_boxed(async {
                    let req_id = req.id;
                    let req_agent_id = req.agent_id.clone();
                    let req_graph = req.graph.clone();
                    {
                        let auth_secret = timed_read(state).await.auth_secret.clone();
                        let authority = match handlers::modality::ModalityAuthority::from_verified(
                            &auth_secret,
                            verified_context.claims(),
                        ) {
                            Ok(authority) => authority,
                            Err(error) => return Response::err(req_id, error),
                        };
                        dispatch_served_modality(
                            state,
                            &req_graph,
                            req_id,
                            req_agent_id.as_deref(),
                            verified_context,
                            op,
                            authority,
                        )
                        .await
                    }
                })
                .await
            }
            #[cfg(feature = "knowledge-batch")]
            Method::KnowledgeStream { request } => {
                dispatch_boxed(async {
    let req_id = req.id;
    let req_agent_id = req.agent_id.clone();
    let req_graph = req.graph.clone();
    {
            let (auth_secret, isolation) = {
                let s = timed_read(state).await;
                (s.auth_secret.clone(), s.isolation.clone())
            };
            // §3's critical finding: as shipped, this was the ONLY production
            // constructor site for a `KnowledgeStreamAuthority`, and it called
            // the lease-less `from_verified` — meaning `authority.policy_lease`
            // was always `None` and every request was unconditionally denied
            // by `validate_request_binding` (mod.rs). Mint the durable lease
            // here and bind it, per GRAPH-POLICY-LEASE-CONTRACT.md §3/§7.
            #[cfg(feature = "security")]
            let authority = match (|| {
                let mint_auth = crate::isolation::MintAuthorization::compute_mac(
                    &auth_secret,
                    verified_context.claims(),
                )
                .and_then(|mac| {
                    crate::isolation::MintAuthorization::new(
                        &auth_secret,
                        verified_context.claims(),
                        &mac,
                    )
                })
                .map_err(|_| {
                    Response::err(req_id, "KnowledgeStream policy authority is unavailable")
                })?;
                let carrier = CarrierAuthority::from_verified(verified_context)
                    .map_err(|denied| Response::err(req_id, denied))?;
                let lease = isolation
                    .mint_policy_decision_lease(
                        &mint_auth,
                        &req_graph,
                        crate::isolation::AccessLevel::Read,
                    )
                    .map(std::sync::Arc::new)
                    .map_err(|error| match error {
                        crate::isolation::MintLeaseError::StoreUnavailable
                        | crate::isolation::MintLeaseError::StoreUnreadable => {
                            Response::err(req_id, "KnowledgeStream policy authority is unavailable")
                        }
                        crate::isolation::MintLeaseError::UnknownActor
                        | crate::isolation::MintLeaseError::AccessDenied => {
                            crate::metrics::access_denied();
                            Response::err(req_id, "ACCESS_DENIED")
                        }
                    })?;
                handlers::knowledge_stream::KnowledgeStreamAuthority::from_verified_with_lease(
                    &auth_secret,
                    verified_context.claims(),
                    &req_graph,
                    &carrier,
                    lease,
                    isolation.policy_store().ok_or_else(|| {
                        Response::err(req_id, "KnowledgeStream policy authority is unavailable")
                    })?,
                )
                .map_err(|error| Response::err(req_id, error))
            })() {
                Ok(authority) => authority,
                Err(response) => return response,
            };
            #[cfg(not(feature = "security"))]
            let authority = {
                let _ = &isolation;
                match handlers::knowledge_stream::KnowledgeStreamAuthority::from_verified(
                    &auth_secret,
                    verified_context.claims(),
                ) {
                    Ok(authority) => authority,
                    Err(error) => return Response::err(req_id, error),
                }
            };
            dispatch_knowledge_stream(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                request,
                authority,
            )
            .await
        }
})
                .await
            }
            other => return ControlFlow::Continue(other),
        })
    }
}

/// Change-envelope replication and content versioning: apply one or many
/// envelopes, read one back, and read the content version / change cursor.
/// `GetChangeCursor` belongs HERE — the pre-domain cut had it alone in a
/// "query and batch" group with two unrelated methods.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
async fn dispatch_change_envelope_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Graph operations (dispatch to target graph) ──────────────
        Method::ApplyChangeEnvelope { envelope } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                let req_graph = req.graph.clone();
                {
                    let claims = verified_context.claims();
                    // A native (non-graph) mutation scope reports no graph name at all.
                    // Comparing `Option<&str>` against `Some(req_graph)` fails closed on
                    // `None` instead of ever coercing it into an empty-string/sentinel
                    // match against the requested graph.
                    if envelope
                        .mutation
                        .identity
                        .scope()
                        .graph_name()
                        .map(crate::mutation_batch::LogicalName::as_str)
                        != Some(req_graph.as_str())
                        || envelope.mutation.identity.tenant().as_str() != claims.tenant
                        || eg_types::mutation_batch::batch_request_number(&envelope.mutation)
                            != Some(req_id)
                        || crate::server::mutation_batch::batch_actor(&envelope.mutation)
                            != Some(verified_context.principal_persistence_id().as_str())
                    // The idempotency key cross-check is GONE: the batch's key
                    // now lives inside the envelope's authority, which the
                    // request boundary mints from this same verified context, so
                    // comparing them would compare a value against itself. The
                    // policy comparison goes with it -- `policy_fingerprint` was
                    // an always-`None` `Option<String>`, so it could only ever
                    // have refused every caller-supplied envelope; the real
                    // policy revision is inside the stable replay identity, where
                    // a change conflicts rather than merely mismatching here.
                    {
                        return Response::err(
                    req_id,
                    "ApplyChangeEnvelope context does not match the verified request authority",
                );
                    }
                    dispatch_graph_op(
                        state,
                        &req_graph,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        Method::ApplyChangeEnvelope { envelope },
                    )
                    .await
                }
            })
            .await
        }
        Method::ApplyChangeEnvelopes { envelopes } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                {
                    dispatch_change_envelopes(
                        state,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        envelopes,
                    )
                    .await
                }
            })
            .await
        }
        Method::GetChangeEnvelope {
            envelope_id,
            tenant,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                let req_graph = req.graph.clone();
                {
                    if tenant != verified_context.claims().tenant {
                        return Response::err(
                            req_id,
                            "ChangeEnvelope reads require the verified tenant context",
                        );
                    }
                    dispatch_graph_op(
                        state,
                        &req_graph,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        Method::GetChangeEnvelope {
                            envelope_id,
                            tenant,
                        },
                    )
                    .await
                }
            })
            .await
        }
        Method::GetContentVersion { object_id, tenant } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                let req_graph = req.graph.clone();
                {
                    if tenant != verified_context.claims().tenant {
                        return Response::err(
                            req_id,
                            "content-version reads require the verified tenant context",
                        );
                    }
                    dispatch_graph_op(
                        state,
                        &req_graph,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        Method::GetContentVersion { object_id, tenant },
                    )
                    .await
                }
            })
            .await
        }
        Method::GetChangeCursor {
            source,
            partition,
            tenant,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                let req_graph = req.graph.clone();
                {
                    if tenant != verified_context.claims().tenant {
                        return Response::err(
                            req_id,
                            "change-cursor reads require the verified tenant context",
                        );
                    }
                    dispatch_graph_op(
                        state,
                        &req_graph,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        Method::GetChangeCursor {
                            source,
                            partition,
                            tenant,
                        },
                    )
                    .await
                }
            })
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// Methods whose graph target rides the METHOD BODY rather than the request
/// envelope: distributed OWL reasoning over a union of graphs, the
/// natural-language query facade (the `/nl` HTTP path has no envelope), and the
/// cross-graph batch write. They self-route here because `dispatch_graph_op`
/// assumes a single `req.graph`.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
async fn dispatch_method_scoped_graph_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Distributed OWL reasoning (CONCEPT:EG-KG.ontology.concept-13) ─────────────
        // Cross-shard: reasons over the UNION of several graphs, so it self-routes
        // here (with `state` to gather each shard's snapshot) BEFORE the per-graph
        // chain — never through `dispatch_graph_op`, which targets a single `req.graph`.
        // Gated `owl`: in a build without it the variant isn't in the enum.
        #[cfg(feature = "owl")]
        method @ Method::OwlReasonDistributed { .. } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let verified_context = // `verified_context` is a `&VerifiedRequestContext` here; spell the
                // clone out so it cannot be read as cloning the reference.
                VerifiedRequestContext::clone(verified_context);
                {
                    let read_authority = {
                        let s = timed_read(state).await;
                        match GraphReadAuthority::from_verified(&verified_context, &s.isolation) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        }
                    };
                    match handlers::rdf::try_handle_distributed(
                        state,
                        req_id,
                        &read_authority,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        // Unreachable: the only variant routed here is OwlReasonDistributed.
                        Err(_) => Response::err(req_id, "owl distributed dispatch routing error"),
                    }
                }
            })
            .await
        }
        // Natural-language query (CONCEPT:EG-KG.query.core-query-input/EG-080): the graph rides the METHOD
        // (the `/nl` HTTP facade path has no request envelope), so route to the method's
        // `graph`, falling back to the request envelope's graph when it is empty. The
        // handler (behind `nl-query`) turns NL→UQL and runs the deterministic
        // `UnifiedQueryText` pipeline; a build without `nl-query` reaches the graph_ops
        // "not available" catch-all like any other feature-off method.
        Method::NlQuery { text, graph } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                let req_graph = req.graph.clone();
                {
                    let target = if graph.is_empty() {
                        req_graph.clone()
                    } else {
                        graph.clone()
                    };
                    dispatch_graph_op(
                        state,
                        &target,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        Method::NlQuery { text, graph },
                    )
                    .await
                }
            })
            .await
        }
        // Batched CROSS-GRAPH write (CONCEPT:EG-KG.storage.multi-graph-batch-write) — the
        // graphs ride the METHOD (one round-trip, many graphs), so like the txn/ts
        // self-routing ops it is handled HERE, BEFORE the single-`req.graph`
        // graph-op path. Each sub-batch fans through the normal per-graph write
        // path CONCURRENTLY, so N distinct graphs commit across N of the K shard
        // writers in parallel.
        Method::MultiGraphBatchUpdate { batches_msgpack } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                {
                    multi_graph_batch_update(
                        state,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        &batches_msgpack,
                    )
                    .await
                }
            })
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// The one guarded arm: a SPARQL-over-HTTP UPDATE arrives as an
/// `ApplyMutation` whose event type is the SPARQL-HTTP update marker. Any
/// other `ApplyMutation` falls through to the ordinary per-graph chain,
/// exactly as the guard's own fallthrough did.
#[cfg(feature = "sparql-http")]
async fn dispatch_sparql_http_update(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::ApplyMutation { event_type, query }
            if event_type == crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT =>
        {
            coordinated_sparql_http_update(
                state,
                req.id,
                req.agent_id.as_deref(),
                verified_context,
                &req.graph,
                query,
            )
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// The control plane: service control, source ingestion, cost telemetry, graph
/// lifecycle, cluster administration, channels, identity/access and the
/// compute/media surfaces — everything resolved before the per-graph data path.
///
/// Each link is a `?` on `ControlFlow`: `Break(response)` short-circuits (the
/// group handled it), `Continue(method)` hands the method to the next group.
async fn dispatch_control_plane_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let method = dispatch_service_control_methods(ctx, method).await?;
    let method = dispatch_source_ingest_methods(ctx, method).await?;
    let method = dispatch_resource_cost_methods(ctx, method).await?;
    let method = dispatch_graph_lifecycle_methods(ctx, method).await?;
    let method = dispatch_agent_library_methods(ctx, method).await?;
    let method = dispatch_cluster_admin_methods(ctx, method).await?;
    let method = dispatch_channel_methods(ctx, method).await?;
    let method = dispatch_identity_and_access_methods(ctx, method).await?;
    dispatch_compute_and_media_methods(ctx, method).await
}

/// The data plane: transactions, the non-graph stores, the subscription plane,
/// the governed stream writes, change-envelope replication, the method-scoped
/// graph targets and the guarded SPARQL-over-HTTP update. A method none of these
/// claims is handed back for the ordinary per-graph chain.
async fn dispatch_data_plane_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let method = dispatch_transaction_methods(ctx, method).await?;
    let method = dispatch_store_methods(ctx, method).await?;
    let method = dispatch_streaming_methods(ctx, method).await?;
    let method = dispatch_governed_stream_write_methods(ctx, method).await?;
    let method = dispatch_change_envelope_methods(ctx, method).await?;
    let method = dispatch_method_scoped_graph_methods(ctx, method).await?;
    #[cfg(feature = "sparql-http")]
    {
        dispatch_sparql_http_update(ctx, method).await
    }
    #[cfg(not(feature = "sparql-http"))]
    {
        ControlFlow::Continue(method)
    }
}

/// Route one authenticated request to its handler.
///
/// The single 59-arm `match req.method` this replaced is now fifteen group
/// dispatchers, one per DOMAIN, over disjoint `Method` variants, tried in
/// order; each hands a method it does not own back as
/// `ControlFlow::Continue`. A method no group claims falls through to the
/// ordinary per-graph chain, which is exactly what the old `_` arm did.
pub(super) async fn dispatch_request_method(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
) -> Response {
    let method = req.method;
    let req = DispatchHeader {
        id: req.id,
        graph: req.graph,
        agent_id: req.agent_id,
    };
    let ctx = DispatchCtx {
        state,
        req: &req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    };
    let method = match dispatch_control_plane_methods(ctx, method).await {
        ControlFlow::Break(response) => return response,
        ControlFlow::Continue(method) => method,
    };
    let method = match dispatch_data_plane_methods(ctx, method).await {
        ControlFlow::Break(response) => return response,
        ControlFlow::Continue(method) => method,
    };
    dispatch_graph_op(
        state,
        &req.graph,
        req.id,
        req.agent_id.as_deref(),
        verified_context,
        method,
    )
    .await
}
