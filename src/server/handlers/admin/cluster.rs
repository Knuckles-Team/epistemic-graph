//! M3 cluster-admin routing and catalog/rebalance operations.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::mutation_batch::DurabilityDomain;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::persistence::rebalance::{
    plan_rebalance, shard_loads_from_catalog, shard_loads_from_graph_loads,
};
use crate::server::state::ServerState;
use eg_types::contract::Nonce;

use super::backup::{
    cleanup_backup_stage, cleanup_restore_retry_stage, create_private_directory, now_secs,
    opaque_ref, resolve_backup_destination, resolve_backup_source,
};
use super::saga::{
    begin_admin_saga_with_nonce, catalog_saga, finish_admin_saga, live_graph_loads, no_catalog,
    rebalance_opts, rebalance_plan_report, reshard_report,
};

/// Route an M3 admin method (CONCEPT:EG-KG.backend.m3-admin-dispatch). `Ok(resp)` = handled; `Err(method)` = not an
/// admin method (unreachable — the dispatch arm only routes admin variants here).
#[cfg(feature = "redb")]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    attempt_nonce: Option<Nonce>,
    method: Method,
) -> Result<Response, Method> {
    // Keep the backend owner alive across every async helper call.
    let backend_arc = { state.read().await.persistence.clone() };
    let Some(backend) = backend_arc.as_ref().and_then(|p| p.as_redb()) else {
        return Ok(Response::err(
            req_id,
            "M3 resharding admin requires a durable redb backend (no persist dir / not a \
             redb build)",
        ));
    };
    let original_method = method.clone();
    let call = AdminSagaCall {
        req_id,
        caller,
        backend,
        original_method: &original_method,
        attempt_nonce,
    };
    match method {
        Method::Reshard { graph, to_shard } => Ok(handle_reshard(call, graph, to_shard).await),
        Method::CatalogAssign { graph, shard, node } => Ok(catalog_saga::<
            eg_types::result_contract::cluster::CatalogAssign,
        >(
            req_id,
            caller,
            backend,
            &original_method,
            attempt_nonce,
            |catalog| catalog.assign(&crate::persist::sanitize(&graph), shard, node),
        )),
        Method::CatalogReassign { graph, shard } => Ok(catalog_saga::<
            eg_types::result_contract::cluster::CatalogReassign,
        >(
            req_id,
            caller,
            backend,
            &original_method,
            attempt_nonce,
            |catalog| catalog.reassign(&crate::persist::sanitize(&graph), shard),
        )),
        Method::CatalogRemove { graph } => Ok(catalog_saga::<
            eg_types::result_contract::cluster::CatalogRemove,
        >(
            req_id,
            caller,
            backend,
            &original_method,
            attempt_nonce,
            |catalog| catalog.remove(&crate::persist::sanitize(&graph)),
        )),
        Method::CatalogList => Ok(handle_catalog_list(req_id, backend)),
        Method::RebalancePlan {
            tolerance,
            max_moves,
        } => handle_rebalance_plan(state, req_id, backend, tolerance, max_moves).await,
        Method::RebalanceExecute {
            tolerance,
            max_moves,
        } => handle_rebalance_execute(state, call, tolerance, max_moves).await,
        Method::Backup { destination, label } => {
            handle_backup(state, req_id, backend, destination, label).await
        }
        Method::Restore {
            source,
            target_shards,
        } => handle_restore(call, source, target_shards).await,
        other => Err(other),
    }
}

#[cfg(feature = "redb")]
async fn handle_reshard(call: AdminSagaCall<'_>, graph: String, to_shard: u32) -> Response {
    let saga = match call.begin_saga(DurabilityDomain::MultiGraph) {
        Ok(saga) => saga,
        Err(response) => return response,
    };
    let fname = crate::persist::sanitize(&graph);
    let report = match call.backend.reshard_graph(&fname, to_shard).await {
        Ok(report) => report,
        Err(e) => return Response::err(call.req_id, format!("Reshard failed: {e}")),
    };
    match ResultPayload::of::<eg_types::result_contract::cluster::Reshard>(reshard_report(&report))
        .and_then(|result| finish_admin_saga(call.backend, saga.batch, saga.created_at_ms, result))
    {
        Ok(result) => Response::ok(call.req_id, result),
        Err(error) => Response::err(call.req_id, error),
    }
}

#[cfg(feature = "redb")]
fn handle_catalog_list(
    req_id: u64,
    backend: &crate::server::persistence::redb_backend::RedbBackend,
) -> Response {
    let Some(cat) = backend.catalog() else {
        return no_catalog(req_id);
    };
    let placements = cat
        .entries()
        .into_iter()
        .map(
            |(graph, assignment)| eg_types::result_contract::cluster::CatalogPlacement {
                graph,
                shard: assignment.shard,
                node: assignment.node,
            },
        )
        .collect();
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::cluster::CatalogList>(
            eg_types::result_contract::cluster::CatalogListing { placements },
        ),
    )
}

#[cfg(feature = "redb")]
async fn handle_rebalance_plan(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    tolerance: Option<f64>,
    max_moves: Option<usize>,
) -> Result<Response, Method> {
    let (loads, k) = live_graph_loads(state).await;
    let shards = match backend.catalog() {
        Some(cat) => shard_loads_from_catalog(&cat, &loads, k),
        None => {
            // No catalog ⇒ pure EG-026 routing for the placement view.
            let routed: Vec<(String, u32, u64)> = loads
                .iter()
                .map(|(g, l)| {
                    (
                        g.clone(),
                        crate::server::persistence::redb_backend::shard_index(g, k) as u32,
                        *l,
                    )
                })
                .collect();
            shard_loads_from_graph_loads(&routed, k)
        }
    };
    let plan = plan_rebalance(&shards, rebalance_opts(tolerance, max_moves));
    Ok(Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::cluster::RebalancePlan>(
            rebalance_plan_report(&plan, &shards),
        ),
    ))
}

/// The request identity every saga-backed admin operation carries: who asked,
/// for which method, against which durable backend, under which retry nonce.
#[cfg(feature = "redb")]
#[derive(Clone, Copy)]
struct AdminSagaCall<'a> {
    req_id: u64,
    caller: Option<&'a str>,
    backend: &'a crate::server::persistence::redb_backend::RedbBackend,
    original_method: &'a Method,
    attempt_nonce: Option<Nonce>,
}

#[cfg(feature = "redb")]
impl AdminSagaCall<'_> {
    /// Begin this call's admin saga in `domain`. `Err` is already the finished
    /// response: the begin failure, or the durable answer of an earlier attempt
    /// that this one replays.
    fn begin_saga(&self, domain: DurabilityDomain) -> Result<super::saga::AdminSaga, Response> {
        let mut saga = begin_admin_saga_with_nonce(
            self.backend,
            self.req_id,
            self.caller,
            self.original_method,
            domain,
            self.attempt_nonce,
        )
        .map_err(|error| Response::err(self.req_id, error))?;
        match saga.replayed.take() {
            Some(result) => Err(Response::ok(self.req_id, result)),
            None => Ok(saga),
        }
    }
}

#[cfg(feature = "redb")]
async fn handle_rebalance_execute(
    state: &Arc<RwLock<ServerState>>,
    call: AdminSagaCall<'_>,
    tolerance: Option<f64>,
    max_moves: Option<usize>,
) -> Result<Response, Method> {
    let AdminSagaCall {
        req_id, backend, ..
    } = call;
    let saga = match call.begin_saga(DurabilityDomain::MultiGraph) {
        Ok(saga) => saga,
        Err(response) => return Ok(response),
    };
    let Some(cat) = backend.catalog() else {
        return Ok(no_catalog(req_id));
    };
    let (loads, k) = live_graph_loads(state).await;
    let shards = shard_loads_from_catalog(&cat, &loads, k);
    let plan = plan_rebalance(&shards, rebalance_opts(tolerance, max_moves));
    match backend.rebalance_execute(&plan).await {
        Ok(reports) => {
            let executed = reports.iter().map(reshard_report).collect();
            match ResultPayload::of::<eg_types::result_contract::cluster::RebalanceExecute>(
                eg_types::result_contract::cluster::RebalanceExecution { executed },
            )
            .and_then(|result| finish_admin_saga(backend, saga.batch, saga.created_at_ms, result))
            {
                Ok(result) => Ok(Response::ok(req_id, result)),
                Err(error) => Ok(Response::err(req_id, error)),
            }
        }
        Err(e) => Ok(Response::err(
            req_id,
            format!("RebalanceExecute failed: {e}"),
        )),
    }
}

#[cfg(feature = "redb")]
async fn handle_backup(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    destination: String,
    label: Option<String>,
) -> Result<Response, Method> {
    // ONLINE, no quiesce: per-shard begin_read() MVCC snapshot streamed verbatim.
    // The engine version + wall-clock timestamp are supplied HERE (application
    // code) — the library `backup` fn never reads the clock.
    if label
        .as_ref()
        .is_some_and(|value| value.len() > 256 || value.chars().any(char::is_control))
    {
        return Ok(Response::err(req_id, "backup label is invalid"));
    }
    let (destination, stage) = match resolve_backup_destination(&destination, req_id) {
        Ok(value) => value,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let ts = now_secs();
    // Durable stores this backend does NOT own but a restore is incomplete
    // without. redb's exclusive per-file lock means the backup path cannot open
    // them itself, so the live handles are handed in from here. `rbac.redb` is
    // the one that made a restore dangerous: without it a rebuilt engine comes
    // up with NO roles, grants or registered identities.
    #[cfg(feature = "security")]
    let rbac_store = {
        let guard = state.read().await;
        guard
            .isolation
            .policy_store()
            .map(crate::server::persistence::durable_stores::RbacBundledStore)
    };
    #[cfg(feature = "kv")]
    let kv_store = { state.read().await.kv.clone() };
    #[cfg(feature = "redb")]
    let agent_library = {
        let mut guard = state.write().await;
        match guard.ensure_agent_library() {
            Ok(store) => Some(store),
            Err(error) => return Ok(Response::err(req_id, error)),
        }
    };
    let mut extra_stores: Vec<&dyn crate::server::persistence::durable_stores::BundledStoreSource> =
        Vec::new();
    #[cfg(feature = "security")]
    if let Some(rbac_store) = rbac_store.as_ref() {
        extra_stores.push(rbac_store);
    }
    #[cfg(feature = "kv")]
    if let Some(kv_store) = kv_store.as_deref() {
        extra_stores.push(kv_store);
    }
    #[cfg(feature = "redb")]
    if let Some(agent_library) = agent_library.as_deref() {
        extra_stores.push(agent_library);
    }
    match backend.backup(
        &stage,
        env!("CARGO_PKG_VERSION"),
        ts,
        label.as_deref().unwrap_or(""),
        &extra_stores,
    ) {
        Ok(r) => {
            if let Err(error) = publish_backup_stage(&stage, &destination).await {
                return Ok(Response::err(
                    req_id,
                    format!(
                        "Backup publication failed; error_ref={}",
                        opaque_ref(&error.to_string())
                    ),
                ));
            }
            let receipt = eg_types::storage_wire::BackupReceipt {
                shards: r.shards,
                graph_scopes: r.graph_scopes(),
                shard_counts: r.shard_counts,
                xshard_prepares: r.xshard_prepares,
                xshard_decisions: r.xshard_decisions,
                bundled_stores: r.bundled_stores,
                admin_batches: r.admin_mutations.batches,
                prepared_parents: r.admin_mutations.prepared,
                encrypted_recovery_plans: r.admin_mutations.encrypted_private_payloads,
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::storage::Backup>(receipt),
            ))
        }
        Err(e) => {
            cleanup_backup_stage(&stage);
            Ok(Response::err(
                req_id,
                format!("Backup failed; error_ref={}", opaque_ref(&e)),
            ))
        }
    }
}

#[cfg(feature = "redb")]
async fn publish_backup_stage(
    stage: &std::path::Path,
    destination: &std::path::Path,
) -> Result<(), String> {
    let publication_stage = stage.to_path_buf();
    let publication_destination = destination.to_path_buf();
    match ::tokio::task::spawn_blocking(move || {
        std::fs::rename(&publication_stage, publication_destination).map_err(|error| {
            cleanup_backup_stage(&publication_stage);
            error.to_string()
        })
    })
    .await
    {
        Ok(result) => result,
        Err(error) => {
            let cleanup_stage = stage.to_path_buf();
            match ::tokio::task::spawn_blocking(move || {
                cleanup_backup_stage(&cleanup_stage);
            })
            .await
            {
                Ok(()) | Err(_) => {}
            }
            Err(error.to_string())
        }
    }
}

#[cfg(feature = "redb")]
async fn handle_restore(
    call: AdminSagaCall<'_>,
    source: String,
    target_shards: usize,
) -> Result<Response, Method> {
    let AdminSagaCall {
        req_id, backend, ..
    } = call;
    if !(1..=64).contains(&target_shards) {
        return Ok(Response::err(
            req_id,
            "restore target shard count is outside bounds",
        ));
    }
    let saga = match call.begin_saga(DurabilityDomain::ControlPlane) {
        Ok(saga) => saga,
        Err(response) => return Ok(response),
    };
    let source = match resolve_backup_source(&source) {
        Ok(value) => value,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    // The running engine holds an exclusive lock on its live persist dir, so an
    // in-place restore is offline-only (use the `restore` CLI). Over the wire we
    // STAGE the rebuilt copy in a sibling dir for the operator to swap in.
    let Some(persist_dir) = backend.persist_dir() else {
        return Ok(Response::err(
            req_id,
            "cannot resolve the engine persist dir for a staged restore",
        ));
    };
    let stage_token = opaque_ref(&saga.batch.batch_id);
    let suffix = stage_token.trim_start_matches("sha256:");
    let stage = persist_dir.with_file_name(format!(
        "{}.restored-{}",
        persist_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "eg".to_string()),
        suffix
    ));
    // A crash after rebuilding the stage but before committing the
    // portable receipt leaves the parent Prepared. Its deterministic
    // retry must rebuild from a clean target rather than becoming
    // permanently wedged on existing files. Never follow a substituted
    // symlink outside the engine-owned sibling location.
    let retry_stage = stage.clone();
    match ::tokio::task::spawn_blocking(move || cleanup_restore_retry_stage(&retry_stage)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Ok(Response::err(req_id, error)),
        Err(error) => {
            return Ok(Response::err(
                req_id,
                format!(
                    "Restore retry inspection failed; error_ref={}",
                    opaque_ref(&error.to_string())
                ),
            ));
        }
    }
    if let Err(error) = create_private_directory(&stage) {
        return Ok(Response::err(req_id, error));
    }
    match crate::server::persistence::backup::restore_bundle(&source, &stage, target_shards) {
        Ok(r) => {
            // The receipt is portable and contains no local username or
            // filesystem reference. The deterministic stage token is
            // sufficient for an operator-side swap workflow.
            let receipt = eg_types::storage_wire::RestoreReceipt {
                stage_ref: stage_token,
                restored_shards: r.restored_shards,
                graphs: r.migration.graphs,
                nodes: r.migration.nodes,
                edges: r.migration.edges,
                ledger: r.migration.ledger,
                semantic: r.migration.semantic,
                audit: r.migration.audit,
                auxiliary: r.migration.auxiliary,
                global: r.migration.global,
                xshard_prepares: r.manifest.xshard_prepares,
                xshard_decisions: r.manifest.xshard_decisions,
                admin_batches: r.admin_mutations.batches,
                prepared_parents: r.admin_mutations.prepared,
                encrypted_recovery_plans: r.admin_mutations.encrypted_private_payloads,
            };
            let committed =
                ResultPayload::of::<eg_types::result_contract::storage::Restore>(receipt).and_then(
                    |durable| finish_admin_saga(backend, saga.batch, saga.created_at_ms, durable),
                );
            match committed {
                // Return the same portable receipt on first execution and
                // replay. The local staging path is intentionally neither
                // persisted nor exposed through the protocol.
                Ok(result) => Ok(Response::ok(req_id, result)),
                Err(error) => Ok(Response::err(req_id, error)),
            }
        }
        Err(e) => {
            cleanup_backup_stage(&stage);
            Ok(Response::err(
                req_id,
                format!("Restore failed; error_ref={}", opaque_ref(&e)),
            ))
        }
    }
}
