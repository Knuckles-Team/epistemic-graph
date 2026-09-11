//! M3 catalog-driven resharding ADMIN RPC (CONCEPT:EG-KG.backend.m3-admin-dispatch).
//!
//! The wire surface that DRIVES the M3 ops the engine already has the building blocks for:
//! online single-node resharding (CONCEPT:EG-KG.backend.catalog-shard-resolve `RedbBackend::reshard_graph`), the durable
//! tenant catalog (CONCEPT:EG-KG.sharding.empty-catalog-routing `RedbBackend::catalog`), the rebalancing planner
//! (CONCEPT:EG-KG.sharding.even-load-rebalance `rebalance::plan_rebalance`), and its execution (CONCEPT:EG-KG.backend.r3-plan-execution
//! `RedbBackend::rebalance_execute`). These CALL the existing persistence APIs — they do not
//! reimplement them.
//!
//! All are durable-redb-only. The module is always declared so the dispatch routing chain is
//! identical across builds; in a build WITHOUT `redb` the catalog/reshard/planner don't
//! exist, so the handler returns a clean "not available in this build" error.

use std::sync::Arc;

use tokio::sync::RwLock;

#[cfg(feature = "redb")]
use crate::mutation_batch::{
    DurabilityDomain, MutationBatch, MutationBatchCommit, MutationBatchRecord,
    MutationScopeIdentity, MutationSurface,
};
use crate::protocol::{Method, Response};
use crate::server::state::ServerState;
use eg_types::contract::Nonce;

#[cfg(feature = "redb")]
const BACKUP_ROOT_ENV: &str = "EPISTEMIC_GRAPH_BACKUP_ROOT";

#[cfg(feature = "redb")]
fn backup_root() -> Result<std::path::PathBuf, String> {
    let configured = std::env::var_os(BACKUP_ROOT_ENV)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("backup RPC is disabled; configure {BACKUP_ROOT_ENV}"))?;
    let configured = std::path::PathBuf::from(configured);
    let metadata = std::fs::symlink_metadata(&configured)
        .map_err(|_| "configured backup root is unavailable".to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("configured backup root must be a real directory".to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("configured backup root must have private permissions".to_string());
        }
    }
    configured
        .canonicalize()
        .map_err(|_| "configured backup root is unavailable".to_string())
}

#[cfg(feature = "redb")]
fn backup_bundle_name(value: &str) -> Result<&str, String> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err("backup bundle name is required".to_string());
    };
    if value.len() > 128
        || !first.is_ascii_alphanumeric()
        || !chars.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
    {
        return Err("backup bundle name must be a bounded logical name".to_string());
    }
    Ok(value)
}

#[cfg(feature = "redb")]
fn resolve_backup_destination(
    value: &str,
    request_id: u64,
) -> Result<(std::path::PathBuf, std::path::PathBuf), String> {
    let root = backup_root()?;
    let name = backup_bundle_name(value)?;
    let destination = root.join(name);
    if destination.exists() || std::fs::symlink_metadata(&destination).is_ok() {
        return Err("backup destination already exists".to_string());
    }
    let token = opaque_ref(&format!("backup-stage:{request_id}:{name}"));
    let stage = root.join(format!(
        ".backup-stage-{}",
        token.trim_start_matches("sha256:")
    ));
    if let Ok(metadata) = std::fs::symlink_metadata(&stage) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err("backup staging target is unsafe".to_string());
        }
        std::fs::remove_dir_all(&stage).map_err(|_| "backup staging cleanup failed".to_string())?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder
            .create(&stage)
            .map_err(|_| "create private backup stage failed".to_string())?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir(&stage).map_err(|_| "create private backup stage failed".to_string())?;
    Ok((destination, stage))
}

#[cfg(feature = "redb")]
fn resolve_backup_source(value: &str) -> Result<std::path::PathBuf, String> {
    let root = backup_root()?;
    let name = backup_bundle_name(value)?;
    let candidate = root.join(name);
    let metadata = std::fs::symlink_metadata(&candidate)
        .map_err(|_| "backup source does not exist".to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("backup source must be a real directory".to_string());
    }
    let source = candidate
        .canonicalize()
        .map_err(|_| "backup source is unavailable".to_string())?;
    if source.parent() != Some(root.as_path()) {
        return Err("backup source escaped the configured root".to_string());
    }
    Ok(source)
}

#[cfg(feature = "redb")]
fn cleanup_backup_stage(stage: &std::path::Path) {
    if matches!(
        std::fs::symlink_metadata(stage),
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink()
    ) {
        let _ = std::fs::remove_dir_all(stage);
    }
}

#[cfg(feature = "redb")]
fn cleanup_restore_retry_stage(stage: &std::path::Path) -> Result<(), String> {
    match std::fs::symlink_metadata(stage) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err("staged restore target is not an engine-owned directory".to_string())
        }
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(stage).map_err(|error| {
            format!(
                "Restore retry cleanup failed; error_ref={}",
                opaque_ref(&error.to_string())
            )
        }),
        Ok(_) => Err("staged restore target is not an engine-owned directory".to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "Restore retry inspection failed; error_ref={}",
            opaque_ref(&error.to_string())
        )),
    }
}

#[cfg(feature = "redb")]
fn create_private_directory(path: &std::path::Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder
            .create(path)
            .map_err(|_| "create private engine directory failed".to_string())
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir(path).map_err(|_| "create private engine directory failed".to_string())
    }
}

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
    use crate::protocol::ResultPayload;
    use crate::server::persistence::rebalance::{
        plan_rebalance, shard_loads_from_catalog, shard_loads_from_graph_loads,
    };

    // Resolve the concrete redb backend once (every admin op needs it). The owning Arc
    // (`backend_arc`) is kept alive for the whole fn so the `&RedbBackend` borrow stays
    // valid across the async reshard/rebalance awaits.
    let backend_arc = { state.read().await.persistence.clone() };
    let backend = match backend_arc.as_ref().and_then(|p| p.as_redb()) {
        Some(b) => b,
        None => {
            return Ok(Response::err(
                req_id,
                "M3 resharding admin requires a durable redb backend (no persist dir / not a \
                 redb build)",
            ))
        }
    };

    let original_method = method.clone();
    match method {
        Method::Reshard { graph, to_shard } => {
            let saga = match begin_admin_saga_with_nonce(
                backend,
                req_id,
                caller,
                &original_method,
                DurabilityDomain::MultiGraph,
                attempt_nonce,
            ) {
                Ok(saga) => saga,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            if let Some(result) = saga.replayed {
                return Ok(Response::ok(req_id, result));
            }
            let fname = crate::persist::sanitize(&graph);
            match backend.reshard_graph(&fname, to_shard).await {
                Ok(report) => match finish_admin_saga(
                    backend,
                    saga.batch,
                    saga.created_at_ms,
                    ResultPayload::Json(report_json(&report)),
                ) {
                    Ok(result) => Ok(Response::ok(req_id, result)),
                    Err(error) => Ok(Response::err(req_id, error)),
                },
                Err(e) => Ok(Response::err(req_id, format!("Reshard failed: {e}"))),
            }
        }
        Method::CatalogAssign { graph, shard, node } => Ok(catalog_saga(
            req_id,
            caller,
            backend,
            &original_method,
            attempt_nonce,
            |catalog| catalog.assign(&crate::persist::sanitize(&graph), shard, node),
        )),
        Method::CatalogReassign { graph, shard } => Ok(catalog_saga(
            req_id,
            caller,
            backend,
            &original_method,
            attempt_nonce,
            |catalog| catalog.reassign(&crate::persist::sanitize(&graph), shard),
        )),
        Method::CatalogRemove { graph } => Ok(catalog_saga(
            req_id,
            caller,
            backend,
            &original_method,
            attempt_nonce,
            |catalog| catalog.remove(&crate::persist::sanitize(&graph)),
        )),
        Method::CatalogList => {
            let Some(cat) = backend.catalog() else {
                return Ok(no_catalog(req_id));
            };
            let entries: Vec<serde_json::Value> = cat
                .entries()
                .into_iter()
                .map(|(graph, a)| {
                    serde_json::json!({"graph": graph, "shard": a.shard, "node": a.node})
                })
                .collect();
            Ok(Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({"placements": entries})),
            ))
        }
        Method::RebalancePlan {
            tolerance,
            max_moves,
        } => {
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
                ResultPayload::Json(plan_json(&plan, &shards)),
            ))
        }
        Method::RebalanceExecute {
            tolerance,
            max_moves,
        } => {
            let saga = match begin_admin_saga_with_nonce(
                backend,
                req_id,
                caller,
                &original_method,
                DurabilityDomain::MultiGraph,
                attempt_nonce,
            ) {
                Ok(saga) => saga,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            if let Some(result) = saga.replayed {
                return Ok(Response::ok(req_id, result));
            }
            let Some(cat) = backend.catalog() else {
                return Ok(no_catalog(req_id));
            };
            let (loads, k) = live_graph_loads(state).await;
            let shards = shard_loads_from_catalog(&cat, &loads, k);
            let plan = plan_rebalance(&shards, rebalance_opts(tolerance, max_moves));
            match backend.rebalance_execute(&plan).await {
                Ok(reports) => {
                    let moves: Vec<serde_json::Value> = reports.iter().map(report_json).collect();
                    match finish_admin_saga(
                        backend,
                        saga.batch,
                        saga.created_at_ms,
                        ResultPayload::Json(serde_json::json!({"executed": moves})),
                    ) {
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
        // ── Online backup / restore (CONCEPT:EG-KG.sharding.reshard-on-restore) ─────────────────
        Method::Backup { destination, label } => {
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
            let mut extra_stores: Vec<
                &dyn crate::server::persistence::durable_stores::BundledStoreSource,
            > = Vec::new();
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
                    let publication_stage = stage.clone();
                    let publication_destination = destination.clone();
                    let publication = match ::tokio::task::spawn_blocking(move || {
                        std::fs::rename(&publication_stage, publication_destination).map_err(
                            |error| {
                                cleanup_backup_stage(&publication_stage);
                                error.to_string()
                            },
                        )
                    })
                    .await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            let cleanup_stage = stage.clone();
                            match ::tokio::task::spawn_blocking(move || {
                                cleanup_backup_stage(&cleanup_stage);
                            })
                            .await
                            {
                                Ok(()) | Err(_) => {}
                            }
                            Err(error.to_string())
                        }
                    };
                    if let Err(error) = publication {
                        return Ok(Response::err(
                            req_id,
                            format!(
                                "Backup publication failed; error_ref={}",
                                opaque_ref(&error.to_string())
                            ),
                        ));
                    }
                    Ok(Response::ok(
                        req_id,
                        ResultPayload::Json(serde_json::json!({
                            "shards": r.shards,
                            "graph_scopes": r.graph_scopes(),
                            "shard_counts": r.shard_counts,
                            "xshard_prepares": r.xshard_prepares,
                            "xshard_decisions": r.xshard_decisions,
                            "bundled_stores": r.bundled_stores,
                            "admin_batches": r.admin_mutations.batches,
                            "prepared_parents": r.admin_mutations.prepared,
                            "encrypted_recovery_plans": r.admin_mutations.encrypted_private_payloads,
                        })),
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
        Method::Restore {
            source,
            target_shards,
        } => {
            if !(1..=64).contains(&target_shards) {
                return Ok(Response::err(
                    req_id,
                    "restore target shard count is outside bounds",
                ));
            }
            let saga = match begin_admin_saga_with_nonce(
                backend,
                req_id,
                caller,
                &original_method,
                DurabilityDomain::ControlPlane,
                attempt_nonce,
            ) {
                Ok(saga) => saga,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            if let Some(result) = saga.replayed {
                return Ok(Response::ok(req_id, result));
            }
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
            match ::tokio::task::spawn_blocking(move || cleanup_restore_retry_stage(&retry_stage))
                .await
            {
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
            match crate::server::persistence::backup::restore_bundle(&source, &stage, target_shards)
            {
                Ok(r) => {
                    // The receipt is portable and contains no local username or
                    // filesystem reference. The deterministic stage token is
                    // sufficient for an operator-side swap workflow.
                    let durable = ResultPayload::Json(serde_json::json!({
                        "stage_ref": stage_token,
                        "restored_shards": r.restored_shards,
                        "graphs": r.migration.graphs,
                        "nodes": r.migration.nodes,
                        "edges": r.migration.edges,
                        "ledger": r.migration.ledger,
                        "semantic": r.migration.semantic,
                        "audit": r.migration.audit,
                        "auxiliary": r.migration.auxiliary,
                        "global": r.migration.global,
                        "xshard_prepares": r.manifest.xshard_prepares,
                        "xshard_decisions": r.manifest.xshard_decisions,
                        "admin_batches": r.admin_mutations.batches,
                        "prepared_parents": r.admin_mutations.prepared,
                        "encrypted_recovery_plans": r.admin_mutations.encrypted_private_payloads,
                    }));
                    match finish_admin_saga(backend, saga.batch, saga.created_at_ms, durable) {
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
        other => Err(other),
    }
}

#[cfg(feature = "redb")]
fn bind_agent_library_context(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut context: eg_types::AgentLibraryMutationContext,
    purpose_id: &str,
    requires_nonce: bool,
) -> Result<eg_types::AgentLibraryMutationContext, String> {
    if context.tenant_id != verified.tenant() {
        return Err(
            "ACCESS_DENIED: Agent Library tenant must match verified request tenant".to_string(),
        );
    }
    let policy_revision = verified.claims().policy_version.clone();
    let caller = verified.principal_persistence_id();
    let nonce = match verified.attempt_nonce() {
        Some(nonce) => nonce,
        None if requires_nonce => {
            return Err("Agent Library writes require an authenticated attempt nonce".to_string())
        }
        // Status is a read-only query and never reaches the mutation kernel;
        // an internal query producer may still provide an explicit ephemeral
        // nonce to satisfy the shared context shape without consuming it.
        None => Nonce::minted(),
    };
    context.request_id = req_id;
    context.principal = store.owner_principal().to_string();
    context.caller_principal = caller.clone();
    context.attempt_nonce = nonce;
    context.tenant_id = verified.tenant().to_string();
    context.actor_scope = caller;
    // A string, not a record-typed enum: this owner carries agent entries AND
    // agent graphs (RF-ADR-008), and the purpose is the only thing that varies.
    context.purpose_id = purpose_id.to_string();
    context.policy_revision = policy_revision;
    context.policy_digest =
        crate::server::persistence::agent_library::current_agent_library_policy_digest()?;
    // This identifier is derived from the admitted policy revision, rather
    // than copied from a request body. It stays stable across transport retries
    // so it cannot turn a byte-identical operation into a false conflict.
    context.policy_decision_id = format!("agent-library:policy:{}", context.policy_revision);
    context.idempotency_key = verified.idempotency_key().to_string();
    context.trace_id = None;
    context.created_at_ms = crate::server::dispatch::authoritative_now_ms();
    Ok(context)
}

#[cfg(feature = "redb")]
fn bind_agent_library_draft(
    draft: eg_types::AgentLibraryEntryDraft,
    verified: &crate::server::auth::VerifiedRequestContext,
    _action_context: &eg_types::AgentLibraryMutationContext,
) -> Result<eg_types::AgentLibraryEntryDraft, String> {
    if draft.tenant_id != verified.tenant() {
        return Err(
            "ACCESS_DENIED: Agent Library definition tenant must match verified request tenant"
                .to_string(),
        );
    }
    // `draft.agent_id` names the selected built definition. Its actor scope,
    // purpose, and policy digest are immutable definition provenance and must
    // survive a later action by another caller under a newer policy. The
    // authenticated request's action authority is carried separately in the
    // mutation context and outbox event; body attribution never authorizes the
    // action or replaces historical definition fields.
    Ok(draft)
}

/// Serve the typed RF-020 Agent Library operations from the same authenticated
/// dispatch boundary as every other self-routing control-plane method.
#[cfg(feature = "redb")]
/// Publish, retire, inspect or SEARCH one durable agent component
/// (RF-ADR-008 layer 1).
///
/// Same store as the two layers above it. `Search` is the capability query the
/// layer exists for -- it resolves a task through EG's native ontology and
/// matches components by subsumption, so the answer comes from the graph rather
/// than from a model.
pub(crate) async fn handle_agent_component(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: eg_types::agent_component::AgentComponentOp,
) -> Response {
    use crate::protocol::ResultPayload;
    use eg_types::agent_component::AgentComponentOp;

    if let Err(error) = op.validate() {
        return Response::err(req_id, error);
    }
    // Every operation is tenant-bound, including the reads. Checked once here
    // rather than in each arm, so a new arm cannot forget it.
    if op.tenant_id() != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: agent component tenant must match verified request tenant",
        );
    }
    let store = {
        let mut guard = state.write().await;
        match guard.ensure_agent_library() {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        }
    };
    match op {
        AgentComponentOp::Publish { mut request } => {
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                "agent-component:publish",
                true,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            request.component.tenant_id = context.tenant_id.clone();
            request.component.actor_scope = context.actor_scope.clone();
            request.component.purpose_id = context.purpose_id.clone();
            request.component.policy_digest = context.policy_digest.clone();
            request.context = context;
            match store.publish_component(*request) {
                Ok(result) => match ResultPayload::raw(&result.result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        AgentComponentOp::Retire { mut request } => {
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                "agent-component:retire",
                true,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            request.context = context;
            match store.retire_component(request) {
                Ok(result) => match ResultPayload::raw(&result.result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        AgentComponentOp::Current {
            tenant_id,
            component_id,
        } => match store.current_component(&tenant_id, &component_id) {
            Ok(entry) => match ResultPayload::raw(&entry) {
                Ok(payload) => Response::ok(req_id, payload),
                Err(error) => Response::err(req_id, error),
            },
            Err(error) => Response::err(req_id, error),
        },
        AgentComponentOp::History {
            tenant_id,
            component_id,
        } => match store.component_revisions(&tenant_id, &component_id) {
            Ok(entries) => match ResultPayload::raw(&entries) {
                Ok(payload) => Response::ok(req_id, payload),
                Err(error) => Response::err(req_id, error),
            },
            Err(error) => Response::err(req_id, error),
        },
        AgentComponentOp::Status { mut request } => {
            let purpose = match request.kind {
                eg_types::agent_component::AgentComponentMutationKind::Publish => {
                    "agent-component:publish"
                }
                eg_types::agent_component::AgentComponentMutationKind::Retire => {
                    "agent-component:retire"
                }
            };
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                purpose,
                false,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            request.context = context;
            match store.component_status(request) {
                Ok(result) => match ResultPayload::raw(&result.map(|result| result.result)) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        AgentComponentOp::Search { request } => match store.search_components(&request) {
            Ok(entries) => match ResultPayload::raw(&entries) {
                Ok(payload) => Response::ok(req_id, payload),
                Err(error) => Response::err(req_id, error),
            },
            Err(error) => Response::err(req_id, error),
        },
    }
}

/// Publish, retire, or inspect one durable agent GRAPH (RF-ADR-008).
///
/// Routed to the SAME store as [`handle_agent_library`]: a graph is published
/// into the agent-library owner alongside the entries it composes, so there is
/// one `ensure_agent_library` and one physical authority (RF-RULING-004).
pub(crate) async fn handle_agent_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: eg_types::agent_graph::AgentGraphOp,
) -> Response {
    use crate::protocol::ResultPayload;
    use eg_types::agent_graph::AgentGraphOp;

    // Refuse before touching the store: the op's own validator is the one place
    // that knows a graph's structural rules, and a malformed shape must not
    // reach the mutation kernel.
    if let Err(error) = op.validate() {
        return Response::err(req_id, error);
    }
    let store = {
        let mut guard = state.write().await;
        match guard.ensure_agent_library() {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        }
    };
    match op {
        AgentGraphOp::Publish { mut request } => {
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                "agent-graph:publish",
                true,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            // The verified caller owns the tenant/actor/purpose on the record,
            // not the request body: a caller must not be able to publish a
            // graph attributed to another tenant.
            request.graph.tenant_id = context.tenant_id.clone();
            request.graph.actor_scope = context.actor_scope.clone();
            request.graph.purpose_id = context.purpose_id.clone();
            request.graph.policy_digest = context.policy_digest.clone();
            request.context = context;
            match store.publish_graph(*request) {
                Ok(result) => match ResultPayload::raw(&result.result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        AgentGraphOp::Retire { mut request } => {
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                "agent-graph:retire",
                true,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            request.context = context;
            match store.retire_graph(request) {
                Ok(result) => match ResultPayload::raw(&result.result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        AgentGraphOp::Current {
            tenant_id,
            graph_id,
        } => {
            if tenant_id != verified.tenant() {
                return Response::err(
                    req_id,
                    "ACCESS_DENIED: agent graph tenant must match verified request tenant",
                );
            }
            match store.current_graph(&tenant_id, &graph_id) {
                Ok(entry) => match ResultPayload::raw(&entry) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        AgentGraphOp::History {
            tenant_id,
            graph_id,
        } => {
            if tenant_id != verified.tenant() {
                return Response::err(
                    req_id,
                    "ACCESS_DENIED: agent graph tenant must match verified request tenant",
                );
            }
            match store.graph_revisions(&tenant_id, &graph_id) {
                Ok(entries) => match ResultPayload::raw(&entries) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        AgentGraphOp::Status { mut request } => {
            let purpose = match request.kind {
                eg_types::agent_graph::AgentGraphMutationKind::Publish => "agent-graph:publish",
                eg_types::agent_graph::AgentGraphMutationKind::Retire => "agent-graph:retire",
            };
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                purpose,
                false,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            request.context = context;
            match store.graph_status(request) {
                Ok(result) => match ResultPayload::raw(&result.map(|result| result.result)) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
    }
}

pub(crate) async fn handle_agent_library(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: eg_types::AgentLibraryOp,
) -> Response {
    use crate::protocol::ResultPayload;

    let store = {
        let mut guard = state.write().await;
        match guard.ensure_agent_library() {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        }
    };
    match op {
        eg_types::AgentLibraryOp::Publish { mut request } => {
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                "agent-library:publish",
                true,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            request.context = context.clone();
            request.entry = match bind_agent_library_draft(request.entry, verified, &context) {
                Ok(entry) => entry,
                Err(error) => return Response::err(req_id, error),
            };
            match store.publish(*request) {
                Ok(result) => match ResultPayload::raw(&result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        eg_types::AgentLibraryOp::Retire { mut request } => {
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                "agent-library:retire",
                true,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            request.context = context;
            match store.retire(request) {
                Ok(result) => match ResultPayload::raw(&result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        eg_types::AgentLibraryOp::Current {
            tenant_id,
            agent_id,
        } => {
            if tenant_id != verified.tenant() {
                return Response::err(
                    req_id,
                    "ACCESS_DENIED: Agent Library tenant must match verified request tenant",
                );
            }
            match store.current(&tenant_id, &agent_id) {
                Ok(result) => match ResultPayload::raw(&result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        eg_types::AgentLibraryOp::History {
            tenant_id,
            agent_id,
        } => {
            if tenant_id != verified.tenant() {
                return Response::err(
                    req_id,
                    "ACCESS_DENIED: Agent Library tenant must match verified request tenant",
                );
            }
            match store.revisions(&tenant_id, &agent_id) {
                Ok(result) => match ResultPayload::raw(&result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
        eg_types::AgentLibraryOp::Status { mut request } => {
            let kind = request.kind;
            let purpose = match kind {
                eg_types::AgentLibraryMutationKind::Publish => "agent-library:publish",
                eg_types::AgentLibraryMutationKind::Retire => "agent-library:retire",
            };
            let context = match bind_agent_library_context(
                &store,
                req_id,
                verified,
                request.context,
                purpose,
                false,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            request.context = context;
            match store.status(request) {
                Ok(result) => match ResultPayload::raw(&result) {
                    Ok(payload) => Response::ok(req_id, payload),
                    Err(error) => Response::err(req_id, error),
                },
                Err(error) => Response::err(req_id, error),
            }
        }
    }
}

/// Wall-clock Unix seconds for the backup/restore RPC (CONCEPT:EG-KG.sharding.reshard-on-restore). Lives in the
/// HANDLER (application code), never in the library `backup`/`restore_bundle` fns.
#[cfg(feature = "redb")]
fn now_secs() -> u64 {
    crate::server::dispatch::authoritative_now_secs()
}

#[cfg(feature = "redb")]
fn opaque_ref(value: &str) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    format!("sha256:{encoded}")
}

/// Non-redb build: every admin method returns a clean "not available" error.
#[cfg(not(feature = "redb"))]
pub(crate) async fn try_handle(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _attempt_nonce: Option<Nonce>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::Reshard { .. }
        | Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. }
        | Method::CatalogList
        | Method::RebalancePlan { .. }
        | Method::RebalanceExecute { .. }
        | Method::Backup { .. }
        | Method::Restore { .. } => Ok(Response::err(
            req_id,
            "M3 resharding admin is not available in this build (requires the `redb` feature)",
        )),
        other => Err(other),
    }
}

#[cfg(feature = "redb")]
pub(crate) struct AdminSaga {
    pub(crate) batch: MutationBatch,
    pub(crate) created_at_ms: u64,
    pub(crate) replayed: Option<crate::protocol::ResultPayload>,
}

#[cfg(feature = "redb")]
pub(crate) fn begin_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    domain: DurabilityDomain,
) -> Result<AdminSaga, String> {
    begin_admin_saga_with_nonce(backend, req_id, caller, method, domain, None)
}

#[cfg(feature = "redb")]
pub(crate) fn begin_admin_saga_with_nonce(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    domain: DurabilityDomain,
    attempt_nonce: Option<Nonce>,
) -> Result<AdminSaga, String> {
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "cluster-admin",
        "cluster-admin",
        req_id,
        method,
    );
    begin_named_admin_saga_with_nonce(
        backend,
        req_id,
        caller,
        method,
        domain,
        &batch_id,
        attempt_nonce,
    )
}

// `admin_saga_request_stamp` is DELETED, not moved.
//
// It replayed the ORIGINAL attempt's `request_id`, `created_at_ms` and observed
// OCC version back into a rebuilt batch, and its own doc comment said exactly
// why: "Re-deriving them live on every call makes a legitimate replay's
// freshly-compiled batch byte-diverge ... spuriously failing closed with
// IDEMPOTENCY_CONFLICT." That was true while the kernel decided replay by
// WHOLE-BATCH byte identity. `OperationReplayIdentity` structurally excludes all
// three -- it has no timestamp field, no request id, and this slice's canonical
// payload digest excludes the OCC expectation -- so re-deriving them live is now
// correct and the workaround is not merely unnecessary but wrong: reusing a
// stale OCC observation would make the retry claim a version it never observed.

/// Begin or resume a coordinator whose identity spans request retries. Callers
/// supply only an already-opaque key; transaction/session ids and payloads are
/// never copied into the admin ledger.
#[cfg(feature = "redb")]
pub(crate) fn begin_named_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    domain: DurabilityDomain,
    batch_id: &str,
) -> Result<AdminSaga, String> {
    begin_named_admin_saga_with_nonce(backend, req_id, caller, method, domain, batch_id, None)
}

#[cfg(feature = "redb")]
pub(crate) fn begin_named_admin_saga_with_nonce(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    domain: DurabilityDomain,
    batch_id: &str,
    attempt_nonce: Option<Nonce>,
) -> Result<AdminSaga, String> {
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let now = crate::server::dispatch::authoritative_now_ms();
    // Live values on every attempt: a retry legitimately observes a later OCC
    // version, a new dispatch request id and a later clock, and the stable
    // operation identity excludes all three.
    let expected = eg_transaction::version(&backend.admin_mutations_read()?)?;
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: req_id,
            attempt_nonce,
            principal: caller,
            tenant: "native",
            graph: "cluster-admin",
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        method,
        MutationSurface::Other,
        domain,
        "cluster_admin_operation",
    )?;
    let replayed = match backend.admin_saga_step(&batch, now, None)? {
        eg_transaction::SagaBegin::Committed(record) => {
            let (_, result) = decode_admin_commit(record, &identity, true)?;
            Some(result)
        }
        eg_transaction::SagaBegin::Execute | eg_transaction::SagaBegin::Resume(_) => None,
    };
    Ok(AdminSaga {
        batch,
        created_at_ms: now,
        replayed,
    })
}

/// Begin or resume a digest-only coordinator and atomically attach opaque private
/// recovery bytes.  The caller must supply authenticated ciphertext whose plaintext
/// SHA-256 is `payload_digest`; neither the canonical batch nor its outbox contains
/// the private body.
/// The sealed private-payload fields for [`begin_named_admin_saga_with_private_payload`],
/// bundled so the function stays under the clippy argument-count ceiling.
#[cfg(feature = "redb")]
pub(crate) struct AdminSagaPayload<'a> {
    pub(crate) domain: DurabilityDomain,
    pub(crate) batch_id: &'a str,
    pub(crate) event_type: &'a str,
    pub(crate) payload_digest: &'a str,
    pub(crate) encrypted_payload: &'a [u8],
}

#[cfg(feature = "redb")]
pub(crate) fn begin_named_admin_saga_with_private_payload(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    payload: AdminSagaPayload<'_>,
) -> Result<AdminSaga, String> {
    begin_named_admin_saga_with_private_payload_and_nonce(backend, req_id, caller, None, payload)
}

#[cfg(feature = "redb")]
pub(crate) fn begin_named_admin_saga_with_private_payload_and_nonce(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    attempt_nonce: Option<Nonce>,
    payload: AdminSagaPayload<'_>,
) -> Result<AdminSaga, String> {
    let AdminSagaPayload {
        domain,
        batch_id,
        event_type,
        payload_digest,
        encrypted_payload,
    } = payload;
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let now = crate::server::dispatch::authoritative_now_ms();
    // Live values on every attempt: a retry legitimately observes a later OCC
    // version, a new dispatch request id and a later clock, and the stable
    // operation identity excludes all three.
    let expected = eg_transaction::version(&backend.admin_mutations_read()?)?;
    let batch = crate::server::mutation_batch::compile_opaque_digest(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: req_id,
            attempt_nonce,
            principal: caller,
            tenant: "native",
            graph: "cluster-admin",
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        payload_digest,
        MutationSurface::Transaction,
        domain,
        event_type,
    )?;
    let replayed = match backend.admin_saga_step(&batch, now, Some(encrypted_payload))? {
        eg_transaction::SagaBegin::Committed(record) => {
            let (_, result) = decode_admin_commit(record, &identity, true)?;
            Some(result)
        }
        eg_transaction::SagaBegin::Execute | eg_transaction::SagaBegin::Resume(_) => None,
    };
    Ok(AdminSaga {
        batch,
        created_at_ms: now,
        replayed,
    })
}

/// Re-open the exact durable coordinator batch without reconstructing its original
/// payload-bearing operation.  This is the crash-recovery path after ephemeral
/// staging has disappeared.
#[cfg(feature = "redb")]
pub(crate) fn resume_named_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch_id: &str,
    caller: Option<&str>,
) -> Result<Option<AdminSaga>, String> {
    let expected_principal = crate::server::mutation_batch::principal_fingerprint(
        caller.ok_or_else(|| "coordinator recovery requires a verified principal".to_string())?,
    )?;
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let Some(record) = eg_transaction::read_ledger(&backend.admin_mutations_read()?, batch_id)?
    else {
        return Ok(None);
    };
    validate_admin_record(&record, &identity)?;
    validate_admin_lookup_key(&record, batch_id)?;
    // The caller lives in the outbox `actor` header, never in
    // `context.principal` -- which is now the committing ledger's serving
    // principal on every domain (RF-RULING-004 application note). A batch with
    // no header is refused rather than matched.
    if record.committing_actor()? != expected_principal {
        return Err("coordinator receipt does not match caller scope".to_string());
    }
    let replayed = match record.status {
        crate::mutation_batch::MutationBatchStatus::Prepared => None,
        crate::mutation_batch::MutationBatchStatus::Committed => {
            let (record, result) = decode_admin_commit(record, &identity, true)?;
            return Ok(Some(AdminSaga {
                batch: record.batch,
                created_at_ms: record.committed_at_ms,
                replayed: Some(result),
            }));
        }
        crate::mutation_batch::MutationBatchStatus::Aborted => {
            return Err("coordinator receipt was aborted".to_string())
        }
    };
    Ok(Some(AdminSaga {
        batch: record.batch,
        created_at_ms: record.committed_at_ms,
        replayed,
    }))
}

/// Read one terminal named coordinator receipt without re-creating the original
/// payload-bearing operation. Used by acknowledgement-lost transaction retries.
#[cfg(feature = "redb")]
pub(crate) fn read_named_admin_saga_result(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch_id: &str,
    caller: Option<&str>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let expected_principal = crate::server::mutation_batch::principal_fingerprint(
        caller.ok_or_else(|| "coordinator recovery requires a verified principal".to_string())?,
    )?;
    let identity = crate::server::persistence::redb_backend::cluster_admin_scope_identity()?;
    let Some(record) = eg_transaction::read_ledger(&backend.admin_mutations_read()?, batch_id)?
    else {
        return Ok(None);
    };
    validate_admin_record(&record, &identity)?;
    validate_admin_lookup_key(&record, batch_id)?;
    if record.status != crate::mutation_batch::MutationBatchStatus::Committed {
        return Ok(None);
    }
    if record.committing_actor()? != expected_principal {
        return Err("committed coordinator receipt does not match caller scope".to_string());
    }
    let (_, result) = decode_admin_commit(record, &identity, true)?;
    Ok(Some(result))
}

#[cfg(feature = "redb")]
pub(crate) fn finish_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch: MutationBatch,
    committed_at_ms: u64,
    result: crate::protocol::ResultPayload,
) -> Result<crate::protocol::ResultPayload, String> {
    let encoded = rmp_serde::to_vec_named(&result).map_err(|error| error.to_string())?;
    let (record, replayed) = backend.admin_saga_end(&batch, encoded, committed_at_ms)?;
    let (_, durable_result) = decode_admin_commit(record, &batch.identity, replayed)?;
    Ok(durable_result)
}

#[cfg(feature = "redb")]
fn validate_admin_record(
    record: &MutationBatchRecord,
    expected_identity: &MutationScopeIdentity,
) -> Result<(), String> {
    record.validate()?;
    if &record.identity != expected_identity {
        return Err("admin saga receipt does not match its requested scope".to_string());
    }
    Ok(())
}

#[cfg(feature = "redb")]
fn validate_admin_lookup_key(record: &MutationBatchRecord, batch_id: &str) -> Result<(), String> {
    if record.batch.batch_id != batch_id || record.batch.idempotency_key() != batch_id {
        return Err("coordinator receipt identity is corrupt".to_string());
    }
    Ok(())
}

#[cfg(feature = "redb")]
fn decode_admin_commit(
    record: MutationBatchRecord,
    expected_identity: &MutationScopeIdentity,
    replayed: bool,
) -> Result<(MutationBatchRecord, crate::protocol::ResultPayload), String> {
    let commit = MutationBatchCommit {
        record,
        identity: expected_identity.clone(),
        replayed,
    };
    commit.validate()?;
    let bytes = commit
        .record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed admin saga has no result".to_string())?;
    let result = rmp_serde::from_slice(bytes).map_err(|error| error.to_string())?;
    Ok((commit.record, result))
}

#[cfg(feature = "redb")]
fn catalog_saga(
    req_id: u64,
    caller: Option<&str>,
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    method: &Method,
    attempt_nonce: Option<Nonce>,
    apply: impl FnOnce(&crate::server::persistence::tenant_catalog::TenantCatalog) -> Result<(), String>,
) -> Response {
    let saga = match begin_admin_saga_with_nonce(
        backend,
        req_id,
        caller,
        method,
        DurabilityDomain::ControlPlane,
        attempt_nonce,
    ) {
        Ok(saga) => saga,
        Err(error) => return Response::err(req_id, error),
    };
    if let Some(result) = saga.replayed {
        return Response::ok(req_id, result);
    }
    let Some(catalog) = backend.catalog() else {
        return no_catalog(req_id);
    };
    if let Err(error) = apply(&catalog) {
        return Response::err(req_id, format!("catalog write failed: {error}"));
    }
    match finish_admin_saga(
        backend,
        saga.batch,
        saga.created_at_ms,
        crate::protocol::ResultPayload::Bool(true),
    ) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn report_json(
    report: &crate::server::persistence::online_reshard::ReshardReport,
) -> serde_json::Value {
    serde_json::json!({
        "graph": report.graph,
        "from_shard": report.from_shard,
        "to_shard": report.to_shard,
        "nodes": report.nodes,
        "edges": report.edges,
        "ledger": report.ledger,
        "semantic": report.semantic,
        "audit": report.audit,
        "delta_nodes": report.delta_nodes,
        "delta_edges": report.delta_edges,
        "no_op": report.no_op,
    })
}

#[cfg(feature = "redb")]
fn plan_json(
    plan: &crate::server::persistence::rebalance::RebalancePlan,
    shards: &[crate::server::persistence::rebalance::ShardLoad],
) -> serde_json::Value {
    let moves: Vec<serde_json::Value> = plan
        .moves
        .iter()
        .map(|m| {
            serde_json::json!({
                "graph": m.graph,
                "from_shard": m.from_shard,
                "to_shard": m.to_shard,
            })
        })
        .collect();
    let loads: Vec<serde_json::Value> = shards
        .iter()
        .map(
            |s| serde_json::json!({"shard": s.shard, "total": s.total(), "graphs": s.graphs.len()}),
        )
        .collect();
    serde_json::json!({"moves": moves, "shards": loads})
}

#[cfg(feature = "redb")]
fn no_catalog(req_id: u64) -> Response {
    Response::err(
        req_id,
        "no tenant catalog attached (set EPISTEMIC_GRAPH_TENANT_CATALOG=1 and restart)",
    )
}

#[cfg(feature = "redb")]
fn rebalance_opts(
    tolerance: Option<f64>,
    max_moves: Option<usize>,
) -> crate::server::persistence::rebalance::RebalanceOptions {
    let mut opts = crate::server::persistence::rebalance::RebalanceOptions::default();
    if let Some(t) = tolerance {
        opts.tolerance = t;
    }
    if let Some(m) = max_moves {
        opts.max_moves = m;
    }
    opts
}

/// Live per-graph load `(sanitized_fname, resident_node_count)` over the registry + the
/// shard count K (CONCEPT:EG-KG.sharding.even-load-rebalance integration). Resident node count is the KG-2.51 per-graph
/// size dimension — a cheap, available balance metric. `__commons__` is included like any
/// other graph. Returns `(loads, k)`.
#[cfg(feature = "redb")]
async fn live_graph_loads(state: &Arc<RwLock<ServerState>>) -> (Vec<(String, u64)>, usize) {
    let s = state.read().await;
    let loads: Vec<(String, u64)> = s
        .registry
        .all_entries()
        .iter()
        .map(|e| {
            (
                crate::persist::sanitize(&e.name),
                e.core.node_count() as u64,
            )
        })
        .collect();
    let k = s
        .persistence
        .as_ref()
        .and_then(|p| p.as_redb())
        .map(|r| r.shard_count())
        .unwrap_or(1);
    (loads, k)
}

#[cfg(all(test, feature = "redb"))]
mod security_tests {
    use super::backup_bundle_name;

    #[test]
    fn backup_bundle_names_are_logical_not_paths() {
        for valid in ["scheduled-001", "snapshot_2", "release.3"] {
            assert_eq!(backup_bundle_name(valid).unwrap(), valid);
        }
        for invalid in [
            "",
            ".hidden",
            "../snapshot",
            "nested/snapshot",
            "C:\\snapshot",
            "snapshot\n",
        ] {
            assert!(backup_bundle_name(invalid).is_err(), "accepted {invalid:?}");
        }
    }
}

#[cfg(all(test, feature = "redb"))]
mod agent_library_security_tests {
    use super::{backup_bundle_name, bind_agent_library_context, bind_agent_library_draft};
    use crate::acl::RequestContextClaims;
    use crate::protocol::{Method, Request, ResultPayload};
    use crate::server::authority_context::VerifiedRequestContext;
    use crate::server::persistence::durable_stores::BundledStoreSource;
    use eg_types::contract::Nonce;
    use eg_types::{
        AgentLibraryEntryDraft, AgentLibraryMutationContext, AgentLibraryMutationKind,
        AgentLibraryOp, AgentLibraryPublishRequest, AgentLibraryRetireRequest,
        AgentLibraryStatusRequest,
    };
    use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};
    use sha2::{Digest, Sha256};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    const LEDGER_BATCHES: TableDefinition<'static, (&str, &str), &[u8]> =
        TableDefinition::new("ledger_batches");
    const LEDGER_OUTBOX: TableDefinition<'static, (&str, &str, u32), &[u8]> =
        TableDefinition::new("ledger_outbox");
    const REPLAY_OPERATIONS: TableDefinition<'static, (&str, &str), &[u8]> =
        TableDefinition::new("replay_operations");

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn forged_context(
        _store: &crate::server::persistence::agent_library::AgentLibraryStore,
        tenant_id: &str,
    ) -> AgentLibraryMutationContext {
        AgentLibraryMutationContext {
            request_id: 1,
            principal: format!("principal:sha256:{}", "f".repeat(64)),
            caller_principal: format!("principal:sha256:{}", "e".repeat(64)),
            attempt_nonce: Nonce::from_bytes([0xf; 32]),
            tenant_id: tenant_id.to_string(),
            actor_scope: "body-forged-scope".to_string(),
            purpose_id: "body-forged-purpose".to_string(),
            policy_revision: "body-forged-policy".to_string(),
            policy_digest: digest('f'),
            policy_decision_id: "body-forged-decision".to_string(),
            idempotency_key: "body-forged-key".to_string(),
            expected_revision: Some(0),
            trace_id: Some("body-forged-trace".to_string()),
            created_at_ms: 1,
        }
    }

    fn definition(tenant_id: &str) -> AgentLibraryEntryDraft {
        AgentLibraryEntryDraft {
            agent_id: "agent-a".to_string(),
            package_id: "package-a".to_string(),
            version: "1.0.0".to_string(),
            role: "researcher".to_string(),
            role_digest: digest('1'),
            system_prompt: eg_types::agent_component::ComponentDependency {
                component_id: "prompt:a".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::SystemPrompt,
                definition_digest: digest('2'),
            },
            tools: vec![
                eg_types::agent_component::ComponentDependency {
                    component_id: "tool:search".to_string(),
                    kind: eg_types::agent_component::AgentComponentKind::Tool,
                    definition_digest: digest('3'),
                },
            ],
            skills: vec![
                eg_types::agent_component::ComponentDependency {
                    component_id: "skill:research".to_string(),
                    kind: eg_types::agent_component::AgentComponentKind::Skill,
                    definition_digest: digest('4'),
                },
            ],
            model_profile: eg_types::agent_component::ComponentDependency {
                component_id: "model:default".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::ModelProfile,
                definition_digest: digest('5'),
            },
            model_identity: "model:default".to_string(),
            ontologies: vec![
                eg_types::agent_component::ComponentDependency {
                    component_id: "ontology:core".to_string(),
                    kind: eg_types::agent_component::AgentComponentKind::Ontology,
                    definition_digest: digest('6'),
                },
            ],
            tenant_id: tenant_id.to_string(),
            actor_scope: "definition:builder-a".to_string(),
            purpose_id: "agent-library:definition".to_string(),
            policy_digest: digest('7'),
            source_revision: "source:42".to_string(),
            source_revision_digest: digest('8'),
            runtime: Default::default(),
            instantiated_from: None,
        }
    }

    fn claims(
        principal: &str,
        tenant: &str,
        scopes: &[&str],
        policy_version: &str,
    ) -> RequestContextClaims {
        RequestContextClaims {
            principal: principal.to_string(),
            tenant: tenant.to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: "agent-a".to_string(),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            delegation: vec![principal.to_string(), "agent-a".to_string()],
            policy_version: policy_version.to_string(),
            ..RequestContextClaims::default()
        }
    }

    fn signed_agent_library_request(
        secret: &str,
        claims: &RequestContextClaims,
        id: u64,
        op: AgentLibraryOp,
        wire_nonce: &str,
        idempotency_key: &str,
    ) -> Request {
        let mut request = Request {
            id,
            graph: claims.tenant.clone(),
            auth_token: String::new(),
            agent_id: Some(claims.agent_id.clone()),
            method: Method::AgentLibrary { op },
        };
        request.auth_token = crate::server::auth::compute_verified_envelope_token(
            secret,
            &request,
            &crate::server::auth::VerifiedEnvelopeParams {
                context: claims,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_secs(),
                nonce: wire_nonce,
                idempotency_key,
            },
        );
        request
    }

    fn response_write_result(
        response: crate::protocol::Response,
    ) -> eg_types::AgentLibraryWriteResult {
        assert!(
            response.error.is_none(),
            "unexpected route error: {:?}",
            response.error
        );
        let Some(ResultPayload::Raw(bytes)) = response.result else {
            panic!("Agent Library route did not return a raw typed result");
        };
        rmp_serde::from_slice(&bytes).expect("decode Agent Library route result")
    }

    fn native_outbox_count(
        store: &crate::server::persistence::agent_library::AgentLibraryStore,
    ) -> usize {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("agent_library.redb");
        assert!(store.copy_into(&path).unwrap() >= 1);
        let database = redb::Database::open(&path).unwrap();
        let read = database.begin_read().unwrap();
        read.open_table(LEDGER_OUTBOX).unwrap().len().unwrap() as usize
    }

    #[test]
    fn backup_bundle_names_are_logical_not_paths() {
        for valid in ["scheduled-001", "snapshot_2", "release.3"] {
            assert_eq!(backup_bundle_name(valid).unwrap(), valid);
        }
        for invalid in [
            "",
            ".hidden",
            "../snapshot",
            "nested/snapshot",
            "C:\\snapshot",
            "snapshot\n",
        ] {
            assert!(backup_bundle_name(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn agent_library_route_binds_verified_action_and_preserves_definition_provenance() {
        let directory = tempfile::tempdir().unwrap();
        let store = crate::server::persistence::agent_library::AgentLibraryStore::open(
            directory.path().to_str().unwrap(),
        )
        .unwrap();
        let nonce = Nonce::from_bytes([0x11; 32]);
        let verified = VerifiedRequestContext::from_verified_claims_with_nonce(
            RequestContextClaims {
                principal: "caller-a".to_string(),
                tenant: "tenant-a".to_string(),
                audience: "epistemic-graph".to_string(),
                agent_id: "agent-a".to_string(),
                scopes: vec!["agent:library-write".to_string()],
                policy_version: "policy-signed-v2".to_string(),
                ..RequestContextClaims::default()
            },
            "signed-idempotency-key".to_string(),
            Some(nonce),
        );
        let body = forged_context(&store, "tenant-a");
        let bound = bind_agent_library_context(
            &store,
            42,
            &verified,
            body,
            "agent-library:publish",
            true,
        )
        .unwrap();
        assert_eq!(bound.request_id, 42);
        assert_eq!(bound.principal, store.owner_principal());
        assert_eq!(bound.caller_principal, verified.principal_persistence_id());
        assert_eq!(bound.attempt_nonce, nonce);
        assert_eq!(bound.tenant_id, "tenant-a");
        assert_eq!(bound.actor_scope, verified.principal_persistence_id());
        assert_eq!(bound.purpose_id, "agent-library:publish");
        assert_eq!(bound.policy_revision, "policy-signed-v2");
        assert_eq!(bound.idempotency_key, "signed-idempotency-key");
        assert_eq!(bound.trace_id, None);

        let draft = definition("tenant-a");
        let retained = bind_agent_library_draft(draft.clone(), &verified, &bound).unwrap();
        assert_eq!(retained, draft);
        assert_ne!(retained.actor_scope, bound.actor_scope);
        assert_ne!(retained.purpose_id, bound.purpose_id);
        assert_ne!(retained.policy_digest, bound.policy_digest);
    }

    #[test]
    fn agent_library_write_route_rejects_missing_verified_nonce() {
        let directory = tempfile::tempdir().unwrap();
        let store = crate::server::persistence::agent_library::AgentLibraryStore::open(
            directory.path().to_str().unwrap(),
        )
        .unwrap();
        let verified = VerifiedRequestContext::from_verified_claims(
            RequestContextClaims {
                principal: "caller-a".to_string(),
                tenant: "tenant-a".to_string(),
                audience: "epistemic-graph".to_string(),
                agent_id: "agent-a".to_string(),
                scopes: vec!["agent:library-write".to_string()],
                policy_version: "policy-signed-v2".to_string(),
                ..RequestContextClaims::default()
            },
            "signed-idempotency-key".to_string(),
        );
        let error = bind_agent_library_context(
            &store,
            43,
            &verified,
            forged_context(&store, "tenant-a"),
            "agent-library:publish",
            true,
        )
        .unwrap_err();
        assert!(error.contains("authenticated attempt nonce"));
    }

    #[test]
    fn agent_library_signed_route_uses_wire_nonce_and_idempotency() {
        let directory = tempfile::tempdir().unwrap();
        let store = crate::server::persistence::agent_library::AgentLibraryStore::open(
            directory.path().to_str().unwrap(),
        )
        .unwrap();
        let claims = RequestContextClaims {
            principal: "agent-a".to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: "agent-a".to_string(),
            scopes: vec!["agent:library-write".to_string()],
            policy_version: "policy-test".to_string(),
            ..RequestContextClaims::default()
        };
        let mut request = crate::protocol::Request {
            id: 44,
            graph: "tenant-shared".to_string(),
            auth_token: String::new(),
            agent_id: Some("agent-a".to_string()),
            method: crate::protocol::Method::AgentLibrary {
                op: AgentLibraryOp::Publish {
                    request: Box::new(AgentLibraryPublishRequest {
                        context: forged_context(&store, "tenant-shared"),
                        entry: definition("tenant-shared"),
                    }),
                },
            },
        };
        request.auth_token = crate::server::auth::compute_verified_envelope_token(
            "agent-library-test-secret",
            &request,
            &crate::server::auth::VerifiedEnvelopeParams {
                context: &claims,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                nonce: "wire-agent-library-nonce",
                idempotency_key: "wire-agent-library-key",
            },
        );
        let verified =
            crate::server::auth::verify_request("agent-library-test-secret", &request).unwrap();
        let context = match request.method {
            crate::protocol::Method::AgentLibrary {
                op: AgentLibraryOp::Publish { request },
            } => bind_agent_library_context(
                &store,
                44,
                &verified,
                request.context,
                "agent-library:publish",
                true,
            )
            .unwrap(),
            _ => panic!("expected Agent Library publish route"),
        };
        assert_eq!(context.idempotency_key, "wire-agent-library-key");
        assert_eq!(
            context.attempt_nonce,
            Nonce::from_bytes(
                Sha256::digest(b"eg/wire-nonce/v1\0wire-agent-library-nonce",).into()
            )
        );
        assert_eq!(
            context.caller_principal,
            verified.principal_persistence_id()
        );
        assert_eq!(context.tenant_id, "tenant-shared");
        assert_eq!(context.principal, store.owner_principal());
    }

    #[tokio::test]
    async fn signed_public_dispatch_persists_publish_retire_action_and_receipts() {
        let secret = "agent-library-public-dispatch-secret";
        let directory = tempfile::tempdir().unwrap();
        let mut server_state = crate::server::state::ServerState::new_for_test(
            secret,
            crate::isolation::IsolationLayer::new(),
        );
        server_state.persist_dir = Some(directory.path().to_string_lossy().into_owned());
        let state = Arc::new(RwLock::new(server_state));
        let store = {
            let mut guard = state.write().await;
            guard.ensure_agent_library().unwrap()
        };

        let publish_claims = claims(
            "caller-a",
            "tenant-shared",
            &["agent:library-write"],
            "policy-test",
        );
        let publish_key = "public-publish-key";
        let publish_request = signed_agent_library_request(
            secret,
            &publish_claims,
            501,
            AgentLibraryOp::Publish {
                request: Box::new(AgentLibraryPublishRequest {
                    context: forged_context(&store, "tenant-shared"),
                    entry: definition("tenant-shared"),
                }),
            },
            "public-publish-nonce",
            publish_key,
        );
        let expected_publish_actor = VerifiedRequestContext::from_verified_claims(
            publish_claims.clone(),
            publish_key.to_string(),
        )
        .principal_persistence_id();
        let published =
            response_write_result(crate::server::dispatch::dispatch(&state, publish_request).await);
        assert!(!published.replayed);
        assert_eq!(published.entry.entry_revision, 1);
        assert_eq!(
            store.revisions("tenant-shared", "agent-a").unwrap().len(),
            1
        );
        assert_eq!(native_outbox_count(&store), 1);

        // A retry with a fresh authenticated nonce after reopening the owner
        // resolves from the native replay receipt. It must not append another
        // outbox row or require the retained definition to be rebuilt.
        let replay_request = signed_agent_library_request(
            secret,
            &publish_claims,
            506,
            AgentLibraryOp::Publish {
                request: Box::new(AgentLibraryPublishRequest {
                    context: forged_context(&store, "tenant-shared"),
                    entry: definition("tenant-shared"),
                }),
            },
            "public-publish-retry-nonce",
            publish_key,
        );
        drop(store);
        state.write().await.agent_library = None;
        let replayed =
            response_write_result(crate::server::dispatch::dispatch(&state, replay_request).await);
        assert!(replayed.replayed);
        assert_eq!(replayed.entry, published.entry);
        let store = {
            let guard = state.read().await;
            guard.agent_library.as_ref().unwrap().clone()
        };
        assert_eq!(native_outbox_count(&store), 1);

        // All three query operations stay on the native snapshot/status paths;
        // none creates an outbox record.
        let read_claims = claims(
            "caller-a",
            "tenant-shared",
            &["agent:library-read"],
            "policy-test",
        );
        for (id, op) in [
            (
                507,
                AgentLibraryOp::Current {
                    tenant_id: "tenant-shared".to_string(),
                    agent_id: "agent-a".to_string(),
                },
            ),
            (
                508,
                AgentLibraryOp::History {
                    tenant_id: "tenant-shared".to_string(),
                    agent_id: "agent-a".to_string(),
                },
            ),
            (
                509,
                AgentLibraryOp::Status {
                    request: AgentLibraryStatusRequest {
                        context: forged_context(&store, "tenant-shared"),
                        agent_id: "agent-a".to_string(),
                        kind: AgentLibraryMutationKind::Publish,
                    },
                },
            ),
        ] {
            let response = crate::server::dispatch::dispatch(
                &state,
                signed_agent_library_request(
                    secret,
                    &read_claims,
                    id,
                    op,
                    &format!("public-read-nonce-{id}"),
                    &format!("public-read-key-{id}"),
                ),
            )
            .await;
            assert!(response.error.is_none(), "query failed: {response:?}");
        }
        assert_eq!(native_outbox_count(&store), 1);

        let mut retire_body = forged_context(&store, "tenant-shared");
        retire_body.expected_revision = Some(1);
        let retire_claims = claims(
            "caller-b",
            "tenant-shared",
            &["agent:library-write"],
            "policy-test",
        );
        let retire_key = "public-retire-key";
        let retire_request = signed_agent_library_request(
            secret,
            &retire_claims,
            502,
            AgentLibraryOp::Retire {
                request: AgentLibraryRetireRequest {
                    context: retire_body,
                    agent_id: "agent-a".to_string(),
                },
            },
            "public-retire-nonce",
            retire_key,
        );
        let expected_retire_actor = VerifiedRequestContext::from_verified_claims(
            retire_claims.clone(),
            retire_key.to_string(),
        )
        .principal_persistence_id();
        let retired =
            response_write_result(crate::server::dispatch::dispatch(&state, retire_request).await);
        assert!(!retired.replayed);
        assert!(retired.entry.is_retired());
        assert_eq!(
            store.revisions("tenant-shared", "agent-a").unwrap().len(),
            2
        );
        assert_eq!(native_outbox_count(&store), 2);

        // Copy the live owner through its backup seam and inspect the copied
        // durable rows. This proves the public signed route reached the native
        // owner, typed replay receipt, and outbox rather than only returning a
        // handler-shaped response.
        let backup_dir = tempfile::tempdir().unwrap();
        let backup_path = backup_dir.path().join("agent_library.redb");
        assert!(store.copy_into(&backup_path).unwrap() >= 1);
        let database = redb::Database::open(&backup_path).unwrap();
        let read = database.begin_read().unwrap();
        let identity = eg_types::MutationScopeIdentity::fixed_native(
            "tenant-shared",
            eg_types::mutation_batch::DurabilityDomain::ControlPlane,
            "agent-library",
            "agent-library:v1",
        )
        .unwrap();
        let scope_key = eg_storage::ledger_scope_key(&identity);
        let batches = read.open_table(LEDGER_BATCHES).unwrap();
        let publish_batch = batches
            .get((
                scope_key.as_str(),
                format!("agent-library/v1/{publish_key}").as_str(),
            ))
            .unwrap()
            .expect("published batch in copied owner")
            .value()
            .to_vec();
        let publish_batch = eg_storage::decode_batch_record(&publish_batch).unwrap();
        assert!(publish_batch.result_msgpack.is_some());
        let result = eg_types::msgpack::decode_bounded::<eg_types::mutation::MutationResult>(
            publish_batch.result_msgpack.as_deref().unwrap(),
            eg_types::msgpack::MsgpackLimits::new(
                16 * 1024 * 1024,
                200_000,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .unwrap();
        assert!(matches!(
            result,
            eg_types::mutation::MutationResult::DomainResult { .. }
        ));
        let replay_operations = read.open_table(REPLAY_OPERATIONS).unwrap();
        let publish_replay = replay_operations
            .get((scope_key.as_str(), publish_key))
            .unwrap()
            .expect("published typed replay receipt")
            .value()
            .to_vec();
        let publish_replay =
            eg_storage::decode_ledger_record::<eg_storage::OperationReplayRow>(&publish_replay)
                .unwrap();
        assert!(matches!(
            publish_replay.recorded,
            eg_storage::RecordedOperation::Receipt(_)
        ));
        let outbox = read.open_table(LEDGER_OUTBOX).unwrap();
        let publish_outbox = outbox
            .get((
                scope_key.as_str(),
                format!("agent-library/v1/{publish_key}").as_str(),
                0,
            ))
            .unwrap()
            .expect("published outbox row in copied owner")
            .value()
            .to_vec();
        let publish_outbox = eg_storage::decode_outbox_record(&publish_outbox).unwrap();
        assert_eq!(
            publish_outbox.intent.headers.get("actor"),
            Some(&expected_publish_actor)
        );
        let event = eg_types::msgpack::decode_bounded::<eg_types::AgentLibraryOutboxEvent>(
            &publish_outbox.intent.payload,
            eg_types::msgpack::MsgpackLimits::new(
                16 * 1024 * 1024,
                200_000,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .unwrap();
        assert_eq!(event.performing_actor, expected_publish_actor);
        assert_eq!(event.entry.actor_scope, "definition:builder-a");

        let retire_batch = batches
            .get((
                scope_key.as_str(),
                format!("agent-library/v1/{retire_key}").as_str(),
            ))
            .unwrap()
            .expect("retire batch in copied owner")
            .value()
            .to_vec();
        let retire_batch = eg_storage::decode_batch_record(&retire_batch).unwrap();
        let retire_replay = replay_operations
            .get((scope_key.as_str(), retire_key))
            .unwrap()
            .expect("retire typed replay receipt")
            .value()
            .to_vec();
        let retire_replay =
            eg_storage::decode_ledger_record::<eg_storage::OperationReplayRow>(&retire_replay)
                .unwrap();
        assert!(matches!(
            retire_replay.recorded,
            eg_storage::RecordedOperation::Receipt(_)
        ));
        let retire_outbox = outbox
            .get((
                scope_key.as_str(),
                format!("agent-library/v1/{retire_key}").as_str(),
                0,
            ))
            .unwrap()
            .expect("retire outbox row in copied owner")
            .value()
            .to_vec();
        let retire_outbox = eg_storage::decode_outbox_record(&retire_outbox).unwrap();
        assert_eq!(
            retire_outbox.intent.headers.get("actor"),
            Some(&expected_retire_actor)
        );
        let retire_event = eg_types::msgpack::decode_bounded::<eg_types::AgentLibraryOutboxEvent>(
            &retire_outbox.intent.payload,
            eg_types::msgpack::MsgpackLimits::new(
                16 * 1024 * 1024,
                200_000,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .unwrap();
        assert_eq!(retire_event.performing_actor, expected_retire_actor);
        assert_eq!(retire_event.entry.actor_scope, "definition:builder-a");
        assert!(retire_event.entry.is_retired());
        assert_ne!(
            publish_batch.committed_version,
            retire_batch.committed_version
        );

        // The original publish key with changed content is a conflict. Even
        // with a fresh nonce, the native operation identity rejects it before
        // any owner row, receipt, or outbox mutation.
        let mut changed = definition("tenant-shared");
        changed.role = "different-role".to_string();
        let mut changed_context = forged_context(&store, "tenant-shared");
        changed_context.expected_revision = Some(0);
        let conflict = crate::server::dispatch::dispatch(
            &state,
            signed_agent_library_request(
                secret,
                &publish_claims,
                510,
                AgentLibraryOp::Publish {
                    request: Box::new(AgentLibraryPublishRequest {
                        context: changed_context,
                        entry: changed,
                    }),
                },
                "public-publish-conflict-nonce",
                publish_key,
            ),
        )
        .await;
        assert!(
            conflict.error.is_some(),
            "changed payload unexpectedly succeeded"
        );
        assert_eq!(native_outbox_count(&store), 2);

        // The same authenticated route boundary rejects a body that names a
        // different tenant, and scope omissions fail before the owner opens.
        let wrong_tenant = signed_agent_library_request(
            secret,
            &publish_claims,
            503,
            AgentLibraryOp::Publish {
                request: Box::new(AgentLibraryPublishRequest {
                    context: forged_context(&store, "tenant-b"),
                    entry: definition("tenant-b"),
                }),
            },
            "wrong-tenant-nonce",
            "wrong-tenant-key",
        );
        let wrong_tenant_response = crate::server::dispatch::dispatch(&state, wrong_tenant).await;
        assert!(
            wrong_tenant_response
                .error
                .as_deref()
                .is_some_and(|error| error.starts_with("ACCESS_DENIED")),
            "{wrong_tenant_response:?}"
        );
        assert!(store.revisions("tenant-b", "agent-a").unwrap().is_empty());

        let read_only_claims = claims(
            "caller-a",
            "tenant-shared",
            &["agent:library-read"],
            "policy-test",
        );
        let missing_write = signed_agent_library_request(
            secret,
            &read_only_claims,
            504,
            AgentLibraryOp::Publish {
                request: Box::new(AgentLibraryPublishRequest {
                    context: forged_context(&store, "tenant-shared"),
                    entry: definition("tenant-shared"),
                }),
            },
            "missing-write-nonce",
            "missing-write-key",
        );
        let missing_write_response = crate::server::dispatch::dispatch(&state, missing_write).await;
        assert!(
            missing_write_response
                .error
                .as_deref()
                .is_some_and(|error| error.contains("agent:library-write")),
            "{missing_write_response:?}"
        );

        let write_only_claims = claims(
            "caller-a",
            "tenant-shared",
            &["agent:library-write"],
            "policy-test",
        );
        let missing_read = signed_agent_library_request(
            secret,
            &write_only_claims,
            505,
            AgentLibraryOp::Current {
                tenant_id: "tenant-shared".to_string(),
                agent_id: "agent-a".to_string(),
            },
            "missing-read-nonce",
            "missing-read-key",
        );
        let missing_read_response = crate::server::dispatch::dispatch(&state, missing_read).await;
        assert!(
            missing_read_response
                .error
                .as_deref()
                .is_some_and(|error| error.contains("agent:library-read")),
            "{missing_read_response:?}"
        );
    }
}
