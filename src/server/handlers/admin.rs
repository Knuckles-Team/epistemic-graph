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
use crate::mutation_batch::{MutationBatch, MutationDomain, MutationSurface};
use crate::protocol::{Method, Response};
use crate::server::state::ServerState;

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
            let saga = match begin_admin_saga(
                backend,
                req_id,
                caller,
                &original_method,
                MutationDomain::MultiGraph,
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
            |catalog| catalog.assign(&crate::persist::sanitize(&graph), shard, node),
        )),
        Method::CatalogReassign { graph, shard } => Ok(catalog_saga(
            req_id,
            caller,
            backend,
            &original_method,
            |catalog| catalog.reassign(&crate::persist::sanitize(&graph), shard),
        )),
        Method::CatalogRemove { graph } => Ok(catalog_saga(
            req_id,
            caller,
            backend,
            &original_method,
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
            let saga = match begin_admin_saga(
                backend,
                req_id,
                caller,
                &original_method,
                MutationDomain::MultiGraph,
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
            match backend.backup(
                &stage,
                env!("CARGO_PKG_VERSION"),
                ts,
                label.as_deref().unwrap_or(""),
                &extra_stores,
            ) {
                Ok(r) => {
                    if let Err(error) = std::fs::rename(&stage, &destination) {
                        cleanup_backup_stage(&stage);
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
                            "graphs": r.graphs,
                            "nodes": r.nodes,
                            "edges": r.edges,
                            "ledger": r.ledger,
                            "semantic": r.semantic,
                            "audit": r.audit,
                            "auxiliary": r.auxiliary,
                            "global": r.global,
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
            let saga = match begin_admin_saga(
                backend,
                req_id,
                caller,
                &original_method,
                MutationDomain::ControlPlane,
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
            if stage.exists() {
                match std::fs::symlink_metadata(&stage) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Ok(Response::err(
                            req_id,
                            "staged restore target is not an engine-owned directory",
                        ));
                    }
                    Ok(metadata) if metadata.is_dir() => {
                        if let Err(error) = std::fs::remove_dir_all(&stage) {
                            return Ok(Response::err(
                                req_id,
                                format!(
                                    "Restore retry cleanup failed; error_ref={}",
                                    opaque_ref(&error.to_string())
                                ),
                            ));
                        }
                    }
                    Ok(_) => {
                        return Ok(Response::err(
                            req_id,
                            "staged restore target is not an engine-owned directory",
                        ));
                    }
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
    domain: MutationDomain,
) -> Result<AdminSaga, String> {
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "cluster-admin",
        "cluster-admin",
        req_id,
        method,
    );
    begin_named_admin_saga(backend, req_id, caller, method, domain, &batch_id)
}

/// Begin or resume a coordinator whose identity spans request retries. Callers
/// supply only an already-opaque key; transaction/session ids and payloads are
/// never copied into the admin ledger.
#[cfg(feature = "redb")]
pub(crate) fn begin_named_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    method: &Method,
    domain: MutationDomain,
    batch_id: &str,
) -> Result<AdminSaga, String> {
    let db = backend.admin_mutation_store();
    let expected = eg_mutation_store::version(db, "native", "cluster-admin")?;
    let now = crate::server::dispatch::authoritative_now_ms();
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: req_id,
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
    let replayed = match eg_mutation_store::prepare_saga(db, &batch, now)? {
        eg_mutation_store::SagaBegin::Committed(record) => {
            let bytes = record
                .result_msgpack
                .as_deref()
                .ok_or_else(|| "committed admin saga has no result".to_string())?;
            Some(rmp_serde::from_slice(bytes).map_err(|error| error.to_string())?)
        }
        eg_mutation_store::SagaBegin::Execute | eg_mutation_store::SagaBegin::Resume(_) => None,
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
    pub(crate) domain: MutationDomain,
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
    let AdminSagaPayload {
        domain,
        batch_id,
        event_type,
        payload_digest,
        encrypted_payload,
    } = payload;
    let db = backend.admin_mutation_store();
    let expected = eg_mutation_store::version(db, "native", "cluster-admin")?;
    let now = crate::server::dispatch::authoritative_now_ms();
    let batch = crate::server::mutation_batch::compile_opaque_digest(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: req_id,
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
    let replayed = match eg_mutation_store::prepare_saga_with_private_payload(
        db,
        &batch,
        now,
        Some(encrypted_payload),
    )? {
        eg_mutation_store::SagaBegin::Committed(record) => {
            let bytes = record
                .result_msgpack
                .as_deref()
                .ok_or_else(|| "committed admin saga has no result".to_string())?;
            Some(rmp_serde::from_slice(bytes).map_err(|error| error.to_string())?)
        }
        eg_mutation_store::SagaBegin::Execute | eg_mutation_store::SagaBegin::Resume(_) => None,
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
    let Some(record) = eg_mutation_store::read_record(backend.admin_mutation_store(), batch_id)?
    else {
        return Ok(None);
    };
    record.batch.validate()?;
    if record.batch.batch_id != batch_id || record.batch.idempotency_key != batch_id {
        return Err("coordinator receipt identity is corrupt".to_string());
    }
    if record.batch.context.principal != expected_principal {
        return Err("coordinator receipt does not match caller scope".to_string());
    }
    let replayed = match record.status {
        crate::mutation_batch::MutationBatchStatus::Prepared => None,
        crate::mutation_batch::MutationBatchStatus::Committed => {
            let bytes = record
                .result_msgpack
                .as_deref()
                .ok_or_else(|| "committed admin saga has no result".to_string())?;
            Some(rmp_serde::from_slice(bytes).map_err(|error| error.to_string())?)
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
    let Some(record) = eg_mutation_store::read_record(backend.admin_mutation_store(), batch_id)?
    else {
        return Ok(None);
    };
    if record.status != crate::mutation_batch::MutationBatchStatus::Committed {
        return Ok(None);
    }
    if record.batch.context.principal != expected_principal {
        return Err("committed coordinator receipt does not match caller scope".to_string());
    }
    let bytes = record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed admin saga has no result".to_string())?;
    rmp_serde::from_slice(bytes)
        .map(Some)
        .map_err(|error| error.to_string())
}

#[cfg(feature = "redb")]
pub(crate) fn finish_admin_saga(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch: MutationBatch,
    committed_at_ms: u64,
    result: crate::protocol::ResultPayload,
) -> Result<crate::protocol::ResultPayload, String> {
    let encoded = rmp_serde::to_vec_named(&result).map_err(|error| error.to_string())?;
    let (record, replayed) = eg_mutation_store::commit_saga(
        backend.admin_mutation_store(),
        &batch,
        encoded,
        committed_at_ms,
    )?;
    if replayed {
        let bytes = record
            .result_msgpack
            .as_deref()
            .ok_or_else(|| "committed admin saga has no result".to_string())?;
        rmp_serde::from_slice(bytes).map_err(|error| error.to_string())
    } else {
        Ok(result)
    }
}

#[cfg(feature = "redb")]
fn catalog_saga(
    req_id: u64,
    caller: Option<&str>,
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    method: &Method,
    apply: impl FnOnce(&crate::server::persistence::tenant_catalog::TenantCatalog) -> Result<(), String>,
) -> Response {
    let saga = match begin_admin_saga(
        backend,
        req_id,
        caller,
        method,
        MutationDomain::ControlPlane,
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
