//! Sweep engine-owned component bodies no revision holds any more (PA3).
//!
//! An import copies new bodies into the engine-owned Blob CAS (each under the
//! set-like holder `pack:<tenant>:<sha256>`) BEFORE its one Agent Library
//! commit. If that commit then fails -- a head conflict, a crash, a stale plan
//! -- the copied bodies are held by nobody the catalog knows about. This
//! reconciler compares the tenant's engine pack-body holders against the body
//! holder rows of an Agent Library snapshot and releases every holder that no
//! component revision names, so the next `BlobGc` can reclaim the bytes.
//!
//! It runs under the tenant's pack lock, the same lock an import holds from
//! its body copy through its commit, so no in-flight import's bodies can be
//! mistaken for orphans. A held body always keeps its holder, so `BlobGc` at
//! any moment cannot remove a body a committed revision names.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

/// How long one sweep's verdict stays useful: the reconciler runs on its own
/// at most this often per tenant stripe, and reports this horizon.
#[cfg(all(feature = "redb", feature = "blob"))]
pub(super) const RECONCILE_INTERVAL_MS: u64 = 60 * 60 * 1000;

/// Run the orphan-body reconciler now.
pub(crate) async fn serve(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::ConnectorPackReconcileRequest,
) -> Response {
    #[cfg(not(all(feature = "redb", feature = "blob")))]
    {
        let _ = (state, verified, request);
        return Response::err(
            req_id,
            "ConnectorPack.reconcile_bodies requires redb and blob",
        );
    }
    #[cfg(all(feature = "redb", feature = "blob"))]
    {
        if request.context.tenant_id != verified.tenant() {
            return Response::err(
                req_id,
                "ACCESS_DENIED: connector pack tenant must match verified request tenant",
            );
        }
        let _tenant_pack_guard = super::tenant_pack_lock(verified.tenant()).await;
        let method = eg_types::protocol::Method::ConnectorPack {
            op: Box::new(eg_types::connector_pack::ConnectorPackOp::ReconcileBodies { request }),
        };
        match reconcile_locked(state, req_id, verified, &method).await {
            Ok(report) => Response::ok(
                req_id,
                crate::protocol::ResultPayload::of_ref::<
                    eg_types::result_contract::storage::ConnectorPackReconcileBodies,
                >(&report),
            ),
            Err(error) => Response::err(req_id, error),
        }
    }
}

/// Run the reconciler at the start of an import when this tenant's stripe
/// has not swept within [`RECONCILE_INTERVAL_MS`]. The caller holds the pack
/// lock. A failed sweep never fails the import that triggered it -- the bodies
/// it would have released stay held, which costs disk, not correctness -- but
/// it is logged with its cause and retried by the next due import.
#[cfg(all(feature = "redb", feature = "blob"))]
pub(super) async fn reconcile_if_due(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    context: &eg_types::agent_library::AgentLibraryMutationContext,
) {
    use std::sync::atomic::{AtomicU64, Ordering};

    static LAST_RUN_MS: [AtomicU64; super::PACK_STRIPES] =
        [const { AtomicU64::new(0) }; super::PACK_STRIPES];
    let clock = &LAST_RUN_MS[super::tenant_stripe(verified.tenant())];
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    if now_ms.saturating_sub(clock.load(Ordering::Acquire)) < RECONCILE_INTERVAL_MS {
        return;
    }
    let method = eg_types::protocol::Method::ConnectorPack {
        op: Box::new(eg_types::connector_pack::ConnectorPackOp::ReconcileBodies {
            request: eg_types::connector_pack::ConnectorPackReconcileRequest {
                context: context.clone(),
            },
        }),
    };
    match reconcile_locked(state, req_id, verified, &method).await {
        Ok(_) => clock.store(now_ms, Ordering::Release),
        Err(error) => tracing::warn!(
            code = "CONNECTOR_PACK_RECONCILE_FAILED",
            %error,
            "connector pack body reconcile before import failed"
        ),
    }
}

/// Reconcile one tenant's engine pack bodies. The caller holds the tenant's
/// pack lock; `method` is the request the blob batch is admitted under.
#[cfg(all(feature = "redb", feature = "blob"))]
pub(super) async fn reconcile_locked(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    method: &eg_types::protocol::Method,
) -> Result<eg_types::connector_pack::PackBodyReconcileReport, String> {
    let store = super::agent_library_store(state).await?;
    let blob = state
        .read()
        .await
        .blob
        .clone()
        .ok_or_else(|| "BODY_MISSING: Blob substrate disabled".to_string())?;
    let authority = crate::server::access::CarrierAuthority::from_verified(verified)?;
    let (batch, now_ms) = crate::server::handlers::blob::compile_blob_batch(
        blob.store.as_ref(),
        req_id,
        &authority,
        method,
    )?;
    let tenant_id = verified.tenant().to_string();
    tokio::task::spawn_blocking(move || {
        let held = store.connector_pack_held_bodies(&tenant_id)?;
        let request = live_holders(&tenant_id, &held)?;
        let stats = blob
            .store
            .reconcile_holders_batch(&request, &batch, now_ms)?;
        Ok(eg_types::connector_pack::PackBodyReconcileReport {
            schema_version: eg_types::connector_pack::CONNECTOR_PACK_SCHEMA_VERSION,
            tenant_id,
            scanned: held.len() as u64,
            orphaned: stats.holders_released,
            released: stats.references_released,
            next_run_after_ms: now_ms.saturating_add(RECONCILE_INTERVAL_MS),
        })
    })
    .await
    .map_err(|error| format!("connector pack reconcile task failed: {error}"))?
}

/// The reconcile request for one tenant: its whole `pack:<tenant>` holder
/// namespace, with every body a committed revision holds marked live.
#[cfg(all(feature = "redb", feature = "blob"))]
pub(super) fn live_holders(
    tenant_id: &str,
    held: &std::collections::BTreeSet<eg_types::contract::Digest256>,
) -> Result<crate::server::blob::store::HolderReconcile, String> {
    use crate::server::blob::engine_bodies::{engine_body_holder, engine_body_holder_namespace};
    use crate::server::blob::store::{HolderId, HolderNamespace, HolderReconcile};

    let namespace = HolderNamespace::new(&engine_body_holder_namespace(tenant_id))?;
    let live = held
        .iter()
        .map(|body| HolderId::new(&engine_body_holder(tenant_id, body)))
        .collect::<Result<Vec<_>, _>>()?;
    HolderReconcile::new(namespace, live)
}
