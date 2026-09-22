//! `Method::ConnectorPack`: atomic connector MCP pack import and its admin
//! surface (RF-ADR-009).
//!
//! The op match below is the whole routing decision and it is EXHAUSTIVE, with
//! no catch-all: a new operation must be given an owner here rather than
//! inheriting one. Each arm's target is owned by a different package after S1,
//! which is why they are separate functions rather than one body.

use std::sync::Arc;
#[cfg(all(feature = "redb", feature = "blob"))]
use std::{hash::Hash, sync::OnceLock};

use tokio::sync::RwLock;

use crate::protocol::{Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

pub(crate) mod admin;
#[cfg(all(feature = "redb", feature = "blob"))]
mod import;
pub(crate) mod reconcile;
pub(crate) mod reproject;

/// Route one connector-pack operation to the handler that owns it.
pub(crate) async fn handle_connector_pack(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: eg_types::connector_pack::ConnectorPackOp,
) -> Response {
    use eg_types::connector_pack::ConnectorPackOp;

    match op {
        ConnectorPackOp::Status { request } => serve_status(state, req_id, verified, request).await,
        ConnectorPackOp::Import { request } => serve_import(state, req_id, verified, request).await,
        ConnectorPackOp::Bind { request } => {
            admin::serve_bind(state, req_id, verified, request).await
        }
        ConnectorPackOp::Unbind { request } => {
            admin::serve_unbind(state, req_id, verified, request).await
        }
        ConnectorPackOp::Retire { request } => {
            admin::serve_retire(state, req_id, verified, request).await
        }
        ConnectorPackOp::Reproject { request } => {
            reproject::serve(state, req_id, verified, request).await
        }
        ConnectorPackOp::ReconcileBodies { request } => {
            reconcile::serve(state, req_id, verified, request).await
        }
    }
}

#[cfg(feature = "redb")]
async fn agent_library_store(
    state: &Arc<RwLock<ServerState>>,
) -> Result<Arc<crate::server::persistence::agent_library::AgentLibraryStore>, String> {
    state.write().await.ensure_agent_library()
}

/// Read one connector's head, member counts, last receipt and warnings.
async fn serve_status(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::ConnectorPackStatusRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        return Response::err(req_id, "ConnectorPack requires the redb feature");
    }
    #[cfg(feature = "redb")]
    {
        if request.tenant_id != verified.tenant() {
            return Response::err(
                req_id,
                "ACCESS_DENIED: connector pack tenant must match verified request tenant",
            );
        }
        let store = match agent_library_store(state).await {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        };
        match store.connector_pack_status(&request.tenant_id, &request.connector) {
            Ok(mut status) => {
                if !verified.allows_action("agent:pack-control") {
                    status.importer = None;
                }
                Response::ok(
                    req_id,
                    ResultPayload::of_ref::<eg_types::result_contract::storage::ConnectorPackStatus>(
                        &status,
                    ),
                )
            }
            Err(error) => Response::err(req_id, error),
        }
    }
}

/// Validate the already-published archive and every concurrency/authority
/// precondition without publishing a partial catalog. The final atomic
/// component/body-holder writer is deliberately a hard prerequisite.
async fn serve_import(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: Box<eg_types::connector_pack::ConnectorPackImportRequest>,
) -> Response {
    #[cfg(not(all(feature = "redb", feature = "blob")))]
    {
        let _ = (state, verified, request);
        return Response::err(req_id, "ConnectorPack.import requires redb and blob");
    }
    #[cfg(all(feature = "redb", feature = "blob"))]
    {
        let _tenant_pack_guard = tenant_pack_lock(&request.context.tenant_id).await;
        match preflight_import(state, verified, &request).await {
            Ok(Preflight::Unchanged(result)) => Response::ok(
                req_id,
                ResultPayload::of_ref::<eg_types::result_contract::storage::ConnectorPackImport>(
                    &result,
                ),
            ),
            Ok(Preflight::Ready {
                store,
                blob,
                archive,
            }) => {
                match import::validate_and_commit(store, blob, verified, *request, archive).await {
                    Ok(result) => Response::ok(
                        req_id,
                        ResultPayload::of_ref::<
                            eg_types::result_contract::storage::ConnectorPackImport,
                        >(&result),
                    ),
                    Err(error) => Response::err(req_id, error),
                }
            }
            Err(error) => Response::err(req_id, error),
        }
    }
}

/// A bounded striped lock table serializes one tenant's head-read/body-copy/
/// catalog-CAS sequence without retaining attacker-controlled tenant strings.
#[cfg(all(feature = "redb", feature = "blob"))]
async fn tenant_pack_lock(tenant_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
    use std::hash::Hasher;

    const STRIPES: usize = 64;
    static LOCKS: OnceLock<Vec<Arc<tokio::sync::Mutex<()>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| {
        (0..STRIPES)
            .map(|_| Arc::new(tokio::sync::Mutex::new(())))
            .collect()
    });
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    tenant_id.hash(&mut hasher);
    let index = (hasher.finish() as usize) % STRIPES;
    locks[index].clone().lock_owned().await
}

#[cfg(all(feature = "redb", feature = "blob"))]
enum Preflight {
    Unchanged(eg_types::connector_pack::PackImportResult),
    Ready {
        store: Arc<crate::server::persistence::agent_library::AgentLibraryStore>,
        blob: Arc<crate::server::blob::BlobCursors>,
        archive: Vec<u8>,
    },
}

#[cfg(all(feature = "redb", feature = "blob"))]
async fn preflight_import(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    request: &eg_types::connector_pack::ConnectorPackImportRequest,
) -> Result<Preflight, String> {
    validate_import_identity(verified, request)?;
    let store = agent_library_store(state).await?;
    let status =
        store.connector_pack_status(&request.context.tenant_id, &request.index.connector)?;
    authorize_importer(&store, verified, &request.index.connector)?;
    if let Some(result) = replay_or_unchanged(request, &status)? {
        return Ok(Preflight::Unchanged(result));
    }
    let blob = import_blob_store(state, request).await?;
    let archive = read_import_archive(&blob, verified, request).await?;
    Ok(Preflight::Ready {
        store,
        blob,
        archive,
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_import_identity(
    verified: &VerifiedRequestContext,
    request: &eg_types::connector_pack::ConnectorPackImportRequest,
) -> Result<(), String> {
    if request.context.tenant_id != verified.tenant() {
        return Err(
            "ACCESS_DENIED: connector pack tenant must match verified request tenant".to_string(),
        );
    }
    let expected_key = format!(
        "connector-pack:{}:import:{}:{}",
        request.index.connector.as_str(),
        request.index.pack_digest,
        request
            .expected_head
            .as_ref()
            .map_or(0, |head| head.binding_revision)
    );
    if verified.idempotency_key() != expected_key {
        return Err(
            "IDEMPOTENCY_CONFLICT: ConnectorPack.import requires its deterministic operation key"
                .to_string(),
        );
    }
    let computed = eg_types::connector_pack::digest::pack_digest(&request.index)?;
    if computed != request.index.pack_digest {
        return Err("PACK_DIGEST_MISMATCH: connector pack digest is not canonical".to_string());
    }
    if request.index.schema_version != eg_types::connector_pack::CONNECTOR_PACK_SCHEMA_VERSION {
        return Err("MALFORMED_INDEX: unsupported connector pack schema version".to_string());
    }
    if request.index.archive.length > eg_types::connector_pack::MAX_PACK_ARCHIVE_BYTES {
        return Err("PACK_TOO_LARGE: connector pack archive exceeds the served bound".to_string());
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_or_unchanged(
    request: &eg_types::connector_pack::ConnectorPackImportRequest,
    status: &eg_types::connector_pack::ConnectorPackStatus,
) -> Result<Option<eg_types::connector_pack::PackImportResult>, String> {
    let actual_head = status
        .head
        .as_ref()
        .map(|head| eg_types::connector_pack::PackHeadRef {
            binding_revision: head.binding_revision,
            pack_digest: head.pack_digest,
        });
    if status
        .head
        .as_ref()
        .is_some_and(|head| head.pack_digest == request.index.pack_digest)
    {
        if let (Some(expected), Some(receipt), Some(head)) = (
            request.expected_head.as_ref(),
            status.last_receipt.clone(),
            status.head.as_ref(),
        ) {
            if expected.binding_revision.checked_add(1) == Some(head.binding_revision)
                && receipt.pack_digest == request.index.pack_digest
            {
                return Ok(Some(eg_types::connector_pack::PackImportResult::Imported {
                    receipt: Box::new(receipt),
                }));
            }
        }
        if actual_head == request.expected_head {
            return Ok(Some(
                eg_types::connector_pack::PackImportResult::Unchanged {
                    pack_digest: request.index.pack_digest,
                    binding_revision: status.head.as_ref().unwrap().binding_revision,
                },
            ));
        }
    }
    if actual_head != request.expected_head {
        return Err("PACK_HEAD_CONFLICT: connector pack head changed".to_string());
    }
    Ok(None)
}

#[cfg(all(feature = "redb", feature = "blob"))]
async fn import_blob_store(
    state: &Arc<RwLock<ServerState>>,
    _request: &eg_types::connector_pack::ConnectorPackImportRequest,
) -> Result<Arc<crate::server::blob::BlobCursors>, String> {
    state
        .read()
        .await
        .blob
        .clone()
        .ok_or_else(|| "ARCHIVE_MISSING: Blob substrate disabled".to_string())
}

#[cfg(all(feature = "redb", feature = "blob"))]
async fn read_import_archive(
    blob: &Arc<crate::server::blob::BlobCursors>,
    verified: &VerifiedRequestContext,
    request: &eg_types::connector_pack::ConnectorPackImportRequest,
) -> Result<Vec<u8>, String> {
    use sha2::{Digest, Sha256};

    let carrier = CarrierAuthority::from_verified(verified)?;
    let digest = request.index.archive.blob_digest.clone();
    let owner = carrier.owner_scope().to_string();
    let expected_length = request.index.archive.length;
    let expected_sha = request.index.archive.sha256;
    let archive_blob = Arc::clone(&blob);
    let archive = tokio::task::spawn_blocking(move || {
        let manifest = archive_blob
            .store
            .get_manifest(&digest)?
            .filter(|manifest| manifest.owner_scope == owner)
            .ok_or_else(|| "ARCHIVE_MISSING: unknown blob digest".to_string())?;
        if manifest.len != expected_length {
            return Err("ARCHIVE_DIGEST_MISMATCH: archive length differs".to_string());
        }
        let mut hasher = Sha256::new();
        let mut archive = Vec::with_capacity(expected_length as usize);
        for chunk in manifest.chunks {
            let bytes = archive_blob
                .store
                .get_chunk(&chunk)?
                .ok_or_else(|| "ARCHIVE_MISSING: archive chunk is missing".to_string())?;
            hasher.update(&bytes);
            archive.extend_from_slice(&bytes);
        }
        let actual = eg_types::contract::Digest256::from_bytes(hasher.finalize().into());
        if actual != expected_sha {
            return Err("ARCHIVE_DIGEST_MISMATCH: archive SHA-256 differs".to_string());
        }
        Ok::<_, String>(archive)
    })
    .await
    .map_err(|error| format!("archive validation task failed: {error}"))??;
    Ok(archive)
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn authorize_importer(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    verified: &VerifiedRequestContext,
    connector: &eg_types::contract::ResourceId,
) -> Result<(), String> {
    let principal = verified.principal_persistence_id();
    let configured = store
        .connector_pack_importer(verified.tenant(), connector)?
        .or_else(|| bootstrap_importer(connector.as_str()));
    match configured {
        Some(importer) if importer == principal => Ok(()),
        _ => Err("IMPORTER_MISMATCH: connector is not bound to this principal".to_string()),
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn bootstrap_importer(connector: &str) -> Option<String> {
    let configured = std::env::var("EPISTEMIC_GRAPH_CONNECTOR_PACK_IMPORTERS").ok()?;
    let mut fallback = None;
    for entry in configured.split(',') {
        let (name, principal) = entry.trim().split_once('=')?;
        if principal.trim().is_empty() {
            continue;
        }
        if name.trim() == connector {
            return Some(principal.trim().to_string());
        }
        if name.trim() == "*" {
            fallback = Some(principal.trim().to_string());
        }
    }
    fallback
}
