//! Dispatch handler for `Method::SemanticIndex` -- the S1-S6 tiered semantic
//! ingestion queue.
//!
//! This is the entry point an external connector actually reaches. Everything
//! that carries authority is derived HERE from the verified request context and
//! never read from the request body: the acting agent, the attempt nonce, the
//! admission clock, the owner handle, and the authoritative SQL source page.
//!
//! One shape to notice, because it is the reason the pipeline is drivable at
//! all: a stage completion is not "tell the engine you finished". It is a
//! leased, fenced, predecessor-proved transition, and the durable store refuses
//! it if the predecessor proof is absent. The handler's job is to bind the
//! caller to a lease and hand the transition to the store; it never decides
//! whether a stage may complete.

#![cfg(all(feature = "ann-redb", feature = "query"))]

#[path = "semantic_index/binding.rs"]
mod binding;
#[path = "semantic_index/contracts.rs"]
mod contracts;
#[path = "semantic_index/dispatch.rs"]
mod dispatch;
#[path = "semantic_index/reads.rs"]
mod reads;
#[path = "semantic_index/source.rs"]
mod source;
#[path = "semantic_index/worker.rs"]
mod worker;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use eg_core::compute::semantic_index_service::SemanticIndexService;
use eg_types::semantic_index::{SemanticBinding, SemanticIndexOp};
use tokio::sync::RwLock;

use crate::protocol::{Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::semantic_index::{open_semantic_service, semantic_cursor_secret};
use crate::server::state::ServerState;

pub(super) struct SemanticIndexContext<'a> {
    pub(super) req_id: u64,
    pub(super) authority: &'a CarrierAuthority,
    pub(super) persist_dir: &'a Path,
    pub(super) service: Arc<SemanticIndexService>,
    pub(super) now_ms: u64,
}

/// Route one semantic-index operation.
pub(crate) async fn handle_semantic_index(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: Box<SemanticIndexOp>,
) -> Response {
    let op = *op;
    let authority = match authorize(req_id, verified, &op) {
        Ok(authority) => authority,
        Err(response) => return response,
    };
    let (persist_dir, service) = match open_service(state, req_id, &authority, &op).await {
        Ok(opened) => opened,
        Err(response) => return response,
    };
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let context = SemanticIndexContext {
        req_id,
        authority: &authority,
        persist_dir: &persist_dir,
        service,
        now_ms,
    };
    dispatch::handle(&context, op).await
}

fn authorize(
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: &SemanticIndexOp,
) -> Result<CarrierAuthority, Response> {
    if let Err(error) = op.validate() {
        return Err(Response::err(
            req_id,
            format!("semantic operation rejected: {error:?}"),
        ));
    }
    // Tenant isolation, checked ONCE rather than per arm, keeps a binding id
    // from becoming an execution grant across tenants.
    if op.tenant_id() != verified.tenant() {
        return Err(Response::err(
            req_id,
            "ACCESS_DENIED: semantic index tenant must match verified request tenant",
        ));
    }
    let authority =
        CarrierAuthority::from_verified(verified).map_err(|error| Response::err(req_id, error))?;
    // The carrier's own capability is checked in addition to the ledger gate.
    if op.is_mutation() && !authority.can_write() {
        return Err(Response::err(
            req_id,
            "ACCESS_DENIED: semantic index mutation requires kg:write",
        ));
    }
    if !op.is_mutation() && !authority.can_read() {
        return Err(Response::err(
            req_id,
            "ACCESS_DENIED: semantic index read requires kg:read",
        ));
    }
    Ok(authority)
}

async fn open_service(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    op: &SemanticIndexOp,
) -> Result<(PathBuf, Arc<SemanticIndexService>), Response> {
    let persist_dir: PathBuf = {
        let guard = state.read().await;
        match guard.persist_dir.as_deref() {
            Some(dir) => PathBuf::from(dir),
            None => {
                return Err(Response::err(
                    req_id,
                    "the semantic index requires a configured persist directory",
                ));
            }
        }
    };
    // The durable owner uses the carrier's opaque tenant scope, while the
    // wire tenant was checked above against the verified request tenant.
    let service = open_semantic_service(&persist_dir, authority.tenant_scope(), op.binding_id())
        .map_err(|error| Response::err(req_id, error))?;
    Ok((persist_dir, service))
}

/// Replace a draft's identity fields with the verified carrier's.
///
/// The same move `handle_agent_component` makes on a published component: the
/// caller SENDS these fields, so they are claims, and a claim that is used
/// unchecked is an assertion. Overwriting is stronger than comparing -- there is
/// no path on which a draft reaches admission carrying an identity the carrier
/// did not prove.
pub(super) fn stamp_draft_identity(
    draft: &mut eg_types::semantic_index::SemanticBindingDraft,
    authority: &CarrierAuthority,
) {
    draft.tenant_id = authority.tenant_scope().to_string();
    draft.actor_scope = authority.actor_scope().to_string();
    draft.effective_actor_scope = authority.agent_id().to_string();
}

/// A lease may only be presented by the consumer it was issued to.
pub(super) fn own_lease(
    lease: &eg_types::mutation_batch::MutationOutboxLease,
    authority: &CarrierAuthority,
) -> Result<(), String> {
    if lease.consumer != authority.agent_id() {
        return Err(
            "ACCESS_DENIED: semantic lease owner does not match verified carrier".to_string(),
        );
    }
    Ok(())
}

pub(super) fn read_port(
    persist_dir: &Path,
    authority: &CarrierAuthority,
) -> crate::server::semantic_index::AuthorizedSqlSourceReadPort {
    crate::server::semantic_index::AuthorizedSqlSourceReadPort::new(
        persist_dir.to_path_buf(),
        authority.clone(),
        semantic_cursor_secret(),
    )
}

/// Decode one opaque engine-minted cursor. Hex on the wire, bytes inside; the
/// bytes themselves are MAC-bound to the tenant that was issued them, so a
/// decoded cursor from another tenant fails its MAC rather than resuming.
pub(super) fn decode_cursor(cursor: Option<String>) -> Result<Option<Vec<u8>>, String> {
    match cursor {
        None => Ok(None),
        Some(cursor) => hex::decode(&cursor)
            .map(Some)
            .map_err(|_| "semantic cursor is not a valid opaque cursor".to_string()),
    }
}

pub(super) async fn current_binding(
    req_id: u64,
    service: &Arc<SemanticIndexService>,
) -> Result<SemanticBinding, Response> {
    let service = Arc::clone(service);
    match blocking(req_id, move || service.binding()).await {
        Ok(Some(binding)) => Ok(binding),
        Ok(None) => Err(Response::err(
            req_id,
            "semantic operation has no durable binding authority",
        )),
        Err(response) => Err(response),
    }
}

/// Marker for the one operation whose success carries no payload of its own.
pub(super) fn consumer_ack() -> bool {
    true
}

/// Run one synchronous owner operation off the async worker.
pub(super) async fn blocking<T, F>(req_id: u64, work: F) -> Result<T, Response>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, eg_core::compute::semantic_ann_codes::SemanticCodeError>
        + Send
        + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Response::err(req_id, error.to_string())),
        Err(_) => Err(Response::err(
            req_id,
            "semantic index operation could not be scheduled",
        )),
    }
}

pub(super) fn reply<M, T>(req_id: u64, result: Result<T, Response>) -> Response
where
    M: eg_types::result_contract::MethodResult<Body = T>,
    M::Encoding: eg_types::result_contract::EncodeRef<T>,
{
    match result {
        Ok(value) => typed_payload::<M, _>(req_id, &value),
        Err(response) => response,
    }
}

pub(super) fn typed_payload<M, T>(req_id: u64, value: &T) -> Response
where
    M: eg_types::result_contract::MethodResult<Body = T>,
    M::Encoding: eg_types::result_contract::EncodeRef<T>,
{
    match ResultPayload::of_ref::<M>(value) {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => Response::err(req_id, error),
    }
}
