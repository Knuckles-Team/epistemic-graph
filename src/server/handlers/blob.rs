//! Streamed content-addressed BLOB handler (CONCEPT:EG-KG.storage.blob-namespace).
//!
//! Owns the `Blob*` methods. These are STATEFUL (they drive
//! [`ServerState::blob`](crate::server::ServerState) — the CAS chunk store + the
//! open upload/fetch cursors), so like the OCC-txn handler they take `state`. The
//! CAS operations touch redb (an fsync per durable commit), so every store call
//! runs on the BLOCKING pool — never the async reactor — so a large media transfer
//! cannot stall unrelated requests.
//!
//! The blob substrate is NOT graph-scoped: a content-addressed blob is keyed by
//! its digest, not a graph, and the same blob may be referenced by `:Media` nodes
//! across graphs. So these route at the top of dispatch (next to the txn methods),
//! before the per-graph chain.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::state::ServerState;
use crate::mutation_batch::{DurabilityDomain, MutationBatch, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::blob::{store, BlobCursors};
use eg_types::contract::Nonce;
use eg_types::result_contract::storage as results;

/// Handle the blob methods. Returns `Err(method)` for any non-blob method so the
/// dispatch chain falls through (routing convention). When the engine is built
/// `--features blob` but the substrate is disabled (no persist dir), the methods
/// return an explicit error rather than panicking.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: Method,
) -> Result<Response, Method> {
    // Pull the cursors handle once (cheap clone of the Arc) so we don't hold the
    // ServerState read lock across the blocking store calls.
    let resolved = resolve_cursors(state, req_id, &method).await;
    let Ok(cursors) = resolved else {
        return resolved.err().unwrap();
    };

    let original_method = method.clone();
    match method {
        Method::BlobBegin { chunk_size } => {
            handle_blob_begin(
                &cursors,
                req_id,
                authority,
                attempt_nonce,
                &original_method,
                chunk_size,
            )
            .await
        }

        Method::BlobChunkPut { cursor, data } => {
            handle_blob_chunk_put(
                &cursors,
                req_id,
                authority,
                attempt_nonce,
                &original_method,
                cursor,
                data,
            )
            .await
        }

        Method::BlobCommit { cursor } => {
            handle_blob_commit(
                &cursors,
                req_id,
                authority,
                attempt_nonce,
                &original_method,
                cursor,
            )
            .await
        }

        Method::BlobFetchBegin { digest } => {
            handle_blob_fetch_begin(&cursors, req_id, authority, digest).await
        }

        Method::BlobChunkGet { cursor, idx } => {
            handle_blob_chunk_get(&cursors, req_id, authority, cursor, idx).await
        }

        Method::BlobFetchEnd { cursor } => {
            Ok(handle_blob_fetch_end(&cursors, req_id, authority, cursor))
        }

        Method::BlobRef { digest } => {
            handle_blob_ref(
                &cursors,
                req_id,
                authority,
                attempt_nonce,
                &original_method,
                digest,
            )
            .await
        }

        Method::BlobUnref { digest } => {
            handle_blob_unref(
                &cursors,
                req_id,
                authority,
                attempt_nonce,
                &original_method,
                digest,
            )
            .await
        }

        Method::BlobGc => {
            handle_blob_gc(&cursors, req_id, authority, attempt_nonce, &original_method).await
        }

        other => Err(other),
    }
}

/// Resolve the blob CAS cursors handle for `method`, or the final routing/error
/// outcome when the substrate is disabled (no persist dir configured): a blob
/// method gets an explicit error response, any other method falls through the
/// dispatch chain via `Err(method)` (routing convention).
async fn resolve_cursors(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: &Method,
) -> Result<Arc<BlobCursors>, Result<Response, Method>> {
    let s = state.read().await;
    match &s.blob {
        Some(cursors) => Ok(cursors.clone()),
        None if is_blob_method(method) => Err(Ok(Response::err(
            req_id,
            "Blob substrate disabled (no persist dir configured)",
        ))),
        None => Err(Err(method.clone())),
    }
}

/// Reserve a never-used cursor id and compile the durable `BlobBegin` batch
/// against it, in one place so `handle_blob_begin` sees ONE fallible step
/// rather than three.
///
/// BUG A2 (2026-08-12): `BlobBegin`'s `Method` payload carries no upload
/// identity of its own (it is the call that MINTS one), so the durable
/// idempotency key must use the freshly allocated cursor id, never `req_id`.
/// See `compile_blob_batch_at`'s doc for the full mechanism.
fn prepare_begin_upload(
    cursors: &BlobCursors,
    authority: &CarrierAuthority,
    method: &Method,
    attempt_nonce: Option<Nonce>,
) -> Result<(u64, MutationBatch, u64), String> {
    let proposed = cursors.allocate_upload_id()?;
    let expected = cursors.store.mutation_version(
        authority.tenant_scope(),
        &authority.namespace("blob-cas", "control"),
    )?;
    let now = crate::server::dispatch::authoritative_now_ms();
    let (batch, now) = compile_blob_batch_at_with_nonce(
        cursors.store.as_ref(),
        proposed,
        authority,
        method,
        expected,
        now,
        attempt_nonce,
    )?;
    Ok((proposed, batch, now))
}

async fn handle_blob_begin(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
    chunk_size: u32,
) -> Result<Response, Method> {
    let chunk_size = if chunk_size == 0 {
        store::DEFAULT_CHUNK_SIZE as u32
    } else {
        chunk_size
    };
    let (proposed, batch, now) =
        match prepare_begin_upload(cursors, authority, method, attempt_nonce) {
            Ok(prepared) => prepared,
            Err(error) => return Ok(Response::err(req_id, error)),
        };
    let store = cursors.store.clone();
    let owner_scope = authority.owner_scope().to_string();
    let committed = run_blocking(req_id, move || {
        store.begin_upload_batch(proposed, chunk_size, &owner_scope, &batch, now)
    })
    .await;
    let id = match committed {
        Ok(Ok(id)) => id,
        Ok(Err(error)) => return Ok(Response::err(req_id, error)),
        Err(response) => return Ok(response),
    };
    if let Err(error) = restore_committed_upload(cursors, id) {
        return Ok(Response::err(req_id, error));
    }
    Ok(Response::ok(
        req_id,
        ResultPayload::scalar::<results::BlobBegin>(id),
    ))
}

async fn handle_blob_chunk_put(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
    cursor: u64,
    data: Vec<u8>,
) -> Result<Response, Method> {
    if let Err(error) = ensure_upload_owner(cursors, cursor, authority.owner_scope()) {
        return Ok(Response::err(req_id, error));
    }
    let (batch, now) = match compile_blob_batch_with_nonce(
        cursors.store.as_ref(),
        req_id,
        authority,
        method,
        attempt_nonce,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let store = cursors.store.clone();
    let put = run_blocking(req_id, move || {
        store.put_upload_chunk_batch(cursor, &data, &batch, now)
    })
    .await;
    let (_digest, count) = match put {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => return Ok(Response::err(req_id, error)),
        Err(response) => return Ok(response),
    };
    Ok(chunk_put_response(cursors, req_id, cursor, count))
}

async fn handle_blob_commit(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
    cursor: u64,
) -> Result<Response, Method> {
    if let Err(error) = ensure_upload_owner(cursors, cursor, authority.owner_scope()) {
        return Ok(Response::err(req_id, error));
    }
    let (batch, now) = match compile_blob_batch_with_nonce(
        cursors.store.as_ref(),
        req_id,
        authority,
        method,
        attempt_nonce,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let store = cursors.store.clone();
    let committed = run_blocking(req_id, move || {
        store.commit_upload_batch(cursor, &batch, now)
    })
    .await;
    match committed {
        Ok(Ok(digest)) => {
            cursors.finish_upload(cursor);
            Ok(Response::ok(
                req_id,
                ResultPayload::scalar::<results::BlobCommit>(digest),
            ))
        }
        Ok(Err(error)) => Ok(Response::err(req_id, error)),
        Err(response) => Ok(response),
    }
}

async fn handle_blob_fetch_begin(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    digest: String,
) -> Result<Response, Method> {
    let store = cursors.store.clone();
    let manifest = run_blocking(req_id, move || store.get_manifest(&digest)).await;
    match manifest {
        Ok(Ok(Some(manifest))) if manifest.owner_scope == authority.owner_scope() => {
            let (cursor, chunks) = cursors.open_fetch(manifest);
            Ok(Response::ok(
                req_id,
                // A two-element id list keeps the wire simple; the client splits it.
                ResultPayload::of_ref::<results::BlobFetchBegin>(&(cursor, chunks)),
            ))
        }
        Ok(Ok(Some(_))) | Ok(Ok(None)) => {
            crate::metrics::access_denied();
            Ok(Response::err(req_id, "unknown blob digest"))
        }
        Ok(Err(error)) => Ok(Response::err(req_id, error)),
        Err(response) => Ok(response),
    }
}

async fn handle_blob_chunk_get(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    cursor: u64,
    idx: u32,
) -> Result<Response, Method> {
    if let Err(error) = cursors.authorize_fetch(cursor, authority.owner_scope()) {
        return Ok(Response::err(req_id, error));
    }
    let digest = match cursors.fetch_chunk_digest(cursor, idx) {
        Ok(digest) => digest,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let store = cursors.store.clone();
    let fetched = run_blocking(req_id, move || store.get_chunk(&digest)).await;
    match fetched {
        // A chunk is arbitrary binary, not a packed map. Wrap it as a Raw
        // MessagePack `bin` so the client recovers the exact bytes.
        Ok(Ok(Some(bytes))) => Ok(Response::ok(
            req_id,
            ResultPayload::of_dynamic::<results::BlobChunkGet, _>(&serde_bytes::Bytes::new(&bytes)),
        )),
        Ok(Ok(None)) => Ok(Response::err(req_id, "chunk missing from CAS")),
        Ok(Err(error)) => Ok(Response::err(req_id, error)),
        Err(response) => Ok(response),
    }
}

// `Response`, not `Result<Response, Method>`: this handler is only ever reached for
// `Method::BlobFetchEnd` (already matched by the caller) and never falls through the
// dispatch chain, so an `Err(Method)` arm can never be constructed here. Clippy's
// `result_large_err` correctly flags a `Method` (an enum whose largest variant carries
// full request payloads) in an `Err` position that is dead weight on every call.
fn handle_blob_fetch_end(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    cursor: u64,
) -> Response {
    if let Err(error) = cursors.authorize_fetch(cursor, authority.owner_scope()) {
        return Response::err(req_id, error);
    }
    cursors.close_fetch(cursor);
    Response::ok(req_id, ResultPayload::scalar::<results::BlobFetchEnd>(true))
}

async fn handle_blob_ref(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
    digest: String,
) -> Result<Response, Method> {
    handle_blob_ref_op::<results::BlobRef>(
        cursors,
        req_id,
        authority,
        attempt_nonce,
        method,
        digest,
        store::HolderChange::owner_acquire,
    )
    .await
}

async fn handle_blob_unref(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
    digest: String,
) -> Result<Response, Method> {
    handle_blob_ref_op::<results::BlobUnref>(
        cursors,
        req_id,
        authority,
        attempt_nonce,
        method,
        digest,
        store::HolderChange::owner_release,
    )
    .await
}

/// Validate ownership, resolve the caller's named holder change, and compile
/// the durable batch for it — one fallible step for `handle_blob_ref_op`
/// instead of three.
///
/// The caller's reference is held under its own owner scope, so a retry or a
/// replay of the same reference is one holder row, never a second count.
fn prepare_blob_ref_op(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
    digest: &str,
    change: fn(&str, &str) -> Result<store::HolderChange, String>,
) -> Result<(store::HolderChange, MutationBatch, u64), String> {
    ensure_blob_owner(cursors, digest, authority.owner_scope())?;
    let change = change(digest, authority.owner_scope())?;
    let (batch, now) = compile_blob_batch_with_nonce(
        cursors.store.as_ref(),
        req_id,
        authority,
        method,
        attempt_nonce,
    )?;
    Ok((change, batch, now))
}

async fn handle_blob_ref_op<M>(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
    digest: String,
    change: fn(&str, &str) -> Result<store::HolderChange, String>,
) -> Result<Response, Method>
where
    M: eg_types::result_contract::MethodResult<Body = u64>,
    M::Encoding: eg_types::result_contract::EncodeScalar<u64>,
{
    let (change, batch, now) = match prepare_blob_ref_op(
        cursors,
        req_id,
        authority,
        attempt_nonce,
        method,
        &digest,
        change,
    ) {
        Ok(prepared) => prepared,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let store = cursors.store.clone();
    ref_op::<M, _>(req_id, move || {
        store
            .holder_batch(&change, &batch, now)
            .map(|outcome| outcome.holders)
    })
    .await
}

async fn handle_blob_gc(
    cursors: &BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    method: &Method,
) -> Result<Response, Method> {
    if let Err(error) = authority.require_admin("blob garbage collection") {
        return Ok(Response::err(req_id, error));
    }
    let (batch, now) = match compile_blob_batch_with_nonce(
        cursors.store.as_ref(),
        req_id,
        authority,
        method,
        attempt_nonce,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let store = cursors.store.clone();
    let request = store::SweepRequest::new(store::GcOwnerScope::AllOwners, cursors.retention());
    let swept = run_blocking(req_id, move || store.sweep_batch(&request, &batch, now)).await;
    match swept {
        Ok(Ok(stats)) => Ok(Response::ok(
            req_id,
            ResultPayload::of_ref::<results::BlobGc>(&(
                stats.blobs_reclaimed,
                stats.chunks_reclaimed,
            )),
        )),
        Ok(Err(error)) => Ok(Response::err(req_id, error)),
        Err(response) => Ok(response),
    }
}

fn restore_committed_upload(cursors: &BlobCursors, id: u64) -> Result<(), String> {
    match cursors.store.load_upload(id)? {
        Some(manifest) => cursors.restore_upload_manifest(id, manifest),
        None => Err("committed blob upload cursor is missing".to_string()),
    }
}

fn chunk_put_response(cursors: &BlobCursors, req_id: u64, cursor: u64, count: u32) -> Response {
    match cursors.store.load_upload(cursor) {
        Ok(Some(manifest)) => match cursors.restore_upload_manifest(cursor, manifest) {
            Ok(()) => Response::ok(
                req_id,
                ResultPayload::scalar::<results::BlobChunkPut>(count as u64),
            ),
            Err(error) => Response::err(req_id, error),
        },
        Ok(None) => Response::err(req_id, "durable upload cursor is missing"),
        Err(error) => Response::err(req_id, error),
    }
}

/// Bind direct/replayed chunk operations to the durable upload owner. A process
/// restart may empty the serving cursor map, so recover only a manifest whose
/// durable owner exactly matches this verified caller.
fn ensure_upload_owner(
    cursors: &BlobCursors,
    cursor: u64,
    owner_scope: &str,
) -> Result<(), String> {
    if cursors.authorize_upload(cursor, owner_scope).is_ok() {
        return Ok(());
    }
    let manifest = cursors
        .store
        .load_upload(cursor)?
        .filter(|manifest| manifest.owner_scope == owner_scope)
        .ok_or_else(|| "unknown upload cursor".to_string())?;
    cursors.restore_upload_manifest(cursor, manifest)?;
    cursors.authorize_upload(cursor, owner_scope)
}

fn ensure_blob_owner(cursors: &BlobCursors, digest: &str, owner_scope: &str) -> Result<(), String> {
    match cursors.store.get_manifest(digest)? {
        Some(manifest) if manifest.owner_scope == owner_scope => Ok(()),
        _ => {
            crate::metrics::access_denied();
            Err("unknown blob digest".to_string())
        }
    }
}

pub(crate) fn compile_blob_batch(
    store: &dyn store::ChunkStore,
    req_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
) -> Result<(MutationBatch, u64), String> {
    compile_blob_batch_with_nonce(store, req_id, authority, method, None)
}

fn compile_blob_batch_with_nonce(
    store: &dyn store::ChunkStore,
    req_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
    attempt_nonce: Option<Nonce>,
) -> Result<(MutationBatch, u64), String> {
    let scope = authority.namespace("blob-cas", "control");
    let expected = store.mutation_version(authority.tenant_scope(), &scope)?;
    let now = crate::server::dispatch::authoritative_now_ms();
    compile_blob_batch_at_with_nonce(
        store,
        req_id,
        authority,
        method,
        expected,
        now,
        attempt_nonce,
    )
}

/// Rebuild an exact blob child after a coordinator restart. The expected native
/// version and timestamp came from the authenticated encrypted parent plan; they
/// are never re-sampled after an acknowledgement-lost child commit.
///
/// `identity_id` is folded into the durable idempotency key
/// (`mutation_batch::opaque_request_key`) alongside `authority`'s tenant/actor
/// scope and `method`'s own encoded bytes — it exists to distinguish otherwise
/// identical-looking attempts, NOT to name the caller's wire request id.
/// **BUG A2 (2026-08-12):** every other blob method's own `Method` payload
/// already carries the cursor/digest it operates on (`BlobChunkPut{cursor,
/// ..}`, `BlobCommit{cursor}`, `BlobRef{digest}`, ...), so passing the wire
/// `req_id` here is safe FOR THOSE — the method bytes alone already
/// disambiguate two different uploads. `BlobBegin{chunk_size}` is the one
/// exception: it carries no upload identity at all (it is the call that
/// MINTS one), so its caller must pass the freshly allocated cursor id here,
/// never the wire `req_id`. The wire request id is a small integer that a
/// brand-new, otherwise-unrelated connection restarts from 1 on every
/// reconnect (`epistemic_graph.client.EpistemicGraphClient._next_id`), while
/// `CarrierAuthority` carries no per-connection/session component at all — so
/// two independent connections from the same tenant/actor calling
/// `BlobBegin()` with the same default `chunk_size` as their first RPC
/// produced the EXACT SAME idempotency key. The engine's
/// `eg_transaction::MutationKernel::admit` idempotency ledger never expires (by design —
/// it survives an acknowledgement-lost retry across a coordinator restart),
/// so the SECOND connection's `BlobBegin` silently REPLAYED the first
/// connection's already-committed result: the FIRST connection's cursor id,
/// whose durable `CAS_UPLOADS` row had already been torn down by ITS
/// `BlobCommit` (`ChunkStore::commit_upload_batch` removes the row on
/// commit — correctly, per-upload teardown). The handler then looked up that
/// stale, already-removed id and failed with "committed blob upload cursor
/// is missing". See `Method::BlobBegin`'s handler arm for the caller-side
/// fix (allocate the cursor id BEFORE compiling the batch, then pass it here
/// as `identity_id`).
pub(crate) fn compile_blob_batch_at(
    _store: &dyn store::ChunkStore,
    identity_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
    expected: u64,
    now: u64,
) -> Result<(MutationBatch, u64), String> {
    compile_blob_batch_at_with_nonce(_store, identity_id, authority, method, expected, now, None)
}

fn compile_blob_batch_at_with_nonce(
    _store: &dyn store::ChunkStore,
    identity_id: u64,
    authority: &CarrierAuthority,
    method: &Method,
    expected: u64,
    now: u64,
    attempt_nonce: Option<Nonce>,
) -> Result<(MutationBatch, u64), String> {
    let scope = authority.namespace("blob-cas", "control");
    let batch_id =
        crate::server::mutation_batch::opaque_request_key("blob", &scope, identity_id, method);
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: identity_id,
            attempt_nonce,
            principal: Some(authority.actor_scope()),
            tenant: authority.tenant_scope(),
            graph: &scope,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        method,
        MutationSurface::Other,
        DurabilityDomain::BlobStore,
        "blob_operation",
    )?;
    Ok((batch, now))
}

/// Run a refcount adjustment on the blocking pool, returning the new count as `M`'s
/// declared result.
async fn ref_op<M, F>(req_id: u64, f: F) -> Result<Response, Method>
where
    M: eg_types::result_contract::MethodResult<Body = u64>,
    M::Encoding: eg_types::result_contract::EncodeScalar<u64>,
    F: FnOnce() -> Result<u64, String> + Send + 'static,
{
    match run_blocking(req_id, f).await {
        Ok(Ok(n)) => Ok(Response::ok(req_id, ResultPayload::scalar::<M>(n))),
        Ok(Err(e)) => Ok(Response::err(req_id, e)),
        Err(resp) => Ok(resp),
    }
}

/// Run a CAS store call on the blocking pool; an `Err(Response)` is a pool-join
/// failure surfaced as a server error.
async fn run_blocking<T, F>(req_id: u64, f: F) -> Result<T, Response>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Response::err(req_id, format!("blob task join error: {e}")))
}

/// Whether `method` is one of the Blob* variants (used to decide whether a disabled
/// substrate should error vs. fall through).
fn is_blob_method(method: &Method) -> bool {
    matches!(
        method,
        Method::BlobBegin { .. }
            | Method::BlobChunkPut { .. }
            | Method::BlobCommit { .. }
            | Method::BlobFetchBegin { .. }
            | Method::BlobChunkGet { .. }
            | Method::BlobFetchEnd { .. }
            | Method::BlobRef { .. }
            | Method::BlobUnref { .. }
            | Method::BlobGc
    )
}

/// Convenience: build a `BlobCursors` over a fresh native CAS in `dir` (used by
/// integration tests to drive the handler end to end).
#[cfg(test)]
pub(crate) fn cursors_for_test(dir: &str) -> Arc<BlobCursors> {
    let store = Arc::new(store::RedbChunkStore::open(dir).unwrap());
    Arc::new(BlobCursors::new(store))
}
