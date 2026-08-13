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
use crate::mutation_batch::{MutationBatch, MutationDomain, MutationSurface};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::blob::{store, BlobCursors};

/// Handle the blob methods. Returns `Err(method)` for any non-blob method so the
/// dispatch chain falls through (routing convention). When the engine is built
/// `--features blob` but the substrate is disabled (no persist dir), the methods
/// return an explicit error rather than panicking.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Response, Method> {
    // Pull the cursors handle once (cheap clone of the Arc) so we don't hold the
    // ServerState read lock across the blocking store calls.
    let cursors = {
        let s = state.read().await;
        match &s.blob {
            Some(c) => c.clone(),
            None => {
                // Substrate disabled (no persist dir): only respond for blob methods.
                if is_blob_method(&method) {
                    return Ok(Response::err(
                        req_id,
                        "Blob substrate disabled (no persist dir configured)",
                    ));
                }
                return Err(method);
            }
        }
    };

    let original_method = method.clone();
    match method {
        Method::BlobBegin { chunk_size } => {
            let cs = if chunk_size == 0 {
                store::DEFAULT_CHUNK_SIZE as u32
            } else {
                chunk_size
            };
            // BUG A2 (2026-08-12): `BlobBegin`'s `Method` payload carries no
            // upload identity of its own (it is the call that MINTS one), so
            // the durable idempotency key compiled for it must be identified
            // by the freshly allocated cursor id, never the wire `req_id` —
            // `req_id` is only locally unique within one connection (a brand
            // new, otherwise unrelated connection restarts its counter from
            // 1 on every reconnect) and `CarrierAuthority` carries no
            // per-connection component, so two independent connections from
            // the same tenant/actor previously collided on the identical
            // idempotency key for their first `BlobBegin()` call and the
            // second silently replayed the first's already-torn-down cursor
            // ("committed blob upload cursor is missing" below). Allocate
            // the id FIRST so it can be folded into the batch identity —
            // see `compile_blob_batch_at`'s doc for the full mechanism.
            let proposed = cursors.allocate_upload_id();
            let expected = match cursors.store.mutation_version(
                authority.tenant_scope(),
                &authority.namespace("blob-cas", "control"),
            ) {
                Ok(v) => v,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            let now = crate::server::dispatch::authoritative_now_ms();
            let (batch, now) = match compile_blob_batch_at(
                cursors.store.as_ref(),
                proposed,
                authority,
                &original_method,
                expected,
                now,
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            let store = cursors.store.clone();
            let owner_scope = authority.owner_scope().to_string();
            let committed = run_blocking(req_id, move || {
                store.begin_upload_batch(proposed, cs, &owner_scope, &batch, now)
            })
            .await;
            let id = match committed {
                Ok(Ok(id)) => id,
                Ok(Err(error)) => return Ok(Response::err(req_id, error)),
                Err(response) => return Ok(response),
            };
            match cursors.store.load_upload(id) {
                Ok(Some(manifest)) => {
                    if let Err(error) = cursors.restore_upload_manifest(id, manifest) {
                        return Ok(Response::err(req_id, error));
                    }
                }
                Ok(None) => {
                    return Ok(Response::err(
                        req_id,
                        "committed blob upload cursor is missing",
                    ))
                }
                Err(error) => return Ok(Response::err(req_id, error)),
            }
            Ok(Response::ok(req_id, ResultPayload::Count(id)))
        }

        Method::BlobChunkPut { cursor, data } => {
            if let Err(error) = ensure_upload_owner(&cursors, cursor, authority.owner_scope()) {
                return Ok(Response::err(req_id, error));
            }
            let (batch, now) = match compile_blob_batch(
                cursors.store.as_ref(),
                req_id,
                authority,
                &original_method,
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
                Ok(Ok(r)) => r,
                Ok(Err(e)) => return Ok(Response::err(req_id, e)),
                Err(resp) => return Ok(resp),
            };
            match cursors.store.load_upload(cursor) {
                Ok(Some(manifest)) => match cursors.restore_upload_manifest(cursor, manifest) {
                    Ok(()) => Ok(Response::ok(req_id, ResultPayload::Count(count as u64))),
                    Err(error) => Ok(Response::err(req_id, error)),
                },
                Ok(None) => Ok(Response::err(req_id, "durable upload cursor is missing")),
                Err(error) => Ok(Response::err(req_id, error)),
            }
        }

        Method::BlobCommit { cursor } => {
            if let Err(error) = ensure_upload_owner(&cursors, cursor, authority.owner_scope()) {
                return Ok(Response::err(req_id, error));
            }
            let (batch, now) = match compile_blob_batch(
                cursors.store.as_ref(),
                req_id,
                authority,
                &original_method,
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            let store = cursors.store.clone();
            let put = run_blocking(req_id, move || {
                store.commit_upload_batch(cursor, &batch, now)
            })
            .await;
            match put {
                Ok(Ok(digest)) => {
                    cursors.finish_upload(cursor);
                    Ok(Response::ok(req_id, ResultPayload::String(digest)))
                }
                Ok(Err(e)) => Ok(Response::err(req_id, e)),
                Err(resp) => Ok(resp),
            }
        }

        Method::BlobFetchBegin { digest } => {
            let store = cursors.store.clone();
            let d2 = digest.clone();
            let manifest = run_blocking(req_id, move || store.get_manifest(&d2)).await;
            match manifest {
                Ok(Ok(Some(m))) if m.owner_scope == authority.owner_scope() => {
                    let (cursor, n) = cursors.open_fetch(m);
                    Ok(Response::ok(
                        req_id,
                        // (cursor, n_chunks) as a 2-element id list keeps the wire
                        // simple — the client splits it.
                        ResultPayload::raw(&(cursor, n)),
                    ))
                }
                Ok(Ok(Some(_))) | Ok(Ok(None)) => {
                    crate::metrics::access_denied();
                    Ok(Response::err(req_id, "unknown blob digest"))
                }
                Ok(Err(e)) => Ok(Response::err(req_id, e)),
                Err(resp) => Ok(resp),
            }
        }

        Method::BlobChunkGet { cursor, idx } => {
            if let Err(error) = cursors.authorize_fetch(cursor, authority.owner_scope()) {
                return Ok(Response::err(req_id, error));
            }
            let digest = match cursors.fetch_chunk_digest(cursor, idx) {
                Ok(d) => d,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            let store = cursors.store.clone();
            let got = run_blocking(req_id, move || store.get_chunk(&digest)).await;
            match got {
                // A chunk is ARBITRARY binary (raw media bytes), NOT a packed map,
                // so it must NOT travel as `PropertiesMsgpack` — the Python `_send`
                // blindly `unpackb`s any top-level `bytes` result, which corrupts /
                // fails on non-MessagePack content. Wrap it as a `Raw` MessagePack
                // `bin` (serde_bytes) so the client's second `unpackb` recovers the
                // exact original bytes.
                Ok(Ok(Some(bytes))) => Ok(Response::ok(
                    req_id,
                    ResultPayload::raw(&serde_bytes::Bytes::new(&bytes)),
                )),
                Ok(Ok(None)) => Ok(Response::err(req_id, "chunk missing from CAS")),
                Ok(Err(e)) => Ok(Response::err(req_id, e)),
                Err(resp) => Ok(resp),
            }
        }

        Method::BlobFetchEnd { cursor } => {
            if let Err(error) = cursors.authorize_fetch(cursor, authority.owner_scope()) {
                return Ok(Response::err(req_id, error));
            }
            cursors.close_fetch(cursor);
            Ok(Response::ok(req_id, ResultPayload::Bool(true)))
        }

        Method::BlobRef { digest } => {
            if let Err(error) = ensure_blob_owner(&cursors, &digest, authority.owner_scope()) {
                return Ok(Response::err(req_id, error));
            }
            let (batch, now) = match compile_blob_batch(
                cursors.store.as_ref(),
                req_id,
                authority,
                &original_method,
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            let store = cursors.store.clone();
            ref_op(req_id, move || {
                store.adjust_ref_batch(&digest, 1, &batch, now)
            })
            .await
        }

        Method::BlobUnref { digest } => {
            if let Err(error) = ensure_blob_owner(&cursors, &digest, authority.owner_scope()) {
                return Ok(Response::err(req_id, error));
            }
            let (batch, now) = match compile_blob_batch(
                cursors.store.as_ref(),
                req_id,
                authority,
                &original_method,
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            let store = cursors.store.clone();
            ref_op(req_id, move || {
                store.adjust_ref_batch(&digest, -1, &batch, now)
            })
            .await
        }

        Method::BlobGc => {
            if let Err(error) = authority.require_admin("blob garbage collection") {
                return Ok(Response::err(req_id, error));
            }
            let (batch, now) = match compile_blob_batch(
                cursors.store.as_ref(),
                req_id,
                authority,
                &original_method,
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            let store = cursors.store.clone();
            let swept = run_blocking(req_id, move || store.sweep_batch(&batch, now)).await;
            match swept {
                Ok(Ok(stats)) => Ok(Response::ok(
                    req_id,
                    ResultPayload::raw(&(stats.blobs_reclaimed, stats.chunks_reclaimed)),
                )),
                Ok(Err(e)) => Ok(Response::err(req_id, e)),
                Err(resp) => Ok(resp),
            }
        }

        other => Err(other),
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
    let scope = authority.namespace("blob-cas", "control");
    let expected = store.mutation_version(authority.tenant_scope(), &scope)?;
    let now = crate::server::dispatch::authoritative_now_ms();
    compile_blob_batch_at(store, req_id, authority, method, expected, now)
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
/// `eg_mutation_store::begin` idempotency ledger never expires (by design —
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
    let scope = authority.namespace("blob-cas", "control");
    let batch_id =
        crate::server::mutation_batch::opaque_request_key("blob", &scope, identity_id, method);
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: identity_id,
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
        MutationDomain::BlobStore,
        "blob_operation",
    )?;
    Ok((batch, now))
}

/// Run a refcount adjustment on the blocking pool, returning the new count.
async fn ref_op<F>(req_id: u64, f: F) -> Result<Response, Method>
where
    F: FnOnce() -> Result<u64, String> + Send + 'static,
{
    match run_blocking(req_id, f).await {
        Ok(Ok(n)) => Ok(Response::ok(req_id, ResultPayload::Count(n))),
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
