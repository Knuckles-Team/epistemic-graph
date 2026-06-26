//! Streamed content-addressed BLOB handler (CONCEPT:KG-2.206).
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
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::blob::store;
#[cfg(test)]
use crate::server::blob::BlobCursors;

/// Handle the blob methods. Returns `Err(method)` for any non-blob method so the
/// dispatch chain falls through (routing convention). When the engine is built
/// `--features blob` but the substrate is disabled (no persist dir), the methods
/// return an explicit error rather than panicking.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
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

    match method {
        Method::BlobBegin { chunk_size } => {
            let cs = if chunk_size == 0 {
                store::DEFAULT_CHUNK_SIZE as u32
            } else {
                chunk_size
            };
            let id = cursors.open_upload(cs);
            Ok(Response::ok(req_id, ResultPayload::Count(id)))
        }

        Method::BlobChunkPut { cursor, data } => {
            let added = data.len() as u64;
            // Hash + persist the chunk on the blocking pool (fsync), then record
            // only its digest on the cursor.
            let store = cursors.store.clone();
            let put = run_blocking(req_id, move || store.put_chunk(&data)).await;
            let (digest, _was_new) = match put {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => return Ok(Response::err(req_id, e)),
                Err(resp) => return Ok(resp),
            };
            match cursors.push_chunk(cursor, digest, added) {
                Ok(n) => Ok(Response::ok(req_id, ResultPayload::Count(n as u64))),
                Err(e) => Ok(Response::err(req_id, e)),
            }
        }

        Method::BlobCommit { cursor } => {
            let manifest = match cursors.take_upload(cursor) {
                Ok(m) => m,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            // The blob digest is the hash of the manifest bytes (stable content
            // address). Store the manifest on the blocking pool.
            let manifest_bytes = match rmp_serde::to_vec_named(&manifest) {
                Ok(b) => b,
                Err(e) => return Ok(Response::err(req_id, e.to_string())),
            };
            let digest = store::hex_digest(&manifest_bytes);
            let store = cursors.store.clone();
            let d2 = digest.clone();
            let m2 = manifest.clone();
            let put = run_blocking(req_id, move || store.put_manifest(&d2, &m2)).await;
            match put {
                Ok(Ok(())) => Ok(Response::ok(req_id, ResultPayload::String(digest))),
                Ok(Err(e)) => Ok(Response::err(req_id, e)),
                Err(resp) => Ok(resp),
            }
        }

        Method::BlobFetchBegin { digest } => {
            let store = cursors.store.clone();
            let d2 = digest.clone();
            let manifest = run_blocking(req_id, move || store.get_manifest(&d2)).await;
            match manifest {
                Ok(Ok(Some(m))) => {
                    let (cursor, n) = cursors.open_fetch(m);
                    Ok(Response::ok(
                        req_id,
                        // (cursor, n_chunks) as a 2-element id list keeps the wire
                        // simple — the client splits it.
                        ResultPayload::raw(&(cursor, n)),
                    ))
                }
                Ok(Ok(None)) => Ok(Response::err(req_id, "unknown blob digest")),
                Ok(Err(e)) => Ok(Response::err(req_id, e)),
                Err(resp) => Ok(resp),
            }
        }

        Method::BlobChunkGet { cursor, idx } => {
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
            cursors.close_fetch(cursor);
            Ok(Response::ok(req_id, ResultPayload::Bool(true)))
        }

        Method::BlobRef { digest } => {
            let store = cursors.store.clone();
            ref_op(req_id, move || store.incref(&digest)).await
        }

        Method::BlobUnref { digest } => {
            let store = cursors.store.clone();
            ref_op(req_id, move || store.decref(&digest)).await
        }

        Method::BlobGc => {
            let store = cursors.store.clone();
            let swept = run_blocking(req_id, move || store.sweep()).await;
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
