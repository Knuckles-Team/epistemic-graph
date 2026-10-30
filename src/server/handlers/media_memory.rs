//! Served multimodal memory (EG-MEDIA-VOCAB-R650.3).
//!
//! `Method::MediaRemember` and `Method::MediaRecall` apply the one set of rules
//! in `eg_modality::media_memory` (`admit`, `fold_remember`, `MediaEntry`)
//! over the engine's durable substrates:
//!
//! * the blob bytes go into the content-addressed blob CAS under the verified
//!   owner's holder (the same admission `SourceIngest` uses for raw records);
//! * the node, its derivations and its citing memory are one index row per
//!   digest in `kv.redb`, under a namespace derived from the verified tenant
//!   scope. Recall reads only that tenant's namespace, so one tenant can never
//!   recall another's media.
//!
//! A refusal (R650.4) is decided by `admit` before either store is touched.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use eg_modality::media_memory::{
    admit, fold_remember, MediaDerivation, MediaEntry, MediaMemoryRefusal, MediaRecall,
};
use eg_types::result_contract::storage as storage_results;

/// Upper bound on index rows one text recall scans.
const MAX_RECALL_SCAN: usize = 10_000;

fn refusal(req_id: u64, refusal: MediaMemoryRefusal) -> Response {
    let detail = match refusal {
        MediaMemoryRefusal::OversizeBlob => "blob exceeds the media memory size ceiling",
        MediaMemoryRefusal::TypeMismatch => "declared type differs from the sniffed type",
        MediaMemoryRefusal::UnknownBlob => "no remembered blob has that digest",
    };
    Response::err(req_id, format!("{}: {detail}", refusal.code()))
}

/// The tenant's private index namespace in `kv.redb`. Opaque and keyed by the
/// verified tenant scope, so it cannot collide with a caller KV namespace
/// (those are keyed by the `kv-namespace` / `kv-shared-namespace` domains).
fn index_namespace(authority: &CarrierAuthority) -> String {
    crate::server::mutation_batch::opaque_coordinator_key(
        "media-memory",
        authority.tenant_scope(),
        "index",
    )
}

struct Stores {
    kv: Arc<crate::server::kv::KvStore>,
    blob: Arc<crate::server::blob::BlobCursors>,
}

async fn stores(state: &Arc<RwLock<ServerState>>) -> Result<Stores, String> {
    let s = state.read().await;
    match (s.kv.clone(), s.blob.clone()) {
        (Some(kv), Some(blob)) => Ok(Stores { kv, blob }),
        _ => Err(
            "ENGINE_UNAVAILABLE: media memory needs the durable blob and KV stores".to_string(),
        ),
    }
}

fn read_entry(kv: &crate::server::kv::KvStore, ns: &str, blob_id: &str) -> Result<Option<MediaEntry>, String> {
    kv.get(ns, blob_id)?
        .map(|bytes| {
            rmp_serde::from_slice(&bytes)
                .map_err(|e| format!("INTERNAL: media memory index row is invalid: {e}"))
        })
        .transpose()
}

/// Store the bytes in the blob CAS under the verified owner's holder.
fn admit_blob(
    blob: &crate::server::blob::BlobCursors,
    req_id: u64,
    authority: &CarrierAuthority,
    bytes: &[u8],
) -> Result<String, String> {
    let (digest, _was_new) = blob.store.put_chunk(bytes)?;
    let holder =
        crate::server::blob::store::HolderChange::owner_acquire(&digest, authority.owner_scope())?;
    let blob_method = Method::BlobRef {
        digest: digest.clone(),
    };
    let (holder_batch, admitted_at_ms) = crate::server::handlers::blob::compile_blob_batch(
        blob.store.as_ref(),
        req_id,
        authority,
        &blob_method,
    )?;
    blob.store
        .holder_batch(&holder, &holder_batch, admitted_at_ms)?;
    Ok(digest)
}

struct RememberArgs {
    bytes: Vec<u8>,
    mime: String,
    source_message: String,
    citing_memory: String,
    derivations: Vec<MediaDerivation>,
}

fn remember(
    stores: &Stores,
    req_id: u64,
    authority: &CarrierAuthority,
    args: RememberArgs,
) -> Response {
    let blob_id = match admit(&args.bytes, &args.mime) {
        Ok(id) => id,
        Err(r) => return refusal(req_id, r),
    };
    let committed = (|| {
        let cas_digest = admit_blob(&stores.blob, req_id, authority, &args.bytes)?;
        if cas_digest != blob_id {
            return Err("INTERNAL: blob CAS digest differs from the media digest".to_string());
        }
        let ns = index_namespace(authority);
        let entry = fold_remember(
            read_entry(&stores.kv, &ns, &blob_id)?,
            &args.bytes,
            &args.mime,
            &args.source_message,
            &args.citing_memory,
            &args.derivations,
        );
        let row = rmp_serde::to_vec_named(&entry).map_err(|e| e.to_string())?;
        stores.kv.put(&ns, &blob_id, row)?;
        Ok(entry.memory(&blob_id))
    })();
    match committed {
        Ok(memory) => Response::ok(
            req_id,
            ResultPayload::of_ref::<storage_results::MediaRemember>(&memory),
        ),
        Err(e) => Response::err(req_id, e),
    }
}

fn recall(
    stores: &Stores,
    authority: &CarrierAuthority,
    blob_id: Option<String>,
    text: Option<String>,
) -> Result<Vec<MediaRecall>, String> {
    let ns = index_namespace(authority);
    match (blob_id, text) {
        (Some(blob_id), None) => Ok(read_entry(&stores.kv, &ns, &blob_id)?
            .map(|e| e.recall(&blob_id))
            .into_iter()
            .collect()),
        (None, Some(text)) => {
            let mut rows = Vec::new();
            for (blob_id, bytes) in stores.kv.scan(&ns, "", MAX_RECALL_SCAN)? {
                let entry: MediaEntry = rmp_serde::from_slice(&bytes)
                    .map_err(|e| format!("INTERNAL: media memory index row is invalid: {e}"))?;
                if entry.matches_text(&text) {
                    rows.push(entry.recall(&blob_id));
                }
            }
            Ok(rows)
        }
        _ => Err("INVALID_ARGUMENT: MediaRecall needs exactly one of blob_id or text".to_string()),
    }
}

/// Handle one media memory method, or hand any other method back.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Response, Method> {
    if !matches!(
        method,
        Method::MediaRemember { .. } | Method::MediaRecall { .. }
    ) {
        return Err(method);
    }
    let stores = match stores(state).await {
        Ok(stores) => stores,
        Err(e) => return Ok(Response::err(req_id, e)),
    };
    Ok(match method {
        Method::MediaRemember {
            bytes,
            mime,
            source_message,
            citing_memory,
            derivations,
        } => remember(
            &stores,
            req_id,
            authority,
            RememberArgs {
                bytes,
                mime,
                source_message,
                citing_memory,
                derivations,
            },
        ),
        Method::MediaRecall { blob_id, text } => match recall(&stores, authority, blob_id, text) {
            Ok(rows) => Response::ok(
                req_id,
                ResultPayload::of_ref::<storage_results::MediaRecall>(&rows),
            ),
            Err(e) => Response::err(req_id, e),
        },
        _ => Response::err(req_id, "media memory routing mismatch"),
    })
}
