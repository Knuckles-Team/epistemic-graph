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

/// Admit the bytes into the blob CAS and lower to the byte-free
/// `MediaIndexPut` that commits the index row.
fn lower_remember(
    stores: &Stores,
    req_id: u64,
    authority: &CarrierAuthority,
    args: RememberArgs,
) -> Result<Method, Response> {
    let blob_id = admit(&args.bytes, &args.mime).map_err(|r| refusal(req_id, r))?;
    let cas_digest =
        admit_blob(&stores.blob, req_id, authority, &args.bytes).map_err(|e| Response::err(req_id, e))?;
    if cas_digest != blob_id {
        return Err(Response::err(
            req_id,
            "INTERNAL: blob CAS digest differs from the media digest",
        ));
    }
    Ok(Method::MediaIndexPut {
        blob_id,
        mime: args.mime,
        size: args.bytes.len() as u64,
        source_message: args.source_message,
        citing_memory: args.citing_memory,
        derivations: args.derivations,
    })
}

/// Commit one index row: fold the delta into the tenant's existing row for
/// the digest, then write it as one ledgered `kv.redb` owner mutation.
fn index_put(stores: &Stores, req_id: u64, authority: &CarrierAuthority, method: Method) -> Response {
    let Method::MediaIndexPut {
        blob_id,
        mime,
        size,
        source_message,
        citing_memory,
        derivations,
    } = method
    else {
        return Response::err(req_id, "media memory routing mismatch");
    };
    if size > eg_modality::media_memory::MEDIA_MEMORY_MAX_BYTES as u64 {
        return refusal(req_id, MediaMemoryRefusal::OversizeBlob);
    }
    let ns = index_namespace(authority);
    let committed = (|| {
        let entry = fold_remember(
            read_entry(&stores.kv, &ns, &blob_id)?,
            size,
            &mime,
            &source_message,
            &citing_memory,
            &derivations,
        );
        let row = rmp_serde::to_vec_named(&entry).map_err(|e| e.to_string())?;
        stores.kv.put(&ns, &blob_id, row)?;
        Ok::<_, String>(entry.memory(&blob_id))
    })();
    match committed {
        Ok(memory) => Response::ok(
            req_id,
            ResultPayload::of_ref::<storage_results::MediaIndexPut>(&memory),
        ),
        Err(e) => Response::err(req_id, e),
    }
}

/// Whether the index row must be proposed through consensus.
async fn proposes_through_consensus(state: &Arc<RwLock<ServerState>>) -> bool {
    #[cfg(feature = "raft")]
    {
        !crate::server::dispatch::is_replicated_apply()
            && matches!(
                state.read().await.placement_authority(),
                crate::server::state::PlacementAuthorityKind::MultiRaft
            )
    }
    #[cfg(not(feature = "raft"))]
    {
        let _ = state;
        false
    }
}

/// Re-label an index-put answer as the public `MediaRemember` result (same body).
fn as_remember(resp: Response) -> Response {
    match resp.result {
        Some(ResultPayload::Raw(bytes)) if resp.error.is_none() => {
            match rmp_serde::from_slice::<eg_types::media_memory::MediaMemory>(&bytes) {
                Ok(memory) => Response::ok(
                    resp.id,
                    ResultPayload::of_ref::<storage_results::MediaRemember>(&memory),
                ),
                Err(e) => Response::err(resp.id, format!("INTERNAL: media index answer: {e}")),
            }
        }
        _ => resp,
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
    request_graph: &str,
    verified_context: &crate::server::auth::VerifiedRequestContext,
    method: Method,
) -> Result<Response, Method> {
    if !matches!(
        method,
        Method::MediaRemember { .. } | Method::MediaIndexPut { .. } | Method::MediaRecall { .. }
    ) {
        return Err(method);
    }
    let authority = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Ok(Response::err(req_id, denied)),
    };
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
        } => {
            let index = match lower_remember(
                &stores,
                req_id,
                &authority,
                RememberArgs {
                    bytes,
                    mime,
                    source_message,
                    citing_memory,
                    derivations,
                },
            ) {
                Ok(index) => index,
                Err(resp) => return Ok(resp),
            };
            if proposes_through_consensus(state).await {
                #[cfg(feature = "raft")]
                {
                    as_remember(
                        crate::server::dispatch::propose_native_mutation(
                            state,
                            request_graph,
                            req_id,
                            verified_context,
                            false,
                            index,
                        )
                        .await,
                    )
                }
                #[cfg(not(feature = "raft"))]
                {
                    let _ = request_graph;
                    unreachable!("consensus needs the raft feature")
                }
            } else {
                let _ = request_graph;
                as_remember(index_put(&stores, req_id, &authority, index))
            }
        }
        method @ Method::MediaIndexPut { .. } => index_put(&stores, req_id, &authority, method),
        Method::MediaRecall { blob_id, text } => match recall(&stores, &authority, blob_id, text) {
            Ok(rows) => Response::ok(
                req_id,
                ResultPayload::of_ref::<storage_results::MediaRecall>(&rows),
            ),
            Err(e) => Response::err(req_id, e),
        },
        _ => Response::err(req_id, "media memory routing mismatch"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EG-MEDIA-VOCAB-R650.3: the replicated command carries the digest and
    /// the index row only, never the blob bytes.
    // spec: EG-MEDIA-VOCAB-R650.3
    #[test]
    fn replicated_index_command_carries_no_blob_bytes() {
        let dir = crate::server::unique_temp_dir("media-memory-lower");
        let dir = dir.to_str().expect("utf-8").to_string();
        let stores = Stores {
            kv: Arc::new(crate::server::kv::KvStore::open(Some(&dir)).unwrap()),
            blob: crate::server::handlers::blob::cursors_for_test(&dir),
        };
        let context = crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
            "agent", "tenant-a",
        );
        let authority = CarrierAuthority::from_verified(&context).expect("carrier");
        let mut bytes = vec![0x89, b'P', b'N', b'G'];
        bytes.extend((0..4096u32).map(|i| (i * 31 % 251) as u8));
        let index = lower_remember(
            &stores,
            1,
            &authority,
            RememberArgs {
                bytes: bytes.clone(),
                mime: "image/png".into(),
                source_message: "m".into(),
                citing_memory: "c".into(),
                derivations: vec![],
            },
        )
        .expect("lowered");
        let Method::MediaIndexPut { ref blob_id, size, .. } = index else {
            panic!("MediaRemember must lower to MediaIndexPut");
        };
        assert_eq!(size, bytes.len() as u64);
        let wire = rmp_serde::to_vec_named(&index).unwrap();
        assert!(
            wire.len() < 512,
            "the replicated command must not carry the {}-byte blob ({} bytes)",
            bytes.len(),
            wire.len()
        );
        assert!(!wire.windows(64).any(|w| w == &bytes[100..164]));
        assert_eq!(
            stores.blob.store.get_chunk(blob_id).unwrap().as_deref(),
            Some(bytes.as_slice()),
            "the bytes travel through the blob CAS instead"
        );
        assert_eq!(
            crate::server::mutation::cluster_mutation_route(&index),
            crate::server::mutation::ClusterMutationRoute::ConsensusNative
        );
    }
}
