//! Shared fixtures for the blob store tests: caller batches with stable replay
//! identities, and a sweep at an explicit instant.

use super::{hex_digest, BlobManifest, ChunkStore, CommittedBlob, BLOB_MANIFEST_VERSION};
use crate::mutation_batch::MutationBatch;
use sha2::{Digest, Sha256};

pub(crate) const TENANT: &str = "tenant-opaque";
pub(crate) const SCOPE: &str = "scope-opaque";

/// One caller operation: a batch id bound to the scope version it was compiled
/// against. Every attempt of it carries a fresh transport nonce, so a retry after
/// a lost acknowledgement replays the committed result instead of being refused
/// as a reused nonce.
#[derive(Debug, Clone)]
pub(crate) struct Operation {
    id: String,
    expected: u64,
}

impl Operation {
    /// Compile `id` against the store's current scope version.
    pub(crate) fn new(store: &dyn ChunkStore, id: &str) -> Self {
        Self {
            id: id.to_string(),
            expected: store.mutation_version(TENANT, SCOPE).unwrap(),
        }
    }

    /// Attempt `attempt` of this operation.
    pub(crate) fn attempt(&self, attempt: u32) -> MutationBatch {
        let mut nonce = Sha256::new();
        nonce.update(self.id.as_bytes());
        nonce.update(attempt.to_be_bytes());
        crate::server::mutation_batch::compile_opaque_method(
            crate::server::mutation_batch::CompileBatch {
                batch_id: &self.id,
                request_id: u64::from(attempt) + 1,
                attempt_nonce: Some(eg_types::contract::Nonce::from_bytes(
                    nonce.finalize().into(),
                )),
                principal: Some("system"),
                tenant: TENANT,
                graph: SCOPE,
                placement_epoch: 0,
                idempotency_key: &self.id,
                expected_graph_version: Some(self.expected),
                fencing_token: None,
                created_at_ms: 1,
                default_surface: crate::mutation_batch::MutationSurface::Other,
                authoritative_state: None,
            },
            &crate::protocol::Method::ApplyMutation {
                event_type: "blob_store_test".to_string(),
                query: format!("sha256:{}", super::hex_digest(self.id.as_bytes())),
            },
            crate::mutation_batch::MutationSurface::Other,
            crate::mutation_batch::DurabilityDomain::BlobStore,
            "blob_store_test",
        )
        .unwrap()
    }

    /// The first attempt.
    pub(crate) fn batch(&self) -> MutationBatch {
        self.attempt(0)
    }
}

/// Compile and return the first attempt of a fresh operation `id`.
pub(crate) fn fresh(store: &dyn ChunkStore, id: &str) -> MutationBatch {
    Operation::new(store, id).batch()
}

/// Stream `data` through the in-process path one chunk at a time (bounded memory),
/// exactly as the protocol cursor does, and commit its engine-owned manifest.
pub(crate) fn chunked(store: &dyn ChunkStore, data: &[u8], chunk_size: usize) -> CommittedBlob {
    let mut chunks = Vec::new();
    let mut chunk_lens = Vec::new();
    for part in data.chunks(chunk_size) {
        let (digest, _was_new) = store.put_chunk(part).unwrap();
        chunks.push(digest);
        chunk_lens.push(part.len() as u32);
    }
    let manifest = BlobManifest {
        schema_version: BLOB_MANIFEST_VERSION,
        owner_scope: super::ENGINE_BLOB_OWNER_SCOPE.to_string(),
        chunks,
        chunk_lens,
        len: data.len() as u64,
        chunk_size: chunk_size as u32,
    };
    let digest = hex_digest(&rmp_serde::to_vec_named(&manifest).unwrap());
    store.put_manifest(&digest, &manifest).unwrap();
    CommittedBlob { digest, manifest }
}

/// Every chunk of `manifest`, read back and concatenated.
pub(crate) fn reassemble(store: &dyn ChunkStore, manifest: &BlobManifest) -> Vec<u8> {
    let mut out = Vec::new();
    for digest in &manifest.chunks {
        out.extend(store.get_chunk(digest).unwrap().unwrap());
    }
    out
}

/// One durable wire-style upload: begin `cursor` for `owner`, put `parts`, all at
/// `at_ms`. Returns the cursor; the upload is left uncommitted.
pub(crate) fn begin_with_parts(
    store: &dyn ChunkStore,
    cursor: u64,
    owner: &str,
    parts: &[&[u8]],
    at_ms: u64,
) -> u64 {
    let tag = format!("{owner}/{cursor}/{at_ms}");
    store
        .begin_upload_batch(
            cursor,
            8,
            owner,
            &fresh(store, &format!("begin:{tag}")),
            at_ms,
        )
        .unwrap();
    for (ordinal, part) in parts.iter().enumerate() {
        let batch = fresh(store, &format!("chunk:{tag}:{ordinal}"));
        store
            .put_upload_chunk_batch(cursor, part, &batch, at_ms)
            .unwrap();
    }
    cursor
}

/// [`begin_with_parts`] and commit at the same instant; returns the blob digest.
pub(crate) fn upload_at(
    store: &dyn ChunkStore,
    cursor: u64,
    owner: &str,
    parts: &[&[u8]],
    at_ms: u64,
) -> String {
    begin_with_parts(store, cursor, owner, parts, at_ms);
    let batch = fresh(store, &format!("commit:{owner}/{cursor}/{at_ms}"));
    store.commit_upload_batch(cursor, &batch, at_ms).unwrap()
}
