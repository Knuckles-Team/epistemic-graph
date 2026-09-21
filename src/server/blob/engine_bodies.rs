//! Connector-pack bodies in the engine-owned Blob CAS.
//!
//! The caller-owned archive is only an input. Bodies are copied into a tenant-
//! scoped engine namespace, and each manifest receives one stable named holder
//! in the same Blob-owner transaction. Agent Library holder rows are the
//! visibility/liveness authority; these Blob holders prevent GC between the
//! earlier body batch and the later atomic catalog commit.

use eg_types::agent_component::MAX_COMPONENT_BODY_BYTES;
use eg_types::contract::Digest256;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::store::{hex_digest, BlobManifest, BLOB_MANIFEST_VERSION};

const MAX_ENGINE_BODY_BATCH: usize = 3_073;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineBody {
    pub sha256: Digest256,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredEngineBody {
    pub sha256: Digest256,
    pub manifest_digest: String,
    pub holder_id: String,
    pub length: u64,
}

pub(super) struct PlannedEngineBody {
    pub(super) stored: StoredEngineBody,
    pub(super) manifest: BlobManifest,
    pub(super) chunk_digest: Option<String>,
    pub(super) body: Vec<u8>,
}

pub(super) fn plan_engine_bodies(
    tenant_id: &str,
    bodies: &[EngineBody],
) -> Result<Vec<PlannedEngineBody>, String> {
    eg_types::agent_library::validate_key(tenant_id, "connector-pack")?;
    if bodies.len() > MAX_ENGINE_BODY_BATCH {
        return Err("engine body batch exceeds resource limits".to_string());
    }
    let owner_scope = format!("engine-internal:connector-pack:{tenant_id}");
    let mut planned = Vec::with_capacity(bodies.len());
    for input in bodies {
        if input.body.len() > MAX_COMPONENT_BODY_BYTES {
            return Err("connector pack body exceeds resource limits".to_string());
        }
        let actual = Digest256::from_bytes(Sha256::digest(&input.body).into());
        if actual != input.sha256 {
            return Err("connector pack body digest differs from its bytes".to_string());
        }
        let chunk_digest = (!input.body.is_empty()).then(|| hex_digest(&input.body));
        let manifest = BlobManifest {
            schema_version: BLOB_MANIFEST_VERSION,
            owner_scope: owner_scope.clone(),
            chunks: chunk_digest.iter().cloned().collect(),
            chunk_lens: (!input.body.is_empty())
                .then(|| u32::try_from(input.body.len()))
                .transpose()
                .map_err(|_| "connector pack body exceeds resource limits")?
                .into_iter()
                .collect(),
            len: input.body.len() as u64,
            chunk_size: u32::try_from(input.body.len())
                .map_err(|_| "connector pack body exceeds resource limits")?,
        };
        let encoded = super::store::manifest::encode_manifest_bytes(&manifest)?;
        let manifest_digest = hex_digest(&encoded);
        let content_hex = input.sha256.to_hex();
        planned.push(PlannedEngineBody {
            stored: StoredEngineBody {
                sha256: input.sha256,
                manifest_digest,
                holder_id: format!("pack:{tenant_id}:{content_hex}"),
                length: input.body.len() as u64,
            },
            manifest,
            chunk_digest,
            body: input.body.clone(),
        });
    }
    Ok(planned)
}

pub(super) fn batch_subject(bodies: &[PlannedEngineBody]) -> String {
    let mut manifests: Vec<&str> = bodies
        .iter()
        .map(|body| body.stored.manifest_digest.as_str())
        .collect();
    manifests.sort_unstable();
    let mut digest = Sha256::new();
    for manifest in manifests {
        digest.update(manifest.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

/// Read one engine-owned body, enforcing tenant namespace, declared length,
/// chunk lengths and the component's raw SHA-256 before returning any bytes.
pub fn read_engine_body(
    store: &dyn super::store::ChunkStore,
    tenant_id: &str,
    manifest_digest: &str,
    expected_sha256: Digest256,
    expected_length: u64,
) -> Result<Vec<u8>, String> {
    let manifest = store
        .get_manifest(manifest_digest)?
        .ok_or_else(|| "BODY_MISSING: engine body manifest is absent".to_string())?;
    if manifest.owner_scope != format!("engine-internal:connector-pack:{tenant_id}")
        || manifest.len != expected_length
        || manifest.len > MAX_COMPONENT_BODY_BYTES as u64
    {
        return Err("BODY_MISSING: engine body manifest does not match its holder".to_string());
    }
    let mut body = Vec::with_capacity(manifest.len as usize);
    for (digest, declared_length) in manifest.chunks.iter().zip(&manifest.chunk_lens) {
        let chunk = store
            .get_chunk(digest)?
            .ok_or_else(|| "BODY_MISSING: engine body chunk is absent".to_string())?;
        if chunk.len() != *declared_length as usize
            || body.len().saturating_add(chunk.len()) > MAX_COMPONENT_BODY_BYTES
        {
            return Err("BODY_MISSING: engine body chunk length is invalid".to_string());
        }
        body.extend_from_slice(&chunk);
    }
    if body.len() as u64 != expected_length
        || Digest256::from_bytes(Sha256::digest(&body).into()) != expected_sha256
    {
        return Err("BODY_DIGEST_MISMATCH: engine body bytes failed verification".to_string());
    }
    Ok(body)
}
