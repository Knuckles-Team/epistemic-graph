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
const MAX_REPOSITORY_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_REPOSITORY_BODY_BATCH: usize = 4_096;
const MAX_REPOSITORY_BATCH_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(super) enum BodyPurpose<'a> {
    ConnectorPack,
    Repository(&'a str),
}

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

pub(super) struct PlannedEngineBody<'a> {
    pub(super) stored: StoredEngineBody,
    pub(super) manifest: BlobManifest,
    pub(super) chunk_digest: Option<String>,
    pub(super) body: &'a [u8],
}

pub(super) fn plan_engine_bodies<'a>(
    tenant_id: &str,
    bodies: &'a [EngineBody],
) -> Result<Vec<PlannedEngineBody<'a>>, String> {
    plan_bodies(tenant_id, BodyPurpose::ConnectorPack, bodies)
}

/// Plan one repository ingest's source bytes for a single CAS owner commit.
/// The namespace includes a digest of repository identity so a tenant's two
/// repositories cannot acquire each other's holder rows by naming one blob.
pub(super) fn plan_repository_bodies<'a>(
    tenant_id: &str,
    repository_id: &str,
    bodies: &'a [EngineBody],
) -> Result<Vec<PlannedEngineBody<'a>>, String> {
    if repository_id.trim().is_empty() {
        return Err("repository body namespace requires a repository id".to_string());
    }
    plan_bodies(tenant_id, BodyPurpose::Repository(repository_id), bodies)
}

fn plan_bodies<'a>(
    tenant_id: &str,
    purpose: BodyPurpose<'_>,
    bodies: &'a [EngineBody],
) -> Result<Vec<PlannedEngineBody<'a>>, String> {
    eg_types::agent_library::validate_key(tenant_id, "connector-pack")?;
    let (namespace, owner_scope, max_count, max_bytes) = match purpose {
        BodyPurpose::ConnectorPack => (
            format!("pack:{tenant_id}"),
            format!("engine-internal:connector-pack:{tenant_id}"),
            MAX_ENGINE_BODY_BATCH,
            MAX_COMPONENT_BODY_BYTES,
        ),
        BodyPurpose::Repository(repository_id) => (
            repository_namespace(tenant_id, repository_id),
            repository_owner_scope(),
            MAX_REPOSITORY_BODY_BATCH,
            MAX_REPOSITORY_BODY_BYTES,
        ),
    };
    if bodies.len() > max_count {
        return Err("engine body batch exceeds resource limits".to_string());
    }
    if matches!(purpose, BodyPurpose::Repository(_))
        && bodies
            .iter()
            .try_fold(0usize, |sum, body| sum.checked_add(body.body.len()))
            .is_none_or(|total| total > MAX_REPOSITORY_BATCH_BYTES)
    {
        return Err("repository body batch exceeds resource limits".to_string());
    }
    let mut planned = Vec::with_capacity(bodies.len());
    for input in bodies {
        if input.body.len() > max_bytes {
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
        planned.push(PlannedEngineBody {
            stored: StoredEngineBody {
                sha256: input.sha256,
                manifest_digest,
                holder_id: format!("{namespace}:{}", input.sha256.to_hex()),
                length: input.body.len() as u64,
            },
            manifest,
            chunk_digest,
            body: &input.body,
        });
    }
    Ok(planned)
}

fn repository_namespace(tenant_id: &str, repository_id: &str) -> String {
    format!(
        "repository:{}:{}",
        hex_digest(tenant_id.as_bytes()),
        hex_digest(repository_id.as_bytes())
    )
}

fn repository_owner_scope() -> String {
    // :Blob is content-keyed across repositories and tenant scopes. Keep its
    // manifest address stable; the named holder below gates each owner's read.
    super::store::ENGINE_BLOB_OWNER_SCOPE.to_string()
}

/// The holder namespace every engine pack body of `tenant_id` is held in.
pub fn engine_body_holder_namespace(tenant_id: &str) -> String {
    format!("pack:{tenant_id}")
}

/// The one set-like holder of one engine pack body: every component revision
/// naming the same bytes shares it, so a retried copy never adds a count.
pub fn engine_body_holder(tenant_id: &str, sha256: &Digest256) -> String {
    format!(
        "{}:{}",
        engine_body_holder_namespace(tenant_id),
        sha256.to_hex()
    )
}

pub(super) fn batch_subject(bodies: &[PlannedEngineBody<'_>]) -> String {
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

pub(super) fn repository_batch_subject(
    tenant_id: &str,
    repository_id: &str,
    bodies: &[PlannedEngineBody<'_>],
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"eg/repository-source-cas-batch/v1");
    digest.update((tenant_id.len() as u64).to_be_bytes());
    digest.update(tenant_id.as_bytes());
    digest.update((repository_id.len() as u64).to_be_bytes());
    digest.update(repository_id.as_bytes());
    digest.update(batch_subject(bodies).as_bytes());
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
    read_body_for_owner(
        store,
        &format!("engine-internal:connector-pack:{tenant_id}"),
        manifest_digest,
        expected_sha256,
        expected_length,
        MAX_COMPONENT_BODY_BYTES,
    )
}

/// Read a repository source only under its tenant and repository namespace,
/// checking the complete raw SHA-256 before any worker can use the bytes.
pub fn read_repository_body(
    store: &dyn super::store::ChunkStore,
    tenant_id: &str,
    repository_id: &str,
    manifest_digest: &str,
    expected_sha256: Digest256,
    expected_length: u64,
) -> Result<Vec<u8>, String> {
    let holder = format!(
        "{}:{}",
        repository_namespace(tenant_id, repository_id),
        expected_sha256.to_hex()
    );
    if !store.has_named_holder(manifest_digest, &holder)? {
        return Err("BODY_MISSING: repository holder is absent".to_string());
    }
    read_body_for_owner(
        store,
        &repository_owner_scope(),
        manifest_digest,
        expected_sha256,
        expected_length,
        MAX_REPOSITORY_BODY_BYTES,
    )
}

/// Resolve the exact graph-row reference produced by scoped repository
/// indexing. A missing CAS body, wrong owner, changed bytes or malformed ref
/// refuses before a background enrichment worker sees source content.
pub fn read_repository_content_ref(
    store: &dyn super::store::ChunkStore,
    tenant_id: &str,
    repository_id: &str,
    content_ref: &str,
    content_digest: &str,
    expected_length: u64,
) -> Result<Vec<u8>, String> {
    let manifest_digest = content_ref
        .strip_prefix("cas:sha256:")
        .ok_or_else(|| "BODY_MISSING: repository content_ref is invalid".to_string())?;
    let sha256 = content_digest
        .strip_prefix("sha256:")
        .ok_or_else(|| "BODY_MISSING: repository content digest is invalid".to_string())?;
    let expected_sha256 = Digest256::parse(sha256)?;
    read_repository_body(
        store,
        tenant_id,
        repository_id,
        manifest_digest,
        expected_sha256,
        expected_length,
    )
}

fn read_body_for_owner(
    store: &dyn super::store::ChunkStore,
    owner_scope: &str,
    manifest_digest: &str,
    expected_sha256: Digest256,
    expected_length: u64,
    max_bytes: usize,
) -> Result<Vec<u8>, String> {
    let manifest = store
        .get_manifest(manifest_digest)?
        .ok_or_else(|| "BODY_MISSING: engine body manifest is absent".to_string())?;
    if manifest.owner_scope != owner_scope
        || manifest.len != expected_length
        || manifest.len > max_bytes as u64
    {
        return Err("BODY_MISSING: engine body manifest does not match its holder".to_string());
    }
    let mut body = Vec::with_capacity(manifest.len as usize);
    for (digest, declared_length) in manifest.chunks.iter().zip(&manifest.chunk_lens) {
        let chunk = store
            .get_chunk(digest)?
            .ok_or_else(|| "BODY_MISSING: engine body chunk is absent".to_string())?;
        if chunk.len() != *declared_length as usize
            || body.len().saturating_add(chunk.len()) > max_bytes
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

#[cfg(test)]
mod repository_tests {
    use super::*;

    #[test]
    fn repository_plan_has_isolated_set_like_holders() {
        let bytes = b"def answer(): return 42".to_vec();
        let sha256 = Digest256::from_bytes(Sha256::digest(&bytes).into());
        let body = EngineBody {
            sha256,
            body: bytes,
        };
        let first_body = [body.clone()];
        let replay_body = [body.clone()];
        let other_body = [body];
        let first = plan_repository_bodies("tenant-a", "repo-one", &first_body).unwrap();
        let replay = plan_repository_bodies("tenant-a", "repo-one", &replay_body).unwrap();
        let other = plan_repository_bodies("tenant-a", "repo-two", &other_body).unwrap();
        assert_eq!(first[0].stored, replay[0].stored);
        assert_ne!(first[0].stored.holder_id, other[0].stored.holder_id);
        assert_eq!(first[0].manifest.owner_scope, other[0].manifest.owner_scope);
        assert_eq!(
            first[0].stored.manifest_digest,
            other[0].stored.manifest_digest
        );
        assert_ne!(
            repository_batch_subject("tenant-a", "repo-one", &first),
            repository_batch_subject("tenant-a", "repo-two", &other)
        );
    }

    #[test]
    fn repository_plan_refuses_unbound_or_changed_bytes() {
        let body = EngineBody {
            sha256: Digest256::from_bytes([0; 32]),
            body: b"source".to_vec(),
        };
        assert!(plan_repository_bodies("tenant-a", "repo", &[body]).is_err());
        assert!(plan_repository_bodies("tenant-a", "", &[]).is_err());
    }
}
