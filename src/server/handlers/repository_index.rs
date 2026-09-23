//! CONCEPT:EH-280 — durable projection of a branch-aware `IndexRepository` batch.
//!
//! A scoped result (symbols anchored on `:Blob`, `:FileVersion`, and
//! `:Branch -hasFileVersion-> :FileVersion` membership) is lowered to graph-row
//! methods and committed as ONE governed ChangeEnvelope through the existing
//! `ApplyChangeEnvelope` authority -- the same single commit path SourceIngest
//! uses, so there is no second graph writer. A tombstone lowers to the removal
//! of its membership edge. The envelope id is the digest of the lowered
//! write-set, so re-indexing an unchanged batch replays instead of committing
//! twice, and every node/edge write is an upsert. The envelope is classified
//! `MaterialClass::RepositorySnapshot`: symbol names, repository-relative paths
//! and code text are screened for HOST identity (absolute host paths, file URIs,
//! e-mail addresses), not blanket-rejected for `@` or a `/home/` path segment.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::change_envelope::{
    ChangeEnvelope, ContentVersion, ContentVersionPosition, MaterialClass, MaterialOperation,
    PolicyRecord, PrivacyAttestation, CHANGE_ENVELOPE_VERSION,
};
use eg_types::contract::Digest256;
use eg_types::ingestion_wire::{ExtractedEdge, ExtractedNode, IndexResult};

use crate::protocol::{Method, Response, ResultPayload};

/// The projection edge a tombstone is reported as; it lowers to a removal.
const TOMBSTONE_EDGE: &str = "removesFileVersion";
const ENVELOPE_PREFIX: &str = "repository-index:";
const VERSION_TYPE: &str = "repository-index-sha256";
const POLICY_VERSION: &str = "repository-index";

/// The verified request coordinates one scoped commit is compiled under.
pub(crate) type IndexCommitContext<'a> = crate::server::mutation_batch::GraphWriteScope<'a>;

/// The lowered, digest-keyed write-set of one scoped result.
pub(crate) struct IndexWriteSet {
    pub envelope_id: String,
    digest: Digest256,
    methods: Vec<Method>,
}

/// Properties are re-keyed into a `BTreeMap` so the encoding -- and therefore
/// the envelope id -- never depends on hash-map iteration order.
fn encode_properties(
    properties: &std::collections::HashMap<String, String>,
    kind_key: &str,
    kind: &str,
) -> Result<Vec<u8>, String> {
    let mut ordered: BTreeMap<&str, &str> = properties
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    ordered.insert(kind_key, kind);
    rmp_serde::to_vec_named(&ordered)
        .map_err(|error| format!("repository index property encoding failed: {error}"))
}

fn node_method(node: &ExtractedNode) -> Result<Method, String> {
    Ok(Method::AddNode {
        node_id: node.node_id.clone(),
        properties_msgpack: encode_properties(&node.properties, "type", &node.node_type)?,
    })
}

fn edge_method(edge: &ExtractedEdge) -> Result<Method, String> {
    if edge.edge_type == TOMBSTONE_EDGE {
        return Ok(Method::RemoveEdge {
            source_id: edge.source.clone(),
            target_id: edge.target.clone(),
        });
    }
    Ok(Method::AddEdge {
        source_id: edge.source.clone(),
        target_id: edge.target.clone(),
        properties_msgpack: encode_properties(&edge.properties, "relationship", &edge.edge_type)?,
    })
}

/// Lower a scoped result to its write-set. The graph keys an edge by its
/// endpoint pair, so the first edge of a pair (resolver order: structural and
/// call edges before resemblance) is the one kept.
pub(crate) fn lower(result: &IndexResult) -> Result<IndexWriteSet, String> {
    let mut methods = result
        .nodes
        .iter()
        .map(node_method)
        .collect::<Result<Vec<_>, _>>()?;
    let mut pairs = BTreeSet::new();
    for edge in &result.edges {
        if pairs.insert((edge.source.as_str(), edge.target.as_str())) {
            methods.push(edge_method(edge)?);
        }
    }
    let encoded = rmp_serde::to_vec_named(&methods)
        .map_err(|error| format!("repository index write-set encoding failed: {error}"))?;
    let digest = Digest256::framed(b"eg/repository-index-batch", &[&encoded])?;
    Ok(IndexWriteSet {
        envelope_id: format!("{ENVELOPE_PREFIX}{}", digest.to_hex()),
        digest,
        methods,
    })
}

fn policy(object_id: &str, tenant: &str) -> Result<PolicyRecord, String> {
    let subject = Digest256::framed(b"eg/repository-index-policy-subject", &[tenant.as_bytes()])?;
    Ok(PolicyRecord {
        policy_id: format!("policy:{object_id}"),
        operation: MaterialOperation::Upsert,
        object_id: object_id.into(),
        tenant: tenant.into(),
        classification: "tenant-private".into(),
        policy_version: POLICY_VERSION.into(),
        subject_set_digest: subject.to_hex(),
        retention_policy: "repository-index".into(),
        legal_hold: false,
    })
}

/// Compile the write-set into one governed, validated ChangeEnvelope.
pub(crate) fn build_envelope(
    ctx: &IndexCommitContext<'_>,
    write_set: IndexWriteSet,
) -> Result<ChangeEnvelope, String> {
    let IndexWriteSet {
        envelope_id,
        digest,
        methods,
    } = write_set;
    let mutation = ctx.compile(&envelope_id, methods)?;
    seal(mutation, envelope_id, &digest, ctx.tenant_scope)
}

/// Wrap a compiled batch as the repository-snapshot envelope of `digest`.
fn seal(
    mutation: eg_types::mutation_batch::MutationBatch,
    envelope_id: String,
    digest: &Digest256,
    tenant: &str,
) -> Result<ChangeEnvelope, String> {
    let digest = digest.to_hex();
    let envelope = ChangeEnvelope {
        schema_version: CHANGE_ENVELOPE_VERSION,
        policies: vec![policy(&envelope_id, tenant)?],
        envelope_id: envelope_id.clone(),
        mutation,
        content_version: ContentVersion {
            object_id: envelope_id,
            digest_algorithm: "sha256".into(),
            digest: digest.clone(),
            previous_digest: None,
            source_version: ContentVersionPosition::Opaque {
                version_type: VERSION_TYPE.into(),
                value: digest.clone(),
            },
        },
        cursor: None,
        blobs: Vec::new(),
        features: Vec::new(),
        evidence: Vec::new(),
        lineage: Vec::new(),
        privacy: PrivacyAttestation {
            policy_version: POLICY_VERSION.into(),
            sanitizer_version: POLICY_VERSION.into(),
            sanitized_payload_digest: digest,
        },
        material_class: MaterialClass::RepositorySnapshot,
        commit_seq: None,
        commit_descriptor_ref: None,
    };
    envelope.validate()?;
    Ok(envelope)
}

/// Answer a batch whose write-set is durable (committed now, or already).
pub(crate) fn respond(request_id: u64, result: IndexResult) -> Response {
    Response::ok(
        request_id,
        ResultPayload::of::<eg_types::result_contract::ingestion::IndexRepository>(result),
    )
}

/// Answer the commit of a batch: its index result, or the commit's refusal.
pub(crate) fn finish(result: IndexResult, commit: Response) -> Response {
    match commit.error {
        Some(error) => Response::err(commit.id, error),
        None => respond(commit.id, result),
    }
}

#[cfg(test)]
#[path = "repository_index_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) use tests::repository_envelope;
