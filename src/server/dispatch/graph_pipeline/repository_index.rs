//! EH-280 — the graph-scoped route of a branch-aware `IndexRepository`.
//!
//! An unscoped `IndexRepository` is a stateless parse answered by the control
//! plane. A scoped one reaches this route only after graph ACL (Write), tenant
//! binding, lazy-open and placement lookup, exactly like `SourceIngest`: it
//! parses each unique blob once, lowers the projection, and delegates the one
//! durable commit to the existing ChangeEnvelope authority.

use super::*;

#[cfg(all(feature = "ast", feature = "redb"))]
mod consumer;
#[cfg(all(feature = "ast", feature = "redb"))]
mod enrichment;
#[cfg(all(
    feature = "ast",
    feature = "redb",
    feature = "blob",
    feature = "raft",
    feature = "security"
))]
pub(crate) use consumer::decode_pending_intent as decode_pending_enrichment_intent;
#[cfg(all(feature = "ast", feature = "redb", feature = "blob", feature = "raft"))]
pub(crate) use consumer::plan_held_underfunded_park;
#[cfg(all(feature = "ast", feature = "redb", feature = "blob"))]
pub(crate) use consumer::{drain_once as drain_repository_enrichment_once, DrainOutcome};
#[cfg(all(
    feature = "ast",
    feature = "redb",
    feature = "blob",
    feature = "raft",
    feature = "security"
))]
pub(crate) use enrichment::intent_for_snapshot as enrichment_intent_for_snapshot;

type IndexScope = Box<eg_types::ingestion_wire::IndexRepositoryScope>;

/// Every scope path obeys the same portable logical-path rule as a source name.
#[cfg(feature = "ast")]
fn validate_scope_paths(
    scope: &eg_types::ingestion_wire::IndexRepositoryScope,
) -> Result<(), String> {
    use super::super::request_boundary::validate_ast_logical_path;
    let versions = scope.file_versions.iter().map(|item| item.path.as_str());
    let removed = scope
        .tombstones
        .iter()
        .flat_map(|item| std::iter::once(item.path.as_str()).chain(item.successor_path.as_deref()));
    versions
        .chain(removed)
        .try_for_each(validate_ast_logical_path)
}

/// Decode, validate and parse+project one scoped batch (off the reactor).
#[cfg(feature = "ast")]
async fn index_scoped(
    ctx: GraphOpRouting<'_>,
    files_msgpack: Vec<u8>,
    scope: IndexScope,
) -> Result<eg_types::ingestion_wire::IndexResult, Response> {
    use super::super::request_boundary::{ast_input_limits, decode_ast_sources};
    let req_id = ctx.req_id;
    // Entries are content-keyed: one path may name several blobs (EH-280).
    let files = decode_ast_sources(&files_msgpack, ast_input_limits())
        .and_then(|files| validate_scope_paths(&scope).map(|()| files))
        .map_err(|error| Response::err(req_id, error))?;
    // Validate the complete scope and parse before acquiring CAS holders. A
    // malformed membership must never pin unreferenced source bytes forever.
    let (mut result, files, repository_id) =
        crate::server::compute::compute_off_lock(req_id, move || {
            let result = crate::parser::branch_index::index_branches(files.clone(), &scope)?;
            Ok::<_, String>((result, files, scope.repository_id.clone()))
        })
        .await?
        .map_err(|error| Response::err(req_id, error))?;
    let refs = admit_repository_sources(ctx, &repository_id, &files).await?;
    attach_content_refs(&mut result, &refs);
    Ok(result)
}

#[cfg(feature = "ast")]
fn attach_content_refs(
    result: &mut eg_types::ingestion_wire::IndexResult,
    refs: &std::collections::BTreeMap<String, (String, u64)>,
) {
    for node in &mut result.nodes {
        if node.node_type == "Blob" {
            if let Some(digest) = node.properties.get("content_digest") {
                if let Some((content_ref, length)) = refs.get(digest) {
                    node.properties
                        .insert("content_ref".into(), content_ref.clone());
                    node.properties
                        .insert("content_length".into(), length.to_string());
                }
            }
        }
    }
}

#[cfg(not(feature = "ast"))]
async fn index_scoped(
    ctx: GraphOpRouting<'_>,
    files_msgpack: Vec<u8>,
    scope: IndexScope,
) -> Result<eg_types::ingestion_wire::IndexResult, Response> {
    let req_id = ctx.req_id;
    let _ = (files_msgpack, scope);
    Err(Response::err(req_id, "AST feature not enabled".to_string()))
}

/// Admit source bytes as one tenant/repository-scoped CAS owner transaction.
/// The graph envelope later pins each submitted Blob's content_ref; a parse or
/// graph refusal leaves only set-like CAS holders, never a partial graph batch.
#[cfg(all(feature = "ast", feature = "blob"))]
async fn admit_repository_sources(
    ctx: GraphOpRouting<'_>,
    repository_id: &str,
    files: &[(String, Vec<u8>)],
) -> Result<std::collections::BTreeMap<String, (String, u64)>, Response> {
    use sha2::{Digest, Sha256};
    let Some(blob) = ctx.state.read().await.blob.clone() else {
        return Ok(Default::default());
    };
    let tenant = ctx.tenant_scope.to_string();
    let repository = repository_id.to_string();
    let bodies: Vec<_> = files
        .iter()
        .map(
            |(_, bytes)| crate::server::blob::engine_bodies::EngineBody {
                sha256: eg_types::contract::Digest256::from_bytes(Sha256::digest(bytes).into()),
                body: bytes.clone(),
            },
        )
        .collect();
    let stored = tokio::task::spawn_blocking(move || {
        blob.store.put_repository_bodies(
            &tenant,
            &repository,
            &bodies,
            crate::server::dispatch::authoritative_now_ms(),
        )
    })
    .await
    .map_err(|error| Response::err(ctx.req_id, error.to_string()))?
    .map_err(|error| Response::err(ctx.req_id, error))?;
    Ok(stored
        .into_iter()
        .map(|body| {
            (
                format!("sha256:{}", body.sha256.to_hex()),
                (format!("cas:sha256:{}", body.manifest_digest), body.length),
            )
        })
        .collect())
}

#[cfg(all(feature = "ast", not(feature = "blob")))]
async fn admit_repository_sources(
    _ctx: GraphOpRouting<'_>,
    _repository_id: &str,
    _files: &[(String, Vec<u8>)],
) -> Result<std::collections::BTreeMap<String, (String, u64)>, Response> {
    Ok(Default::default())
}

/// Whether this exact write-set was already committed to the graph.
async fn already_committed(ctx: &GraphOpRouting<'_>, envelope_id: &str) -> Result<bool, Response> {
    let Some(persistence) = ctx.persistence.as_ref() else {
        return Err(Response::err(
            ctx.req_id,
            "branch-aware repository indexing requires durable persistence",
        ));
    };
    persistence
        .read_change_envelope(&crate::persist::sanitize(ctx.graph_name), envelope_id)
        .await
        .map(|record| record.is_some())
        .map_err(|error| Response::err(ctx.req_id, error))
}

/// Parse, project and durably commit one scoped `IndexRepository` batch.
pub(super) async fn route_repository_index(
    ctx: GraphOpRouting<'_>,
    files_msgpack: Vec<u8>,
    scope: IndexScope,
    placement_epoch: u64,
    fencing_token: Option<u64>,
) -> Response {
    let result = match index_scoped(ctx, files_msgpack, scope).await {
        Ok(result) => result,
        Err(response) => return response,
    };
    let write_set = match handlers::repository_index::lower(&result) {
        Ok(write_set) => write_set,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let envelope_id = write_set.envelope_id.clone();
    match already_committed(&ctx, &envelope_id).await {
        Ok(true) => return handlers::repository_index::respond(ctx.req_id, result),
        Ok(false) => {}
        Err(response) => return response,
    }
    #[cfg(all(feature = "ast", feature = "redb"))]
    let pending = match enrichment::pending_intent(ctx, &result, &envelope_id).await {
        Ok(pending) => pending,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    #[cfg(all(feature = "ast", feature = "redb", feature = "raft"))]
    if pending.is_some() {
        return Response::err(
            ctx.req_id,
            "CONFLICT: repository enrichment budget seed requires replicated authority",
        );
    }
    #[cfg(all(feature = "ast", feature = "redb"))]
    let outbox = pending
        .as_ref()
        .map(|entry| entry.intent.clone())
        .into_iter()
        .collect();
    #[cfg(not(all(feature = "ast", feature = "redb")))]
    let outbox = Vec::new();
    let commit_ctx = handlers::repository_index::IndexCommitContext {
        request_id: ctx.req_id,
        graph_name: ctx.graph_name,
        tenant_scope: ctx.tenant_scope,
        verified: ctx.verified_context,
        graph_version: ctx.core.version(),
        placement_epoch,
        fencing_token,
    };
    let envelope = match handlers::repository_index::build_envelope_with_outbox(
        &commit_ctx,
        write_set,
        outbox,
    ) {
        Ok(envelope) => envelope,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    #[cfg(all(feature = "ast", feature = "redb"))]
    let commit = crate::server::dispatch::change_envelope::apply_repository_change_envelope(
        ctx,
        envelope,
        pending.map(|entry| entry.budget),
    )
    .await;
    #[cfg(not(all(feature = "ast", feature = "redb")))]
    let commit =
        crate::server::dispatch::change_envelope::apply_one_change_envelope(ctx, envelope).await;
    handlers::repository_index::finish(result, commit)
}

#[cfg(test)]
mod tests {
    use super::super::super::request_boundary::preflight_request_msgpack;
    use crate::protocol::Method;

    #[cfg(feature = "ast")]
    #[test]
    fn only_cas_admitted_blob_rows_receive_source_refs() {
        let blob = |digest: &str| eg_types::ingestion_wire::ExtractedNode {
            node_id: format!("blob:{digest}"),
            node_type: "Blob".into(),
            properties: std::collections::HashMap::from([("content_digest".into(), digest.into())]),
        };
        let mut result = eg_types::ingestion_wire::IndexResult {
            nodes: vec![blob("sha256:admitted"), blob("sha256:prior-tombstone")],
            ..Default::default()
        };
        super::attach_content_refs(
            &mut result,
            &std::collections::BTreeMap::from([(
                "sha256:admitted".into(),
                ("cas:sha256:manifest".into(), 12),
            )]),
        );
        assert_eq!(
            result.nodes[0]
                .properties
                .get("content_ref")
                .map(String::as_str),
            Some("cas:sha256:manifest")
        );
        assert_eq!(
            result.nodes[0]
                .properties
                .get("content_length")
                .map(String::as_str),
            Some("12")
        );
        assert!(!result.nodes[1].properties.contains_key("content_ref"));
    }

    /// A caller cannot claim the repository-snapshot class on the public
    /// envelope method to reach the repository-content screening rule.
    #[test]
    fn callers_cannot_submit_repository_snapshot_material() {
        let result = eg_types::ingestion_wire::IndexResult {
            nodes: vec![eg_types::ingestion_wire::ExtractedNode {
                node_id: "blob:x".to_string(),
                node_type: "Blob".to_string(),
                properties: std::collections::HashMap::new(),
            }],
            ..Default::default()
        };
        let envelope = crate::server::handlers::repository_index::repository_envelope(&result)
            .expect("repository envelope");
        let error = preflight_request_msgpack(&Method::ApplyChangeEnvelope {
            envelope: Box::new(envelope),
        })
        .expect_err("engine-attested only");
        assert!(error.contains("engine-attested"), "{error}");
    }
}
