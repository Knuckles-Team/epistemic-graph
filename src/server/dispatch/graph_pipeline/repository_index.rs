//! CONCEPT:EH-280 — the graph-scoped route of a branch-aware `IndexRepository`.
//!
//! An unscoped `IndexRepository` is a stateless parse answered by the control
//! plane. A scoped one reaches this route only after graph ACL (Write), tenant
//! binding, lazy-open and placement lookup, exactly like `SourceIngest`: it
//! parses each unique blob once, lowers the projection, and delegates the one
//! durable commit to the existing ChangeEnvelope authority.

use super::*;

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
    req_id: u64,
    files_msgpack: Vec<u8>,
    scope: IndexScope,
) -> Result<eg_types::ingestion_wire::IndexResult, Response> {
    use super::super::request_boundary::{ast_input_limits, decode_ast_files};
    let files = decode_ast_files(&files_msgpack, ast_input_limits())
        .and_then(|files| validate_scope_paths(&scope).map(|()| files))
        .map_err(|error| Response::err(req_id, error))?;
    crate::server::compute::compute_off_lock(req_id, move || {
        crate::parser::branch_index::index_branches(files, &scope)
    })
    .await?
    .map_err(|error| Response::err(req_id, error))
}

#[cfg(not(feature = "ast"))]
async fn index_scoped(
    req_id: u64,
    files_msgpack: Vec<u8>,
    scope: IndexScope,
) -> Result<eg_types::ingestion_wire::IndexResult, Response> {
    let _ = (files_msgpack, scope);
    Err(Response::err(req_id, "AST feature not enabled".to_string()))
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
    let result = match index_scoped(ctx.req_id, files_msgpack, scope).await {
        Ok(result) => result,
        Err(response) => return response,
    };
    let write_set = match handlers::repository_index::lower(&result) {
        Ok(write_set) => write_set,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    match already_committed(&ctx, &write_set.envelope_id).await {
        Ok(true) => return handlers::repository_index::respond(ctx.req_id, result),
        Ok(false) => {}
        Err(response) => return response,
    }
    let commit_ctx = handlers::repository_index::IndexCommitContext {
        request_id: ctx.req_id,
        graph_name: ctx.graph_name,
        tenant_scope: ctx.tenant_scope,
        verified: ctx.verified_context,
        graph_version: ctx.core.version(),
        placement_epoch,
        fencing_token,
    };
    let envelope = match handlers::repository_index::build_envelope(&commit_ctx, write_set) {
        Ok(envelope) => envelope,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let commit =
        crate::server::dispatch::change_envelope::apply_one_change_envelope(ctx, envelope).await;
    handlers::repository_index::finish(result, commit)
}

#[cfg(test)]
mod tests {
    use super::super::super::request_boundary::preflight_request_msgpack;
    use crate::protocol::Method;

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
