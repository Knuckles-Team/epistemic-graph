//! Re-drive the graph projection of a connector's current committed pack head.

use crate::protocol::Response;
#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
use crate::protocol::ResultPayload;
use crate::server::auth::VerifiedRequestContext;
#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
use crate::server::persistence::connector_pack::projection::{
    ConnectorPackProjectionPlan, PackProjectionBody,
};
use crate::server::state::ServerState;
#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
use eg_types::connector_pack::{PackEntryKind, PackProjectionState};
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct PackProjectionDocuments {
    shapes: Vec<crate::server::graph_schema::PackSchemaDocument>,
    ontologies: Vec<crate::server::graph_schema::PackSchemaDocument>,
}

pub(crate) async fn serve(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::ConnectorPackReprojectRequest,
) -> Response {
    #[cfg(not(all(feature = "redb", feature = "blob", feature = "shacl")))]
    {
        let _ = (state, verified, request);
        return Response::err(
            req_id,
            "ConnectorPack.reproject requires redb, blob and shacl",
        );
    }
    #[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
    {
        serve_with_substrates(state, req_id, verified, request).await
    }
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
async fn serve_with_substrates(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::ConnectorPackReprojectRequest,
) -> Response {
    let (store, context, plan) =
        match prepare_projection_request(state, req_id, verified, request).await {
            Ok(prepared) => prepared,
            Err(error) => return Response::err(req_id, error),
        };
    let documents = match materialize_current_projection(state, &plan).await {
        Ok(documents) => documents,
        Err(error) => {
            return Response::err(
                req_id,
                record_projection_failure(&store, &context, &plan, &error),
            );
        }
    };
    let committed = match commit_schema_projection(state, req_id, verified, &plan, documents).await
    {
        Ok(committed) => committed,
        Err(error) => {
            return Response::err(
                req_id,
                record_projection_failure(&store, &context, &plan, &error),
            );
        }
    };
    let receipt = match finish_projection(&store, context, &plan, committed) {
        Ok(receipt) => receipt,
        Err(error) => return Response::err(req_id, error),
    };
    Response::ok(
        req_id,
        ResultPayload::of_ref::<eg_types::result_contract::storage::ConnectorPackReproject>(
            &receipt,
        ),
    )
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
async fn prepare_projection_request(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::ConnectorPackReprojectRequest,
) -> Result<
    (
        Arc<crate::server::persistence::agent_library::AgentLibraryStore>,
        eg_types::agent_library::AgentLibraryMutationContext,
        ConnectorPackProjectionPlan,
    ),
    String,
> {
    if request.context.tenant_id != verified.tenant() {
        return Err(
            "ACCESS_DENIED: connector pack tenant must match verified request tenant".to_string(),
        );
    }
    let store = super::agent_library_store(state).await?;
    let plan =
        store.prepare_connector_pack_projection(&request.context.tenant_id, &request.connector)?;
    let context = crate::server::handlers::admin::agent::bind_agent_library_context(
        &store,
        req_id,
        verified,
        request.context,
        "connector-pack:reproject",
        true,
    )?;
    Ok((store, context, plan))
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
async fn materialize_current_projection(
    state: &Arc<RwLock<ServerState>>,
    plan: &ConnectorPackProjectionPlan,
) -> Result<PackProjectionDocuments, String> {
    let blob = state
        .read()
        .await
        .blob
        .clone()
        .ok_or_else(|| "BODY_MISSING: Blob substrate disabled".to_string())?;
    materialize_projection_documents(blob, plan).await
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
async fn commit_schema_projection(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    plan: &ConnectorPackProjectionPlan,
    documents: PackProjectionDocuments,
) -> Result<eg_types::graph_schema::GraphSchemaCommitted, String> {
    crate::server::graph_schema::commit_pack_projection(
        state,
        req_id,
        verified,
        &plan.connector,
        &plan.record_id,
        documents.shapes,
        documents.ontologies,
        verified.idempotency_key(),
    )
    .await
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
fn finish_projection(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    context: eg_types::agent_library::AgentLibraryMutationContext,
    plan: &ConnectorPackProjectionPlan,
    committed: eg_types::graph_schema::GraphSchemaCommitted,
) -> Result<eg_types::connector_pack::PackImportReceipt, String> {
    let expected_graph = committed.graph;
    let expected_graph_version = committed.graph_version;
    let receipt = store.commit_connector_pack_projection(
        context,
        plan,
        PackProjectionState::Applied {
            graph: expected_graph.clone(),
            graph_version: expected_graph_version,
        },
    )?;
    match &receipt.projection {
        PackProjectionState::Applied {
            graph,
            graph_version,
        } if graph == &expected_graph && *graph_version == expected_graph_version => Ok(receipt),
        PackProjectionState::Failed { code } => {
            Err(format!("PACK_PROJECTION_PREVIOUSLY_FAILED: {code}"))
        }
        _ => {
            Err("CORRUPT_MUTATION_LEDGER: reproject replay result differs from graph commit".into())
        }
    }
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
fn record_projection_failure(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    context: &eg_types::agent_library::AgentLibraryMutationContext,
    plan: &ConnectorPackProjectionPlan,
    error: &str,
) -> String {
    let code = projection_error_code(error);
    match store.commit_connector_pack_projection(
        context.clone(),
        plan,
        PackProjectionState::Failed { code },
    ) {
        Ok(_) => error.to_string(),
        Err(persistence) => format!(
            "{error}; connector pack projection failure state was not persisted: {persistence}"
        ),
    }
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
async fn materialize_projection_documents(
    blob: Arc<crate::server::blob::BlobCursors>,
    plan: &ConnectorPackProjectionPlan,
) -> Result<PackProjectionDocuments, String> {
    let store = Arc::clone(&blob.store);
    let tenant_id = plan.tenant_id.clone();
    let bodies = plan.bodies.clone();
    tokio::task::spawn_blocking(move || {
        projection_documents_from_store(store.as_ref(), &tenant_id, &bodies)
    })
    .await
    .map_err(|error| format!("connector pack projection body task failed: {error}"))?
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
fn projection_documents_from_store(
    store: &dyn crate::server::blob::store::ChunkStore,
    tenant_id: &str,
    bodies: &[PackProjectionBody],
) -> Result<PackProjectionDocuments, String> {
    let mut shapes = Vec::new();
    let mut ontologies = Vec::new();
    let mut shapes_length = 0usize;
    let mut ontology_length = 0usize;
    for body in bodies {
        let bytes = crate::server::blob::engine_bodies::read_engine_body(
            store,
            tenant_id,
            &body.engine_manifest_digest,
            body.body_sha256,
            body.length,
        )?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| "BODY_MISSING: projected schema body is not UTF-8".to_string())?;
        let (target, total_length) = match body.kind {
            PackEntryKind::Ontology => (&mut ontologies, &mut ontology_length),
            PackEntryKind::Shapes => (&mut shapes, &mut shapes_length),
            _ => {
                return Err("CORRUPT_CONNECTOR_PACK: projection contains a non-schema body".into())
            }
        };
        push_schema_document(target, total_length, &body.uri, text)?;
    }
    Ok(PackProjectionDocuments { shapes, ontologies })
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
fn push_schema_document(
    target: &mut Vec<crate::server::graph_schema::PackSchemaDocument>,
    total_length: &mut usize,
    uri: &str,
    document: &str,
) -> Result<(), String> {
    let length = total_length
        .checked_add(document.len())
        .ok_or_else(|| "SCHEMA_SOURCES_TOO_LARGE: pack schema length overflow".to_string())?;
    if length > eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES {
        return Err("SCHEMA_SOURCES_TOO_LARGE: pack schema exceeds source bound".into());
    }
    target.push(crate::server::graph_schema::PackSchemaDocument {
        uri: uri.to_string(),
        body: Arc::from(document),
    });
    *total_length = length;
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob", feature = "shacl"))]
fn projection_error_code(error: &str) -> String {
    let code = error.split_once(':').map_or(error, |(code, _)| code).trim();
    if code.is_empty()
        || code.len() > 256
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        "PACK_PROJECTION_FAILED".to_string()
    } else {
        code.to_string()
    }
}

#[cfg(all(test, feature = "redb", feature = "blob", feature = "shacl"))]
mod tests {
    use super::*;
    use crate::server::blob::engine_bodies::EngineBody;
    use crate::server::blob::store::ChunkStore as _;
    use eg_types::contract::Digest256;
    use sha2::{Digest as _, Sha256};

    fn body(bytes: &[u8]) -> EngineBody {
        EngineBody {
            sha256: Digest256::from_bytes(Sha256::digest(bytes).into()),
            body: bytes.to_vec(),
        }
    }

    #[test]
    fn engine_owned_schema_bodies_materialize_by_kind() {
        let store = crate::server::blob::store::RedbChunkStore::open_temp().unwrap();
        let ontology = body(b"<urn:Class> a <urn:Ontology> .");
        let shapes = body(b"<urn:Shape> a <urn:NodeShape> .");
        let stored = store
            .put_engine_bodies("tenant-a", &[ontology.clone(), shapes.clone()], 1)
            .unwrap();
        let pointers = vec![
            PackProjectionBody {
                uri: "ontology://demo/schema.ttl".into(),
                kind: PackEntryKind::Ontology,
                body_sha256: ontology.sha256,
                engine_manifest_digest: stored[0].manifest_digest.clone(),
                length: stored[0].length,
            },
            PackProjectionBody {
                uri: "shapes://demo/schema.ttl".into(),
                kind: PackEntryKind::Shapes,
                body_sha256: shapes.sha256,
                engine_manifest_digest: stored[1].manifest_digest.clone(),
                length: stored[1].length,
            },
        ];
        let documents = projection_documents_from_store(&store, "tenant-a", &pointers).unwrap();
        assert_eq!(documents.ontologies.len(), 1);
        assert_eq!(documents.shapes.len(), 1);
    }

    #[test]
    fn missing_engine_body_fails_closed() {
        let store = crate::server::blob::store::RedbChunkStore::open_temp().unwrap();
        let pointer = PackProjectionBody {
            uri: "ontology://demo/missing.ttl".into(),
            kind: PackEntryKind::Ontology,
            body_sha256: Digest256::from_bytes([7; 32]),
            engine_manifest_digest: "0".repeat(64),
            length: 12,
        };
        let error = projection_documents_from_store(&store, "tenant-a", &[pointer]).unwrap_err();
        assert!(error.starts_with("BODY_MISSING:"), "{error}");
    }

    #[test]
    fn documents_keep_file_local_blank_node_scopes_and_aggregate_bound() {
        let mut documents = Vec::new();
        let mut length = 0;
        push_schema_document(
            &mut documents,
            &mut length,
            "ontology://demo/a.ttl",
            "_:b0 <urn:p> <urn:a> .",
        )
        .unwrap();
        push_schema_document(
            &mut documents,
            &mut length,
            "ontology://demo/b.ttl",
            "_:b0 <urn:p> <urn:b> .",
        )
        .unwrap();
        assert_eq!(documents.len(), 2);
        assert_ne!(documents[0].uri, documents[1].uri);
        assert!(documents.iter().all(|doc| doc.body.starts_with("_:b0")));
        let oversized = "x".repeat(eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES + 1);
        let mut oversized_docs = Vec::new();
        let mut oversized_length = 0;
        assert!(push_schema_document(
            &mut oversized_docs,
            &mut oversized_length,
            "ontology://demo/large.ttl",
            &oversized
        )
        .is_err());
    }

    #[test]
    fn failure_codes_are_stable_and_bounded() {
        assert_eq!(
            projection_error_code("SCHEMA_SOURCE_REGRESSION: stale"),
            "SCHEMA_SOURCE_REGRESSION"
        );
        assert_eq!(
            projection_error_code("unstructured failure"),
            "PACK_PROJECTION_FAILED"
        );
    }
}
