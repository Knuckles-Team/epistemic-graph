//! `Method::GraphSchema` and `Method::GraphSchemaList` (X9): keyed shapes and
//! ontology sources on one request graph.
//!
//! The write path is GATEWAY-routed, exactly like `IcvConfigure`: the arm runs
//! inside the graph commit kernel, so an attach is audited, emits CDC and is
//! ordered against every other write to that graph. The read is a separate
//! method because a read op inside a gateway-routed method would need a
//! runtime-conditional gateway plan.
//!
//! The schema-sources package owns composition, validation and the memoised
//! composed digest. Writes reach the ordinary graph mutation gateway, so schema
//! changes share the graph's persistence, audit, CDC, idempotency and Raft
//! ordering rather than maintaining a second control-plane commit path.

pub(crate) mod attach_pack;
pub(crate) mod compose;

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::graph::GraphCore;

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::mutation::{MutationCtx, MutationPlan};
use crate::server::state::ServerState;

const PACK_PROJECTION_GRAPH_DOMAIN: &[u8] = b"eg/connector-pack-projection-graph/v1";

/// One exact semantic member selected from a committed ConnectorPack record.
/// Keeping URI and body together preserves the per-file RDF blank-node scope;
/// callers must never concatenate Turtle documents before this boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PackSchemaDocument {
    pub(crate) uri: String,
    pub(crate) body: Arc<str>,
}

/// Tenant-safe, non-reversible physical graph identity for one connector pack.
///
/// Both inputs are canonical identifiers from verified/typed authority.  The
/// physical name exposes neither tenant nor connector and the framed digest
/// preserves their boundary, while the logical GraphSchema key inside that
/// graph remains the human-readable `pack:<connector>`.
pub(crate) fn pack_projection_graph_name(
    tenant_id: &str,
    connector: &eg_types::contract::ResourceId,
) -> Result<String, String> {
    eg_types::contract::TenantId::new(tenant_id).map_err(|error| {
        format!("invalid verified tenant for connector pack projection: {error}")
    })?;
    let digest = eg_types::contract::Digest256::framed(
        PACK_PROJECTION_GRAPH_DOMAIN,
        &[tenant_id.as_bytes(), connector.as_str().as_bytes()],
    )?;
    Ok(format!("pack__{}", digest.to_hex()))
}

/// Commit the semantic projection of one ConnectorPack head.
///
/// This is the server-level companion to [`apply_pack_source`].  ConnectorPack
/// owns its head/receipt CAS; GraphSchema owns physical graph identity, durable
/// graph lifecycle, the mutation gateway, and the reserved schema source.  A
/// newer head with no semantic documents installs an empty Pack-origin
/// tombstone, preserving the record high-water mark so stale schema cannot be
/// resurrected.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_pack_projection(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    connector: &eg_types::contract::ResourceId,
    record_id: &str,
    shapes: Vec<PackSchemaDocument>,
    ontologies: Vec<PackSchemaDocument>,
    idempotency_key: &str,
) -> Result<eg_types::graph_schema::GraphSchemaCommitted, String> {
    let graph_name = pack_projection_graph_name(verified.tenant(), connector)?;
    #[cfg(feature = "raft")]
    if state.read().await.multi_raft.is_some() {
        return Err(
            "CONNECTOR_PACK_PROJECTION_CLUSTER_UNAVAILABLE: ConnectorPack control state is not replicated"
                .to_string(),
        );
    }
    crate::server::dispatch::ensure_internal_global_graph(
        state,
        req_id,
        verified,
        &graph_name,
        idempotency_key,
    )
    .await?;
    let authority = crate::server::access::CarrierAuthority::from_verified(verified)?;
    let (entry, isolation, persistence, materialization_manifest, write_coalescer) = {
        let state = state.read().await;
        let entry = state
            .registry
            .get(&graph_name)
            .cloned()
            .ok_or_else(|| format!("pack projection graph '{graph_name}' is not resident"))?;
        let materialization_manifest = state.registry.materialization_handle(&graph_name);
        (
            entry,
            state.isolation.clone(),
            state.persistence.clone(),
            materialization_manifest,
            state.routed_write_coalescer.clone(),
        )
    };
    if materialization_manifest
        .as_ref()
        .is_some_and(|manifest| manifest.read().map_or(true, |manifest| !manifest.valid))
    {
        return Err(
            "PACK_PROJECTION_GRAPH_NOT_MATERIALIZED: projection graph is incomplete".into(),
        );
    }
    #[cfg(feature = "streaming")]
    let cdc = state.read().await.cdc.clone();
    let method = Method::GraphSchema {
        op: Box::new(eg_types::graph_schema::GraphSchemaOp::AttachPack {
            connector: connector.clone(),
            if_composed_digest: None,
        }),
    };
    let plan = MutationPlan::for_method(&method);
    let result_graph_name = graph_name.clone();
    let ctx = MutationCtx {
        req_id,
        caller: Some(verified.agent_id()),
        attempt_nonce: verified.attempt_nonce(),
        idempotency_key,
        tenant_scope: authority.tenant_scope(),
        graph_name: &graph_name,
        graph_type: entry.graph_type,
        owner: entry.owner.as_deref(),
        isolation: &isolation,
        core: &entry.core,
        persistence: persistence.as_ref(),
        #[cfg(feature = "streaming")]
        cdc: cdc.as_ref(),
        materialization_manifest: materialization_manifest.as_ref(),
        write_coalescer: Some(&write_coalescer),
    };
    let connector = connector.clone();
    let record_id = record_id.to_string();
    let response =
        crate::server::handlers::graph_ops::commit_gateway(&ctx, &plan, &method, move |core| {
            apply_pack_source(
                core,
                &result_graph_name,
                &connector,
                &record_id,
                shapes,
                ontologies,
                None,
            )
            .and_then(ResultPayload::of::<eg_types::result_contract::reasoning::GraphSchema>)
        })
        .await;
    if let Some(error) = response.error {
        return Err(error);
    }
    match response.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes)
            .map_err(|error| format!("GraphSchema projection result decode failed: {error}")),
        _ => Err("GraphSchema projection returned an undeclared result encoding".to_string()),
    }
}

/// Attach, replace or detach one keyed schema source, inside the gateway.
///
/// The op match is exhaustive and has no catch-all: `AttachPack` resolves a
/// connector's pack head and belongs to a different package from the operator
/// attach, so it must stay separately routed.
pub(crate) async fn handle_gateway(
    state: &Arc<RwLock<ServerState>>,
    tenant_id: &str,
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    op: &eg_types::graph_schema::GraphSchemaOp,
) -> Response {
    use eg_types::graph_schema::GraphSchemaOp;

    match op {
        GraphSchemaOp::Attach { .. } | GraphSchemaOp::Detach { .. } => {
            let op = op.clone();
            let graph_name = ctx.graph_name.to_string();
            crate::server::handlers::graph_ops::commit_gateway(ctx, plan, method, move |core| {
                apply(core, &graph_name, &op)
            })
            .await
        }
        GraphSchemaOp::AttachPack { connector, .. } => {
            handle_attach_pack_gateway(
                state,
                tenant_id,
                ctx,
                plan,
                method,
                connector,
                op.expected_composed_digest(),
            )
            .await
        }
    }
}

async fn handle_attach_pack_gateway(
    state: &Arc<RwLock<ServerState>>,
    tenant_id: &str,
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    connector: &eg_types::contract::ResourceId,
    expected_digest: Option<&str>,
) -> Response {
    #[cfg(feature = "raft")]
    if state.read().await.multi_raft.is_some() {
        return Response::err(
            ctx.req_id,
            "ATTACH_PACK_CLUSTER_UNAVAILABLE: ConnectorPack control state is not replicated",
        );
    }
    let resolved = match attach_pack::resolve(state, tenant_id, connector).await {
        Ok(source) => source,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let connector = connector.clone();
    let expected_digest = expected_digest.map(str::to_owned);
    let graph_name = ctx.graph_name.to_string();
    crate::server::handlers::graph_ops::commit_gateway(ctx, plan, method, move |core| {
        apply_pack_source(
            core,
            &graph_name,
            &connector,
            &resolved.record_id,
            resolved.shapes,
            resolved.ontologies,
            expected_digest.as_deref(),
        )
        .and_then(ResultPayload::of::<eg_types::result_contract::reasoning::GraphSchema>)
    })
    .await
}

/// Install one ConnectorPack semantic projection under its reserved source.
///
/// This is the sole internal seam shared by `GraphSchema.AttachPack` and the
/// ConnectorPack reproject worker.  Callers must invoke it inside the graph
/// mutation gateway on the gateway's staged [`GraphCore`].  The seam accepts
/// only connector identity, immutable record identity, and engine-owned bytes;
/// it constructs the reserved key and provenance itself.  Consequently neither
/// a wire caller nor a pack worker can forge a core/admin/ingestion origin.
pub(crate) fn apply_pack_source(
    core: &GraphCore,
    graph_name: &str,
    connector: &eg_types::contract::ResourceId,
    record_id: &str,
    shapes: Vec<PackSchemaDocument>,
    ontologies: Vec<PackSchemaDocument>,
    if_composed_digest: Option<&str>,
) -> Result<eg_types::graph_schema::GraphSchemaCommitted, String> {
    use eg_types::graph_schema::GraphSchemaCommitted;

    if record_id.is_empty() {
        return Err("ATTACH_PACK_SCHEMA_INVALID: pack record id is empty".to_string());
    }
    if let Some(expected) = if_composed_digest {
        eg_types::contract::Digest256::parse(expected)
            .map_err(|error| format!("invalid if_composed_digest: {error}"))?;
    }
    let mut sources = (*core.schema_sources()).clone();
    if let Some(expected) = if_composed_digest {
        let actual = sources.composed_digest().to_hex();
        if expected != actual {
            return Err(format!(
                "COMPOSED_DIGEST_MISMATCH: expected {expected}, actual {actual}"
            ));
        }
    }
    let shapes_ttl = compose_pack_documents("SHAPES", shapes)?;
    let ontology_ttl = compose_pack_documents("ONTOLOGY", ontologies)?;
    let source = crate::graph::GraphSchemaSource::new(
        crate::graph::SchemaSourceOrigin::Pack {
            connector: connector.as_str().to_string(),
            record_id: record_id.to_string(),
        },
        shapes_ttl,
        ontology_ttl,
        0,
    )?;
    let source_id = format!("pack:{}", connector.as_str());
    let changed = sources.attach_dynamic(source_id, source)?;
    compose::validate_and_compose(&sources)?;
    if changed {
        core.install_schema_sources(Arc::new(sources.clone()));
    }
    Ok(GraphSchemaCommitted {
        schema_version: eg_types::graph_schema::GRAPH_SCHEMA_RESULT_SCHEMA_VERSION,
        graph: graph_name.to_string(),
        composed_digest: sources.composed_digest().to_hex(),
        graph_version: core.version().saturating_add(u64::from(changed)),
        changed,
    })
}

/// Parse each pack member independently, scope its blank nodes by immutable
/// member URI, then render a deterministic N-Triples/Turtle union. Sorting the
/// URI first makes pack archive order irrelevant; parsing before union is what
/// prevents two local `_:b0` labels from becoming one RDF node.
fn compose_pack_documents(
    kind: &str,
    mut documents: Vec<PackSchemaDocument>,
) -> Result<Option<Arc<str>>, String> {
    if documents.is_empty() {
        return Ok(None);
    }
    documents.sort_by(|left, right| left.uri.as_bytes().cmp(right.uri.as_bytes()));
    let mut previous_uri: Option<&str> = None;
    let mut rendered = std::collections::BTreeSet::new();
    for document in &documents {
        if document.uri.is_empty() || document.uri.chars().any(char::is_control) {
            return Err(format!(
                "ATTACH_PACK_SCHEMA_INVALID: {kind} member URI is invalid"
            ));
        }
        if previous_uri == Some(document.uri.as_str()) {
            return Err(format!(
                "ATTACH_PACK_SCHEMA_AMBIGUOUS: duplicate {kind} member URI '{}'",
                document.uri
            ));
        }
        previous_uri = Some(document.uri.as_str());
        if document.body.len() > eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES {
            return Err(format!(
                "ATTACH_PACK_SCHEMA_TOO_LARGE: {kind} member '{}' exceeds {} bytes",
                document.uri,
                eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES
            ));
        }
        let scope = eg_types::contract::Digest256::sha256(document.uri.as_bytes()).to_hex();
        let triples = eg_rdf::mapping::parse_turtle(&document.body).map_err(|error| {
            format!(
                "ATTACH_PACK_SCHEMA_INVALID: {kind} member '{}': {error}",
                document.uri
            )
        })?;
        for triple in triples {
            let triple = compose::scope_blank_nodes(triple, &scope).map_err(|error| {
                format!(
                    "ATTACH_PACK_SCHEMA_INVALID: {kind} member '{}': {error}",
                    document.uri
                )
            })?;
            rendered.insert(triple.to_string());
        }
    }
    let mut body = rendered.into_iter().collect::<Vec<_>>().join("\n");
    body.push('\n');
    if body.len() > eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES {
        return Err(format!(
            "ATTACH_PACK_SCHEMA_TOO_LARGE: composed {kind} projection exceeds {} bytes",
            eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES
        ));
    }
    Ok(Some(Arc::from(body)))
}

/// List the request graph's schema sources and its composed digest.
pub(crate) async fn handle_list(req_id: u64, graph_name: &str, core: &Arc<GraphCore>) -> Response {
    list_response(req_id, list(graph_name, core))
}

fn list_response(
    req_id: u64,
    result: Result<eg_types::graph_schema::GraphSchemaSourcesView, String>,
) -> Response {
    match result {
        Ok(body) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::reasoning::GraphSchemaList>(body),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

fn apply(
    core: &GraphCore,
    graph_name: &str,
    op: &eg_types::graph_schema::GraphSchemaOp,
) -> Result<ResultPayload, String> {
    use eg_types::graph_schema::{GraphSchemaCommitted, GraphSchemaOp};

    op.validate()?;
    let mut sources = (*core.schema_sources()).clone();
    if let Some(expected) = op.expected_composed_digest() {
        let actual = sources.composed_digest().to_hex();
        if expected != actual {
            return Err(format!(
                "COMPOSED_DIGEST_MISMATCH: expected {expected}, actual {actual}"
            ));
        }
    }
    let changed = match op {
        GraphSchemaOp::Attach {
            source_id,
            shapes_ttl,
            ontology_ttl,
            ..
        } => sources.attach_dynamic(
            source_id.clone(),
            crate::graph::GraphSchemaSource::new(
                crate::graph::SchemaSourceOrigin::Admin {
                    name: source_id
                        .strip_prefix("admin:")
                        .expect("validated generic schema ids use the admin namespace")
                        .to_string(),
                },
                shapes_ttl.as_deref().map(Arc::from),
                ontology_ttl.as_deref().map(Arc::from),
                0,
            )?,
        )?,
        GraphSchemaOp::Detach { source_id, .. } => sources.detach_dynamic(source_id),
        GraphSchemaOp::AttachPack { .. } => unreachable!("attach-pack has its own route"),
    };
    compose::validate_and_compose(&sources)?;
    let composed_digest = sources.composed_digest().to_hex();
    if changed {
        core.install_schema_sources(Arc::new(sources));
    }
    ResultPayload::of::<eg_types::result_contract::reasoning::GraphSchema>(GraphSchemaCommitted {
        schema_version: eg_types::graph_schema::GRAPH_SCHEMA_RESULT_SCHEMA_VERSION,
        graph: graph_name.to_string(),
        composed_digest,
        graph_version: core.version().saturating_add(u64::from(changed)),
        changed,
    })
}

fn list(
    graph_name: &str,
    core: &GraphCore,
) -> Result<eg_types::graph_schema::GraphSchemaSourcesView, String> {
    use eg_types::graph_schema::{
        GraphSchemaSourceView, GraphSchemaSourcesView, SchemaSourceOriginView,
        GRAPH_SCHEMA_RESULT_SCHEMA_VERSION,
    };
    let sources = core.schema_sources();
    let project =
        |source_id: &str, source: &crate::graph::GraphSchemaSource| -> GraphSchemaSourceView {
            GraphSchemaSourceView {
                source_id: source_id.to_string(),
                origin: match &source.origin {
                    crate::graph::SchemaSourceOrigin::Core {
                        module,
                        version,
                        set_digest,
                    } => SchemaSourceOriginView::Core {
                        module: module.clone(),
                        version: *version,
                        set_digest: set_digest.to_hex(),
                    },
                    crate::graph::SchemaSourceOrigin::Operator => SchemaSourceOriginView::Operator,
                    crate::graph::SchemaSourceOrigin::Admin { name } => {
                        SchemaSourceOriginView::Admin { name: name.clone() }
                    }
                    crate::graph::SchemaSourceOrigin::Pack {
                        connector,
                        record_id,
                    } => SchemaSourceOriginView::Pack {
                        connector: connector.clone(),
                        record_id: record_id.clone(),
                    },
                    crate::graph::SchemaSourceOrigin::Ingestion { mapping, revision } => {
                        SchemaSourceOriginView::Ingestion {
                            mapping: mapping.clone(),
                            revision: *revision,
                        }
                    }
                },
                shapes_sha256: source.shapes_sha256.map(|digest| digest.to_hex()),
                ontology_sha256: source.ontology_sha256.map(|digest| digest.to_hex()),
                shapes_bytes: source.shapes_ttl.as_deref().map_or(0, str::len) as u64,
                ontology_bytes: source.ontology_ttl.as_deref().map_or(0, str::len) as u64,
                attached_at_ms: source.attached_at_ms,
            }
        };
    let core_sources = sources
        .core
        .iter()
        .map(|(source_id, source)| project(source_id, source))
        .collect();
    let dynamic_sources = sources
        .dynamic
        .iter()
        .map(|(source_id, source)| project(source_id, source))
        .collect();
    Ok(GraphSchemaSourcesView {
        schema_version: GRAPH_SCHEMA_RESULT_SCHEMA_VERSION,
        graph: graph_name.to_string(),
        core_catalog_digest: crate::graph::current_core_set_digest().to_hex(),
        composed_digest: sources.composed_digest().to_hex(),
        core_sources: eg_types::contract::BoundedVec::new(core_sources)?,
        dynamic_sources: eg_types::contract::BoundedVec::new(dynamic_sources)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::graph_schema::GraphSchemaOp;

    fn pack_document(uri: &str, body: Arc<str>) -> PackSchemaDocument {
        PackSchemaDocument {
            uri: uri.to_string(),
            body,
        }
    }

    #[test]
    fn pack_projection_graph_identity_is_framed_opaque_and_tenant_safe() {
        let graph_os = eg_types::contract::ResourceId::new("graph-os").unwrap();
        let utilities = eg_types::contract::ResourceId::new("agent-utilities").unwrap();
        let golden = pack_projection_graph_name("tenant-a", &graph_os).unwrap();
        assert_eq!(
            golden,
            "pack__710a40e26f5757a7d0ea74593af842cd8444bd62bc8f31e76d2d37f5d5611947"
        );
        assert_eq!(golden.len(), "pack__".len() + 64);
        assert!(!golden.contains("tenant-a") && !golden.contains("graph-os"));
        assert_ne!(
            golden,
            pack_projection_graph_name("tenant-b", &graph_os).unwrap()
        );
        assert_ne!(
            golden,
            pack_projection_graph_name("tenant-a", &utilities).unwrap()
        );
        assert!(pack_projection_graph_name("tenant-a ", &graph_os).is_err());
    }

    fn attach(source_id: &str, ontology_ttl: &str) -> GraphSchemaOp {
        GraphSchemaOp::Attach {
            source_id: source_id.to_string(),
            shapes_ttl: None,
            ontology_ttl: Some(ontology_ttl.to_string()),
            if_composed_digest: None,
        }
    }

    #[test]
    fn attach_list_and_detach_share_one_authoritative_source_set() {
        let core = GraphCore::new();
        let initial = list("g", &core).unwrap();
        let core_ids: std::collections::BTreeSet<_> = initial
            .core_sources
            .iter()
            .filter_map(|source| match &source.origin {
                eg_types::graph_schema::SchemaSourceOriginView::Core { .. } => {
                    Some(source.source_id.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            core_ids,
            std::collections::BTreeSet::from([
                "core:archimate@1",
                "core:capability@1",
                "core:catalog@1",
                "core:enterprise@1",
                "core:foundation@1",
                "core:governance-shapes@1",
            ])
        );
        assert!(initial
            .core_sources
            .iter()
            .all(|source| { source.shapes_sha256.is_some() ^ source.ontology_sha256.is_some() }));
        let ontology = "@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <http://example/> . ex:Local a owl:Class .";
        apply(&core, "g", &attach("admin:local", ontology)).unwrap();
        let listed = list("g", &core).unwrap();
        assert!(listed
            .dynamic_sources
            .iter()
            .any(|source| source.source_id == "admin:local"));
        assert!(listed
            .core_sources
            .iter()
            .any(|source| source.source_id == "core:foundation@1"));

        apply(
            &core,
            "g",
            &GraphSchemaOp::Detach {
                source_id: "admin:local".to_string(),
                if_composed_digest: None,
            },
        )
        .unwrap();
        assert!(!core.schema_sources().dynamic.contains_key("admin:local"));
        assert!(core.schema_sources().core.contains_key("core:foundation@1"));

        apply(&core, "g", &attach("admin:local", ontology)).unwrap();
        assert!(core.schema_sources().dynamic.contains_key("admin:local"));
    }

    #[test]
    fn invalid_attach_and_stale_cas_publish_nothing() {
        let core = GraphCore::new();
        let before = core.schema_sources();
        assert!(apply(&core, "g", &attach("admin:broken", "not turtle")).is_err());
        assert_eq!(core.schema_sources(), before);

        let fenced = GraphSchemaOp::Attach {
            source_id: "admin:fenced".to_string(),
            shapes_ttl: None,
            ontology_ttl: Some("@prefix owl: <http://www.w3.org/2002/07/owl#> .".to_string()),
            if_composed_digest: Some("0".repeat(64)),
        };
        assert!(apply(&core, "g", &fenced)
            .unwrap_err()
            .contains("COMPOSED_DIGEST_MISMATCH"));
        assert_eq!(core.schema_sources(), before);
    }

    #[test]
    fn generic_graph_schema_cannot_detach_or_replace_core() {
        let core = GraphCore::new();
        let before = core.schema_sources();
        for op in [
            attach(
                "core:foundation@1",
                "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
            ),
            GraphSchemaOp::Detach {
                source_id: "core:foundation@1".to_string(),
                if_composed_digest: None,
            },
        ] {
            assert!(apply(&core, "g", &op)
                .unwrap_err()
                .contains("SCHEMA_SOURCE_RESERVED"));
        }
        assert_eq!(core.schema_sources(), before);
    }

    #[test]
    fn internal_pack_projection_owns_identity_and_rejects_regression() {
        let core = GraphCore::new();
        let connector = eg_types::contract::ResourceId::new("graph-os").unwrap();
        let ontology_a = Arc::<str>::from(
            "@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <http://example/> . ex:A a owl:Class .",
        );
        let ontology_b = Arc::<str>::from(
            "@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <http://example/> . ex:B a owl:Class .",
        );

        let first = apply_pack_source(
            &core,
            "pack__graph-os",
            &connector,
            "pack:graph-os:1:aaaa",
            Vec::new(),
            vec![pack_document(
                "ontology://graph-os/a.ttl",
                Arc::clone(&ontology_a),
            )],
            None,
        )
        .unwrap();
        assert!(first.changed);
        let installed = core
            .schema_sources()
            .dynamic
            .get("pack:graph-os")
            .cloned()
            .unwrap();
        assert_eq!(
            installed.origin,
            crate::graph::SchemaSourceOrigin::Pack {
                connector: "graph-os".to_string(),
                record_id: "pack:graph-os:1:aaaa".to_string(),
            }
        );

        let identical = apply_pack_source(
            &core,
            "pack__graph-os",
            &connector,
            "pack:graph-os:1:aaaa",
            Vec::new(),
            vec![pack_document(
                "ontology://graph-os/a.ttl",
                Arc::clone(&ontology_a),
            )],
            Some(&first.composed_digest),
        )
        .unwrap();
        assert!(!identical.changed);
        assert_eq!(identical.composed_digest, first.composed_digest);

        let before = core.schema_sources();
        let error = apply_pack_source(
            &core,
            "pack__graph-os",
            &connector,
            "pack:graph-os:0:bbbb",
            Vec::new(),
            vec![pack_document("ontology://graph-os/b.ttl", ontology_b)],
            None,
        )
        .unwrap_err();
        assert!(error.contains("SCHEMA_SOURCE_REGRESSION"));
        assert_eq!(core.schema_sources(), before);
    }

    #[test]
    fn internal_pack_projection_withdrawal_retains_record_high_water() {
        let core = GraphCore::new();
        let connector = eg_types::contract::ResourceId::new("agent-utilities").unwrap();
        let ontology = Arc::<str>::from(
            "@prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix ex: <http://example/> . ex:A a owl:Class .",
        );
        apply_pack_source(
            &core,
            "pack__test",
            &connector,
            "pack:agent-utilities:1:aaaa",
            Vec::new(),
            vec![pack_document(
                "ontology://agent-utilities/a.ttl",
                Arc::clone(&ontology),
            )],
            None,
        )
        .unwrap();
        let withdrawal = apply_pack_source(
            &core,
            "pack__test",
            &connector,
            "pack:agent-utilities:2:bbbb",
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap();
        assert!(withdrawal.changed);
        let tombstone = core
            .schema_sources()
            .dynamic
            .get("pack:agent-utilities")
            .cloned()
            .unwrap();
        assert!(tombstone.shapes_ttl.is_none() && tombstone.ontology_ttl.is_none());
        let persisted = core.to_msgpack().unwrap();
        let restarted = GraphCore::new();
        restarted.from_msgpack(&persisted).unwrap();
        assert_eq!(restarted.schema_sources(), core.schema_sources());
        assert!(apply_pack_source(
            &restarted,
            "pack__test",
            &connector,
            "pack:agent-utilities:1:cccc",
            Vec::new(),
            vec![pack_document("ontology://agent-utilities/a.ttl", ontology)],
            None,
        )
        .unwrap_err()
        .contains("SCHEMA_SOURCE_REGRESSION"));
    }

    #[test]
    fn pack_members_keep_file_local_blank_nodes_and_uri_order_is_stable() {
        let first = PackSchemaDocument {
            uri: "shapes://test/a.ttl".to_string(),
            body: Arc::from(
                "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
                 @prefix ex: <http://example/> .\n\
                 ex:A a sh:NodeShape ; sh:property _:b0 .\n\
                 _:b0 sh:path ex:a .\n",
            ),
        };
        let second = PackSchemaDocument {
            uri: "shapes://test/b.ttl".to_string(),
            body: Arc::from(
                "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
                 @prefix ex: <http://example/> .\n\
                 ex:B a sh:NodeShape ; sh:property _:b0 .\n\
                 _:b0 sh:path ex:b .\n",
            ),
        };
        let forward = compose_pack_documents("SHAPES", vec![first.clone(), second.clone()])
            .unwrap()
            .unwrap();
        let reverse = compose_pack_documents("SHAPES", vec![second, first])
            .unwrap()
            .unwrap();
        assert_eq!(
            forward, reverse,
            "archive member order cannot change identity"
        );
        assert_eq!(
            eg_types::contract::Digest256::sha256(forward.as_bytes()),
            eg_types::contract::Digest256::sha256(reverse.as_bytes())
        );

        let triples = eg_rdf::mapping::parse_turtle(&forward).unwrap();
        let blank_subjects: std::collections::BTreeSet<_> = triples
            .iter()
            .filter_map(|triple| match &triple.subject {
                eg_rdf::oxrdf::NamedOrBlankNode::BlankNode(node) => Some(node.as_str().to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(
            blank_subjects.len(),
            2,
            "two local _:b0 labels must not merge"
        );
    }

    #[test]
    fn pack_member_failures_name_the_exact_uri_and_enforce_each_bound() {
        let error = compose_pack_documents(
            "ONTOLOGY",
            vec![PackSchemaDocument {
                uri: "ontology://test/broken.ttl".to_string(),
                body: Arc::from("not turtle"),
            }],
        )
        .unwrap_err();
        assert!(error.contains("ontology://test/broken.ttl"));

        let error = compose_pack_documents(
            "SHAPES",
            vec![PackSchemaDocument {
                uri: "shapes://test/oversized.ttl".to_string(),
                body: Arc::from("x".repeat(eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES + 1)),
            }],
        )
        .unwrap_err();
        assert!(error.contains("shapes://test/oversized.ttl"));
        assert!(error.contains("TOO_LARGE"));
    }
}
