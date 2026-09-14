use super::*;

/// Multi-tenant graph lifecycle: create, delete and list graphs.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_graph_lifecycle_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Multi-tenant graph management ────────────────────────────
        Method::CreateGraph {
            graph_name,
            graph_type,
        } => {
            dispatch_boxed(create_graph(
                state,
                req.id,
                req.agent_id.clone(),
                verified_context.attempt_nonce(),
                verified_context.idempotency_key().to_string(),
                graph_name,
                graph_type,
            ))
            .await
        }

        Method::DeleteGraph { graph_name } => {
            dispatch_boxed(delete_graph(
                state,
                req.id,
                req.agent_id.clone(),
                verified_context.attempt_nonce(),
                verified_context.idempotency_key().to_string(),
                state_machine_authorized,
                &graph_name,
            ))
            .await
        }

        Method::ListGraphs => dispatch_boxed(list_graphs(state, verified_context, req.id)).await,
        other => return ControlFlow::Continue(other),
    })
}

/// `ListGraphs`: every graph the verified caller may read, with its materialization
/// and index readiness.
async fn list_graphs(
    state: &Arc<RwLock<ServerState>>,
    verified_context: &VerifiedRequestContext,
    req_id: u64,
) -> Response {
    let s = timed_read(state).await;
    let read_authority = match GraphReadAuthority::from_verified(verified_context, &s.isolation) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    let graphs: Vec<eg_types::result_contract::cluster::GraphListing> = s
        .registry
        .list()
        .iter()
        .filter(|(name, _)| {
            s.registry.get(name).is_some_and(|entry| {
                check_graph_access(
                    &s.isolation,
                    read_authority.actor(),
                    name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Read,
                )
                .is_ok()
            })
        })
        .map(|(name, graph_type)| graph_listing(&s.registry, name, *graph_type))
        .collect();
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::cluster::ListGraphs>(graphs),
    )
}

/// One readable graph's listing: its materialization readiness and index manifests.
fn graph_listing(
    registry: &crate::registry::GraphRegistry,
    name: &str,
    graph_type: crate::protocol::GraphType,
) -> eg_types::result_contract::cluster::GraphListing {
    let readiness = registry.materialization_manifest(name);
    let index_manifests = registry
        .get(name)
        .map(|entry| {
            entry
                .core
                .indexes()
                .server_manifests()
                .into_iter()
                .map(|(kind, manifest)| index_manifest_listing(kind, manifest))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    eg_types::result_contract::cluster::GraphListing {
        name: name.to_string(),
        graph_type,
        materialization: readiness
            .as_ref()
            .map(|value| materialization_phase(&value.phase)),
        source_snapshot_version: readiness
            .as_ref()
            .and_then(|value| value.source_snapshot_version),
        completeness_cursor: readiness
            .as_ref()
            .and_then(|value| value.completeness_cursor.as_ref())
            .map(
                |cursor| eg_types::result_contract::cluster::MaterializationCursor {
                    node_offset: cursor.node_offset as u64,
                    edge_offset: cursor.edge_offset as u64,
                },
            ),
        valid: readiness.as_ref().is_some_and(|value| value.valid),
        index_manifests,
    }
}

fn materialization_phase(
    phase: &crate::registry::MaterializationPhase,
) -> eg_types::result_contract::cluster::MaterializationPhase {
    match phase {
        crate::registry::MaterializationPhase::CatalogOnly => {
            eg_types::result_contract::cluster::MaterializationPhase::CatalogOnly
        }
        crate::registry::MaterializationPhase::Partial => {
            eg_types::result_contract::cluster::MaterializationPhase::Partial
        }
        crate::registry::MaterializationPhase::Complete => {
            eg_types::result_contract::cluster::MaterializationPhase::Complete
        }
        crate::registry::MaterializationPhase::Failed => {
            eg_types::result_contract::cluster::MaterializationPhase::Failed
        }
    }
}

fn index_manifest_listing(
    kind: crate::index::IndexKind,
    manifest: crate::index::IndexManifest,
) -> eg_types::result_contract::cluster::IndexManifestListing {
    eg_types::result_contract::cluster::IndexManifestListing {
        kind: index_kind_label(kind).to_string(),
        source_snapshot_version: manifest.source_snapshot_version,
        build_version: manifest.build_version,
        completeness_cursor: eg_types::result_contract::cluster::IndexCompleteness {
            nodes: manifest.completeness.nodes,
            edges: manifest.completeness.edges,
            complete: manifest.completeness.complete,
        },
        validity: index_validity_label(manifest.validity).to_string(),
    }
}
