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
        } => match lifecycle_request(req, verified_context) {
            Ok(request) => {
                dispatch_boxed(create_graph(state, request, graph_name, graph_type)).await
            }
            Err(denied) => denied,
        },

        Method::DeleteGraph { graph_name } => match lifecycle_request(req, verified_context) {
            Ok(request) => {
                dispatch_boxed(delete_graph(
                    state,
                    request,
                    state_machine_authorized,
                    &graph_name,
                ))
                .await
            }
            Err(denied) => denied,
        },

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
    let graphs: Vec<medulla_types::result_contract::cluster::GraphListing> = s
        .registry
        .list()
        .iter()
        .filter(|(name, _)| {
            // The catalog record, not the resident entry (#709): after a
            // restart every recovered graph is catalog-only until first
            // access, and `registry.get` is resident-only, so deciding from it
            // dropped every graph from the listing without an ACL decision.
            // The record keeps the graph's type and owner, so the decision is
            // the same one a resident graph gets.
            s.registry.catalog_record(name).is_some_and(|entry| {
                match check_graph_access(
                    &s.isolation,
                    read_authority.actor(),
                    name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Read,
                ) {
                    Ok(()) => true,
                    Err(reason) => {
                        // A filtered graph is not an error to the caller, but the
                        // reason must not vanish (#709): an unprovisioned policy or
                        // an unregistered caller drops every graph at once. The
                        // reason names the refusal class and the graph only.
                        tracing::debug!(
                            target: "medulla::access::list_graphs",
                            graph = name.as_str(),
                            graph_type = ?entry.graph_type,
                            reason = %reason,
                            "graph filtered from ListGraphs"
                        );
                        false
                    }
                }
            })
        })
        .map(|(name, graph_type)| graph_listing(&s.registry, name, *graph_type))
        .collect();
    Response::ok(
        req_id,
        ResultPayload::of::<medulla_types::result_contract::cluster::ListGraphs>(graphs),
    )
}

/// One readable graph's listing: its materialization readiness and index manifests.
fn graph_listing(
    registry: &crate::registry::GraphRegistry,
    name: &str,
    graph_type: crate::protocol::GraphType,
) -> medulla_types::result_contract::cluster::GraphListing {
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
    medulla_types::result_contract::cluster::GraphListing {
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
                |cursor| medulla_types::result_contract::cluster::MaterializationCursor {
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
) -> medulla_types::result_contract::cluster::MaterializationPhase {
    match phase {
        crate::registry::MaterializationPhase::CatalogOnly => {
            medulla_types::result_contract::cluster::MaterializationPhase::CatalogOnly
        }
        crate::registry::MaterializationPhase::Partial => {
            medulla_types::result_contract::cluster::MaterializationPhase::Partial
        }
        crate::registry::MaterializationPhase::Complete => {
            medulla_types::result_contract::cluster::MaterializationPhase::Complete
        }
        crate::registry::MaterializationPhase::Failed => {
            medulla_types::result_contract::cluster::MaterializationPhase::Failed
        }
    }
}

fn index_manifest_listing(
    kind: crate::index::IndexKind,
    manifest: crate::index::IndexManifest,
) -> medulla_types::result_contract::cluster::IndexManifestListing {
    medulla_types::result_contract::cluster::IndexManifestListing {
        kind: index_kind_label(kind).to_string(),
        source_snapshot_version: manifest.source_snapshot_version,
        build_version: manifest.build_version,
        completeness_cursor: medulla_types::result_contract::cluster::IndexCompleteness {
            nodes: manifest.completeness.nodes,
            edges: manifest.completeness.edges,
            complete: manifest.completeness.complete,
        },
        validity: index_validity_label(manifest.validity).to_string(),
    }
}

/// The verified lifecycle identity of `req` (EH-375: tenant scope from the verified
/// carrier), or the refusal response.
fn lifecycle_request(
    req: &DispatchHeader,
    verified_context: &VerifiedRequestContext,
) -> Result<GraphLifecycleRequest, Response> {
    GraphLifecycleRequest::verified(
        req.id,
        req.agent_id.clone(),
        verified_context,
        verified_context.idempotency_key().to_string(),
    )
    .map_err(|denied| Response::err(req.id, denied))
}
