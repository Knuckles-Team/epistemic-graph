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

        Method::ListGraphs => {
            dispatch_boxed(async {
    let req_id = req.id;
    {
            let s = timed_read(state).await;
            let read_authority =
                match GraphReadAuthority::from_verified(verified_context, &s.isolation) {
                    Ok(authority) => authority,
                    Err(denied) => return Response::err(req_id, denied),
                };
            let graphs: Vec<serde_json::Value> = s
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
                .map(|(name, gt)| {
                    let readiness = s.registry.materialization_manifest(name);
                    let indexes = s.registry.get(name).map(|entry| {
                        entry
                            .core
                            .indexes()
                            .server_manifests()
                            .into_iter()
                            .map(|(kind, manifest)| {
                                serde_json::json!({
                                    "kind": index_kind_label(kind),
                                    "source_snapshot_version": manifest.source_snapshot_version,
                                    "build_version": manifest.build_version,
                                    "completeness_cursor": {
                                        "nodes": manifest.completeness.nodes,
                                        "edges": manifest.completeness.edges,
                                        "complete": manifest.completeness.complete,
                                    },
                                    "validity": index_validity_label(manifest.validity),
                                })
                            })
                            .collect::<Vec<_>>()
                    });
                    serde_json::json!({
                        "name": name,
                        "type": gt,
                        "materialization": readiness.as_ref().map(|value| match value.phase {
                            crate::registry::MaterializationPhase::CatalogOnly => "catalog_only",
                            crate::registry::MaterializationPhase::Partial => "partial",
                            crate::registry::MaterializationPhase::Complete => "complete",
                            crate::registry::MaterializationPhase::Failed => "failed",
                        }),
                        "source_snapshot_version": readiness.as_ref().and_then(|value| value.source_snapshot_version),
                        "completeness_cursor": readiness.as_ref().and_then(|value| value.completeness_cursor.as_ref()).map(|cursor| {
                            serde_json::json!({
                                "node_offset": cursor.node_offset,
                                "edge_offset": cursor.edge_offset,
                            })
                        }),
                        "valid": readiness.as_ref().is_some_and(|value| value.valid),
                        "index_manifests": indexes.unwrap_or_default(),
                    })
                })
                .collect();
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(graphs)))
        }
})
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}
