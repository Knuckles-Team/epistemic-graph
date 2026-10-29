use super::*;

use super::terminal::GraphOpsContext;

/// Intelligent overload backstop for the `GetNodes` full-graph dump
/// (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation). Given the graph's node `count` and the configured `cap`,
/// returns `Some(error_message)` when the dump would exceed the cap (so the
/// handler can refuse with a typed `RESULT_TOO_LARGE` error instead of building
/// a gigabyte-scale frame that resets the client connection), or `None` when the
/// dump is within bounds and safe to materialize. The cap is always positive in
/// served state and cannot be disabled. Pure + side-effect-free so the threshold
/// logic is unit-tested directly, independent of process-global env.
fn oversize_dump_error(count: usize, cap: usize) -> Option<String> {
    if count > cap {
        Some(format!(
            "RESULT_TOO_LARGE: GetNodes would return {count} nodes (> cap {cap}); \
             the full-graph dump is refused to protect the connection. Use a \
             bounded query instead (get_nodes_by_label(label, limit) or \
             paginate), or raise EPISTEMIC_GRAPH_MAX_RESPONSE_NODES."
        ))
    } else {
        None
    }
}

/// `GetNodePropertiesBatch`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_get_node_properties_batch(
    req_id: u64,
    core: &Arc<GraphCore>,
    node_ids: Vec<String>,
) -> Response {
    if node_ids.len() > MAX_BATCH_IDS {
        return Response::err(
            req_id,
            format!(
                "batch too large: {} ids (max {})",
                node_ids.len(),
                MAX_BATCH_IDS
            ),
        );
    }
    let g = &**core;
    // [id, properties_msgpack | nil] in input order — one round-trip for N
    // nodes; nil preserves which ids were absent. serde_bytes keeps the
    // property blobs as MessagePack `bin`, not int arrays.
    let out: Vec<(String, Option<eg_types::types::PropertyBlob>)> = node_ids
        .into_iter()
        .map(|id| {
            let props = g
                .get_node_properties(&id)
                .map(eg_types::types::PropertyBlob);
            (id, props)
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::of_ref::<eg_types::result_contract::graph::GetNodePropertiesBatch>(&out),
    )
}

/// `HasNodesBatch`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_has_nodes_batch(req_id: u64, core: &Arc<GraphCore>, node_ids: &[String]) -> Response {
    if node_ids.len() > MAX_BATCH_IDS {
        return Response::err(
            req_id,
            format!(
                "batch too large: {} ids (max {})",
                node_ids.len(),
                MAX_BATCH_IDS
            ),
        );
    }
    let g = &**core;
    let out: Vec<bool> = node_ids.iter().map(|id| g.has_node(id)).collect();
    Response::ok(
        req_id,
        ResultPayload::of_ref::<eg_types::result_contract::graph::HasNodesBatch>(&out),
    )
}

/// `GetNodeProperties`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_get_node_properties(
    req_id: u64,
    core: &Arc<GraphCore>,
    raw_core: &Arc<GraphCore>,
    read_authority: &GraphReadAuthority,
    node_id: &str,
) -> Response {
    let g = &**core;
    // The RLS projection is RAM-topology-only and can never see a node
    // `EvictLRU` fully evicted from the live topology (see `raw_core`'s
    // doc comment above) — fall back to the RAW core, whose
    // `read_through` seam is intact, then re-check row visibility on
    // exactly this one row before returning it.
    //
    // BUG A3 (2026-08-12): TBox membership is DERIVED from
    // `raw_core.is_schema_node`, not decoded from the blob
    // (see `can_see_node`'s doc) — `node_id` is exactly the
    // one row this fallback is checking, so the live lookup
    // is as cheap as reading a single DashMap entry.
    let props = g.get_node_properties(node_id).or_else(|| {
        raw_core
            .get_node_properties(node_id)
            .filter(|props| read_authority.can_see_node(props, raw_core.is_schema_node(node_id)))
    });
    Response::ok(
        req_id,
        ResultPayload::of_encoded_or_null::<eg_types::result_contract::graph::GetNodeProperties>(
            props,
        ),
    )
}

/// Route gateway-owned node creation and removal operations.
pub(super) async fn try_handle_node_gateway_writes(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let _ = ctx;
    match method {
        Method::AddNode { .. } => unreachable!(
            "AddNode is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
                 through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::CreateNodeIfAbsent { .. } => unreachable!(
            "CreateNodeIfAbsent is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
                 through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::RemoveNode { .. } => unreachable!(
            "RemoveNode is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
                 through try_handle_gateway before it ever reaches this terminal handler"
        ),
        other => ControlFlow::Continue(other),
    }
}

/// Handle point and paginated node reads.
pub(super) async fn try_handle_node_reads(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext {
        req_id,
        read_authority,
        core,
        raw_core,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::HasNode { node_id } => {
            let g = core;
            Response::ok(req_id, ResultPayload::Bool(g.has_node(&node_id)))
        }
        Method::GetNodes => {
            let g = core;
            // Intelligent overload backstop (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation): a `GetNodes` is an
            // UNBOUNDED full-graph dump. On a large graph (e.g. `__commons__` with
            // 166K+ nodes carrying 1024-dim embeddings) materializing every node's
            // properties into ONE response frame is a gigabyte-scale payload that
            // overruns/resets the client connection. Check the cheap topology count
            // BEFORE building the Vec, and return a typed, catchable error instead of
            // the pathological frame. The bounded reads (`GetNodesByLabel`, per-id)
            // are intentionally unaffected.
            if let Some(msg) = oversize_dump_error(g.node_count(), max_response_nodes()) {
                return ControlFlow::Break(Response::err(req_id, msg));
            }
            let nodes: Vec<(String, serde_json::Value)> = g
                .get_nodes()
                .into_iter()
                .map(|(k, p)| {
                    let val = eg_types::msgpack::decode_property_value(&p)
                        .unwrap_or(serde_json::json!({}));
                    (k, val)
                })
                .collect();
            Response::ok(req_id, ResultPayload::NodeList(nodes))
        }
        Method::GetNodesByLabel {
            label,
            after,
            limit,
        } => {
            let g = core;
            let nodes: Vec<(String, serde_json::Value)> = g
                .get_nodes_by_label_page(&label, after.as_deref(), limit)
                .into_iter()
                .map(|(k, p)| {
                    let val = eg_types::msgpack::decode_property_value(&p)
                        .unwrap_or(serde_json::json!({}));
                    (k, val)
                })
                .collect();
            Response::ok(req_id, ResultPayload::NodeList(nodes))
        }
        Method::GetNodeProperties { node_id } => {
            handle_get_node_properties(req_id, core, raw_core, read_authority, &node_id)
        }
        // CompareAndSetNodeFields/ClaimNext (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        other => return ControlFlow::Continue(other),
    })
}

/// Route gateway-owned node coordination operations.
pub(super) async fn try_handle_node_gateway_claims(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let _ = ctx;
    match method {
        Method::CompareAndSetNodeFields { .. } => unreachable!(
            "CompareAndSetNodeFields is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
                 route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::ClaimNext { .. } => unreachable!(
            "ClaimNext is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
                 through try_handle_gateway before it ever reaches this terminal handler"
        ),
        // ── Message broker admin + data (CONCEPT:EG-KG.compute.message-broker-exchanges) ─────────────────
        // Built on the KG-2.303 queue: exchanges/bindings are nodes on this target
        // graph; publish routes + enqueues; consume/ack REUSE ClaimNext + CAS above.
        // Same handler home + precedent as ClaimNext. Gated `broker`; a slim build
        // drops the variants (they fall to the catch-all "not available").
        // Broker/stream admin+data family (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above. `StreamRead`/
        // `StreamCommittedOffset` are pure reads (not in GATEWAY_ROUTED) and keep
        // their normal arms below.
        other => ControlFlow::Continue(other),
    }
}

/// Handle batched and indexed node reads.
pub(super) async fn try_handle_node_batch(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::GetNodePropertiesBatch { node_ids } => {
            handle_get_node_properties_batch(req_id, core, node_ids)
        }
        Method::HasNodesBatch { node_ids } => handle_has_nodes_batch(req_id, core, &node_ids),
        Method::NodeCount => {
            let g = core;
            Response::ok(req_id, ResultPayload::Count(g.node_count() as u64))
        }
        Method::NodeIds => {
            let g = core;
            Response::ok(req_id, ResultPayload::Ids(g.node_ids()))
        }
        Method::MatchOntologyTerms { query } => {
            // CONCEPT:EG-ORCH.routing.lexical-capability-escalation — lexical capability gate; cached aho-corasick scan.
            let g = core;
            Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::compute::MatchOntologyTerms>(
                    g.match_ontology_terms(&query),
                ),
            )
        }
        // AddEmbedding (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED — see
        // the AddNode/RemoveNode comment above.
        other => return ControlFlow::Continue(other),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn under_cap_returns_no_error_so_data_is_served() {
        // A graph at or below the cap is dumped normally — the guard is inert.
        assert_eq!(oversize_dump_error(0, 50_000), None);
        assert_eq!(oversize_dump_error(1, 50_000), None);
        assert_eq!(oversize_dump_error(49_999, 50_000), None);
        // Exactly at the cap is allowed (the guard fires only when EXCEEDED).
        assert_eq!(oversize_dump_error(50_000, 50_000), None);
    }

    #[test]
    fn over_cap_returns_typed_error_not_a_giant_payload() {
        // The pathological full-graph dump (e.g. 166K __commons__ nodes with
        // 1024-dim embeddings) is refused with a clean, catchable error rather
        // than serialized into one gigabyte-scale frame that resets the client.
        let err = oversize_dump_error(166_000, 50_000)
            .expect("over-cap dump must produce an error, not the data");
        assert!(err.starts_with("RESULT_TOO_LARGE"), "got: {err}");
        // The message must steer the caller to the bounded alternative.
        assert!(err.contains("get_nodes_by_label"), "got: {err}");
        assert!(
            err.contains("166000") && err.contains("50000"),
            "got: {err}"
        );
    }

    #[test]
    fn zero_cap_fails_safe() {
        assert!(oversize_dump_error(1, 0).is_some());
    }

    #[cfg(feature = "security")]
    #[test]
    fn evicted_point_read_stays_graph_scoped_and_rechecks_row_visibility() {
        #[derive(Debug)]
        struct FixedReadThrough(Vec<u8>);

        impl eg_core::read_through::ReadThrough for FixedReadThrough {
            fn read_node_blob(&self, node_id: &str) -> Option<Vec<u8>> {
                (node_id == "shared-node").then(|| self.0.clone())
            }
        }

        fn blob(owner: &str, visibility: Option<&str>, marker: &str) -> Vec<u8> {
            let mut properties = serde_json::Map::new();
            if let Some(visibility) = visibility {
                properties.insert("_owner".into(), serde_json::json!(owner));
                properties.insert("_visibility".into(), serde_json::json!(visibility));
            } else {
                properties.insert("_owner_id".into(), serde_json::json!(owner));
            }
            properties.insert("marker".into(), serde_json::json!(marker));
            rmp_serde::to_vec_named(&serde_json::Value::Object(properties)).unwrap()
        }

        fn cold_graph(properties: Vec<u8>) -> Arc<GraphCore> {
            let core = Arc::new(GraphCore::new());
            core.set_read_through(Arc::new(FixedReadThrough(properties)));
            core
        }

        let isolation = crate::server::access::two_agent_isolation();
        let context = crate::server::auth::VerifiedRequestContext::verified_for_test("alice");
        let authority = GraphReadAuthority::from_verified(&context, &isolation).unwrap();
        let filtered_projection = Arc::new(GraphCore::new());

        // A cache miss for the same ID must resolve through each graph's own
        // read-through authority, never another graph's durable row.
        for (owner, marker) in [("store-a", "graph-a"), ("store-b", "graph-b")] {
            let properties = blob(owner, None, marker);
            let core = cold_graph(properties.clone());
            let response = handle_get_node_properties(
                7,
                &filtered_projection,
                &core,
                &authority,
                "shared-node",
            );
            assert_eq!(response.error, None);
            assert!(
                matches!(response.result, Some(ResultPayload::Raw(bytes)) if bytes == properties)
            );
        }

        // The raw-core fallback must reapply the verified actor's row policy;
        // a private row owned by another actor is indistinguishable from absence.
        let hidden = blob("bob", Some("private"), "bob-private");
        let core = cold_graph(hidden);
        let response =
            handle_get_node_properties(8, &filtered_projection, &core, &authority, "shared-node");
        assert_eq!(response.error, None);
        assert!(matches!(
            response.result,
            Some(ResultPayload::Json(serde_json::Value::Null))
        ));
    }
}
