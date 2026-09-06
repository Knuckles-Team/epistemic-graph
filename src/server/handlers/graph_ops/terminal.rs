use super::*;

#[derive(Clone, Copy)]
pub(super) struct GraphOpsContext<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) read_authority: &'a GraphReadAuthority,
    pub(super) core: &'a Arc<GraphCore>,
    pub(super) raw_core: &'a Arc<GraphCore>,
    pub(super) raw_ledger_len: u64,
}

pub(super) async fn try_handle_graph_domains(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let mut method = method;
    method = super::nodes::try_handle_node_gateway_writes(ctx, method).await?;
    method = super::nodes::try_handle_node_reads(ctx, method).await?;
    method = super::nodes::try_handle_node_gateway_claims(ctx, method).await?;
    method = super::broker::try_handle_broker_exchange(ctx, method).await?;
    method = super::broker::try_handle_broker_consumption(ctx, method).await?;
    method = super::broker::try_handle_streams(ctx, method).await?;
    method = super::broker::try_handle_publisher_confirms(ctx, method).await?;
    method = super::memory::try_handle_memory_maintenance(ctx, method).await?;
    method = super::memory::try_handle_summary_reads(ctx, method).await?;
    method = super::memory::try_handle_scene_graph(ctx, method).await?;
    method = super::memory::try_handle_trajectory_memory(ctx, method).await?;
    method = super::nodes::try_handle_node_batch(ctx, method).await?;
    method = super::semantic::try_handle_semantic_compute(ctx, method).await?;
    method = super::edges::try_handle_edge_writes(ctx, method).await?;
    method = super::edges::try_handle_edge_reads(ctx, method).await?;
    method = super::edges::try_handle_graph_counts(ctx, method).await?;
    method = super::algorithms::try_handle_graph_algorithms(ctx, method).await?;
    method = super::memory::try_handle_lifecycle_serialization(ctx, method).await?;
    method = super::algorithms::try_handle_neighbor_queries(ctx, method).await?;
    method = super::algorithms::try_handle_centrality_algorithms(ctx, method).await?;
    method = super::algorithms::try_handle_community_algorithms(ctx, method).await?;
    method = super::hierarchy::try_handle_hierarchy_visualization(ctx, method).await?;
    method = super::memory::try_handle_lifecycle_context(ctx, method).await?;
    method = super::memory::try_handle_ledger(ctx, method).await?;
    method = super::subgraph::try_handle_subgraph_reads(ctx, method).await?;
    method = super::union::try_handle_cross_graph_union(ctx, method).await?;
    let _ = super::union::try_handle_subgraph_comparison(ctx, method).await?;
    ControlFlow::Break(Response::err(
        ctx.req_id,
        "Method not available in this server build (unknown method, or a feature — finance/datascience/reasoning/query — not enabled)",
    ))
}
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    // VIZ-1: `ClusterHierarchyRefresh`/`Clusters`/`Expand` key their durable
    // side-cache (`server::persistence::cluster_hierarchy_store`) by graph
    // name — nothing else in this terminal handler previously needed it. Placed
    // BEFORE `read_authority` (not after) so every call site keeps
    // `read_authority,` immediately followed by `core.clone(),` — pinned
    // verbatim by `scripts/check_universal_read_rls.py`'s literal-adjacency
    // check, which a between-them insertion would otherwise break.
    graph_name: &str,
    read_authority: &GraphReadAuthority,
    core: Arc<GraphCore>,
    method: Method,
) -> Response {
    // The mutation ledger is process-observability (an audit/debug trail), not
    // row-visible data — `GraphReadAuthority::build_projection` (D-EGP-1,
    // t1-grounding-0802) deliberately builds the RLS-filtered projection with
    // `add_node_no_ledger`/`add_edge_no_ledger` (an unfiltered ledger cannot be
    // safely row-filtered from its unstructured string form, so the projection
    // carries none at all). `Method::Metrics`'s `total_mutations` must therefore
    // read the ledger length off the RAW core, captured here BEFORE the
    // projection below shadows it — reading it off the projected core would
    // silently report 0 for every graph on every request once `security` is
    // compiled in, regardless of how many mutations actually landed.
    let raw_ledger_len = core.ledger.lock().len() as u64;
    // Kept for the SAME reason as `raw_ledger_len` above, but for a different gap:
    // `GraphReadAuthority::build_projection` builds its filtered view from
    // `core.analysis_snapshot()`, which enumerates only the RAM-resident topology.
    // `EvictLRU`'s per-node residency eviction (`GraphCore::evict_resident_nodes`)
    // fully removes an evicted node from `topo.node_map` — not merely its
    // properties — relying entirely on `read_through_get` for a direct point read.
    // The projection snapshot can therefore never "discover" an evicted-but-durable
    // node's id to re-fetch it, so `Method::GetNodeProperties` falls back to this
    // RAW core (read-through intact) on a projected-core miss, re-applying the
    // SAME single-row RLS check `filter_view` would have (`can_see_node`) so the
    // fallback narrows visibility exactly like the bulk path, never widens it.
    let raw_core = core.clone();
    // Keep the projection inside the terminal handler: any future internal caller
    // must supply a GraphReadAuthority and receives the same pre-compute projection
    // before the first primitive can inspect existence, counts, embeddings, or
    // topology. Query/RDF handlers instead retain their snapshot-level filter.
    // Point membership lookups answer ONE question per id and do not need a
    // materialized projection to do it. `project_core` is
    // `O(V log V + E log E + V*d)` and its cache is keyed on
    // `GraphCore::version()`, so an ingest that interleaves reads and writes
    // misses on nearly every read: measured live at ~1.2s per `HasNode` on a
    // 56k-node graph -- the cost of a full graph dump, to answer a boolean.
    //
    // This MUST sit here, above the projection in the terminal handler, and not
    // in `try_handle_gateway`: `HasNode`/`HasNodesBatch` are plain reads and are
    // never gateway-routed, so an interception there is unreachable for them. A
    // first attempt put it there and changed nothing measurable -- the live
    // `HasNode` count and `projection_cache_miss` count stayed exactly equal.
    //
    // `node_visible` delegates to the SAME `IsolationLayer::can_see_node` that
    // `filter_view` applies per node, so this short-circuit cannot answer
    // differently from the projection it skips.
    match &method {
        Method::HasNode { node_id } => {
            return Response::ok(
                req_id,
                ResultPayload::Bool(read_authority.node_visible(&core, node_id)),
            );
        }
        Method::HasNodesBatch { node_ids } if node_ids.len() <= MAX_BATCH_IDS => {
            let out: Vec<bool> = node_ids
                .iter()
                .map(|id| read_authority.node_visible(&core, id))
                .collect();
            return Response::ok(req_id, ResultPayload::raw(&out));
        }
        _ => {}
    }
    let core = read_authority.project_core(&core);
    let ctx = GraphOpsContext {
        state,
        req_id,
        graph_name,
        read_authority,
        core: &core,
        raw_core: &raw_core,
        raw_ledger_len,
    };
    match try_handle_graph_domains(ctx, method).await {
        ControlFlow::Break(response) => response,
        ControlFlow::Continue(_) => {
            unreachable!("graph-operation domain routing must terminate in its catch-all")
        }
    }
}
