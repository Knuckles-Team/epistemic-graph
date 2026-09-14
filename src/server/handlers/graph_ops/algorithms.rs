use super::*;

use super::terminal::GraphOpsContext;
use eg_types::result_contract::compute as results;

/// `InDegree`: pure extract-method from `try_handle`'s match arm, byte-identical
/// behaviour, no signature change.
fn handle_in_degree(req_id: u64, core: &Arc<GraphCore>, node_id: &str) -> Response {
    let g = &**core;
    match g.in_degree(node_id) {
        Ok(deg) => Response::ok(req_id, ResultPayload::Count(deg as u64)),
        Err(e) => Response::err(req_id, e),
    }
}

/// `OutDegree`: pure extract-method from `try_handle`'s match arm, byte-identical
/// behaviour, no signature change.
fn handle_out_degree(req_id: u64, core: &Arc<GraphCore>, node_id: &str) -> Response {
    let g = &**core;
    match g.out_degree(node_id) {
        Ok(deg) => Response::ok(req_id, ResultPayload::Count(deg as u64)),
        Err(e) => Response::err(req_id, e),
    }
}

/// `GetPredecessors`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_get_predecessors(req_id: u64, core: &Arc<GraphCore>, node_id: &str) -> Response {
    let g = &**core;
    match g.get_predecessors(node_id) {
        Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
        Err(e) => Response::err(req_id, e),
    }
}

/// `GetSuccessors`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_get_successors(req_id: u64, core: &Arc<GraphCore>, node_id: &str) -> Response {
    let g = &**core;
    match g.get_successors(node_id) {
        Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
        Err(e) => Response::err(req_id, e),
    }
}

/// `GetNeighbors`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_get_neighbors(req_id: u64, core: &Arc<GraphCore>, node_id: &str) -> Response {
    let g = &**core;
    match g.get_neighbors(node_id) {
        Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
        Err(e) => Response::err(req_id, e),
    }
}

/// `DegreeCentrality`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_degree_centrality(req_id: u64, core: &Arc<GraphCore>, node_id: &str) -> Response {
    let g = core.topology_snapshot();
    match crate::algorithms::compute_degree_centrality(&g, node_id) {
        Ok(val) => Response::ok(
            req_id,
            ResultPayload::scalar::<results::DegreeCentrality>(val),
        ),
        Err(e) => Response::err(req_id, e),
    }
}

/// `GetNeighborsBatch`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_get_neighbors_batch(
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
    // [node_id, Vec<neighbor_id>] in input order — one round-trip and one
    // topo-lock acquisition for N nodes (D-DPF-1) instead of N of each.
    let out = g.get_neighbors_batch(node_ids);
    Response::ok(
        req_id,
        ResultPayload::of_ref::<eg_types::result_contract::graph::GetNeighborsBatch>(&out),
    )
}

/// `BetweennessCentrality`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_betweenness_centrality(req_id: u64, core: &Arc<GraphCore>) -> Response {
    // O(V·E) Brandes — snapshot topology, compute off-lock (KG-2.51).
    let snap = { core.topology_snapshot() };
    match compute_off_lock(req_id, move || {
        crate::algorithms::betweenness_centrality(&snap)
    })
    .await
    {
        Ok(v) => Response::ok(
            req_id,
            ResultPayload::of::<results::BetweennessCentrality>(v),
        ),
        Err(resp) => resp,
    }
}

/// `PersonalizedPageRank`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_personalized_page_rank(
    req_id: u64,
    core: &Arc<GraphCore>,
    seed_nodes: Vec<(String, f64)>,
    damping: f64,
    iterations: usize,
) -> Response {
    // O(iterations·E) — snapshot topology, compute off-lock (KG-2.51).
    let snap = { core.topology_snapshot() };
    match compute_off_lock(req_id, move || {
        crate::algorithms::personalized_pagerank(&snap, &seed_nodes, damping, iterations)
    })
    .await
    {
        Ok(v) => Response::ok(
            req_id,
            ResultPayload::of::<results::PersonalizedPageRank>(v),
        ),
        Err(resp) => resp,
    }
}

/// `CommunityDetection`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_community_detection(
    req_id: u64,
    core: &Arc<GraphCore>,
    resolution: f64,
) -> Response {
    // Louvain — NOT label propagation, as this comment used to claim: the method
    // routes to `algorithms::community_detection`, which is the Louvain kernel,
    // and so did the hand-rolled `community_detection` that carried the original
    // `COMMUNITY_DETECTION_BUDGET`. That 15s wall-clock budget now lives in the
    // kernel loop itself (`LouvainConfig::budget`), which is the only place that
    // can actually STOP the work — a handler-side timeout would bound the
    // response while the compute thread kept burning CPU. Commit `a14b9c28`
    // deleted the budget along with the duplicate kernel and left this comment
    // describing a bound that no longer existed.
    //
    // That budget used to burn entirely UNDER the read lock, stalling every
    // writer on the graph. Snapshot topology, compute off-lock (KG-2.51).
    let snap = { core.topology_snapshot() };
    match compute_off_lock(req_id, move || {
        crate::algorithms::community_detection(&snap, resolution)
    })
    .await
    {
        Ok(v) => Response::ok(req_id, ResultPayload::of::<results::CommunityDetection>(v)),
        Err(resp) => resp,
    }
}

/// `ComputeSimilarityEdges`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_compute_similarity_edges(
    req_id: u64,
    core: &Arc<GraphCore>,
    threshold: f64,
) -> Response {
    // O(V²·d) all-pairs cosine on rayon — must never run under the
    // graph lock OR on the tokio runtime threads. Snapshot the
    // property blobs it reads, compute off-lock (KG-2.51).
    let snap = { core.analysis_snapshot() };
    match compute_off_lock(req_id, move || {
        crate::algorithms::compute_similarity_edges(&snap, threshold)
    })
    .await
    {
        Ok(v) => Response::ok(
            req_id,
            ResultPayload::of::<results::ComputeSimilarityEdges>(v),
        ),
        Err(resp) => resp,
    }
}

/// `ResolveCandidates`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_resolve_candidates(
    req_id: u64,
    core: &Arc<GraphCore>,
    sim_threshold: f64,
    merge_threshold: f64,
    node_type: Option<String>,
) -> Response {
    // Entity-resolution candidate generation (KG-2.260): embedding
    // similarity + clustering composed into one READ/propose op. Same
    // off-lock discipline as ComputeSimilarityEdges (it shares the O(V²)
    // cosine pass). Returns merge proposals; never mutates the graph.
    let snap = { core.analysis_snapshot() };
    match compute_off_lock(req_id, move || {
        crate::algorithms::resolve_candidates(
            &snap,
            sim_threshold,
            merge_threshold,
            node_type.as_deref(),
        )
    })
    .await
    {
        Ok(v) => Response::ok(
            req_id,
            ResultPayload::of::<results::ResolveCandidates>(
                v.into_iter()
                    .map(|proposal| results::MergeProposal {
                        canonical: proposal.canonical,
                        members: proposal.members,
                        score: proposal.score,
                        kind: proposal.kind,
                    })
                    .collect(),
            ),
        ),
        Err(resp) => resp,
    }
}

/// `TopologicalSort`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_topological_sort(req_id: u64, core: &Arc<GraphCore>) -> Response {
    let g = core.topology_snapshot();
    match crate::algorithms::topological_sort(&g) {
        Ok(order) => Response::ok(req_id, ResultPayload::of::<results::TopologicalSort>(order)),
        Err(e) => Response::err(req_id, e.to_string()),
    }
}

/// `PageRank`: pure extract-method from `try_handle`'s match arm, byte-identical
/// behaviour, no signature change.
async fn handle_page_rank(
    req_id: u64,
    core: &Arc<GraphCore>,
    damping: f64,
    iterations: usize,
) -> Response {
    // O(iterations·E) — snapshot topology, compute off-lock (KG-2.51).
    let snap = { core.topology_snapshot() };
    match compute_off_lock(req_id, move || {
        crate::algorithms::pagerank(&snap, damping, iterations)
    })
    .await
    {
        Ok(v) => Response::ok(req_id, ResultPayload::of::<results::PageRank>(v)),
        Err(resp) => resp,
    }
}

/// `MinimumSpanningTree`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_minimum_spanning_tree(req_id: u64, core: &Arc<GraphCore>) -> Response {
    // O(E log E) + per-edge JSON weight parsing — snapshot (incl. the
    // edge property blobs it reads), compute off-lock (KG-2.51).
    let snap = { core.analysis_snapshot() };
    match compute_off_lock(req_id, move || {
        crate::algorithms::minimum_spanning_tree(&snap)
    })
    .await
    {
        Ok(v) => Response::ok(req_id, ResultPayload::of::<results::MinimumSpanningTree>(v)),
        Err(resp) => resp,
    }
}

/// `Metrics`: pure extract-method from `try_handle`'s match arm, byte-identical
/// behaviour, no signature change.
async fn handle_metrics(req_id: u64, core: &Arc<GraphCore>, raw_ledger_len: u64) -> Response {
    // Parses every node's property JSON — memcpy snapshot under the
    // lock is cheaper than O(V) JSON parsing under it (KG-2.51). The
    // ledger is not snapshotted; `raw_ledger_len` (captured off the
    // UN-projected core, above) is the total_mutations field — the
    // projected `core` in scope here carries no ledger at all (see the
    // comment above `project_core`).
    let snap = core.analysis_snapshot();
    let ledger_len = raw_ledger_len;
    let m = match compute_off_lock(req_id, move || {
        let mut m = crate::algorithms::compute_metrics(&snap);
        m.total_mutations = ledger_len;
        m
    })
    .await
    {
        Ok(m) => m,
        Err(resp) => return resp,
    };
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::graph::Metrics>(m),
    )
}

/// `CommunityDetectEphemeral`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_community_detect_ephemeral(
    req_id: u64,
    node_ids: Vec<String>,
    edges: Vec<(String, String)>,
    resolution: f64,
) -> Response {
    match compute_off_lock(req_id, move || {
        let g = crate::graph::GraphCore::new();
        for id in &node_ids {
            g.add_node(id.clone(), Vec::new());
        }
        for (s, t) in &edges {
            let _ = g.add_edge(s.clone(), t.clone(), Vec::new());
        }
        crate::algorithms::community_detection(&g.analysis_snapshot(), resolution)
    })
    .await
    {
        Ok(v) => Response::ok(
            req_id,
            ResultPayload::of::<results::CommunityDetectEphemeral>(v),
        ),
        Err(resp) => resp,
    }
}

/// Handle graph algorithms and metrics.
pub(super) async fn try_handle_graph_algorithms(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext {
        req_id,
        core,
        raw_ledger_len,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::TopologicalSort => handle_topological_sort(req_id, core),
        Method::FindCycle => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::of::<results::FindCycle>(crate::algorithms::find_cycle(&g)),
            )
        }
        Method::GetShortestPath {
            source_id,
            target_id,
        } => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::of::<results::GetShortestPath>(
                    crate::algorithms::get_shortest_path(&g, &source_id, &target_id),
                ),
            )
        }
        Method::PageRank {
            damping,
            iterations,
        } => handle_page_rank(req_id, core, damping, iterations).await,
        Method::ConnectedComponents => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::of::<results::ConnectedComponents>(
                    crate::algorithms::connected_components(&g),
                ),
            )
        }
        Method::StronglyConnectedComponents => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::of::<results::StronglyConnectedComponents>(
                    crate::algorithms::strongly_connected_components(&g),
                ),
            )
        }
        Method::MinimumSpanningTree => handle_minimum_spanning_tree(req_id, core).await,
        Method::Metrics => handle_metrics(req_id, core, raw_ledger_len).await,
        // EvictLRU/DecaySweep/TouchNodes/FromMsgpack/Reconcile (CONCEPT:EG-P0-2
        // bypass guard, L11): GATEWAY_ROUTED — see the AddNode/RemoveNode
        // comment above. `ToMsgpack` is a pure read and keeps its normal arm.
        other => return ControlFlow::Continue(other),
    })
}

/// Handle graph neighbor queries.
pub(super) async fn try_handle_neighbor_queries(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::InDegree { node_id } => handle_in_degree(req_id, core, &node_id),
        Method::OutDegree { node_id } => handle_out_degree(req_id, core, &node_id),
        Method::GetPredecessors { node_id } => handle_get_predecessors(req_id, core, &node_id),
        Method::GetSuccessors { node_id } => handle_get_successors(req_id, core, &node_id),
        Method::GetNeighbors { node_id } => handle_get_neighbors(req_id, core, &node_id),
        Method::GetNeighborsBatch { node_ids } => {
            handle_get_neighbors_batch(req_id, core, node_ids)
        }
        other => return ControlFlow::Continue(other),
    })
}

/// Handle centrality and blast-radius algorithms.
pub(super) async fn try_handle_centrality_algorithms(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::GetBlastRadius { node_id, max_depth } => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::of::<results::GetBlastRadius>(crate::algorithms::get_blast_radius(
                    &g, &node_id, max_depth,
                )),
            )
        }
        Method::DegreeCentrality { node_id } => handle_degree_centrality(req_id, core, &node_id),
        Method::DegreeCentralityAll => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::of::<results::DegreeCentralityAll>(
                    crate::algorithms::degree_centrality_all(&g),
                ),
            )
        }
        Method::BetweennessCentrality => handle_betweenness_centrality(req_id, core).await,
        Method::PersonalizedPageRank {
            seed_nodes,
            damping,
            iterations,
        } => handle_personalized_page_rank(req_id, core, seed_nodes, damping, iterations).await,
        other => return ControlFlow::Continue(other),
    })
}

/// Handle community and similarity algorithms.
pub(super) async fn try_handle_community_algorithms(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::CommunityDetection { resolution } => {
            handle_community_detection(req_id, core, resolution).await
        }
        // Stateless community detection over an inline call graph — no tenant load,
        // no persistence, no graph lock. Builds a throwaway in-memory graph from the
        // passed nodes/edges and runs detection off-reactor. Replaces the prior
        // "bulk-load ~160k edges into a scratch tenant, detect, delete tenant"
        // round-trip (the dominant ingest community cost + the tenant-sprawl source).
        Method::CommunityDetectEphemeral {
            node_ids,
            edges,
            resolution,
        } => handle_community_detect_ephemeral(req_id, node_ids, edges, resolution).await,
        // GraphColoring: greedy coloring is a single O(V+E) sweep over a cheap
        // topology snapshot (Phase C-B: read algorithms take an unlocked view).
        Method::GraphColoring => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::of::<results::GraphColoring>(crate::algorithms::graph_coloring(&g)),
            )
        }
        Method::ComputeSimilarityEdges { threshold } => {
            handle_compute_similarity_edges(req_id, core, threshold).await
        }
        Method::ResolveCandidates {
            sim_threshold,
            merge_threshold,
            node_type,
        } => {
            handle_resolve_candidates(req_id, core, sim_threshold, merge_threshold, node_type).await
        }
        // ── VIZ-1: hierarchical cluster-tree RPCs (CONCEPT:EG-KG.compute.leiden-hierarchy) ──
        // Same off-lock discipline as CommunityDetection/ResolveCandidates above —
        // `analysis_snapshot` under the topo READ lock, the actual clustering runs
        // on the blocking pool. See `Method::ClusterHierarchyRefresh`'s doc for why
        // the persisted result is NOT a graph mutation (no GATEWAY_ROUTED routing,
        // no WAL/CDC/audit — a plain durable side-cache keyed by graph name).
        other => return ControlFlow::Continue(other),
    })
}
