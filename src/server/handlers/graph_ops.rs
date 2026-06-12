//! Graph-targeted operation handlers (node/edge CRUD, embeddings + semantic
//! search, topology/centrality/community algorithms, lifecycle/decay, ledger,
//! reasoning, and cross-graph fork/diff/subgraph-match). These borrow the graph
//! `core` (and, for cross-graph ops, the registry via `state`); heavy reads run
//! off-lock. The dispatch shell owns the cross-cutting write side-effects
//! (dirty/WAL/gauge) — handlers here only produce the `Response`.

use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::access::check_graph_access;
use super::super::compute::{compute_off_lock, weight_semantic_results};
use super::super::state::{ServerState, MAX_BATCH_IDS};
use crate::graph::GraphCore;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};

/// Dispatch a graph-targeted method. This is the terminal handler in the routing
/// chain (it owns the catch-all), so it returns a `Response` directly.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    core: Arc<GraphCore>,
    method: Method,
) -> Response {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            let g = &*core;
            g.add_node(node_id, properties_msgpack);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::RemoveNode { node_id } => {
            let g = &*core;
            g.remove_node(node_id);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::HasNode { node_id } => {
            let g = &*core;
            Response::ok(req_id, ResultPayload::Bool(g.has_node(&node_id)))
        }
        Method::GetNodes => {
            let g = &*core;
            let nodes: Vec<(String, serde_json::Value)> = g
                .get_nodes()
                .into_iter()
                .map(|(k, p)| {
                    let val = rmp_serde::from_slice::<serde_json::Value>(&p)
                        .unwrap_or(serde_json::json!({}));
                    (k, val)
                })
                .collect();
            Response::ok(req_id, ResultPayload::NodeList(nodes))
        }
        Method::GetNodeProperties { node_id } => {
            let g = &*core;
            let val = match g.get_node_properties(&node_id) {
                Some(props_msgpack) => ResultPayload::PropertiesMsgpack(props_msgpack),
                None => ResultPayload::Json(serde_json::Value::Null),
            };
            Response::ok(req_id, val)
        }
        Method::GetNodePropertiesBatch { node_ids } => {
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
            let g = &*core;
            // [id, properties_msgpack | nil] in input order — one round-trip for N
            // nodes; nil preserves which ids were absent. serde_bytes keeps the
            // property blobs as MessagePack `bin`, not int arrays.
            let out: Vec<(String, Option<serde_bytes::ByteBuf>)> = node_ids
                .into_iter()
                .map(|id| {
                    let props = g.get_node_properties(&id).map(serde_bytes::ByteBuf::from);
                    (id, props)
                })
                .collect();
            Response::ok(req_id, ResultPayload::raw(&out))
        }
        Method::HasNodesBatch { node_ids } => {
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
            let g = &*core;
            let out: Vec<bool> = node_ids.iter().map(|id| g.has_node(id)).collect();
            Response::ok(req_id, ResultPayload::raw(&out))
        }
        Method::NodeCount => {
            let g = &*core;
            Response::ok(req_id, ResultPayload::Count(g.node_count() as u64))
        }
        Method::NodeIds => {
            let g = &*core;
            Response::ok(req_id, ResultPayload::Ids(g.node_ids()))
        }
        Method::AddEmbedding { node_id, embedding } => {
            core.semantic_store
                .write()
                .add_embedding(node_id, embedding);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::SemanticSearch {
            query_embedding,
            n_results,
        } => {
            // CONCEPT:KG-2.51 / Phase C-D — the HNSW index is now maintained
            // incrementally, so the ANN query is O(log n): hold the embedding read
            // lock only for the query itself (no whole-store clone, no per-query
            // index rebuild — at most a one-time lazy rebuild after load). The
            // per-candidate Ebbinghaus decay scoring still runs off-lock below.
            // Fetch more results initially to account for filtered-out nodes.
            let raw_results = {
                core.semantic_store
                    .read()
                    .semantic_search(&query_embedding, n_results * 2)
            };

            // Bounded candidate-metadata fetch: only the hit ids, not the graph.
            let candidates: Vec<(String, f32, Option<Vec<u8>>)> = {
                let g = &*core;
                raw_results
                    .into_iter()
                    .map(|(id, sim)| {
                        let props = g.get_node_properties(&id);
                        (id, sim, props)
                    })
                    .collect()
            };

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            match compute_off_lock(req_id, move || {
                weight_semantic_results(candidates, now, n_results)
            })
            .await
            {
                Ok(weighted_results) => Response::ok(req_id, ResultPayload::raw(&weighted_results)),
                Err(resp) => resp,
            }
        }
        Method::SpectralCluster {
            vectors: _,
            max_k: _,
            domain: _,
        } => Response::err(
            req_id,
            "SpectralCluster is deprecated. Use datascience primitives.".to_string(),
        ),
        Method::HypergraphEncodeInteraction {
            pos_a: _,
            pos_b: _,
            pos_dim: _,
            hidden_dim: _,
            out_dim: _,
            seed: _,
        } => Response::err(
            req_id,
            "HypergraphEncodeInteraction is deprecated. Use datascience primitives.".to_string(),
        ),
        Method::BatchCosineSimilarity {
            query: _,
            targets: _,
        } => Response::err(
            req_id,
            "BatchCosineSimilarity is deprecated. Use datascience primitives.".to_string(),
        ),
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let g = &*core;
            match g.add_edge(source_id, target_id, properties_msgpack) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            let g = &*core;
            g.remove_edge(source_id, target_id);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::HasEdge {
            source_id,
            target_id,
        } => {
            let g = &*core;
            Response::ok(
                req_id,
                ResultPayload::Bool(g.has_edge(&source_id, &target_id)),
            )
        }
        Method::GetEdges => {
            let g = &*core;
            Response::ok(req_id, ResultPayload::EdgeList(g.get_edges()))
        }
        Method::GetTriples => {
            // Bulk RDF-triple export for local SPARQL materialization
            // (CONCEPT:KG-2.7). One call instead of per-node round-trips.
            let g = &*core;
            let mut triples: Vec<[String; 3]> = Vec::new();
            // Edges → (subject, predicate=rel_type, object).
            for (src, tgt, props) in g.get_edges() {
                let v: serde_json::Value =
                    rmp_serde::from_slice(&props).unwrap_or(serde_json::json!({}));
                let rel = v
                    .get("type")
                    .and_then(|x| x.as_str())
                    .or_else(|| v.get("rel_type").and_then(|x| x.as_str()))
                    .unwrap_or("RELATED_TO")
                    .to_string();
                triples.push([src, rel, tgt]);
            }
            // Nodes → (id, rdf:type, node_type) + scalar properties as literals.
            for (id, props) in g.get_nodes() {
                let v: serde_json::Value =
                    rmp_serde::from_slice(&props).unwrap_or(serde_json::json!({}));
                if let Some(obj) = v.as_object() {
                    if let Some(nt) = obj
                        .get("type")
                        .or_else(|| obj.get("node_type"))
                        .and_then(|x| x.as_str())
                    {
                        triples.push([id.clone(), "rdf:type".to_string(), nt.to_string()]);
                    }
                    for (k, val) in obj {
                        if k == "type" || k == "node_type" || k == "embedding" {
                            continue;
                        }
                        let lit = match val {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            _ => continue, // skip arrays/objects/null
                        };
                        triples.push([id.clone(), k.clone(), lit]);
                    }
                }
            }
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(triples)))
        }
        Method::GetEdgeProperties {
            source_id,
            target_id,
        } => {
            let g = &*core;
            let props = g.get_edge_properties(&source_id, &target_id);
            let val: Vec<serde_json::Value> = props
                .into_iter()
                .map(|p| rmp_serde::from_slice(&p).unwrap_or(serde_json::json!({})))
                .collect();
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(val)))
        }
        Method::GetEdgePropertiesBatch { edges } => {
            if edges.len() > MAX_BATCH_IDS {
                return Response::err(
                    req_id,
                    format!(
                        "batch too large: {} edges (max {})",
                        edges.len(),
                        MAX_BATCH_IDS
                    ),
                );
            }
            let g = &*core;
            // One round-trip for N edges. Each entry is the list of property blobs
            // for that (src, tgt) pair (a pair may have multiple edges), in input
            // order; an empty inner list ⇒ no such edge.
            let out: Vec<Vec<serde_bytes::ByteBuf>> = edges
                .into_iter()
                .map(|(src, tgt)| {
                    g.get_edge_properties(&src, &tgt)
                        .into_iter()
                        .map(serde_bytes::ByteBuf::from)
                        .collect()
                })
                .collect();
            Response::ok(req_id, ResultPayload::raw(&out))
        }
        Method::ClearGraph => {
            let g = &*core;
            g.clear();
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::EdgeCount => {
            let g = &*core;
            Response::ok(req_id, ResultPayload::Count(g.edge_count() as u64))
        }
        // TopologicalSort / FindCycle / GetShortestPath / components / blast
        // radius / degree centrality are single-pass O(V+E); they run on a cheap
        // topology snapshot (Phase C-B: the read algorithms take an unlocked
        // GraphView, so the structural copy replaces the held read lock).
        Method::TopologicalSort => {
            let g = core.topology_snapshot();
            match crate::algorithms::topological_sort(&g) {
                Ok(order) => Response::ok(req_id, ResultPayload::raw(&order)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::FindCycle => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::find_cycle(&g))),
            )
        }
        Method::GetShortestPath {
            source_id,
            target_id,
        } => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::get_shortest_path(
                    &g, &source_id, &target_id
                ))),
            )
        }
        Method::PageRank {
            damping,
            iterations,
        } => {
            // O(iterations·E) — snapshot topology, compute off-lock (KG-2.51).
            let snap = { core.topology_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::pagerank(&snap, damping, iterations)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::raw(&v)),
                Err(resp) => resp,
            }
        }
        Method::ConnectedComponents => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::connected_components(
                    &g
                ))),
            )
        }
        Method::StronglyConnectedComponents => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(
                    crate::algorithms::strongly_connected_components(&g)
                )),
            )
        }
        Method::MinimumSpanningTree => {
            // O(E log E) + per-edge JSON weight parsing — snapshot (incl. the
            // edge property blobs it reads), compute off-lock (KG-2.51).
            let snap = { core.analysis_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::minimum_spanning_tree(&snap)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::raw(&v)),
                Err(resp) => resp,
            }
        }
        Method::Metrics => {
            // Parses every node's property JSON — memcpy snapshot under the
            // lock is cheaper than O(V) JSON parsing under it (KG-2.51). The
            // ledger is not snapshotted; capture its length for the
            // total_mutations field before releasing the lock.
            let (snap, ledger_len) = {
                let g = &*core;
                (g.analysis_snapshot(), g.ledger.lock().len() as u64)
            };
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
            match serde_json::to_value(&m) {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::EvictLRU { max_nodes } => {
            let g = &*core;
            let evicted = g.evict_lru(max_nodes);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(evicted)))
        }
        Method::DecaySweep {
            half_life_secs,
            floor,
            prune,
        } => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let g = &*core;
            let stats = g.decay_sweep(now, half_life_secs, floor, prune);
            match serde_json::to_value(&stats) {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::TouchNodes { node_ids } => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let g = &*core;
            let touched = g.touch_nodes(&node_ids, now);
            Response::ok(req_id, ResultPayload::Count(touched as u64))
        }
        Method::ToMsgpack => {
            let g = &*core;
            match g.to_msgpack() {
                Ok(json) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(json))),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::FromMsgpack { msgpack } => {
            let g = &*core;
            match g.from_msgpack(&msgpack) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::Reconcile {
            graph_name: _,
            msgpack,
        } => {
            let g = &*core;
            match g.from_msgpack(&msgpack) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("reconciled".to_string())),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::ApplyMutation { event_type, query } => {
            let g = &*core;

            // Very rudimentary parsing to apply SPARQL changes to petgraph
            // In a production system, a full SPARQL AST parser would be used here.
            let is_insert = event_type == "TRIPLE_INSERT";

            // Extract triples from "INSERT DATA { <A> <B> <C> }" using naive string splitting
            // Format assumed: <s1> <p1> <o1> . <s2> <p2> <o2>
            if let Some(brace_start) = query.find('{') {
                if let Some(brace_end) = query.rfind('}') {
                    let inner = &query[brace_start + 1..brace_end];
                    let triples = inner.split('.');
                    for t in triples {
                        let tokens: Vec<&str> = t.split_whitespace().collect();
                        if tokens.len() >= 3 {
                            let s = tokens[0].trim_matches(|c| c == '<' || c == '>');
                            let p = tokens[1].trim_matches(|c| c == '<' || c == '>');
                            let o = tokens[2].trim_matches(|c| c == '<' || c == '>');

                            if is_insert {
                                g.add_node(s.to_string(), "{}".to_string().into_bytes());
                                g.add_node(o.to_string(), "{}".to_string().into_bytes());
                                let _ = g.add_edge(
                                    s.to_string(),
                                    o.to_string(),
                                    format!("{{\"predicate\": \"{}\"}}", p).into_bytes(),
                                );
                            } else {
                                // For delete, we just remove the edge matching the predicate
                                g.remove_edge(s.to_string(), o.to_string());
                            }
                        }
                    }
                }
            }
            Response::ok(
                req_id,
                ResultPayload::String("mutation_applied".to_string()),
            )
        }
        // CONCEPT:KG-2.17 - Compiled Semantic Reasoner. Forward-chaining
        // OWL/RDFS inference over the target graph. Runs Datalog reasoning
        // (subclass / subproperty / symmetric / transitive / inverse) and,
        // when supplied, domain/range and property-chain inference. All
        // inferred edges and type annotations are materialised in-place and
        // the inferred triples are returned to the caller.
        //
        // Intentionally under the write lock (KG-2.51): reasoning MUTATES the
        // graph as it infers, so it cannot run on a snapshot — materialising
        // on a clone and merging back would cost more than the inference.
        // Feature-gated: excluded from a slim (no `reasoning`) build, where the
        // variant falls to the catch-all "not available in this build" error.
        #[cfg(feature = "reasoning")]
        Method::RunDatalogReasoning {
            subclass_relations,
            subproperty_relations,
            symmetric_properties,
            transitive_properties,
            inverse_properties,
            domain_rules,
            range_rules,
            property_chains,
        } => {
            let g = &*core;
            let mut all_inferred: Vec<std::collections::HashMap<String, String>> = Vec::new();

            match crate::reasoning::run_datalog_reasoning(
                g,
                subclass_relations,
                subproperty_relations,
                symmetric_properties,
                transitive_properties,
                inverse_properties,
            ) {
                Ok(triples) => all_inferred.extend(triples),
                Err(e) => return Response::err(req_id, e),
            }

            if !domain_rules.is_empty() || !range_rules.is_empty() {
                all_inferred.extend(crate::reasoning::infer_domain_range(
                    g,
                    domain_rules,
                    range_rules,
                ));
            }

            if !property_chains.is_empty() {
                all_inferred.extend(crate::reasoning::infer_property_chains(g, property_chains));
            }

            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "inferred_count": all_inferred.len(),
                    "inferred_triples": all_inferred,
                })),
            )
        }
        Method::InDegree { node_id } => {
            let g = &*core;
            match g.in_degree(&node_id) {
                Ok(deg) => Response::ok(req_id, ResultPayload::Count(deg as u64)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::OutDegree { node_id } => {
            let g = &*core;
            match g.out_degree(&node_id) {
                Ok(deg) => Response::ok(req_id, ResultPayload::Count(deg as u64)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetPredecessors { node_id } => {
            let g = &*core;
            match g.get_predecessors(&node_id) {
                Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetSuccessors { node_id } => {
            let g = &*core;
            match g.get_successors(&node_id) {
                Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetNeighbors { node_id } => {
            let g = &*core;
            match g.get_neighbors(&node_id) {
                Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetBlastRadius { node_id, max_depth } => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::get_blast_radius(
                    &g, &node_id, max_depth
                ))),
            )
        }
        Method::DegreeCentrality { node_id } => {
            let g = core.topology_snapshot();
            match crate::algorithms::compute_degree_centrality(&g, &node_id) {
                Ok(val) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(val))),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::DegreeCentralityAll => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::degree_centrality_all(
                    &g
                ))),
            )
        }
        Method::BetweennessCentrality => {
            // O(V·E) Brandes — snapshot topology, compute off-lock (KG-2.51).
            let snap = { core.topology_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::betweenness_centrality(&snap)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::raw(&v)),
                Err(resp) => resp,
            }
        }
        Method::PersonalizedPageRank {
            seed_nodes,
            damping,
            iterations,
        } => {
            // O(iterations·E) — snapshot topology, compute off-lock (KG-2.51).
            let snap = { core.topology_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::personalized_pagerank(&snap, &seed_nodes, damping, iterations)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::raw(&v)),
                Err(resp) => resp,
            }
        }
        Method::CommunityDetection { resolution } => {
            // Label propagation has an internal 15s wall-clock budget — that
            // budget used to burn entirely UNDER the read lock, stalling every
            // writer on the graph. Snapshot topology, compute off-lock
            // (KG-2.51).
            let snap = { core.topology_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::community_detection(&snap, resolution)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::raw(&v)),
                Err(resp) => resp,
            }
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
        } => {
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
                Ok(v) => Response::ok(req_id, ResultPayload::raw(&v)),
                Err(resp) => resp,
            }
        }
        // GraphColoring: greedy coloring is a single O(V+E) sweep over a cheap
        // topology snapshot (Phase C-B: read algorithms take an unlocked view).
        Method::GraphColoring => {
            let g = core.topology_snapshot();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::graph_coloring(&g))),
            )
        }
        Method::ComputeSimilarityEdges { threshold } => {
            // O(V²·d) all-pairs cosine on rayon — must never run under the
            // graph lock OR on the tokio runtime threads. Snapshot the
            // property blobs it reads, compute off-lock (KG-2.51).
            let snap = { core.analysis_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::compute_similarity_edges(&snap, threshold)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::raw(&v)),
                Err(resp) => resp,
            }
        }
        Method::PruneByLifecycle {
            max_age_secs,
            min_score,
        } => {
            let stats = crate::algorithms::prune_by_lifecycle(&core, max_age_secs, min_score);
            match serde_json::to_value(&stats) {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::GetContextView {
            agent_id,
            max_tokens,
        } => {
            let g = core.analysis_snapshot();
            let view = crate::algorithms::get_context_view(&g, &agent_id, max_tokens);
            match serde_json::to_value(&view) {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::BatchUpdate { operations_msgpack } => {
            match crate::algorithms::batch_update(&core, &operations_msgpack) {
                Ok(res) => match rmp_serde::from_slice::<serde_json::Value>(&res) {
                    Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req_id, format!("Invalid batch result: {}", e)),
                },
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::ParseRepository { root_path } => {
            let g = &*core;
            match g.parse_repository(&root_path) {
                Ok(_) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::Vf2SubgraphMatch { pattern_graph_name } => {
            let s = state.read().await;
            // The pattern graph is read too — gate it like any other read.
            if let Some(entry) = s.registry.get(&pattern_graph_name) {
                if let Err(denied) = check_graph_access(
                    &s.isolation,
                    caller,
                    &pattern_graph_name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Read,
                ) {
                    return Response::err(req_id, denied);
                }
            }
            let pattern_core = s
                .registry
                .get(&pattern_graph_name)
                .map(|entry| entry.core.clone());
            drop(s);
            if let Some(p_core) = pattern_core {
                // Exponential-worst-case matching never runs under either
                // graph's lock: snapshot pattern then host SEQUENTIALLY (no
                // nested cross-graph locks), compute off-lock (KG-2.51).
                let p_snap = p_core.analysis_snapshot();
                // vf2_subgraph_match snapshots the host internally, so the
                // exponential-worst-case matching runs entirely off-lock.
                let host = core.clone();
                match compute_off_lock(req_id, move || host.vf2_subgraph_match(&p_snap)).await {
                    Ok(v) => Response::ok(req_id, ResultPayload::raw(&v)),
                    Err(resp) => resp,
                }
            } else {
                Response::err(
                    req_id,
                    format!("Pattern graph '{}' not found", pattern_graph_name),
                )
            }
        }
        Method::GetLedger => {
            let g = &*core;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(g.get_ledger())),
            )
        }

        Method::ClearLedger => {
            let g = &*core;
            g.clear_ledger();
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::ApplyLedger { transactions } => {
            let g = &*core;
            match g.apply_ledger(transactions) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetSubgraph { node_ids } => {
            // Batched subgraph read: return the induced nodes (with DECODED
            // properties) and the edges among them in ONE round-trip, so callers
            // never loop per-node `GetNodeProperties` or pull the whole edge set.
            // (Previously serialized to msgpack then mis-parsed as JSON → error.)
            let g = &*core;
            let sub = g.get_subgraph(&node_ids);
            let mut nodes = Vec::with_capacity(sub.node_properties.len());
            for (id, blob) in &sub.node_properties {
                let props: serde_json::Value =
                    rmp_serde::from_slice(blob).unwrap_or(serde_json::Value::Null);
                nodes.push(serde_json::json!({ "id": id, "properties": props }));
            }
            let mut edges = Vec::new();
            for ((src, tgt), blobs) in &sub.edge_properties {
                for blob in blobs {
                    let props: serde_json::Value =
                        rmp_serde::from_slice(blob).unwrap_or(serde_json::Value::Null);
                    edges.push(serde_json::json!({
                        "source": src, "target": tgt, "properties": props
                    }));
                }
            }
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({ "nodes": nodes, "edges": edges })),
            )
        }
        Method::Fork => {
            // Cannot return the forked GraphCore directly because it needs to be registered.
            // A true fork method in the registry might be better.
            // For now, we return the JSON representation of the fork.
            let g = &*core;
            let sub = g.fork();
            match sub.to_msgpack() {
                Ok(json) => match serde_json::from_slice::<serde_json::Value>(&json) {
                    Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req_id, e.to_string()),
                },
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::DiffAgainst { other_graph } => {
            let s_lock = state.read().await;
            let other_entry = match s_lock.registry.get(&other_graph) {
                Some(e) => e,
                None => {
                    return Response::err(
                        req_id,
                        format!("Other graph '{}' not found", other_graph),
                    )
                }
            };
            // Diffing reads the other graph's content — gate it as a read.
            if let Err(denied) = check_graph_access(
                &s_lock.isolation,
                caller,
                &other_graph,
                other_entry.graph_type,
                other_entry.owner.as_deref(),
                AccessLevel::Read,
            ) {
                return Response::err(req_id, denied);
            }
            let other_core = other_entry.core.clone();
            drop(s_lock);

            // Snapshot the other graph first, then diff under only THIS
            // graph's lock — never hold two graph locks at once (two
            // concurrent opposite-direction diffs plus a queued writer can
            // deadlock a write-preferring RwLock). The diff itself is a
            // single O(V+E) comparison, so it stays under-lock (KG-2.51).
            let other_snap = { other_core.analysis_snapshot() };
            let g1 = &*core;
            let diff_str = g1.diff_against(&other_snap);
            match serde_json::from_slice::<serde_json::Value>(diff_str.as_bytes()) {
                Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::CompactNodesByType {
            node_type,
            threshold,
        } => {
            let g = &*core;
            let removed = g.compact_nodes_by_type(&node_type, threshold);
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({ "removed_nodes": removed })),
            )
        }
        // Catch-all: an unknown graph method, OR a compute method whose feature
        // (finance / datascience / reasoning) was not built into this server.
        _ => Response::err(
            req_id,
            "Method not available in this server build (unknown method, or a \
             compute feature — finance/datascience/reasoning — not enabled)",
        ),
    }
}
