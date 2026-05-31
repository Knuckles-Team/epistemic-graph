// CONCEPT:KG-2.19 — Tokio Service Server
//
// Long-running Tokio server that holds the GraphRegistry in memory
// and serves requests over UDS or TCP with HMAC-SHA256 authentication.

use std::sync::Arc;
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::channels::ChannelManager;
use crate::isolation::IsolationLayer;
use crate::protocol::{Method, Request, Response};
use crate::registry::GraphRegistry;

/// Shared server state behind Arc<RwLock<>>.
pub struct ServerState {
    pub registry: GraphRegistry,
    pub isolation: IsolationLayer,
    pub channels: ChannelManager,
    pub auth_secret: String,
    pub persist_dir: Option<String>,
}

/// Verify HMAC-SHA256 authentication token.
fn verify_auth(secret: &str, request_id: u64, token: &str) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    if secret.is_empty() {
        return true; // No auth configured.
    }

    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(request_id.to_string().as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());
    token == expected
}

/// Dispatch a single request to the appropriate handler.
pub async fn dispatch(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
) -> Response {
    // Auth check.
    {
        let s = state.read().await;
        if !verify_auth(&s.auth_secret, req.id, &req.auth_token) {
            return Response::err(req.id, "Authentication failed");
        }
    }

    match req.method {
        // ── Service-level ────────────────────────────────────────────
        Method::Ping => Response::ok(req.id, serde_json::json!("pong")),

        Method::ParseFile { file_path, source } => {
            #[cfg(feature = "ast")]
            match crate::parser::tree_sitter::parse_file(&file_path, &source) {
                Ok(result) => match serde_json::to_value(&result) {
                    Ok(val) => Response::ok(req.id, val),
                    Err(e) => Response::err(req.id, format!("Serialization error: {}", e)),
                },
                Err(e) => Response::err(req.id, e),
            }
            #[cfg(not(feature = "ast"))]
            {
                let _ = (file_path, source);
                Response::err(req.id, "AST feature not enabled".to_string())
            }
        }

        Method::Shutdown => {
            info!("Shutdown requested via protocol");
            Response::ok(req.id, serde_json::json!("shutting_down"))
        }

        Method::Checkpoint => {
            info!("Checkpoint requested");
            // Checkpoint logic would serialize all graphs to disk here.
            Response::ok(req.id, serde_json::json!("checkpoint_complete"))
        }

        // ── Multi-tenant graph management ────────────────────────────
        Method::CreateGraph { graph_name, graph_type } => {
            let mut s = state.write().await;
            match s.registry.create_graph(&graph_name, graph_type, None) {
                Ok(()) => Response::ok(req.id, serde_json::json!({"created": graph_name})),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::DeleteGraph { graph_name } => {
            let mut s = state.write().await;
            match s.registry.delete_graph(&graph_name) {
                Ok(()) => Response::ok(req.id, serde_json::json!({"deleted": graph_name})),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::ListGraphs => {
            let s = state.read().await;
            let graphs: Vec<serde_json::Value> = s.registry.list().iter().map(|(name, gt)| {
                serde_json::json!({"name": name, "type": gt})
            }).collect();
            Response::ok(req.id, serde_json::json!(graphs))
        }

        // ── Channel operations ───────────────────────────────────────
        Method::CreateChannel { channel_id, channel_type, creator, initial_members } => {
            let mut s = state.write().await;
            match s.channels.create_channel(&channel_id, channel_type, &creator, initial_members) {
                Ok(()) => Response::ok(req.id, serde_json::json!({"channel": channel_id})),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::JoinChannel { channel_id, agent_id } => {
            let mut s = state.write().await;
            match s.channels.join_channel(&channel_id, &agent_id) {
                Ok(()) => Response::ok(req.id, serde_json::json!("joined")),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::LeaveChannel { channel_id, agent_id } => {
            let mut s = state.write().await;
            match s.channels.leave_channel(&channel_id, &agent_id) {
                Ok(imprint) => {
                    let val = match imprint {
                        Some(imp) => serde_json::to_value(&imp).unwrap_or(serde_json::json!("closed")),
                        None => serde_json::json!("left"),
                    };
                    Response::ok(req.id, val)
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::CloseChannel { channel_id, summary_embedding, topic_metadata } => {
            let mut s = state.write().await;
            match s.channels.close_channel(&channel_id, summary_embedding, topic_metadata) {
                Ok(imprint) => {
                    let val = match imprint {
                        Some(imp) => serde_json::to_value(&imp).unwrap_or(serde_json::json!("closed")),
                        None => serde_json::json!("closed"),
                    };
                    Response::ok(req.id, val)
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::SendMessage { channel_id, sender, payload } => {
            let mut s = state.write().await;
            match s.channels.send_message(&channel_id, &sender, &payload) {
                Ok(()) => Response::ok(req.id, serde_json::json!("sent")),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::GetChannelMessages { channel_id, limit } => {
            let s = state.read().await;
            match s.channels.get_messages(&channel_id, limit) {
                Ok(msgs) => {
                    let val: Vec<serde_json::Value> = msgs.iter().map(|m| {
                        serde_json::json!({"sender": m.sender, "payload": m.payload, "timestamp": m.timestamp})
                    }).collect();
                    Response::ok(req.id, serde_json::json!(val))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::ListChannels => {
            let s = state.read().await;
            let channels: Vec<serde_json::Value> = s.channels.list_channels().iter().map(|(id, ct, members)| {
                serde_json::json!({"id": id, "type": ct, "members": members})
            }).collect();
            Response::ok(req.id, serde_json::json!(channels))
        }

        Method::GetChannelMembers { channel_id } => {
            let s = state.read().await;
            match s.channels.get_members(&channel_id) {
                Ok(members) => Response::ok(req.id, serde_json::json!(members)),
                Err(e) => Response::err(req.id, e),
            }
        }

        // ── Zero-Trust Consensus ─────────────────────────────────────────
        Method::RegisterIdentity { agent_id, role, teams, signature } => {
            info!("RegisterIdentity: agent_id={}, role={:?}, signature={}", agent_id, role, signature);
            let mut s = state.write().await;
            s.isolation.register_agent(crate::isolation::AgentIdentity {
                agent_id: agent_id.clone(),
                role,
                teams,
            });
            Response::ok(req.id, serde_json::json!("registered"))
        }

        Method::ApplyMultisigMutation { signatures, threshold, mutation_type, query } => {
            if signatures.len() < threshold {
                return Response::err(req.id, format!("Insufficient signatures: {} < {}", signatures.len(), threshold));
            }
            // Delegate mutation application to the target graph
            dispatch_graph_op(state, &req.graph, req.id, Method::ApplyMutation {
                event_type: mutation_type,
                query,
            }).await
        }

        // ── Graph operations (dispatch to target graph) ──────────────
        _ => {
            dispatch_graph_op(state, &req.graph, req.id, req.method).await
        }
    }
}

/// Dispatch a graph-level operation to the target named graph.
async fn dispatch_graph_op(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    req_id: u64,
    method: Method,
) -> Response {
    let s = state.read().await;
    let entry = match s.registry.get(graph_name) {
        Some(e) => e,
        None => return Response::err(req_id, format!("Graph '{}' not found", graph_name)),
    };

    let core = entry.core.clone();
    drop(s); // Release registry lock before graph lock.

    match method {
        Method::AddNode { node_id, properties_msgpack} => {
            let mut g = core.write().await;
            g.add_node(node_id, properties_msgpack);
            Response::ok(req_id, serde_json::json!("ok"))
        }
        Method::RemoveNode { node_id } => {
            let mut g = core.write().await;
            g.remove_node(node_id);
            Response::ok(req_id, serde_json::json!("ok"))
        }
        Method::HasNode { node_id } => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(g.has_node(&node_id)))
        }
        Method::GetNodes => {
            let g = core.read().await;
            let nodes: Vec<(String, serde_json::Value)> = g.get_nodes()
                .into_iter()
                .map(|(k, p)| {
                    let val = rmp_serde::from_slice::<serde_json::Value>(&p).unwrap_or(serde_json::json!({}));
                    (k, val)
                })
                .collect();
            Response::ok(req_id, serde_json::json!(nodes))
        }
        Method::GetNodeProperties { node_id } => {
            let g = core.read().await;
            let val = match g.get_node_properties(&node_id) {
                Some(props_msgpack) => {
                    rmp_serde::from_slice::<serde_json::Value>(&props_msgpack).unwrap_or(serde_json::json!({}))
                }
                None => serde_json::Value::Null,
            };
            Response::ok(req_id, val)
        }
        Method::NodeCount => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(g.node_count()))
        }
        Method::NodeIds => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(g.node_ids()))
        }
        Method::AddEmbedding { node_id, embedding } => {
            let mut g = core.write().await;
            g.semantic_store.add_embedding(node_id, embedding);
            Response::ok(req_id, serde_json::json!("ok"))
        }
        Method::SemanticSearch { query_embedding, n_results } => {
            let g = core.read().await;
            // Fetch more results initially to account for filtered out nodes
            let raw_results = g.semantic_store.semantic_search(&query_embedding, n_results * 2);

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let mut weighted_results = Vec::new();
            for (node_id, mut similarity) in raw_results {
                if let Some(props_bytes) = g.get_node_properties(&node_id) {
                    if let Ok(json_str) = String::from_utf8(props_bytes) {
                        let node_data = crate::types::NodeData::from_json_props(node_id.clone(), &json_str);

                        // Filter out strictly stale facts where the validity window has closed
                        if let Some(vu) = node_data.valid_until {
                            if now > vu {
                                continue;
                            }
                        }

                        // Apply temporal decay to confidence (Ebbinghaus Forgetting Curve)
                        let mut current_confidence = node_data.confidence;
                        if let Some(vf) = node_data.valid_from {
                            if now > vf {
                                let age_secs = now - vf;
                                let age_days = age_secs as f64 / 86400.0;
                                // Half-life of 30 days (decay rate lambda = ln(2) / 30)
                                let decay_rate = 0.693147 / 30.0;
                                current_confidence *= (-decay_rate * age_days).exp();
                            }
                        }

                        // Adjust similarity by current confidence (salience)
                        similarity *= current_confidence as f32;
                    }
                }
                weighted_results.push((node_id, similarity));
            }

            // Re-sort descending based on the new confidence-weighted similarity
            weighted_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            weighted_results.truncate(n_results);

            Response::ok(req_id, serde_json::json!(weighted_results))
        }
        Method::SpectralCluster { vectors: _, max_k: _, domain: _ } => {
            Response::err(req_id, "SpectralCluster is deprecated. Use datascience primitives.".to_string())
        }
        Method::HypergraphEncodeInteraction { pos_a: _, pos_b: _, pos_dim: _, hidden_dim: _, out_dim: _, seed: _ } => {
            Response::err(req_id, "HypergraphEncodeInteraction is deprecated. Use datascience primitives.".to_string())
        }
        Method::BatchCosineSimilarity { query: _, targets: _ } => {
            Response::err(req_id, "BatchCosineSimilarity is deprecated. Use datascience primitives.".to_string())
        }
        Method::FinanceOptimizePortfolio { expected_returns, cov_matrix, risk_free_rate } => {
            let result = crate::finance::optimizer::mean_variance_optimization(&expected_returns, &cov_matrix, risk_free_rate);
            Response::ok(req_id, serde_json::json!(result))
        }
        Method::FindSimilarPairs { embeddings: _, ids: _, threshold: _, use_lsh: _, lsh_num_tables: _, lsh_hash_size: _, seed: _ } => {
            Response::err(req_id, "FindSimilarPairs is deprecated. Use datascience primitives.".to_string())
        }
        Method::AddEdge { source_id, target_id, properties_msgpack} => {
            let mut g = core.write().await;
            match g.add_edge(source_id, target_id, properties_msgpack) {
                Ok(()) => Response::ok(req_id, serde_json::json!("ok")),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::RemoveEdge { source_id, target_id } => {
            let mut g = core.write().await;
            g.remove_edge(source_id, target_id);
            Response::ok(req_id, serde_json::json!("ok"))
        }
        Method::HasEdge { source_id, target_id } => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(g.has_edge(&source_id, &target_id)))
        }
        Method::GetEdges => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(g.get_edges()))
        }
        Method::GetEdgeProperties { source_id, target_id } => {
            let g = core.read().await;
            let props = g.get_edge_properties(&source_id, &target_id);
            let val: Vec<serde_json::Value> = props.into_iter()
                .map(|p| rmp_serde::from_slice(&p).unwrap_or(serde_json::json!({})))
                .collect();
            Response::ok(req_id, serde_json::json!(val))
        }
        Method::ClearGraph => {
            let mut g = core.write().await;
            g.clear();
            Response::ok(req_id, serde_json::json!("ok"))
        }
        Method::EdgeCount => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(g.edge_count()))
        }
        Method::TopologicalSort => {
            let g = core.read().await;
            match crate::algorithms::topological_sort(&g) {
                Ok(order) => Response::ok(req_id, serde_json::json!(order)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::FindCycle => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(crate::algorithms::find_cycle(&g)))
        }
        Method::GetShortestPath { source_id, target_id } => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(
                crate::algorithms::get_shortest_path(&g, &source_id, &target_id)
            ))
        }
        Method::PageRank { damping, iterations } => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(
                crate::algorithms::pagerank(&g, damping, iterations)
            ))
        }
        Method::ConnectedComponents => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(
                crate::algorithms::connected_components(&g)
            ))
        }
        Method::StronglyConnectedComponents => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(
                crate::algorithms::strongly_connected_components(&g)
            ))
        }
        Method::MinimumSpanningTree => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(
                crate::algorithms::minimum_spanning_tree(&g)
            ))
        }
        Method::Metrics => {
            let g = core.read().await;
            let m = crate::algorithms::compute_metrics(&g);
            match serde_json::to_value(&m) {
                Ok(v) => Response::ok(req_id, v),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::EvictLRU { max_nodes } => {
            let mut g = core.write().await;
            let evicted = g.evict_lru(max_nodes);
            Response::ok(req_id, serde_json::json!(evicted))
        }
        Method::ToMsgpack => {
            let g = core.read().await;
            match g.to_msgpack() {
                Ok(json) => Response::ok(req_id, serde_json::json!(json)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::FromMsgpack { msgpack } => {
            let mut g = core.write().await;
            match g.from_msgpack(&msgpack) {
                Ok(()) => Response::ok(req_id, serde_json::json!("ok")),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::Reconcile { graph_name: _, msgpack } => {
            let mut g = core.write().await;
            match g.from_msgpack(&msgpack) {
                Ok(()) => Response::ok(req_id, serde_json::json!("reconciled")),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::ApplyMutation { event_type, query } => {
            let mut g = core.write().await;

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
                                let _ = g.add_edge(s.to_string(), o.to_string(), format!("{{\"predicate\": \"{}\"}}", p).into_bytes());
                            } else {
                                // For delete, we just remove the edge matching the predicate
                                g.remove_edge(s.to_string(), o.to_string());
                            }
                        }
                    }
                }
            }
            Response::ok(req_id, serde_json::json!("mutation_applied"))
        }
        Method::InDegree { node_id } => {
            let g = core.read().await;
            match g.in_degree(&node_id) {
                Ok(deg) => Response::ok(req_id, serde_json::json!(deg)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::OutDegree { node_id } => {
            let g = core.read().await;
            match g.out_degree(&node_id) {
                Ok(deg) => Response::ok(req_id, serde_json::json!(deg)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetPredecessors { node_id } => {
            let g = core.read().await;
            match g.get_predecessors(&node_id) {
                Ok(nodes) => Response::ok(req_id, serde_json::json!(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetSuccessors { node_id } => {
            let g = core.read().await;
            match g.get_successors(&node_id) {
                Ok(nodes) => Response::ok(req_id, serde_json::json!(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetNeighbors { node_id } => {
            let g = core.read().await;
            match g.get_neighbors(&node_id) {
                Ok(nodes) => Response::ok(req_id, serde_json::json!(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetBlastRadius { node_id, max_depth } => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(crate::algorithms::get_blast_radius(&g, &node_id, max_depth)))
        }
        Method::DegreeCentrality { node_id } => {
            let g = core.read().await;
            match crate::algorithms::compute_degree_centrality(&g, &node_id) {
                Ok(val) => Response::ok(req_id, serde_json::json!(val)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::DegreeCentralityAll => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(crate::algorithms::degree_centrality_all(&g)))
        }
        Method::BetweennessCentrality => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(crate::algorithms::betweenness_centrality(&g)))
        }
        Method::PersonalizedPageRank { seed_nodes, damping, iterations } => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(crate::algorithms::personalized_pagerank(&g, &seed_nodes, damping, iterations)))
        }
        Method::CommunityDetection { resolution } => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(crate::algorithms::community_detection(&g, resolution)))
        }
        Method::GraphColoring => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(crate::algorithms::graph_coloring(&g)))
        }
        Method::ComputeSimilarityEdges { threshold } => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(crate::algorithms::compute_similarity_edges(&g, threshold)))
        }
        Method::PruneByLifecycle { max_age_secs, min_score } => {
            let mut g = core.write().await;
            let stats = crate::algorithms::prune_by_lifecycle(&mut g, max_age_secs, min_score);
            match serde_json::to_value(&stats) {
                Ok(v) => Response::ok(req_id, v),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::GetContextView { agent_id, max_tokens } => {
            let g = core.read().await;
            let view = crate::algorithms::get_context_view(&g, &agent_id, max_tokens);
            match serde_json::to_value(&view) {
                Ok(v) => Response::ok(req_id, v),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::BatchUpdate { operations_msgpack } => {
            let mut g = core.write().await;
            match crate::algorithms::batch_update(&mut g, &operations_msgpack) {
                Ok(res) => match rmp_serde::from_slice::<serde_json::Value>(&res) {
                    Ok(val) => Response::ok(req_id, val),
                    Err(e) => Response::err(req_id, format!("Invalid batch result: {}", e)),
                },
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::ParseRepository { root_path } => {
            let mut g = core.write().await;
            match g.parse_repository(&root_path) {
                Ok(_) => Response::ok(req_id, serde_json::json!("ok")),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::Vf2SubgraphMatch { pattern_graph_name } => {
            let s = state.read().await;
            let pattern_core = s.registry.get(&pattern_graph_name).map(|entry| entry.core.clone());
            drop(s);
            if let Some(p_core) = pattern_core {
                let p = p_core.read().await;
                let g = core.read().await;
                Response::ok(req_id, serde_json::json!(g.vf2_subgraph_match(&p)))
            } else {
                Response::err(req_id, format!("Pattern graph '{}' not found", pattern_graph_name))
            }
        }
        Method::GetLedger => {
            let g = core.read().await;
            Response::ok(req_id, serde_json::json!(g.get_ledger()))
        }

        Method::ClearLedger => {
            let mut g = core.write().await;
            g.clear_ledger();
            Response::ok(req_id, serde_json::json!("ok"))
        }
        Method::ApplyLedger { transactions } => {
            let mut g = core.write().await;
            match g.apply_ledger(transactions) {
                Ok(()) => Response::ok(req_id, serde_json::json!("ok")),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetSubgraph { node_ids } => {
            let g = core.read().await;
            let sub = g.get_subgraph(&node_ids);
            match sub.to_msgpack() {
                Ok(json) => match serde_json::from_slice::<serde_json::Value>(&json) {
                    Ok(val) => Response::ok(req_id, val),
                    Err(e) => Response::err(req_id, e.to_string()),
                },
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::Fork => {
            // Cannot return the forked GraphCore directly because it needs to be registered.
            // A true fork method in the registry might be better.
            // For now, we return the JSON representation of the fork.
            let g = core.read().await;
            let sub = g.fork();
            match sub.to_msgpack() {
                Ok(json) => match serde_json::from_slice::<serde_json::Value>(&json) {
                    Ok(val) => Response::ok(req_id, val),
                    Err(e) => Response::err(req_id, e.to_string()),
                },
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::DiffAgainst { other_graph } => {
            let s_lock = state.read().await;
            let other_entry = match s_lock.registry.get(&other_graph) {
                Some(e) => e,
                None => return Response::err(req_id, format!("Other graph '{}' not found", other_graph)),
            };
            let other_core = other_entry.core.clone();
            drop(s_lock);

            let g1 = core.read().await;
            let g2 = other_core.read().await;
            let diff_str = g1.diff_against(&g2);
            match serde_json::from_slice::<serde_json::Value>(diff_str.as_bytes()) {
                Ok(val) => Response::ok(req_id, val),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::CompactNodesByType { node_type, threshold } => {
            let mut g = core.write().await;
            let removed = g.compact_nodes_by_type(&node_type, threshold);
            Response::ok(req_id, serde_json::json!({ "removed_nodes": removed }))
        }
        // Catch-all for methods not yet fully dispatched.
        _ => Response::err(req_id, format!("Method not yet implemented for graph dispatch")),
    }
}

/// Handle a single client connection (UDS or TCP).
pub async fn handle_connection<S>(mut stream: S, state: Arc<RwLock<ServerState>>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    loop {
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        if stream.read_exact(&mut payload).await.is_err() {
            break;
        }

        let req: Request = match rmp_serde::from_slice(&payload) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::err(0, format!("Invalid request MsgPack: {}", e));
                let out = rmp_serde::to_vec_named(&resp).unwrap_or_default();
                let out_len = out.len() as u32;
                let _ = stream.write_all(&out_len.to_be_bytes()).await;
                let _ = stream.write_all(&out).await;
                continue;
            }
        };

        let is_shutdown = matches!(req.method, Method::Shutdown);
        let resp = dispatch(&state, req).await;

        let out = rmp_serde::to_vec_named(&resp).unwrap_or_default();
        let out_len = out.len() as u32;
        if stream.write_all(&out_len.to_be_bytes()).await.is_err() {
            break;
        }
        if stream.write_all(&out).await.is_err() {
            break;
        }

        if is_shutdown {
            break;
        }
    }
}

/// Start the server on a Unix Domain Socket.
pub async fn serve_uds(socket_path: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    // Remove stale socket file.
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    info!("Listening on UDS: {}", socket_path);

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    handle_connection(stream, state).await;
                });
            }
            Err(e) => {
                error!("Accept error: {}", e);
            }
        }
    }
}

/// Start the server on a TCP address.
pub async fn serve_tcp(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on TCP: {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("TCP connection from {}", addr);
                let state = state.clone();
                tokio::spawn(async move {
                    handle_connection(stream, state).await;
                });
            }
            Err(e) => {
                error!("Accept error: {}", e);
            }
        }
    }
}
