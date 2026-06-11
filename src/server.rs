// CONCEPT:KG-2.19 — Tokio Service Server
//
// Long-running Tokio server that holds the GraphRegistry in memory
// and serves requests over UDS or TCP with HMAC-SHA256 authentication.

use std::sync::Arc;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info};

use crate::channels::ChannelManager;
use crate::isolation::{AccessLevel, IsolationLayer};
use crate::protocol::{Method, Request, Response, ResultPayload};
use crate::registry::GraphRegistry;

/// Shared server state behind Arc<RwLock<>>.
pub struct ServerState {
    pub registry: GraphRegistry,
    pub isolation: IsolationLayer,
    pub channels: ChannelManager,
    pub auth_secret: String,
    pub persist_dir: Option<String>,
    /// Global backpressure: caps concurrent in-flight requests across all
    /// connections. Exhaustion yields a `BUSY` response so clients retry with
    /// jitter instead of the server queueing unbounded work (Plan 01 Step 8).
    pub max_in_flight: Arc<Semaphore>,
}

/// Compute the HMAC-SHA256 hex token for a request id. Shared by `verify_auth`
/// and trusted in-process callers (Kafka event bus, tests) that dispatch
/// requests without going through a socket client.
pub fn compute_auth_token(secret: &str, request_id: u64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return String::new();
    };
    mac.update(request_id.to_string().as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Verify HMAC-SHA256 authentication token.
///
/// An empty secret accepts everything — the binary only allows that
/// configuration behind the explicit insecure opt-out enforced at startup
/// (`--allow-insecure` / `EPISTEMIC_GRAPH_ALLOW_INSECURE=1`, see `main.rs`).
fn verify_auth(secret: &str, request_id: u64, token: &str) -> bool {
    if secret.is_empty() {
        return true; // Explicitly opted-out of authentication at startup.
    }
    token == compute_auth_token(secret, request_id)
}

/// Whether a graph-targeted method mutates the target graph (Write) or only
/// reads from it (Read). Pure-compute methods (finance, datascience, parse)
/// never touch graph state and classify as Read.
fn requires_write(method: &Method) -> bool {
    matches!(
        method,
        Method::AddNode { .. }
            | Method::RemoveNode { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::ClearGraph
            | Method::AddEmbedding { .. }
            | Method::PruneByLifecycle { .. }
            | Method::BatchUpdate { .. }
            | Method::EvictLRU { .. }
            | Method::DecaySweep { .. }
            | Method::TouchNodes { .. }
            | Method::FromMsgpack { .. }
            | Method::ClearLedger
            | Method::ApplyLedger { .. }
            | Method::CompactNodesByType { .. }
            | Method::RunDatalogReasoning { .. }
            | Method::Reconcile { .. }
            | Method::ApplyMutation { .. }
            | Method::ApplyMultisigMutation { .. }
            | Method::ParseRepository { .. }
            | Method::DeleteGraph { .. }
    )
}

/// Enforce the isolation ACL for a graph-targeted operation.
///
/// Back-compat invariant: while no identities are registered the layer has no
/// rules and everything is allowed (single-tenant deployments are unchanged).
/// Once rules exist, `check_access` decides: peer agent graphs are denied,
/// managers reach subordinate graphs, team graphs are member-read/manager-write,
/// the `__bus__` stays open to all authenticated agents.
fn check_graph_access(
    isolation: &IsolationLayer,
    caller: Option<&str>,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    graph_owner: Option<&str>,
    access: AccessLevel,
) -> Result<(), String> {
    if !isolation.has_rules() {
        return Ok(());
    }
    let agent = caller.unwrap_or("");
    if isolation.check_access(agent, graph_name, graph_type, graph_owner, access) {
        Ok(())
    } else {
        Err(format!(
            "ACCESS_DENIED: agent '{}' lacks {:?} access to graph '{}'",
            if agent.is_empty() {
                "<anonymous>"
            } else {
                agent
            },
            access,
            graph_name
        ))
    }
}

/// Run a CPU-heavy, read-only computation on the blocking thread pool
/// (CONCEPT:KG-2.51). Callers snapshot whatever graph state the computation
/// needs under a short read lock, drop the lock, then hand the owned snapshot
/// here — so the tokio runtime threads and the per-graph RwLock are never
/// held across O(V·E)-class work.
async fn compute_off_lock<T, F>(req_id: u64, f: F) -> Result<T, Response>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Response::err(req_id, format!("Blocking compute task failed: {}", e)))
}

/// Confidence-weight raw semantic-search hits (CONCEPT:KG-2.51): drop
/// strictly-stale facts (validity window closed), apply Ebbinghaus temporal
/// decay (30-day half-life) to each hit's confidence, re-rank by the
/// decay-weighted similarity and truncate to `n_results`. Pure function —
/// runs on the blocking pool, never under the graph lock.
fn weight_semantic_results(
    candidates: Vec<(String, f32, Option<Vec<u8>>)>,
    now: u64,
    n_results: usize,
) -> Vec<(String, f32)> {
    let mut weighted_results = Vec::new();
    for (node_id, mut similarity, props) in candidates {
        if let Some(props_bytes) = props {
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
                        let decay_rate = std::f64::consts::LN_2 / 30.0;
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
    weighted_results
}

/// Dispatch a single request to the appropriate handler.
pub async fn dispatch(state: &Arc<RwLock<ServerState>>, req: Request) -> Response {
    // Auth check.
    {
        let s = state.read().await;
        if !verify_auth(&s.auth_secret, req.id, &req.auth_token) {
            return Response::err(req.id, "Authentication failed");
        }
    }

    match req.method {
        // ── Service-level ────────────────────────────────────────────
        Method::Ping => Response::ok(req.id, ResultPayload::String("pong".to_string())),

        Method::Health => {
            // Simplified health check for now
            let uptime_s = 0; // you can capture start time in ServerState
            let mem_bytes = 0;
            // ``version`` + ``ops`` let clients negotiate capabilities (e.g. only
            // use ``ParseFiles`` against an engine that advertises it) and fall
            // back gracefully against an older binary. (CONCEPT:KG-2.19)
            Response::ok(
                req.id,
                ResultPayload::Json(serde_json::json!({
                    "status": "ok",
                    "uptime_s": uptime_s,
                    "mem_bytes": mem_bytes,
                    "version": env!("CARGO_PKG_VERSION"),
                    "ops": ["ParseFiles"]
                })),
            )
        }

        Method::ParseFile { file_path, source } => {
            #[cfg(feature = "ast")]
            match crate::parser::tree_sitter::parse_file(&file_path, &source) {
                Ok(result) => match serde_json::to_value(&result) {
                    Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
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

        Method::ParseFiles { files_msgpack } => {
            #[cfg(feature = "ast")]
            {
                // Blob is MessagePack `Vec<(file_path, source_bytes)>`; inner bytes
                // arrive as msgpack `bin`, so decode the source as ByteBuf.
                let files: Vec<(String, serde_bytes::ByteBuf)> =
                    match rmp_serde::from_slice(&files_msgpack) {
                        Ok(f) => f,
                        Err(e) => {
                            return Response::err(req.id, format!("Invalid files_msgpack: {}", e));
                        }
                    };
                let owned: Vec<(String, Vec<u8>)> =
                    files.into_iter().map(|(p, b)| (p, b.into_vec())).collect();
                let results = crate::parser::tree_sitter::parse_files(&owned);
                match serde_json::to_value(&results) {
                    Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req.id, format!("Serialization error: {}", e)),
                }
            }
            #[cfg(not(feature = "ast"))]
            {
                let _ = files_msgpack;
                Response::err(req.id, "AST feature not enabled".to_string())
            }
        }

        Method::Shutdown => {
            info!("Shutdown requested via protocol");
            Response::ok(req.id, ResultPayload::String("shutting_down".to_string()))
        }

        Method::Checkpoint => {
            info!("Checkpoint requested");
            match crate::persist::checkpoint_all(state).await {
                Ok(n) => Response::ok(
                    req.id,
                    ResultPayload::String(format!("checkpoint_complete:{}", n)),
                ),
                Err(e) => Response::err(req.id, e),
            }
        }

        // ── Multi-tenant graph management ────────────────────────────
        Method::CreateGraph {
            graph_name,
            graph_type,
        } => {
            let mut s = state.write().await;
            // The creator (when identified) becomes the graph owner, which is
            // what peer-deny / manager-access checks resolve against.
            match s
                .registry
                .create_graph(&graph_name, graph_type, req.agent_id.clone())
            {
                Ok(()) => Response::ok(
                    req.id,
                    ResultPayload::Json(serde_json::json!({"created": graph_name})),
                ),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::DeleteGraph { ref graph_name } => {
            let mut s = state.write().await;
            if let Some(entry) = s.registry.get(graph_name) {
                if let Err(denied) = check_graph_access(
                    &s.isolation,
                    req.agent_id.as_deref(),
                    graph_name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Write,
                ) {
                    return Response::err(req.id, denied);
                }
            }
            match s.registry.delete_graph(graph_name) {
                Ok(()) => Response::ok(
                    req.id,
                    ResultPayload::Json(serde_json::json!({"deleted": graph_name})),
                ),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::ListGraphs => {
            let s = state.read().await;
            let graphs: Vec<serde_json::Value> = s
                .registry
                .list()
                .iter()
                .map(|(name, gt)| serde_json::json!({"name": name, "type": gt}))
                .collect();
            Response::ok(req.id, ResultPayload::Json(serde_json::json!(graphs)))
        }

        // ── Channel operations ───────────────────────────────────────
        Method::CreateChannel {
            channel_id,
            channel_type,
            creator,
            initial_members,
        } => {
            let mut s = state.write().await;
            match s
                .channels
                .create_channel(&channel_id, channel_type, &creator, initial_members)
            {
                Ok(()) => Response::ok(
                    req.id,
                    ResultPayload::Json(serde_json::json!({"channel": channel_id})),
                ),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::JoinChannel {
            channel_id,
            agent_id,
        } => {
            let mut s = state.write().await;
            match s.channels.join_channel(&channel_id, &agent_id) {
                Ok(()) => Response::ok(req.id, ResultPayload::String("joined".to_string())),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::LeaveChannel {
            channel_id,
            agent_id,
        } => {
            let mut s = state.write().await;
            match s.channels.leave_channel(&channel_id, &agent_id) {
                Ok(imprint) => {
                    let val = match imprint {
                        Some(imp) => {
                            serde_json::to_value(&imp).unwrap_or(serde_json::json!("closed"))
                        }
                        None => serde_json::json!("left"),
                    };
                    Response::ok(req.id, ResultPayload::Json(val))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::CloseChannel {
            channel_id,
            summary_embedding,
            topic_metadata,
        } => {
            let mut s = state.write().await;
            match s
                .channels
                .close_channel(&channel_id, summary_embedding, topic_metadata)
            {
                Ok(imprint) => {
                    let val = match imprint {
                        Some(imp) => {
                            serde_json::to_value(&imp).unwrap_or(serde_json::json!("closed"))
                        }
                        None => serde_json::json!("closed"),
                    };
                    Response::ok(req.id, ResultPayload::Json(val))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::SendMessage {
            channel_id,
            sender,
            payload,
        } => {
            let mut s = state.write().await;
            match s.channels.send_message(&channel_id, &sender, &payload) {
                Ok(()) => Response::ok(req.id, ResultPayload::String("sent".to_string())),
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
                    Response::ok(req.id, ResultPayload::Json(serde_json::json!(val)))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::ListChannels => {
            let s = state.read().await;
            let channels: Vec<serde_json::Value> = s.channels.list_channels().iter().map(|(id, ct, members)| {
                serde_json::json!({"id": id, "type": ct, "members": members})
            }).collect();
            Response::ok(req.id, ResultPayload::Json(serde_json::json!(channels)))
        }

        Method::GetChannelMembers { channel_id } => {
            let s = state.read().await;
            match s.channels.get_members(&channel_id) {
                Ok(members) => {
                    Response::ok(req.id, ResultPayload::Json(serde_json::json!(members)))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        // ── Zero-Trust Consensus ─────────────────────────────────────────
        Method::RegisterIdentity {
            agent_id,
            role,
            teams,
            signature,
        } => {
            info!(
                "RegisterIdentity: agent_id={}, role={:?}, signature={}",
                agent_id, role, signature
            );
            let mut s = state.write().await;
            s.isolation.register_agent(crate::isolation::AgentIdentity {
                agent_id: agent_id.clone(),
                role,
                teams,
            });
            Response::ok(req.id, ResultPayload::String("registered".to_string()))
        }

        Method::ApplyMultisigMutation {
            signatures,
            threshold,
            mutation_type,
            query,
        } => {
            if signatures.len() < threshold {
                return Response::err(
                    req.id,
                    format!(
                        "Insufficient signatures: {} < {}",
                        signatures.len(),
                        threshold
                    ),
                );
            }
            // Delegate mutation application to the target graph
            dispatch_graph_op(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                Method::ApplyMutation {
                    event_type: mutation_type,
                    query,
                },
            )
            .await
        }

        // ── Graph operations (dispatch to target graph) ──────────────
        _ => {
            dispatch_graph_op(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                req.method,
            )
            .await
        }
    }
}

/// Dispatch a graph-level operation to the target named graph, enforcing the
/// isolation ACL (`isolation.rs::check_access`) when rules are registered.
async fn dispatch_graph_op(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    req_id: u64,
    caller: Option<&str>,
    method: Method,
) -> Response {
    let s = state.read().await;
    let entry = match s.registry.get(graph_name) {
        Some(e) => e,
        None => return Response::err(req_id, format!("Graph '{}' not found", graph_name)),
    };

    let access = if requires_write(&method) {
        AccessLevel::Write
    } else {
        AccessLevel::Read
    };
    if let Err(denied) = check_graph_access(
        &s.isolation,
        caller,
        graph_name,
        entry.graph_type,
        entry.owner.as_deref(),
        access,
    ) {
        return Response::err(req_id, denied);
    }

    let core = entry.core.clone();
    drop(s); // Release registry lock before graph lock.

    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            let mut g = core.write().await;
            g.add_node(node_id, properties_msgpack);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::RemoveNode { node_id } => {
            let mut g = core.write().await;
            g.remove_node(node_id);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::HasNode { node_id } => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Bool(g.has_node(&node_id)))
        }
        Method::GetNodes => {
            let g = core.read().await;
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
            let g = core.read().await;
            let val = match g.get_node_properties(&node_id) {
                Some(props_msgpack) => ResultPayload::PropertiesMsgpack(props_msgpack),
                None => ResultPayload::Json(serde_json::Value::Null),
            };
            Response::ok(req_id, val)
        }
        Method::NodeCount => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Count(g.node_count() as u64))
        }
        Method::NodeIds => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Ids(g.node_ids()))
        }
        Method::AddEmbedding { node_id, embedding } => {
            let mut g = core.write().await;
            g.semantic_store.add_embedding(node_id, embedding);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::SemanticSearch {
            query_embedding,
            n_results,
        } => {
            // CONCEPT:KG-2.51 — never run heavy compute under the graph lock.
            // The ANN search (the HNSW path rebuilds its index per query) and
            // the per-candidate Ebbinghaus decay scoring both run on the
            // blocking pool; the read lock is held only to memcpy the
            // embedding store, then briefly again to fetch the ~2·n candidate
            // property blobs. A large search no longer stalls writers.
            let store = { core.read().await.semantic_store.clone() };
            // Fetch more results initially to account for filtered out nodes
            let raw_results = match compute_off_lock(req_id, move || {
                store.semantic_search(&query_embedding, n_results * 2)
            })
            .await
            {
                Ok(r) => r,
                Err(resp) => return resp,
            };

            // Bounded candidate-metadata fetch: only the hit ids, not the graph.
            let candidates: Vec<(String, f32, Option<Vec<u8>>)> = {
                let g = core.read().await;
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
                Ok(weighted_results) => Response::ok(
                    req_id,
                    ResultPayload::Json(serde_json::json!(weighted_results)),
                ),
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
        Method::FinanceOptimizePortfolio {
            expected_returns,
            cov_matrix,
            risk_free_rate,
            min_weight,
            max_weight,
        } => {
            let result = crate::finance::optimizer::mean_variance_optimization(
                &expected_returns,
                &cov_matrix,
                risk_free_rate,
                min_weight,
                max_weight,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::FinanceRiskParity { cov_matrix } => {
            let result = crate::finance::optimizer::risk_parity(&cov_matrix);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::FinanceBlackLitterman {
            market_weights,
            cov_matrix,
            views,
            pick_matrix,
            tau,
            risk_aversion,
        } => {
            let result = crate::finance::optimizer::black_litterman(
                &market_weights,
                &cov_matrix,
                &views,
                &pick_matrix,
                tau,
                risk_aversion,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::FinanceEfficientFrontier {
            expected_returns,
            cov_matrix,
            target_return,
        } => {
            let result = crate::finance::optimizer::efficient_frontier_target(
                &expected_returns,
                &cov_matrix,
                target_return,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }

        // ── Data Science Primitives (CONCEPT:KG-2.22) ─────────────────
        Method::DsLinearRegression { x, y } => {
            let result = crate::datascience::primitives::linear_regression(&x, &y);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::DsKMeans { data, k, max_iter } => {
            let result = crate::datascience::primitives::kmeans(&data, k, max_iter);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::DsPca { data, n_components } => {
            let result = crate::datascience::primitives::pca(&data, n_components);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::DsComputeStats { data } => {
            let result = crate::datascience::primitives::compute_stats(&data);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::DsTrainTestSplit {
            data,
            labels,
            test_ratio,
            shuffle,
            seed,
        } => {
            let (x_train, x_test, y_train, y_test) =
                crate::datascience::primitives::train_test_split(
                    &data, &labels, test_ratio, shuffle, seed,
                );
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "x_train": x_train,
                    "x_test": x_test,
                    "y_train": y_train,
                    "y_test": y_test,
                })),
            )
        }
        Method::DsFitEstimator {
            estimator,
            x,
            y,
            params,
        } => match crate::datascience::estimators::fit_estimator(&estimator, &x, &y, &params) {
            Ok(model) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(model))),
            Err(e) => Response::err(req_id, e),
        },
        Method::DsPredictEstimator { model, x } => {
            let preds = crate::datascience::estimators::predict(&model, &x);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(preds)))
        }

        // ── Training loss / optimizer kernels (CONCEPT:KG-2.22) ────────
        Method::DsSoftmax {
            logits,
            temperature,
        } => {
            let r = crate::datascience::training::softmax(&logits, temperature);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsLogSoftmax { logits } => {
            let r = crate::datascience::training::log_softmax(&logits);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsCrossEntropy { logits, labels } => {
            let r = crate::datascience::training::cross_entropy(&logits, &labels);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsDpoLoss {
            policy_chosen,
            policy_rejected,
            ref_chosen,
            ref_rejected,
            beta,
        } => {
            let r = crate::datascience::training::dpo_loss(
                &policy_chosen,
                &policy_rejected,
                &ref_chosen,
                &ref_rejected,
                beta,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsGrpoSurrogate {
            logprob,
            old_logprob,
            advantage,
            clip_eps,
        } => {
            let r = crate::datascience::training::grpo_surrogate(
                &logprob,
                &old_logprob,
                &advantage,
                clip_eps,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsKlDivergence {
            logprob,
            ref_logprob,
        } => {
            let r = crate::datascience::training::kl_divergence(&logprob, &ref_logprob);
            Response::ok(req_id, ResultPayload::Float(r))
        }
        Method::DsAdamStep {
            params,
            grads,
            m,
            v,
            lr,
            beta1,
            beta2,
            eps,
            t,
        } => {
            let r = crate::datascience::training::adam_step(
                &params, &grads, &m, &v, lr, beta1, beta2, eps, t,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsSgdStep { params, grads, lr } => {
            let r = crate::datascience::training::sgd_step(&params, &grads, lr);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }

        // ── Extended Finance: Risk (CONCEPT:KG-2.20) ──────────────────
        Method::FinanceVar {
            returns,
            confidence,
        } => {
            let v = crate::finance::risk::historical_var(&returns, confidence);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceCvar {
            returns,
            confidence,
        } => {
            let v = crate::finance::risk::historical_cvar(&returns, confidence);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceMaxDrawdown { returns } => {
            let v = crate::finance::risk::max_drawdown(&returns);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceDrawdownSeries { returns } => {
            let v = crate::finance::risk::drawdown_series(&returns);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceDownsideDeviation { returns, target } => {
            let v = crate::finance::risk::downside_deviation(&returns, target);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceRiskMetrics {
            returns,
            risk_free_rate,
        } => {
            let result = crate::finance::risk::compute_risk_metrics(&returns, risk_free_rate);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::FinanceMonteCarloVar {
            mean,
            std_dev,
            n_simulations,
            confidence,
        } => {
            let v = crate::finance::risk::monte_carlo_var(mean, std_dev, n_simulations, confidence);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceStressTest {
            weights,
            expected_returns,
            cov_matrix,
            shock_factors,
        } => {
            let v = crate::finance::risk::stress_test(
                &weights,
                &expected_returns,
                &cov_matrix,
                &shock_factors,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── Extended Finance: Regime detection (HMM) ──────────────────
        Method::FinanceDetectRegimes {
            observations,
            n_states,
            max_iter,
            tol,
        } => {
            let result =
                crate::finance::regime::detect_regimes(&observations, n_states, max_iter, tol);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }

        // ── Extended Finance: Signals / alpha ─────────────────────────
        Method::FinanceRollingZscore { values, window } => {
            let v = crate::finance::signals::rolling_zscore(&values, window);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceEwma { values, span } => {
            let v = crate::finance::signals::ewma_signal(&values, span);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceSignalDecay { signal, half_life } => {
            let v = crate::finance::signals::signal_decay(&signal, half_life);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceCombineAlphas { signals, weights } => {
            let v = crate::finance::signals::combine_alphas(&signals, &weights);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceCrossSectionalRank { cross_section } => {
            let v = crate::finance::signals::cross_sectional_rank(&cross_section);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMomentum { prices, lookback } => {
            let v = crate::finance::signals::momentum(&prices, lookback);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMeanReversion { values, window } => {
            let v = crate::finance::signals::mean_reversion(&values, window);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceInformationCoefficient {
            signal,
            forward_returns,
        } => {
            let v = crate::finance::signals::information_coefficient(&signal, &forward_returns);
            Response::ok(req_id, ResultPayload::Float(v))
        }

        // ── Extended Finance: Execution / microstructure ──────────────
        Method::FinanceTwap {
            total_quantity,
            n_slices,
            start_time,
            interval_secs,
        } => {
            let v = crate::finance::exchange::twap_schedule(
                total_quantity,
                n_slices,
                start_time,
                interval_secs,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceVwap {
            total_quantity,
            volume_profile,
            start_time,
            interval_secs,
        } => {
            let v = crate::finance::exchange::vwap_schedule(
                total_quantity,
                &volume_profile,
                start_time,
                interval_secs,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMarketImpact {
            daily_volatility,
            order_quantity,
            average_daily_volume,
            impact_coefficient,
        } => {
            let v = crate::finance::exchange::estimate_market_impact(
                daily_volatility,
                order_quantity,
                average_daily_volume,
                impact_coefficient,
            );
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinancePairsTrading {
            prices_a,
            prices_b,
            lookback,
        } => {
            let v = crate::finance::exchange::pairs_trading_signal(&prices_a, &prices_b, lookback);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMatchOrders { orders } => {
            let v = crate::finance::exchange::match_orders(&orders);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── Market Making / Microstructure (CONCEPT:KG-2.20f) ─────────
        Method::FinanceAvellanedaStoikov {
            mid,
            inventory,
            sigma,
            gamma,
            kappa,
            tau,
        } => {
            let v =
                crate::finance::quant::avellaneda_stoikov(mid, inventory, sigma, gamma, kappa, tau);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceGltQuotes {
            mid,
            inventory,
            sigma,
            gamma,
            kappa,
            a,
        } => {
            let v = crate::finance::quant::glt_quotes(mid, inventory, sigma, gamma, kappa, a);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceLogitQuotes {
            p_mid,
            inventory,
            sigma,
            gamma,
            kappa,
            tau,
            boundary_m,
        } => {
            let v = crate::finance::quant::logit_space_quotes(
                p_mid, inventory, sigma, gamma, kappa, tau, boundary_m,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceGlostenMilgromSpread { alpha, p } => {
            let v = crate::finance::quant::glosten_milgrom_spread(alpha, p);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceExpectedPnlRate {
            delta,
            a,
            kappa,
            alpha,
            p,
            v_h,
            v_l,
        } => {
            let v = crate::finance::quant::expected_pnl_rate(delta, a, kappa, alpha, p, v_h, v_l);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceBreakevenAlpha { delta, p, v_h, v_l } => {
            let v = crate::finance::quant::breakeven_alpha(delta, p, v_h, v_l);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceOfiSeries {
            ts,
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
            window_secs,
        } => {
            let v = crate::finance::quant::ofi_series(
                &ts,
                &bid_px,
                &bid_sz,
                &ask_px,
                &ask_sz,
                window_secs,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMicropriceSeries {
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
        } => {
            let v = crate::finance::quant::microprice_series(&bid_px, &bid_sz, &ask_px, &ask_sz);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceVpinPm {
            buy_vol,
            sell_vol,
            p_mean,
        } => {
            let v = crate::finance::quant::vpin_pm(&buy_vol, &sell_vol, &p_mean);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceHawkesMle {
            times,
            t_horizon,
            max_iter,
        } => {
            let v = crate::finance::quant::hawkes_mle(&times, t_horizon, max_iter);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceHardimanBouchaud {
            times,
            t_horizon,
            n_windows,
        } => {
            let v = crate::finance::quant::hardiman_bouchaud_branching_ratio(
                &times, t_horizon, n_windows,
            );
            Response::ok(req_id, ResultPayload::Float(v))
        }

        // ── Position Sizing (CONCEPT:KG-2.20f) ────────────────────────
        Method::FinanceKellyFraction { q, c, fraction } => {
            let v = crate::finance::quant::kelly_fraction(q, c, fraction);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceBayesianKelly {
            alpha,
            beta,
            c,
            n_quadrature,
        } => {
            let v = crate::finance::quant::bayesian_kelly_fraction(alpha, beta, c, n_quadrature);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinancePosteriorCredibleInterval { alpha, beta, level } => {
            let (lo, hi) = crate::finance::quant::posterior_credible_interval(alpha, beta, level);
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({"lower": lo, "upper": hi})),
            )
        }

        // ── Backtest Validation (CONCEPT:KG-2.20f) ────────────────────
        Method::FinancePurgedCpcv {
            n_samples,
            n_groups,
            n_test_groups,
            purge_window,
            embargo,
        } => {
            let v = crate::finance::quant::purged_cpcv_splits(
                n_samples,
                n_groups,
                n_test_groups,
                purge_window,
                embargo,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceDeflatedSharpe {
            observed_sr,
            n_trials,
            sr_returns,
        } => {
            let v =
                crate::finance::quant::deflated_sharpe_ratio(observed_sr, n_trials, &sr_returns);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceProbabilityBacktestOverfit { insample, oos } => {
            let v = crate::finance::quant::probability_of_backtest_overfit(&insample, &oos);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceDieboldMariano {
            losses_a,
            losses_b,
            h,
        } => {
            let v = crate::finance::quant::diebold_mariano(&losses_a, &losses_b, h);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── Forensic Accounting (CONCEPT:KG-2.20g) ────────────────────
        Method::FinanceForensicReport {
            this_year,
            prior_year,
        } => {
            let v = crate::finance::forensic::forensic_report(&this_year, &prior_year);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── State-Space / Stat-Arb (CONCEPT:KG-2.20h) ─────────────────
        Method::FinanceKalmanFilter1d {
            observations,
            f,
            q,
            h,
            r,
            x0,
            p0,
        } => {
            let v = crate::finance::statespace::kalman_filter_1d(&observations, f, q, h, r, x0, p0);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceKalmanBeta {
            market_returns,
            asset_returns,
            q,
            r,
            beta0,
            p0,
        } => {
            let v = crate::finance::statespace::kalman_beta(
                &market_returns,
                &asset_returns,
                q,
                r,
                beta0,
                p0,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceKalmanVolatility {
            returns,
            q,
            r,
            log_var0,
            p0,
            annualization,
        } => {
            let v = crate::finance::statespace::kalman_volatility(
                &returns,
                q,
                r,
                log_var0,
                p0,
                annualization,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceAdfTest { series, max_lag } => {
            let v = crate::finance::statespace::adf_test(&series, max_lag);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceOuCalibrate { spread, dt } => {
            let v = crate::finance::statespace::ou_calibrate(&spread, dt);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceOuOptimalThresholds {
            theta,
            mu,
            sigma,
            sigma_eq,
            cost,
        } => {
            let params = crate::finance::statespace::OuParams {
                theta,
                mu,
                sigma,
                sigma_eq,
                half_life: if theta > 1e-12 {
                    std::f64::consts::LN_2 / theta
                } else {
                    f64::INFINITY
                },
            };
            let v = crate::finance::statespace::ou_optimal_thresholds(&params, cost);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMarkovTransitionMatrix { states, n_states } => {
            let v = crate::finance::statespace::markov_transition_matrix(&states, n_states);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── Signal Combination / Sizing / Calibration (CONCEPT:KG-2.20i) ──
        Method::FinanceOrderBookImbalance { v_bid, v_ask } => {
            let v = crate::finance::quant::order_book_imbalance(&v_bid, &v_ask);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceQueueImbalance {
            bid_q,
            ask_q,
            bid_rate,
            ask_rate,
        } => {
            let v = crate::finance::quant::queue_imbalance(&bid_q, &ask_q, &bid_rate, &ask_rate);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceRealizedVolTick { mid, window } => {
            let v = crate::finance::quant::realized_vol_tick(&mid, window);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceSpreadReversion {
            bid_px,
            ask_px,
            window,
        } => {
            let v = crate::finance::quant::spread_reversion(&bid_px, &ask_px, window);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceInformationRatio { ic, n_independent } => {
            let v = crate::finance::quant::information_ratio(ic, n_independent);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceEffectiveIndependentN { returns_matrix } => {
            let v = crate::finance::quant::effective_independent_n(&returns_matrix);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceAlphaCombinationEngine {
            returns_matrix,
            lookback,
        } => {
            let v = crate::finance::quant::alpha_combination_engine(&returns_matrix, lookback);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceBrierScore {
            forecasts,
            outcomes,
        } => {
            let v = crate::finance::quant::brier_score(&forecasts, &outcomes);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceConvergenceGate {
            strengths,
            strong_threshold,
            min_agree,
        } => {
            let v =
                crate::finance::quant::convergence_gate(&strengths, strong_threshold, min_agree);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceEmpiricalKelly {
            p,
            b,
            historical_returns,
            n_simulations,
            seed,
        } => {
            let v = crate::finance::quant::empirical_kelly(
                p,
                b,
                &historical_returns,
                n_simulations,
                seed,
            );
            Response::ok(req_id, ResultPayload::Float(v))
        }

        // ── Derivatives: SABR volatility surface (CONCEPT:KG-2.20j) ────
        Method::FinanceSabrImpliedVol {
            f,
            k,
            t,
            alpha,
            beta,
            rho,
            nu,
        } => {
            let v = crate::finance::derivatives::sabr_implied_vol(f, k, t, alpha, beta, rho, nu);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceSabrSmile {
            f,
            strikes,
            t,
            alpha,
            beta,
            rho,
            nu,
        } => {
            let v = crate::finance::derivatives::sabr_smile(f, &strikes, t, alpha, beta, rho, nu);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceSabrCalibrate {
            f,
            t,
            strikes,
            market_vols,
            beta,
        } => {
            let v = crate::finance::derivatives::sabr_calibrate(f, t, &strikes, &market_vols, beta);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FindSimilarPairs {
            embeddings: _,
            ids: _,
            threshold: _,
            use_lsh: _,
            lsh_num_tables: _,
            lsh_hash_size: _,
            seed: _,
        } => Response::err(
            req_id,
            "FindSimilarPairs is deprecated. Use datascience primitives.".to_string(),
        ),
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let mut g = core.write().await;
            match g.add_edge(source_id, target_id, properties_msgpack) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            let mut g = core.write().await;
            g.remove_edge(source_id, target_id);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::HasEdge {
            source_id,
            target_id,
        } => {
            let g = core.read().await;
            Response::ok(
                req_id,
                ResultPayload::Bool(g.has_edge(&source_id, &target_id)),
            )
        }
        Method::GetEdges => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::EdgeList(g.get_edges()))
        }
        Method::GetEdgeProperties {
            source_id,
            target_id,
        } => {
            let g = core.read().await;
            let props = g.get_edge_properties(&source_id, &target_id);
            let val: Vec<serde_json::Value> = props
                .into_iter()
                .map(|p| rmp_serde::from_slice(&p).unwrap_or(serde_json::json!({})))
                .collect();
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(val)))
        }
        Method::ClearGraph => {
            let mut g = core.write().await;
            g.clear();
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::EdgeCount => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Count(g.edge_count() as u64))
        }
        // TopologicalSort / FindCycle / GetShortestPath / components / blast
        // radius / degree centrality stay under the read lock intentionally
        // (CONCEPT:KG-2.51): they are single-pass O(V+E), so a structural
        // snapshot would cost as much as the computation itself.
        Method::TopologicalSort => {
            let g = core.read().await;
            match crate::algorithms::topological_sort(&g) {
                Ok(order) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(order))),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::FindCycle => {
            let g = core.read().await;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::find_cycle(&g))),
            )
        }
        Method::GetShortestPath {
            source_id,
            target_id,
        } => {
            let g = core.read().await;
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
            let snap = { core.read().await.topology_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::pagerank(&snap, damping, iterations)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(v))),
                Err(resp) => resp,
            }
        }
        Method::ConnectedComponents => {
            let g = core.read().await;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::connected_components(
                    &g
                ))),
            )
        }
        Method::StronglyConnectedComponents => {
            let g = core.read().await;
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
            let snap = { core.read().await.analysis_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::minimum_spanning_tree(&snap)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(v))),
                Err(resp) => resp,
            }
        }
        Method::Metrics => {
            // Parses every node's property JSON — memcpy snapshot under the
            // lock is cheaper than O(V) JSON parsing under it (KG-2.51). The
            // ledger is not snapshotted; capture its length for the
            // total_mutations field before releasing the lock.
            let (snap, ledger_len) = {
                let g = core.read().await;
                (g.analysis_snapshot(), g.ledger.len() as u64)
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
            let mut g = core.write().await;
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
            let mut g = core.write().await;
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
            let mut g = core.write().await;
            let touched = g.touch_nodes(&node_ids, now);
            Response::ok(req_id, ResultPayload::Count(touched as u64))
        }
        Method::ToMsgpack => {
            let g = core.read().await;
            match g.to_msgpack() {
                Ok(json) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(json))),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::FromMsgpack { msgpack } => {
            let mut g = core.write().await;
            match g.from_msgpack(&msgpack) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::Reconcile {
            graph_name: _,
            msgpack,
        } => {
            let mut g = core.write().await;
            match g.from_msgpack(&msgpack) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("reconciled".to_string())),
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
            let mut g = core.write().await;
            let mut all_inferred: Vec<std::collections::HashMap<String, String>> = Vec::new();

            match crate::reasoning::run_datalog_reasoning(
                &mut g,
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
                    &mut g,
                    domain_rules,
                    range_rules,
                ));
            }

            if !property_chains.is_empty() {
                all_inferred.extend(crate::reasoning::infer_property_chains(
                    &mut g,
                    property_chains,
                ));
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
            let g = core.read().await;
            match g.in_degree(&node_id) {
                Ok(deg) => Response::ok(req_id, ResultPayload::Count(deg as u64)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::OutDegree { node_id } => {
            let g = core.read().await;
            match g.out_degree(&node_id) {
                Ok(deg) => Response::ok(req_id, ResultPayload::Count(deg as u64)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetPredecessors { node_id } => {
            let g = core.read().await;
            match g.get_predecessors(&node_id) {
                Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetSuccessors { node_id } => {
            let g = core.read().await;
            match g.get_successors(&node_id) {
                Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetNeighbors { node_id } => {
            let g = core.read().await;
            match g.get_neighbors(&node_id) {
                Ok(nodes) => Response::ok(req_id, ResultPayload::Ids(nodes)),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::GetBlastRadius { node_id, max_depth } => {
            let g = core.read().await;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::get_blast_radius(
                    &g, &node_id, max_depth
                ))),
            )
        }
        Method::DegreeCentrality { node_id } => {
            let g = core.read().await;
            match crate::algorithms::compute_degree_centrality(&g, &node_id) {
                Ok(val) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(val))),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::DegreeCentralityAll => {
            let g = core.read().await;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::degree_centrality_all(
                    &g
                ))),
            )
        }
        Method::BetweennessCentrality => {
            // O(V·E) Brandes — snapshot topology, compute off-lock (KG-2.51).
            let snap = { core.read().await.topology_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::betweenness_centrality(&snap)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(v))),
                Err(resp) => resp,
            }
        }
        Method::PersonalizedPageRank {
            seed_nodes,
            damping,
            iterations,
        } => {
            // O(iterations·E) — snapshot topology, compute off-lock (KG-2.51).
            let snap = { core.read().await.topology_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::personalized_pagerank(&snap, &seed_nodes, damping, iterations)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(v))),
                Err(resp) => resp,
            }
        }
        Method::CommunityDetection { resolution } => {
            // Label propagation has an internal 15s wall-clock budget — that
            // budget used to burn entirely UNDER the read lock, stalling every
            // writer on the graph. Snapshot topology, compute off-lock
            // (KG-2.51).
            let snap = { core.read().await.topology_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::community_detection(&snap, resolution)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(v))),
                Err(resp) => resp,
            }
        }
        // GraphColoring stays under the read lock intentionally (KG-2.51):
        // greedy coloring is a single O(V+E) sweep, so a snapshot would cost
        // as much as the computation.
        Method::GraphColoring => {
            let g = core.read().await;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(crate::algorithms::graph_coloring(&g))),
            )
        }
        Method::ComputeSimilarityEdges { threshold } => {
            // O(V²·d) all-pairs cosine on rayon — must never run under the
            // graph lock OR on the tokio runtime threads. Snapshot the
            // property blobs it reads, compute off-lock (KG-2.51).
            let snap = { core.read().await.analysis_snapshot() };
            match compute_off_lock(req_id, move || {
                crate::algorithms::compute_similarity_edges(&snap, threshold)
            })
            .await
            {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(v))),
                Err(resp) => resp,
            }
        }
        Method::PruneByLifecycle {
            max_age_secs,
            min_score,
        } => {
            let mut g = core.write().await;
            let stats = crate::algorithms::prune_by_lifecycle(&mut g, max_age_secs, min_score);
            match serde_json::to_value(&stats) {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::GetContextView {
            agent_id,
            max_tokens,
        } => {
            let g = core.read().await;
            let view = crate::algorithms::get_context_view(&g, &agent_id, max_tokens);
            match serde_json::to_value(&view) {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::BatchUpdate { operations_msgpack } => {
            let mut g = core.write().await;
            match crate::algorithms::batch_update(&mut g, &operations_msgpack) {
                Ok(res) => match rmp_serde::from_slice::<serde_json::Value>(&res) {
                    Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req_id, format!("Invalid batch result: {}", e)),
                },
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::ParseRepository { root_path } => {
            let mut g = core.write().await;
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
                let p_snap = { p_core.read().await.analysis_snapshot() };
                let g_snap = { core.read().await.analysis_snapshot() };
                match compute_off_lock(req_id, move || g_snap.vf2_subgraph_match(&p_snap)).await {
                    Ok(v) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(v))),
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
            let g = core.read().await;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!(g.get_ledger())),
            )
        }

        Method::ClearLedger => {
            let mut g = core.write().await;
            g.clear_ledger();
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::ApplyLedger { transactions } => {
            let mut g = core.write().await;
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
            let g = core.read().await;
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
            let g = core.read().await;
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
            let other_snap = { other_core.read().await.analysis_snapshot() };
            let g1 = core.read().await;
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
            let mut g = core.write().await;
            let removed = g.compact_nodes_by_type(&node_type, threshold);
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({ "removed_nodes": removed })),
            )
        }
        // Catch-all for methods not yet fully dispatched.
        _ => Response::err(req_id, "Method not yet implemented for graph dispatch"),
    }
}

/// Handle a single client connection (UDS or TCP).
pub async fn handle_connection<S>(mut stream: S, state: Arc<RwLock<ServerState>>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Snapshot the shared backpressure semaphore once per connection.
    let sem = { state.read().await.max_in_flight.clone() };

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

        // Backpressure: acquire an in-flight permit, or shed load with BUSY.
        let _permit = match sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let resp = Response::err(req.id, "BUSY: server at capacity, retry with backoff");
                let out = rmp_serde::to_vec_named(&resp).unwrap_or_default();
                let out_len = out.len() as u32;
                if stream.write_all(&out_len.to_be_bytes()).await.is_err() {
                    break;
                }
                if stream.write_all(&out).await.is_err() {
                    break;
                }
                continue;
            }
        };
        let resp = dispatch(&state, req).await;
        drop(_permit);

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

/// Start the server on a Unix Domain Socket (unix only; Windows uses TCP).
#[cfg(unix)]
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

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::isolation::{AgentIdentity, AgentRole};
    use crate::protocol::GraphType;

    const SECRET: &str = "dispatch-test-secret";

    fn test_state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
        }))
    }

    /// State with worker1/worker2 (team alpha) + their manager registered, and
    /// per-agent + team graphs created with real owners.
    async fn multi_tenant_state() -> Arc<RwLock<ServerState>> {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.isolation.register_agent(AgentIdentity {
                agent_id: "manager".into(),
                role: AgentRole::Manager {
                    subordinates: vec!["worker1".into(), "worker2".into()],
                },
                teams: vec!["alpha".into()],
            });
            for w in ["worker1", "worker2"] {
                s.isolation.register_agent(AgentIdentity {
                    agent_id: w.into(),
                    role: AgentRole::Agent,
                    teams: vec!["alpha".into()],
                });
            }
            s.registry
                .create_graph("agent:worker1", GraphType::Agent, Some("worker1".into()))
                .unwrap();
            s.registry
                .create_graph("team:alpha", GraphType::Team, Some("manager".into()))
                .unwrap();
            s.registry
                .create_graph("global:ontology", GraphType::Global, None)
                .unwrap();
        }
        state
    }

    fn request(id: u64, graph: &str, agent_id: Option<&str>, method: Method) -> Request {
        Request {
            id,
            graph: graph.to_string(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: agent_id.map(str::to_string),
            method,
        }
    }

    fn add_node(node_id: &str) -> Method {
        Method::AddNode {
            node_id: node_id.to_string(),
            properties_msgpack: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
        }
    }

    fn assert_denied(resp: &Response) {
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.starts_with("ACCESS_DENIED"),
            "expected ACCESS_DENIED, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    fn assert_ok(resp: &Response) {
        assert!(
            resp.error.is_none(),
            "expected success, got error: {:?}",
            resp.error
        );
    }

    #[tokio::test]
    async fn test_bad_auth_token_rejected() {
        let state = test_state();
        let mut req = request(1, "__bus__", None, Method::Ping);
        req.auth_token = "bogus".to_string();
        let resp = dispatch(&state, req).await;
        assert_eq!(resp.error.as_deref(), Some("Authentication failed"));
    }

    #[tokio::test]
    async fn test_no_isolation_rules_is_back_compat() {
        // No identities registered → no rules → any caller (even anonymous)
        // can write to any graph, exactly as before enforcement existed.
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:worker1", GraphType::Agent, Some("worker1".into()))
                .unwrap();
        }
        let resp = dispatch(&state, request(1, "agent:worker1", None, add_node("n1"))).await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(2, "agent:worker1", Some("worker2"), add_node("n2")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_owner_can_write_own_graph() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "agent:worker1", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_peer_denied_read_and_write() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "agent:worker1", Some("worker2"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
        let resp = dispatch(
            &state,
            request(2, "agent:worker1", Some("worker2"), Method::GetNodes),
        )
        .await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_anonymous_denied_when_rules_exist() {
        let state = multi_tenant_state().await;
        let resp = dispatch(&state, request(1, "agent:worker1", None, Method::GetNodes)).await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_manager_reaches_subordinate_graph() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "agent:worker1", Some("manager"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_team_member_read_only() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "team:alpha", Some("worker1"), Method::GetNodes),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(2, "team:alpha", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
        let resp = dispatch(
            &state,
            request(3, "team:alpha", Some("manager"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_global_graph_read_only() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(1, "global:ontology", Some("worker1"), Method::GetNodes),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(2, "global:ontology", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_bus_stays_open_to_all() {
        let state = multi_tenant_state().await;
        for (id, agent) in [(1, Some("worker1")), (2, Some("worker2")), (3, None)] {
            let resp = dispatch(
                &state,
                request(id, "__bus__", agent, add_node(&format!("n{}", id))),
            )
            .await;
            assert_ok(&resp);
        }
    }

    #[tokio::test]
    async fn test_create_graph_records_caller_as_owner() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(
                1,
                "__bus__",
                Some("worker2"),
                Method::CreateGraph {
                    graph_name: "agent:worker2".to_string(),
                    graph_type: GraphType::Agent,
                },
            ),
        )
        .await;
        assert_ok(&resp);
        {
            let s = state.read().await;
            assert_eq!(
                s.registry.get("agent:worker2").unwrap().owner.as_deref(),
                Some("worker2")
            );
        }
        // Owner writes fine; the peer is denied.
        let resp = dispatch(
            &state,
            request(2, "agent:worker2", Some("worker2"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(3, "agent:worker2", Some("worker1"), add_node("n2")),
        )
        .await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_delete_graph_requires_write_access() {
        let state = multi_tenant_state().await;
        let del = || Method::DeleteGraph {
            graph_name: "agent:worker1".to_string(),
        };
        let resp = dispatch(&state, request(1, "__bus__", Some("worker2"), del())).await;
        assert_denied(&resp);
        let resp = dispatch(&state, request(2, "__bus__", Some("worker1"), del())).await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_channel_operations_unaffected_by_rules() {
        let state = multi_tenant_state().await;
        let resp = dispatch(
            &state,
            request(
                1,
                "__bus__",
                Some("worker1"),
                Method::CreateChannel {
                    channel_id: "channel:p2p:worker1:worker2".to_string(),
                    channel_type: crate::protocol::ChannelType::PeerToPeer,
                    creator: "worker1".to_string(),
                    initial_members: vec!["worker1".to_string(), "worker2".to_string()],
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch(
            &state,
            request(
                2,
                "__bus__",
                Some("worker2"),
                Method::SendMessage {
                    channel_id: "channel:p2p:worker1:worker2".to_string(),
                    sender: "worker2".to_string(),
                    payload: "hello".to_string(),
                },
            ),
        )
        .await;
        assert_ok(&resp);
    }

    // ── Lock-free compute (CONCEPT:KG-2.51) ─────────────────────────────

    fn json_props(val: serde_json::Value) -> Option<Vec<u8>> {
        // SemanticSearch's decay path reads properties as a UTF-8 JSON string
        // (NodeData::from_json_props), so encode candidates the same way.
        Some(val.to_string().into_bytes())
    }

    #[test]
    fn test_weight_semantic_results_orders_decays_and_truncates() {
        let now = 100_000_000u64;
        let thirty_days = 30 * 86_400u64;
        let candidates = vec![
            // Fresh fact: confidence 1.0, no decay → keeps raw similarity.
            (
                "fresh".to_string(),
                0.8f32,
                json_props(serde_json::json!({"type": "Fact", "valid_from": now})),
            ),
            // One half-life old: 0.9 similarity decays to ~0.45 → ranks below.
            (
                "aged".to_string(),
                0.9f32,
                json_props(serde_json::json!({"type": "Fact", "valid_from": now - thirty_days})),
            ),
            // Validity window closed → filtered out entirely.
            (
                "stale".to_string(),
                0.99f32,
                json_props(serde_json::json!({"type": "Fact", "valid_until": now - 1})),
            ),
            // No properties → similarity passes through unweighted.
            ("bare".to_string(), 0.5f32, None),
        ];

        let out = weight_semantic_results(candidates, now, 10);
        let ids: Vec<&str> = out.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["fresh", "bare", "aged"]);
        let aged_score = out[2].1;
        assert!(
            (aged_score - 0.45).abs() < 0.01,
            "expected ~0.45 after one half-life, got {aged_score}"
        );

        // Truncation honors n_results after re-ranking.
        let top1 = weight_semantic_results(
            vec![
                ("a".to_string(), 0.4f32, None),
                ("b".to_string(), 0.7f32, None),
            ],
            now,
            1,
        );
        assert_eq!(top1.len(), 1);
        assert_eq!(top1[0].0, "b");
    }

    /// Writers must keep making progress while a large semantic search (HNSW
    /// path, index rebuilt per query) runs concurrently on the same graph.
    /// Before KG-2.51 the search held the graph read lock for its whole
    /// duration; now it only memcpys the embedding store under the lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_writers_not_starved_by_large_semantic_search() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:busy", GraphType::Agent, None)
                .unwrap();
        }
        // Seed enough embeddings to take the HNSW path (>= brute-force
        // threshold) and make the search non-trivial.
        {
            let s = state.read().await;
            let core = s.registry.get("agent:busy").unwrap().core.clone();
            drop(s);
            let mut g = core.write().await;
            for i in 0..2_000u32 {
                let id = format!("n{}", i);
                g.add_node(
                    id.clone(),
                    rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                );
                let emb: Vec<f32> = (0..64).map(|d| ((i + d) % 97) as f32 / 97.0).collect();
                g.semantic_store.add_embedding(id, emb);
            }
        }

        let search_state = state.clone();
        let search = tokio::spawn(async move {
            dispatch(
                &search_state,
                request(
                    1,
                    "agent:busy",
                    None,
                    Method::SemanticSearch {
                        query_embedding: vec![0.5f32; 64],
                        n_results: 25,
                    },
                ),
            )
            .await
        });

        // Interleave writes while the search task runs; each must complete
        // promptly instead of queueing behind a long-held read lock.
        for i in 0..50u64 {
            let resp = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                dispatch(
                    &state,
                    request(100 + i, "agent:busy", None, add_node(&format!("w{}", i))),
                ),
            )
            .await
            .expect("writer starved during semantic search");
            assert_ok(&resp);
            tokio::task::yield_now().await;
        }

        let resp = search.await.expect("search task panicked");
        assert_ok(&resp);
        assert!(matches!(resp.result, Some(ResultPayload::Json(_))));
    }

    #[tokio::test]
    async fn test_offloaded_algorithms_round_trip() {
        // Snapshot+spawn_blocking arms must preserve result semantics.
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:algo", GraphType::Agent, None)
                .unwrap();
        }
        for (id, m) in [
            (1, add_node("a")),
            (2, add_node("b")),
            (3, add_node("c")),
            (
                4,
                Method::AddEdge {
                    source_id: "a".into(),
                    target_id: "b".into(),
                    properties_msgpack: rmp_serde::to_vec(&serde_json::json!({"weight": 2.0}))
                        .unwrap(),
                },
            ),
            (
                5,
                Method::AddEdge {
                    source_id: "b".into(),
                    target_id: "c".into(),
                    properties_msgpack: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                },
            ),
        ] {
            assert_ok(&dispatch(&state, request(id, "agent:algo", None, m)).await);
        }

        let pagerank = dispatch(
            &state,
            request(
                10,
                "agent:algo",
                None,
                Method::PageRank {
                    damping: 0.85,
                    iterations: 20,
                },
            ),
        )
        .await;
        assert_ok(&pagerank);
        let Some(ResultPayload::Json(scores)) = pagerank.result else {
            panic!("expected JSON pagerank result");
        };
        assert_eq!(scores.as_array().map(|a| a.len()), Some(3));

        let communities = dispatch(
            &state,
            request(
                11,
                "agent:algo",
                None,
                Method::CommunityDetection { resolution: 1.0 },
            ),
        )
        .await;
        assert_ok(&communities);

        let metrics = dispatch(&state, request(12, "agent:algo", None, Method::Metrics)).await;
        assert_ok(&metrics);
        let Some(ResultPayload::Json(m)) = metrics.result else {
            panic!("expected JSON metrics result");
        };
        assert_eq!(m.get("node_count").and_then(|v| v.as_u64()), Some(3));
        assert_eq!(m.get("edge_count").and_then(|v| v.as_u64()), Some(2));
        // total_mutations comes from the ledger length captured under-lock
        // (3 adds + 2 edges) — the snapshot itself carries no ledger.
        assert_eq!(m.get("total_mutations").and_then(|v| v.as_u64()), Some(5));
    }

    #[tokio::test]
    async fn test_diff_against_gates_other_graph() {
        let state = multi_tenant_state().await;
        // worker2 owns nothing here; create their graph for the diff source.
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:worker2", GraphType::Agent, Some("worker2".into()))
                .unwrap();
        }
        // worker2 may read its own graph but NOT diff it against worker1's.
        let resp = dispatch(
            &state,
            request(
                1,
                "agent:worker2",
                Some("worker2"),
                Method::DiffAgainst {
                    other_graph: "agent:worker1".to_string(),
                },
            ),
        )
        .await;
        assert_denied(&resp);
    }
}
