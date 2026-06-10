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
use crate::isolation::IsolationLayer;
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
        Method::Ping => Response::ok(req.id, ResultPayload::String("pong".to_string())),

        Method::Health => {
            // Simplified health check for now
            let uptime_s = 0; // you can capture start time in ServerState
            let mem_bytes = 0;
            // ``version`` + ``ops`` let clients negotiate capabilities (e.g. only
            // use ``ParseFiles`` against an engine that advertises it) and fall
            // back gracefully against an older binary. (CONCEPT:KG-2.19)
            Response::ok(req.id, ResultPayload::Json(serde_json::json!({
                "status": "ok",
                "uptime_s": uptime_s,
                "mem_bytes": mem_bytes,
                "version": env!("CARGO_PKG_VERSION"),
                "ops": ["ParseFiles"]
            })))
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
                            return Response::err(
                                req.id,
                                format!("Invalid files_msgpack: {}", e),
                            );
                        }
                    };
                let owned: Vec<(String, Vec<u8>)> = files
                    .into_iter()
                    .map(|(p, b)| (p, b.into_vec()))
                    .collect();
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
        Method::CreateGraph { graph_name, graph_type } => {
            let mut s = state.write().await;
            match s.registry.create_graph(&graph_name, graph_type, None) {
                Ok(()) => Response::ok(req.id, ResultPayload::Json(serde_json::json!({"created": graph_name}))),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::DeleteGraph { graph_name } => {
            let mut s = state.write().await;
            match s.registry.delete_graph(&graph_name) {
                Ok(()) => Response::ok(req.id, ResultPayload::Json(serde_json::json!({"deleted": graph_name}))),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::ListGraphs => {
            let s = state.read().await;
            let graphs: Vec<serde_json::Value> = s.registry.list().iter().map(|(name, gt)| {
                serde_json::json!({"name": name, "type": gt})
            }).collect();
            Response::ok(req.id, ResultPayload::Json(serde_json::json!(graphs)))
        }

        // ── Channel operations ───────────────────────────────────────
        Method::CreateChannel { channel_id, channel_type, creator, initial_members } => {
            let mut s = state.write().await;
            match s.channels.create_channel(&channel_id, channel_type, &creator, initial_members) {
                Ok(()) => Response::ok(req.id, ResultPayload::Json(serde_json::json!({"channel": channel_id}))),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::JoinChannel { channel_id, agent_id } => {
            let mut s = state.write().await;
            match s.channels.join_channel(&channel_id, &agent_id) {
                Ok(()) => Response::ok(req.id, ResultPayload::String("joined".to_string())),
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
                    Response::ok(req.id, ResultPayload::Json(val))
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
                    Response::ok(req.id, ResultPayload::Json(val))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::SendMessage { channel_id, sender, payload } => {
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
                Ok(members) => Response::ok(req.id, ResultPayload::Json(serde_json::json!(members))),
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
            Response::ok(req.id, ResultPayload::String("registered".to_string()))
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
            let nodes: Vec<(String, serde_json::Value)> = g.get_nodes()
                .into_iter()
                .map(|(k, p)| {
                    let val = rmp_serde::from_slice::<serde_json::Value>(&p).unwrap_or(serde_json::json!({}));
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

            Response::ok(req_id, ResultPayload::Json(serde_json::json!(weighted_results)))
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
        Method::FinanceOptimizePortfolio { expected_returns, cov_matrix, risk_free_rate, min_weight, max_weight } => {
            let result = crate::finance::optimizer::mean_variance_optimization(&expected_returns, &cov_matrix, risk_free_rate, min_weight, max_weight);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::FinanceRiskParity { cov_matrix } => {
            let result = crate::finance::optimizer::risk_parity(&cov_matrix);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::FinanceBlackLitterman { market_weights, cov_matrix, views, pick_matrix, tau, risk_aversion } => {
            let result = crate::finance::optimizer::black_litterman(
                &market_weights, &cov_matrix, &views, &pick_matrix, tau, risk_aversion
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::FinanceEfficientFrontier { expected_returns, cov_matrix, target_return } => {
            let result = crate::finance::optimizer::efficient_frontier_target(&expected_returns, &cov_matrix, target_return);
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
        Method::DsTrainTestSplit { data, labels, test_ratio, shuffle, seed } => {
            let (x_train, x_test, y_train, y_test) =
                crate::datascience::primitives::train_test_split(&data, &labels, test_ratio, shuffle, seed);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!({
                "x_train": x_train,
                "x_test": x_test,
                "y_train": y_train,
                "y_test": y_test,
            })))
        }
        Method::DsFitEstimator { estimator, x, y, params } => {
            match crate::datascience::estimators::fit_estimator(&estimator, &x, &y, &params) {
                Ok(model) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(model))),
                Err(e) => Response::err(req_id, e),
            }
        }
        Method::DsPredictEstimator { model, x } => {
            let preds = crate::datascience::estimators::predict(&model, &x);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(preds)))
        }

        // ── Training loss / optimizer kernels (CONCEPT:KG-2.22) ────────
        Method::DsSoftmax { logits, temperature } => {
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
        Method::DsGrpoSurrogate { logprob, old_logprob, advantage, clip_eps } => {
            let r = crate::datascience::training::grpo_surrogate(
                &logprob,
                &old_logprob,
                &advantage,
                clip_eps,
            );
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(r)))
        }
        Method::DsKlDivergence { logprob, ref_logprob } => {
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
        Method::FinanceVar { returns, confidence } => {
            let v = crate::finance::risk::historical_var(&returns, confidence);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceCvar { returns, confidence } => {
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
        Method::FinanceRiskMetrics { returns, risk_free_rate } => {
            let result = crate::finance::risk::compute_risk_metrics(&returns, risk_free_rate);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(result)))
        }
        Method::FinanceMonteCarloVar { mean, std_dev, n_simulations, confidence } => {
            let v = crate::finance::risk::monte_carlo_var(mean, std_dev, n_simulations, confidence);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceStressTest { weights, expected_returns, cov_matrix, shock_factors } => {
            let v = crate::finance::risk::stress_test(&weights, &expected_returns, &cov_matrix, &shock_factors);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── Extended Finance: Regime detection (HMM) ──────────────────
        Method::FinanceDetectRegimes { observations, n_states, max_iter, tol } => {
            let result = crate::finance::regime::detect_regimes(&observations, n_states, max_iter, tol);
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
        Method::FinanceInformationCoefficient { signal, forward_returns } => {
            let v = crate::finance::signals::information_coefficient(&signal, &forward_returns);
            Response::ok(req_id, ResultPayload::Float(v))
        }

        // ── Extended Finance: Execution / microstructure ──────────────
        Method::FinanceTwap { total_quantity, n_slices, start_time, interval_secs } => {
            let v = crate::finance::exchange::twap_schedule(total_quantity, n_slices, start_time, interval_secs);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceVwap { total_quantity, volume_profile, start_time, interval_secs } => {
            let v = crate::finance::exchange::vwap_schedule(total_quantity, &volume_profile, start_time, interval_secs);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMarketImpact { daily_volatility, order_quantity, average_daily_volume, impact_coefficient } => {
            let v = crate::finance::exchange::estimate_market_impact(daily_volatility, order_quantity, average_daily_volume, impact_coefficient);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinancePairsTrading { prices_a, prices_b, lookback } => {
            let v = crate::finance::exchange::pairs_trading_signal(&prices_a, &prices_b, lookback);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMatchOrders { orders } => {
            let v = crate::finance::exchange::match_orders(&orders);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── Market Making / Microstructure (CONCEPT:KG-2.20f) ─────────
        Method::FinanceAvellanedaStoikov { mid, inventory, sigma, gamma, kappa, tau } => {
            let v = crate::finance::quant::avellaneda_stoikov(mid, inventory, sigma, gamma, kappa, tau);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceGltQuotes { mid, inventory, sigma, gamma, kappa, a } => {
            let v = crate::finance::quant::glt_quotes(mid, inventory, sigma, gamma, kappa, a);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceLogitQuotes { p_mid, inventory, sigma, gamma, kappa, tau, boundary_m } => {
            let v = crate::finance::quant::logit_space_quotes(p_mid, inventory, sigma, gamma, kappa, tau, boundary_m);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceGlostenMilgromSpread { alpha, p } => {
            let v = crate::finance::quant::glosten_milgrom_spread(alpha, p);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceExpectedPnlRate { delta, a, kappa, alpha, p, v_h, v_l } => {
            let v = crate::finance::quant::expected_pnl_rate(delta, a, kappa, alpha, p, v_h, v_l);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceBreakevenAlpha { delta, p, v_h, v_l } => {
            let v = crate::finance::quant::breakeven_alpha(delta, p, v_h, v_l);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceOfiSeries { ts, bid_px, bid_sz, ask_px, ask_sz, window_secs } => {
            let v = crate::finance::quant::ofi_series(&ts, &bid_px, &bid_sz, &ask_px, &ask_sz, window_secs);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceMicropriceSeries { bid_px, bid_sz, ask_px, ask_sz } => {
            let v = crate::finance::quant::microprice_series(&bid_px, &bid_sz, &ask_px, &ask_sz);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceVpinPm { buy_vol, sell_vol, p_mean } => {
            let v = crate::finance::quant::vpin_pm(&buy_vol, &sell_vol, &p_mean);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceHawkesMle { times, t_horizon, max_iter } => {
            let v = crate::finance::quant::hawkes_mle(&times, t_horizon, max_iter);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceHardimanBouchaud { times, t_horizon, n_windows } => {
            let v = crate::finance::quant::hardiman_bouchaud_branching_ratio(&times, t_horizon, n_windows);
            Response::ok(req_id, ResultPayload::Float(v))
        }

        // ── Position Sizing (CONCEPT:KG-2.20f) ────────────────────────
        Method::FinanceKellyFraction { q, c, fraction } => {
            let v = crate::finance::quant::kelly_fraction(q, c, fraction);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceBayesianKelly { alpha, beta, c, n_quadrature } => {
            let v = crate::finance::quant::bayesian_kelly_fraction(alpha, beta, c, n_quadrature);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinancePosteriorCredibleInterval { alpha, beta, level } => {
            let (lo, hi) = crate::finance::quant::posterior_credible_interval(alpha, beta, level);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!({"lower": lo, "upper": hi})))
        }

        // ── Backtest Validation (CONCEPT:KG-2.20f) ────────────────────
        Method::FinancePurgedCpcv { n_samples, n_groups, n_test_groups, purge_window, embargo } => {
            let v = crate::finance::quant::purged_cpcv_splits(n_samples, n_groups, n_test_groups, purge_window, embargo);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceDeflatedSharpe { observed_sr, n_trials, sr_returns } => {
            let v = crate::finance::quant::deflated_sharpe_ratio(observed_sr, n_trials, &sr_returns);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceProbabilityBacktestOverfit { insample, oos } => {
            let v = crate::finance::quant::probability_of_backtest_overfit(&insample, &oos);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceDieboldMariano { losses_a, losses_b, h } => {
            let v = crate::finance::quant::diebold_mariano(&losses_a, &losses_b, h);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── Forensic Accounting (CONCEPT:KG-2.20g) ────────────────────
        Method::FinanceForensicReport { this_year, prior_year } => {
            let v = crate::finance::forensic::forensic_report(&this_year, &prior_year);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }

        // ── State-Space / Stat-Arb (CONCEPT:KG-2.20h) ─────────────────
        Method::FinanceKalmanFilter1d { observations, f, q, h, r, x0, p0 } => {
            let v = crate::finance::statespace::kalman_filter_1d(&observations, f, q, h, r, x0, p0);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceKalmanBeta { market_returns, asset_returns, q, r, beta0, p0 } => {
            let v = crate::finance::statespace::kalman_beta(&market_returns, &asset_returns, q, r, beta0, p0);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceKalmanVolatility { returns, q, r, log_var0, p0, annualization } => {
            let v = crate::finance::statespace::kalman_volatility(&returns, q, r, log_var0, p0, annualization);
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
        Method::FinanceOuOptimalThresholds { theta, mu, sigma, sigma_eq, cost } => {
            let params = crate::finance::statespace::OuParams {
                theta, mu, sigma, sigma_eq,
                half_life: if theta > 1e-12 { std::f64::consts::LN_2 / theta } else { f64::INFINITY },
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
        Method::FinanceInformationRatio { ic, n_independent } => {
            let v = crate::finance::quant::information_ratio(ic, n_independent);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceEffectiveIndependentN { returns_matrix } => {
            let v = crate::finance::quant::effective_independent_n(&returns_matrix);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceAlphaCombinationEngine { returns_matrix, lookback } => {
            let v = crate::finance::quant::alpha_combination_engine(&returns_matrix, lookback);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceBrierScore { forecasts, outcomes } => {
            let v = crate::finance::quant::brier_score(&forecasts, &outcomes);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceConvergenceGate { strengths, strong_threshold, min_agree } => {
            let v = crate::finance::quant::convergence_gate(&strengths, strong_threshold, min_agree);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceEmpiricalKelly { p, b, historical_returns, n_simulations, seed } => {
            let v = crate::finance::quant::empirical_kelly(p, b, &historical_returns, n_simulations, seed);
            Response::ok(req_id, ResultPayload::Float(v))
        }

        // ── Derivatives: SABR volatility surface (CONCEPT:KG-2.20j) ────
        Method::FinanceSabrImpliedVol { f, k, t, alpha, beta, rho, nu } => {
            let v = crate::finance::derivatives::sabr_implied_vol(f, k, t, alpha, beta, rho, nu);
            Response::ok(req_id, ResultPayload::Float(v))
        }
        Method::FinanceSabrSmile { f, strikes, t, alpha, beta, rho, nu } => {
            let v = crate::finance::derivatives::sabr_smile(f, &strikes, t, alpha, beta, rho, nu);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FinanceSabrCalibrate { f, t, strikes, market_vols, beta } => {
            let v = crate::finance::derivatives::sabr_calibrate(f, t, &strikes, &market_vols, beta);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(v)))
        }
        Method::FindSimilarPairs { embeddings: _, ids: _, threshold: _, use_lsh: _, lsh_num_tables: _, lsh_hash_size: _, seed: _ } => {
            Response::err(req_id, "FindSimilarPairs is deprecated. Use datascience primitives.".to_string())
        }
        Method::AddEdge { source_id, target_id, properties_msgpack} => {
            let mut g = core.write().await;
            match g.add_edge(source_id, target_id, properties_msgpack) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::RemoveEdge { source_id, target_id } => {
            let mut g = core.write().await;
            g.remove_edge(source_id, target_id);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        Method::HasEdge { source_id, target_id } => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Bool(g.has_edge(&source_id, &target_id)))
        }
        Method::GetEdges => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::EdgeList(g.get_edges()))
        }
        Method::GetEdgeProperties { source_id, target_id } => {
            let g = core.read().await;
            let props = g.get_edge_properties(&source_id, &target_id);
            let val: Vec<serde_json::Value> = props.into_iter()
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
        Method::TopologicalSort => {
            let g = core.read().await;
            match crate::algorithms::topological_sort(&g) {
                Ok(order) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(order))),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::FindCycle => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(crate::algorithms::find_cycle(&g))))
        }
        Method::GetShortestPath { source_id, target_id } => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(
                crate::algorithms::get_shortest_path(&g, &source_id, &target_id)
            )))
        }
        Method::PageRank { damping, iterations } => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(
                crate::algorithms::pagerank(&g, damping, iterations)
            )))
        }
        Method::ConnectedComponents => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(
                crate::algorithms::connected_components(&g)
            )))
        }
        Method::StronglyConnectedComponents => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(
                crate::algorithms::strongly_connected_components(&g)
            )))
        }
        Method::MinimumSpanningTree => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(
                crate::algorithms::minimum_spanning_tree(&g)
            )))
        }
        Method::Metrics => {
            let g = core.read().await;
            let m = crate::algorithms::compute_metrics(&g);
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
        Method::DecaySweep { half_life_secs, floor, prune } => {
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
        Method::Reconcile { graph_name: _, msgpack } => {
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
                                let _ = g.add_edge(s.to_string(), o.to_string(), format!("{{\"predicate\": \"{}\"}}", p).into_bytes());
                            } else {
                                // For delete, we just remove the edge matching the predicate
                                g.remove_edge(s.to_string(), o.to_string());
                            }
                        }
                    }
                }
            }
            Response::ok(req_id, ResultPayload::String("mutation_applied".to_string()))
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
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(crate::algorithms::get_blast_radius(&g, &node_id, max_depth))))
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
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(crate::algorithms::degree_centrality_all(&g))))
        }
        Method::BetweennessCentrality => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(crate::algorithms::betweenness_centrality(&g))))
        }
        Method::PersonalizedPageRank { seed_nodes, damping, iterations } => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(crate::algorithms::personalized_pagerank(&g, &seed_nodes, damping, iterations))))
        }
        Method::CommunityDetection { resolution } => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(crate::algorithms::community_detection(&g, resolution))))
        }
        Method::GraphColoring => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(crate::algorithms::graph_coloring(&g))))
        }
        Method::ComputeSimilarityEdges { threshold } => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(crate::algorithms::compute_similarity_edges(&g, threshold))))
        }
        Method::PruneByLifecycle { max_age_secs, min_score } => {
            let mut g = core.write().await;
            let stats = crate::algorithms::prune_by_lifecycle(&mut g, max_age_secs, min_score);
            match serde_json::to_value(&stats) {
                Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::GetContextView { agent_id, max_tokens } => {
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
            let pattern_core = s.registry.get(&pattern_graph_name).map(|entry| entry.core.clone());
            drop(s);
            if let Some(p_core) = pattern_core {
                let p = p_core.read().await;
                let g = core.read().await;
                Response::ok(req_id, ResultPayload::Json(serde_json::json!(g.vf2_subgraph_match(&p))))
            } else {
                Response::err(req_id, format!("Pattern graph '{}' not found", pattern_graph_name))
            }
        }
        Method::GetLedger => {
            let g = core.read().await;
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(g.get_ledger())))
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
                None => return Response::err(req_id, format!("Other graph '{}' not found", other_graph)),
            };
            let other_core = other_entry.core.clone();
            drop(s_lock);

            let g1 = core.read().await;
            let g2 = other_core.read().await;
            let diff_str = g1.diff_against(&g2);
            match serde_json::from_slice::<serde_json::Value>(diff_str.as_bytes()) {
                Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::CompactNodesByType { node_type, threshold } => {
            let mut g = core.write().await;
            let removed = g.compact_nodes_by_type(&node_type, threshold);
            Response::ok(req_id, ResultPayload::Json(serde_json::json!({ "removed_nodes": removed })))
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
                let resp =
                    Response::err(req.id, "BUSY: server at capacity, retry with backoff");
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
