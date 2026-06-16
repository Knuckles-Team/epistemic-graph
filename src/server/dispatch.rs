//! The request dispatch shell: auth, service-level methods, and the graph-op
//! routing chain. Per-domain logic lives in `handlers/`; this file only routes
//! and owns the centralized post-match write side-effects (dirty/WAL/gauge).

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use super::access::{check_graph_access, requires_write};
use super::auth::verify_auth;
// Only the ast-gated ParseFiles handler offloads to the blocking pool here; the
// graph-op off-lock sites live in handlers/graph_ops.rs.
#[cfg(feature = "ast")]
use super::compute::compute_off_lock;
use super::handlers;
use super::state::ServerState;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Request, Response, ResultPayload};

/// Dispatch a single request to the appropriate handler, recording
/// per-operation request counters and latency (CONCEPT:KG-2.51).
pub async fn dispatch(state: &Arc<RwLock<ServerState>>, req: Request) -> Response {
    #[cfg(feature = "metrics")]
    {
        let op: &'static str = (&req.method).into();
        let start = std::time::Instant::now();
        let resp = dispatch_inner(state, req).await;
        crate::metrics::record_request(op, start.elapsed().as_secs_f64());
        resp
    }
    #[cfg(not(feature = "metrics"))]
    dispatch_inner(state, req).await
}

async fn dispatch_inner(state: &Arc<RwLock<ServerState>>, req: Request) -> Response {
    // Auth check.
    {
        let s = state.read().await;
        if !verify_auth(&s.auth_secret, req.id, &req.auth_token) {
            crate::metrics::auth_failure();
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
                    "ops": ["ParseFiles", "IndexRepository"]
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
                // Parse on the blocking pool, NOT the async reactor: parse_files is
                // CPU-bound (rayon tree-sitter over every file) and a large batch
                // would otherwise stall the runtime thread, blocking unrelated
                // requests until it finishes. (CONCEPT:KG-2.8 — work off-reactor, A4)
                let results = match compute_off_lock(req.id, move || {
                    crate::parser::tree_sitter::parse_files(&owned)
                })
                .await
                {
                    Ok(r) => r,
                    Err(resp) => return resp,
                };
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

        Method::IndexRepository { files_msgpack } => {
            #[cfg(feature = "ast")]
            {
                // Same blob shape as ParseFiles (`Vec<(file_path, source_bytes)>`),
                // but parsed AND cross-file-resolved into a single IndexResult.
                let files: Vec<(String, serde_bytes::ByteBuf)> =
                    match rmp_serde::from_slice(&files_msgpack) {
                        Ok(f) => f,
                        Err(e) => {
                            return Response::err(req.id, format!("Invalid files_msgpack: {}", e));
                        }
                    };
                let owned: Vec<(String, Vec<u8>)> =
                    files.into_iter().map(|(p, b)| (p, b.into_vec())).collect();
                // Off-reactor like ParseFiles: parse (rayon) + resolution are
                // CPU-bound over the whole batch. (CONCEPT:KG-2.8r)
                let result = match compute_off_lock(req.id, move || {
                    crate::parser::resolve::index_repository(&owned)
                })
                .await
                {
                    Ok(r) => r,
                    Err(resp) => return resp,
                };
                match serde_json::to_value(&result) {
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
                Ok(()) => {
                    crate::metrics::set_graph_size(&graph_name, 0, 0);
                    Response::ok(
                        req.id,
                        ResultPayload::Json(serde_json::json!({"created": graph_name})),
                    )
                }
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
                Ok(()) => {
                    crate::metrics::drop_graph(graph_name);
                    Response::ok(
                        req.id,
                        ResultPayload::Json(serde_json::json!({"deleted": graph_name})),
                    )
                }
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
    // WAL (Phase B3): clone the off-reactor writer handle under the registry lock
    // so a durable mutation can enqueue its append after it applies, with no extra
    // locking and no file I/O on this Tokio worker. Only durable DATA mutations are
    // logged, and only when the WAL service is running (i.e. a persist dir is set).
    let wal_service = s.wal_service.clone();
    drop(s); // Release registry lock before graph lock.

    let wal_method = match (&wal_service, crate::wal::is_durable_mutation(&method)) {
        (Some(_), true) => Some(method.clone()),
        _ => None,
    };

    crate::metrics::graph_op(graph_name);

    let response = 'dispatch: {
        // Pure-compute domains (stateless: no graph core / lock) route first; a
        // method that isn't theirs is handed back via Err and falls through to the
        // graph-op match below. (CONCEPT:KG-2.19 — thin routing; logic in handlers/.)
        // Feature-gated: in a slim build the line is absent and the method flows
        // straight through to graph_ops (whose catch-all reports "not available").
        #[cfg(feature = "finance")]
        let method = match handlers::finance::try_handle(req_id, method) {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        #[cfg(feature = "datascience")]
        let method = match handlers::datascience::try_handle(req_id, method) {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Terminal handler: graph-targeted ops (borrow the core; cross-graph ops
        // re-enter the registry via `state`). Owns the catch-all, returns a Response.
        // Bind then `break` (not a tail expr) so the `'dispatch` label stays used
        // even when both compute-routing lines above are feature-gated out.
        let resp =
            handlers::graph_ops::try_handle(state, req_id, caller, core.clone(), method).await;
        break 'dispatch resp;
    };

    // Refresh the per-graph size gauges after mutations — both petgraph
    // counts are O(1), so this adds no meaningful write-path cost.
    #[cfg(feature = "metrics")]
    if matches!(access, AccessLevel::Write) {
        let topo = core.topo.read();
        crate::metrics::set_graph_size(
            graph_name,
            topo.graph.node_count() as i64,
            topo.graph.edge_count() as i64,
        );
    }

    // Mark the graph dirty after any successful write so the next checkpoint
    // rewrites it; clean graphs are skipped (Phase C-C — incremental checkpoint).
    if matches!(access, AccessLevel::Write) && response.error.is_none() {
        core.mark_dirty();
    }

    // Enqueue the WAL append after a SUCCESSFUL durable mutation (write-behind; the
    // durable backend is the system-of-record, this is the fast local crash-
    // consistency layer that closes the between-checkpoint loss window). The append
    // is serialized here (cheap CPU) and handed to the off-reactor writer thread so
    // no file I/O runs on this Tokio worker. (CONCEPT:KG-2.8 / Phase B3)
    if let (Some(m), Some(svc)) = (wal_method, wal_service) {
        if response.error.is_none() {
            match rmp_serde::to_vec_named(&m) {
                Ok(bytes) => svc.append(crate::persist::sanitize(graph_name), bytes),
                Err(e) => tracing::warn!("WAL serialize failed for '{}': {}", graph_name, e),
            }
        }
    }
    response
}

// ── Tests ───────────────────────────────────────────────────────────────
