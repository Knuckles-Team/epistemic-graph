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
                    "ops": ["ParseFiles", "IndexRepository", "ObserveScreen"]
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

        Method::ObserveScreen { obs_msgpack } => {
            // MessagePack map → a captured desktop frame. png rides as a bin field;
            // elements are the AT-SPI accessibles. (CONCEPT:KG-2.185)
            #[derive(serde::Deserialize)]
            struct Wire {
                session_id: String,
                #[serde(default)]
                frame_seq: u64,
                #[serde(default)]
                prev_frame_id: String,
                #[serde(default)]
                prev_hash: u64,
                #[serde(with = "serde_bytes", default)]
                png: Vec<u8>,
                #[serde(default)]
                elements: Vec<crate::screen::UiElementInput>,
            }
            let wire: Wire = match rmp_serde::from_slice(&obs_msgpack) {
                Ok(w) => w,
                Err(e) => return Response::err(req.id, format!("Invalid obs_msgpack: {}", e)),
            };
            let input = crate::screen::ScreenObservationInput {
                session_id: wire.session_id,
                frame_seq: wire.frame_seq,
                prev_frame_id: wire.prev_frame_id,
                prev_hash: wire.prev_hash,
                png: wire.png,
                elements: wire.elements,
            };
            // Inline: PNG hashing + node/edge build over the element set is
            // microsecond-cheap (no AST parse), so it doesn't need the blocking pool.
            let result = crate::screen::observe_screen(&input);
            match serde_json::to_value(&result) {
                Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
                Err(e) => Response::err(req.id, format!("Serialization error: {}", e)),
            }
        }

        Method::Shutdown => {
            info!("Shutdown requested via protocol");
            Response::ok(req.id, ResultPayload::String("shutting_down".to_string()))
        }

        Method::Checkpoint => {
            info!("Checkpoint requested");
            // Route through the configured durable backend (CONCEPT:KG-2.177) so a
            // protocol-triggered checkpoint persists via whichever tier is active.
            let backend = { state.read().await.persistence.clone() };
            let result = match backend {
                Some(p) => p.checkpoint_all(state).await,
                None => Ok(0),
            };
            match result {
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

        // ── Transactions (CONCEPT:KG-2.180 — multi-op OCC ACID) ──────
        // Stateful + self-routing: a Txn* op targets the graph the txn was opened
        // against (resolved from `open_txns`), NOT necessarily `req.graph`, and
        // BeginTxn carries its own graph. So they are handled here (with `state`)
        // BEFORE the graph-op path — never through `dispatch_graph_op`, whose
        // coalescer/registry-lookup assumes a single `req.graph` target. For
        // BeginTxn the request envelope's `graph` is the default target when the
        // body omits one.
        Method::BeginTxn { .. }
        | Method::TxnAddNode { .. }
        | Method::TxnRemoveNode { .. }
        | Method::TxnAddEdge { .. }
        | Method::TxnRemoveEdge { .. }
        | Method::TxnCas { .. }
        | Method::Commit { .. }
        | Method::Rollback { .. } => {
            // BeginTxn defaults its target to the request envelope's graph.
            let method = match req.method {
                Method::BeginTxn {
                    graph: None,
                    isolation,
                } => Method::BeginTxn {
                    graph: Some(req.graph.clone()),
                    isolation,
                },
                m => m,
            };
            match handlers::txn::try_handle(state, req.id, req.agent_id.as_deref(), method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a txn method.
                Err(_) => Response::err(req.id, "txn dispatch routing error"),
            }
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
    // Persistence (CONCEPT:KG-2.177): clone the durable backend handle under the
    // registry lock so a durable mutation can record itself after it applies, with
    // no extra locking and no file I/O on this Tokio worker (the backend hands the
    // write to its own off-reactor writer). Only durable DATA mutations are
    // recorded, and only when a backend is configured (i.e. a persist dir is set).
    let persistence = s.persistence.clone();
    // Per-graph write coalescer (CONCEPT:KG-2.182): clone the registry handle so the
    // hot single-op writes can be batched onto this graph's writer (lazily created,
    // keyed by name — automatic per new graph/connector), collapsing N concurrent
    // topology-lock acquisitions into one per batch. Cheap Arc clone under the lock.
    let write_coalescer = s.write_coalescer.clone();
    drop(s); // Release registry lock before graph lock.

    let record_method = match (&persistence, crate::wal::is_durable_mutation(&method)) {
        (Some(_), true) => Some(method.clone()),
        _ => None,
    };

    crate::metrics::graph_op(graph_name);

    let response = 'dispatch: {
        // Per-graph write coalescer (CONCEPT:KG-2.182): the five high-frequency
        // single-op writes are batched onto this graph's writer so M concurrent
        // writers cost ⌈M/batch⌉ topology-lock acquisitions instead of M. The shell
        // below still owns dirty/WAL/gauge off the returned Response, so durability
        // and checkpoint semantics are unchanged — only WHERE the lock is taken
        // moved. On a full queue / disabled coalescer the method is handed back and
        // flows through the inline path unchanged.
        let method =
            match try_coalesce_write(req_id, &write_coalescer, graph_name, &core, method).await {
                Ok(resp) => break 'dispatch resp,
                Err(m) => m,
            };
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
        // Read-only SQL query (CONCEPT:KG-2.178): borrows the graph core for an
        // off-lock snapshot, runs DataFusion on the blocking pool. Slim builds omit
        // this line and Method::Sql falls to the graph_ops not-available catch-all.
        #[cfg(feature = "query")]
        let method = match handlers::query::try_handle(req_id, core.clone(), method).await {
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

    // Record the durable mutation after it SUCCEEDED (write-behind; the abstracted
    // backend remains the system-of-record, this is the fast local crash-
    // consistency layer that closes the between-checkpoint loss window). The chosen
    // backend serializes + hands the write to its own off-reactor writer thread, so
    // no file I/O runs on this Tokio worker. (CONCEPT:KG-2.177 / KG-2.8)
    if let (Some(m), Some(p)) = (record_method, persistence) {
        if response.error.is_none() {
            p.record(&crate::persist::sanitize(graph_name), &m);
        }
    }
    response
}

/// Route the five high-frequency single-op writes through the per-graph write
/// coalescer (CONCEPT:KG-2.182). On success returns the same `Response` the inline
/// handler would have produced (so the dispatch shell's dirty/WAL/gauge logic runs
/// identically against it). Returns `Err(method)` — handing the method back
/// untouched — for any method that isn't coalescable, or when the coalescer is
/// disabled / its bounded queue is full (backpressure → inline fallback) / the
/// worker is gone, so behavior is never lost, only the lock-acquisition path
/// changes.
async fn try_coalesce_write(
    req_id: u64,
    coalescer: &crate::write_coalescer::WriteCoalescerRegistry,
    graph_name: &str,
    core: &Arc<crate::graph::GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    use crate::write_coalescer::{WriteOp, WriteOutcome};
    use tokio::sync::oneshot;

    // Not enabled → no writer; stay inline (no lazy creation, no spawn).
    if !coalescer.enabled() {
        return Err(method);
    }

    // Build this op's reply channel; the op carries the sender, we await the receiver
    // regardless of whether it was batched or applied inline on fallback.
    let (reply, reply_rx) = oneshot::channel::<WriteOutcome>();

    // Map the method → a WriteOp (consuming its args). For CompareAndSetNodeFields,
    // decode the two msgpack blobs FIRST: a decode failure is a CAS failure
    // (Bool(false)) that does NOT touch the graph — exactly the inline handler's
    // contract — so short-circuit without enqueuing (the shell then WAL-logs the
    // method, matching inline). Non-coalescable methods are handed straight back.
    let op = match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => WriteOp::AddNode {
            node_id,
            properties_msgpack,
            reply,
        },
        Method::RemoveNode { node_id } => WriteOp::RemoveNode { node_id, reply },
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => WriteOp::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
            reply,
        },
        Method::RemoveEdge {
            source_id,
            target_id,
        } => WriteOp::RemoveEdge {
            source_id,
            target_id,
            reply,
        },
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            let conditions = match rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                &conditions_msgpack,
            ) {
                Ok(m) => m,
                Err(_) => return Ok(Response::ok(req_id, ResultPayload::Bool(false))),
            };
            let updates = match rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                &updates_msgpack,
            ) {
                Ok(m) => m,
                Err(_) => return Ok(Response::ok(req_id, ResultPayload::Bool(false))),
            };
            WriteOp::CompareAndSet {
                node_id,
                conditions,
                updates,
                reply,
            }
        }
        other => return Err(other),
    };

    // Lazily get/create this graph's writer (automatic per new graph/connector).
    let writer = match coalescer.writer_for(graph_name, core) {
        Some(w) => w,
        None => unreachable!("writer_for returned None while coalescer.enabled() was true"),
    };

    // Enqueue; on a full/closed queue (backpressure) apply this single op inline
    // under its own txn — same engine effect, just not batched — so a saturated
    // worker never drops or stalls a write.
    if let Err(op) = writer.try_enqueue(op) {
        writer.apply_one_inline(core, graph_name, op);
    }

    // Await the outcome (from the batch worker or the inline fallback) and rebuild
    // the exact Response the inline handler would have returned.
    let outcome = reply_rx.await.unwrap_or(WriteOutcome::WriterGone);
    let resp = match outcome {
        WriteOutcome::Ok => Response::ok(req_id, ResultPayload::String("ok".to_string())),
        WriteOutcome::Cas(b) => Response::ok(req_id, ResultPayload::Bool(b)),
        WriteOutcome::Err(e) => Response::err(req_id, e),
        WriteOutcome::WriterGone => Response::err(req_id, "write worker unavailable"),
    };
    Ok(resp)
}
