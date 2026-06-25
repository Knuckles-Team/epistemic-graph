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

        // ── Cost / efficiency (CONCEPT:KG-2.234, Lane V) ──────────────
        #[cfg(feature = "cost")]
        Method::ResourceStats => {
            let snapshot = crate::cost::collect_resource_stats(state).await;
            match serde_json::to_value(&snapshot) {
                Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
                Err(e) => Response::err(req.id, format!("ResourceStats serialization: {e}")),
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
                    // Authoritative durable graph registration (CONCEPT:KG-2.187):
                    // persist the graph's real name/type so a kill -9 before the next
                    // checkpoint still recovers it. Commit-before-ack: on failure the
                    // create is reported as an error (graph identity not durable).
                    let (authoritative, backend) = (s.redb_authoritative, s.persistence.clone());
                    drop(s);
                    if authoritative {
                        if let Some(p) = backend {
                            let fname = crate::persist::sanitize(&graph_name);
                            if let Err(e) = p.register_graph(&fname, &graph_name, graph_type).await
                            {
                                return Response::err(
                                    req.id,
                                    format!("durable graph registration failed: {e}"),
                                );
                            }
                        }
                    }
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
                    // In-memory teardown (CONCEPT:KG-2.237) — distinct from the durable
                    // purge below. The registry entry (the live GraphCore) is gone, but
                    // per-graph state keyed by NAME elsewhere in ServerState would
                    // survive and shadow a same-name recreate. Drop it so the recreate
                    // starts truly clean every cycle:
                    //  • the write-coalescer's cached writer — its worker owns an
                    //    `Arc<GraphCore>` of THIS (deleted) incarnation; left cached,
                    //    `writer_for` returns it on recreate (it is name-keyed and
                    //    ignores the new core) and routes the new tenant's writes into
                    //    the orphaned core — silently dropping them in RAM. THIS is the
                    //    tenant-churn corruption.
                    //  • the per-graph in-flight semaphore (no data, but bounds an
                    //    unbounded entry leak across many churn cycles).
                    s.write_coalescer.remove(graph_name);
                    s.per_graph_inflight.remove(graph_name);
                    // Authoritative durable purge (CONCEPT:KG-2.221): the registry
                    // entry is gone from RAM, but the graph's durable rows (nodes/
                    // edges/ledger/semantic/graph_meta, keyed by the sanitized name)
                    // must ALSO be removed — otherwise a recreate of the SAME name
                    // inherits the deleted incarnation's rows via the read-through-on-
                    // RAM-miss path and via `load_all`, silently dropping/corrupting
                    // the new tenant's writes. Commit-before-ack: await the purge so a
                    // same-name recreate after this ack starts from a clean slate, no
                    // race with the async writer. A purge failure is surfaced as an
                    // error (the delete is not durably complete).
                    let (authoritative, backend) = (s.redb_authoritative, s.persistence.clone());
                    drop(s);
                    if authoritative {
                        if let Some(p) = backend {
                            let fname = crate::persist::sanitize(graph_name);
                            if let Err(e) = p.purge_graph(&fname).await {
                                return Response::err(
                                    req.id,
                                    format!("durable graph purge failed: {e}"),
                                );
                            }
                        }
                    }
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
        | Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. }
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

        // ── Time-series (CONCEPT:KG-2.210/211 — native TSDB) ─────────
        // Stateful + self-routing: a Ts* op targets the SERIES store (keyed by
        // `series_id`), NOT a graph, so it is handled here (with `state`, which
        // holds the `SeriesStore`) BEFORE the graph-op path — never through
        // `dispatch_graph_op`'s graph registry/coalescer. Gated on `tsdb`: in a
        // slim build the arm is absent and these variants fall to the graph_ops
        // not-built catch-all (never a panic, never a mis-route).
        #[cfg(feature = "tsdb")]
        Method::TsAppend { .. }
        | Method::TsRange { .. }
        | Method::TsAsofJoin { .. }
        | Method::TsWindow { .. }
        | Method::TsGapFill { .. } => {
            match handlers::timeseries::try_handle(state, req.id, req.method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a ts method.
                Err(_) => Response::err(req.id, "timeseries dispatch routing error"),
            }
        }

        // ── Blob (CONCEPT:KG-2.206) ──────────────────────────────────
        // Content-addressed, NOT graph-scoped: a blob is keyed by digest and may be
        // referenced across graphs, so route at the top level (like txn) before the
        // per-graph chain. The variants only exist with the `blob` feature; without
        // it they aren't in the enum and a slim build can't reach this arm.
        #[cfg(feature = "blob")]
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobFetchBegin { .. }
        | Method::BlobChunkGet { .. }
        | Method::BlobFetchEnd { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => {
            match handlers::blob::try_handle(state, req.id, req.method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a blob method.
                Err(_) => Response::err(req.id, "blob dispatch routing error"),
            }
        }

        // ── Streaming / CDC / subscriptions (CONCEPT:KG-2.229/230) ───
        // The reactive READ + REGISTER surface over the CDC hub on `state` (the WRITE
        // side — emitting changes — lives in the dispatch_graph_op write-side-effect
        // block). These are NOT graph-mutating (CdcRead/Watch/FiredTriggers tail a
        // cursor; Register*/Drop* manage hub registrations), so they self-route here
        // BEFORE the per-graph chain, like tsdb/blob. Gated `streaming`: in a slim
        // build the arm is absent and the variants fall to the graph_ops not-built
        // catch-all (never a panic, never a mis-route).
        #[cfg(feature = "streaming")]
        Method::CdcRead { .. }
        | Method::RegisterContinuousQuery { .. }
        | Method::ReadContinuousQuery { .. }
        | Method::DropContinuousQuery { .. }
        | Method::Watch { .. }
        | Method::RegisterTrigger { .. }
        | Method::DropTrigger { .. }
        | Method::ListTriggers { .. }
        | Method::FiredTriggers { .. } => {
            match handlers::streaming::try_handle(state, req.id, req.method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a streaming method.
                Err(_) => Response::err(req.id, "streaming dispatch routing error"),
            }
        }

        // ── Distributed OWL reasoning (CONCEPT:KG-2.236) ─────────────
        // Cross-shard: reasons over the UNION of several graphs, so it self-routes
        // here (with `state` to gather each shard's snapshot) BEFORE the per-graph
        // chain — never through `dispatch_graph_op`, which targets a single `req.graph`.
        // Gated `owl`: in a build without it the variant isn't in the enum.
        #[cfg(feature = "owl")]
        Method::OwlReasonDistributed { .. } => {
            match handlers::rdf::try_handle_distributed(state, req.id, req.method).await {
                Ok(resp) => resp,
                // Unreachable: the only variant routed here is OwlReasonDistributed.
                Err(_) => Response::err(req.id, "owl distributed dispatch routing error"),
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
    // redb-authoritative mode (CONCEPT:KG-2.187): when ON, the durable mutation must
    // be COMMITTED to redb BEFORE we ack the client (commit-before-ack), and a
    // commit failure becomes an ERROR response. Read once under the same lock.
    let redb_authoritative = s.redb_authoritative;
    // Change-Data-Capture hub (CONCEPT:KG-2.229/230): clone the handle under the same
    // lock so a successful durable mutation can emit an ordered change into this
    // graph's feed AFTER it applies. `None` ⇒ a non-streaming build ⇒ no emit, the
    // write path is byte-for-byte unchanged.
    #[cfg(feature = "streaming")]
    let cdc = s.cdc.clone();
    // Per-graph write coalescer (CONCEPT:KG-2.182): clone the registry handle so the
    // hot single-op writes can be batched onto this graph's writer (lazily created,
    // keyed by name — automatic per new graph/connector), collapsing N concurrent
    // topology-lock acquisitions into one per batch. Cheap Arc clone under the lock.
    let write_coalescer = s.write_coalescer.clone();
    // In-engine Raft replication (CONCEPT:KG-2.188): if a cluster is active, capture
    // the handle + the graph's type so a durable mutation can be routed through
    // consensus. `None` ⇒ single-node ⇒ everything below is byte-for-byte unchanged.
    #[cfg(feature = "raft")]
    let raft = s.raft.clone();
    #[cfg(feature = "raft")]
    let graph_type = entry.graph_type;
    // Per-agent Row-Level Security (CONCEPT:KG-2.231): clone the isolation policy
    // under the same registry lock so the read-only query handler can filter its
    // off-lock snapshot down to the rows the caller may see. Only the read/query
    // surfaces need it (writes are already graph-ACL-gated above); cheap clone of a
    // small identity map, shared by Arc into the handler. `has_rules()==false` ⇒ the
    // filter is a no-op, single-tenant unchanged.
    #[cfg(feature = "security")]
    let rls = std::sync::Arc::new(s.isolation.clone());
    // Referenced by the read-query routing below only when a query/cypher/rdf surface
    // is compiled; keep it used in a security-but-no-query-surface build.
    #[cfg(all(
        feature = "security",
        not(any(feature = "query", feature = "cypher", feature = "rdf"))
    ))]
    let _ = &rls;
    drop(s); // Release registry lock before graph lock.

    // Tamper-evident audit verification (CONCEPT:KG-2.231): a read-only walk of the
    // target graph's durable hash-chained audit log. Routed to the redb backend's
    // owner thread (which flushes pending first). Handled here — AFTER the registry
    // lock is released — so blocking on the writer-thread reply never holds the lock.
    #[cfg(feature = "security")]
    if matches!(method, Method::AuditVerify) {
        let fname = crate::persist::sanitize(graph_name);
        return match persistence.as_ref().and_then(|p| p.as_redb()) {
            Some(redb) => match redb.audit_verify_blocking(&fname) {
                Ok(report) => Response::ok(req_id, ResultPayload::raw(&report)),
                Err(e) => Response::err(req_id, format!("AuditVerify error: {e}")),
            },
            None => Response::err(
                req_id,
                "AuditVerify requires a durable redb backend (no persist dir configured)"
                    .to_string(),
            ),
        };
    }

    // ── Raft write-routing barrier (CONCEPT:KG-2.188) ──────────────────────
    // When a cluster is active, a durable mutation goes through Raft consensus
    // (the leader's `client_write`) BEFORE it is applied+acked: the entry is
    // replicated to a quorum and then APPLIED on every node by the Raft state
    // machine (which runs the SAME apply path as below). So we replace the local
    // apply/record here with `client_write` and return its outcome — the in-memory
    // apply + M2 record happens inside `apply_to_state_machine`, not here. A
    // follower returns a ForwardToLeader error which we surface so the client
    // retries against the leader. This branch is the ONLY behavioral difference vs
    // single-node, and it is taken only for durable mutations with Raft active.
    #[cfg(feature = "raft")]
    if let Some(handle) = raft {
        if crate::wal::is_durable_mutation(&method) {
            let req = crate::raft::RaftRequest {
                graph_fname: crate::persist::sanitize(graph_name),
                graph_name: graph_name.to_string(),
                graph_type,
                method,
            };
            return match handle.client_write(req).await {
                Ok(_) => Response::ok(
                    req_id,
                    ResultPayload::Json(serde_json::json!({"replicated": true})),
                ),
                Err(e) => {
                    let leader = handle.current_leader().await;
                    Response::err(
                        req_id,
                        match leader {
                            Some(l) => format!("not leader; forward to node {l}: {e}"),
                            None => format!("raft write failed (no leader): {e}"),
                        },
                    )
                }
            };
        }
    }

    let record_method = match (&persistence, crate::wal::is_durable_mutation(&method)) {
        (Some(_), true) => Some(method.clone()),
        _ => None,
    };

    crate::metrics::graph_op(graph_name);

    // CDC pre-image (CONCEPT:KG-2.229): for a durable single-row mutation, capture the
    // affected node/edge's CURRENT property blob BEFORE the write applies, so the
    // emitted change carries an accurate `before`. Reads the core directly, so it is
    // correct for both the inline and the coalescer apply paths. No-op (Skip) for a
    // non-streaming build or a multi-row method (BatchUpdate/ClearGraph).
    #[cfg(feature = "streaming")]
    let cdc_pre = match (&cdc, crate::wal::is_durable_mutation(&method)) {
        (Some(_), true) => crate::server::cdc::capture_before(&core, &method),
        _ => crate::server::cdc::CdcPre::Skip,
    };
    // The method is consumed by the dispatch block below; keep its identity for the
    // post-emit (the emit only needs the variant + ids, all cloned by capture_before).
    #[cfg(feature = "streaming")]
    let cdc_method = if cdc.is_some() && crate::wal::is_durable_mutation(&method) {
        Some(method.clone())
    } else {
        None
    };

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
        // Read-only query surface — SQL (CONCEPT:KG-2.178, DataFusion behind
        // `query`) AND Cypher (CONCEPT:KG-2.179, dep-free behind `cypher`) AND GraphQL
        // (CONCEPT:KG-2.235, pure-Rust eg-graphql behind `graphql`): borrows the graph
        // core for an off-lock snapshot, runs on the blocking pool. Gated on ANY of the
        // three features so CypherQuery still routes in a cypher-only (no-DataFusion) Pi
        // build and GraphQl routes in a graphql build; the handler's per-method arm
        // falls through (Err) when ITS feature is off, so Sql/CypherQuery/GraphQl then
        // reach the graph_ops not-available catch-all. GraphQL — like SQL/Cypher/SPARQL
        // — runs UNDER the SAME RLS-aware result-cache compose (`caller`/`&rls` threaded
        // in, the cache key folds the caller's RLS context, the snapshot is RLS-filtered
        // to the caller) so a GraphQL read NEVER leaks across agents. Slim builds with
        // NONE of the three omit this line.
        #[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
        let method = match handlers::query::try_handle(
            req_id,
            core.clone(),
            method,
            #[cfg(feature = "security")]
            caller,
            #[cfg(feature = "security")]
            &rls,
        )
        .await
        {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Native RDF/SPARQL surface (CONCEPT:KG-2.217/218, features `rdf`/`sparql`):
        // AddTriples (durable — the shell below records it like any write),
        // GetRdf + Sparql (read-only, off-lock snapshot). Graph-scoped, so the
        // handler takes the graph core + name; AddTriples also reads the optional
        // lossless quad store off `state`. Gated on `rdf`; a method whose feature is
        // off falls through (Err) to the graph_ops not-available catch-all.
        #[cfg(feature = "rdf")]
        let method = match handlers::rdf::try_handle(
            state,
            req_id,
            graph_name,
            core.clone(),
            method,
            #[cfg(feature = "security")]
            caller,
            #[cfg(feature = "security")]
            &rls,
        )
        .await
        {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // WASM-sandboxed UDF surface (CONCEPT:KG-2.228, feature `wasm-udf`):
        // RegisterUdf compiles+caches, RunUdf runs sandboxed (fuel+memory+no host
        // caps) — both off-reactor. Process-global (not graph-scoped), so it takes
        // `state` for the UdfRegistry. A method whose feature is off falls through.
        #[cfg(feature = "wasm-udf")]
        let method = match handlers::wasm_udf::try_handle(state, req_id, method).await {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Query federation (CONCEPT:KG-2.232, feature `federation`):
        // RegisterForeignSource records a named foreign source on ServerState. The
        // `Op::ForeignScan` op itself runs through the unified-query handler above
        // (inline spec). Process-global, so it takes `state`. A method whose feature
        // is off falls through to the graph_ops not-available catch-all.
        #[cfg(feature = "federation")]
        let method = match handlers::federation::try_handle(state, req_id, method).await {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Distributed graph compute (CONCEPT:KG-2.227, feature `compute-dist`):
        // DistributedCompute + the matview lifecycle. Cross-shard, so it takes
        // `state` (it gathers each shard graph's snapshot from the registry).
        #[cfg(feature = "compute-dist")]
        let method = match handlers::dist_compute::try_handle(state, req_id, method).await {
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

    // Record the durable mutation after it SUCCEEDED in memory.
    //
    // Two regimes (CONCEPT:KG-2.177 / KG-2.187):
    //   * NOT authoritative (default): write-BEHIND. The abstracted backend remains
    //     the system-of-record; `record()` is fire-and-forget — serialize + hand to
    //     the off-reactor writer, no file I/O on this Tokio worker, no await. This is
    //     BYTE-FOR-BYTE today's behavior.
    //   * AUTHORITATIVE: commit-BEFORE-ACK. redb is the source of truth, so we AWAIT
    //     `record_durable` (which group-commits this op + fsyncs, coalescing with
    //     concurrent writers) before returning. If the durable commit FAILS, we did
    //     NOT land the write — convert the (in-memory-applied) success into an ERROR
    //     response rather than acking a write that isn't on disk. The in-RAM model is
    //     still ahead, but a checkpoint or eviction-gate keeps it readable, and the
    //     client correctly sees the write as not-durably-acknowledged so it can retry.
    if let (Some(m), Some(p)) = (record_method, persistence) {
        if response.error.is_none() {
            let fname = crate::persist::sanitize(graph_name);
            if redb_authoritative {
                if let Err(e) = p.record_durable(&fname, &m).await {
                    tracing::error!(
                        "redb authoritative: durable commit FAILED for graph '{}': {} — \
                         returning error (write not acked)",
                        graph_name,
                        e
                    );
                    return Response::err(
                        req_id,
                        format!("durable commit failed (write not acknowledged): {e}"),
                    );
                }
            } else {
                p.record(&fname, &m);
            }
        }
    }

    // Emit the CDC change (CONCEPT:KG-2.229/230) AFTER the write succeeded in memory
    // and (in authoritative mode) committed durably — the durable-fail path above
    // returns early, so reaching here means the change is real. The hub assigns the
    // per-graph seq, appends the ring, maintains continuous queries, fires triggers,
    // and wakes watchers. Off the returned Response (not in any handler), so both the
    // inline and coalescer apply paths feed the SAME feed.
    #[cfg(feature = "streaming")]
    if let (Some(hub), Some(m)) = (cdc, cdc_method) {
        if response.error.is_none() {
            crate::server::cdc::emit_for_method(&hub, &core, graph_name, &m, cdc_pre);
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

// ── Blob substrate dispatch round-trip (CONCEPT:KG-2.206) ─────────────────────
//
// Drives the Blob* methods through the SAME `dispatch` entrypoint a wire request
// hits (auth → routing → handler → CAS), proving streamed round-trip integrity +
// dedup + bounded memory + GC over the real protocol — not just the store unit.
#[cfg(all(test, feature = "blob"))]
mod blob_dispatch_tests {
    use super::*;
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request};
    use crate::registry::GraphRegistry;
    use crate::server::auth::compute_auth_token;
    use crate::server::blob::{BlobCursors, RedbChunkStore};
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "blob-test-secret";

    fn state_with_blob(dir: &str) -> Arc<RwLock<ServerState>> {
        let store = Arc::new(RedbChunkStore::open(dir).unwrap());
        Arc::new(RwLock::new(ServerState {
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir.to_string()),
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            blob: Some(Arc::new(BlobCursors::new(store))),
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        Request {
            id,
            graph: "__commons__".into(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: None,
            method,
        }
    }

    fn peak_rss_mb() -> u64 {
        let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                if let Some(kb) = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    return kb / 1024;
                }
            }
        }
        0
    }

    /// Upload `data` chunk-by-chunk via dispatch (never resident whole), commit,
    /// return the blob digest.
    async fn upload(
        state: &Arc<RwLock<ServerState>>,
        next_id: &mut u64,
        data: &[u8],
        chunk_size: usize,
    ) -> String {
        let begin = dispatch(
            state,
            req(
                *next_id,
                Method::BlobBegin {
                    chunk_size: chunk_size as u32,
                },
            ),
        )
        .await;
        *next_id += 1;
        let cursor = match begin.result {
            Some(ResultPayload::Count(c)) => c,
            other => panic!("BlobBegin: {:?} / {:?}", other, begin.error),
        };
        for part in data.chunks(chunk_size) {
            let r = dispatch(
                state,
                req(
                    *next_id,
                    Method::BlobChunkPut {
                        cursor,
                        data: part.to_vec(),
                    },
                ),
            )
            .await;
            *next_id += 1;
            assert!(r.error.is_none(), "BlobChunkPut: {:?}", r.error);
        }
        let commit = dispatch(state, req(*next_id, Method::BlobCommit { cursor })).await;
        *next_id += 1;
        match commit.result {
            Some(ResultPayload::String(d)) => d,
            other => panic!("BlobCommit: {:?} / {:?}", other, commit.error),
        }
    }

    /// Stream `digest` back down chunk-by-chunk via dispatch, reassemble.
    async fn download(
        state: &Arc<RwLock<ServerState>>,
        next_id: &mut u64,
        digest: &str,
    ) -> Vec<u8> {
        let begin = dispatch(
            state,
            req(
                *next_id,
                Method::BlobFetchBegin {
                    digest: digest.into(),
                },
            ),
        )
        .await;
        *next_id += 1;
        let (cursor, n): (u64, u32) = match begin.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("BlobFetchBegin: {:?} / {:?}", other, begin.error),
        };
        let mut out = Vec::new();
        for idx in 0..n {
            let r = dispatch(state, req(*next_id, Method::BlobChunkGet { cursor, idx })).await;
            *next_id += 1;
            match r.result {
                Some(ResultPayload::PropertiesMsgpack(bytes)) => out.extend(bytes),
                other => panic!("BlobChunkGet: {:?} / {:?}", other, r.error),
            }
        }
        let _ = dispatch(state, req(*next_id, Method::BlobFetchEnd { cursor })).await;
        *next_id += 1;
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn roundtrip_dedup_bounded_memory_and_gc() {
        let dir = std::env::temp_dir().join(format!("eg-blob-dispatch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = state_with_blob(&dir.to_string_lossy());
        let mut id = 1u64;

        // 16 MB blob streamed as 2 MiB chunks. NON-dedupable content (offset-seeded)
        // so real chunks are stored; the file is never held whole in this test
        // either — each chunk is generated, dispatched, then dropped.
        let chunk_size = 2 * 1024 * 1024usize;
        let n_chunks = 8u64;
        let mut full = Vec::new(); // only kept to verify the round-trip equals source
        {
            // Upload streaming: build+dispatch one chunk at a time.
            let begin = dispatch(
                &state,
                req(
                    id,
                    Method::BlobBegin {
                        chunk_size: chunk_size as u32,
                    },
                ),
            )
            .await;
            id += 1;
            let cursor = match begin.result {
                Some(ResultPayload::Count(c)) => c,
                o => panic!("begin {:?}", o),
            };
            for c in 0..n_chunks {
                let mut buf = vec![0u8; chunk_size];
                let mut x = (c + 1).wrapping_mul(0x9E3779B97F4A7C15) | 1;
                for b in buf.iter_mut() {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    *b = (x & 0xFF) as u8;
                }
                full.extend_from_slice(&buf);
                let r = dispatch(&state, req(id, Method::BlobChunkPut { cursor, data: buf })).await;
                id += 1;
                assert!(r.error.is_none());
            }
            let commit = dispatch(&state, req(id, Method::BlobCommit { cursor })).await;
            id += 1;
            let digest = match commit.result {
                Some(ResultPayload::String(d)) => d,
                o => panic!("commit {:?}", o),
            };

            // Round-trip integrity.
            let got = download(&state, &mut id, &digest).await;
            assert_eq!(got.len(), full.len());
            assert_eq!(got, full);

            // Bounded memory: the whole 16 MB blob was streamed through dispatch,
            // and peak RSS must stay well under buffering the whole object on both
            // sides. We keep ONE copy (`full`) for the integrity assert, so allow
            // total + a fixed floor; a regression that buffers the file in the
            // cursor/handler would blow past this.
            let total_mb = (n_chunks * chunk_size as u64) / (1024 * 1024);
            let peak = peak_rss_mb();
            assert!(
                peak < total_mb + 512,
                "peak RSS {peak}MB should stay bounded for a {total_mb}MB streamed blob"
            );

            // Reference the blob (a :Media node points at it).
            let r = dispatch(
                &state,
                req(
                    id,
                    Method::BlobRef {
                        digest: digest.clone(),
                    },
                ),
            )
            .await;
            id += 1;
            assert!(matches!(r.result, Some(ResultPayload::Count(1))));

            // Dedup: re-upload identical content → same digest, ZERO new chunks.
            let store = state.read().await.blob.as_ref().unwrap().store.clone();
            let chunks_before = store.chunk_count().unwrap();
            let digest2 = upload(&state, &mut id, &full, chunk_size).await;
            let chunks_after = store.chunk_count().unwrap();
            assert_eq!(digest, digest2, "identical content ⇒ identical digest");
            assert_eq!(chunks_before, chunks_after, "dedup: no new chunks");

            // GC keeps a referenced blob, reclaims an unreferenced one. digest is
            // referenced (count 1); digest2 == digest so still 1 reference total.
            let gc = dispatch(&state, req(id, Method::BlobGc)).await;
            id += 1;
            let (blobs, _chunks): (u64, u64) = match gc.result {
                Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
                o => panic!("gc {:?}", o),
            };
            assert_eq!(blobs, 0, "referenced blob is kept");
            // Still fetchable after GC.
            assert_eq!(download(&state, &mut id, &digest).await, full);

            // Drop the reference → GC reclaims the blob + all its chunks.
            let r = dispatch(
                &state,
                req(
                    id,
                    Method::BlobUnref {
                        digest: digest.clone(),
                    },
                ),
            )
            .await;
            id += 1;
            assert!(matches!(r.result, Some(ResultPayload::Count(0))));
            let gc = dispatch(&state, req(id, Method::BlobGc)).await;
            id += 1;
            let (blobs, chunks): (u64, u64) = match gc.result {
                Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
                o => panic!("gc {:?}", o),
            };
            assert_eq!(blobs, 1, "unreferenced blob reclaimed");
            assert_eq!(chunks, n_chunks, "all its orphan chunks reclaimed");
            assert_eq!(store.chunk_count().unwrap(), 0);
            // Fetching a reclaimed blob now fails.
            let r = dispatch(&state, req(id, Method::BlobFetchBegin { digest })).await;
            assert!(r.error.is_some());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
