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
use super::super::mutation::{self, GatewayAuthzCtx, MutationCtx, MutationPlan};
use super::super::persistence::PersistenceBackend;
use super::super::state::{max_response_nodes, ServerState, MAX_BATCH_IDS};
use crate::graph::GraphCore;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};

/// Resolve a cross-graph union read's graph set to their cores (CONCEPT:EG-KG.query.cross-graph-union).
///
/// Access-checks each graph as Read, clones the `Arc<GraphCore>`s, and holds only
/// the registry read lock for the resolution — the per-graph topology locks are
/// NOT taken here; the caller reads each core sequentially after this returns, so
/// two graph locks are never held at once (the cross-graph deadlock discipline).
/// Missing graphs are skipped (a lane graph may not exist yet); one denied graph
/// fails the whole union.
async fn resolve_union_cores(
    state: &Arc<RwLock<ServerState>>,
    caller: Option<&str>,
    graphs: &[String],
) -> Result<Vec<Arc<GraphCore>>, String> {
    let s = state.read().await;
    let mut cores = Vec::with_capacity(graphs.len());
    for name in graphs {
        let entry = match s.registry.get(name) {
            Some(e) => e,
            None => continue,
        };
        check_graph_access(
            &s.isolation,
            caller,
            name,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Read,
        )?;
        cores.push(entry.core.clone());
    }
    Ok(cores)
}

/// Intelligent overload backstop for the `GetNodes` full-graph dump
/// (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation). Given the graph's node `count` and the configured `cap`,
/// returns `Some(error_message)` when the dump would exceed the cap (so the
/// handler can refuse with a typed `RESULT_TOO_LARGE` error instead of building
/// a gigabyte-scale frame that resets the client connection), or `None` when the
/// dump is within bounds and safe to materialize. A `cap` of `0` disables the
/// guard entirely (the unbounded legacy behavior, for an operator who opts in via
/// `EPISTEMIC_GRAPH_MAX_RESPONSE_NODES=0`). Pure + side-effect-free so the
/// threshold logic is unit-tested directly, independent of process-global env.
fn oversize_dump_error(count: usize, cap: usize) -> Option<String> {
    if cap != 0 && count > cap {
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

/// Decode a MessagePack-encoded JSON object blob (the `props_msgpack` /
/// `semantic_props_msgpack` wire fields) into a `serde_json` object map
/// (CONCEPT:EG-KG.memory.eg-batch-decay-caller). A missing/undecodable/non-object blob yields an empty map, so
/// a caller may omit props entirely — the eg-core primitive injects the structural
/// markers regardless. Mirrors the `CompareAndSetNodeFields` blob-decode discipline.
fn decode_json_object(blob: &[u8]) -> serde_json::Map<String, serde_json::Value> {
    match rmp_serde::from_slice::<serde_json::Value>(blob) {
        Ok(serde_json::Value::Object(o)) => o,
        _ => serde_json::Map::new(),
    }
}

/// Read a node's human-readable `(name, description, type)` triple from its
/// MessagePack property blob (CONCEPT:EG-KG.retrieval.one-round-trip-discovery), used to hydrate `Discover` hits.
/// `name` falls back to the node id, `type` falls back to a `node_type` field, and
/// a missing/undecodable blob yields `(id, "", "")` — so the op never fails on a
/// text-less node, it just returns what it can.
fn node_text(core: &GraphCore, node_id: &str) -> (String, String, String) {
    let Some(blob) = core.get_node_properties(node_id) else {
        return (node_id.to_string(), String::new(), String::new());
    };
    let obj = decode_json_object(&blob);
    let get = |k: &str| {
        obj.get(k)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    let name = {
        let n = get("name");
        if n.is_empty() {
            node_id.to_string()
        } else {
            n
        }
    };
    let ntype = {
        let t = get("type");
        if t.is_empty() {
            get("node_type")
        } else {
            t
        }
    };
    (name, get("description"), ntype)
}

/// How many candidates to over-fetch from the HNSW index per requested result, so
/// the keyword re-rank has room to promote a lexically-strong hit above a slightly
/// closer pure-vector neighbour.
const DISCOVER_FANOUT: usize = 4;
/// Hard cap on the embedding-absent keyword-only fallback scan, so a degraded
/// (no-embedder) `Discover` can never turn into an unbounded full-graph walk.
const DISCOVER_LEX_SCAN_CAP: usize = 4096;
/// Blend weights when BOTH signals are present (they sum to 1.0). Semantic gets the
/// larger share (dense retrieval is the primary recall signal); the keyword overlap
/// is a lexical re-rank boost.
const DISCOVER_SEM_WEIGHT: f32 = 0.6;
const DISCOVER_KW_WEIGHT: f32 = 0.4;

/// Fraction of the distinct query keywords that appear (as a substring, case-insensitively)
/// anywhere in a candidate's `name`/`description`/`type` — a `[0.0, 1.0]` lexical score.
fn keyword_overlap(kws: &[String], name: &str, description: &str, ntype: &str) -> f32 {
    if kws.is_empty() {
        return 0.0;
    }
    let haystack = format!(
        "{} {} {}",
        name.to_lowercase(),
        description.to_lowercase(),
        ntype.to_lowercase()
    );
    let matched = kws.iter().filter(|kw| haystack.contains(*kw)).count();
    matched as f32 / kws.len() as f32
}

/// One-round-trip hybrid discovery (CONCEPT:EG-KG.retrieval.one-round-trip-discovery).
///
/// Ranks nodes by BOTH lexical keyword overlap (over `name`/`description`/`type`)
/// AND semantic similarity to `query_embedding`, returning the top-`k` hydrated
/// with their human-readable text as `[{id,name,description,type,score}, …]`.
/// Complements [`Method::SemanticSearch`], which returns bare `(id, score)` and
/// leaves keyword matching + text hydration to the caller.
///
/// Candidate generation reuses the HNSW batch primitive
/// ([`SemanticStore::semantic_search`]) — over-fetched by [`DISCOVER_FANOUT`] so
/// the keyword re-rank has headroom — rather than an O(N) scan. Only when
/// `query_embedding` is empty (embedder/vLLM degraded) does it fall back to a
/// keyword-only scan, and even then bounded by [`DISCOVER_LEX_SCAN_CAP`].
///
/// Scoring (all reads are cheap in-memory, so this runs inline):
/// * both signals → `DISCOVER_SEM_WEIGHT · sim + DISCOVER_KW_WEIGHT · kw`;
/// * embedding only → `sim`;
/// * keywords only → `kw`.
fn discover(
    core: &GraphCore,
    keywords: &[String],
    query_embedding: &[f32],
    k: usize,
    req_id: u64,
) -> Response {
    // De-duplicate + lowercase the keyword set (order-independent).
    let kws: Vec<String> = {
        let mut seen = std::collections::BTreeSet::new();
        keywords
            .iter()
            .map(|w| w.to_lowercase())
            .filter(|w| !w.is_empty() && seen.insert(w.clone()))
            .collect()
    };
    let has_kw = !kws.is_empty();
    let has_emb = !query_embedding.is_empty();

    // Per-candidate score components, keyed by node id.
    struct Hit {
        sim: f32,
        kw: f32,
    }
    let mut hits: std::collections::HashMap<String, Hit> = std::collections::HashMap::new();

    // 1. Dense candidates via the HNSW batch primitive (over-fetched for re-rank).
    if has_emb {
        let fanout = k.max(1).saturating_mul(DISCOVER_FANOUT).max(k);
        let raw = core
            .semantic_store
            .read()
            .semantic_search(query_embedding, fanout);
        for (id, sim) in raw {
            let kw = if has_kw {
                let (name, desc, ntype) = node_text(core, &id);
                keyword_overlap(&kws, &name, &desc, &ntype)
            } else {
                0.0
            };
            hits.insert(id, Hit { sim, kw });
        }
    } else if has_kw {
        // 2. Embedding-absent fallback: bounded keyword-only scan (documented
        //    degraded path — no HNSW batch primitive is usable without a vector).
        for id in core.node_ids().into_iter().take(DISCOVER_LEX_SCAN_CAP) {
            let (name, desc, ntype) = node_text(core, &id);
            let kw = keyword_overlap(&kws, &name, &desc, &ntype);
            if kw > 0.0 {
                hits.insert(id, Hit { sim: 0.0, kw });
            }
        }
    }

    // Combine, sort, take top-k, hydrate text.
    let mut ranked: Vec<(String, f32)> = hits
        .into_iter()
        .map(|(id, h)| {
            let score = if has_emb && has_kw {
                DISCOVER_SEM_WEIGHT * h.sim.clamp(0.0, 1.0) + DISCOVER_KW_WEIGHT * h.kw
            } else if has_emb {
                h.sim.clamp(0.0, 1.0)
            } else {
                h.kw
            };
            (id, score)
        })
        .collect();
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    ranked.truncate(k);

    let results: Vec<serde_json::Value> = ranked
        .into_iter()
        .map(|(id, score)| {
            let (name, description, ntype) = node_text(core, &id);
            serde_json::json!({
                "id": id,
                "name": name,
                "description": description,
                "type": ntype,
                "score": score,
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::Value::Array(results)),
    )
}

/// Decode a MessagePack-encoded `{translation,rotation,scale}` JSON blob into an
/// eg-core [`eg_core::scene::Pose`] (CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087). `None` only if the blob
/// is not a decodable JSON object or a present sub-object is malformed (a bare `{}`
/// reads back as the identity pose, since translation/rotation default to identity
/// and scale to unit). Keeps the `eg-types` wire crate free of the eg-core scene
/// dependency — the Pose lives only handler-side.
fn decode_pose(blob: &[u8]) -> Option<eg_core::scene::Pose> {
    let val = rmp_serde::from_slice::<serde_json::Value>(blob).ok()?;
    eg_core::scene::Pose::from_json(&val)
}

/// Route a [`mutation::GATEWAY_ROUTED`] method through the single commit gateway
/// (CONCEPT:EG-P0-2). Called from `dispatch_graph_op` AHEAD of both the write-
/// coalescer and the legacy per-method arms below, so a routed method NEVER falls
/// through to `g.add_node(...)` etc. directly — the only path left for it is this
/// one, which builds a [`MutationPlan`] straight from `eg_capabilities::policy` and
/// calls [`mutation::commit_mutation`]. A method NOT in the routed set is handed
/// straight back (`Err(method)`), unchanged, exactly like every other domain
/// router in `dispatch.rs`'s routing chain.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn try_handle_gateway(
    req_id: u64,
    caller: Option<&str>,
    graph_name: &str,
    core: &Arc<GraphCore>,
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    redb_authoritative: bool,
    #[cfg(feature = "streaming")] cdc: Option<&Arc<crate::server::cdc::CdcHub>>,
    write_coalescer: Option<&Arc<crate::write_coalescer::WriteCoalescerRegistry>>,
    authz_ctx: Option<&GatewayAuthzCtx>,
    method: Method,
) -> Result<Response, Method> {
    if !mutation::is_gateway_routed(&method) {
        return Err(method);
    }
    let (isolation, graph_type, owner) = authz_ctx.expect(
        "dispatch_graph_op must capture a GatewayAuthzCtx for every mutation::is_gateway_routed method",
    );
    let plan = MutationPlan::for_method(&method);
    let ctx = MutationCtx {
        req_id,
        caller,
        graph_name,
        graph_type: *graph_type,
        owner: owner.as_deref(),
        isolation,
        core,
        persistence,
        redb_authoritative,
        #[cfg(feature = "streaming")]
        cdc,
        write_coalescer,
    };
    let resp = match &method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            let (node_id, properties_msgpack) = (node_id.clone(), properties_msgpack.clone());
            mutation::commit_mutation(&ctx, &plan, &method, move |core| {
                core.add_node(node_id, properties_msgpack);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::RemoveNode { node_id } => {
            let node_id = node_id.clone();
            mutation::commit_mutation(&ctx, &plan, &method, move |core| {
                core.remove_node(node_id);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let (source_id, target_id, properties_msgpack) = (
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
            );
            mutation::commit_mutation(&ctx, &plan, &method, move |core| {
                core.add_edge(source_id, target_id, properties_msgpack)
                    .map(|()| ResultPayload::String("ok".to_string()))
                    .map_err(|e| e.to_string())
            })
            .await
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            let (source_id, target_id) = (source_id.clone(), target_id.clone());
            mutation::commit_mutation(&ctx, &plan, &method, move |core| {
                core.remove_edge(source_id, target_id);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::CreateSummaryNode {
            level,
            child_ids,
            props_msgpack,
        } => {
            let (level, child_ids, props_msgpack) =
                (*level, child_ids.clone(), props_msgpack.clone());
            mutation::commit_mutation(&ctx, &plan, &method, move |core| {
                let props = decode_json_object(&props_msgpack);
                let id = core.create_summary_node(level, &child_ids, props);
                Ok(ResultPayload::String(id))
            })
            .await
        }
        Method::Consolidate {
            episodic_ids,
            semantic_props_msgpack,
        } => {
            let (episodic_ids, semantic_props_msgpack) =
                (episodic_ids.clone(), semantic_props_msgpack.clone());
            mutation::commit_mutation(&ctx, &plan, &method, move |core| {
                let props = decode_json_object(&semantic_props_msgpack);
                let id = core.consolidate(&episodic_ids, props);
                Ok(ResultPayload::String(id))
            })
            .await
        }
        Method::Reinforce {
            node_id,
            now_ms,
            weight,
        } => {
            let (node_id, now_ms, weight) = (node_id.clone(), *now_ms, *weight);
            mutation::commit_mutation(&ctx, &plan, &method, move |core| {
                let existed = core.reinforce(&node_id, now_ms, weight);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        _ => unreachable!(
            "mutation::is_gateway_routed guarantees method is one of the routed variants"
        ),
    };
    Ok(resp)
}

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
        // AddNode/RemoveNode (CONCEPT:EG-P0-2 bypass guard): these — along with
        // AddEdge/RemoveEdge/CreateSummaryNode/Consolidate/Reinforce below — are
        // GATEWAY_ROUTED. `dispatch_graph_op` calls `try_handle_gateway` BEFORE
        // this terminal handler, so a routed method is intercepted and returns
        // through `commit_mutation` long before reaching this `match` — this
        // arm is structurally unreachable, not merely undocumented. Kept as an
        // explicit `unreachable!()` (rather than deleting the arm and falling
        // into the wildcard read-only-methods-only assumption below) so a
        // regression in the dispatch-side routing — e.g. someone re-adding a
        // direct call path that skips `try_handle_gateway` — fails LOUDLY here
        // instead of silently re-mutating `eg-core` outside the gateway.
        Method::AddNode { .. } => unreachable!(
            "AddNode is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::RemoveNode { .. } => unreachable!(
            "RemoveNode is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::HasNode { node_id } => {
            let g = &*core;
            Response::ok(req_id, ResultPayload::Bool(g.has_node(&node_id)))
        }
        Method::GetNodes => {
            let g = &*core;
            // Intelligent overload backstop (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation): a `GetNodes` is an
            // UNBOUNDED full-graph dump. On a large graph (e.g. `__commons__` with
            // 166K+ nodes carrying 1024-dim embeddings) materializing every node's
            // properties into ONE response frame is a gigabyte-scale payload that
            // overruns/resets the client connection. Check the cheap topology count
            // BEFORE building the Vec, and return a typed, catchable error instead of
            // the pathological frame. `cap == 0` disables the guard. The bounded
            // reads (`GetNodesByLabel`, per-id) are intentionally unaffected.
            if let Some(msg) = oversize_dump_error(g.node_count(), max_response_nodes()) {
                return Response::err(req_id, msg);
            }
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
        Method::GetNodesByLabel { label, limit } => {
            let g = &*core;
            let nodes: Vec<(String, serde_json::Value)> = g
                .get_nodes_by_label(&label, limit)
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
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            // Decode the two msgpack blobs to JSON objects. A decode failure is a
            // CAS failure (false), not a transport error — the node is untouched.
            let conditions = match rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                &conditions_msgpack,
            ) {
                Ok(m) => m,
                Err(_) => return Response::ok(req_id, ResultPayload::Bool(false)),
            };
            let updates = match rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                &updates_msgpack,
            ) {
                Ok(m) => m,
                Err(_) => return Response::ok(req_id, ResultPayload::Bool(false)),
            };
            let g = &*core;
            let ok = g.compare_and_set_fields(&node_id, &conditions, &updates);
            Response::ok(req_id, ResultPayload::Bool(ok))
        }
        Method::ClaimNext {
            label,
            updates_msgpack,
        } => {
            // CONCEPT:EG-KG.compute.atomically-claim-oldest-pending — atomically claim the oldest pending node of `label`.
            // A decode failure ⇒ nothing claimed (Raw None), never a transport error.
            let updates = match rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(
                &updates_msgpack,
            ) {
                Ok(m) => m,
                Err(_) => {
                    return Response::ok(
                        req_id,
                        ResultPayload::raw(&Option::<(String, serde_json::Value)>::None),
                    )
                }
            };
            let g = &*core;
            let claimed = g.claim_next_fields(&label, &updates);
            Response::ok(req_id, ResultPayload::raw(&claimed))
        }
        // ── Message broker admin + data (CONCEPT:EG-KG.compute.message-broker-exchanges) ─────────────────
        // Built on the KG-2.303 queue: exchanges/bindings are nodes on this target
        // graph; publish routes + enqueues; consume/ack REUSE ClaimNext + CAS above.
        // Same handler home + precedent as ClaimNext. Gated `broker`; a slim build
        // drops the variants (they fall to the catch-all "not available").
        #[cfg(feature = "broker")]
        Method::DeclareExchange { exchange, kind } => {
            let Some(k) = crate::broker::ExchangeKind::parse(&kind) else {
                return Response::err(
                    req_id,
                    format!("unknown exchange kind '{kind}' (want direct/topic/fanout)"),
                );
            };
            match crate::broker::declare_exchange(&core, &exchange, k) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e),
            }
        }
        #[cfg(feature = "broker")]
        Method::DeleteExchange { exchange } => {
            let existed = crate::broker::delete_exchange(&core, &exchange);
            Response::ok(req_id, ResultPayload::Bool(existed))
        }
        #[cfg(feature = "broker")]
        Method::BindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            crate::broker::bind_queue(&core, &exchange, &queue, &routing_key);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        #[cfg(feature = "broker")]
        Method::UnbindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            let existed = crate::broker::unbind_queue(&core, &exchange, &queue, &routing_key);
            Response::ok(req_id, ResultPayload::Bool(existed))
        }
        #[cfg(feature = "broker")]
        Method::Publish {
            exchange,
            routing_key,
            payload,
        } => {
            // Deterministic (routes over current bindings + monotonic seq); the same
            // Method replays identically from the WAL (see `wal::apply`).
            let delivered = crate::broker::publish(&core, &exchange, &routing_key, &payload);
            Response::ok(req_id, ResultPayload::Count(delivered as u64))
        }
        // ── Broker policy extensions (CONCEPT:EG-KG.compute.dead-letter-queues..280) ───────────────
        // Same handler home + durable/deterministic contract as EG-275: each mutates
        // control-graph nodes from explicit args (caller-supplied `now_ms`), so
        // `wal::apply` replays them identically. Consume/ack/reject reuse the CAS +
        // claim primitives internally.
        #[cfg(feature = "broker")]
        Method::DeclareQueue {
            queue,
            dl_exchange,
            dl_routing_key,
            max_delivery_count,
            message_ttl_ms,
            queue_expiry_ms,
            max_priority,
        } => {
            let policy = crate::broker::QueuePolicy {
                dl_exchange,
                dl_routing_key,
                max_delivery_count,
                message_ttl_ms,
                queue_expiry_ms,
                max_priority,
            };
            crate::broker::declare_queue(&core, &queue, &policy);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        #[cfg(feature = "broker")]
        Method::PublishEx {
            exchange,
            routing_key,
            payload,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let delivered = crate::broker::publish_ex(
                &core,
                &exchange,
                &routing_key,
                &payload,
                priority,
                delay_ms,
                ttl_ms,
                now_ms,
            );
            Response::ok(req_id, ResultPayload::Count(delivered as u64))
        }
        #[cfg(feature = "broker")]
        Method::BrokerConsume {
            queue,
            group,
            consumer,
            now_ms,
            lease_ms,
            prefetch,
        } => {
            let claimed = crate::broker::broker_consume(
                &core, &queue, &group, &consumer, now_ms, lease_ms, prefetch,
            );
            Response::ok(req_id, ResultPayload::raw(&claimed))
        }
        #[cfg(feature = "broker")]
        Method::BrokerAck { queue, node_id } => {
            let existed = crate::broker::broker_ack(&core, &queue, &node_id);
            Response::ok(req_id, ResultPayload::Bool(existed))
        }
        #[cfg(feature = "broker")]
        Method::BrokerReject {
            queue,
            node_id,
            requeue,
            now_ms,
        } => {
            let outcome = crate::broker::broker_reject(&core, &queue, &node_id, requeue, now_ms);
            Response::ok(req_id, ResultPayload::String(outcome))
        }
        #[cfg(feature = "broker")]
        Method::SweepExpired { now_ms } => {
            let acted = crate::broker::sweep_expired(&core, now_ms);
            Response::ok(req_id, ResultPayload::Count(acted as u64))
        }
        // ── Replayable append-log streams (CONCEPT:EG-KG.compute.replayable-append-log) ───────────────
        // Same handler home + durable/deterministic contract as EG-275/276..280: each
        // mutation writes control-graph nodes from explicit args (caller `now_ms` +
        // durable counters), so `wal::apply` replays them identically. Reads are pure.
        #[cfg(feature = "broker")]
        Method::StreamDeclare {
            stream,
            max_messages,
            max_age_ms,
        } => {
            let retention = crate::broker::StreamRetention {
                max_messages,
                max_age_ms,
            };
            crate::broker::declare_stream(&core, &stream, &retention);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        #[cfg(feature = "broker")]
        Method::StreamPublish {
            stream,
            payload,
            now_ms,
        } => {
            let offset = crate::broker::stream_publish(&core, &stream, &payload, now_ms);
            Response::ok(req_id, ResultPayload::Count(offset as u64))
        }
        #[cfg(feature = "broker")]
        Method::StreamRead {
            stream,
            from_offset,
            max,
        } => {
            let from = crate::broker::ReadFrom::from_wire(from_offset);
            let msgs = crate::broker::stream_read(&core, &stream, from, max as usize);
            Response::ok(req_id, ResultPayload::raw(&msgs))
        }
        #[cfg(feature = "broker")]
        Method::StreamTrim { stream, now_ms } => {
            let dropped = crate::broker::stream_trim(&core, &stream, now_ms);
            Response::ok(req_id, ResultPayload::Count(dropped as u64))
        }
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset {
            stream,
            group,
            offset,
        } => {
            crate::broker::commit_offset(&core, &stream, &group, offset);
            Response::ok(req_id, ResultPayload::String("ok".to_string()))
        }
        #[cfg(feature = "broker")]
        Method::StreamCommittedOffset { stream, group } => {
            let committed = crate::broker::committed_offset(&core, &stream, &group);
            Response::ok(req_id, ResultPayload::raw(&committed))
        }
        // ── Publisher confirms + consumer QoS acks (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos) ──────
        #[cfg(feature = "broker")]
        Method::PublishConfirmed {
            exchange,
            routing_key,
            payload,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let token = crate::broker::publish_confirmed(
                &core,
                &exchange,
                &routing_key,
                &payload,
                priority,
                delay_ms,
                ttl_ms,
                now_ms,
            );
            Response::ok(req_id, ResultPayload::raw(&token))
        }
        // ── Idempotent producer / effectively-once (CONCEPT:EG-KG.ingest.broker-reject-publish) ──────
        #[cfg(feature = "broker")]
        Method::PublishIdempotent {
            exchange,
            routing_key,
            payload,
            producer_id,
            seq,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let result = crate::broker::publish_idempotent(
                &core,
                &exchange,
                &routing_key,
                &payload,
                producer_id.as_deref(),
                seq,
                priority,
                delay_ms,
                ttl_ms,
                now_ms,
            );
            Response::ok(req_id, ResultPayload::raw(&result))
        }
        #[cfg(feature = "broker")]
        Method::BrokerAckTag { delivery_tag } => {
            let existed = crate::broker::broker_ack_tag(&core, delivery_tag);
            Response::ok(req_id, ResultPayload::Bool(existed))
        }
        #[cfg(feature = "broker")]
        Method::BrokerNackTag {
            delivery_tag,
            requeue,
            now_ms,
        } => {
            let outcome = crate::broker::broker_nack_tag(&core, delivery_tag, requeue, now_ms);
            Response::ok(req_id, ResultPayload::String(outcome))
        }
        // ── Agent-memory / scene-graph / trajectory wire ops (CONCEPT:EG-KG.memory.eg-batch-decay-caller) ────
        // Route each Method to its eg-core `GraphCore` primitive. The mutating arms
        // share the SAME durable/deterministic contract as the broker precedent: the
        // dispatch shell records them (via `is_durable_mutation`) and `wal::apply`
        // re-runs the SAME primitive over the same pre-image, and every generated id
        // derives deterministically from sorted inputs / node-count / step ordinals,
        // so a replayed WAL record reproduces byte-identical state. Reads are pure.
        // CreateSummaryNode/Consolidate/Reinforce (CONCEPT:EG-P0-2 bypass guard):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above for why these
        // are structurally unreachable here, not merely undocumented.
        Method::CreateSummaryNode { .. } => unreachable!(
            "CreateSummaryNode is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::Consolidate { .. } => unreachable!(
            "Consolidate is mutation::GATEWAY_ROUTED; dispatch_graph_op must route \
             it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::Reinforce { .. } => unreachable!(
            "Reinforce is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::DecayNode {
            node_id,
            now_ms,
            half_life_ms,
        } => {
            let acted = core.decay_node(&node_id, now_ms, half_life_ms);
            Response::ok(req_id, ResultPayload::Bool(acted))
        }
        Method::DecayMemories {
            now_ms,
            half_life_ms,
            ids,
        } => {
            let n = core.decay_memories(now_ms, half_life_ms, &ids);
            Response::ok(req_id, ResultPayload::Count(n as u64))
        }
        Method::EvictBelow {
            ids,
            threshold,
            delete,
        } => {
            let pruned = core.evict_below(&ids, threshold, delete);
            Response::ok(req_id, ResultPayload::Ids(pruned))
        }
        Method::Maintain {
            ids,
            now_ms,
            half_life_ms,
            evict_threshold,
            delete,
        } => {
            let out = core.maintain(&ids, now_ms, half_life_ms, evict_threshold, delete);
            // (decayed_count, pruned_ids) — compact msgpack tuple.
            Response::ok(req_id, ResultPayload::raw(&out))
        }
        Method::SummaryChildren { node_id } => {
            Response::ok(req_id, ResultPayload::Ids(core.summary_children(&node_id)))
        }
        Method::SummariesAtLevel { level } => {
            Response::ok(req_id, ResultPayload::Ids(core.summaries_at_level(level)))
        }
        Method::AddSceneObject {
            pose_msgpack,
            parent,
        } => {
            let Some(pose) = decode_pose(&pose_msgpack) else {
                return Response::err(req_id, "AddSceneObject: undecodable pose_msgpack");
            };
            let id = core.add_scene_object(&pose, parent.as_deref());
            Response::ok(req_id, ResultPayload::String(id))
        }
        Method::SetPose {
            node_id,
            pose_msgpack,
        } => {
            let Some(pose) = decode_pose(&pose_msgpack) else {
                return Response::err(req_id, "SetPose: undecodable pose_msgpack");
            };
            let ok = core.set_pose(&node_id, &pose);
            Response::ok(req_id, ResultPayload::Bool(ok))
        }
        Method::Reparent {
            node_id,
            new_parent,
        } => {
            let ok = core.reparent(&node_id, new_parent.as_deref());
            Response::ok(req_id, ResultPayload::Bool(ok))
        }
        Method::WorldTransform { node_id } => {
            let payload = match core.world_transform(&node_id) {
                Some(pose) => ResultPayload::Json(pose.to_json()),
                None => ResultPayload::Json(serde_json::Value::Null),
            };
            Response::ok(req_id, payload)
        }
        Method::SceneChildren { node_id } => {
            Response::ok(req_id, ResultPayload::Ids(core.scene_children(&node_id)))
        }
        Method::StartTrajectory { props_msgpack } => {
            let props = decode_json_object(&props_msgpack);
            let id = core.start_trajectory(props);
            Response::ok(req_id, ResultPayload::String(id))
        }
        Method::AppendStep {
            traj_id,
            action_msgpack,
            reward,
            state_ref,
            next_state_ref,
            t,
        } => {
            let action = rmp_serde::from_slice::<serde_json::Value>(&action_msgpack)
                .unwrap_or(serde_json::Value::Null);
            let step_id = core.append_step(
                &traj_id,
                action,
                reward,
                state_ref.as_deref(),
                next_state_ref.as_deref(),
                t,
            );
            // Option<String> — nil ⇒ the trajectory was absent (no partial write).
            Response::ok(req_id, ResultPayload::raw(&step_id))
        }
        Method::DiscountedReturn { traj_id, gamma } => Response::ok(
            req_id,
            ResultPayload::Float(core.discounted_return(&traj_id, gamma)),
        ),
        Method::BestTrajectory { traj_ids, gamma } => Response::ok(
            req_id,
            ResultPayload::raw(&core.best_trajectory(&traj_ids, gamma)),
        ),
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
        Method::MatchOntologyTerms { query } => {
            // CONCEPT:EG-ORCH.routing.lexical-capability-escalation — lexical capability gate; cached aho-corasick scan.
            let g = &*core;
            Response::ok(req_id, ResultPayload::raw(&g.match_ontology_terms(&query)))
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
            // CONCEPT:EG-KG.txn.per-graph-write-isolation / Phase C-D — the HNSW index is now maintained
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
        Method::Discover {
            keywords,
            query_embedding,
            k,
        } => discover(&core, &keywords, &query_embedding, k, req_id),
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
        // CONCEPT:EG-KG.compute.l2-normalize-batch-vectors — kernel-backed in-engine batch L2-normalize (compute-near-data).
        // The `numeric` feature links the pure eg-numeric kernel (faer/ndarray, no Python-extension FFI);
        // a no-numeric build (e.g. `pi`) has no eg-numeric, so the op reports it's absent.
        Method::BatchL2Normalize { vectors } => {
            #[cfg(feature = "numeric")]
            {
                let out = eg_numeric::linalg::batch_l2_normalize(&vectors);
                Response::ok(req_id, ResultPayload::raw(&out))
            }
            #[cfg(not(feature = "numeric"))]
            {
                let _ = vectors;
                Response::err(
                    req_id,
                    "BatchL2Normalize requires the `numeric` feature (eg-numeric kernel)."
                        .to_string(),
                )
            }
        }
        // AddEdge/RemoveEdge (CONCEPT:EG-P0-2 bypass guard): GATEWAY_ROUTED — see
        // the AddNode/RemoveNode comment above for why these are structurally
        // unreachable here, not merely undocumented.
        Method::AddEdge { .. } => unreachable!(
            "AddEdge is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::RemoveEdge { .. } => unreachable!(
            "RemoveEdge is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::InvalidateEdge {
            source_id,
            target_id,
            relationship,
            invalid_at,
            tx_now,
        } => {
            let g = &*core;
            let n = g.invalidate_edge(&source_id, &target_id, &relationship, invalid_at, tx_now);
            Response::ok(req_id, ResultPayload::Count(n as u64))
        }
        Method::SupersedeEdge {
            source_id,
            target_id,
            properties_msgpack,
            prior_source,
            prior_target,
            prior_relationship,
            valid_at,
            tx_now,
        } => {
            let g = &*core;
            match g.supersede_edge(
                source_id,
                target_id,
                properties_msgpack,
                &prior_source,
                &prior_target,
                &prior_relationship,
                valid_at,
                tx_now,
            ) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("ok".to_string())),
                Err(e) => Response::err(req_id, e),
            }
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
            // (CONCEPT:AU-KG.query.vendor-agnostic-traversal). One call instead of per-node round-trips.
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
        // ApplyMutation carries a SPARQL UPDATE string (governance / CDC mutation).
        // Replaced the legacy naive `{ <s> <p> <o> }` string-split shim with the REAL
        // SPARQL 1.1 UPDATE executor (CONCEPT:EG-KG.query.named-graph-support): a full spargebra parse + the
        // native merge-aware property-graph write ops (INSERT/DELETE DATA, DELETE/INSERT
        // … WHERE, CLEAR/CREATE/DROP GRAPH). Single-graph: every graph term routes to the
        // request graph's core (true named-graph routing lives on the /sparql endpoint,
        // which has the registry). `event_type` is now advisory (the query is
        // self-describing). Gated `sparql`; a non-sparql build rejects it explicitly.
        Method::ApplyMutation { event_type, query } => {
            #[cfg(feature = "sparql")]
            {
                let _ = event_type;
                struct SingleCoreStore(Arc<GraphCore>);
                impl eg_rdf::update::GraphStore for SingleCoreStore {
                    fn core(&self, _graph: Option<&str>) -> Option<Arc<GraphCore>> {
                        Some(self.0.clone())
                    }
                }
                let store = SingleCoreStore(core.clone());
                // X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard): run the SPARQL
                // UPDATE under the eg-shacl ICV `WriteGuard` when `shacl` is built —
                // `execute_guarded_str` is the library's OWN guarded twin of
                // `execute_str` (simulate-and-diff against the registered policy;
                // NOTHING is applied to the real store on a rejection). `SingleCoreStore`
                // exposes only the DEFAULT graph (no `named()`), so this checks the
                // `IcvConfigure(graph=None, …)` policy — configure the per-named-graph
                // policy for the `AddTriples`/`RemoveTriples` surface instead. Without
                // `shacl` this is byte-identical to the pre-X5 unguarded path.
                #[cfg(feature = "shacl")]
                let result = crate::server::icv_guard::with_write_guard(|guard| {
                    eg_rdf::update::execute_guarded_str(
                        &query,
                        &store,
                        &eg_rdf::sparql::Projection::raw(),
                        guard,
                    )
                    .map_err(|e| e.to_string())
                });
                #[cfg(not(feature = "shacl"))]
                let result =
                    eg_rdf::update::execute_str(&query, &store, &eg_rdf::sparql::Projection::raw());
                match result {
                    Ok(report) => match serde_json::to_value(&report) {
                        Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
                        Err(e) => Response::err(req_id, e.to_string()),
                    },
                    Err(e) => Response::err(req_id, format!("ApplyMutation: {e}")),
                }
            }
            #[cfg(not(feature = "sparql"))]
            {
                let _ = (event_type, query, &core);
                Response::err(
                    req_id,
                    "ApplyMutation (SPARQL UPDATE) requires the `sparql` feature".to_string(),
                )
            }
        }
        // CONCEPT:EG-KG.compute.compiled-semantic-reasoner - Compiled Semantic Reasoner. Forward-chaining
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
        Method::ResolveCandidates {
            sim_threshold,
            merge_threshold,
            node_type,
        } => {
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
        Method::UnionGetNodeProperties { graphs, node_id } => {
            // First-found across the graph set (in order); point reads, no
            // snapshot. Registry lock released before any per-core read.
            let cores = match resolve_union_cores(state, caller, &graphs).await {
                Ok(c) => c,
                Err(denied) => return Response::err(req_id, denied),
            };
            for c in &cores {
                if let Some(props) = c.get_node_properties(&node_id) {
                    return Response::ok(req_id, ResultPayload::PropertiesMsgpack(props));
                }
            }
            Response::ok(req_id, ResultPayload::Json(serde_json::Value::Null))
        }
        Method::UnionGetNodesByLabel {
            graphs,
            label,
            limit,
        } => {
            let cores = match resolve_union_cores(state, caller, &graphs).await {
                Ok(c) => c,
                Err(denied) => return Response::err(req_id, denied),
            };
            let mut seen = std::collections::HashSet::new();
            let mut nodes: Vec<(String, serde_json::Value)> = Vec::new();
            'outer: for c in &cores {
                for (k, p) in c.get_nodes_by_label(&label, limit) {
                    if seen.insert(k.clone()) {
                        let val = rmp_serde::from_slice::<serde_json::Value>(&p)
                            .unwrap_or(serde_json::json!({}));
                        nodes.push((k, val));
                        if limit != 0 && nodes.len() >= limit {
                            break 'outer;
                        }
                    }
                }
            }
            Response::ok(req_id, ResultPayload::NodeList(nodes))
        }
        Method::UnionGetNeighbors { graphs, node_id } => {
            let cores = match resolve_union_cores(state, caller, &graphs).await {
                Ok(c) => c,
                Err(denied) => return Response::err(req_id, denied),
            };
            let mut seen = std::collections::HashSet::new();
            let mut out: Vec<String> = Vec::new();
            for c in &cores {
                if let Ok(ns) = c.get_neighbors(&node_id) {
                    for n in ns {
                        if seen.insert(n.clone()) {
                            out.push(n);
                        }
                    }
                }
            }
            Response::ok(req_id, ResultPayload::Ids(out))
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
        // Catch-all: an unknown graph method, OR a feature-gated method whose
        // feature (finance / datascience / reasoning / query) was not built in.
        _ => Response::err(
            req_id,
            "Method not available in this server build (unknown method, or a \
             feature — finance/datascience/reasoning/query — not enabled)",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // CONCEPT:EG-KG.ingest.resets-socket-so-assimilation — the GetNodes overload backstop decision logic.

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
    fn zero_cap_disables_the_guard() {
        // An operator can opt back into the unbounded legacy behavior with
        // EPISTEMIC_GRAPH_MAX_RESPONSE_NODES=0 — no error even for a huge graph.
        assert_eq!(oversize_dump_error(10_000_000, 0), None);
    }
}
