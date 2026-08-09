//! Graph-targeted operation handlers (node/edge CRUD, embeddings + semantic
//! search, topology/centrality/community algorithms, lifecycle/decay, ledger,
//! reasoning, and cross-graph fork/diff/subgraph-match). These borrow the graph
//! `core` (and, for cross-graph ops, the registry via `state`); heavy reads run
//! off-lock. The dispatch shell owns the cross-cutting write side-effects
//! (dirty/WAL/gauge) — handlers here only produce the `Response`.

use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::access::{check_graph_access, requires_write, GraphReadAuthority};
use super::super::compute::{compute_off_lock, weight_semantic_results};
use super::super::mutation::{self, GatewayAuthzCtx, MutationCtx, MutationPlan};
use super::super::persistence::PersistenceBackend;
use super::super::state::{max_response_edges, max_response_nodes, ServerState, MAX_BATCH_IDS};
use crate::graph::GraphCore;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload, Vf2MatchResult};

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
    read_authority: &GraphReadAuthority,
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
            read_authority.actor(),
            name,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Read,
        )?;
        cores.push(entry.core.clone());
    }
    drop(s);
    Ok(cores
        .iter()
        .map(|core| read_authority.project_core(core))
        .collect())
}

/// Intelligent overload backstop for the `GetNodes` full-graph dump
/// (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation). Given the graph's node `count` and the configured `cap`,
/// returns `Some(error_message)` when the dump would exceed the cap (so the
/// handler can refuse with a typed `RESULT_TOO_LARGE` error instead of building
/// a gigabyte-scale frame that resets the client connection), or `None` when the
/// dump is within bounds and safe to materialize. The cap is always positive in
/// served state and cannot be disabled. Pure + side-effect-free so the threshold
/// logic is unit-tested directly, independent of process-global env.
fn oversize_dump_error(count: usize, cap: usize) -> Option<String> {
    if count > cap {
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

/// Intelligent overload backstop for the `GetEdges` full-graph dump — the
/// edge-count sibling of [`oversize_dump_error`]. Given the graph's edge `count`
/// and the configured `cap`, returns `Some(error_message)` when the dump would
/// exceed the cap, or `None` when it is within bounds and safe to materialize.
/// The cap is always positive in served state and cannot be disabled. Pure +
/// side-effect-free so the threshold logic is unit-tested directly, independent
/// of process-global env.
fn oversize_edge_dump_error(count: usize, cap: usize) -> Option<String> {
    if count > cap {
        Some(format!(
            "RESULT_TOO_LARGE: GetEdges would return {count} edges (> cap {cap}); \
             the full-graph dump is refused to protect the connection. Use a \
             bounded query instead (GetEdgesPage(after, limit) / paginate), or \
             raise EPISTEMIC_GRAPH_MAX_RESPONSE_EDGES."
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
    eg_types::msgpack::decode_property_object(blob).unwrap_or_default()
}

/// Keep the mutation kernel behind a heap indirection so this module's large
/// gateway match does not embed one copy of the kernel future in every arm.
/// Without this boundary the generated `try_handle_gateway` future exceeds
/// Tokio's default worker-thread stack on ordinary mutation requests.
async fn commit_gateway<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    Box::pin(mutation::commit_mutation(ctx, plan, method, apply)).await
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
    let val = eg_types::msgpack::decode_property_value(blob).ok()?;
    eg_core::scene::Pose::from_json(&val)
}

/// Route a [`mutation::GATEWAY_ROUTED`] method through the single commit gateway
/// (CONCEPT:EG-P0-2). Called from `dispatch_graph_op` AHEAD of both the write-
/// coalescer and the terminal exhaustive match below, so a routed method NEVER falls
/// through to `g.add_node(...)` etc. directly — the only path left for it is this
/// one, which builds a [`MutationPlan`] straight from `eg_capabilities::policy` and
/// calls [`mutation::commit_mutation`]. A method NOT in the routed set is handed
/// straight back (`Err(method)`), unchanged, exactly like every other domain
/// router in `dispatch.rs`'s routing chain.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn try_handle_gateway(
    req_id: u64,
    caller: Option<&str>,
    tenant_scope: &str,
    graph_name: &str,
    core: &Arc<GraphCore>,
    materialization_manifest: Option<
        &Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>,
    >,
    read_authority: Option<&GraphReadAuthority>,
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    #[cfg(feature = "streaming")] cdc: Option<&Arc<crate::server::cdc::CdcHub>>,
    write_coalescer: Option<&Arc<crate::write_coalescer::WriteCoalescerRegistry>>,
    authz_ctx: Option<&GatewayAuthzCtx>,
    // CONCEPT:EG-KG.mining.tsdb-typed-absent — the server's live tsdb store, needed ONLY by
    // the gateway-routed `Mine*` arms below to bind a plan-sourced `Op::TsScan` leg (mirrors
    // `mining::try_handle`'s own binding for the one Mine* method NOT gateway-routed).
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))] tsdb_store: Option<
        &Arc<eg_tsdb::store::SeriesStore>,
    >,
    method: Method,
) -> Result<Response, Method> {
    if !mutation::is_gateway_routed(&method) {
        return Err(method);
    }
    // L11 batch 4: the query surface (`Sql`/`CypherQuery`/`GraphQl`) and the native
    // RDF write surface (`AddTriples`/`RemoveTriples`/`DropNamedGraph`) ARE
    // `GATEWAY_ROUTED`, but their execution is `async` and needs `state`/`rls` that this graph-ops entry point
    // does not carry — so they are routed via `commit_conditional_mutation_async` at
    // their OWN dispatch sites in `dispatch.rs`. Hand them back here so they reach
    // those sites; the `record_method`/`cdc_*` gating in `dispatch.rs` already keys
    // off the SAME `is_gateway_routed`, so nothing double-applies.
    if mutation::is_query_gateway_method(&method) || mutation::is_rdf_gateway_method(&method) {
        return Err(method);
    }
    // Runtime-conditional gateway methods may be reads (`writeback = false`).
    // Give those closures a detached, row-filtered core; writes ignore any read
    // authority and must keep operating on the authoritative graph. This is
    // deliberately after both hand-back checks above, so SQL/RDF reads retain
    // their existing snapshot-level RLS path without paying for a second copy.
    let mutates = requires_write(&method);
    let projected_core = if mutates {
        None
    } else {
        read_authority.map(|authority| authority.project_core(core))
    };
    let selected_core = projected_core.as_ref().unwrap_or(core);
    debug_assert!(
        !mutates || Arc::ptr_eq(selected_core, core),
        "mutation gateway must retain the authoritative serving projection"
    );
    let core = selected_core;
    let (isolation, graph_type, owner) = authz_ctx.expect(
        "dispatch_graph_op must capture a GatewayAuthzCtx for every mutation::is_gateway_routed method",
    );
    let plan = MutationPlan::for_method(&method);
    let ctx = MutationCtx {
        req_id,
        caller,
        tenant_scope,
        graph_name,
        graph_type: *graph_type,
        owner: owner.as_deref(),
        isolation,
        core,
        persistence,
        #[cfg(feature = "streaming")]
        cdc,
        materialization_manifest,
        write_coalescer,
    };
    let resp = match &method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            let (node_id, properties_msgpack) = (node_id.clone(), properties_msgpack.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                core.add_node(node_id, properties_msgpack);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::CreateNodeIfAbsent {
            node_id,
            properties_msgpack,
        } => {
            let (node_id, properties_msgpack) = (node_id.clone(), properties_msgpack.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                Ok(ResultPayload::Bool(
                    core.create_node_if_absent(node_id, properties_msgpack),
                ))
            })
            .await
        }
        Method::RemoveNode { node_id } => {
            let node_id = node_id.clone();
            commit_gateway(&ctx, &plan, &method, move |core| {
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
            commit_gateway(&ctx, &plan, &method, move |core| {
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
            commit_gateway(&ctx, &plan, &method, move |core| {
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
            commit_gateway(&ctx, &plan, &method, move |core| {
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
            commit_gateway(&ctx, &plan, &method, move |core| {
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
            commit_gateway(&ctx, &plan, &method, move |core| {
                let existed = core.reinforce(&node_id, now_ms, weight);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        // ── L11 rollout batch 2 (EG-P0-2 continued): graph-core family ──
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            let (node_id, conditions_msgpack, updates_msgpack) = (
                node_id.clone(),
                conditions_msgpack.clone(),
                updates_msgpack.clone(),
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let conditions =
                    match eg_types::msgpack::decode_property_object(&conditions_msgpack) {
                        Ok(m) => m,
                        Err(_) => return Ok(ResultPayload::Bool(false)),
                    };
                let updates = match eg_types::msgpack::decode_property_object(&updates_msgpack) {
                    Ok(m) => m,
                    Err(_) => return Ok(ResultPayload::Bool(false)),
                };
                let ok = core.compare_and_set_fields(&node_id, &conditions, &updates);
                Ok(ResultPayload::Bool(ok))
            })
            .await
        }
        Method::ClaimNext {
            label,
            updates_msgpack,
        } => {
            let (label, updates_msgpack) = (label.clone(), updates_msgpack.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let updates = match eg_types::msgpack::decode_property_object(&updates_msgpack) {
                    Ok(m) => m,
                    Err(_) => {
                        return Ok(ResultPayload::raw(
                            &Option::<(String, serde_json::Value)>::None,
                        ))
                    }
                };
                let claimed = core.claim_next_fields(&label, &updates);
                Ok(ResultPayload::raw(&claimed))
            })
            .await
        }
        Method::DecayNode {
            node_id,
            now_ms,
            half_life_ms,
        } => {
            let (node_id, now_ms, half_life_ms) = (node_id.clone(), *now_ms, *half_life_ms);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let acted = core.decay_node(&node_id, now_ms, half_life_ms);
                Ok(ResultPayload::Bool(acted))
            })
            .await
        }
        Method::DecayMemories {
            now_ms,
            half_life_ms,
            ids,
        } => {
            let (now_ms, half_life_ms, ids) = (*now_ms, *half_life_ms, ids.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let n = core.decay_memories(now_ms, half_life_ms, &ids);
                Ok(ResultPayload::Count(n as u64))
            })
            .await
        }
        Method::EvictBelow {
            ids,
            threshold,
            delete,
        } => {
            let (ids, threshold, delete) = (ids.clone(), *threshold, *delete);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let pruned = core.evict_below(&ids, threshold, delete);
                Ok(ResultPayload::Ids(pruned))
            })
            .await
        }
        Method::Maintain {
            ids,
            now_ms,
            half_life_ms,
            evict_threshold,
            delete,
        } => {
            let (ids, now_ms, half_life_ms, evict_threshold, delete) = (
                ids.clone(),
                *now_ms,
                *half_life_ms,
                *evict_threshold,
                *delete,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let out = core.maintain(&ids, now_ms, half_life_ms, evict_threshold, delete);
                Ok(ResultPayload::raw(&out))
            })
            .await
        }
        Method::AddSceneObject {
            pose_msgpack,
            parent,
        } => {
            let (pose_msgpack, parent) = (pose_msgpack.clone(), parent.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let Some(pose) = decode_pose(&pose_msgpack) else {
                    return Err("AddSceneObject: undecodable pose_msgpack".to_string());
                };
                let id = core.add_scene_object(&pose, parent.as_deref());
                Ok(ResultPayload::String(id))
            })
            .await
        }
        Method::SetPose {
            node_id,
            pose_msgpack,
        } => {
            let (node_id, pose_msgpack) = (node_id.clone(), pose_msgpack.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let Some(pose) = decode_pose(&pose_msgpack) else {
                    return Err("SetPose: undecodable pose_msgpack".to_string());
                };
                let ok = core.set_pose(&node_id, &pose);
                Ok(ResultPayload::Bool(ok))
            })
            .await
        }
        Method::Reparent {
            node_id,
            new_parent,
        } => {
            let (node_id, new_parent) = (node_id.clone(), new_parent.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let ok = core.reparent(&node_id, new_parent.as_deref());
                Ok(ResultPayload::Bool(ok))
            })
            .await
        }
        Method::StartTrajectory { props_msgpack } => {
            let props_msgpack = props_msgpack.clone();
            commit_gateway(&ctx, &plan, &method, move |core| {
                let props = decode_json_object(&props_msgpack);
                let id = core.start_trajectory(props);
                Ok(ResultPayload::String(id))
            })
            .await
        }
        Method::AppendStep {
            traj_id,
            action_msgpack,
            reward,
            state_ref,
            next_state_ref,
            t,
        } => {
            let (traj_id, action_msgpack, reward, state_ref, next_state_ref, t) = (
                traj_id.clone(),
                action_msgpack.clone(),
                *reward,
                state_ref.clone(),
                next_state_ref.clone(),
                *t,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let action = eg_types::msgpack::decode_property_value(&action_msgpack)
                    .unwrap_or(serde_json::Value::Null);
                let step_id = core.append_step(
                    &traj_id,
                    action,
                    reward,
                    state_ref.as_deref(),
                    next_state_ref.as_deref(),
                    t,
                );
                Ok(ResultPayload::raw(&step_id))
            })
            .await
        }
        Method::AddEmbedding { node_id, embedding } => {
            let (node_id, embedding) = (node_id.clone(), embedding.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let source_version = core.version();
                core.semantic_store
                    .write()
                    .add_embedding(node_id, embedding)
                    .map_err(|error| error.to_string())?;
                // Content-derived indexes are unchanged by a vector-only
                // mutation, but their completeness manifest must advance with
                // the graph version that commit_finalize publishes.
                core.maintain_indexes_at(
                    &crate::index::ChangeSet::new(),
                    source_version.saturating_add(1),
                    core.node_count(),
                    core.edge_count(),
                );
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::InvalidateEdge {
            source_id,
            target_id,
            relationship,
            invalid_at,
            tx_now,
        } => {
            let (source_id, target_id, relationship, invalid_at, tx_now) = (
                source_id.clone(),
                target_id.clone(),
                relationship.clone(),
                *invalid_at,
                *tx_now,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let n =
                    core.invalidate_edge(&source_id, &target_id, &relationship, invalid_at, tx_now);
                Ok(ResultPayload::Count(n as u64))
            })
            .await
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
            let (
                source_id,
                target_id,
                properties_msgpack,
                prior_source,
                prior_target,
                prior_relationship,
                valid_at,
                tx_now,
            ) = (
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
                prior_source.clone(),
                prior_target.clone(),
                prior_relationship.clone(),
                *valid_at,
                *tx_now,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                match core.supersede_edge(
                    source_id,
                    target_id,
                    properties_msgpack,
                    &prior_source,
                    &prior_target,
                    &prior_relationship,
                    valid_at,
                    tx_now,
                ) {
                    Ok(()) => Ok(ResultPayload::String("ok".to_string())),
                    Err(e) => Err(e),
                }
            })
            .await
        }
        Method::ClearGraph => {
            commit_gateway(&ctx, &plan, &method, move |core| {
                core.clear();
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::EvictLRU { max_nodes } => {
            let max_nodes = *max_nodes;
            // Eviction changes RAM RESIDENCY ONLY — never durable content.
            //
            // It used to run `core.evict_lru()` on the gateway's STAGED copy. The
            // gateway then diffs `base_snapshot` (the live core, which holds the
            // node) against the staged image (which no longer does) and publishes
            // that delta, so every eviction durably DELETED exactly the rows it
            // evicted. That is silent data loss, and it made the read-through seam
            // unreachable by construction: `read_node_blocking`'s own contract says
            // "eviction is durability-gated ... so an evicted node is always served
            // here", and both `delete_then_recreate_same_name_keeps_new_writes` and
            // `evicted_graph_lazy_reopens_with_data_intact` assert the row survives.
            // Measured directly: read_node_blocking returned Some(4) before an
            // EvictLRU and None immediately after it.
            //
            // So the staged image is left UNTOUCHED (an eviction is not a content
            // change, so the correct delta is the empty one), and the residency
            // change is applied to the LIVE core afterwards — durability-gated
            // exactly like the background evictor in `persist::evict_oversized_all`,
            // which never had this bug because it never went through the gateway.
            // The gateway call stays so the op keeps its `node:admin` authz, its
            // fencing, and its version/plan semantics.
            let response = commit_gateway(&ctx, &plan, &method, move |_staged| {
                Ok(ResultPayload::Json(serde_json::json!(0)))
            })
            .await;
            if response.error.is_some() {
                return Ok(response);
            }
            let candidates = ctx.core.lru_eviction_candidates(max_nodes);
            let evicted = if candidates.is_empty() {
                0
            } else if let Some(backend) = ctx.persistence {
                let fname = crate::persist::sanitize(ctx.graph_name);
                match backend.durable_node_presence(&fname, &candidates) {
                    Ok(presence) if presence.len() == candidates.len() => {
                        let durable = candidates
                            .into_iter()
                            .zip(presence)
                            .filter_map(|(node_id, present)| present.then_some(node_id))
                            .collect::<Vec<_>>();
                        ctx.core.evict_resident_nodes(&durable)
                    }
                    // Durability unconfirmed: keep the nodes resident rather than
                    // risk evicting something that is not on disk yet.
                    Ok(_) | Err(_) => 0,
                }
            } else {
                // No durable tier to fall back to, so eviction would lose the node.
                0
            };
            Response::ok(ctx.req_id, ResultPayload::Json(serde_json::json!(evicted)))
        }
        Method::DecaySweep {
            half_life_secs,
            floor,
            prune,
        } => {
            let (half_life_secs, floor, prune) = (*half_life_secs, *floor, *prune);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let stats = core.decay_sweep(now, half_life_secs, floor, prune);
                serde_json::to_value(&stats)
                    .map(ResultPayload::Json)
                    .map_err(|e| e.to_string())
            })
            .await
        }
        Method::TouchNodes { node_ids } => {
            let node_ids = node_ids.clone();
            commit_gateway(&ctx, &plan, &method, move |core| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let touched = core.touch_nodes(&node_ids, now);
                Ok(ResultPayload::Count(touched as u64))
            })
            .await
        }
        Method::FromMsgpack { msgpack } => {
            let msgpack = msgpack.clone();
            commit_gateway(&ctx, &plan, &method, move |core| {
                core.from_msgpack(&msgpack)
                    .map(|()| ResultPayload::String("ok".to_string()))
                    .map_err(|e| e.to_string())
            })
            .await
        }
        Method::Reconcile { msgpack, .. } => {
            let msgpack = msgpack.clone();
            commit_gateway(&ctx, &plan, &method, move |core| {
                core.from_msgpack(&msgpack)
                    .map(|()| ResultPayload::String("reconciled".to_string()))
                    .map_err(|e| e.to_string())
            })
            .await
        }
        Method::ApplyMutation { event_type, query } => {
            let (event_type, query) = (event_type.clone(), query.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                #[cfg(feature = "sparql")]
                {
                    let _ = event_type;
                    struct SingleCoreStore(std::sync::Arc<GraphCore>);
                    impl eg_rdf::update::GraphStore for SingleCoreStore {
                        fn core(&self, graph: Option<&str>) -> Option<std::sync::Arc<GraphCore>> {
                            graph.is_none().then(|| self.0.clone())
                        }
                    }
                    #[cfg(feature = "shacl")]
                    {
                        let update = eg_rdf::update::parse_update(&query)
                            .map_err(|error| format!("ApplyMutation: {error}"))?;
                        if !eg_rdf::update::referenced_named_graphs(&update).is_empty() {
                            return Err(
                                "ApplyMutation: graph-scoped updates cannot address a named RDF graph"
                                    .to_string(),
                            );
                        }
                        // GraphStore requires an owned Arc. Clone the isolated
                        // gateway image, execute there, then copy the successful
                        // result back into that same staged image. The live core is
                        // never exposed before the authoritative commit succeeds.
                        let update_core = std::sync::Arc::new(GraphCore::from_snapshot(
                            core.snapshot(),
                            core.version(),
                        )?);
                        let store = SingleCoreStore(update_core.clone());
                        let guard =
                            crate::server::icv_guard::CoreIcvGuard::single(update_core.as_ref());
                        let report = eg_rdf::update::execute(
                            &update,
                            &store,
                            &eg_rdf::sparql::Projection::raw(),
                            &guard,
                        )
                        .map_err(|error| format!("ApplyMutation: {error}"))?;
                        core.replace_snapshot(update_core.snapshot())?;
                        serde_json::to_value(&report)
                            .map(ResultPayload::Json)
                            .map_err(|error| error.to_string())
                    }
                    #[cfg(not(feature = "shacl"))]
                    {
                        Err("ApplyMutation requires the shacl integrity-guard feature".to_string())
                    }
                }
                #[cfg(not(feature = "sparql"))]
                {
                    let _ = (event_type, query, core);
                    Err("ApplyMutation (SPARQL UPDATE) requires the `sparql` feature".to_string())
                }
            })
            .await
        }
        // IcvConfigure is ordinary graph control state: validate and stage it on
        // the authorized graph image so policy + rows share one commit/snapshot.
        #[cfg(feature = "shacl")]
        Method::IcvConfigure {
            graph,
            mode,
            shapes,
        } => {
            let (graph, mode, shapes) = (graph.clone(), mode.clone(), shapes.clone());
            let request_graph = ctx.graph_name.to_string();
            commit_gateway(&ctx, &plan, &method, move |core| {
                crate::server::icv_guard::configure(
                    core,
                    &request_graph,
                    graph.as_deref(),
                    &mode,
                    &shapes,
                )
                .map(|()| ResultPayload::Bool(true))
            })
            .await
        }
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
            let (
                subclass_relations,
                subproperty_relations,
                symmetric_properties,
                transitive_properties,
                inverse_properties,
                domain_rules,
                range_rules,
                property_chains,
            ) = (
                subclass_relations.clone(),
                subproperty_relations.clone(),
                symmetric_properties.clone(),
                transitive_properties.clone(),
                inverse_properties.clone(),
                domain_rules.clone(),
                range_rules.clone(),
                property_chains.clone(),
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let mut all_inferred: Vec<std::collections::HashMap<String, String>> = Vec::new();
                match crate::reasoning::run_datalog_reasoning(
                    core,
                    subclass_relations,
                    subproperty_relations,
                    symmetric_properties,
                    transitive_properties,
                    inverse_properties,
                ) {
                    Ok(triples) => all_inferred.extend(triples),
                    Err(e) => return Err(e),
                }
                if !domain_rules.is_empty() || !range_rules.is_empty() {
                    all_inferred.extend(crate::reasoning::infer_domain_range(
                        core,
                        domain_rules,
                        range_rules,
                    ));
                }
                if !property_chains.is_empty() {
                    all_inferred.extend(crate::reasoning::infer_property_chains(
                        core,
                        property_chains,
                    ));
                }
                Ok(ResultPayload::Json(serde_json::json!({
                    "inferred_count": all_inferred.len(),
                    "inferred_triples": all_inferred,
                })))
            })
            .await
        }
        Method::PruneByLifecycle {
            max_age_secs,
            min_score,
        } => {
            let (max_age_secs, min_score) = (*max_age_secs, *min_score);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let stats = crate::algorithms::prune_by_lifecycle(core, max_age_secs, min_score);
                serde_json::to_value(&stats)
                    .map(ResultPayload::Json)
                    .map_err(|e| e.to_string())
            })
            .await
        }
        Method::BatchUpdate { operations_msgpack } => {
            let operations_msgpack = operations_msgpack.clone();
            commit_gateway(
                &ctx,
                &plan,
                &method,
                move |core| match crate::algorithms::batch_update(core, &operations_msgpack) {
                    Ok(res) => eg_types::msgpack::decode_property_value(&res)
                        .map(ResultPayload::Json)
                        .map_err(|_| "Invalid batch result".to_string()),
                    Err(e) => Err(e),
                },
            )
            .await
        }
        Method::ClearLedger => {
            commit_gateway(&ctx, &plan, &method, move |core| {
                core.clear_ledger();
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::ApplyLedger { transactions } => {
            let transactions = transactions.clone();
            commit_gateway(&ctx, &plan, &method, move |core| {
                core.apply_ledger(transactions)
                    .map(|()| ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::CompactNodesByType {
            node_type,
            threshold,
        } => {
            let (node_type, threshold) = (node_type.clone(), *threshold);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let removed = core.compact_nodes_by_type(&node_type, threshold);
                Ok(ResultPayload::Json(
                    serde_json::json!({ "removed_nodes": removed }),
                ))
            })
            .await
        }
        // ── L11 rollout batch 2: message-broker / stream family (Outbox
        // durability domain), behind `feature = "broker"` — see the module docs. ──
        #[cfg(feature = "broker")]
        Method::DeclareExchange { exchange, kind } => {
            let (exchange, kind) = (exchange.clone(), kind.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let Some(k) = crate::broker::ExchangeKind::parse(&kind) else {
                    return Err(format!(
                        "unknown exchange kind '{kind}' (want direct/topic/fanout)"
                    ));
                };
                crate::broker::declare_exchange(core, &exchange, k)
                    .map(|()| ResultPayload::String("ok".to_string()))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::DeleteExchange { exchange } => {
            let exchange = exchange.clone();
            commit_gateway(&ctx, &plan, &method, move |core| {
                let existed = crate::broker::delete_exchange(core, &exchange);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            let (exchange, queue, routing_key) =
                (exchange.clone(), queue.clone(), routing_key.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                crate::broker::bind_queue(core, &exchange, &queue, &routing_key);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::UnbindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            let (exchange, queue, routing_key) =
                (exchange.clone(), queue.clone(), routing_key.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let existed = crate::broker::unbind_queue(core, &exchange, &queue, &routing_key);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::Publish {
            exchange,
            routing_key,
            payload,
        } => {
            let (exchange, routing_key, payload) =
                (exchange.clone(), routing_key.clone(), payload.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let delivered = crate::broker::publish(core, &exchange, &routing_key, &payload);
                Ok(ResultPayload::Count(delivered as u64))
            })
            .await
        }
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
            let (
                queue,
                dl_exchange,
                dl_routing_key,
                max_delivery_count,
                message_ttl_ms,
                queue_expiry_ms,
                max_priority,
            ) = (
                queue.clone(),
                dl_exchange.clone(),
                dl_routing_key.clone(),
                *max_delivery_count,
                *message_ttl_ms,
                *queue_expiry_ms,
                *max_priority,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let policy = crate::broker::QueuePolicy {
                    dl_exchange,
                    dl_routing_key,
                    max_delivery_count,
                    message_ttl_ms,
                    queue_expiry_ms,
                    max_priority,
                };
                crate::broker::declare_queue(core, &queue, &policy);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
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
            let (exchange, routing_key, payload, priority, delay_ms, ttl_ms, now_ms) = (
                exchange.clone(),
                routing_key.clone(),
                payload.clone(),
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let delivered = crate::broker::publish_ex(
                    core,
                    &exchange,
                    &routing_key,
                    &payload,
                    priority,
                    delay_ms,
                    ttl_ms,
                    now_ms,
                );
                Ok(ResultPayload::Count(delivered as u64))
            })
            .await
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
            let (queue, group, consumer, now_ms, lease_ms, prefetch) = (
                queue.clone(),
                group.clone(),
                consumer.clone(),
                *now_ms,
                *lease_ms,
                *prefetch,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let claimed = crate::broker::broker_consume(
                    core, &queue, &group, &consumer, now_ms, lease_ms, prefetch,
                );
                Ok(ResultPayload::raw(&claimed))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BrokerAck { queue, node_id } => {
            let (queue, node_id) = (queue.clone(), node_id.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let existed = crate::broker::broker_ack(core, &queue, &node_id);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BrokerReject {
            queue,
            node_id,
            requeue,
            now_ms,
        } => {
            let (queue, node_id, requeue, now_ms) =
                (queue.clone(), node_id.clone(), *requeue, *now_ms);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let outcome = crate::broker::broker_reject(core, &queue, &node_id, requeue, now_ms);
                Ok(ResultPayload::String(outcome))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::SweepExpired { now_ms } => {
            let now_ms = *now_ms;
            commit_gateway(&ctx, &plan, &method, move |core| {
                let acted = crate::broker::sweep_expired(core, now_ms);
                Ok(ResultPayload::Count(acted as u64))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::StreamDeclare {
            stream,
            max_messages,
            max_age_ms,
        } => {
            let (stream, max_messages, max_age_ms) = (stream.clone(), *max_messages, *max_age_ms);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let retention = crate::broker::StreamRetention {
                    max_messages,
                    max_age_ms,
                };
                crate::broker::declare_stream(core, &stream, &retention);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::StreamPublish {
            stream,
            payload,
            now_ms,
        } => {
            let (stream, payload, now_ms) = (stream.clone(), payload.clone(), *now_ms);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let offset = crate::broker::stream_publish(core, &stream, &payload, now_ms);
                Ok(ResultPayload::Count(offset as u64))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::StreamTrim { stream, now_ms } => {
            let (stream, now_ms) = (stream.clone(), *now_ms);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let dropped = crate::broker::stream_trim(core, &stream, now_ms);
                Ok(ResultPayload::Count(dropped as u64))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset {
            stream,
            group,
            offset,
        } => {
            let (stream, group, offset) = (stream.clone(), group.clone(), *offset);
            commit_gateway(&ctx, &plan, &method, move |core| {
                crate::broker::commit_offset(core, &stream, &group, offset);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
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
            let (exchange, routing_key, payload, priority, delay_ms, ttl_ms, now_ms) = (
                exchange.clone(),
                routing_key.clone(),
                payload.clone(),
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let token = crate::broker::publish_confirmed(
                    core,
                    &exchange,
                    &routing_key,
                    &payload,
                    priority,
                    delay_ms,
                    ttl_ms,
                    now_ms,
                );
                Ok(ResultPayload::raw(&token))
            })
            .await
        }
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
            let (
                exchange,
                routing_key,
                payload,
                producer_id,
                seq,
                priority,
                delay_ms,
                ttl_ms,
                now_ms,
            ) = (
                exchange.clone(),
                routing_key.clone(),
                payload.clone(),
                producer_id.clone(),
                *seq,
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
            commit_gateway(&ctx, &plan, &method, move |core| {
                let result = crate::broker::publish_idempotent(
                    core,
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
                Ok(ResultPayload::raw(&result))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BrokerAckTag {
            delivery_tag,
            consumer,
        } => {
            let (delivery_tag, consumer) = (*delivery_tag, consumer.clone());
            commit_gateway(&ctx, &plan, &method, move |core| {
                let existed = crate::broker::broker_ack_tag(core, delivery_tag, &consumer);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BrokerNackTag {
            delivery_tag,
            consumer,
            requeue,
            now_ms,
        } => {
            let (delivery_tag, consumer, requeue, now_ms) =
                (*delivery_tag, consumer.clone(), *requeue, *now_ms);
            commit_gateway(&ctx, &plan, &method, move |core| {
                let outcome =
                    crate::broker::broker_nack_tag(core, delivery_tag, &consumer, requeue, now_ms);
                Ok(ResultPayload::String(outcome))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag {
            delivery_tag,
            consumer,
            now_ms,
            lease_ms,
        } => {
            let (delivery_tag, consumer, now_ms, lease_ms) =
                (*delivery_tag, consumer.clone(), *now_ms, *lease_ms);
            commit_gateway(&ctx, &plan, &method, move |core| {
                Ok(ResultPayload::Bool(crate::broker::broker_renew_tag(
                    core,
                    delivery_tag,
                    &consumer,
                    now_ms,
                    lease_ms,
                )))
            })
            .await
        }
        // ── L11 rollout batch 3: RUNTIME-CONDITIONAL graph-learning family — the
        // request's own `writeback` field decides whether THIS call mutates;
        // `commit_conditional_mutation` only drives the write-authz/durability/
        // audit/CDC gateway when it actually does (see the module docs' L11 note
        // and `eg-capabilities`'s `RUNTIME_CONDITIONAL` divergence table). ──
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnFit {
            source,
            params,
            writeback,
        } => {
            let (source, params, writeback) = (source.clone(), params.clone(), *writeback);
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let resp = super::graphlearn::handle_fit(req_id, core, source, params, writeback);
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnPredict {
            model,
            source,
            candidate_pairs,
            top_k,
            writeback,
        } => {
            let (model, source, candidate_pairs, top_k, writeback) = (
                model.clone(),
                source.clone(),
                candidate_pairs.clone(),
                *top_k,
                *writeback,
            );
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let resp = super::graphlearn::handle_predict(
                    req_id,
                    core,
                    model,
                    source,
                    candidate_pairs,
                    top_k,
                    writeback,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        // ── ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): RUNTIME-CONDITIONAL writes,
        // same `commit_conditional_mutation` shape as the GraphLearn*/Mine* families.
        // Train/Predict mutate only when their own `writeback` is true; Serve ALWAYS
        // writes the `:ServedModel` pointer (it passes an unconditional `true`). ──
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineTrain {
            name,
            source,
            x,
            y,
            spec,
            writeback,
        } => {
            let (name, source, x, y, spec, writeback) = (
                name.clone(),
                source.clone(),
                x.clone(),
                y.clone(),
                spec.clone(),
                *writeback,
            );
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let resp = super::pipeline::handle_train(
                    req_id, core, name, source, x, y, spec, writeback,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineServe { name, version } => {
            let (name, version) = (name.clone(), *version);
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, true, move |core| {
                let resp = super::pipeline::handle_serve(req_id, core, name, version);
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelinePredict {
            name,
            version,
            source,
            x,
            writeback,
        } => {
            let (name, version, source, x, writeback) = (
                name.clone(),
                *version,
                source.clone(),
                x.clone(),
                *writeback,
            );
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let resp = super::pipeline::handle_predict(
                    req_id, core, name, version, source, x, writeback,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        // ── L11 rollout batch 3: RUNTIME-CONDITIONAL data-mining family — same
        // shape as GraphLearn* above (the request's own `writeback` field decides
        // whether THIS call mutates). Each arm clones the whole `method` (cheap;
        // `Method` derives `Clone`) and re-destructures the OWNED clone inside the
        // `apply` closure via `let-else` — this keeps the field list a verbatim
        // copy of `mining::try_handle`'s own destructuring (the single source of
        // truth for each method's fields), rather than hand-cloning 10+ fields
        // per arm. `MineClassifyFit` is NOT here (policy explicit-false: it never
        // writes back) and keeps its ordinary read-only arm in `mining.rs`.
        #[cfg(feature = "mining")]
        Method::MineAssociate { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineAssociate {
                    transactions,
                    source,
                    min_support,
                    min_confidence,
                    algorithm,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_associate(
                    req_id,
                    core,
                    transactions,
                    source,
                    min_support,
                    min_confidence,
                    algorithm,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineCluster { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            let core_arc = ctx.core;
            #[cfg(all(feature = "query", feature = "tsdb"))]
            let tsdb_bind = super::mining::MiningTsdbBind {
                graph_name,
                read_authority,
                tsdb_store,
            };
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |_core| {
                let Method::MineCluster {
                    features,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    algorithm,
                    eps,
                    min_pts,
                    k,
                    linkage,
                    max_iter,
                    seed,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_cluster(
                    req_id,
                    core_arc,
                    features,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    algorithm,
                    eps,
                    min_pts,
                    k,
                    linkage,
                    max_iter,
                    seed,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                    #[cfg(all(feature = "query", feature = "tsdb"))]
                    tsdb_bind,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineAnomaly { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            let core_arc = ctx.core;
            #[cfg(all(feature = "query", feature = "tsdb"))]
            let tsdb_bind = super::mining::MiningTsdbBind {
                graph_name,
                read_authority,
                tsdb_store,
            };
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |_core| {
                let Method::MineAnomaly {
                    features,
                    values,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    algorithm,
                    k,
                    n_trees,
                    sample_size,
                    seed,
                    nu,
                    gamma,
                    kernel,
                    threshold,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_anomaly(
                    req_id,
                    core_arc,
                    features,
                    values,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    algorithm,
                    k,
                    n_trees,
                    sample_size,
                    seed,
                    nu,
                    gamma,
                    kernel,
                    threshold,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                    #[cfg(all(feature = "query", feature = "tsdb"))]
                    tsdb_bind,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineClassifyPredict { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            let core_arc = ctx.core;
            #[cfg(all(feature = "query", feature = "tsdb"))]
            let tsdb_bind = super::mining::MiningTsdbBind {
                graph_name,
                read_authority,
                tsdb_store,
            };
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |_core| {
                let Method::MineClassifyPredict {
                    model,
                    x,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_classify_predict(
                    req_id,
                    core_arc,
                    model,
                    x,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                    #[cfg(all(feature = "query", feature = "tsdb"))]
                    tsdb_bind,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineReduce { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            let core_arc = ctx.core;
            #[cfg(all(feature = "query", feature = "tsdb"))]
            let tsdb_bind = super::mining::MiningTsdbBind {
                graph_name,
                read_authority,
                tsdb_store,
            };
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |_core| {
                let Method::MineReduce {
                    x,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    labels,
                    algorithm,
                    n_components,
                    n_neighbors,
                    min_dist,
                    perplexity,
                    epochs,
                    lr,
                    seed,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_reduce(
                    req_id,
                    core_arc,
                    x,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    labels,
                    algorithm,
                    n_components,
                    n_neighbors,
                    min_dist,
                    perplexity,
                    epochs,
                    lr,
                    seed,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                    #[cfg(all(feature = "query", feature = "tsdb"))]
                    tsdb_bind,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineSequence { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineSequence {
                    sequences,
                    source,
                    min_support,
                    algorithm,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_sequence(
                    req_id,
                    core,
                    sequences,
                    source,
                    min_support,
                    algorithm,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineForecast { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineForecast {
                    values,
                    algorithm,
                    horizon,
                    p,
                    d,
                    q,
                    period,
                    alpha,
                    beta,
                    gamma,
                    confidence,
                    series_id,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_forecast(
                    req_id,
                    core,
                    values,
                    algorithm,
                    horizon,
                    p,
                    d,
                    q,
                    period,
                    alpha,
                    beta,
                    gamma,
                    confidence,
                    series_id,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineText { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineText {
                    docs,
                    source,
                    algorithm,
                    k,
                    alpha,
                    beta,
                    iterations,
                    seed,
                    top_n,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_text(
                    req_id,
                    core,
                    docs,
                    source,
                    algorithm,
                    k,
                    alpha,
                    beta,
                    iterations,
                    seed,
                    top_n,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineSubgraph { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineSubgraph {
                    label,
                    min_support,
                    max_edges,
                    algorithm,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_subgraph(
                    req_id,
                    core,
                    label,
                    min_support,
                    max_edges,
                    algorithm,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineEntityResolve { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineEntityResolve {
                    records,
                    block_keys,
                    vectors,
                    source,
                    ids,
                    bucket_precision,
                    threshold,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_entity_resolve(
                    req_id,
                    core,
                    records,
                    block_keys,
                    vectors,
                    source,
                    ids,
                    bucket_precision,
                    threshold,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineCausalImpact { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineCausalImpact {
                    series,
                    control,
                    intervention_index,
                    series_id,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_causal_impact(
                    req_id,
                    core,
                    series,
                    control,
                    intervention_index,
                    series_id,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineProcess { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineProcess {
                    traces,
                    process_id,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_process(
                    req_id,
                    core,
                    traces,
                    process_id,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineRootCause { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineRootCause {
                    nodes,
                    scores,
                    edges,
                    symptom,
                    max_hops,
                    decay,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_root_cause(
                    req_id,
                    core,
                    nodes,
                    scores,
                    edges,
                    symptom,
                    max_hops,
                    decay,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineRiskPropagation { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineRiskPropagation {
                    nodes,
                    seed,
                    edges,
                    damping,
                    tolerance,
                    max_iterations,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_risk_propagation(
                    req_id,
                    core,
                    nodes,
                    seed,
                    edges,
                    damping,
                    tolerance,
                    max_iterations,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineOntologyGap { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineOntologyGap {
                    label,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_ontology_gap(
                    req_id,
                    core,
                    label,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineRetrievalQuality { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineRetrievalQuality {
                    traces,
                    k,
                    query_id,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_retrieval_quality(
                    req_id,
                    core,
                    traces,
                    k,
                    query_id,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineCommunity { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(&ctx, &plan, &method, writeback, move |core| {
                let Method::MineCommunity {
                    label,
                    algorithm,
                    resolution,
                    max_iterations,
                    seed,
                    weighted,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                } = method_owned
                else {
                    unreachable!()
                };
                let resp = super::mining::handle_community(
                    req_id,
                    core,
                    label,
                    algorithm,
                    resolution,
                    max_iterations,
                    seed,
                    weighted,
                    writeback,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                );
                match resp.error {
                    Some(e) => Err(e),
                    None => Ok(resp
                        .result
                        .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                }
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
    _caller: Option<&str>,
    read_authority: &GraphReadAuthority,
    core: Arc<GraphCore>,
    method: Method,
) -> Response {
    // Keep the projection inside the terminal handler: any future internal caller
    // must supply a GraphReadAuthority and receives the same pre-compute projection
    // before the first primitive can inspect existence, counts, embeddings, or
    // topology. Query/RDF handlers instead retain their snapshot-level filter.
    let core = read_authority.project_core(&core);
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
        Method::CreateNodeIfAbsent { .. } => unreachable!(
            "CreateNodeIfAbsent is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
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
            // the pathological frame. The bounded reads (`GetNodesByLabel`, per-id)
            // are intentionally unaffected.
            if let Some(msg) = oversize_dump_error(g.node_count(), max_response_nodes()) {
                return Response::err(req_id, msg);
            }
            let nodes: Vec<(String, serde_json::Value)> = g
                .get_nodes()
                .into_iter()
                .map(|(k, p)| {
                    let val = eg_types::msgpack::decode_property_value(&p)
                        .unwrap_or(serde_json::json!({}));
                    (k, val)
                })
                .collect();
            Response::ok(req_id, ResultPayload::NodeList(nodes))
        }
        Method::GetNodesByLabel {
            label,
            after,
            limit,
        } => {
            let g = &*core;
            let nodes: Vec<(String, serde_json::Value)> = g
                .get_nodes_by_label_page(&label, after.as_deref(), limit)
                .into_iter()
                .map(|(k, p)| {
                    let val = eg_types::msgpack::decode_property_value(&p)
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
        // CompareAndSetNodeFields/ClaimNext (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::CompareAndSetNodeFields { .. } => unreachable!(
            "CompareAndSetNodeFields is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::ClaimNext { .. } => unreachable!(
            "ClaimNext is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        // ── Message broker admin + data (CONCEPT:EG-KG.compute.message-broker-exchanges) ─────────────────
        // Built on the KG-2.303 queue: exchanges/bindings are nodes on this target
        // graph; publish routes + enqueues; consume/ack REUSE ClaimNext + CAS above.
        // Same handler home + precedent as ClaimNext. Gated `broker`; a slim build
        // drops the variants (they fall to the catch-all "not available").
        // Broker/stream admin+data family (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above. `StreamRead`/
        // `StreamCommittedOffset` are pure reads (not in GATEWAY_ROUTED) and keep
        // their normal arms below.
        #[cfg(feature = "broker")]
        Method::DeclareExchange { .. } => unreachable!(
            "DeclareExchange is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::DeleteExchange { .. } => unreachable!(
            "DeleteExchange is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BindQueue { .. } => unreachable!(
            "BindQueue is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::UnbindQueue { .. } => unreachable!(
            "UnbindQueue is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::Publish { .. } => unreachable!(
            "Publish is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::DeclareQueue { .. } => unreachable!(
            "DeclareQueue is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::PublishEx { .. } => unreachable!(
            "PublishEx is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerConsume { .. } => unreachable!(
            "BrokerConsume is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerAck { .. } => unreachable!(
            "BrokerAck is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerReject { .. } => unreachable!(
            "BrokerReject is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::SweepExpired { .. } => unreachable!(
            "SweepExpired is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::StreamDeclare { .. } => unreachable!(
            "StreamDeclare is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::StreamPublish { .. } => unreachable!(
            "StreamPublish is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
        Method::StreamTrim { .. } => unreachable!(
            "StreamTrim is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset { .. } => unreachable!(
            "StreamCommitOffset is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::StreamCommittedOffset { stream, group } => {
            let committed = crate::broker::committed_offset(&core, &stream, &group);
            Response::ok(req_id, ResultPayload::raw(&committed))
        }
        #[cfg(feature = "broker")]
        Method::PublishConfirmed { .. } => unreachable!(
            "PublishConfirmed is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::PublishIdempotent { .. } => unreachable!(
            "PublishIdempotent is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerAckTag { .. } => unreachable!(
            "BrokerAckTag is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerNackTag { .. } => unreachable!(
            "BrokerNackTag is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag { .. } => unreachable!(
            "BrokerRenewTag is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        // ── Agent-memory / scene-graph / trajectory wire ops (CONCEPT:EG-KG.memory.eg-batch-decay-caller) ────
        // Route each Method to its eg-core `GraphCore` primitive. The mutating arms
        // share the SAME durable/deterministic contract as the broker precedent: the
        // dispatch shell records them (via `is_durable_mutation`) and `mutation_apply::apply`
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
        // DecayNode/DecayMemories/EvictBelow/Maintain (CONCEPT:EG-P0-2 bypass
        // guard, L11): GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::DecayNode { .. } => unreachable!(
            "DecayNode is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::DecayMemories { .. } => unreachable!(
            "DecayMemories is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::EvictBelow { .. } => unreachable!(
            "EvictBelow is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::Maintain { .. } => unreachable!(
            "Maintain is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::SummaryChildren { node_id } => {
            Response::ok(req_id, ResultPayload::Ids(core.summary_children(&node_id)))
        }
        Method::SummariesAtLevel { level } => {
            Response::ok(req_id, ResultPayload::Ids(core.summaries_at_level(level)))
        }
        // AddSceneObject/SetPose/Reparent (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::AddSceneObject { .. } => unreachable!(
            "AddSceneObject is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::SetPose { .. } => unreachable!(
            "SetPose is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::Reparent { .. } => unreachable!(
            "Reparent is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
        // StartTrajectory/AppendStep (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::StartTrajectory { .. } => unreachable!(
            "StartTrajectory is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::AppendStep { .. } => unreachable!(
            "AppendStep is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
        // AddEmbedding (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED — see
        // the AddNode/RemoveNode comment above.
        Method::AddEmbedding { .. } => unreachable!(
            "AddEmbedding is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
        // InvalidateEdge/SupersedeEdge (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::InvalidateEdge { .. } => unreachable!(
            "InvalidateEdge is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::SupersedeEdge { .. } => unreachable!(
            "SupersedeEdge is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
            // Intelligent overload backstop (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation), the edge-count
            // sibling of the `GetNodes` guard just above `try_handle`'s match:
            // check the cheap O(1) edge count BEFORE building the Vec, and
            // return a typed, catchable error instead of the pathological
            // gigabyte-scale frame. `GetEdgesPage` (bounded pagination) is
            // intentionally unaffected.
            if let Some(msg) = oversize_edge_dump_error(g.edge_count(), max_response_edges()) {
                return Response::err(req_id, msg);
            }
            Response::ok(req_id, ResultPayload::EdgeList(g.get_edges()))
        }
        Method::GetEdgesPage { after, limit } => {
            let g = &*core;
            let after_ref = after
                .as_ref()
                .map(|(s, t, ord)| (s.as_str(), t.as_str(), *ord));
            let edges = g.get_edges_page(after_ref, limit);
            Response::ok(req_id, ResultPayload::raw(&edges))
        }
        Method::GetEdgeProperties {
            source_id,
            target_id,
        } => {
            let g = &*core;
            let props = g.get_edge_properties(&source_id, &target_id);
            let val: Vec<serde_json::Value> = props
                .into_iter()
                .map(|p| {
                    eg_types::msgpack::decode_property_value(&p).unwrap_or(serde_json::json!({}))
                })
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
        // ClearGraph (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED — see
        // the AddNode/RemoveNode comment above.
        Method::ClearGraph => unreachable!(
            "ClearGraph is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
        // EvictLRU/DecaySweep/TouchNodes/FromMsgpack/Reconcile (CONCEPT:EG-P0-2
        // bypass guard, L11): GATEWAY_ROUTED — see the AddNode/RemoveNode
        // comment above. `ToMsgpack` is a pure read and keeps its normal arm.
        Method::EvictLRU { .. } => unreachable!(
            "EvictLRU is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::DecaySweep { .. } => unreachable!(
            "DecaySweep is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::TouchNodes { .. } => unreachable!(
            "TouchNodes is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::ToMsgpack => {
            let g = &*core;
            match g.to_msgpack() {
                Ok(json) => Response::ok(req_id, ResultPayload::Json(serde_json::json!(json))),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        Method::FromMsgpack { .. } => unreachable!(
            "FromMsgpack is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::Reconcile { .. } => unreachable!(
            "Reconcile is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        // ApplyMutation carries a SPARQL UPDATE string (governance / CDC mutation).
        // Replaced the legacy naive `{ <s> <p> <o> }` string-split shim with the REAL
        // SPARQL 1.1 UPDATE executor (CONCEPT:EG-KG.query.named-graph-support): a full spargebra parse + the
        // native merge-aware property-graph write ops (INSERT/DELETE DATA, DELETE/INSERT
        // … WHERE, CLEAR/CREATE/DROP GRAPH). Single-graph: every graph term routes to the
        // request graph's core (true named-graph routing lives on the /sparql endpoint,
        // which has the registry). `event_type` is now advisory (the query is
        // self-describing). Gated `sparql`; a non-sparql build rejects it explicitly.
        // ApplyMutation/RunDatalogReasoning (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::ApplyMutation { .. } => unreachable!(
            "ApplyMutation is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "reasoning")]
        Method::RunDatalogReasoning { .. } => unreachable!(
            "RunDatalogReasoning is mutation::GATEWAY_ROUTED; dispatch_graph_op \
             must route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
        Method::GetNeighborsBatch { node_ids } => {
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
            // [node_id, Vec<neighbor_id>] in input order — one round-trip and one
            // topo-lock acquisition for N nodes (D-DPF-1) instead of N of each.
            let out = g.get_neighbors_batch(node_ids);
            Response::ok(req_id, ResultPayload::raw(&out))
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
        // PruneByLifecycle (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED —
        // see the AddNode/RemoveNode comment above.
        Method::PruneByLifecycle { .. } => unreachable!(
            "PruneByLifecycle is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
        // BatchUpdate (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::BatchUpdate { .. } => unreachable!(
            "BatchUpdate is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::Vf2SubgraphMatch {
            pattern_graph_name,
            max_results,
            max_steps,
        } => {
            let s = state.read().await;
            // The pattern graph is read too — gate it like any other read.
            if let Some(entry) = s.registry.get(&pattern_graph_name) {
                if let Err(denied) = check_graph_access(
                    &s.isolation,
                    read_authority.actor(),
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
                let p_core = read_authority.project_core(&p_core);
                let p_snap = p_core.analysis_snapshot();
                // vf2_subgraph_match snapshots the host internally, so the
                // NP-hard backtracking (bounded by max_results/max_steps) runs
                // entirely off-lock.
                let host = core.clone();
                match compute_off_lock(req_id, move || {
                    host.vf2_subgraph_match(&p_snap, max_results, max_steps)
                })
                .await
                {
                    Ok((matches, truncated)) => Response::ok(
                        req_id,
                        ResultPayload::raw(&Vf2MatchResult { matches, truncated }),
                    ),
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

        // ClearLedger/ApplyLedger (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::ClearLedger => unreachable!(
            "ClearLedger is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::ApplyLedger { .. } => unreachable!(
            "ApplyLedger is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::GetSubgraph { node_ids } => {
            // Batched subgraph read: return the induced nodes (with DECODED
            // properties) and the edges among them in ONE round-trip, so callers
            // never loop per-node `GetNodeProperties` or pull the whole edge set.
            // (Previously serialized to msgpack then mis-parsed as JSON → error.)
            let g = &*core;
            let sub = g.get_subgraph(&node_ids);
            let mut nodes = Vec::with_capacity(sub.node_properties.len());
            for (id, blob) in &sub.node_properties {
                let props = eg_types::msgpack::decode_property_value(blob)
                    .unwrap_or(serde_json::Value::Null);
                nodes.push(serde_json::json!({ "id": id, "properties": props }));
            }
            let mut edges = Vec::new();
            for ((src, tgt), blobs) in &sub.edge_properties {
                for blob in blobs {
                    let props = eg_types::msgpack::decode_property_value(blob)
                        .unwrap_or(serde_json::Value::Null);
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
            let cores = match resolve_union_cores(state, read_authority, &graphs).await {
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
            let cores = match resolve_union_cores(state, read_authority, &graphs).await {
                Ok(c) => c,
                Err(denied) => return Response::err(req_id, denied),
            };
            let mut seen = std::collections::HashSet::new();
            let mut nodes: Vec<(String, serde_json::Value)> = Vec::new();
            'outer: for c in &cores {
                for (k, p) in c.get_nodes_by_label(&label, limit) {
                    if seen.insert(k.clone()) {
                        let val = eg_types::msgpack::decode_property_value(&p)
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
            let cores = match resolve_union_cores(state, read_authority, &graphs).await {
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
                read_authority.actor(),
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
            let other_core = read_authority.project_core(&other_core);
            let other_snap = { other_core.analysis_snapshot() };
            let g1 = &*core;
            let diff_str = g1.diff_against(&other_snap);
            match serde_json::from_slice::<serde_json::Value>(diff_str.as_bytes()) {
                Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                Err(e) => Response::err(req_id, e.to_string()),
            }
        }
        // CompactNodesByType (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED
        // — see the AddNode/RemoveNode comment above.
        Method::CompactNodesByType { .. } => unreachable!(
            "CompactNodesByType is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
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
    fn zero_cap_fails_safe() {
        assert!(oversize_dump_error(1, 0).is_some());
    }

    // CONCEPT:EG-KG.ingest.resets-socket-so-assimilation — the GetEdges overload backstop decision logic
    // (edge-count sibling of the GetNodes tests above).

    #[test]
    fn edges_under_cap_returns_no_error_so_data_is_served() {
        assert_eq!(oversize_edge_dump_error(0, 50_000), None);
        assert_eq!(oversize_edge_dump_error(1, 50_000), None);
        assert_eq!(oversize_edge_dump_error(49_999, 50_000), None);
        // Exactly at the cap is allowed (the guard fires only when EXCEEDED).
        assert_eq!(oversize_edge_dump_error(50_000, 50_000), None);
    }

    #[test]
    fn edges_over_cap_returns_typed_error_not_a_giant_payload() {
        let err = oversize_edge_dump_error(166_000, 50_000)
            .expect("over-cap dump must produce an error, not the data");
        assert!(err.starts_with("RESULT_TOO_LARGE"), "got: {err}");
        // The message must steer the caller to the bounded alternative.
        assert!(err.contains("GetEdgesPage"), "got: {err}");
        assert!(
            err.contains("166000") && err.contains("50000"),
            "got: {err}"
        );
    }

    #[test]
    fn edges_zero_cap_fails_safe() {
        assert!(oversize_edge_dump_error(1, 0).is_some());
    }
}
