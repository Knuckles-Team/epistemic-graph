use super::*;

use super::gateway::{commit_gateway, commit_gateway_coalescable};

/// Decode a MessagePack-encoded JSON object blob (the `props_msgpack` /
/// `semantic_props_msgpack` wire fields) into a `serde_json` object map
/// (CONCEPT:EG-KG.memory.eg-batch-decay-caller). A missing/undecodable/non-object blob yields an empty map, so
/// a caller may omit props entirely — the eg-core primitive injects the structural
/// markers regardless. Mirrors the `CompareAndSetNodeFields` blob-decode discipline.
pub(super) fn decode_json_object(blob: &[u8]) -> serde_json::Map<String, serde_json::Value> {
    eg_types::msgpack::decode_property_object(blob).unwrap_or_default()
}

/// BUG-193: stamp the BUG-052/GOC-61 canonical `_owner_id` onto a brand-new
/// node's property blob when the caller did not already supply an ownership
/// key, so a genuinely natively-written row is no longer permanently unowned
/// (the gap BUG-193 names: `_owner`/`isolation.rs`'s own native key had no
/// production writer at all). Mutating `method` HERE — before either arm below
/// clones it for the durable-commit path AND the `apply` closure — keeps the
/// persisted blob and the applied RAM blob byte-identical, which is required
/// for crash-recovery replay to reproduce the same row. Scoped to the two
/// methods that hand the gateway a caller-supplied node property blob directly
/// (`AddNode`, `CreateNodeIfAbsent`); the coalescable batch surface
/// (`BatchUpdate`) and the staged Cypher/RDF/GraphQL write path are explicitly
/// OUT of scope for this lane — see `plans/graph-os-completion-program/
/// BUG-LEDGER.md#BUG-193` for the documented boundary. A `System`-role caller
/// (bootstrap/migration/internal maintenance) is exempt: it is not a real
/// per-agent owner and must not be stamped as one. An absent `caller`
/// (state-machine-authorized replicated apply) is likewise exempt — there is no
/// per-request identity to stamp with there. Pure extract-method from
/// `try_handle_gateway`'s preamble, byte-identical behaviour, no signature
/// change.
#[cfg(feature = "security")]
pub(super) fn stamp_owner_id_if_applicable(
    method: Method,
    caller: Option<&str>,
    isolation: &crate::isolation::IsolationLayer,
) -> Method {
    // Flat guard clauses rather than nested `if`s: each exemption below returns
    // `method` untouched, which is byte-for-byte what the nested form did by
    // falling out of its `if` chain. Same conditions, same order, same result --
    // only the nesting depth (and so the cognitive complexity) differs.
    if !matches!(
        method,
        Method::AddNode { .. } | Method::CreateNodeIfAbsent { .. }
    ) {
        return method;
    }
    // An absent `caller` (state-machine-authorized replicated apply) has no
    // per-request identity to stamp with.
    let Some(caller_id) = caller else {
        return method;
    };
    // A `System`-role caller is not a real per-agent owner.
    if isolation.is_system(caller_id) {
        return method;
    }
    let blob = match &method {
        Method::AddNode {
            properties_msgpack, ..
        }
        | Method::CreateNodeIfAbsent {
            properties_msgpack, ..
        } => properties_msgpack,
        _ => unreachable!("matched above"),
    };
    // Already owned (or unstampable) => leave the blob exactly as the caller sent it.
    let Some(stamped) = crate::isolation::stamp_owner_id_if_absent(blob, caller_id) else {
        return method;
    };
    let mut method = method;
    match &mut method {
        Method::AddNode {
            properties_msgpack, ..
        }
        | Method::CreateNodeIfAbsent {
            properties_msgpack, ..
        } => *properties_msgpack = stamped,
        _ => unreachable!("matched above"),
    }
    method
}

/// Eviction changes RAM RESIDENCY ONLY — never durable content.
///
/// It used to run `core.evict_lru()` on the gateway's STAGED copy. The gateway
/// then diffs `base_snapshot` (the live core, which holds the node) against the
/// staged image (which no longer does) and publishes that delta, so every
/// eviction durably DELETED exactly the rows it evicted. That is silent data
/// loss, and it made the read-through seam unreachable by construction:
/// `read_node_blocking`'s own contract says "eviction is durability-gated ... so
/// an evicted node is always served here", and both
/// `delete_then_recreate_same_name_keeps_new_writes` and
/// `evicted_graph_lazy_reopens_with_data_intact` assert the row survives.
/// Measured directly: read_node_blocking returned Some(4) before an EvictLRU and
/// None immediately after it.
///
/// So the staged image is left UNTOUCHED (an eviction is not a content change,
/// so the correct delta is the empty one), and the residency change is applied
/// to the LIVE core afterwards — durability-gated exactly like the background
/// evictor in `persist::evict_oversized_all`, which never had this bug because
/// it never went through the gateway. The gateway call stays so the op keeps its
/// `node:admin` authz, its fencing, and its version/plan semantics. Pure
/// extract-method from `try_handle_gateway`'s `EvictLRU` arm: the original early
/// `return Ok(response)` on a failed staged commit is equivalent to this
/// function returning `response` directly, since nothing ran after the match in
/// the caller besides wrapping the arm's value in `Ok(..)`.
async fn apply_evict_lru(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    max_nodes: usize,
) -> Response {
    let response = commit_gateway(ctx, plan, method, move |_staged| {
        Ok(ResultPayload::Json(serde_json::json!(0)))
    })
    .await;
    if response.error.is_some() {
        return response;
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

/// `ApplyMutation` (SPARQL UPDATE): pure extract-method from `try_handle_gateway`'s
/// closure, byte-identical behaviour, no signature change.
fn apply_apply_mutation(
    core: &GraphCore,
    event_type: String,
    query: String,
) -> Result<ResultPayload, String> {
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
            let update_core =
                std::sync::Arc::new(GraphCore::from_snapshot(core.snapshot(), core.version())?);
            let store = SingleCoreStore(update_core.clone());
            let guard = crate::server::icv_guard::CoreIcvGuard::single(update_core.as_ref());
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
}

/// `RunDatalogReasoning`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
///
/// Gated on `reasoning`: `crate::reasoning` (see `lib.rs`, `#[cfg(feature =
/// "reasoning")] pub use eg_compute::reasoning;`) exists only under that feature, and
/// this function's only caller is the `#[cfg(feature = "reasoning")]
/// Method::RunDatalogReasoning` arm in `try_handle_gateway` below. Same shape as
/// BUG-CX-104 / the `dispatch.rs` fix (commit `bc280437`): a function with no cfg of
/// its own reaching a cfg-gated module. Without this gate, `cargo check
/// --no-default-features --features server` fails E0433 on every `crate::reasoning::*`
/// call in this body, even though the function is unreachable in that build.
#[cfg(feature = "reasoning")]
#[allow(clippy::too_many_arguments)]
fn apply_run_datalog_reasoning(
    core: &GraphCore,
    subclass_relations: Vec<(String, String)>,
    subproperty_relations: Vec<(String, String)>,
    symmetric_properties: Vec<String>,
    transitive_properties: Vec<String>,
    inverse_properties: Vec<(String, String)>,
    domain_rules: Vec<(String, String)>,
    range_rules: Vec<(String, String)>,
    property_chains: Vec<(String, String, String)>,
) -> Result<ResultPayload, String> {
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
}

/// `ClaimNext`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
fn apply_claim_next(
    core: &GraphCore,
    label: &str,
    updates_msgpack: &[u8],
) -> Result<ResultPayload, String> {
    let updates = match eg_types::msgpack::decode_property_object(updates_msgpack) {
        Ok(m) => m,
        Err(_) => return ResultPayload::raw(&Option::<(String, serde_json::Value)>::None),
    };
    let claimed = core.claim_next_fields(label, &updates);
    ResultPayload::raw(&claimed)
}

/// `AddSceneObject`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
fn apply_add_scene_object(
    core: &GraphCore,
    pose_msgpack: &[u8],
    parent: Option<&str>,
) -> Result<ResultPayload, String> {
    let Some(pose) = decode_pose(pose_msgpack) else {
        return Err("AddSceneObject: undecodable pose_msgpack".to_string());
    };
    let id = core.add_scene_object(&pose, parent);
    Ok(ResultPayload::String(id))
}

/// `SetPose`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
fn apply_set_pose(
    core: &GraphCore,
    node_id: &str,
    pose_msgpack: &[u8],
) -> Result<ResultPayload, String> {
    let Some(pose) = decode_pose(pose_msgpack) else {
        return Err("SetPose: undecodable pose_msgpack".to_string());
    };
    let ok = core.set_pose(node_id, &pose);
    Ok(ResultPayload::Bool(ok))
}

/// `AddEmbedding`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
fn apply_add_embedding(
    core: &GraphCore,
    node_id: String,
    embedding: Vec<f32>,
) -> Result<ResultPayload, String> {
    let source_version = core.version();
    core.semantic_store
        .write()
        .add_embedding(node_id, embedding)
        .map_err(|error| error.to_string())?;
    // Content-derived indexes are unchanged by a vector-only mutation, but their
    // completeness manifest must advance with the graph version that
    // commit_finalize publishes.
    core.maintain_indexes_at(
        &crate::index::ChangeSet::new(),
        source_version.saturating_add(1),
        core.node_count(),
        core.edge_count(),
    );
    Ok(ResultPayload::String("ok".to_string()))
}

/// `SupersedeEdge`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
#[allow(clippy::too_many_arguments)]
fn apply_supersede_edge(
    core: &GraphCore,
    source_id: String,
    target_id: String,
    properties_msgpack: Vec<u8>,
    prior_source: String,
    prior_target: String,
    prior_relationship: String,
    valid_at: u64,
    tx_now: u64,
) -> Result<ResultPayload, String> {
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
}

/// `BatchUpdate`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
fn apply_batch_update(
    core: &GraphCore,
    operations_msgpack: &[u8],
) -> Result<ResultPayload, String> {
    match crate::algorithms::batch_update(core, operations_msgpack) {
        Ok(res) => eg_types::msgpack::decode_property_value(&res)
            .map(ResultPayload::Json)
            .map_err(|_| "Invalid batch result".to_string()),
        Err(e) => Err(e),
    }
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

pub(super) async fn try_handle(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let resp = match method {
        Method::AddNode { .. } => {
            let owned_method = method.clone();
            commit_gateway_coalescable(ctx, plan, method, move |core| {
                mutation::apply_coalescable_write(core, &owned_method)
            })
            .await
        }
        Method::CreateNodeIfAbsent {
            node_id,
            properties_msgpack,
        } => {
            let (node_id, properties_msgpack) = (node_id.clone(), properties_msgpack.clone());
            commit_gateway(ctx, plan, method, move |core| {
                Ok(ResultPayload::Bool(
                    core.create_node_if_absent(node_id, properties_msgpack),
                ))
            })
            .await
        }
        Method::RemoveNode { .. } => {
            let owned_method = method.clone();
            commit_gateway_coalescable(ctx, plan, method, move |core| {
                mutation::apply_coalescable_write(core, &owned_method)
            })
            .await
        }
        Method::AddEdge { .. } => {
            let owned_method = method.clone();
            commit_gateway_coalescable(ctx, plan, method, move |core| {
                mutation::apply_coalescable_write(core, &owned_method)
            })
            .await
        }
        Method::RemoveEdge { .. } => {
            let owned_method = method.clone();
            commit_gateway_coalescable(ctx, plan, method, move |core| {
                mutation::apply_coalescable_write(core, &owned_method)
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
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
                let existed = core.reinforce(&node_id, now_ms, weight);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        // ── L11 rollout batch 2 (EG-P0-2 continued): graph-core family ──
        Method::CompareAndSetNodeFields { .. } => {
            let owned_method = method.clone();
            commit_gateway_coalescable(ctx, plan, method, move |core| {
                mutation::apply_coalescable_write(core, &owned_method)
            })
            .await
        }
        Method::ClaimNext {
            label,
            updates_msgpack,
        } => {
            let (label, updates_msgpack) = (label.clone(), updates_msgpack.clone());
            commit_gateway(ctx, plan, method, move |core| {
                apply_claim_next(core, &label, &updates_msgpack)
            })
            .await
        }
        Method::DecayNode {
            node_id,
            now_ms,
            half_life_ms,
        } => {
            let (node_id, now_ms, half_life_ms) = (node_id.clone(), *now_ms, *half_life_ms);
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
                let out = core.maintain(&ids, now_ms, half_life_ms, evict_threshold, delete);
                ResultPayload::raw(&out)
            })
            .await
        }
        Method::AddSceneObject {
            pose_msgpack,
            parent,
        } => {
            let (pose_msgpack, parent) = (pose_msgpack.clone(), parent.clone());
            commit_gateway(ctx, plan, method, move |core| {
                apply_add_scene_object(core, &pose_msgpack, parent.as_deref())
            })
            .await
        }
        Method::SetPose {
            node_id,
            pose_msgpack,
        } => {
            let (node_id, pose_msgpack) = (node_id.clone(), pose_msgpack.clone());
            commit_gateway(ctx, plan, method, move |core| {
                apply_set_pose(core, &node_id, &pose_msgpack)
            })
            .await
        }
        Method::Reparent {
            node_id,
            new_parent,
        } => {
            let (node_id, new_parent) = (node_id.clone(), new_parent.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let ok = core.reparent(&node_id, new_parent.as_deref());
                Ok(ResultPayload::Bool(ok))
            })
            .await
        }
        Method::StartTrajectory { props_msgpack } => {
            let props_msgpack = props_msgpack.clone();
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
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
                ResultPayload::raw(&step_id)
            })
            .await
        }
        Method::AddEmbedding { node_id, embedding } => {
            let (node_id, embedding) = (node_id.clone(), embedding.clone());
            commit_gateway(ctx, plan, method, move |core| {
                apply_add_embedding(core, node_id, embedding)
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
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
                apply_supersede_edge(
                    core,
                    source_id,
                    target_id,
                    properties_msgpack,
                    prior_source,
                    prior_target,
                    prior_relationship,
                    valid_at,
                    tx_now,
                )
            })
            .await
        }
        Method::ClearGraph => {
            commit_gateway(ctx, plan, method, move |core| {
                core.clear();
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        Method::EvictLRU { max_nodes } => apply_evict_lru(ctx, plan, method, *max_nodes).await,
        Method::DecaySweep {
            half_life_secs,
            floor,
            prune,
        } => {
            let (half_life_secs, floor, prune) = (*half_life_secs, *floor, *prune);
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
                core.from_msgpack(&msgpack)
                    .map(|()| {
                        ResultPayload::scalar::<eg_types::result_contract::storage::FromMsgpack>(
                            "ok".to_string(),
                        )
                    })
                    .map_err(|e| e.to_string())
            })
            .await
        }
        Method::Reconcile { msgpack, .. } => {
            let msgpack = msgpack.clone();
            commit_gateway(ctx, plan, method, move |core| {
                core.from_msgpack(&msgpack)
                    .map(|()| ResultPayload::String("reconciled".to_string()))
                    .map_err(|e| e.to_string())
            })
            .await
        }
        Method::ApplyMutation { event_type, query } => {
            let (event_type, query) = (event_type.clone(), query.clone());
            commit_gateway(ctx, plan, method, move |core| {
                apply_apply_mutation(core, event_type, query)
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
            commit_gateway(ctx, plan, method, move |core| {
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
            commit_gateway(ctx, plan, method, move |core| {
                apply_run_datalog_reasoning(
                    core,
                    subclass_relations,
                    subproperty_relations,
                    symmetric_properties,
                    transitive_properties,
                    inverse_properties,
                    domain_rules,
                    range_rules,
                    property_chains,
                )
            })
            .await
        }
        Method::PruneByLifecycle {
            max_age_secs,
            min_score,
        } => {
            let (max_age_secs, min_score) = (*max_age_secs, *min_score);
            commit_gateway(ctx, plan, method, move |core| {
                let stats = crate::algorithms::prune_by_lifecycle(core, max_age_secs, min_score);
                serde_json::to_value(&stats)
                    .map(ResultPayload::Json)
                    .map_err(|e| e.to_string())
            })
            .await
        }
        Method::BatchUpdate { operations_msgpack } => {
            let operations_msgpack = operations_msgpack.clone();
            commit_gateway(ctx, plan, method, move |core| {
                apply_batch_update(core, &operations_msgpack)
            })
            .await
        }
        Method::ClearLedger => {
            commit_gateway(ctx, plan, method, move |core| {
                core.clear_ledger();
                Ok(ResultPayload::scalar::<
                    eg_types::result_contract::storage::ClearLedger,
                >("ok".to_string()))
            })
            .await
        }
        Method::ApplyLedger { transactions } => {
            let transactions = transactions.clone();
            commit_gateway(ctx, plan, method, move |core| {
                core.apply_ledger(transactions).map(|()| {
                    ResultPayload::scalar::<eg_types::result_contract::storage::ApplyLedger>(
                        "ok".to_string(),
                    )
                })
            })
            .await
        }
        Method::CompactNodesByType {
            node_type,
            threshold,
        } => {
            let (node_type, threshold) = (node_type.clone(), *threshold);
            commit_gateway(ctx, plan, method, move |core| {
                let removed = core.compact_nodes_by_type(&node_type, threshold);
                Ok(ResultPayload::Json(
                    serde_json::json!({ "removed_nodes": removed }),
                ))
            })
            .await
        }
        _ => return None,
    };
    Some(resp)
}
