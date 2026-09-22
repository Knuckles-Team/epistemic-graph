use super::*;

use super::gateway::{commit_gateway, commit_gateway_coalescable};

#[derive(Clone, Copy)]
enum Route {
    Coalescable,
    NodeState,
    Memory,
    Scene,
    Trajectory,
    Edges,
    Lifecycle,
    Import,
    #[cfg(any(feature = "shacl", feature = "reasoning"))]
    Policy,
    Analytics,
    Other,
}

#[derive(Clone, Copy)]
enum RouteHandler {
    Core,
    Admin,
    None,
}

fn is_node_state(method: &Method) -> bool {
    is_node_creation(method) || is_node_memory_update(method)
}

fn is_node_creation(method: &Method) -> bool {
    matches!(
        method,
        Method::CreateNodeIfAbsent { .. } | Method::ClaimNext { .. }
    )
}

fn is_node_memory_update(method: &Method) -> bool {
    matches!(
        method,
        Method::CreateSummaryNode { .. } | Method::Consolidate { .. } | Method::Reinforce { .. }
    )
}

fn is_memory(method: &Method) -> bool {
    matches!(
        method,
        Method::DecayNode { .. }
            | Method::DecayMemories { .. }
            | Method::EvictBelow { .. }
            | Method::Maintain { .. }
    )
}

fn is_scene(method: &Method) -> bool {
    matches!(
        method,
        Method::AddSceneObject { .. }
            | Method::SetPose { .. }
            | Method::Reparent { .. }
            | Method::StartTrajectory { .. }
    )
}

fn is_trajectory(method: &Method) -> bool {
    matches!(
        method,
        Method::AppendStep { .. } | Method::AddEmbedding { .. }
    )
}

fn is_edges(method: &Method) -> bool {
    matches!(
        method,
        Method::InvalidateEdge { .. } | Method::SupersedeEdge { .. }
    )
}

fn is_lifecycle(method: &Method) -> bool {
    matches!(
        method,
        Method::ClearGraph
            | Method::EvictLRU { .. }
            | Method::DecaySweep { .. }
            | Method::TouchNodes { .. }
    )
}

fn is_import(method: &Method) -> bool {
    matches!(
        method,
        Method::FromMsgpack { .. } | Method::Reconcile { .. } | Method::ApplyMutation { .. }
    )
}

#[cfg(any(feature = "shacl", feature = "reasoning"))]
fn is_policy(method: &Method) -> bool {
    #[cfg(feature = "shacl")]
    if matches!(
        method,
        Method::IcvConfigure { .. } | Method::GraphSchema { .. }
    ) {
        return true;
    }
    #[cfg(feature = "reasoning")]
    if matches!(method, Method::RunDatalogReasoning { .. }) {
        return true;
    }
    false
}

fn is_analytics(method: &Method) -> bool {
    matches!(
        method,
        Method::PruneByLifecycle { .. }
            | Method::BatchUpdate { .. }
            | Method::ClearLedger
            | Method::ApplyLedger { .. }
            | Method::CompactNodesByType { .. }
    )
}

/// A predicate selecting the methods one gateway route owns.
type RouteMatcher = fn(&Method) -> bool;

const ROUTE_MATCHERS: &[(RouteMatcher, Route)] = &[
    (
        crate::server::mutation::is_coalescable_structural_write,
        Route::Coalescable,
    ),
    (is_node_state, Route::NodeState),
    (is_memory, Route::Memory),
    (is_scene, Route::Scene),
    (is_trajectory, Route::Trajectory),
    (is_edges, Route::Edges),
    (is_lifecycle, Route::Lifecycle),
    (is_import, Route::Import),
    #[cfg(any(feature = "shacl", feature = "reasoning"))]
    (is_policy, Route::Policy),
    (is_analytics, Route::Analytics),
];

fn route_for(method: &Method) -> Route {
    ROUTE_MATCHERS
        .iter()
        .find_map(|(matcher, route)| matcher(method).then_some(*route))
        .unwrap_or(Route::Other)
}

fn route_handler(route: Route) -> RouteHandler {
    match route {
        Route::Coalescable
        | Route::NodeState
        | Route::Memory
        | Route::Scene
        | Route::Trajectory => RouteHandler::Core,
        Route::Edges | Route::Lifecycle | Route::Import | Route::Analytics => RouteHandler::Admin,
        #[cfg(any(feature = "shacl", feature = "reasoning"))]
        Route::Policy => RouteHandler::Admin,
        Route::Other => RouteHandler::None,
    }
}

pub(super) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    tenant_id: &str,
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let route = route_for(method);
    match route_handler(route) {
        RouteHandler::Core => try_handle_core(ctx, plan, method, route).await,
        RouteHandler::Admin => try_handle_admin(state, tenant_id, ctx, plan, method, route).await,
        RouteHandler::None => None,
    }
}

async fn try_handle_core(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    route: Route,
) -> Option<Response> {
    match route {
        Route::Coalescable => try_handle_coalescable(ctx, plan, method).await,
        Route::NodeState => try_handle_node_state(ctx, plan, method).await,
        Route::Memory => try_handle_memory(ctx, plan, method).await,
        Route::Scene => try_handle_scene(ctx, plan, method).await,
        Route::Trajectory => try_handle_trajectory(ctx, plan, method).await,
        _ => None,
    }
}

async fn try_handle_admin(
    state: &Arc<RwLock<ServerState>>,
    tenant_id: &str,
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    route: Route,
) -> Option<Response> {
    match route {
        Route::Edges => try_handle_edges(ctx, plan, method).await,
        Route::Lifecycle => try_handle_lifecycle(ctx, plan, method).await,
        Route::Import => try_handle_import(ctx, plan, method).await,
        #[cfg(any(feature = "shacl", feature = "reasoning"))]
        Route::Policy => try_handle_policy(state, tenant_id, ctx, plan, method).await,
        Route::Analytics => try_handle_analytics(ctx, plan, method).await,
        _ => None,
    }
}

async fn try_handle_coalescable(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    if !matches!(
        method,
        Method::AddNode { .. }
            | Method::RemoveNode { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::CompareAndSetNodeFields { .. }
    ) {
        return None;
    }
    let owned_method = method.clone();
    Some(
        commit_gateway_coalescable(ctx, plan, method, move |core| {
            mutation::apply_coalescable_write(core, &owned_method)
        })
        .await,
    )
}

async fn try_handle_node_state(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
        _ => return None,
    };
    Some(response)
}

async fn try_handle_memory(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
                ResultPayload::of::<eg_types::result_contract::graph::Maintain>(out)
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}

async fn try_handle_scene(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
        _ => return None,
    };
    Some(response)
}

async fn try_handle_trajectory(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
                ResultPayload::of::<eg_types::result_contract::graph::AppendStep>(step_id)
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
        _ => return None,
    };
    Some(response)
}

async fn try_handle_edges(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
            let input = SupersedeEdgeInput {
                source_id: source_id.clone(),
                target_id: target_id.clone(),
                properties_msgpack: properties_msgpack.clone(),
                prior_source: prior_source.clone(),
                prior_target: prior_target.clone(),
                prior_relationship: prior_relationship.clone(),
                valid_at: *valid_at,
                tx_now: *tx_now,
            };
            commit_gateway(ctx, plan, method, move |core| {
                apply_supersede_edge(core, input)
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}

async fn try_handle_lifecycle(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
                ResultPayload::of::<eg_types::result_contract::graph::DecaySweep>(stats)
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
        _ => return None,
    };
    Some(response)
}

async fn try_handle_import(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
        _ => return None,
    };
    Some(response)
}

#[cfg(any(feature = "shacl", feature = "reasoning"))]
async fn try_handle_policy(
    state: &Arc<RwLock<ServerState>>,
    tenant_id: &str,
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
        #[cfg(feature = "shacl")]
        Method::GraphSchema { op } => {
            crate::server::graph_schema::handle_gateway(state, tenant_id, ctx, plan, method, op)
                .await
        }
        _ => return try_handle_reasoning_policy(ctx, plan, method).await,
    };
    Some(response)
}

/// The reasoning half of the policy route: rule programs, which are governed
/// by a different subsystem from the SHACL shapes and schema sources above and
/// share nothing with them but the commit gateway.
#[cfg(any(feature = "shacl", feature = "reasoning"))]
async fn try_handle_reasoning_policy(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
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
            let input = DatalogReasoningInput {
                schema_digests: Vec::new(),
                subclass_relations: subclass_relations.clone(),
                subproperty_relations: subproperty_relations.clone(),
                symmetric_properties: symmetric_properties.clone(),
                transitive_properties: transitive_properties.clone(),
                inverse_properties: inverse_properties.clone(),
                domain_rules: domain_rules.clone(),
                range_rules: range_rules.clone(),
                property_chains: property_chains
                    .iter()
                    .map(|(first, second, sup)| (vec![first.clone(), second.clone()], sup.clone()))
                    .collect(),
            };
            commit_gateway(ctx, plan, method, move |core| {
                apply_run_datalog_reasoning(core, input)
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}

async fn try_handle_analytics(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
        Method::PruneByLifecycle {
            max_age_secs,
            min_score,
        } => {
            let (max_age_secs, min_score) = (*max_age_secs, *min_score);
            commit_gateway(ctx, plan, method, move |core| {
                let stats = crate::algorithms::prune_by_lifecycle(core, max_age_secs, min_score);
                ResultPayload::of::<eg_types::result_contract::graph::PruneByLifecycle>(stats)
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
                ResultPayload::of::<eg_types::result_contract::graph::CompactNodesByType>(
                    eg_types::types::CompactNodesResult {
                        removed_nodes: removed,
                    },
                )
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}
