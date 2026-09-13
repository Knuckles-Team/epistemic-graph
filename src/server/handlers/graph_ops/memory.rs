use super::*;

use super::terminal::GraphOpsContext;

/// `ToMsgpack`: pure extract-method from `try_handle`'s match arm, byte-identical
/// behaviour, no signature change.
fn handle_to_msgpack(req_id: u64, core: &Arc<GraphCore>) -> Response {
    let g = &**core;
    Response::ok(
        req_id,
        g.to_msgpack()
            .and_then(ResultPayload::of::<eg_types::result_contract::storage::ToMsgpack>),
    )
}

/// Route memory maintenance operations.
pub(super) async fn try_handle_memory_maintenance(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let _ = ctx;
    match method {
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
        other => ControlFlow::Continue(other),
    }
}

/// Handle summary hierarchy reads.
pub(super) async fn try_handle_summary_reads(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::SummaryChildren { node_id } => {
            Response::ok(req_id, ResultPayload::Ids(core.summary_children(&node_id)))
        }
        Method::SummariesAtLevel { level } => {
            Response::ok(req_id, ResultPayload::Ids(core.summaries_at_level(level)))
        }
        // AddSceneObject/SetPose/Reparent (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        other => return ControlFlow::Continue(other),
    })
}

/// Handle scene graph operations.
pub(super) async fn try_handle_scene_graph(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
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
        other => return ControlFlow::Continue(other),
    })
}

/// Handle trajectory memory operations.
pub(super) async fn try_handle_trajectory_memory(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
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
        other => return ControlFlow::Continue(other),
    })
}

/// Route lifecycle mutations and handle graph serialization.
pub(super) async fn try_handle_lifecycle_serialization(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
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
            Method::ToMsgpack => handle_to_msgpack(req_id, core),
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
        other => return ControlFlow::Continue(other),
    })
}

/// Handle lifecycle and context operations.
pub(super) async fn try_handle_lifecycle_context(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext {
        state,
        req_id,
        read_authority,
        core,
        ..
    } = ctx;
    ControlFlow::Break(match method {
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
            super::subgraph::handle_vf2_subgraph_match(
                state,
                req_id,
                read_authority,
                core,
                pattern_graph_name,
                max_results,
                max_steps,
            )
            .await
        }
        // BUG A1 (2026-08-12): this used to read `core.get_ledger()` off the
        // RLS-projected `core` shadowed above (`read_authority.project_core`),
        // whose detached copy is built via `add_node_no_ledger`/
        // `add_edge_no_ledger` (`access.rs::build_projection`'s own doc: "there
        // is no longer a ledger to clear") and therefore NEVER carries a
        // ledger. Because `security` (hence `GraphReadAuthority::is_active()`)
        // is compiled into the default `full` build, that made `GetLedger`
        // return `[]` on EVERY request in production, indistinguishable from
        // "nothing to sync" — and `agent_utilities.workflows.epistemic_sync`'s
        // `flush_ledger_to_backend` (a real production sync path) silently
        // flushed nothing as a result.
        //
        // The mutation ledger is process-observability, not row-visible data
        // (same reasoning `raw_ledger_len` above already established for
        // `Metrics.total_mutations`), and it is authorized by its own
        // dedicated `ledger:read` RBAC action (`eg_capabilities::policy`),
        // enforced upstream in `dispatch.rs` (`verified_context.allows_method`)
        // BEFORE any handler runs — so routing it through row-level RLS here
        // was a redundant SECOND gate that, instead of narrowing visibility,
        // destroyed the data outright. It now reads `raw_core` (captured
        // before the projection, for the identical reason `raw_ledger_len`
        // was) and is classified `NON_ROW_SCOPED` in access.rs's read-method
        // audit table, not `RLS_ROUTED`.
        //
        // The response is a typed `LedgerReadResult`, not a bare array, so
        // "genuinely empty" and "could not be read for this scope" can never
        // collapse into the same indistinguishable `[]` again.
        //
        // BUG A1 follow-up (2026-08-12): fixing the query above is NOT
        // sufficient on its own — `raw_core.get_ledger()` is a purely
        // IN-MEMORY, capped ring (`GraphCore::push_ledger`'s doc), not part
        // of the durable path at all. Cold-tenant idle offload/hibernate,
        // `MAX_RESIDENT_GRAPHS` eviction + lazy rehydrate, a process
        // restart, or simply exceeding the cap can all empty or truncate it
        // while the underlying mutations remain fully durable in redb — a
        // SEPARATE, real gap from the RLS-projection bug above, of the same
        // "ephemeral buffer callers assume is durable" shape `CdcHub`
        // (`src/server/cdc.rs`) also has. `watermark` is what makes that
        // honest: it is the sequence of the oldest entry `entries` can
        // vouch for, so a caller comparing it across reads can detect
        // truncation instead of inferring completeness from a merely
        // nonzero read.
        other => return ControlFlow::Continue(other),
    })
}

/// Handle mutation ledger reads and gateway-owned ledger writes.
pub(super) async fn try_handle_ledger(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext {
        req_id, raw_core, ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::GetLedger => Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!(LedgerReadResult::populated(
                raw_core.get_ledger(),
                raw_core.ledger_watermark(),
            ))),
        ),

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
        other => return ControlFlow::Continue(other),
    })
}
