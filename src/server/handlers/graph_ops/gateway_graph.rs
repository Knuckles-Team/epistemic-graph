use super::*;

use super::gateway::commit_gateway;

#[path = "gateway_graph_routes.rs"]
mod gateway_graph_routes;

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
            ResultPayload::of::<eg_types::result_contract::graph::ApplyMutation>(
                eg_types::result_contract::transactions::SparqlUpdateReport {
                    operations: report.operations as u64,
                    inserted: report.inserted as u64,
                    deleted: report.deleted as u64,
                    updated_graphs: 1,
                    created_graphs: 0,
                },
            )
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
/// byte-identical behaviour.
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
struct DatalogReasoningInput {
    subclass_relations: Vec<(String, String)>,
    subproperty_relations: Vec<(String, String)>,
    symmetric_properties: Vec<String>,
    transitive_properties: Vec<String>,
    inverse_properties: Vec<(String, String)>,
    domain_rules: Vec<(String, String)>,
    range_rules: Vec<(String, String)>,
    property_chains: Vec<(String, String, String)>,
}

#[cfg(feature = "reasoning")]
fn apply_run_datalog_reasoning(
    core: &GraphCore,
    input: DatalogReasoningInput,
) -> Result<ResultPayload, String> {
    let mut all_inferred: Vec<std::collections::HashMap<String, String>> = Vec::new();
    match crate::reasoning::run_datalog_reasoning(
        core,
        input.subclass_relations,
        input.subproperty_relations,
        input.symmetric_properties,
        input.transitive_properties,
        input.inverse_properties,
    ) {
        Ok(triples) => all_inferred.extend(triples),
        Err(e) => return Err(e),
    }
    if !input.domain_rules.is_empty() || !input.range_rules.is_empty() {
        all_inferred.extend(crate::reasoning::infer_domain_range(
            core,
            input.domain_rules,
            input.range_rules,
        ));
    }
    if !input.property_chains.is_empty() {
        all_inferred.extend(crate::reasoning::infer_property_chains(
            core,
            input.property_chains,
        ));
    }
    ResultPayload::of::<eg_types::result_contract::reasoning::RunDatalogReasoning>(
        eg_types::types::DatalogReasoningResult {
            inferred_count: all_inferred.len(),
            inferred_triples: all_inferred,
        },
    )
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
        Err(_) => {
            return ResultPayload::of::<eg_types::result_contract::coordination::ClaimNext>(None)
        }
    };
    let claimed = core.claim_next_fields(label, &updates);
    ResultPayload::of::<eg_types::result_contract::coordination::ClaimNext>(claimed)
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
    Ok(ResultPayload::scalar::<
        eg_types::result_contract::graph::AddSceneObject,
    >(id))
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
    Ok(ResultPayload::scalar::<
        eg_types::result_contract::graph::SetPose,
    >(ok))
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
/// byte-identical behaviour.
struct SupersedeEdgeInput {
    source_id: String,
    target_id: String,
    properties_msgpack: Vec<u8>,
    prior_source: String,
    prior_target: String,
    prior_relationship: String,
    valid_at: u64,
    tx_now: u64,
}

fn apply_supersede_edge(
    core: &GraphCore,
    input: SupersedeEdgeInput,
) -> Result<ResultPayload, String> {
    match core.supersede_edge(
        input.source_id,
        input.target_id,
        input.properties_msgpack,
        &input.prior_source,
        &input.prior_target,
        &input.prior_relationship,
        input.valid_at,
        input.tx_now,
    ) {
        Ok(()) => Ok(ResultPayload::scalar::<
            eg_types::result_contract::graph::SupersedeEdge,
        >("ok".to_string())),
        Err(e) => Err(e),
    }
}

/// `BatchUpdate`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
fn apply_batch_update(
    core: &GraphCore,
    operations_msgpack: &[u8],
) -> Result<ResultPayload, String> {
    let summary = crate::algorithms::batch_update(core, operations_msgpack)?;
    eg_types::result_contract::transactions::BatchUpdateReport::decode(&summary)
        .and_then(ResultPayload::of::<eg_types::result_contract::transactions::BatchUpdate>)
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
    gateway_graph_routes::try_handle(ctx, plan, method).await
}
