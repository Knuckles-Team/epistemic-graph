use super::*;

// ── EXPLAIN surfaces (CONCEPT:EG-KG.query.plan-dag, E5 phase 4) ──────────────
#[cfg(feature = "query")]
pub(crate) async fn handle_explain_plan(
    ctx: &QueryHandlerCtx<'_>,
    plan: eg_plan::Plan,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    let snap = explain_snapshot(
        &core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    // L34: reuse the served `UnifiedQuery` idiom instead of a per-request
    // `SemanticStore` clone — move the cheap `Arc<GraphCore>` clone into the
    // off-lock closure and take the read guard THERE, on the blocking pool. This
    // diagnostic surface is low-traffic, but the fix costs nothing (same clone
    // count: an `Arc` clone instead of a whole-store clone) so there is no reason
    // to keep the heavier path.
    let core_for_ctx = core.clone();
    let resp = match compute_off_lock(req_id, move || {
        let semantic = core_for_ctx.semantic_store.read();
        explain_plan(plan, &snap, &semantic)
    })
    .await
    {
        Ok(Ok(result)) => raw_response(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("ExplainPlan error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}

#[cfg(feature = "query")]
pub(crate) async fn handle_explain_provenance(
    ctx: &QueryHandlerCtx<'_>,
    plan: eg_plan::Plan,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    let snap = explain_snapshot(
        &core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    // L34: see `ExplainPlan` above — clone the cheap `Arc<GraphCore>` into the
    // closure and take the `SemanticStore` read guard inside it, instead of
    // cloning the whole store per request.
    let core_for_ctx = core.clone();
    let resp = match compute_off_lock(req_id, move || {
        let semantic = core_for_ctx.semantic_store.read();
        explain_provenance(req_id, plan, &snap, &semantic)
    })
    .await
    {
        Ok(Ok(result)) => raw_response(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("ExplainProvenance error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}

// CONCEPT:EG-KB-CURRENCY — ID-seeded sibling of `ExplainProvenance`: same
// per-row epistemic wire shape, no `Op` plan needed.
#[cfg(feature = "query")]
pub(crate) async fn handle_explain_provenance_by_ids(
    ctx: &QueryHandlerCtx<'_>,
    ids: Vec<String>,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    let snap = explain_snapshot(
        &core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    let core_for_ctx = core.clone();
    let resp = match compute_off_lock(req_id, move || {
        let semantic = core_for_ctx.semantic_store.read();
        explain_provenance_by_ids(req_id, ids, &snap, &semantic)
    })
    .await
    {
        Ok(Ok(result)) => raw_response(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("ExplainProvenanceByIds error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}

#[cfg(feature = "query")]
pub(crate) async fn handle_explain_policy(
    ctx: &QueryHandlerCtx<'_>,
    plan: eg_plan::Plan,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    // BOTH the unfiltered snapshot and the caller's RLS-filtered one, so the
    // diagnostic can report exactly which rows the policy denied (reuses the
    // SAME `IsolationLayer::filter_view` every other read path applies).
    let full = core.analysis_snapshot();
    let filtered = explain_snapshot(
        &core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    // L34: see `ExplainPlan` above — clone the cheap `Arc<GraphCore>` into the
    // closure and take the `SemanticStore` read guard inside it, instead of
    // cloning the whole store per request.
    let core_for_ctx = core.clone();
    let resp = match compute_off_lock(req_id, move || {
        let semantic = core_for_ctx.semantic_store.read();
        explain_policy(plan, &full, &filtered, &semantic)
    })
    .await
    {
        Ok(Ok(result)) => raw_response(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("ExplainPolicy error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}

// L51: redaction-aware arm. Handles BOTH `disclosure_level: None` (byte-for-
// byte the classic path below) AND `Some(_)` (routes through
// `eg_epistemic::redact::explain_belief_redacted_capped` under the caller's
// own RLS actor). Mutually exclusive with the arm below via `cfg` — exactly
// one of the two is ever compiled for a given `Method::ExplainBelief` pattern.
#[cfg(feature = "epistemic-redaction")]
pub(crate) async fn handle_explain_belief(
    ctx: &QueryHandlerCtx<'_>,
    node_id: String,
    disclosure_level: Option<crate::protocol::DisclosureLevelWire>,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    let snap = core.analysis_snapshot();
    let caller_id = caller.to_string();
    let rls = rls.clone();
    let resp = match disclosure_level {
        None => match compute_off_lock(req_id, move || explain_belief(&node_id, &snap)).await {
            Ok(result) => raw_response(req_id, &result),
            Err(resp) => resp,
        },
        Some(cap) => {
            match compute_off_lock(req_id, move || {
                explain_belief_redacted_wire(&node_id, &snap, cap, &rls, &caller_id)
            })
            .await
            {
                Ok(result) => raw_response(req_id, &result),
                Err(resp) => resp,
            }
        }
    };
    Ok(resp)
}

// Classic-only arm: compiled when `epistemic` is on but `epistemic-redaction`
// is off. `disclosure_level: Some(_)` gets an explicit error — never a silent
// fall-back to the un-redacted tree, which would leak exactly what redaction
// exists to hide.
#[cfg(all(feature = "epistemic", not(feature = "epistemic-redaction")))]
pub(crate) async fn handle_explain_belief(
    ctx: &QueryHandlerCtx<'_>,
    node_id: String,
    disclosure_level: Option<crate::protocol::DisclosureLevelWire>,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let core = ctx.core.clone();
    if disclosure_level.is_some() {
        return Ok(Response::err(
            req_id,
            "ExplainBelief.disclosure_level requires the epistemic-redaction \
                     feature, not enabled in this build"
                .to_string(),
        ));
    }
    let snap = core.analysis_snapshot();
    let resp = match compute_off_lock(req_id, move || explain_belief(&node_id, &snap)).await {
        Ok(result) => raw_response(req_id, &result),
        Err(resp) => resp,
    };
    Ok(resp)
}

// L53 (EPI-P3-5): the acceptance capstone.
#[cfg(feature = "epistemic-tms")]
pub(crate) async fn handle_epistemic_status(
    ctx: &QueryHandlerCtx<'_>,
    node_id: String,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let core = ctx.core.clone();
    let snap = core.analysis_snapshot();
    let resp = match compute_off_lock(req_id, move || epistemic_status_wire(&node_id, &snap)).await
    {
        Ok(result) => raw_response(req_id, &result),
        Err(resp) => resp,
    };
    Ok(resp)
}

// L53 (EPI-P3-5): the one facet not subsumed by `EpistemicStatus` — a
// whole-graph bitemporal diff between two transaction times.
#[cfg(feature = "epistemic-tms")]
pub(crate) async fn handle_what_changed(
    ctx: &QueryHandlerCtx<'_>,
    tx_from: u64,
    tx_to: u64,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let core = ctx.core.clone();
    let snap = core.analysis_snapshot();
    let resp =
        match compute_off_lock(req_id, move || what_changed_wire(&snap, tx_from, tx_to)).await {
            Ok(result) => raw_response(req_id, &result),
            Err(resp) => resp,
        };
    Ok(resp)
}

// Fenced recompute/writeback against the durable per-graph projection. The
// graph snapshot supplies current provenance; request data cannot choose the
// dependency set or generator persisted by the projection.
#[cfg(feature = "epistemic-tms")]
pub(crate) async fn handle_recompute_materialization(
    ctx: &QueryHandlerCtx<'_>,
    derived_id: String,
    expected_source_graph_version: u64,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    // `caller` is consumed only by the `raft`-gated replicated-apply fast path
    // below; keep it referenced in a non-`raft` build so no dead-param warning
    // fires (mirrors the SAME convention `try_handle`'s own preamble already
    // used for `state`/`read_authority`/`graph_name` before this extraction).
    #[cfg(not(feature = "raft"))]
    let _ = caller;
    let core = ctx.core.clone();
    #[cfg(feature = "raft")]
    if crate::server::dispatch::is_replicated_apply() {
        let method = Method::RecomputeMaterialization {
            derived_id: derived_id.clone(),
            expected_source_graph_version,
        };
        let persistence = state.read().await.persistence.clone();
        let Some(persistence) = persistence else {
            return Ok(Response::err(
                req_id,
                "reasoning recompute requires an authoritative MutationBatch backend",
            ));
        };
        let graph_fname = crate::persist::sanitize(graph_name);
        let authoritative_graph_version =
            match crate::server::mutation_batch::authoritative_graph_version(
                &persistence,
                &graph_fname,
                &core,
            )
            .await
            {
                Ok(version) => version,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
        if authoritative_graph_version != expected_source_graph_version {
            return Ok(Response::err(
                req_id,
                "STALE_RECOMPUTE_FENCE: authoritative graph version changed",
            ));
        }
        let Some(target_graph_version) = expected_source_graph_version.checked_add(1) else {
            return Ok(Response::err(
                req_id,
                "reasoning recompute graph version exhausted",
            ));
        };
        let result = crate::protocol::RecomputeMaterializationResult {
            id: eg_epistemic::projection_identity(&derived_id),
            depends_on: Vec::new(),
            generating_activity: None,
            status: "Queued".to_string(),
            source_graph_version: target_graph_version,
            fence_epoch: 0,
            projection_pending: true,
        };
        let payload = match ResultPayload::raw(&result) {
            Ok(payload) => payload,
            Err(error) => return Ok(Response::err(req_id, error)),
        };
        let batch_id = crate::server::mutation_batch::opaque_request_key(
            "reasoning-recompute",
            graph_name,
            req_id,
            &method,
        );
        if let Err(error) = crate::server::mutation_batch::commit_internal_graph_methods(
            crate::server::mutation_batch::InternalGraphCommitRequest::new(
                Some(&persistence),
                &core,
                req_id,
                Some(caller),
                graph_name,
                &batch_id,
                vec![method],
                &payload,
            ),
        )
        .await
        {
            return Ok(Response::err(req_id, error));
        }
        return Ok(Response::ok(req_id, payload));
    }
    let authoritative_graph_version = core.version();
    let snap = core.analysis_snapshot();
    let persist_dir = state.read().await.persist_dir.clone();
    let (materialization, fence_epoch) =
        match crate::server::reasoning_projection::recompute_materialization(
            persist_dir.as_deref(),
            graph_name,
            snap,
            authoritative_graph_version,
            &derived_id,
            expected_source_graph_version,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => return Ok(Response::err(req_id, error)),
        };
    let result = crate::protocol::RecomputeMaterializationResult {
        id: materialization.materialization_ref,
        depends_on: materialization.dependency_refs,
        generating_activity: materialization.generator_ref,
        status: format!("{:?}", materialization.status),
        source_graph_version: materialization.source_graph_version,
        fence_epoch,
        projection_pending: false,
    };
    Ok(raw_response(req_id, &result))
}

// Read-only status lookup on the durable per-graph projection.
#[cfg(feature = "epistemic-tms")]
pub(crate) async fn handle_materialization_status(
    ctx: &QueryHandlerCtx<'_>,
    id: String,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let persist_dir = state.read().await.persist_dir.clone();
    let (status, source_graph_version) =
        match crate::server::reasoning_projection::materialization_status(
            persist_dir.as_deref(),
            graph_name,
            &id,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => return Ok(Response::err(req_id, error)),
        };
    let result = crate::protocol::MaterializationStatusResult {
        status: status.map(|status| format!("{status:?}")),
        source_graph_version,
    };
    Ok(raw_response(req_id, &result))
}

// Bulk "what's stale" read on the same durable per-graph projection.
#[cfg(feature = "epistemic-tms")]
pub(crate) async fn handle_stale_materializations(
    ctx: &QueryHandlerCtx<'_>,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let persist_dir = state.read().await.persist_dir.clone();
    let (ids, source_graph_version) =
        match crate::server::reasoning_projection::stale_materializations(
            persist_dir.as_deref(),
            graph_name,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => return Ok(Response::err(req_id, error)),
        };
    let result = crate::protocol::StaleMaterializationsResult {
        ids,
        source_graph_version,
    };
    Ok(raw_response(req_id, &result))
}

// EPI-P3-7 (gap-fill): standalone Dung argumentation conflict resolution. A
// pure classification over a `BeliefGraph` snapshot — no writes. Gated
// `epistemic-tms` at the handler (same fallback convention as
// `EpistemicStatus`/`WhatChanged`). L-RLS-1 (RLS_ROUTED): `node_ids` are
// caller-supplied, and the grounded/preferred/stable extension is computed
// over the WHOLE argumentation graph before `node_ids` is partitioned against
// it — an RLS-invisible node's own status would otherwise be classified
// directly, and its attack/support edges would still shape OTHER (visible)
// nodes' computed status. `rls.filter_view` the snapshot first, the SAME
// idiom every other read arm in this file applies, so an invisible node (and
// its edges) never enters `BeliefGraph::from_graph_view` at all.
#[cfg(feature = "epistemic-tms")]
pub(crate) async fn handle_resolve_conflict(
    ctx: &QueryHandlerCtx<'_>,
    node_ids: Vec<String>,
    semantics: String,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
    let resp = match compute_off_lock(req_id, move || {
        resolve_conflict_wire(&node_ids, &semantics, &snap)
    })
    .await
    {
        Ok(Ok(result)) => raw_response(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("ResolveConflict error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}

// X-1 (CONCEPT:EG-X1): the multimodal-evidence citation resolver. Gated
// `evidence-graph` at the handler; the wire `Method` variant itself is gated
// only `epistemic` (see its doc comment), so a build with `epistemic` but not
// `evidence-graph` falls through to the not-built catch-all. SURPASS
// gap-closure ("unify the two evidence resolvers"): with `alignment` ALSO
// compiled in, resolve the configured blob store (if any) BEFORE entering
// the off-lock closure (needs `state.read().await`, not available inside a
// `spawn_blocking` closure) and thread it through so citations carry REAL
// resolved content, not just locus metadata. L-RLS-1 (RLS_ROUTED): `node_id`
// is caller-supplied and `evidence_citations` walks the graph's incoming
// support/attack edges transitively — an unfiltered snapshot would resolve
// (and return) an RLS-invisible evidence node's citation. `rls.filter_view`
// the snapshot first, the SAME idiom every other read arm in this file
// applies, so a hidden node (and the edges naming it) never enters
// `BeliefGraph::from_graph_view` at all.
#[cfg(all(feature = "evidence-graph", feature = "alignment"))]
pub(crate) async fn handle_explain_evidence(
    ctx: &QueryHandlerCtx<'_>,
    node_id: String,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
    let blob_store = state.read().await.blob.as_ref().map(|b| b.store.clone());
    let resp = match compute_off_lock(req_id, move || {
        explain_evidence_wire(&node_id, &snap, blob_store)
    })
    .await
    {
        Ok(result) => raw_response(req_id, &result),
        Err(resp) => resp,
    };
    Ok(resp)
}

#[cfg(all(feature = "evidence-graph", not(feature = "alignment")))]
pub(crate) async fn handle_explain_evidence(
    ctx: &QueryHandlerCtx<'_>,
    node_id: String,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
    let resp = match compute_off_lock(req_id, move || explain_evidence_wire(&node_id, &snap)).await
    {
        Ok(result) => raw_response(req_id, &result),
        Err(resp) => resp,
    };
    Ok(resp)
}

// EPI-P3-3/P3-6: do-calculus intervention OR observational conditioning
// (selected by `mode`) over a request-carried SCM. A pure function over
// `variables`/`do_values`/`mode` — no graph snapshot needed. Gated
// `epistemic-causal` at the handler (same fallback convention as above).
#[cfg(feature = "epistemic-causal")]
pub(crate) async fn handle_causal_estimate(
    ctx: &QueryHandlerCtx<'_>,
    variables: Vec<crate::protocol::StructuralEquationWire>,
    do_values: std::collections::BTreeMap<String, f64>,
    mode: crate::protocol::CausalQueryModeWire,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let resp = match compute_off_lock(req_id, move || {
        causal_estimate_wire(&variables, &do_values, mode)
    })
    .await
    {
        Ok(Ok(result)) => raw_response(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("CausalEstimate error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}

// EPI-P3-6: Pearl's point-counterfactual recipe over a request-carried SCM +
// a fully-observed `actual` unit. A pure function over request-carried
// inputs — no graph snapshot needed. Gated `epistemic-causal` at the
// handler (same fallback convention as `CausalEstimate`).
#[cfg(feature = "epistemic-causal")]
pub(crate) async fn handle_causal_counterfactual(
    ctx: &QueryHandlerCtx<'_>,
    variables: Vec<crate::protocol::StructuralEquationWire>,
    actual: std::collections::BTreeMap<String, f64>,
    do_values: std::collections::BTreeMap<String, f64>,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let resp = match compute_off_lock(req_id, move || {
        causal_counterfactual_wire(&variables, &actual, &do_values)
    })
    .await
    {
        Ok(Ok(result)) => raw_response(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("CausalCounterfactual error: {msg}")),
        Err(resp) => resp,
    };
    Ok(resp)
}

// EPI-P3-3: provenance-aware retrieval ranking. A pure function over
// request-carried inputs — no graph snapshot needed. Gated `epistemic-causal`
// at the handler (same fallback convention as above).
#[cfg(feature = "epistemic-causal")]
pub(crate) async fn handle_rank_by_provenance(
    ctx: &QueryHandlerCtx<'_>,
    candidates: Vec<crate::protocol::RetrievalCandidateWire>,
    weights: crate::protocol::RankWeightsWire,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let resp = match compute_off_lock(req_id, move || {
        rank_by_provenance_wire(&candidates, weights)
    })
    .await
    {
        Ok(result) => raw_response(req_id, &result),
        Err(resp) => resp,
    };
    Ok(resp)
}
