use super::super::mining::{
    AnomalyRequest, ClassifyPredictRequest, ClusterRequest, ReduceRequest, WritebackOptions,
};
use super::*;

use super::gateway_mining_derived::{
    apply_mine_associate, apply_mine_causal_impact, apply_mine_community,
    apply_mine_entity_resolve, apply_mine_forecast, apply_mine_process,
    apply_mine_retrieval_quality, apply_mine_risk_propagation, apply_mine_root_cause,
    apply_mine_sequence, apply_mine_subgraph, apply_mine_text,
};

#[cfg(feature = "mining")]
fn apply_mine_cluster(
    core: &Arc<GraphCore>,
    req_id: u64,
    method_owned: Method,
    tsdb_bind: super::super::mining::MiningTsdbBind<'_>,
) -> Result<ResultPayload, String> {
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
    let resp = super::super::mining::handle_cluster(
        req_id,
        core,
        ClusterRequest {
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
            writeback: WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        },
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb_bind,
    );
    super::gateway::mining_response_to_gateway_result(resp)
}

#[cfg(feature = "mining")]
fn apply_mine_anomaly(
    core: &Arc<GraphCore>,
    req_id: u64,
    method_owned: Method,
    tsdb_bind: super::super::mining::MiningTsdbBind<'_>,
) -> Result<ResultPayload, String> {
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
    let resp = super::super::mining::handle_anomaly(
        req_id,
        core,
        AnomalyRequest {
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
            writeback: WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        },
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb_bind,
    );
    super::gateway::mining_response_to_gateway_result(resp)
}

#[cfg(feature = "mining")]
fn apply_mine_classify_predict(
    core: &Arc<GraphCore>,
    req_id: u64,
    method_owned: Method,
    tsdb_bind: super::super::mining::MiningTsdbBind<'_>,
) -> Result<ResultPayload, String> {
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
    let resp = super::super::mining::handle_classify_predict(
        req_id,
        core,
        ClassifyPredictRequest {
            model,
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            writeback: WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        },
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb_bind,
    );
    super::gateway::mining_response_to_gateway_result(resp)
}

#[cfg(feature = "mining")]
fn apply_mine_reduce(
    core: &Arc<GraphCore>,
    req_id: u64,
    method_owned: Method,
    tsdb_bind: super::super::mining::MiningTsdbBind<'_>,
) -> Result<ResultPayload, String> {
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
    let resp = super::super::mining::handle_reduce(
        req_id,
        core,
        ReduceRequest {
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
            writeback: WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        },
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb_bind,
    );
    super::gateway::mining_response_to_gateway_result(resp)
}

#[cfg(feature = "mining")]
fn apply_mine_ontology_gap(
    core: &GraphCore,
    req_id: u64,
    method_owned: Method,
) -> Result<ResultPayload, String> {
    let Method::MineOntologyGap {
        label,
        writeback,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method_owned
    else {
        unreachable!()
    };
    let resp = super::super::mining::handle_ontology_gap(
        req_id,
        core,
        label,
        WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    );
    super::gateway::mining_response_to_gateway_result(resp)
}

/// Dispatch one of the mining methods whose `apply` needs only the graph core (no
/// TSDB bind) — the common case shared by most of the data-mining family. Pulled
/// out so the two `try_handle` match arms stay one line per method instead of
/// repeating the writeback/clone/closure boilerplate per arm (the arms differ only
/// in which `apply_mine_*` function they name).
#[cfg(feature = "mining")]
async fn dispatch_simple_mining(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    writeback: bool,
    apply: fn(&GraphCore, u64, Method) -> Result<ResultPayload, String>,
) -> Response {
    let method_owned = method.clone();
    let req_id = ctx.req_id;
    mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
        apply(core, req_id, method_owned)
    })
    .await
}

/// Dispatch one of the mining methods whose `apply` also needs the TSDB bind
/// (`MineCluster`/`MineAnomaly`/`MineClassifyPredict`/`MineReduce` — the ones that
/// can pull a query-plan's own series data as features). Same boilerplate-sharing
/// purpose as [`dispatch_simple_mining`]; kept as a separate helper because these
/// four close over `ctx.core` directly (the callback ignores the `&GraphCore` the
/// mutation harness offers) rather than using it. Takes `ctx.graph_name` (not a
/// separate parameter — it's the same value `try_handle`'s caller already put in
/// `ctx`) to stay under clippy's argument-count cap.
#[cfg(feature = "mining")]
async fn dispatch_mining_with_tsdb(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    writeback: bool,
    read_authority: Option<&GraphReadAuthority>,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb_store: Option<
        &Arc<eg_tsdb::store::SeriesStore>,
    >,
    apply: fn(
        &Arc<GraphCore>,
        u64,
        Method,
        super::super::mining::MiningTsdbBind<'_>,
    ) -> Result<ResultPayload, String>,
) -> Response {
    let method_owned = method.clone();
    let req_id = ctx.req_id;
    let core_arc = ctx.core;
    #[cfg(all(feature = "query", feature = "tsdb"))]
    let tsdb_bind = super::super::mining::MiningTsdbBind {
        graph_name: ctx.graph_name,
        read_authority,
        tsdb_store,
    };
    mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |_core| {
        apply(core_arc, req_id, method_owned, tsdb_bind)
    })
    .await
}

pub(super) async fn try_handle(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    graph_name: &str,
    read_authority: Option<&GraphReadAuthority>,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb_store: Option<
        &Arc<eg_tsdb::store::SeriesStore>,
    >,
) -> Option<Response> {
    // `graph_name` is carried on `ctx` too (`dispatch_mining_with_tsdb` reads
    // `ctx.graph_name` — the same value — instead of taking it as a second
    // parameter); this one is kept only to match the shared gateway dispatch
    // signature every `try_handle` in this family uses.
    let _ = graph_name;
    #[cfg(not(all(feature = "query", feature = "tsdb")))]
    let _ = read_authority;
    let resp = match method {
        // ── L11 rollout batch 3: RUNTIME-CONDITIONAL data-mining family — same
        // shape as GraphLearn* above (the request's own `writeback` field decides
        // whether THIS call mutates). Each arm clones the whole `method` (cheap;
        // `Method` derives `Clone`) and re-destructures the OWNED clone inside the
        // `apply` closure via `let-else` — this keeps the field list a verbatim
        // copy of `mining::try_handle`'s own destructuring (the single source of
        // truth for each method's fields), rather than hand-cloning 10+ fields
        // per arm. `MineClassifyFit` is NOT here (policy explicit-false: it never
        // writes back) and keeps its ordinary read-only arm in `mining.rs`. Split
        // across `try_handle`/`try_handle_mining_rest` (dispatch-chain convention,
        // same as this file's routing over methods it doesn't own) to keep each
        // match's cyclomatic complexity within cap.
        #[cfg(feature = "mining")]
        Method::MineAssociate { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_associate).await
        }
        #[cfg(feature = "mining")]
        Method::MineCluster { writeback, .. } => {
            dispatch_mining_with_tsdb(
                ctx,
                plan,
                method,
                *writeback,
                read_authority,
                #[cfg(all(feature = "query", feature = "tsdb"))]
                tsdb_store,
                apply_mine_cluster,
            )
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineAnomaly { writeback, .. } => {
            dispatch_mining_with_tsdb(
                ctx,
                plan,
                method,
                *writeback,
                read_authority,
                #[cfg(all(feature = "query", feature = "tsdb"))]
                tsdb_store,
                apply_mine_anomaly,
            )
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineClassifyPredict { writeback, .. } => {
            dispatch_mining_with_tsdb(
                ctx,
                plan,
                method,
                *writeback,
                read_authority,
                #[cfg(all(feature = "query", feature = "tsdb"))]
                tsdb_store,
                apply_mine_classify_predict,
            )
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineReduce { writeback, .. } => {
            dispatch_mining_with_tsdb(
                ctx,
                plan,
                method,
                *writeback,
                read_authority,
                #[cfg(all(feature = "query", feature = "tsdb"))]
                tsdb_store,
                apply_mine_reduce,
            )
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineSequence { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_sequence).await
        }
        #[cfg(feature = "mining")]
        Method::MineForecast { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_forecast).await
        }
        #[cfg(feature = "mining")]
        Method::MineText { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_text).await
        }
        #[cfg(feature = "mining")]
        _ => return try_handle_mining_rest(ctx, plan, method).await,
        #[cfg(not(feature = "mining"))]
        _ => return None,
    };
    Some(resp)
}

/// The rest of the data-mining dispatch (CONCEPT:EG-KG.storage.feature) — the methods
/// `try_handle` doesn't match fall through here (same dispatch-chain convention this
/// module already uses across handler files). All share `dispatch_simple_mining`'s
/// shape (no TSDB bind needed), split into its own function purely to keep each
/// match's cyclomatic complexity within cap — `try_handle` owns the ones needing
/// `graph_name`/`read_authority`/`tsdb_store`, this one doesn't need them at all.
#[cfg(feature = "mining")]
async fn try_handle_mining_rest(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let resp = match method {
        Method::MineSubgraph { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_subgraph).await
        }
        Method::MineEntityResolve { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_entity_resolve).await
        }
        Method::MineCausalImpact { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_causal_impact).await
        }
        Method::MineProcess { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_process).await
        }
        Method::MineRootCause { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_root_cause).await
        }
        Method::MineRiskPropagation { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_risk_propagation).await
        }
        Method::MineOntologyGap { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_ontology_gap).await
        }
        Method::MineRetrievalQuality { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_retrieval_quality)
                .await
        }
        Method::MineCommunity { writeback, .. } => {
            dispatch_simple_mining(ctx, plan, method, *writeback, apply_mine_community).await
        }
        _ => return None,
    };
    Some(resp)
}
