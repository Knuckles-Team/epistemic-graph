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
    #[cfg(not(all(feature = "query", feature = "tsdb")))]
    let _ = (graph_name, read_authority);
    let resp = match method {
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
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_associate(core, req_id, method_owned)
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
            let tsdb_bind = super::super::mining::MiningTsdbBind {
                graph_name,
                read_authority,
                tsdb_store,
            };
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |_core| {
                apply_mine_cluster(core_arc, req_id, method_owned, tsdb_bind)
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
            let tsdb_bind = super::super::mining::MiningTsdbBind {
                graph_name,
                read_authority,
                tsdb_store,
            };
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |_core| {
                apply_mine_anomaly(core_arc, req_id, method_owned, tsdb_bind)
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
            let tsdb_bind = super::super::mining::MiningTsdbBind {
                graph_name,
                read_authority,
                tsdb_store,
            };
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |_core| {
                apply_mine_classify_predict(core_arc, req_id, method_owned, tsdb_bind)
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
            let tsdb_bind = super::super::mining::MiningTsdbBind {
                graph_name,
                read_authority,
                tsdb_store,
            };
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |_core| {
                apply_mine_reduce(core_arc, req_id, method_owned, tsdb_bind)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineSequence { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_sequence(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineForecast { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_forecast(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineText { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_text(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineSubgraph { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_subgraph(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineEntityResolve { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_entity_resolve(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineCausalImpact { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_causal_impact(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineProcess { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_process(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineRootCause { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_root_cause(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineRiskPropagation { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_risk_propagation(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineOntologyGap { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_ontology_gap(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineRetrievalQuality { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_retrieval_quality(core, req_id, method_owned)
            })
            .await
        }
        #[cfg(feature = "mining")]
        Method::MineCommunity { writeback, .. } => {
            let writeback = *writeback;
            let method_owned = method.clone();
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                apply_mine_community(core, req_id, method_owned)
            })
            .await
        }
        _ => return None,
    };
    Some(resp)
}
