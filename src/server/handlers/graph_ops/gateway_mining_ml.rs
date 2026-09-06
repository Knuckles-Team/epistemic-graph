use super::*;

pub(super) async fn try_handle(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let resp = match method {
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
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                let resp =
                    super::super::graphlearn::handle_fit(req_id, core, source, params, writeback);
                super::gateway::mining_response_to_gateway_result(resp)
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
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                let resp = super::super::graphlearn::handle_predict(
                    req_id,
                    core,
                    model,
                    source,
                    candidate_pairs,
                    top_k,
                    writeback,
                );
                super::gateway::mining_response_to_gateway_result(resp)
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
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                let resp = super::super::pipeline::handle_train(
                    req_id, core, name, source, x, y, spec, writeback,
                );
                super::gateway::mining_response_to_gateway_result(resp)
            })
            .await
        }
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineServe { name, version } => {
            let (name, version) = (name.clone(), *version);
            let req_id = ctx.req_id;
            mutation::commit_conditional_mutation(ctx, plan, method, true, move |core| {
                let resp = super::super::pipeline::handle_serve(req_id, core, name, version);
                super::gateway::mining_response_to_gateway_result(resp)
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
            mutation::commit_conditional_mutation(ctx, plan, method, writeback, move |core| {
                let resp = super::super::pipeline::handle_predict(
                    req_id, core, name, version, source, x, writeback,
                );
                super::gateway::mining_response_to_gateway_result(resp)
            })
            .await
        }
        _ => return None,
    };
    Some(resp)
}
