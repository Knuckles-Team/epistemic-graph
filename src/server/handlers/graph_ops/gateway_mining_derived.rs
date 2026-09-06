use super::*;

/// Generate the uniform method-destructure → mining-handler → response-result
/// adapter used by the mining methods without duplicating that gateway glue.
macro_rules! define_mining_apply {
    ($name:ident, $method:ident { $($field:ident),* $(,)? }, $handler:ident) => {
        #[cfg(feature = "mining")]
        pub(super) fn $name(
            core: &GraphCore,
            req_id: u64,
            method_owned: Method,
        ) -> Result<ResultPayload, String> {
            let Method::$method {
                $($field,)*
                #[cfg(feature = "epistemic")]
                as_claim,
            } = method_owned
            else {
                unreachable!()
            };
            let resp = super::super::mining::$handler(
                req_id,
                core,
                $($field,)*
                #[cfg(feature = "epistemic")]
                as_claim,
            );
            super::gateway::mining_response_to_gateway_result(resp)
        }
    };
}

define_mining_apply!(
    apply_mine_associate,
    MineAssociate {
        transactions,
        source,
        min_support,
        min_confidence,
        algorithm,
        writeback,
    },
    handle_associate
);

define_mining_apply!(
    apply_mine_sequence,
    MineSequence {
        sequences,
        source,
        min_support,
        algorithm,
        writeback,
    },
    handle_sequence
);

define_mining_apply!(
    apply_mine_forecast,
    MineForecast {
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
    },
    handle_forecast
);

define_mining_apply!(
    apply_mine_text,
    MineText {
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
    },
    handle_text
);

define_mining_apply!(
    apply_mine_subgraph,
    MineSubgraph {
        label,
        min_support,
        max_edges,
        algorithm,
        writeback,
    },
    handle_subgraph
);

define_mining_apply!(
    apply_mine_entity_resolve,
    MineEntityResolve {
        records,
        block_keys,
        vectors,
        source,
        ids,
        bucket_precision,
        threshold,
        writeback,
    },
    handle_entity_resolve
);

define_mining_apply!(
    apply_mine_causal_impact,
    MineCausalImpact {
        series,
        control,
        intervention_index,
        series_id,
        writeback,
    },
    handle_causal_impact
);

define_mining_apply!(
    apply_mine_process,
    MineProcess {
        traces,
        process_id,
        writeback,
    },
    handle_process
);

define_mining_apply!(
    apply_mine_root_cause,
    MineRootCause {
        nodes,
        scores,
        edges,
        symptom,
        max_hops,
        decay,
        writeback,
    },
    handle_root_cause
);

define_mining_apply!(
    apply_mine_risk_propagation,
    MineRiskPropagation {
        nodes,
        seed,
        edges,
        damping,
        tolerance,
        max_iterations,
        writeback,
    },
    handle_risk_propagation
);

define_mining_apply!(
    apply_mine_retrieval_quality,
    MineRetrievalQuality {
        traces,
        k,
        query_id,
        writeback,
    },
    handle_retrieval_quality
);

define_mining_apply!(
    apply_mine_community,
    MineCommunity {
        label,
        algorithm,
        resolution,
        max_iterations,
        seed,
        weighted,
        writeback,
    },
    handle_community
);
