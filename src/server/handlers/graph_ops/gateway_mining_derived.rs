#[cfg(feature = "mining")]
use super::super::mining::{
    AssociationRequest, CommunityRequest, EntityResolutionRequest, ForecastRequest,
    RiskPropagationRequest, RootCauseRequest, TextRequest, WritebackOptions,
};
use super::*;

/// Generate the uniform method-destructure → mining-handler → response-result
/// adapter used by the mining methods without duplicating that gateway glue.
macro_rules! define_mining_apply {
    ($name:ident, $method:ident { $($field:ident),* $(,)? }, $claim:ident, $handler:ident($($arg:tt)*)) => {
        #[cfg(feature = "mining")]
        pub(super) fn $name(
            core: &GraphCore,
            req_id: u64,
            method_owned: Method,
        ) -> Result<ResultPayload, String> {
            let Method::$method {
                $($field,)*
                #[cfg(feature = "epistemic")]
                $claim,
            } = method_owned
            else {
                unreachable!()
            };
            let resp = super::super::mining::$handler(
                req_id,
                core,
                $($arg)*
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
    as_claim,
    handle_associate(AssociationRequest {
        transactions,
        source,
        min_support,
        min_confidence,
        algorithm,
        writeback: WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    })
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
    as_claim,
    handle_sequence(
        sequences,
        source,
        min_support,
        algorithm,
        WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    )
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
    as_claim,
    handle_forecast(ForecastRequest {
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
        writeback: WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    })
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
    as_claim,
    handle_text(TextRequest {
        docs,
        source,
        algorithm,
        k,
        alpha,
        beta,
        iterations,
        seed,
        top_n,
        writeback: WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    })
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
    as_claim,
    handle_subgraph(
        label,
        min_support,
        max_edges,
        algorithm,
        WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    )
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
    as_claim,
    handle_entity_resolve(EntityResolutionRequest {
        records,
        block_keys,
        vectors,
        source,
        ids,
        bucket_precision,
        threshold,
        writeback: WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    })
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
    as_claim,
    handle_causal_impact(
        series,
        control,
        intervention_index,
        series_id,
        WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    )
);

define_mining_apply!(
    apply_mine_process,
    MineProcess {
        traces,
        process_id,
        writeback,
    },
    as_claim,
    handle_process(
        traces,
        process_id,
        WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    )
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
    as_claim,
    handle_root_cause(RootCauseRequest {
        nodes,
        scores,
        edges,
        symptom,
        max_hops,
        decay,
        writeback: WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    })
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
    as_claim,
    handle_risk_propagation(RiskPropagationRequest {
        nodes,
        seed,
        edges,
        damping,
        tolerance,
        max_iterations,
        writeback: WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    })
);

define_mining_apply!(
    apply_mine_retrieval_quality,
    MineRetrievalQuality {
        traces,
        k,
        query_id,
        writeback,
    },
    as_claim,
    handle_retrieval_quality(
        traces,
        k,
        query_id,
        WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    )
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
    as_claim,
    handle_community(CommunityRequest {
        label,
        algorithm,
        resolution,
        max_iterations,
        seed,
        weighted,
        writeback: WritebackOptions {
            enabled: writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        },
    })
);
