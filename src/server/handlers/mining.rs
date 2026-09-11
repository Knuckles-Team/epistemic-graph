//! Data-mining ops (CONCEPT:EG-KG.mining.frequent-itemset-mining): association-rule
//! mining over EITHER explicit transactions OR a graph-derived transaction source
//! (compute-near-data), with optional KG write-back of the mined rules.
//!
//! Unlike the stateless finance/datascience handlers, mining is GRAPH-SCOPED: the
//! graph-derived source reads node neighborhoods off the live core, and write-back
//! materializes `:AssociationRule` nodes into the same core. So it routes in the
//! `dispatch_graph_op` chain with the graph core in hand (like the query/rdf
//! handlers) rather than the pre-graph pure-compute path.

// The Result router moves the large `Method` enum by value on the fall-through
// path; boxing the Err would allocate per non-mining request (see datascience.rs).
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::{
    AnomalyAlgorithm, ClassifyAlgorithm, ClusterAlgorithm, CommunityAlgorithm, ForecastAlgorithm,
    Linkage, Method, MineAlgorithm, MineSeqAlgorithm, ReduceAlgorithm, Response, ResultPayload,
    RetrievalTraceSpec, SequenceSource, SubgraphAlgorithm, SvmKernel, TextAlgorithm, TextSource,
    TransactionSource, VectorSource,
};

mod association;
mod classic;
mod entity;
mod input;
mod insight;
mod process;
mod replay;
mod vector;
mod writeback;

pub(crate) use association::{handle_associate, AssociationRequest};
pub(super) use classic::{
    handle_forecast, handle_sequence, handle_subgraph, handle_text, ForecastRequest, TextRequest,
};
pub(super) use entity::{handle_entity_resolve, EntityResolutionRequest};
#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) use input::MiningTsdbBind;
pub(super) use insight::handle_ontology_gap;
pub(super) use insight::{
    handle_community, handle_retrieval_quality, handle_risk_propagation, handle_root_cause,
    CommunityRequest, RiskPropagationRequest, RootCauseRequest,
};
pub(super) use process::{handle_causal_impact, handle_process};
pub(crate) use replay::replay;
use vector::handle_classify_fit;
pub(crate) use vector::{
    handle_anomaly, handle_classify_predict, handle_cluster, handle_reduce, AnomalyRequest,
    ClassifyPredictRequest, ClusterRequest, ReduceRequest,
};
pub(crate) use writeback::WritebackOptions;

#[cfg(test)]
use eg_compute::mining::classify::FittedClassifier;

/// Handle a `Mine*` method. `Err(method)` hands a non-mining method back to the
/// dispatcher (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention.)
pub(crate) fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    read_authority: Option<&crate::server::access::GraphReadAuthority>,
    // CONCEPT:EG-KG.mining.tsdb-typed-absent — the graph this request is scoped to, and the
    // server's live tsdb store handle, both needed ONLY to bind a plan-sourced `Op::TsScan`
    // leg for `MineClassifyFit` (the one Mine* method NOT routed through
    // `graph_ops::try_handle_gateway`, which binds the same pair for the gateway-routed
    // Mine* methods). `dispatch_graph_op_inner` is the sole caller with both in scope.
    #[cfg(all(feature = "query", feature = "tsdb"))] graph_name: &str,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb_store: Option<
        &Arc<eg_tsdb::store::SeriesStore>,
    >,
    method: Method,
) -> Result<Response, Method> {
    match method {
        // Every writeback-capable Mine* method (CONCEPT:EG-P0-2 bypass guard,
        // L11) is now `mutation::GATEWAY_ROUTED` (runtime-conditional on
        // `writeback`) — `dispatch_graph_op` routes it through
        // `graph_ops::try_handle_gateway` BEFORE this handler is ever reached
        // (see `mutation::commit_conditional_mutation`), so these arms are
        // structurally unreachable here now, not merely undocumented.
        // `MineClassifyFit` is the ONE exception: it never writes back
        // (policy explicit-false), so it stays an ordinary read below.
        Method::MineAssociate { .. }
        | Method::MineCluster { .. }
        | Method::MineAnomaly { .. }
        | Method::MineClassifyPredict { .. }
        | Method::MineReduce { .. }
        | Method::MineSequence { .. }
        | Method::MineForecast { .. }
        | Method::MineText { .. }
        | Method::MineSubgraph { .. }
        | Method::MineEntityResolve { .. }
        | Method::MineCausalImpact { .. }
        | Method::MineProcess { .. }
        | Method::MineRootCause { .. }
        | Method::MineRiskPropagation { .. }
        | Method::MineOntologyGap { .. }
        | Method::MineRetrievalQuality { .. }
        | Method::MineCommunity { .. } => unreachable!(
            "writeback-capable Mine* methods are mutation::GATEWAY_ROUTED; \
             dispatch_graph_op must route them through try_handle_gateway before \
             they reach this fallback handler"
        ),
        Method::MineClassifyFit {
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            y,
            algorithm,
            k,
            alpha,
            lr,
            epochs,
            l2,
            c,
        } => {
            // This is the only mining method that bypasses the mutation gateway.
            // Project here, after routing has identified it as a true graph read,
            // so unrelated methods do not pay an O(V+E) copy.
            let authority = read_authority
                .expect("MineClassifyFit must carry the universal served-read authority");
            let core = authority.project_core(&core);
            #[cfg(all(feature = "query", feature = "tsdb"))]
            let tsdb_bind = MiningTsdbBind {
                graph_name,
                read_authority: Some(authority),
                tsdb_store,
            };
            Ok(handle_classify_fit(
                req_id,
                &core,
                vector::ClassifyFitRequest {
                    x,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    y,
                    algorithm,
                    k,
                    alpha,
                    lr,
                    epochs,
                    l2,
                    c,
                    #[cfg(all(feature = "query", feature = "tsdb"))]
                    tsdb: tsdb_bind,
                    #[cfg(not(all(feature = "query", feature = "tsdb")))]
                    marker: std::marker::PhantomData,
                },
            ))
        }
        other => Err(other),
    }
}

/// Test-only dispatch shim (CONCEPT:EG-P0-2 bypass guard, L11): this module's
/// white-box unit tests exercise `handle_*` business logic directly against a
/// `Method` value WITHOUT going through `dispatch_graph_op`/`graph_ops::
/// try_handle_gateway`/`commit_conditional_mutation` at all, so they never see
/// the real gateway routing that makes `try_handle`'s own arms above correctly
/// unreachable. This is the SAME routing table `try_handle` had before L11 routed
/// these methods through the gateway, kept ONLY as a test entry point — it calls
/// the EXACT SAME `pub(crate) handle_*` functions the gateway match arm in
/// `graph_ops::try_handle_gateway` calls, so there is still only ONE
/// implementation, never a second copy that could drift.
#[cfg(test)]
fn dispatch_for_test(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    dispatch_record_families_for_test(req_id, Arc::clone(&core), method)
        .or_else(|method| dispatch_vector_families_for_test(req_id, Arc::clone(&core), method))
        .or_else(|method| dispatch_insight_families_for_test(req_id, core, method))
}

/// Exercise the vector-oriented mining families through their production
/// business handlers. The served mutation gateway remains covered by
/// integration tests; this unit seam intentionally supplies no durability or
/// authorization state.
#[cfg(test)]
fn dispatch_vector_families_for_test(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    // No live server wiring in this harness (CONCEPT:EG-KG.mining.tsdb-typed-absent) — a
    // `TsScan`-bearing plan run through this shim is a typed error, exactly as it would be
    // against a real server with no tsdb store configured. None of this module's own tests
    // exercise a `TsScan` leg, so this is otherwise inert.
    #[cfg(all(feature = "query", feature = "tsdb"))]
    let tsdb_bind = MiningTsdbBind {
        graph_name: "test",
        read_authority: None,
        tsdb_store: None,
    };
    match method {
        Method::MineCluster {
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
        } => Ok(handle_cluster(
            req_id,
            &core,
            vector::ClusterRequest {
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
        )),
        Method::MineAnomaly {
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
        } => Ok(handle_anomaly(
            req_id,
            &core,
            vector::AnomalyRequest {
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
        )),
        Method::MineClassifyFit {
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            y,
            algorithm,
            k,
            alpha,
            lr,
            epochs,
            l2,
            c,
        } => Ok(handle_classify_fit(
            req_id,
            &core,
            vector::ClassifyFitRequest {
                x,
                source,
                #[cfg(feature = "query")]
                plan,
                y,
                algorithm,
                k,
                alpha,
                lr,
                epochs,
                l2,
                c,
                #[cfg(all(feature = "query", feature = "tsdb"))]
                tsdb: tsdb_bind,
                #[cfg(not(all(feature = "query", feature = "tsdb")))]
                marker: std::marker::PhantomData,
            },
        )),
        Method::MineClassifyPredict {
            model,
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_classify_predict(
            req_id,
            &core,
            vector::ClassifyPredictRequest {
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
        )),
        Method::MineReduce {
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
        } => Ok(handle_reduce(
            req_id,
            &core,
            vector::ReduceRequest {
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
        )),
        other => Err(other),
    }
}

/// Exercise the record, sequence, scalar-series, and topology mining families
/// through their production business handlers.
#[cfg(test)]
fn dispatch_record_families_for_test(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::MineAssociate {
            transactions,
            source,
            min_support,
            min_confidence,
            algorithm,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_associate(
            req_id,
            &core,
            AssociationRequest {
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
            },
        )),
        Method::MineSequence {
            sequences,
            source,
            min_support,
            algorithm,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_sequence(
            req_id,
            &core,
            sequences,
            source,
            min_support,
            algorithm,
            WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        )),
        Method::MineForecast {
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
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_forecast(
            req_id,
            &core,
            classic::ForecastRequest {
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
            },
        )),
        Method::MineText {
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
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_text(
            req_id,
            &core,
            classic::TextRequest {
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
            },
        )),
        Method::MineSubgraph {
            label,
            min_support,
            max_edges,
            algorithm,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_subgraph(
            req_id,
            &core,
            label,
            min_support,
            max_edges,
            algorithm,
            WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        )),
        other => Err(other),
    }
}

/// Exercise the graph-insight families added after the original mining surface.
/// They share one graph-native input/result contract and no TSDB binding.
#[cfg(test)]
fn dispatch_insight_families_for_test(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::MineEntityResolve {
            records,
            block_keys,
            vectors,
            source,
            ids,
            bucket_precision,
            threshold,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_entity_resolve(
            req_id,
            &core,
            entity::EntityResolutionRequest {
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
            },
        )),
        Method::MineCausalImpact {
            series,
            control,
            intervention_index,
            series_id,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_causal_impact(
            req_id,
            &core,
            series,
            control,
            intervention_index,
            series_id,
            WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        )),
        Method::MineProcess {
            traces,
            process_id,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_process(
            req_id,
            &core,
            traces,
            process_id,
            WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        )),
        Method::MineRootCause {
            nodes,
            scores,
            edges,
            symptom,
            max_hops,
            decay,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_root_cause(
            req_id,
            &core,
            insight::RootCauseRequest {
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
            },
        )),
        Method::MineRiskPropagation {
            nodes,
            seed,
            edges,
            damping,
            tolerance,
            max_iterations,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_risk_propagation(
            req_id,
            &core,
            insight::RiskPropagationRequest {
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
            },
        )),
        Method::MineOntologyGap {
            label,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_ontology_gap(
            req_id,
            &core,
            label,
            WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        )),
        Method::MineRetrievalQuality {
            traces,
            k,
            query_id,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_retrieval_quality(
            req_id,
            &core,
            traces,
            k,
            query_id,
            WritebackOptions {
                enabled: writeback,
                #[cfg(feature = "epistemic")]
                as_claim,
            },
        )),
        Method::MineCommunity {
            label,
            algorithm,
            resolution,
            max_iterations,
            seed,
            weighted,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => Ok(handle_community(
            req_id,
            &core,
            insight::CommunityRequest {
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
            },
        )),
        other => Err(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphCore;
    use crate::protocol::TransactionSource;

    fn node(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    #[test]
    fn non_mining_method_falls_through() {
        let core = Arc::new(GraphCore::new());
        let m = Method::NodeCount;
        assert!(matches!(
            dispatch_for_test(1, core, m),
            Err(Method::NodeCount)
        ));
    }

    #[test]
    fn explicit_transactions_produce_rules() {
        let core = Arc::new(GraphCore::new());
        let txns = vec![
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "c".to_string()],
            vec!["b".to_string(), "c".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        ];
        let m = Method::MineAssociate {
            transactions: txns,
            source: None,
            min_support: 0.4,
            min_confidence: 0.5,
            algorithm: MineAlgorithm::Apriori,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(7, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_transactions"], 5);
        assert!(v["n_rules"].as_u64().unwrap() > 0);
        assert_eq!(v["written_back"], 0);
    }

    #[test]
    fn graph_derived_source_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Two "Cart" owners, each linked to its purchased "Item" nodes.
        core.add_node("cart1".into(), node(serde_json::json!({"type": "Cart"})));
        core.add_node("cart2".into(), node(serde_json::json!({"type": "Cart"})));
        for item in ["milk", "bread"] {
            core.add_node(item.into(), node(serde_json::json!({"type": "Item"})));
        }
        let _ = core.add_edge("cart1".into(), "milk".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart1".into(), "bread".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart2".into(), "milk".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart2".into(), "bread".into(), node(serde_json::json!({})));

        let m = Method::MineAssociate {
            transactions: Vec::new(),
            source: Some(TransactionSource {
                node_label: "Cart".into(),
                direction: "out".into(),
                item_field: None, // neighbor node id ⇒ "milk"/"bread"
                relation: None,
                limit: 0,
            }),
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: MineAlgorithm::Fpgrowth,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(9, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_transactions"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written > 0);
        // The dispatch shell calls `mark_dirty()` after a write (invalidating the
        // lazy label index); mirror that here so the label query sees the new nodes.
        core.mark_dirty();
        // Write-back created queryable :AssociationRule nodes.
        let rule_nodes = core.get_nodes_by_label("AssociationRule", 0);
        assert_eq!(rule_nodes.len() as u64, written);
    }

    #[test]
    fn cluster_explicit_features_dbscan() {
        let core = Arc::new(GraphCore::new());
        let features = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.1],
            vec![0.2, 0.0],
            vec![10.0, 10.0],
            vec![10.1, 9.9],
            vec![10.0, 10.2],
        ];
        let m = Method::MineCluster {
            features,
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            algorithm: ClusterAlgorithm::Dbscan,
            eps: 1.0,
            min_pts: 2,
            k: 3,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(1, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 6);
        assert_eq!(v["n_clusters"], 2);
        assert_eq!(v["written_back"], 0);
    }

    #[test]
    fn cluster_over_node_embeddings_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Six :Doc nodes with 2-D embeddings forming two groups.
        let embs = [
            ("d0", [0.0f32, 0.0]),
            ("d1", [0.2, 0.1]),
            ("d2", [0.1, 0.2]),
            ("d3", [9.0, 9.0]),
            ("d4", [9.2, 8.9]),
            ("d5", [8.9, 9.1]),
        ];
        for (id, e) in embs {
            core.add_node(id.into(), node(serde_json::json!({"type": "Doc"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec())
                .unwrap();
        }
        let m = Method::MineCluster {
            features: Vec::new(),
            source: Some(VectorSource {
                node_label: "Doc".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            algorithm: ClusterAlgorithm::Kmedoids,
            eps: 0.5,
            min_pts: 5,
            k: 2,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(2, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 6);
        assert_eq!(v["n_clusters"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 2);
        // Members are reported as node ids (not indices).
        let first = &v["clusters"][0]["members"][0];
        assert!(first.is_string());
        core.mark_dirty();
        let cluster_nodes = core.get_nodes_by_label("Cluster", 0);
        assert_eq!(cluster_nodes.len() as u64, written);
    }

    /// The headline fused example (CONCEPT:EG-KG.mining.fused-plan-source, Phase 5):
    /// vector-retrieve a neighborhood via an upstream `Op::Rank` PLAN — no
    /// `VectorSource` label spec at all — cluster the retrieved rows, and write
    /// `:Cluster` nodes back, all in ONE `MineCluster` call. Proves
    /// `retrieve → mine → writeback` composes as ONE plan (compute-near-data, no
    /// client round-trip between the retrieval leg and the mining leg).
    #[test]
    #[cfg(feature = "query")]
    fn fused_plan_rank_then_cluster_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Six :Doc nodes with 2-D embeddings forming two well-separated groups.
        let embs = [
            ("d0", [0.0f32, 0.0]),
            ("d1", [0.2, 0.1]),
            ("d2", [0.1, 0.2]),
            ("d3", [9.0, 9.0]),
            ("d4", [9.2, 8.9]),
            ("d5", [8.9, 9.1]),
        ];
        for (id, e) in embs {
            core.add_node(id.into(), node(serde_json::json!({"type": "Doc"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec())
                .unwrap();
        }
        // Upstream retrieval plan: scan all :Doc nodes, then rank by cosine
        // similarity to a query near the FIRST group — a cross-modal
        // Scan→Rank→Limit leg that runs BEFORE the mining op ever sees a row.
        let plan = crate::wire::Plan::new(vec![
            crate::wire::Op::Scan {
                label: "Doc".into(),
            },
            crate::wire::Op::Rank {
                query: vec![0.1, 0.1],
            },
            crate::wire::Op::Limit { k: 6 },
        ]);
        let m = Method::MineCluster {
            features: Vec::new(),
            source: None,
            plan: Some(plan),
            algorithm: ClusterAlgorithm::Kmedoids,
            eps: 0.5,
            min_pts: 5,
            k: 2,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(42, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        // The plan's Rank/Limit legs ran FIRST — the vector kNN leg is an
        // approximate search (documented "approximate, small-N" contract shared
        // with UMAP/t-SNE elsewhere in this surface), so it may recall slightly
        // fewer than all 6 candidates; the mining op then clustered exactly the
        // rows the plan handed it — one round trip, no client marshalling
        // between "retrieve" and "mine".
        let n_rows = v["n_rows"].as_u64().unwrap();
        assert!(
            (4..=6).contains(&n_rows),
            "expected the Rank leg to recall most of the 6 candidates, got {n_rows}"
        );
        assert_eq!(v["n_clusters"], 2); // k-medoids always forms exactly k=2 groups
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 2);
        core.mark_dirty();
        let cluster_nodes = core.get_nodes_by_label("Cluster", 0);
        assert_eq!(cluster_nodes.len() as u64, written);
    }

    /// A plan that finds NO matching rows (a label the graph doesn't carry)
    /// degrades to an empty feature set rather than erroring — the same
    /// "no match ⇒ empty" contract every other mining source honors.
    #[test]
    #[cfg(feature = "query")]
    fn fused_plan_no_match_degrades_to_empty() {
        let core = Arc::new(GraphCore::new());
        let plan = crate::wire::Plan::new(vec![crate::wire::Op::Scan {
            label: "NoSuchLabel".into(),
        }]);
        let m = Method::MineCluster {
            features: Vec::new(),
            source: None,
            plan: Some(plan),
            algorithm: ClusterAlgorithm::Dbscan,
            eps: 1.0,
            min_pts: 2,
            k: 2,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(43, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 0);
    }

    #[test]
    fn anomaly_values_series_zscore_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // A flat series with one spike — the tsdb RCA path via `values`.
        let mut values: Vec<f64> = (0..10).map(|i| 1.0 + 0.01 * i as f64).collect();
        values.push(100.0); // the anomaly
        let m = Method::MineAnomaly {
            features: Vec::new(),
            values,
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            algorithm: AnomalyAlgorithm::Zscore,
            k: 20,
            n_trees: 100,
            sample_size: 256,
            seed: 0,
            nu: 0.1,
            gamma: 0.0,
            kernel: SvmKernel::Rbf,
            threshold: None,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(3, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 11);
        assert_eq!(v["n_anomalies"], 1);
        // The spike (last row) is the flagged one.
        let rows = v["rows"].as_array().unwrap();
        assert!(rows[10]["is_anomaly"].as_bool().unwrap());
    }

    #[test]
    fn anomaly_over_node_embeddings_writeback_links_source() {
        let core = Arc::new(GraphCore::new());
        let embs = [
            ("m0", [0.0f32, 0.0]),
            ("m1", [0.1, 0.0]),
            ("m2", [0.0, 0.1]),
            ("m3", [0.1, 0.1]),
            ("m4", [0.05, 0.05]),
            ("m5", [50.0, 50.0]), // outlier
        ];
        for (id, e) in embs {
            core.add_node(id.into(), node(serde_json::json!({"type": "Metric"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec())
                .unwrap();
        }
        let m = Method::MineAnomaly {
            features: Vec::new(),
            values: Vec::new(),
            source: Some(VectorSource {
                node_label: "Metric".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            algorithm: AnomalyAlgorithm::Zscore,
            k: 20,
            n_trees: 100,
            sample_size: 256,
            seed: 0,
            nu: 0.1,
            gamma: 0.0,
            kernel: SvmKernel::Rbf,
            threshold: None,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(4, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 6);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written >= 1);
        core.mark_dirty();
        let anomaly_nodes = core.get_nodes_by_label("Anomaly", 0);
        assert_eq!(anomaly_nodes.len() as u64, written);
        // The anomaly node links to its source (m5) via ANOMALY_OF.
        let succ = core.get_successors(&anomaly_nodes[0].0).unwrap_or_default();
        assert!(succ.iter().any(|s| s == "m5"));
    }

    #[test]
    fn classify_fit_then_predict_roundtrip() {
        let core = Arc::new(GraphCore::new());
        // Separable 2-class training set.
        let x = vec![
            vec![0.0, 0.0],
            vec![0.5, 0.3],
            vec![0.2, 0.8],
            vec![10.0, 10.0],
            vec![10.5, 9.7],
            vec![9.8, 10.4],
        ];
        let y = vec![0, 0, 0, 1, 1, 1];
        let fit = Method::MineClassifyFit {
            x: x.clone(),
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            y,
            algorithm: ClassifyAlgorithm::Logistic,
            k: 5,
            alpha: 1.0,
            lr: 0.5,
            epochs: 500,
            l2: 0.0,
            c: 1.0,
        };
        let resp = dispatch_for_test(1, Arc::clone(&core), fit).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_samples"], 6);
        let model: FittedClassifier = serde_json::from_value(v["model"].clone()).unwrap();

        let predict = Method::MineClassifyPredict {
            model,
            x: vec![vec![0.3, 0.3], vec![10.0, 10.0]],
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(2, core, predict).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        let rows = v["rows"].as_array().unwrap();
        assert_eq!(rows[0]["label"], 0);
        assert_eq!(rows[1]["label"], 1);
    }

    #[test]
    fn classify_predict_over_embeddings_writeback() {
        let core = Arc::new(GraphCore::new());
        // Fit a GaussianNB in-memory, then predict over node embeddings + writeback.
        let x = vec![
            vec![0.0, 0.0],
            vec![0.2, 0.1],
            vec![9.0, 9.0],
            vec![9.1, 8.8],
        ];
        let y = vec![0, 0, 1, 1];
        let model = eg_compute::mining::classify::fit(
            &x,
            &y,
            eg_compute::mining::classify::Algorithm::GaussianNb,
        )
        .unwrap();
        for (id, e) in [("p0", [0.1f32, 0.0]), ("p1", [9.0, 9.1])] {
            core.add_node(id.into(), node(serde_json::json!({"type": "Sample"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec())
                .unwrap();
        }
        let m = Method::MineClassifyPredict {
            model,
            x: Vec::new(),
            source: Some(VectorSource {
                node_label: "Sample".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(3, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 2);
        core.mark_dirty();
        let cls_nodes = core.get_nodes_by_label("Classification", 0);
        assert_eq!(cls_nodes.len() as u64, written);
        // The classification node links to its source via CLASSIFIED_AS.
        let succ = core.get_successors(&cls_nodes[0].0).unwrap_or_default();
        assert!(succ.iter().any(|s| s == "p0" || s == "p1"));
    }

    #[test]
    fn reduce_svd_and_writeback_embedding2d() {
        let core = Arc::new(GraphCore::new());
        let embs = [
            ("d0", [1.0f32, 0.0, 0.0]),
            ("d1", [0.0, 1.0, 0.0]),
            ("d2", [1.0, 1.0, 0.0]),
            ("d3", [2.0, 1.0, 0.0]),
        ];
        for (id, e) in embs {
            core.add_node(id.into(), node(serde_json::json!({"type": "Vec"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec())
                .unwrap();
        }
        let m = Method::MineReduce {
            x: Vec::new(),
            source: Some(VectorSource {
                node_label: "Vec".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            labels: Vec::new(),
            algorithm: ReduceAlgorithm::Svd,
            n_components: 2,
            n_neighbors: 15,
            min_dist: 0.1,
            perplexity: 30.0,
            epochs: 300,
            lr: 100.0,
            seed: 0,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(4, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 4);
        assert_eq!(v["n_components"], 2);
        assert!(v["singular_values"].is_array());
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 4);
        core.mark_dirty();
        let e2d = core.get_nodes_by_label("Embedding2D", 0);
        assert_eq!(e2d.len() as u64, written);
    }

    #[test]
    fn reduce_lda_requires_labels() {
        let core = Arc::new(GraphCore::new());
        let m = Method::MineReduce {
            x: vec![vec![0.0, 0.0], vec![1.0, 1.0]],
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            labels: Vec::new(), // missing → error for LDA
            algorithm: ReduceAlgorithm::Lda,
            n_components: 1,
            n_neighbors: 15,
            min_dist: 0.1,
            perplexity: 30.0,
            epochs: 300,
            lr: 100.0,
            seed: 0,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(5, core, m).expect("handled");
        assert!(resp.result.is_none()); // an error response carries no result payload
    }

    #[test]
    fn explicit_sequences_produce_patterns() {
        let core = Arc::new(GraphCore::new());
        let seqs = vec![
            vec![
                "login".to_string(),
                "browse".to_string(),
                "purchase".to_string(),
            ],
            vec![
                "login".to_string(),
                "search".to_string(),
                "browse".to_string(),
                "purchase".to_string(),
            ],
            vec![
                "login".to_string(),
                "browse".to_string(),
                "purchase".to_string(),
            ],
        ];
        let m = Method::MineSequence {
            sequences: seqs,
            source: None,
            min_support: 0.5,
            algorithm: MineSeqAlgorithm::Prefixspan,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(11, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_sequences"], 3);
        assert!(v["n_patterns"].as_u64().unwrap() > 0);
        assert_eq!(v["written_back"], 0);
        let patterns = v["patterns"].as_array().unwrap();
        assert!(patterns
            .iter()
            .any(|p| { p["items"] == serde_json::json!(["login", "browse", "purchase"]) }));
    }

    #[test]
    fn sequence_graph_derived_source_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Two "Session" owners whose ordered "out" edges (insertion order) are the
        // event sequence: session1/session2 both go view -> add_cart -> checkout.
        core.add_node("s1".into(), node(serde_json::json!({"type": "Session"})));
        core.add_node("s2".into(), node(serde_json::json!({"type": "Session"})));
        for ev in ["view", "add_cart", "checkout"] {
            core.add_node(ev.into(), node(serde_json::json!({"type": "Event"})));
        }
        for owner in ["s1", "s2"] {
            for ev in ["view", "add_cart", "checkout"] {
                let _ = core.add_edge(owner.into(), ev.into(), node(serde_json::json!({})));
            }
        }
        let m = Method::MineSequence {
            sequences: Vec::new(),
            source: Some(SequenceSource {
                node_label: "Session".into(),
                direction: "out".into(),
                item_field: None, // neighbor node id ⇒ "view"/"add_cart"/"checkout"
                relation: None,
                limit: 0,
            }),
            min_support: 0.5,
            algorithm: MineSeqAlgorithm::Gsp,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(13, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_sequences"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written > 0);
        core.mark_dirty();
        let pattern_nodes = core.get_nodes_by_label("SequentialPattern", 0);
        assert_eq!(pattern_nodes.len() as u64, written);
        // The full 3-item pattern must have been recovered (both sessions match).
        let patterns = v["patterns"].as_array().unwrap();
        assert!(patterns
            .iter()
            .any(|p| { p["items"] == serde_json::json!(["view", "add_cart", "checkout"]) }));
    }

    #[test]
    fn forecast_arima_and_writeback() {
        let core = Arc::new(GraphCore::new());
        let values: Vec<f64> = (0..30).map(|t| 5.0 + 3.0 * t as f64).collect();
        let m = Method::MineForecast {
            values: values.clone(),
            algorithm: ForecastAlgorithm::Arima,
            horizon: 5,
            p: 1,
            d: 1,
            q: 0,
            period: 0,
            alpha: 0.3,
            beta: 0.1,
            gamma: 0.1,
            confidence: 0.95,
            series_id: "metric1".into(),
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(17, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        let forecast_vals = v["forecast"].as_array().unwrap();
        assert_eq!(forecast_vals.len(), 5);
        // A pure linear trend (5 + 3t) should extrapolate close to truth at h=1..5.
        for (h, fv) in forecast_vals.iter().enumerate() {
            let t = 30 + h;
            let truth = 5.0 + 3.0 * t as f64;
            let got = fv.as_f64().unwrap();
            assert!(
                (got - truth).abs() < 3.0,
                "forecast[{h}]={got} truth={truth}"
            );
        }
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 1);
        core.mark_dirty();
        let forecast_nodes = core.get_nodes_by_label("Forecast", 0);
        assert_eq!(forecast_nodes.len(), 1);
    }

    #[test]
    fn forecast_missing_values_is_error() {
        let core = Arc::new(GraphCore::new());
        let m = Method::MineForecast {
            values: Vec::new(),
            algorithm: ForecastAlgorithm::Arima,
            horizon: 5,
            p: 1,
            d: 1,
            q: 0,
            period: 0,
            alpha: 0.3,
            beta: 0.1,
            gamma: 0.1,
            confidence: 0.95,
            series_id: String::new(),
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(19, core, m).expect("handled");
        assert!(resp.result.is_none());
    }

    #[test]
    fn forecast_holtwinters_seasonal() {
        let core = Arc::new(GraphCore::new());
        let values: Vec<f64> = (0..48)
            .map(|t| {
                10.0 + 2.0 * t as f64 + 5.0 * (2.0 * std::f64::consts::PI * t as f64 / 12.0).sin()
            })
            .collect();
        let m = Method::MineForecast {
            values,
            algorithm: ForecastAlgorithm::Holtwinters,
            horizon: 12,
            p: 1,
            d: 1,
            q: 0,
            period: 12,
            alpha: 0.5,
            beta: 0.3,
            gamma: 0.3,
            confidence: 0.95,
            series_id: String::new(),
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(21, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        let lower = v["lower"].as_array().unwrap();
        let upper = v["upper"].as_array().unwrap();
        assert_eq!(lower.len(), 12);
        for i in 0..12 {
            assert!(lower[i].as_f64().unwrap() <= upper[i].as_f64().unwrap());
        }
    }

    fn words(s: &str) -> Vec<String> {
        s.split_whitespace().map(|w| w.to_string()).collect()
    }

    #[test]
    fn text_tfidf_explicit_docs() {
        let core = Arc::new(GraphCore::new());
        let docs = vec![
            words("the cat sat on the mat"),
            words("the dog ran in the park"),
            words("the rocket launched into orbit"),
        ];
        let m = Method::MineText {
            docs,
            source: None,
            algorithm: TextAlgorithm::Tfidf,
            k: 3,
            alpha: 0.1,
            beta: 0.01,
            iterations: 200,
            seed: 1,
            top_n: 10,
            writeback: true, // ignored for tfidf
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(23, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_docs"], 3);
        assert_eq!(v["written_back"], 0); // tfidf never writes back
        let doc_terms = v["doc_terms"].as_array().unwrap();
        assert_eq!(doc_terms.len(), 3);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("Topic", 0).len(), 0);
    }

    #[test]
    fn text_lda_graph_derived_source_and_writeback() {
        let core = Arc::new(GraphCore::new());
        let pet_words = ["cat", "dog", "pet", "leash", "vet"];
        let fin_words = ["stock", "market", "bond", "yield", "trader"];
        for i in 0..15 {
            let n = 6 + (i % 4);
            let pet_text: String = (0..n)
                .map(|j| pet_words[(i + j) % pet_words.len()])
                .collect::<Vec<_>>()
                .join(" ");
            let fin_text: String = (0..n)
                .map(|j| fin_words[(i + j) % fin_words.len()])
                .collect::<Vec<_>>()
                .join(" ");
            core.add_node(
                format!("doc_pet_{i}"),
                node(serde_json::json!({"type": "Doc", "body": pet_text})),
            );
            core.add_node(
                format!("doc_fin_{i}"),
                node(serde_json::json!({"type": "Doc", "body": fin_text})),
            );
        }
        let m = Method::MineText {
            docs: Vec::new(),
            source: Some(TextSource {
                node_label: "Doc".into(),
                field: "body".into(),
                limit: 0,
            }),
            algorithm: TextAlgorithm::Lda,
            k: 2,
            alpha: 0.1,
            beta: 0.01,
            iterations: 200,
            seed: 42,
            top_n: 5,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(25, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_docs"], 30);
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 2);
        core.mark_dirty();
        let topic_nodes = core.get_nodes_by_label("Topic", 0);
        assert_eq!(topic_nodes.len(), 2);
        // Every doc must have exactly one HAS_TOPIC edge (its dominant topic).
        for i in 0..15 {
            for prefix in ["doc_pet_", "doc_fin_"] {
                let id = format!("{prefix}{i}");
                let succ = core.get_successors(&id).unwrap();
                let topic_edges: Vec<&String> =
                    succ.iter().filter(|s| s.starts_with("topic:")).collect();
                assert_eq!(
                    topic_edges.len(),
                    1,
                    "doc {id} should link to exactly one topic"
                );
            }
        }
    }

    #[test]
    fn text_nmf_explicit_docs_topics_and_doc_topics() {
        let core = Arc::new(GraphCore::new());
        let docs = vec![
            words("cat dog pet leash vet cat dog"),
            words("stock market bond yield trader stock market"),
            words("cat dog pet vet leash"),
            words("bond yield trader stock market"),
        ];
        let m = Method::MineText {
            docs,
            source: None,
            algorithm: TextAlgorithm::Nmf,
            k: 2,
            alpha: 0.1,
            beta: 0.01,
            iterations: 200,
            seed: 7,
            top_n: 5,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(27, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        let topics = v["topics"].as_array().unwrap();
        assert_eq!(topics.len(), 2);
        let doc_topics = v["doc_topics"].as_array().unwrap();
        assert_eq!(doc_topics.len(), 4);
        assert_eq!(v["written_back"], 0); // writeback=false
    }

    #[test]
    fn subgraph_gspan_recovers_planted_pattern_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Plant 4 instances of :Concept --touches--> :Capability, plus a
        // handful of unrelated noise nodes/edges under different types.
        for i in 0..4 {
            core.add_node(
                format!("concept_{i}"),
                node(serde_json::json!({"type": "Concept"})),
            );
            core.add_node(
                format!("capability_{i}"),
                node(serde_json::json!({"type": "Capability"})),
            );
            let _ = core.add_edge(
                format!("concept_{i}"),
                format!("capability_{i}"),
                node(serde_json::json!({"relationship": "touches"})),
            );
        }
        core.add_node("noise_a".into(), node(serde_json::json!({"type": "Noise"})));
        core.add_node("noise_b".into(), node(serde_json::json!({"type": "Noise"})));
        let _ = core.add_edge(
            "noise_a".into(),
            "noise_b".into(),
            node(serde_json::json!({"relationship": "unrelated"})),
        );

        let m = Method::MineSubgraph {
            label: None,
            min_support: 0.1,
            max_edges: 1,
            algorithm: SubgraphAlgorithm::Gspan,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(29, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_host_nodes"], 10);
        assert_eq!(v["n_host_edges"], 5);
        let patterns = v["patterns"].as_array().unwrap();
        let hit = patterns.iter().find(|p| {
            let nodes = p["nodes"].as_array().unwrap();
            let has_concept = nodes.iter().any(|n| n == "Concept");
            let has_capability = nodes.iter().any(|n| n == "Capability");
            has_concept && has_capability && p["edges"].as_array().unwrap().len() == 1
        });
        assert!(
            hit.is_some(),
            "planted pattern not in response: {patterns:?}"
        );
        assert_eq!(hit.unwrap()["count"], 4);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written > 0);
        core.mark_dirty();
        let subgraph_nodes = core.get_nodes_by_label("FrequentSubgraph", 0);
        assert_eq!(subgraph_nodes.len() as u64, written);
        // The planted pattern's :FrequentSubgraph must link to all 8 involved nodes.
        let sg_id = subgraph_nodes
            .iter()
            .find(|(_, blob)| {
                let props: serde_json::Value = rmp_serde::from_slice(blob).unwrap();
                let nodes = props["nodes"].as_array().unwrap();
                nodes.iter().any(|n| n == "Concept") && nodes.iter().any(|n| n == "Capability")
            })
            .map(|(id, _)| id.clone())
            .expect("planted subgraph node present");
        let members = core.get_successors(&sg_id).unwrap();
        assert_eq!(members.len(), 8); // 4 concept + 4 capability nodes
    }

    #[test]
    fn subgraph_motif_census_is_readonly() {
        let core = Arc::new(GraphCore::new());
        for i in 0..3 {
            core.add_node(format!("n{i}"), node(serde_json::json!({"type": "N"})));
        }
        let _ = core.add_edge(
            "n0".into(),
            "n1".into(),
            node(serde_json::json!({"relationship": "e"})),
        );
        let _ = core.add_edge(
            "n1".into(),
            "n2".into(),
            node(serde_json::json!({"relationship": "e"})),
        );
        let _ = core.add_edge(
            "n2".into(),
            "n0".into(),
            node(serde_json::json!({"relationship": "e"})),
        );

        let m = Method::MineSubgraph {
            label: None,
            min_support: 0.1,
            max_edges: 3,
            algorithm: SubgraphAlgorithm::Motif,
            writeback: true, // ignored for motif
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(31, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["motifs"]["triangle"], 1);
        assert_eq!(v["motifs"]["directed_cycle3"], 1);
        assert_eq!(v["written_back"], 0);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("FrequentSubgraph", 0).len(), 0);
    }

    #[test]
    fn subgraph_label_filter_restricts_host_graph() {
        let core = Arc::new(GraphCore::new());
        core.add_node("a".into(), node(serde_json::json!({"type": "A"})));
        core.add_node("b".into(), node(serde_json::json!({"type": "B"})));
        core.add_node("a2".into(), node(serde_json::json!({"type": "A"})));
        let _ = core.add_edge(
            "a".into(),
            "b".into(),
            node(serde_json::json!({"relationship": "e"})),
        );
        let _ = core.add_edge(
            "a".into(),
            "a2".into(),
            node(serde_json::json!({"relationship": "e"})),
        );

        let m = Method::MineSubgraph {
            label: Some("A".into()),
            min_support: 0.1,
            max_edges: 1,
            algorithm: SubgraphAlgorithm::Gspan,
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(33, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        // Only the two A nodes + the a->a2 edge should be in the filtered host
        // graph (a->b is excluded since b is not type A).
        assert_eq!(v["n_host_nodes"], 2);
        assert_eq!(v["n_host_edges"], 1);
    }

    // ─────────────── E6: mining → epistemic objects (feature `epistemic`) ───────────────

    /// Assert `as_claim=false` left NO epistemic objects — the write-back is
    /// byte-identical to the pre-E6 path.
    #[cfg(feature = "epistemic")]
    fn assert_no_claims(core: &GraphCore) {
        core.mark_dirty();
        assert!(
            core.get_nodes_by_label("Claim", 0).is_empty(),
            "as_claim=false must not materialize Claim nodes"
        );
        assert!(
            core.get_nodes_by_label("Evidence", 0).is_empty(),
            "as_claim=false must not materialize Evidence nodes"
        );
    }

    /// Assert `as_claim=true` materialized `:Claim` (+ `:Evidence` + `:Activity`)
    /// objects: the claim has `validation_state="unvalidated"`, a confidence in
    /// `[0,1]`, a `SUPPORTS` in-edge the `eg_epistemic` belief layer recognizes +
    /// propagates over, AND the CONCEPT:EG-P3-1 universal writeback-lineage tuple
    /// (input-snapshot version, algo family/code/env version, an honest `null`
    /// calibration slot, `invalidation_deps`, and a `GENERATED_BY` edge to an
    /// `:Activity` node). Returns `(first claim id, its stored confidence)` for the
    /// caller's family-specific check.
    #[cfg(feature = "epistemic")]
    fn assert_claim_objects(core: &GraphCore) -> (String, f64) {
        core.mark_dirty();
        let claims = core.get_nodes_by_label("Claim", 0);
        assert!(
            !claims.is_empty(),
            "as_claim=true must materialize Claim nodes"
        );
        assert!(
            !core.get_nodes_by_label("Evidence", 0).is_empty(),
            "as_claim=true must materialize Evidence nodes"
        );
        assert!(
            !core.get_nodes_by_label("Activity", 0).is_empty(),
            "as_claim=true must materialize an Activity node (CONCEPT:EG-P3-1)"
        );
        let (claim_id, blob) = &claims[0];
        let props: serde_json::Value = rmp_serde::from_slice(blob).unwrap();
        assert_eq!(props["type"], "Claim");
        assert_eq!(props["validation_state"], "unvalidated");
        let conf = props["confidence"].as_f64().unwrap();
        assert!(
            (0.0..=1.0).contains(&conf),
            "claim confidence {conf} out of [0,1]"
        );
        // CONCEPT:EG-P3-1 — the universal writeback-lineage tuple.
        assert!(props["input_snapshot_version"].as_u64().is_some());
        assert!(props["algo_family"].as_str().is_some());
        assert!(props["algo_code_version"].as_str().is_some());
        assert!(props["algo_env_version"].as_str().is_some());
        assert!(
            props["calibration"].is_null(),
            "calibration is honestly null — no signal computed by the generic path"
        );
        let deps = props["invalidation_deps"]
            .as_array()
            .expect("invalidation_deps must be an array");
        assert!(!deps.is_empty(), "invalidation_deps must be non-empty");

        // The DERIVED/SUPPORTS edge is understood verbatim by the epistemic layer;
        // the DERIVED/GENERATED_BY edge is NOT (it stays epistemically neutral).
        let view = core.analysis_snapshot();
        let bg = eg_epistemic::BeliefGraph::from_graph_view(&view);
        let ins = bg
            .in_edges
            .get(claim_id)
            .expect("claim must have supporters");
        assert!(
            ins.iter()
                .any(|(_, k)| matches!(k, eg_epistemic::EdgeKind::Supports)),
            "claim must carry a SUPPORTS in-edge"
        );
        assert!(
            ins.iter().all(|(_, k)| !matches!(
                k,
                eg_epistemic::EdgeKind::Contradicts | eg_epistemic::EdgeKind::Attacks
            )),
            "as_claim writeback must never self-contradict"
        );
        let bs = eg_epistemic::propagate_confidence(
            &bg,
            claim_id,
            &eg_epistemic::AuthorityPolicy::default(),
        );
        assert!((0.0..=1.0).contains(&bs.confidence));

        // The claim's own outgoing GENERATED_BY edge resolves to a resident Activity.
        let successors = core.get_successors(claim_id).unwrap_or_default();
        let activity_id = successors
            .into_iter()
            .find(|nbr| {
                core.get_edge_properties(claim_id, nbr).iter().any(|blob| {
                    rmp_serde::from_slice::<serde_json::Value>(blob)
                        .ok()
                        .and_then(|v| {
                            v.get("relationship")
                                .and_then(|r| r.as_str())
                                .map(str::to_string)
                        })
                        .as_deref()
                        == Some("GENERATED_BY")
                })
            })
            .expect("claim must carry an outgoing GENERATED_BY edge");
        assert!(core.has_node(&activity_id));
        let activity_blob = core.get_node_properties(&activity_id).unwrap();
        let activity_props: serde_json::Value = rmp_serde::from_slice(&activity_blob).unwrap();
        assert_eq!(activity_props["type"], "Activity");

        (claim_id.clone(), conf)
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn associate_as_claim_materializes_claim_and_evidence() {
        let build = |core: &GraphCore| {
            core.add_node("cart1".into(), node(serde_json::json!({"type": "Cart"})));
            core.add_node("cart2".into(), node(serde_json::json!({"type": "Cart"})));
            for item in ["milk", "bread"] {
                core.add_node(item.into(), node(serde_json::json!({"type": "Item"})));
            }
            for owner in ["cart1", "cart2"] {
                for item in ["milk", "bread"] {
                    let _ = core.add_edge(owner.into(), item.into(), node(serde_json::json!({})));
                }
            }
        };
        let mk = |as_claim: bool| Method::MineAssociate {
            transactions: Vec::new(),
            source: Some(TransactionSource {
                node_label: "Cart".into(),
                direction: "out".into(),
                item_field: None,
                relation: None,
                limit: 0,
            }),
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: MineAlgorithm::Fpgrowth,
            writeback: true,
            as_claim,
        };
        // as_claim=false ⇒ unchanged (only the mined :AssociationRule nodes).
        let c0 = Arc::new(GraphCore::new());
        build(&c0);
        dispatch_for_test(1, Arc::clone(&c0), mk(false)).expect("handled");
        assert_no_claims(&c0);
        // as_claim=true ⇒ Claim + Evidence, milk⇒bread has support=confidence=1 ⇒ conf=1.
        let c1 = Arc::new(GraphCore::new());
        build(&c1);
        dispatch_for_test(2, Arc::clone(&c1), mk(true)).expect("handled");
        let (_, conf) = assert_claim_objects(&c1);
        assert!(
            (conf - 1.0).abs() < 1e-9,
            "assoc claim confidence {conf} != 1.0"
        );
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn cluster_as_claim_materializes_claim_and_evidence() {
        let build = |core: &GraphCore| {
            let embs = [
                ("d0", [0.0f32, 0.0]),
                ("d1", [0.2, 0.1]),
                ("d2", [0.1, 0.2]),
                ("d3", [9.0, 9.0]),
                ("d4", [9.2, 8.9]),
                ("d5", [8.9, 9.1]),
            ];
            for (id, e) in embs {
                core.add_node(id.into(), node(serde_json::json!({"type": "Doc"})));
                core.semantic_store
                    .write()
                    .add_embedding(id.to_string(), e.to_vec())
                    .unwrap();
            }
        };
        let mk = |as_claim: bool| Method::MineCluster {
            features: Vec::new(),
            source: Some(VectorSource {
                node_label: "Doc".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            algorithm: ClusterAlgorithm::Kmedoids,
            eps: 0.5,
            min_pts: 5,
            k: 2,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: true,
            as_claim,
        };
        let c0 = Arc::new(GraphCore::new());
        build(&c0);
        dispatch_for_test(3, Arc::clone(&c0), mk(false)).expect("handled");
        assert_no_claims(&c0);
        let c1 = Arc::new(GraphCore::new());
        build(&c1);
        dispatch_for_test(4, Arc::clone(&c1), mk(true)).expect("handled");
        let (_, conf) = assert_claim_objects(&c1);
        // Two tight clusters ⇒ compactness score small ⇒ conf = 1/(1+score) close to 1.
        assert!(
            conf > 0.5 && conf <= 1.0,
            "cluster claim confidence {conf} unexpected"
        );
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn anomaly_as_claim_materializes_claim_and_evidence() {
        let build = |core: &GraphCore| {
            let embs = [
                ("m0", [0.0f32, 0.0]),
                ("m1", [0.1, 0.0]),
                ("m2", [0.0, 0.1]),
                ("m3", [0.1, 0.1]),
                ("m4", [0.05, 0.05]),
                ("m5", [50.0, 50.0]), // outlier
            ];
            for (id, e) in embs {
                core.add_node(id.into(), node(serde_json::json!({"type": "Metric"})));
                core.semantic_store
                    .write()
                    .add_embedding(id.to_string(), e.to_vec())
                    .unwrap();
            }
        };
        let mk = |as_claim: bool| Method::MineAnomaly {
            features: Vec::new(),
            values: Vec::new(),
            source: Some(VectorSource {
                node_label: "Metric".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            algorithm: AnomalyAlgorithm::Zscore,
            k: 20,
            n_trees: 100,
            sample_size: 256,
            seed: 0,
            nu: 0.1,
            gamma: 0.0,
            kernel: SvmKernel::Rbf,
            threshold: None,
            writeback: true,
            as_claim,
        };
        let c0 = Arc::new(GraphCore::new());
        build(&c0);
        dispatch_for_test(5, Arc::clone(&c0), mk(false)).expect("handled");
        assert_no_claims(&c0);
        let c1 = Arc::new(GraphCore::new());
        build(&c1);
        dispatch_for_test(6, Arc::clone(&c1), mk(true)).expect("handled");
        let (_, conf) = assert_claim_objects(&c1);
        // Anomaly confidence = score/(1+score) ∈ (0,1).
        assert!(
            conf > 0.0 && conf < 1.0,
            "anomaly claim confidence {conf} unexpected"
        );
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn sequence_as_claim_materializes_claim_and_evidence() {
        let build = |core: &GraphCore| {
            core.add_node("s1".into(), node(serde_json::json!({"type": "Session"})));
            core.add_node("s2".into(), node(serde_json::json!({"type": "Session"})));
            for ev in ["view", "add_cart", "checkout"] {
                core.add_node(ev.into(), node(serde_json::json!({"type": "Event"})));
            }
            for owner in ["s1", "s2"] {
                for ev in ["view", "add_cart", "checkout"] {
                    let _ = core.add_edge(owner.into(), ev.into(), node(serde_json::json!({})));
                }
            }
        };
        let mk = |as_claim: bool| Method::MineSequence {
            sequences: Vec::new(),
            source: Some(SequenceSource {
                node_label: "Session".into(),
                direction: "out".into(),
                item_field: None,
                relation: None,
                limit: 0,
            }),
            min_support: 0.5,
            algorithm: MineSeqAlgorithm::Gsp,
            writeback: true,
            as_claim,
        };
        let c0 = Arc::new(GraphCore::new());
        build(&c0);
        dispatch_for_test(7, Arc::clone(&c0), mk(false)).expect("handled");
        assert_no_claims(&c0);
        let c1 = Arc::new(GraphCore::new());
        build(&c1);
        dispatch_for_test(8, Arc::clone(&c1), mk(true)).expect("handled");
        let (_, conf) = assert_claim_objects(&c1);
        // Both sessions share every subsequence ⇒ support=1 ⇒ conf=1.
        assert!(
            (conf - 1.0).abs() < 1e-9,
            "sequence claim confidence {conf} != 1.0"
        );
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn forecast_as_claim_materializes_claim_and_evidence() {
        let values: Vec<f64> = (0..30).map(|t| 5.0 + 3.0 * t as f64).collect();
        let mk = |as_claim: bool| Method::MineForecast {
            values: values.clone(),
            algorithm: ForecastAlgorithm::Arima,
            horizon: 5,
            p: 1,
            d: 1,
            q: 0,
            period: 0,
            alpha: 0.3,
            beta: 0.1,
            gamma: 0.1,
            confidence: 0.95,
            series_id: "metric1".into(),
            writeback: true,
            as_claim,
        };
        let c0 = Arc::new(GraphCore::new());
        c0.add_node(
            "metric1".into(),
            node(serde_json::json!({"type": "Series"})),
        );
        dispatch_for_test(9, Arc::clone(&c0), mk(false)).expect("handled");
        assert_no_claims(&c0);
        let c1 = Arc::new(GraphCore::new());
        c1.add_node(
            "metric1".into(),
            node(serde_json::json!({"type": "Series"})),
        );
        dispatch_for_test(10, Arc::clone(&c1), mk(true)).expect("handled");
        let (_, conf) = assert_claim_objects(&c1);
        // Forecast claim confidence = the band level.
        assert!(
            (conf - 0.95).abs() < 1e-9,
            "forecast claim confidence {conf} != 0.95"
        );
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn subgraph_as_claim_materializes_claim_and_evidence() {
        let build = |core: &GraphCore| {
            for i in 0..4 {
                core.add_node(
                    format!("concept_{i}"),
                    node(serde_json::json!({"type": "Concept"})),
                );
                core.add_node(
                    format!("capability_{i}"),
                    node(serde_json::json!({"type": "Capability"})),
                );
                let _ = core.add_edge(
                    format!("concept_{i}"),
                    format!("capability_{i}"),
                    node(serde_json::json!({"relationship": "touches"})),
                );
            }
            core.add_node("noise_a".into(), node(serde_json::json!({"type": "Noise"})));
            core.add_node("noise_b".into(), node(serde_json::json!({"type": "Noise"})));
            let _ = core.add_edge(
                "noise_a".into(),
                "noise_b".into(),
                node(serde_json::json!({"relationship": "unrelated"})),
            );
        };
        let mk = |as_claim: bool| Method::MineSubgraph {
            label: None,
            min_support: 0.1,
            max_edges: 1,
            algorithm: SubgraphAlgorithm::Gspan,
            writeback: true,
            as_claim,
        };
        let c0 = Arc::new(GraphCore::new());
        build(&c0);
        dispatch_for_test(11, Arc::clone(&c0), mk(false)).expect("handled");
        assert_no_claims(&c0);
        let c1 = Arc::new(GraphCore::new());
        build(&c1);
        dispatch_for_test(12, Arc::clone(&c1), mk(true)).expect("handled");
        assert_claim_objects(&c1);
        // The planted concept→capability pattern has support 4/5 = 0.8 ⇒ some claim
        // must carry that confidence.
        c1.mark_dirty();
        let has_08 = c1.get_nodes_by_label("Claim", 0).iter().any(|(_, blob)| {
            let p: serde_json::Value = rmp_serde::from_slice(blob).unwrap();
            p["confidence"]
                .as_f64()
                .map(|c| (c - 0.8).abs() < 1e-9)
                .unwrap_or(false)
        });
        assert!(
            has_08,
            "subgraph claim for the planted pattern (support 0.8) missing"
        );
    }

    /// The first E1↔E6 end-to-end proof (CONCEPT:EG-KG.epistemic.epistemic-substrate):
    /// two `MineAssociate` runs over OVERLAPPING data with DISTINCT provenance
    /// corroborate the SAME `:Claim` with a SECOND `:Evidence`; building a
    /// `BeliefGraph::from_graph_view` and running `eg_epistemic::propagate_confidence`
    /// shows two corroborating runs raise the belief above a single one.
    #[test]
    #[cfg(feature = "epistemic")]
    fn corroboration_two_runs_raise_belief_above_one() {
        use eg_epistemic::{propagate_confidence, AuthorityPolicy, BeliefGraph};
        let core = Arc::new(GraphCore::new());
        // Two owner labels (CartA / CartB) whose baskets BOTH yield {milk, bread}.
        for c in ["ca1", "ca2"] {
            core.add_node(c.into(), node(serde_json::json!({"type": "CartA"})));
        }
        for c in ["cb1", "cb2"] {
            core.add_node(c.into(), node(serde_json::json!({"type": "CartB"})));
        }
        for item in ["milk", "bread"] {
            core.add_node(item.into(), node(serde_json::json!({"type": "Item"})));
        }
        for owner in ["ca1", "ca2", "cb1", "cb2"] {
            for item in ["milk", "bread"] {
                let _ = core.add_edge(owner.into(), item.into(), node(serde_json::json!({})));
            }
        }
        let mk = |label: &str| Method::MineAssociate {
            transactions: Vec::new(),
            source: Some(TransactionSource {
                node_label: label.into(),
                direction: "out".into(),
                item_field: None,
                relation: None,
                limit: 0,
            }),
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: MineAlgorithm::Fpgrowth,
            writeback: true,
            as_claim: true,
        };
        let policy = AuthorityPolicy::default();

        // Run 1 (provenance CartA) ⇒ the claim has ONE provenance evidence.
        dispatch_for_test(1, Arc::clone(&core), mk("CartA")).expect("handled");
        core.mark_dirty();
        let claim_id = core.get_nodes_by_label("Claim", 0)[0].0.clone();
        let bg1 = BeliefGraph::from_graph_view(&core.analysis_snapshot());
        let belief_one = propagate_confidence(&bg1, &claim_id, &policy).confidence;

        // Run 2 (DISTINCT provenance CartB, same rule ⇒ same claim) ⇒ a SECOND evidence.
        dispatch_for_test(2, Arc::clone(&core), mk("CartB")).expect("handled");
        core.mark_dirty();
        let bg2 = BeliefGraph::from_graph_view(&core.analysis_snapshot());
        let belief_two = propagate_confidence(&bg2, &claim_id, &policy).confidence;

        assert!(
            belief_two > belief_one,
            "two corroborating runs ({belief_two}) must raise belief above one ({belief_one})"
        );
        // Distinct provenance ⇒ ≥2 Evidence nodes corroborating the shared claim(s).
        assert!(
            core.get_nodes_by_label("Evidence", 0).len() >= 2,
            "distinct provenance must yield ≥2 Evidence nodes"
        );
    }

    // ── D3 — the 3 remaining mining families get `as_claim` too ──

    #[test]
    #[cfg(feature = "epistemic")]
    fn classify_predict_as_claim_materializes_claim_and_evidence() {
        let x = vec![
            vec![0.0, 0.0],
            vec![0.2, 0.1],
            vec![9.0, 9.0],
            vec![9.1, 8.8],
        ];
        let y = vec![0, 0, 1, 1];
        let model = eg_compute::mining::classify::fit(
            &x,
            &y,
            eg_compute::mining::classify::Algorithm::GaussianNb,
        )
        .unwrap();
        let build = |core: &GraphCore| {
            for (id, e) in [("p0", [0.1f32, 0.0]), ("p1", [9.0, 9.1])] {
                core.add_node(id.into(), node(serde_json::json!({"type": "Sample"})));
                core.semantic_store
                    .write()
                    .add_embedding(id.to_string(), e.to_vec())
                    .unwrap();
            }
        };
        let mk = |model: FittedClassifier, as_claim: bool| Method::MineClassifyPredict {
            model,
            x: Vec::new(),
            source: Some(VectorSource {
                node_label: "Sample".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            writeback: true,
            as_claim,
        };
        let c0 = Arc::new(GraphCore::new());
        build(&c0);
        dispatch_for_test(28, Arc::clone(&c0), mk(model.clone(), false)).expect("handled");
        assert_no_claims(&c0);
        let c1 = Arc::new(GraphCore::new());
        build(&c1);
        dispatch_for_test(29, Arc::clone(&c1), mk(model, true)).expect("handled");
        let (_, conf) = assert_claim_objects(&c1);
        // GaussianNB on two well-separated Gaussians ⇒ near-certain max class proba.
        assert!(
            conf > 0.9,
            "classification claim confidence {conf} unexpectedly low"
        );
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn reduce_svd_as_claim_materializes_claim_and_evidence() {
        let build = |core: &GraphCore| {
            let embs = [
                ("d0", [1.0f32, 0.0, 0.0]),
                ("d1", [0.0, 1.0, 0.0]),
                ("d2", [1.0, 1.0, 0.0]),
                ("d3", [2.0, 1.0, 0.0]),
            ];
            for (id, e) in embs {
                core.add_node(id.into(), node(serde_json::json!({"type": "Vec"})));
                core.semantic_store
                    .write()
                    .add_embedding(id.to_string(), e.to_vec())
                    .unwrap();
            }
        };
        let mk = |as_claim: bool| Method::MineReduce {
            x: Vec::new(),
            source: Some(VectorSource {
                node_label: "Vec".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            labels: Vec::new(),
            algorithm: ReduceAlgorithm::Svd,
            n_components: 2,
            n_neighbors: 15,
            min_dist: 0.1,
            perplexity: 30.0,
            epochs: 300,
            lr: 100.0,
            seed: 0,
            writeback: true,
            as_claim,
        };
        let c0 = Arc::new(GraphCore::new());
        build(&c0);
        dispatch_for_test(30, Arc::clone(&c0), mk(false)).expect("handled");
        assert_no_claims(&c0);
        let c1 = Arc::new(GraphCore::new());
        build(&c1);
        dispatch_for_test(31, Arc::clone(&c1), mk(true)).expect("handled");
        let (_, conf) = assert_claim_objects(&c1);
        // These 4 vectors are rank-3 (embedded in R^3); keeping k=2 of 3 components
        // must retain SOME but not necessarily ALL variance.
        assert!(
            conf > 0.0 && conf <= 1.0,
            "reduce claim confidence {conf} out of (0,1]"
        );
        // Every materialized :Embedding2D row gets a claim sharing the SAME
        // reduction-level explained-variance-ratio score.
        c1.mark_dirty();
        let claims = c1.get_nodes_by_label("Claim", 0);
        assert_eq!(claims.len(), 4, "one claim per materialized row");
        for (_, blob) in &claims {
            let props: serde_json::Value = rmp_serde::from_slice(blob).unwrap();
            assert!((props["confidence"].as_f64().unwrap() - conf).abs() < 1e-9);
        }
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn reduce_umap_as_claim_is_a_documented_noop() {
        // UMAP has no principled [0,1] quality score (approximate neighborhood
        // layout, no reconstruction-error analogue) — as_claim=true must be a no-op,
        // never a fabricated confidence.
        let core = Arc::new(GraphCore::new());
        let m = Method::MineReduce {
            x: vec![
                vec![0.0, 0.0],
                vec![1.0, 1.0],
                vec![2.0, 0.5],
                vec![0.5, 2.0],
            ],
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            labels: Vec::new(),
            algorithm: ReduceAlgorithm::Umap,
            n_components: 2,
            n_neighbors: 2,
            min_dist: 0.1,
            perplexity: 30.0,
            epochs: 50,
            lr: 100.0,
            seed: 0,
            writeback: true,
            as_claim: true,
        };
        dispatch_for_test(32, Arc::clone(&core), m).expect("handled");
        assert_no_claims(&core);
    }

    #[test]
    #[cfg(feature = "epistemic")]
    fn text_lda_as_claim_materializes_claim_and_evidence() {
        let pet_words = ["cat", "dog", "pet", "leash", "vet"];
        let fin_words = ["stock", "market", "bond", "yield", "trader"];
        let build = |core: &GraphCore| {
            for i in 0..15 {
                let n = 6 + (i % 4);
                let pet_text: String = (0..n)
                    .map(|j| pet_words[(i + j) % pet_words.len()])
                    .collect::<Vec<_>>()
                    .join(" ");
                let fin_text: String = (0..n)
                    .map(|j| fin_words[(i + j) % fin_words.len()])
                    .collect::<Vec<_>>()
                    .join(" ");
                core.add_node(
                    format!("doc_pet_{i}"),
                    node(serde_json::json!({"type": "Doc", "body": pet_text})),
                );
                core.add_node(
                    format!("doc_fin_{i}"),
                    node(serde_json::json!({"type": "Doc", "body": fin_text})),
                );
            }
        };
        let mk = |as_claim: bool| Method::MineText {
            docs: Vec::new(),
            source: Some(TextSource {
                node_label: "Doc".into(),
                field: "body".into(),
                limit: 0,
            }),
            algorithm: TextAlgorithm::Lda,
            k: 2,
            alpha: 0.1,
            beta: 0.01,
            iterations: 200,
            seed: 42,
            top_n: 5,
            writeback: true,
            as_claim,
        };
        let c0 = Arc::new(GraphCore::new());
        build(&c0);
        dispatch_for_test(33, Arc::clone(&c0), mk(false)).expect("handled");
        assert_no_claims(&c0);
        let c1 = Arc::new(GraphCore::new());
        build(&c1);
        dispatch_for_test(34, Arc::clone(&c1), mk(true)).expect("handled");
        let (_, conf) = assert_claim_objects(&c1);
        // Two well-separated topics ⇒ high mean dominant-doc membership.
        assert!(
            conf > 0.5,
            "topic claim confidence {conf} unexpectedly low for well-separated topics"
        );
        c1.mark_dirty();
        assert_eq!(
            c1.get_nodes_by_label("Claim", 0).len(),
            2,
            "one claim per topic"
        );
    }

    #[test]
    fn text_tfidf_as_claim_is_a_documented_noop() {
        // tfidf has no topics to claim about — as_claim=true must be a no-op, mirroring
        // its own `writeback` no-op (never a fabricated confidence).
        let core = Arc::new(GraphCore::new());
        let docs = vec![words("the cat sat"), words("the dog ran")];
        let m = Method::MineText {
            docs,
            source: None,
            algorithm: TextAlgorithm::Tfidf,
            k: 3,
            alpha: 0.1,
            beta: 0.01,
            iterations: 200,
            seed: 1,
            top_n: 10,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: true,
        };
        dispatch_for_test(35, Arc::clone(&core), m).expect("handled");
        #[cfg(feature = "epistemic")]
        assert_no_claims(&core);
    }

    // ═══════════════════ Residual insight/mining families — round-trip tests ═══════════════════

    #[test]
    fn entity_resolve_links_records_within_a_block_and_writes_back() {
        let core = Arc::new(GraphCore::new());
        let mk = |as_claim: bool| Method::MineEntityResolve {
            records: vec![
                vec!["john".into(), "smith".into(), "12345".into()],
                vec!["jon".into(), "smith".into(), "12345".into()],
                vec!["mary".into(), "jones".into(), "99999".into()],
            ],
            block_keys: vec!["b".into(), "b".into(), "c".into()],
            vectors: Vec::new(),
            source: None,
            ids: vec!["r1".into(), "r2".into(), "r3".into()],
            bucket_precision: 1,
            threshold: 0.4,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        };
        let resp = dispatch_for_test(101, Arc::clone(&core), mk(false)).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_matches"], 1);
        assert_eq!(v["written_back"], 1);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("EntityMatch", 0).len(), 1);

        // Vector input reaches the same response + node-writeback phase after
        // its cosine-specific compute step.
        let vector_core = Arc::new(GraphCore::new());
        let vector = Method::MineEntityResolve {
            records: Vec::new(),
            block_keys: Vec::new(),
            vectors: vec![vec![1.0, 0.0], vec![1.0, 0.0]],
            source: None,
            ids: vec!["v1".into(), "v2".into()],
            bucket_precision: 1,
            threshold: 0.99,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let resp = dispatch_for_test(102, Arc::clone(&vector_core), vector).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_records"], 2);
        assert_eq!(v["n_matches"], 1);
        assert_eq!(v["written_back"], 1);

        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            dispatch_for_test(103, Arc::clone(&c1), mk(true)).expect("handled");
            let (_, conf) = assert_claim_objects(&c1);
            assert!(conf > 0.4);
        }
    }

    #[test]
    fn causal_impact_detects_a_level_shift_and_writes_back() {
        let core = Arc::new(GraphCore::new());
        let mk = |as_claim: bool| Method::MineCausalImpact {
            series: vec![1.0, 1.1, 0.9, 1.0, 1.05, 5.0, 5.1, 4.9, 5.0, 5.05],
            control: Vec::new(),
            intervention_index: 5,
            series_id: "s1".into(),
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        };
        let resp = dispatch_for_test(103, Arc::clone(&core), mk(false)).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert!((v["effect_size"].as_f64().unwrap() - 4.0).abs() < 1e-6);
        assert_eq!(v["written_back"], 1);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("CausalEffect", 0).len(), 1);
        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            dispatch_for_test(104, Arc::clone(&c1), mk(true)).expect("handled");
            let (_, conf) = assert_claim_objects(&c1);
            assert!(conf > 0.9);
        }
    }

    #[test]
    fn process_mining_derives_a_footprint_and_writes_back() {
        let core = Arc::new(GraphCore::new());
        let mk = |as_claim: bool| Method::MineProcess {
            traces: vec![
                vec!["register".into(), "check".into(), "accept".into()],
                vec!["register".into(), "check".into(), "accept".into()],
            ],
            process_id: "p1".into(),
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        };
        let resp = dispatch_for_test(105, Arc::clone(&core), mk(false)).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_activities"], 3);
        assert_eq!(v["written_back"], 1);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("ProcessModel", 0).len(), 1);
        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            dispatch_for_test(106, Arc::clone(&c1), mk(true)).expect("handled");
            assert_claim_objects(&c1);
        }
    }

    #[test]
    fn root_cause_finds_the_upstream_node_and_writes_back() {
        let core = Arc::new(GraphCore::new());
        let mk = |as_claim: bool| Method::MineRootCause {
            nodes: vec!["n0".into(), "n1".into(), "n2".into()],
            scores: vec![5.0, 0.1, 0.2],
            edges: vec![
                ("n0".into(), "n1".into(), 1.0),
                ("n1".into(), "n2".into(), 1.0),
            ],
            symptom: "n2".into(),
            max_hops: 5,
            decay: 0.9,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        };
        let resp = dispatch_for_test(107, Arc::clone(&core), mk(false)).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["best"], "n0");
        assert_eq!(v["written_back"], 1);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("RootCause", 0).len(), 1);
        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            dispatch_for_test(108, Arc::clone(&c1), mk(true)).expect("handled");
            assert_claim_objects(&c1);
        }
    }

    #[test]
    fn risk_propagation_flows_from_seed_and_writes_back() {
        let core = Arc::new(GraphCore::new());
        let mk = |as_claim: bool| Method::MineRiskPropagation {
            nodes: vec!["n0".into(), "n1".into(), "n2".into()],
            seed: vec![1.0, 0.0, 0.0],
            edges: vec![
                ("n0".into(), "n1".into(), 1.0),
                ("n1".into(), "n2".into(), 1.0),
            ],
            damping: 0.85,
            tolerance: 1e-7,
            max_iterations: 2000,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        };
        let resp = dispatch_for_test(109, Arc::clone(&core), mk(false)).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert!(v["written_back"].as_u64().unwrap() >= 2);
        core.mark_dirty();
        assert!(!core.get_nodes_by_label("RiskScore", 0).is_empty());
        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            dispatch_for_test(110, Arc::clone(&c1), mk(true)).expect("handled");
            assert_claim_objects(&c1);
        }
    }

    #[test]
    fn ontology_gap_flags_a_disconnected_propertyless_class_and_writes_back() {
        let core = Arc::new(GraphCore::new());
        let build = |core: &GraphCore| {
            core.add_node("ClassA".into(), node(serde_json::json!({"type": "Class"})));
        };
        let mk = |as_claim: bool| Method::MineOntologyGap {
            label: None,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        };
        build(&core);
        let resp = dispatch_for_test(111, Arc::clone(&core), mk(false)).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_gaps"], 2); // disconnected AND no_properties
        assert_eq!(v["written_back"], 2);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("OntologyGap", 0).len(), 2);
        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            build(&c1);
            dispatch_for_test(112, Arc::clone(&c1), mk(true)).expect("handled");
            assert_claim_objects(&c1);
        }
    }

    #[test]
    fn retrieval_quality_scores_a_trace_and_writes_back() {
        let core = Arc::new(GraphCore::new());
        let mk = |as_claim: bool| Method::MineRetrievalQuality {
            traces: vec![RetrievalTraceSpec {
                retrieved: vec!["irrelevant".into(), "a".into()],
                relevant: vec!["a".into()],
            }],
            k: 2,
            query_id: "q1".into(),
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        };
        let resp = dispatch_for_test(113, Arc::clone(&core), mk(false)).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert!((v["mrr"].as_f64().unwrap() - 0.5).abs() < 1e-9);
        // GOC-08: NDCG must be present on both the API response AND the
        // materialized node — not just internal to `RetrievalQuality`. Ground
        // truth is a single relevant id ("a") at retrieved rank 2, so DCG@2 =
        // 1/log2(3) and IDCG@2 = 1/log2(2) = 1 ⇒ NDCG = 1/log2(3).
        let expected_ndcg = 1.0 / 3.0f64.log2();
        assert!((v["ndcg_at_k"].as_f64().unwrap() - expected_ndcg).abs() < 1e-9);
        assert_eq!(v["written_back"], 1);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("RetrievalQuality", 0).len(), 1);
        let (_node_id, props_blob) = core
            .get_nodes_by_label("RetrievalQuality", 0)
            .into_iter()
            .next()
            .unwrap();
        let props: serde_json::Value = rmp_serde::from_slice(&props_blob).unwrap();
        assert!(
            (props["ndcg_at_k"].as_f64().unwrap() - expected_ndcg).abs() < 1e-9,
            "the materialized :RetrievalQuality node must carry ndcg_at_k too, \
             not only the API response"
        );
        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            dispatch_for_test(114, Arc::clone(&c1), mk(true)).expect("handled");
            assert_claim_objects(&c1);
        }
    }

    #[test]
    fn community_detects_two_triangles_and_writes_back() {
        let core = Arc::new(GraphCore::new());
        let build = |core: &GraphCore| {
            for id in ["a0", "a1", "a2", "b0", "b1", "b2"] {
                core.add_node(id.into(), node(serde_json::json!({"type": "Node"})));
            }
            let tri_edges = [
                ("a0", "a1"),
                ("a1", "a2"),
                ("a2", "a0"),
                ("b0", "b1"),
                ("b1", "b2"),
                ("b2", "b0"),
            ];
            for (s, t) in tri_edges {
                let _ = core.add_edge(s.into(), t.into(), node(serde_json::json!({})));
            }
        };
        let mk = |as_claim: bool| Method::MineCommunity {
            label: None,
            algorithm: CommunityAlgorithm::Louvain,
            resolution: 1.0,
            max_iterations: 100,
            seed: 0,
            weighted: true,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        };
        build(&core);
        let resp = dispatch_for_test(115, Arc::clone(&core), mk(false)).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["written_back"], 2);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("Community", 0).len(), 2);
        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            build(&c1);
            dispatch_for_test(116, Arc::clone(&c1), mk(true)).expect("handled");
            let (_, conf) = assert_claim_objects(&c1);
            assert!(
                conf > 0.5,
                "tight triangle community should have high density confidence"
            );
        }
    }
}
