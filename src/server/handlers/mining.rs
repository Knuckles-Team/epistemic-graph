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

use eg_compute::graph_algos::AdjacencyGraph;
use eg_compute::mining::anomaly;
use eg_compute::mining::association::{self, Algorithm, LabeledRule};
use eg_compute::mining::causal_impact;
use eg_compute::mining::classify::{self, FittedClassifier};
use eg_compute::mining::cluster;
use eg_compute::mining::community;
use eg_compute::mining::entity_resolution;
use eg_compute::mining::forecast;
use eg_compute::mining::ontology_gap;
use eg_compute::mining::process_mining;
use eg_compute::mining::reduce;
use eg_compute::mining::retrieval_quality;
use eg_compute::mining::risk_propagation;
use eg_compute::mining::root_cause;
use eg_compute::mining::sequence::{self, LabeledPattern};
use eg_compute::mining::subgraph::{self, HostGraph};
use eg_compute::mining::text;

use crate::graph::GraphCore;
use crate::protocol::{
    AnomalyAlgorithm, ClassifyAlgorithm, ClusterAlgorithm, CommunityAlgorithm, ForecastAlgorithm,
    Linkage, Method, MineAlgorithm, MineSeqAlgorithm, ReduceAlgorithm, Response, ResultPayload,
    RetrievalTraceSpec, SequenceSource, SubgraphAlgorithm, SvmKernel, TextAlgorithm, TextSource,
    TransactionSource, VectorSource,
};

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
        Method::MineAssociate { .. } => unreachable!(
            "MineAssociate is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineCluster { .. } => unreachable!(
            "MineCluster is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineAnomaly { .. } => unreachable!(
            "MineAnomaly is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
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
                tsdb_bind,
            ))
        }
        Method::MineClassifyPredict { .. } => unreachable!(
            "MineClassifyPredict is mutation::GATEWAY_ROUTED; dispatch_graph_op \
             must route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineReduce { .. } => unreachable!(
            "MineReduce is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineSequence { .. } => unreachable!(
            "MineSequence is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineForecast { .. } => unreachable!(
            "MineForecast is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineText { .. } => unreachable!(
            "MineText is mutation::GATEWAY_ROUTED; dispatch_graph_op must route \
             it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineSubgraph { .. } => unreachable!(
            "MineSubgraph is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineEntityResolve { .. } => unreachable!(
            "MineEntityResolve is mutation::GATEWAY_ROUTED; dispatch_graph_op \
             must route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineCausalImpact { .. } => unreachable!(
            "MineCausalImpact is mutation::GATEWAY_ROUTED; dispatch_graph_op \
             must route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineProcess { .. } => unreachable!(
            "MineProcess is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineRootCause { .. } => unreachable!(
            "MineRootCause is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineRiskPropagation { .. } => unreachable!(
            "MineRiskPropagation is mutation::GATEWAY_ROUTED; dispatch_graph_op \
             must route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineOntologyGap { .. } => unreachable!(
            "MineOntologyGap is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineRetrievalQuality { .. } => unreachable!(
            "MineRetrievalQuality is mutation::GATEWAY_ROUTED; dispatch_graph_op \
             must route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        Method::MineCommunity { .. } => unreachable!(
            "MineCommunity is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this fallback handler"
        ),
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
            transactions,
            source,
            min_support,
            min_confidence,
            algorithm,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        )),
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
            tsdb_bind,
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
            model,
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
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
            #[cfg(all(feature = "query", feature = "tsdb"))]
            tsdb_bind,
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
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
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
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        )),
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
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
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
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
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
            nodes,
            scores,
            edges,
            symptom,
            max_hops,
            decay,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
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
            nodes,
            seed,
            edges,
            damping,
            tolerance,
            max_iterations,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
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
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
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
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
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
            label,
            algorithm,
            resolution,
            max_iterations,
            seed,
            weighted,
            writeback,
            #[cfg(feature = "epistemic")]
            as_claim,
        )),
        other => Err(other),
    }
}

/// Re-run a `MineAssociate` purely for its write-back side effect on WAL replay
/// (CONCEPT:EG-KG.mining.frequent-itemset-mining). Deterministic for explicit
/// transactions; a graph-derived source re-derives from the current graph state
/// (like the broker/memory replay ops). The response is discarded.
#[allow(dead_code)]
pub(crate) fn replay(core: &GraphCore, method: &Method) {
    match method {
        Method::MineAssociate {
            transactions,
            source,
            min_support,
            min_confidence,
            algorithm,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let txns = match build_transactions(core, transactions, source) {
                Ok(t) => t,
                Err(_) => return,
            };
            let rules = association::mine_labeled(
                &txns,
                *min_support,
                *min_confidence,
                to_algo(*algorithm),
            );
            materialize_rules(core, &rules);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_rule_claims(core, &rules, source);
            }
        }
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
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let (rows, ids) = build_vectors_replay(
                core,
                features,
                source,
                #[cfg(feature = "query")]
                plan,
            );
            if rows.is_empty() {
                return;
            }
            let algo = cluster_algo(*algorithm, *eps, *min_pts, *k, *linkage, *max_iter, *seed);
            let out = cluster::cluster(&rows, algo);
            materialize_clusters(core, &out, &ids, *algorithm);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_cluster_claims(
                    core,
                    &out,
                    &ids,
                    *algorithm,
                    cluster_provenance(source),
                );
            }
        }
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
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let (rows, ids) = build_anomaly_rows_replay(
                core,
                features,
                values,
                source,
                #[cfg(feature = "query")]
                plan,
            );
            if rows.is_empty() {
                return;
            }
            let algo = anomaly_algo(
                *algorithm,
                *k,
                *n_trees,
                *sample_size,
                *seed,
                *nu,
                *gamma,
                *kernel,
            );
            let out = anomaly::detect(&rows, algo, *threshold);
            materialize_anomalies(core, &out, &ids, *algorithm);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_anomaly_claims(
                    core,
                    &out,
                    &ids,
                    *algorithm,
                    anomaly_provenance(source),
                );
            }
        }
        Method::MineClassifyPredict {
            model,
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let (rows, ids) = build_vectors_replay(
                core,
                x,
                source,
                #[cfg(feature = "query")]
                plan,
            );
            if rows.is_empty() {
                return;
            }
            let out = classify::predict(model, &rows);
            materialize_classifications(core, &out, &ids);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_classification_claims(core, &out, &ids, classify_provenance(source));
            }
        }
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
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let (rows, ids) = build_vectors_replay(
                core,
                x,
                source,
                #[cfg(feature = "query")]
                plan,
            );
            if rows.is_empty() {
                return;
            }
            let algo = reduce_algo(
                *algorithm,
                *n_neighbors,
                *min_dist,
                *perplexity,
                *epochs,
                *lr,
                *seed,
            );
            let lbls = (!labels.is_empty()).then_some(labels.as_slice());
            let out = reduce::reduce(&rows, lbls, algo, *n_components);
            materialize_embeddings(core, &out, &ids);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_reduce_claims(core, &rows, &out, &ids, *algorithm, source);
            }
        }
        Method::MineSequence {
            sequences,
            source,
            min_support,
            algorithm,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let seqs = match build_sequences(core, sequences, source) {
                Ok(s) => s,
                Err(_) => return,
            };
            let patterns = sequence::mine_labeled(&seqs, *min_support, to_seq_algo(*algorithm));
            materialize_patterns(core, &patterns);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_sequence_claims(core, &patterns, sequence_provenance(source));
            }
        }
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
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            if values.is_empty() {
                return;
            }
            let algo = forecast_algo(*algorithm, *p, *d, *q, *period, *alpha, *beta, *gamma);
            let out = forecast::forecast(values, algo, *horizon, *confidence);
            materialize_forecast(
                core,
                &out,
                *horizon,
                series_id,
                values,
                forecast_algo_name(*algorithm),
            );
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_forecast_claim(
                    core,
                    series_id,
                    values,
                    forecast_algo_name(*algorithm),
                    *confidence,
                );
            }
        }
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
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            if matches!(algorithm, TextAlgorithm::Tfidf) {
                return; // tfidf has no topics to write back
            }
            let (tokenized, ids) = build_text_docs(core, docs, source);
            if tokenized.is_empty() {
                return;
            }
            let algo = to_text_algo(*algorithm, *k, *alpha, *beta, *iterations, *seed);
            let out = text::mine_labeled(&tokenized, algo, *top_n);
            materialize_topics(core, &out, &ids, text_algo_name(*algorithm));
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_topic_claims(core, &out, text_algo_name(*algorithm));
            }
        }
        Method::MineSubgraph {
            label,
            min_support,
            max_edges,
            algorithm,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            if matches!(algorithm, SubgraphAlgorithm::Motif) {
                return; // motif has no patterns to write back
            }
            let (host, ids) = build_host_graph(core, label);
            if host.node_count() == 0 {
                return;
            }
            let results = subgraph::mine_gspan(&host, *min_support, *max_edges);
            materialize_subgraphs(core, &results, &ids);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_subgraph_claims(core, &results, subgraph_provenance(label));
            }
        }
        Method::MineEntityResolve {
            records,
            block_keys,
            vectors,
            source,
            ids,
            bucket_precision,
            threshold,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            if !records.is_empty() {
                let keys = resolve_block_keys(block_keys, records.len());
                let matches = entity_resolution::link_records(records, &keys, *threshold);
                let resolved_ids = resolve_ids(ids, records.len());
                materialize_entity_matches(core, &matches, &resolved_ids, "jaccard");
                #[cfg(feature = "epistemic")]
                if *as_claim {
                    materialize_entity_match_claims(
                        core,
                        &matches,
                        &resolved_ids,
                        "records:explicit",
                    );
                }
                return;
            }
            let (rows, resolved_ids) = if !vectors.is_empty() {
                (vectors.clone(), resolve_ids(ids, vectors.len()))
            } else {
                match source {
                    Some(spec) => gather_embeddings(core, spec),
                    None => (Vec::new(), Vec::new()),
                }
            };
            if rows.is_empty() {
                return;
            }
            let matches = entity_resolution::resolve_entities(&rows, *bucket_precision, *threshold);
            materialize_entity_matches(core, &matches, &resolved_ids, "cosine");
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_entity_match_claims(
                    core,
                    &matches,
                    &resolved_ids,
                    &entity_provenance(source),
                );
            }
        }
        Method::MineCausalImpact {
            series,
            control,
            intervention_index,
            series_id,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            if series.is_empty() {
                return;
            }
            let effect = if control.is_empty() {
                causal_impact::interrupted_time_series(series, *intervention_index)
            } else {
                causal_impact::diff_in_diff(series, control, *intervention_index)
            };
            materialize_causal_effect(core, &effect, series_id, series, control);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_causal_effect_claim(core, &effect, series_id, series, control);
            }
        }
        Method::MineProcess {
            traces,
            process_id,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            if traces.is_empty() {
                return;
            }
            let (labels, model) = process_mining::alpha_lite_labeled(traces);
            materialize_process_model(core, &model, &labels, process_id);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_process_model_claim(core, &model, &labels, process_id);
            }
        }
        Method::MineRootCause {
            nodes,
            scores,
            edges,
            symptom,
            max_hops,
            decay,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let Some(out) = run_root_cause(nodes, scores, edges, symptom, *max_hops, *decay) else {
                return;
            };
            materialize_root_cause(core, &out, nodes, symptom);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_root_cause_claim(core, &out, nodes, symptom);
            }
        }
        Method::MineRiskPropagation {
            nodes,
            seed,
            edges,
            damping,
            tolerance,
            max_iterations,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            if nodes.is_empty() {
                return;
            }
            let out =
                run_risk_propagation(nodes, seed, edges, *damping, *tolerance, *max_iterations);
            materialize_risk_scores(core, &out, nodes);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_risk_score_claims(core, &out, nodes);
            }
        }
        Method::MineOntologyGap {
            label,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let (classes, class_ids) = build_ontology_classes(core, label);
            if classes.is_empty() {
                return;
            }
            let gaps = ontology_gap::find_gaps(&classes);
            materialize_ontology_gaps(core, &gaps, &class_ids);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_ontology_gap_claims(core, &gaps, &class_ids, label);
            }
        }
        Method::MineRetrievalQuality {
            traces,
            k,
            query_id,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            if traces.is_empty() {
                return;
            }
            let specs: Vec<retrieval_quality::RetrievalTrace> =
                traces.iter().map(to_retrieval_trace).collect();
            let report = retrieval_quality::evaluate(&specs, *k);
            materialize_retrieval_quality(core, &report, query_id);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_retrieval_quality_claim(core, &report, query_id);
            }
        }
        Method::MineCommunity {
            label,
            algorithm,
            resolution,
            max_iterations,
            seed,
            weighted,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim,
        } => {
            let (graph, ids) = build_id_graph(core, label);
            if graph.node_count() == 0 {
                return;
            }
            let algo = to_community_algo(*algorithm);
            let out =
                community::detect(&graph, algo, *resolution, *max_iterations, *seed, *weighted);
            materialize_communities(core, &out, &ids);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_community_claims(core, &out, &ids, community_provenance(label));
            }
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_associate(
    req_id: u64,
    core: &GraphCore,
    transactions: Vec<Vec<String>>,
    source: Option<TransactionSource>,
    min_support: f64,
    min_confidence: f64,
    algorithm: MineAlgorithm,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    let txns = match build_transactions(core, &transactions, &source) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };
    let rules = association::mine_labeled(&txns, min_support, min_confidence, to_algo(algorithm));

    let written = if writeback {
        materialize_rules(core, &rules)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_rule_claims(core, &rules, &source);
    }

    let rows: Vec<serde_json::Value> = rules
        .iter()
        .map(|r| {
            serde_json::json!({
                "antecedent": r.antecedent,
                "consequent": r.consequent,
                "support": r.support,
                "confidence": r.confidence,
                "lift": r.lift,
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "rules": rows,
            "n_transactions": txns.len(),
            "n_rules": rules.len(),
            "written_back": written,
        })),
    )
}

/// Resolve the transaction set: explicit `transactions` win; otherwise derive them
/// from the graph via `source`. An empty request (neither provided) yields no
/// transactions (⇒ no rules), which is a valid empty result, not an error.
fn build_transactions(
    core: &GraphCore,
    transactions: &[Vec<String>],
    source: &Option<TransactionSource>,
) -> Result<Vec<Vec<String>>, String> {
    if !transactions.is_empty() {
        return Ok(transactions.to_vec());
    }
    match source {
        Some(spec) => Ok(derive_from_graph(core, spec)),
        None => Ok(Vec::new()),
    }
}

/// Build one transaction per `node_label` instance from its neighbor set
/// (CONCEPT:EG-KG.mining.graph-derived-transactions). Each transaction is the deduped
/// set of `item_field` values over the owner's neighbors in `direction`, optionally
/// filtered to a `relation`.
fn derive_from_graph(core: &GraphCore, spec: &TransactionSource) -> Vec<Vec<String>> {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut out: Vec<Vec<String>> = Vec::with_capacity(owners.len());
    for (owner_id, _blob) in owners {
        let neighbors = neighbors_in_direction(core, &owner_id, &spec.direction);
        let mut basket: Vec<String> = Vec::new();
        for nbr in neighbors {
            if let Some(rel) = &spec.relation {
                if !edge_matches_relation(core, &owner_id, &nbr, &spec.direction, rel) {
                    continue;
                }
            }
            if let Some(item) = extract_item(core, &nbr, &spec.item_field) {
                basket.push(item);
            }
        }
        basket.sort_unstable();
        basket.dedup();
        if !basket.is_empty() {
            out.push(basket);
        }
    }
    out
}

fn neighbors_in_direction(core: &GraphCore, node_id: &str, direction: &str) -> Vec<String> {
    match direction {
        "in" => core.get_predecessors(node_id).unwrap_or_default(),
        "any" => {
            let mut v = core.get_successors(node_id).unwrap_or_default();
            v.extend(core.get_predecessors(node_id).unwrap_or_default());
            v.sort_unstable();
            v.dedup();
            v
        }
        // "out" (default) and anything else.
        _ => core.get_successors(node_id).unwrap_or_default(),
    }
}

/// Whether an edge between owner and neighbor carries the requested canonical
/// `relationship`. Checks both directions when `direction == "any"`.
fn edge_matches_relation(
    core: &GraphCore,
    owner: &str,
    neighbor: &str,
    direction: &str,
    relation: &str,
) -> bool {
    let pairs: &[(&str, &str)] = match direction {
        "in" => &[(neighbor, owner)],
        "any" => &[(owner, neighbor), (neighbor, owner)],
        _ => &[(owner, neighbor)],
    };
    for &(s, t) in pairs {
        for blob in core.get_edge_properties(s, t) {
            if let Ok(val) = eg_types::msgpack::decode_property_value(&blob) {
                if val.get("relationship").and_then(|v| v.as_str()) == Some(relation) {
                    return true;
                }
            }
        }
    }
    false
}

/// Extract the item value for `neighbor` per `item_field`:
///   * `None`         ⇒ the neighbor's node id.
///   * `"label"`      ⇒ the neighbor's type/label.
///   * `"prop:<key>"` ⇒ the neighbor's property `<key>`.
fn extract_item(core: &GraphCore, neighbor: &str, item_field: &Option<String>) -> Option<String> {
    let field = match item_field {
        None => return Some(neighbor.to_string()),
        Some(f) => f.as_str(),
    };
    let props = core.get_node_properties(neighbor)?;
    let val = eg_types::msgpack::decode_property_value(&props).ok()?;
    if field == "label" {
        for key in ["type", "node_type", "label"] {
            if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
                return Some(s.to_string());
            }
        }
        return None;
    }
    if let Some(key) = field.strip_prefix("prop:") {
        return val.get(key).and_then(json_scalar_string);
    }
    // Bare field name ⇒ treat as a property key.
    val.get(field).and_then(json_scalar_string)
}

fn json_scalar_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Materialize each rule as a typed `:AssociationRule` node (the discovery
/// flywheel, CONCEPT:EG-KG.mining.rule-writeback). The node id is a deterministic
/// digest of `antecedent ⇒ consequent` so replay is idempotent. Each rule is linked
/// (best-effort) to any item that is itself a resident node id, via a `RULE_ITEM`
/// edge — so OWL reasoning + the next mining pass can traverse from the rule to its
/// sources. Returns the number of rule nodes written.
fn materialize_rules(core: &GraphCore, rules: &[LabeledRule]) -> usize {
    let mut written = 0usize;
    for r in rules {
        let node_id = rule_node_id(&r.antecedent, &r.consequent);
        let props = serde_json::json!({
            "type": "AssociationRule",
            "antecedent": r.antecedent,
            "consequent": r.consequent,
            "support": r.support,
            "confidence": r.confidence,
            "lift": r.lift,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        // Link the rule to any item that is a resident node (source objects).
        for item in r.antecedent.iter().chain(r.consequent.iter()) {
            if core.has_node(item) {
                let edge = serde_json::json!({ "relationship": "RULE_ITEM" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), item.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

/// Deterministic, collision-resistant node id for a rule (order-stable — the items
/// are already sorted within each side by the rule generator).
fn rule_node_id(antecedent: &[String], consequent: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(antecedent.join("\u{1}").as_bytes());
    hasher.update([0u8]);
    hasher.update(consequent.join("\u{1}").as_bytes());
    let digest = hasher.finalize();
    format!("assocrule:{}", hex::encode(&digest[..12]))
}

fn to_algo(a: MineAlgorithm) -> Algorithm {
    match a {
        MineAlgorithm::Apriori => Algorithm::Apriori,
        MineAlgorithm::Fpgrowth => Algorithm::FpGrowth,
        MineAlgorithm::Eclat => Algorithm::Eclat,
    }
}

// ─────────────────────────── Clustering ───────────────────────────

/// Handle `MineCluster` (CONCEPT:EG-KG.mining.dbscan-density): build the feature
/// rows (explicit or node embeddings), run the chosen clustering engine, return
/// `{clusters, labels, ...}`, and optionally write `:Cluster` nodes back.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_cluster(
    req_id: u64,
    core: &Arc<GraphCore>,
    features: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    algorithm: ClusterAlgorithm,
    eps: f64,
    min_pts: usize,
    k: usize,
    linkage: Linkage,
    max_iter: usize,
    seed: u64,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let (rows, ids) = match build_vectors(
        core,
        &features,
        &source,
        #[cfg(feature = "query")]
        &plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = cluster_algo(algorithm, eps, min_pts, k, linkage, max_iter, seed);
    let out = cluster::cluster(&rows, algo);

    let written = if writeback {
        materialize_clusters(core, &out, &ids, algorithm)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_cluster_claims(core, &out, &ids, algorithm, cluster_provenance(&source));
    }

    let cluster_rows: Vec<serde_json::Value> = out
        .clusters
        .iter()
        .map(|c| {
            // Report member node ids when the rows came from a node source, else the
            // raw row indices.
            let members: Vec<serde_json::Value> = c
                .members
                .iter()
                .map(|&i| match ids.get(i) {
                    Some(id) => serde_json::Value::String(id.clone()),
                    None => serde_json::json!(i),
                })
                .collect();
            serde_json::json!({
                "cluster_id": c.cluster_id,
                "members": members,
                "centroid": c.centroid,
                "score": c.score,
            })
        })
        .collect();

    let mut payload = serde_json::json!({
        "clusters": cluster_rows,
        "labels": out.labels,
        "n_rows": rows.len(),
        "n_clusters": out.clusters.iter().filter(|c| c.cluster_id >= 0).count(),
        "written_back": written,
    });
    if let Some(resp) = &out.responsibilities {
        payload["responsibilities"] = serde_json::json!(resp);
    }
    Response::ok(req_id, ResultPayload::Json(payload))
}

fn cluster_algo(
    a: ClusterAlgorithm,
    eps: f64,
    min_pts: usize,
    k: usize,
    linkage: Linkage,
    max_iter: usize,
    seed: u64,
) -> cluster::Algorithm {
    match a {
        ClusterAlgorithm::Dbscan => cluster::Algorithm::Dbscan { eps, min_pts },
        ClusterAlgorithm::Hierarchical => cluster::Algorithm::Hierarchical {
            k,
            linkage: to_linkage(linkage),
        },
        ClusterAlgorithm::Gmm => cluster::Algorithm::Gmm { k, max_iter, seed },
        ClusterAlgorithm::Kmedoids => cluster::Algorithm::KMedoids { k, max_iter },
    }
}

fn to_linkage(l: Linkage) -> cluster::Linkage {
    match l {
        Linkage::Single => cluster::Linkage::Single,
        Linkage::Complete => cluster::Linkage::Complete,
        Linkage::Average => cluster::Linkage::Average,
    }
}

/// Materialize each non-noise cluster as a typed `:Cluster` node (CONCEPT:EG-KG.mining.cluster-writeback),
/// id = a deterministic digest of `algo` + its sorted member node-ids (idempotent
/// replay). Members that are resident nodes are linked via `CLUSTER_MEMBER` edges.
fn materialize_clusters(
    core: &GraphCore,
    out: &cluster::Clustering,
    ids: &[String],
    algorithm: ClusterAlgorithm,
) -> usize {
    let algo = cluster_algo_name(algorithm);
    let mut written = 0usize;
    for c in &out.clusters {
        if c.cluster_id < 0 {
            continue; // never materialize the DBSCAN noise bucket
        }
        let member_ids: Vec<String> = c
            .members
            .iter()
            .map(|&i| match ids.get(i) {
                Some(id) => id.clone(),
                None => i.to_string(),
            })
            .collect();
        let node_id = cluster_node_id(algo, &member_ids);
        let props = serde_json::json!({
            "type": "Cluster",
            "algo": algo,
            "cluster_id": c.cluster_id,
            "size": member_ids.len(),
            "members": member_ids,
            "centroid": c.centroid,
            "score": c.score,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for mid in &member_ids {
            if core.has_node(mid) {
                let edge = serde_json::json!({ "relationship": "CLUSTER_MEMBER" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), mid.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

fn cluster_algo_name(a: ClusterAlgorithm) -> &'static str {
    match a {
        ClusterAlgorithm::Dbscan => "dbscan",
        ClusterAlgorithm::Hierarchical => "hierarchical",
        ClusterAlgorithm::Gmm => "gmm",
        ClusterAlgorithm::Kmedoids => "kmedoids",
    }
}

fn cluster_node_id(algo: &str, member_ids: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted = member_ids.to_vec();
    sorted.sort();
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    hasher.update(sorted.join("\u{1}").as_bytes());
    format!("cluster:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Anomaly detection ───────────────────────────

/// Handle `MineAnomaly` (CONCEPT:EG-KG.mining.isolation-forest): build rows
/// (explicit features, a 1-D values series, or node embeddings), run the detector,
/// return per-row `{id, anomaly_score, is_anomaly}`, and optionally write `:Anomaly`
/// nodes back for the flagged rows.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_anomaly(
    req_id: u64,
    core: &Arc<GraphCore>,
    features: Vec<Vec<f64>>,
    values: Vec<f64>,
    source: Option<VectorSource>,
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    algorithm: AnomalyAlgorithm,
    k: usize,
    n_trees: usize,
    sample_size: usize,
    seed: u64,
    nu: f64,
    gamma: f64,
    kernel: SvmKernel,
    threshold: Option<f64>,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let (rows, ids) = match build_anomaly_rows(
        core,
        &features,
        &values,
        &source,
        #[cfg(feature = "query")]
        &plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = anomaly_algo(algorithm, k, n_trees, sample_size, seed, nu, gamma, kernel);
    let out = anomaly::detect(&rows, algo, threshold);

    let written = if writeback {
        materialize_anomalies(core, &out, &ids, algorithm)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_anomaly_claims(core, &out, &ids, algorithm, anomaly_provenance(&source));
    }

    let rows_json: Vec<serde_json::Value> = (0..rows.len())
        .map(|i| {
            let id = match ids.get(i) {
                Some(id) => serde_json::Value::String(id.clone()),
                None => serde_json::json!(i),
            };
            serde_json::json!({
                "id": id,
                "anomaly_score": out.scores[i],
                "is_anomaly": out.is_anomaly[i],
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "rows": rows_json,
            "n_rows": rows.len(),
            "n_anomalies": out.is_anomaly.iter().filter(|&&a| a).count(),
            "threshold": out.threshold,
            "written_back": written,
        })),
    )
}

#[allow(clippy::too_many_arguments)]
fn anomaly_algo(
    a: AnomalyAlgorithm,
    k: usize,
    n_trees: usize,
    sample_size: usize,
    seed: u64,
    nu: f64,
    gamma: f64,
    kernel: SvmKernel,
) -> anomaly::Algorithm {
    match a {
        AnomalyAlgorithm::Zscore => anomaly::Algorithm::ZScoreMad,
        AnomalyAlgorithm::Isoforest => anomaly::Algorithm::IsolationForest {
            n_trees,
            sample_size,
            seed,
        },
        AnomalyAlgorithm::Lof => anomaly::Algorithm::Lof { k },
        AnomalyAlgorithm::Ocsvm => anomaly::Algorithm::OneClassSvm {
            kernel: match kernel {
                SvmKernel::Linear => anomaly::Kernel::Linear,
                SvmKernel::Rbf => anomaly::Kernel::Rbf { gamma },
            },
            nu,
        },
    }
}

/// Materialize each FLAGGED row as a typed `:Anomaly` node (CONCEPT:EG-KG.mining.anomaly-writeback),
/// id = a deterministic digest of `algo` + the source node-id / row index. Linked
/// to its source node via an `ANOMALY_OF` edge when that node is resident.
fn materialize_anomalies(
    core: &GraphCore,
    out: &anomaly::Anomalies,
    ids: &[String],
    algorithm: AnomalyAlgorithm,
) -> usize {
    let algo = anomaly_algo_name(algorithm);
    let mut written = 0usize;
    for i in 0..out.scores.len() {
        if !out.is_anomaly[i] {
            continue;
        }
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = anomaly_node_id(algo, &src);
        let props = serde_json::json!({
            "type": "Anomaly",
            "algo": algo,
            "score": out.scores[i],
            "is_anomaly": true,
            "source": src,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        if core.has_node(&src) {
            let edge = serde_json::json!({ "relationship": "ANOMALY_OF" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id.clone(), src.clone(), eb);
            }
        }
        written += 1;
    }
    written
}

fn anomaly_algo_name(a: AnomalyAlgorithm) -> &'static str {
    match a {
        AnomalyAlgorithm::Zscore => "zscore",
        AnomalyAlgorithm::Isoforest => "isoforest",
        AnomalyAlgorithm::Lof => "lof",
        AnomalyAlgorithm::Ocsvm => "ocsvm",
    }
}

fn anomaly_node_id(algo: &str, source: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    hasher.update(source.as_bytes());
    format!("anomaly:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Classification (fit / predict) ───────────────────────────

/// Handle `MineClassifyFit` (CONCEPT:EG-KG.mining.naive-bayes): build the feature rows
/// (explicit or node embeddings — the cross-modal "classify these nodes using their
/// embeddings + ontology features" hook), fit the chosen classifier, and return the
/// serializable model blob. PREDICTIVE + read-only (no graph mutation).
#[allow(clippy::too_many_arguments)]
fn handle_classify_fit(
    req_id: u64,
    core: &Arc<GraphCore>,
    x: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    y: Vec<i64>,
    algorithm: ClassifyAlgorithm,
    k: usize,
    alpha: f64,
    lr: f64,
    epochs: usize,
    l2: f64,
    c: f64,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let (rows, _ids) = match build_vectors(
        core,
        &x,
        &source,
        #[cfg(feature = "query")]
        &plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = classify_algo(algorithm, k, alpha, lr, epochs, l2, c);
    match classify::fit(&rows, &y, algo) {
        Ok(model) => Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({
                "model": model,
                "algorithm": classify_algo_name(algorithm),
                "n_samples": rows.len(),
                "classes": classify_classes(&model),
            })),
        ),
        Err(e) => Response::err(req_id, e),
    }
}

/// Handle `MineClassifyPredict` (CONCEPT:EG-KG.mining.naive-bayes): build rows, run the
/// fitted model, return per-row `{id, label, proba}`, and optionally write
/// `:Classification` nodes back for each prediction.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_classify_predict(
    req_id: u64,
    core: &Arc<GraphCore>,
    model: FittedClassifier,
    x: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let (rows, ids) = match build_vectors(
        core,
        &x,
        &source,
        #[cfg(feature = "query")]
        &plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let out = classify::predict(&model, &rows);

    let written = if writeback {
        materialize_classifications(core, &out, &ids)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_classification_claims(core, &out, &ids, classify_provenance(&source));
    }

    let rows_json: Vec<serde_json::Value> = (0..rows.len())
        .map(|i| {
            let id = match ids.get(i) {
                Some(id) => serde_json::Value::String(id.clone()),
                None => serde_json::json!(i),
            };
            serde_json::json!({
                "id": id,
                "label": out.labels[i],
                "proba": out.proba[i],
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "rows": rows_json,
            "classes": out.classes,
            "n_rows": rows.len(),
            "written_back": written,
        })),
    )
}

fn classify_algo(
    a: ClassifyAlgorithm,
    k: usize,
    alpha: f64,
    lr: f64,
    epochs: usize,
    l2: f64,
    c: f64,
) -> classify::Algorithm {
    match a {
        ClassifyAlgorithm::Gaussiannb => classify::Algorithm::GaussianNb,
        ClassifyAlgorithm::Multinomialnb => classify::Algorithm::MultinomialNb { alpha },
        ClassifyAlgorithm::Knn => classify::Algorithm::Knn { k },
        ClassifyAlgorithm::Logistic => classify::Algorithm::Logistic { lr, epochs, l2 },
        ClassifyAlgorithm::Svc => classify::Algorithm::LinearSvc { c, epochs, lr },
    }
}

fn classify_algo_name(a: ClassifyAlgorithm) -> &'static str {
    match a {
        ClassifyAlgorithm::Gaussiannb => "gaussiannb",
        ClassifyAlgorithm::Multinomialnb => "multinomialnb",
        ClassifyAlgorithm::Knn => "knn",
        ClassifyAlgorithm::Logistic => "logistic",
        ClassifyAlgorithm::Svc => "svc",
    }
}

/// The sorted class set embedded in a fitted model (for the fit response).
fn classify_classes(model: &FittedClassifier) -> Vec<i64> {
    match model {
        FittedClassifier::GaussianNb { classes, .. }
        | FittedClassifier::MultinomialNb { classes, .. }
        | FittedClassifier::Knn { classes, .. }
        | FittedClassifier::LinearOvr { classes, .. } => classes.clone(),
    }
}

/// Materialize each prediction as a typed `:Classification` node (CONCEPT:EG-KG.mining.classify-writeback),
/// id = a deterministic digest of the source node-id / row index. Linked to its source
/// node via a `CLASSIFIED_AS` edge when that node is resident.
fn materialize_classifications(
    core: &GraphCore,
    out: &classify::Classification,
    ids: &[String],
) -> usize {
    let mut written = 0usize;
    for i in 0..out.labels.len() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = classification_node_id(&src);
        let props = serde_json::json!({
            "type": "Classification",
            "label": out.labels[i],
            "proba": out.proba[i],
            "classes": out.classes,
            "source": src,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        if core.has_node(&src) {
            let edge = serde_json::json!({ "relationship": "CLASSIFIED_AS" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id.clone(), src.clone(), eb);
            }
        }
        written += 1;
    }
    written
}

fn classification_node_id(source: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"classification");
    hasher.update([0u8]);
    hasher.update(source.as_bytes());
    format!("classification:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Dimensionality reduction ───────────────────────────

/// Handle `MineReduce` (CONCEPT:EG-KG.mining.truncated-svd): build rows (explicit or
/// node embeddings — reduce node vectors for the graphviz), run the chosen reduction,
/// return per-row `{id, coords}`, and optionally write `:Embedding2D` nodes back.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_reduce(
    req_id: u64,
    core: &Arc<GraphCore>,
    x: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    labels: Vec<i64>,
    algorithm: ReduceAlgorithm,
    n_components: usize,
    n_neighbors: usize,
    min_dist: f64,
    perplexity: f64,
    epochs: usize,
    lr: f64,
    seed: u64,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let (rows, ids) = match build_vectors(
        core,
        &x,
        &source,
        #[cfg(feature = "query")]
        &plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    if matches!(algorithm, ReduceAlgorithm::Lda) && labels.len() != rows.len() {
        return Response::err(
            req_id,
            "mining: LDA requires one label per row (supervised)",
        );
    }
    let algo = reduce_algo(
        algorithm,
        n_neighbors,
        min_dist,
        perplexity,
        epochs,
        lr,
        seed,
    );
    let lbls = (!labels.is_empty()).then_some(labels.as_slice());
    let out = reduce::reduce(&rows, lbls, algo, n_components);

    let written = if writeback {
        materialize_embeddings(core, &out, &ids)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_reduce_claims(core, &rows, &out, &ids, algorithm, &source);
    }

    let rows_json: Vec<serde_json::Value> = (0..out.coords.len())
        .map(|i| {
            let id = match ids.get(i) {
                Some(id) => serde_json::Value::String(id.clone()),
                None => serde_json::json!(i),
            };
            serde_json::json!({ "id": id, "coords": out.coords[i] })
        })
        .collect();

    let mut payload = serde_json::json!({
        "rows": rows_json,
        "algorithm": reduce_algo_name(algorithm),
        "n_rows": rows.len(),
        "n_components": out.coords.first().map(|c| c.len()).unwrap_or(0),
        "written_back": written,
    });
    if !out.singular_values.is_empty() {
        payload["singular_values"] = serde_json::json!(out.singular_values);
    }
    Response::ok(req_id, ResultPayload::Json(payload))
}

fn reduce_algo(
    a: ReduceAlgorithm,
    n_neighbors: usize,
    min_dist: f64,
    perplexity: f64,
    epochs: usize,
    lr: f64,
    seed: u64,
) -> reduce::Algorithm {
    match a {
        ReduceAlgorithm::Svd => reduce::Algorithm::TruncatedSvd,
        ReduceAlgorithm::Lda => reduce::Algorithm::Lda,
        ReduceAlgorithm::Umap => reduce::Algorithm::Umap {
            n_neighbors,
            min_dist,
            epochs,
            seed,
        },
        ReduceAlgorithm::Tsne => reduce::Algorithm::Tsne {
            perplexity,
            epochs,
            learning_rate: lr,
            seed,
        },
    }
}

fn reduce_algo_name(a: ReduceAlgorithm) -> &'static str {
    match a {
        ReduceAlgorithm::Svd => "svd",
        ReduceAlgorithm::Lda => "lda",
        ReduceAlgorithm::Umap => "umap",
        ReduceAlgorithm::Tsne => "tsne",
    }
}

/// Materialize each row's reduced vector as a typed `:Embedding2D` node
/// (CONCEPT:EG-KG.mining.reduce-writeback), id = a deterministic digest of the source
/// node-id / row index. Linked to its source node via a `REDUCED_FROM` edge when that
/// node is resident — feeding the web-UI graphviz + downstream clustering.
fn materialize_embeddings(core: &GraphCore, out: &reduce::Reduction, ids: &[String]) -> usize {
    let mut written = 0usize;
    for (i, coords) in out.coords.iter().enumerate() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = embedding2d_node_id(&src);
        let props = serde_json::json!({
            "type": "Embedding2D",
            "coords": coords,
            "dims": coords.len(),
            "source": src,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        if core.has_node(&src) {
            let edge = serde_json::json!({ "relationship": "REDUCED_FROM" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id.clone(), src.clone(), eb);
            }
        }
        written += 1;
    }
    written
}

fn embedding2d_node_id(source: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"embedding2d");
    hasher.update([0u8]);
    hasher.update(source.as_bytes());
    format!("embedding2d:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Sequential-pattern mining ───────────────────────────

/// Handle `MineSequence` (CONCEPT:EG-KG.mining.prefixspan — Phase 4): build the
/// ordered sequences (explicit or graph-derived), run the chosen engine
/// (PrefixSpan/GSP — both agree), return `{patterns, ...}`, and optionally write
/// `:SequentialPattern` nodes back.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_sequence(
    req_id: u64,
    core: &GraphCore,
    sequences: Vec<Vec<String>>,
    source: Option<SequenceSource>,
    min_support: f64,
    algorithm: MineSeqAlgorithm,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    let seqs = match build_sequences(core, &sequences, &source) {
        Ok(s) => s,
        Err(e) => return Response::err(req_id, e),
    };
    let patterns = sequence::mine_labeled(&seqs, min_support, to_seq_algo(algorithm));

    let written = if writeback {
        materialize_patterns(core, &patterns)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_sequence_claims(core, &patterns, sequence_provenance(&source));
    }

    let rows: Vec<serde_json::Value> = patterns
        .iter()
        .map(|p| {
            serde_json::json!({
                "items": p.items,
                "support": p.support,
                "count": p.count,
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "patterns": rows,
            "n_sequences": seqs.len(),
            "n_patterns": patterns.len(),
            "written_back": written,
        })),
    )
}

/// Resolve the sequence set: explicit `sequences` win; otherwise derive them
/// from the graph via `source`. An empty request yields no sequences (⇒ no
/// patterns), a valid empty result.
fn build_sequences(
    core: &GraphCore,
    sequences: &[Vec<String>],
    source: &Option<SequenceSource>,
) -> Result<Vec<Vec<String>>, String> {
    if !sequences.is_empty() {
        return Ok(sequences.to_vec());
    }
    match source {
        Some(spec) => Ok(derive_sequences_from_graph(core, spec)),
        None => Ok(Vec::new()),
    }
}

/// Build one ORDERED sequence per `node_label` instance from its neighbor list
/// (CONCEPT:EG-KG.mining.prefixspan): each sequence is the `item_field` values of
/// the owner's neighbors in `direction`, restored to chronological (edge
/// insertion) order — unlike `derive_from_graph`'s unordered dedup, order is the
/// whole point of a sequence — optionally filtered to a `relation`.
///
/// `core.get_successors`/`get_predecessors` walk the underlying petgraph
/// adjacency list, which is LIFO (the most-recently-added edge comes back
/// FIRST); `neighbors_in_direction` passes that through unchanged for
/// `out`/`in`, so it is reversed here to recover true chronological order.
fn derive_sequences_from_graph(core: &GraphCore, spec: &SequenceSource) -> Vec<Vec<String>> {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut out: Vec<Vec<String>> = Vec::with_capacity(owners.len());
    for (owner_id, _blob) in owners {
        let mut neighbors = neighbors_in_direction(core, &owner_id, &spec.direction);
        if spec.direction != "any" {
            neighbors.reverse();
        }
        let mut seq: Vec<String> = Vec::new();
        for nbr in neighbors {
            if let Some(rel) = &spec.relation {
                if !edge_matches_relation(core, &owner_id, &nbr, &spec.direction, rel) {
                    continue;
                }
            }
            if let Some(item) = extract_item(core, &nbr, &spec.item_field) {
                seq.push(item);
            }
        }
        if !seq.is_empty() {
            out.push(seq);
        }
    }
    out
}

fn to_seq_algo(a: MineSeqAlgorithm) -> sequence::Algorithm {
    match a {
        MineSeqAlgorithm::Prefixspan => sequence::Algorithm::PrefixSpan,
        MineSeqAlgorithm::Gsp => sequence::Algorithm::Gsp,
    }
}

/// Materialize each mined pattern as a typed `:SequentialPattern` node
/// (CONCEPT:EG-KG.mining.sequence-writeback), id = a deterministic digest of its
/// (order-preserving) item list. Linked to any item that is a resident node via
/// a `PATTERN_ITEM` edge, mirroring `materialize_rules`.
fn materialize_patterns(core: &GraphCore, patterns: &[LabeledPattern]) -> usize {
    let mut written = 0usize;
    for p in patterns {
        let node_id = pattern_node_id(&p.items);
        let props = serde_json::json!({
            "type": "SequentialPattern",
            "items": p.items,
            "support": p.support,
            "count": p.count,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for item in &p.items {
            if core.has_node(item) {
                let edge = serde_json::json!({ "relationship": "PATTERN_ITEM" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), item.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

/// Deterministic, collision-resistant node id for a pattern (order matters, so
/// the digest is over the items in sequence — unlike a rule's antecedent/
/// consequent, which are pre-sorted sets).
fn pattern_node_id(items: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(items.join("\u{1}").as_bytes());
    format!("seqpattern:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Forecasting ───────────────────────────

/// Handle `MineForecast` (CONCEPT:EG-KG.mining.arima — Phase 4): forecast
/// `horizon` future points off a 1-D `values` series (a tsdb window handed in
/// by the caller — the same client-supplied cut `MineAnomaly` took in Phase 2)
/// via ARIMA/Holt-Winters/STL, return `{forecast, lower, upper, ...}`, and
/// optionally write a `:Forecast` node back.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_forecast(
    req_id: u64,
    core: &GraphCore,
    values: Vec<f64>,
    algorithm: ForecastAlgorithm,
    horizon: usize,
    p: usize,
    d: usize,
    q: usize,
    period: usize,
    alpha: f64,
    beta: f64,
    gamma: f64,
    confidence: f64,
    series_id: String,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    if values.is_empty() {
        return Response::err(
            req_id,
            "mining: forecast requires a non-empty `values` series",
        );
    }
    let algo = forecast_algo(algorithm, p, d, q, period, alpha, beta, gamma);
    let out = forecast::forecast(&values, algo, horizon, confidence);

    let written = if writeback {
        materialize_forecast(
            core,
            &out,
            horizon,
            &series_id,
            &values,
            forecast_algo_name(algorithm),
        )
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_forecast_claim(
            core,
            &series_id,
            &values,
            forecast_algo_name(algorithm),
            confidence,
        );
    }

    let mut payload = serde_json::json!({
        "forecast": out.values,
        "lower": out.lower,
        "upper": out.upper,
        "algorithm": forecast_algo_name(algorithm),
        "horizon": horizon,
        "n_obs": values.len(),
        "written_back": written,
    });
    if matches!(algorithm, ForecastAlgorithm::Stl) {
        payload["trend"] = serde_json::json!(out.trend);
        payload["seasonal"] = serde_json::json!(out.seasonal);
        payload["residual"] = serde_json::json!(out.residual);
    }
    Response::ok(req_id, ResultPayload::Json(payload))
}

#[allow(clippy::too_many_arguments)]
fn forecast_algo(
    a: ForecastAlgorithm,
    p: usize,
    d: usize,
    q: usize,
    period: usize,
    alpha: f64,
    beta: f64,
    gamma: f64,
) -> forecast::Algorithm {
    match a {
        ForecastAlgorithm::Arima => forecast::Algorithm::Arima { p, d, q },
        ForecastAlgorithm::Holtwinters => forecast::Algorithm::HoltWinters {
            period,
            alpha,
            beta,
            gamma,
        },
        ForecastAlgorithm::Stl => forecast::Algorithm::Stl { period },
    }
}

fn forecast_algo_name(a: ForecastAlgorithm) -> &'static str {
    match a {
        ForecastAlgorithm::Arima => "arima",
        ForecastAlgorithm::Holtwinters => "holtwinters",
        ForecastAlgorithm::Stl => "stl",
    }
}

/// Materialize the forecast as a typed `:Forecast` node
/// (CONCEPT:EG-KG.mining.forecast-writeback), id = a deterministic digest of
/// `algo` + (`series_id` when given, else the input `values` — so identical
/// explicit input reproduces the same id on WAL replay). Linked to a resident
/// node named `series_id` via a `FORECAST_OF` edge when one exists.
fn materialize_forecast(
    core: &GraphCore,
    out: &forecast::Forecast,
    horizon: usize,
    series_id: &str,
    values: &[f64],
    algo: &str,
) -> usize {
    let node_id = forecast_node_id(algo, series_id, values);
    let props = serde_json::json!({
        "type": "Forecast",
        "algo": algo,
        "horizon": horizon,
        "values": out.values,
        "lower": out.lower,
        "upper": out.upper,
        "series_id": series_id,
    });
    let Ok(blob) = rmp_serde::to_vec_named(&props) else {
        return 0;
    };
    core.add_node(node_id.clone(), blob);
    if !series_id.is_empty() && core.has_node(series_id) {
        let edge = serde_json::json!({ "relationship": "FORECAST_OF" });
        if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
            let _ = core.add_edge(node_id, series_id.to_string(), eb);
        }
    }
    1
}

fn forecast_node_id(algo: &str, series_id: &str, values: &[f64]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    if !series_id.is_empty() {
        hasher.update(series_id.as_bytes());
    } else {
        for v in values {
            hasher.update(v.to_bits().to_le_bytes());
        }
    }
    format!("forecast:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Text mining ───────────────────────────

/// Handle `MineText` (CONCEPT:EG-KG.mining.tfidf — Phase 4): tokenize the
/// corpus (explicit or graph-derived), run the chosen engine, return
/// `{doc_terms}` (tfidf) or `{topics, doc_topics}` (lda/nmf), and optionally
/// write `:Topic` nodes back (lda/nmf only).
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_text(
    req_id: u64,
    core: &GraphCore,
    docs: Vec<Vec<String>>,
    source: Option<TextSource>,
    algorithm: TextAlgorithm,
    k: usize,
    alpha: f64,
    beta: f64,
    iterations: usize,
    seed: u64,
    top_n: usize,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    let (tokenized, ids) = build_text_docs(core, &docs, &source);
    if tokenized.is_empty() {
        return Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({
                "doc_terms": [],
                "topics": [],
                "doc_topics": [],
                "n_docs": 0,
                "written_back": 0,
            })),
        );
    }
    let algo = to_text_algo(algorithm, k, alpha, beta, iterations, seed);
    let out = text::mine_labeled(&tokenized, algo, top_n);

    let written = if writeback && !matches!(algorithm, TextAlgorithm::Tfidf) {
        materialize_topics(core, &out, &ids, text_algo_name(algorithm))
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim && !matches!(algorithm, TextAlgorithm::Tfidf) {
        materialize_topic_claims(core, &out, text_algo_name(algorithm));
    }

    let doc_terms_json: Vec<serde_json::Value> = out
        .doc_terms
        .iter()
        .enumerate()
        .map(|(i, terms)| {
            let id = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
            let term_rows: Vec<serde_json::Value> = terms
                .iter()
                .map(|(t, w)| serde_json::json!({ "term": t, "weight": w }))
                .collect();
            serde_json::json!({ "doc_id": id, "terms": term_rows })
        })
        .collect();

    let topics_json: Vec<serde_json::Value> = out
        .topics
        .iter()
        .enumerate()
        .map(|(i, terms)| {
            let term_rows: Vec<serde_json::Value> = terms
                .iter()
                .map(|(t, w)| serde_json::json!({ "term": t, "weight": w }))
                .collect();
            serde_json::json!({ "topic_id": i, "terms": term_rows })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "doc_terms": doc_terms_json,
            "topics": topics_json,
            "doc_topics": out.doc_topics,
            "algorithm": text_algo_name(algorithm),
            "n_docs": tokenized.len(),
            "written_back": written,
        })),
    )
}

fn to_text_algo(
    a: TextAlgorithm,
    k: usize,
    alpha: f64,
    beta: f64,
    iterations: usize,
    seed: u64,
) -> text::Algorithm {
    match a {
        TextAlgorithm::Tfidf => text::Algorithm::Tfidf,
        TextAlgorithm::Lda => text::Algorithm::Lda {
            k,
            alpha,
            beta,
            iterations,
            seed,
        },
        TextAlgorithm::Nmf => text::Algorithm::Nmf {
            k,
            iterations,
            seed,
        },
    }
}

fn text_algo_name(a: TextAlgorithm) -> &'static str {
    match a {
        TextAlgorithm::Tfidf => "tfidf",
        TextAlgorithm::Lda => "lda",
        TextAlgorithm::Nmf => "nmf",
    }
}

/// Resolve the tokenized corpus: explicit `docs` win (already tokenized, ids
/// empty); otherwise tokenize the `field` string property of every
/// `source.node_label` instance (compute-near-data — no Tantivy/eg-text
/// dependency), skipping nodes with no non-empty text. Returns the corpus AND
/// a parallel `ids` vec (node ids for the graph-derived path).
fn build_text_docs(
    core: &GraphCore,
    docs: &[Vec<String>],
    source: &Option<TextSource>,
) -> (Vec<Vec<String>>, Vec<String>) {
    if !docs.is_empty() {
        return (docs.to_vec(), Vec::new());
    }
    let Some(spec) = source else {
        return (Vec::new(), Vec::new());
    };
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut tokenized = Vec::with_capacity(owners.len());
    let mut ids = Vec::with_capacity(owners.len());
    for (node_id, blob) in owners {
        let Ok(props) = eg_types::msgpack::decode_property_value(&blob) else {
            continue;
        };
        let Some(text_val) = props.get(&spec.field).and_then(|v| v.as_str()) else {
            continue;
        };
        let toks = text::tokenize(text_val);
        if toks.is_empty() {
            continue;
        }
        tokenized.push(toks);
        ids.push(node_id);
    }
    (tokenized, ids)
}

/// Materialize each topic as a typed `:Topic` node
/// (CONCEPT:EG-KG.mining.topic-writeback), id = a deterministic digest of `algo` +
/// its top terms (order-sensitive — the terms are already sorted by
/// descending weight). Linked, via a `HAS_TOPIC` edge, to every resident
/// source document whose DOMINANT topic (argmax of its `doc_topics`
/// distribution) is this one — only available when the corpus came from a
/// graph-derived `source` (`ids` non-empty).
fn materialize_topics(
    core: &GraphCore,
    out: &text::LabeledTextResult,
    ids: &[String],
    algo: &str,
) -> usize {
    let dominant: Vec<usize> = out
        .doc_topics
        .iter()
        .map(|dist| {
            dist.iter()
                .enumerate()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(t, _)| t)
                .unwrap_or(0)
        })
        .collect();

    let mut written = 0usize;
    for (t, terms) in out.topics.iter().enumerate() {
        let term_labels: Vec<&str> = terms.iter().map(|(term, _)| term.as_str()).collect();
        let node_id = topic_node_id(algo, &term_labels);
        let term_rows: Vec<serde_json::Value> = terms
            .iter()
            .map(|(term, w)| serde_json::json!({ "term": term, "weight": w }))
            .collect();
        let props = serde_json::json!({
            "type": "Topic",
            "algo": algo,
            "terms": term_rows,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for (i, doc_id) in ids.iter().enumerate() {
            if dominant.get(i) == Some(&t) && core.has_node(doc_id) {
                let edge = serde_json::json!({ "relationship": "HAS_TOPIC" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(doc_id.clone(), node_id.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

fn topic_node_id(algo: &str, terms: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    hasher.update(terms.join("\u{1}").as_bytes());
    format!("topic:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Frequent subgraph mining + motifs ───────────────────────────

/// Handle `MineSubgraph` (CONCEPT:EG-KG.mining.gspan-frequent-subgraph — Phase
/// 4, the graph-native family member): build a labeled host graph from the
/// RESIDENT graph itself (no rows/vectors handed in), run gSpan-style
/// frequent-subgraph mining or a motif census, and optionally write
/// `:FrequentSubgraph` nodes back (`gspan` only).
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_subgraph(
    req_id: u64,
    core: &GraphCore,
    label: Option<String>,
    min_support: f64,
    max_edges: usize,
    algorithm: SubgraphAlgorithm,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    let (host, ids) = build_host_graph(core, &label);
    let n_host_nodes = host.node_count();
    let n_host_edges = host.edge_count();

    match algorithm {
        SubgraphAlgorithm::Gspan => {
            let results = subgraph::mine_gspan(&host, min_support, max_edges);
            let written = if writeback {
                materialize_subgraphs(core, &results, &ids)
            } else {
                0
            };
            #[cfg(feature = "epistemic")]
            if writeback && as_claim {
                materialize_subgraph_claims(core, &results, subgraph_provenance(&label));
            }
            let patterns: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    let edges: Vec<serde_json::Value> = r
                        .pattern
                        .edges
                        .iter()
                        .map(|(a, b, lbl)| serde_json::json!({ "from": a, "to": b, "label": lbl }))
                        .collect();
                    serde_json::json!({
                        "nodes": r.pattern.node_labels,
                        "edges": edges,
                        "support": r.support,
                        "count": r.count,
                    })
                })
                .collect();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "patterns": patterns,
                    "algorithm": subgraph_algo_name(SubgraphAlgorithm::Gspan),
                    "n_host_nodes": n_host_nodes,
                    "n_host_edges": n_host_edges,
                    "written_back": written,
                })),
            )
        }
        SubgraphAlgorithm::Motif => {
            let motifs = subgraph::count_motifs(&host);
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "motifs": {
                        "wedge": motifs.wedge,
                        "triangle": motifs.triangle,
                        "directed_cycle3": motifs.directed_cycle3,
                    },
                    "algorithm": subgraph_algo_name(SubgraphAlgorithm::Motif),
                    "n_host_nodes": n_host_nodes,
                    "n_host_edges": n_host_edges,
                    "written_back": 0,
                })),
            )
        }
    }
}

/// Build a [`HostGraph`] from the resident graph (CONCEPT:EG-KG.mining.gspan-frequent-subgraph):
/// every node's type/label property (checked in the same `type`/`node_type`/
/// `label` precedence as `extract_item`'s `"label"` field), every edge's
/// canonical `relationship` label (defaulting to `"_"` when absent). When
/// `label_filter` is given, only nodes of that ONE type are included (both
/// edge endpoints must be included for the edge to count). Returns the host
/// graph AND a parallel `ids` vec (dense index → resident node id).
fn build_host_graph(core: &GraphCore, label_filter: &Option<String>) -> (HostGraph, Vec<String>) {
    let all_nodes = core.get_nodes();
    let mut ids: Vec<String> = Vec::new();
    let mut labels: Vec<String> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (node_id, blob) in &all_nodes {
        let node_label = node_type_label(blob).unwrap_or_else(|| "_".to_string());
        if let Some(want) = label_filter {
            if &node_label != want {
                continue;
            }
        }
        index.insert(node_id.clone(), ids.len());
        ids.push(node_id.clone());
        labels.push(node_label);
    }

    let all_edges = core.get_edges();
    let mut edges: Vec<(usize, usize, String)> = Vec::new();
    for (src, dst, blob) in &all_edges {
        let (Some(&si), Some(&di)) = (index.get(src), index.get(dst)) else {
            continue;
        };
        let rel = edge_relation_label(blob);
        edges.push((si, di, rel));
    }
    (HostGraph::build(labels, &edges), ids)
}

/// Extract a node's type/label from its property blob, per the
/// `type`/`node_type`/`label` precedence used elsewhere in this handler.
fn node_type_label(blob: &[u8]) -> Option<String> {
    let val = eg_types::msgpack::decode_property_value(blob).ok()?;
    for key in ["type", "node_type", "label"] {
        if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
    }
    None
}

/// Extract an edge's canonical `relationship` from its property blob, defaulting
/// to `"_"` when none is set — an unlabeled edge is
/// still a valid, matchable edge, just under one shared label.
fn edge_relation_label(blob: &[u8]) -> String {
    let Ok(val) = eg_types::msgpack::decode_property_value(blob) else {
        return "_".to_string();
    };
    if let Some(s) = val.get("relationship").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    "_".to_string()
}

fn subgraph_algo_name(a: SubgraphAlgorithm) -> &'static str {
    match a {
        SubgraphAlgorithm::Gspan => "gspan",
        SubgraphAlgorithm::Motif => "motif",
    }
}

/// Materialize each frequent pattern as a typed `:FrequentSubgraph` node
/// (CONCEPT:EG-KG.mining.gspan-frequent-subgraph), id = a deterministic digest
/// of its canonical shape (node labels + edges). Linked, via a
/// `SUBGRAPH_MEMBER` edge, to every resident host node appearing in ANY of its
/// embeddings.
fn materialize_subgraphs(
    core: &GraphCore,
    results: &[subgraph::FrequentSubgraph],
    ids: &[String],
) -> usize {
    let mut written = 0usize;
    for r in results {
        let node_id = subgraph_node_id(&r.pattern);
        let edges_json: Vec<serde_json::Value> = r
            .pattern
            .edges
            .iter()
            .map(|(a, b, lbl)| serde_json::json!({ "from": a, "to": b, "label": lbl }))
            .collect();
        let props = serde_json::json!({
            "type": "FrequentSubgraph",
            "nodes": r.pattern.node_labels,
            "edges": edges_json,
            "support": r.support,
            "count": r.count,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for &member_idx in &r.member_nodes {
            if let Some(member_id) = ids.get(member_idx) {
                if core.has_node(member_id) {
                    let edge = serde_json::json!({ "relationship": "SUBGRAPH_MEMBER" });
                    if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                        let _ = core.add_edge(node_id.clone(), member_id.clone(), eb);
                    }
                }
            }
        }
        written += 1;
    }
    written
}

fn subgraph_node_id(pattern: &subgraph::Pattern) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(pattern.node_labels.join("\u{1}").as_bytes());
    hasher.update([0u8]);
    for (a, b, lbl) in &pattern.edges {
        hasher.update(a.to_le_bytes());
        hasher.update(b.to_le_bytes());
        hasher.update(lbl.as_bytes());
        hasher.update([0u8]);
    }
    format!("subgraph:{}", hex::encode(&hasher.finalize()[..12]))
}

// ═══════════════════ Residual insight/mining families (Gap-5) ═══════════════════
//
// 8 more families rounding out the mining surface: entity resolution + record
// linkage, causal impact, process mining, root-cause propagation, seeded risk
// propagation, ontology-gap detection, retrieval quality, and a thin
// community-detection wrapper. Each follows the SAME shape as the 9 families
// above (explicit-or-derived input → compute → optional typed-node write-back
// → optional `:Claim`/`:Evidence` epistemic write-back via `materialize_claim`).

// ─────────────────────────── Entity resolution + record linkage ───────────────────────────

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_entity_resolve(
    req_id: u64,
    core: &GraphCore,
    records: Vec<Vec<String>>,
    block_keys: Vec<String>,
    vectors: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    ids: Vec<String>,
    bucket_precision: i32,
    threshold: f64,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    if !records.is_empty() {
        let keys = resolve_block_keys(&block_keys, records.len());
        let matches = entity_resolution::link_records(&records, &keys, threshold);
        let resolved_ids = resolve_ids(&ids, records.len());
        let written = if writeback {
            materialize_entity_matches(core, &matches, &resolved_ids, "jaccard")
        } else {
            0
        };
        #[cfg(feature = "epistemic")]
        if writeback && as_claim {
            materialize_entity_match_claims(core, &matches, &resolved_ids, "records:explicit");
        }
        return entity_resolve_response(req_id, &matches, &resolved_ids, records.len(), written);
    }
    let (rows, resolved_ids) = if !vectors.is_empty() {
        let n = vectors.len();
        (vectors, resolve_ids(&ids, n))
    } else {
        match &source {
            Some(spec) => gather_embeddings(core, spec),
            None => (Vec::new(), Vec::new()),
        }
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let matches = entity_resolution::resolve_entities(&rows, bucket_precision, threshold);
    let written = if writeback {
        materialize_entity_matches(core, &matches, &resolved_ids, "cosine")
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_entity_match_claims(core, &matches, &resolved_ids, &entity_provenance(&source));
    }
    entity_resolve_response(req_id, &matches, &resolved_ids, rows.len(), written)
}

/// Resolve an id vector to exactly `n` entries: explicit ids win positionally;
/// a missing/short entry falls back to its index (stringified).
fn resolve_ids(ids: &[String], n: usize) -> Vec<String> {
    (0..n)
        .map(|i| ids.get(i).cloned().unwrap_or_else(|| i.to_string()))
        .collect()
}

/// Resolve block keys to exactly `n` entries: a length mismatch (the caller
/// omitted them) degrades to ONE global block (empty key for every record).
fn resolve_block_keys(block_keys: &[String], n: usize) -> Vec<String> {
    if block_keys.len() == n {
        block_keys.to_vec()
    } else {
        vec![String::new(); n]
    }
}

fn entity_resolve_response(
    req_id: u64,
    matches: &[entity_resolution::EntityMatch],
    ids: &[String],
    n_records: usize,
    written: usize,
) -> Response {
    let rows: Vec<serde_json::Value> = matches
        .iter()
        .map(|m| {
            let left = ids
                .get(m.left)
                .cloned()
                .unwrap_or_else(|| m.left.to_string());
            let right = ids
                .get(m.right)
                .cloned()
                .unwrap_or_else(|| m.right.to_string());
            serde_json::json!({
                "left": left,
                "right": right,
                "similarity": m.similarity,
                "block_key": m.block_key,
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "matches": rows,
            "n_records": n_records,
            "n_matches": matches.len(),
            "written_back": written,
        })),
    )
}

/// Materialize each match as a typed `:EntityMatch` node (CONCEPT:EG-KG.mining.entity-resolution),
/// id = a deterministic digest of the CANONICALIZED (order-independent) member
/// pair. Linked to both members via `ENTITY_MATCH_MEMBER` edges when resident.
fn materialize_entity_matches(
    core: &GraphCore,
    matches: &[entity_resolution::EntityMatch],
    ids: &[String],
    method: &str,
) -> usize {
    let mut written = 0usize;
    for m in matches {
        let left = ids
            .get(m.left)
            .cloned()
            .unwrap_or_else(|| m.left.to_string());
        let right = ids
            .get(m.right)
            .cloned()
            .unwrap_or_else(|| m.right.to_string());
        let node_id = entity_match_node_id(&left, &right);
        let props = serde_json::json!({
            "type": "EntityMatch",
            "left": left,
            "right": right,
            "similarity": m.similarity,
            "block_key": m.block_key,
            "method": method,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for member in [&left, &right] {
            if core.has_node(member) {
                let edge = serde_json::json!({ "relationship": "ENTITY_MATCH_MEMBER" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), member.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

fn entity_match_node_id(left: &str, right: &str) -> String {
    use sha2::{Digest, Sha256};
    let (a, b) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    let mut hasher = Sha256::new();
    hasher.update(a.as_bytes());
    hasher.update([0u8]);
    hasher.update(b.as_bytes());
    format!("entity_match:{}", hex::encode(&hasher.finalize()[..12]))
}

#[cfg(feature = "epistemic")]
fn materialize_entity_match_claims(
    core: &GraphCore,
    matches: &[entity_resolution::EntityMatch],
    ids: &[String],
    provenance: &str,
) {
    for m in matches {
        let left = ids
            .get(m.left)
            .cloned()
            .unwrap_or_else(|| m.left.to_string());
        let right = ids
            .get(m.right)
            .cloned()
            .unwrap_or_else(|| m.right.to_string());
        let node_id = entity_match_node_id(&left, &right);
        materialize_claim(
            core,
            &node_id,
            "entity_resolution",
            m.similarity.clamp(0.0, 1.0),
            provenance,
        );
    }
}

#[cfg(feature = "epistemic")]
fn entity_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

// ─────────────────────────── Causal impact (ITS / DiD) ───────────────────────────

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_causal_impact(
    req_id: u64,
    core: &GraphCore,
    series: Vec<f64>,
    control: Vec<f64>,
    intervention_index: usize,
    series_id: String,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    if series.is_empty() {
        return Response::err(
            req_id,
            "mining: causal_impact requires a non-empty `series`",
        );
    }
    let effect = if control.is_empty() {
        causal_impact::interrupted_time_series(&series, intervention_index)
    } else {
        causal_impact::diff_in_diff(&series, &control, intervention_index)
    };
    let written = if writeback {
        materialize_causal_effect(core, &effect, &series_id, &series, &control)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_causal_effect_claim(core, &effect, &series_id, &series, &control);
    }
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "pre_mean": effect.pre_mean,
            "post_mean": effect.post_mean,
            "effect_size": effect.effect_size,
            "relative_effect": effect.relative_effect,
            "std_error": effect.std_error,
            "confidence": effect.confidence,
            "method": if control.is_empty() { "its" } else { "did" },
            "written_back": written,
        })),
    )
}

/// Materialize the estimate as a typed `:CausalEffect` node (CONCEPT:EG-KG.mining.causal-impact),
/// id = a deterministic digest of `method` + `series_id` (or the input series
/// when empty, so identical explicit input reproduces the same id on WAL replay).
/// Linked to a resident node named `series_id` via a `CAUSAL_EFFECT_OF` edge
/// when one exists.
fn materialize_causal_effect(
    core: &GraphCore,
    effect: &causal_impact::CausalEffect,
    series_id: &str,
    series: &[f64],
    control: &[f64],
) -> usize {
    let method = if control.is_empty() { "its" } else { "did" };
    let node_id = causal_effect_node_id(method, series_id, series, control);
    let props = serde_json::json!({
        "type": "CausalEffect",
        "method": method,
        "pre_mean": effect.pre_mean,
        "post_mean": effect.post_mean,
        "effect_size": effect.effect_size,
        "relative_effect": effect.relative_effect,
        "std_error": effect.std_error,
        "confidence": effect.confidence,
        "series_id": series_id,
    });
    let Ok(blob) = rmp_serde::to_vec_named(&props) else {
        return 0;
    };
    core.add_node(node_id.clone(), blob);
    if !series_id.is_empty() && core.has_node(series_id) {
        let edge = serde_json::json!({ "relationship": "CAUSAL_EFFECT_OF" });
        if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
            let _ = core.add_edge(node_id, series_id.to_string(), eb);
        }
    }
    1
}

fn causal_effect_node_id(method: &str, series_id: &str, series: &[f64], control: &[f64]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(method.as_bytes());
    hasher.update([0u8]);
    hasher.update(series_id.as_bytes());
    hasher.update([0u8]);
    for v in series {
        hasher.update(v.to_le_bytes());
    }
    hasher.update([0u8]);
    for v in control {
        hasher.update(v.to_le_bytes());
    }
    format!("causal_effect:{}", hex::encode(&hasher.finalize()[..12]))
}

#[cfg(feature = "epistemic")]
fn materialize_causal_effect_claim(
    core: &GraphCore,
    effect: &causal_impact::CausalEffect,
    series_id: &str,
    series: &[f64],
    control: &[f64],
) {
    let method = if control.is_empty() { "its" } else { "did" };
    let node_id = causal_effect_node_id(method, series_id, series, control);
    let provenance = if series_id.is_empty() {
        "series:values".to_string()
    } else {
        format!("series:{series_id}")
    };
    materialize_claim(
        core,
        &node_id,
        "causal_impact",
        effect.confidence.clamp(0.0, 1.0),
        &provenance,
    );
}

// ─────────────────────────── Process mining ───────────────────────────

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_process(
    req_id: u64,
    core: &GraphCore,
    traces: Vec<Vec<String>>,
    process_id: String,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    if traces.is_empty() {
        return Response::err(req_id, "mining: process mining requires non-empty `traces`");
    }
    let (labels, model) = process_mining::alpha_lite_labeled(&traces);
    let written = if writeback {
        materialize_process_model(core, &model, &labels, &process_id)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_process_model_claim(core, &model, &labels, &process_id);
    }
    let label_of = |i: process_mining::ActivityId| labels[i as usize].clone();
    let dfg: Vec<serde_json::Value> = model
        .dfg_edges
        .iter()
        .map(|&(a, b, c)| serde_json::json!({ "from": label_of(a), "to": label_of(b), "count": c }))
        .collect();
    let causal: Vec<serde_json::Value> = model
        .causal
        .iter()
        .map(|&(a, b)| serde_json::json!({ "from": label_of(a), "to": label_of(b) }))
        .collect();
    let parallel: Vec<serde_json::Value> = model
        .parallel
        .iter()
        .map(|&(a, b)| serde_json::json!({ "a": label_of(a), "b": label_of(b) }))
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "dfg": dfg,
            "causal": causal,
            "parallel": parallel,
            "start_activities": model.start_activities.iter().map(|&i| label_of(i)).collect::<Vec<_>>(),
            "end_activities": model.end_activities.iter().map(|&i| label_of(i)).collect::<Vec<_>>(),
            "n_traces": traces.len(),
            "n_activities": model.n_activities,
            "written_back": written,
        })),
    )
}

/// Materialize the footprint as a typed `:ProcessModel` node (CONCEPT:EG-KG.mining.process-mining),
/// linked to every activity that IS a resident node via `PROCESS_MEMBER` edges.
fn materialize_process_model(
    core: &GraphCore,
    model: &process_mining::ProcessModel,
    labels: &[String],
    process_id: &str,
) -> usize {
    let node_id = process_model_node_id(process_id, labels, model);
    let causal: Vec<serde_json::Value> = model
        .causal
        .iter()
        .map(|&(a, b)| serde_json::json!({ "from": labels[a as usize], "to": labels[b as usize] }))
        .collect();
    let parallel: Vec<serde_json::Value> = model
        .parallel
        .iter()
        .map(|&(a, b)| serde_json::json!({ "a": labels[a as usize], "b": labels[b as usize] }))
        .collect();
    let props = serde_json::json!({
        "type": "ProcessModel",
        "activities": labels,
        "causal": causal,
        "parallel": parallel,
        "start_activities": model.start_activities.iter().map(|&i| labels[i as usize].clone()).collect::<Vec<_>>(),
        "end_activities": model.end_activities.iter().map(|&i| labels[i as usize].clone()).collect::<Vec<_>>(),
        "process_id": process_id,
    });
    let Ok(blob) = rmp_serde::to_vec_named(&props) else {
        return 0;
    };
    core.add_node(node_id.clone(), blob);
    for label in labels {
        if core.has_node(label) {
            let edge = serde_json::json!({ "relationship": "PROCESS_MEMBER" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id.clone(), label.clone(), eb);
            }
        }
    }
    1
}

fn process_model_node_id(
    process_id: &str,
    labels: &[String],
    model: &process_mining::ProcessModel,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(process_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(labels.join("\u{1}").as_bytes());
    hasher.update([0u8]);
    for &(a, b, c) in &model.dfg_edges {
        hasher.update(a.to_le_bytes());
        hasher.update(b.to_le_bytes());
        hasher.update((c as u64).to_le_bytes());
    }
    format!("process_model:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality = FOOTPRINT COVERAGE: the fraction of all possible activity pairs
/// (`n choose 2`) that the log actually observed directly-following (causal OR
/// parallel) at least once — already `[0,1]`, `0.0` when fewer than 2 activities.
#[cfg(feature = "epistemic")]
fn materialize_process_model_claim(
    core: &GraphCore,
    model: &process_mining::ProcessModel,
    labels: &[String],
    process_id: &str,
) {
    let node_id = process_model_node_id(process_id, labels, model);
    let n = model.n_activities;
    let total_possible = if n >= 2 {
        (n * (n - 1) / 2) as f64
    } else {
        0.0
    };
    let observed = (model.causal.len() + model.parallel.len()) as f64;
    let confidence = if total_possible > 0.0 {
        (observed / total_possible).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let provenance = if process_id.is_empty() {
        "process:traces".to_string()
    } else {
        format!("process:{process_id}")
    };
    materialize_claim(core, &node_id, "process_mining", confidence, &provenance);
}

// ─────────────────────────── Root-cause propagation ───────────────────────────

/// Index every distinct `edges` endpoint against `nodes` (positional ids),
/// dropping any edge referencing an id NOT present in `nodes`.
fn edge_indices(nodes: &[String], edges: &[(String, String, f64)]) -> Vec<(usize, usize, f64)> {
    let index: std::collections::HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();
    edges
        .iter()
        .filter_map(|(a, b, w)| Some((*index.get(a.as_str())?, *index.get(b.as_str())?, *w)))
        .collect()
}

fn run_root_cause(
    nodes: &[String],
    scores: &[f64],
    edges: &[(String, String, f64)],
    symptom: &str,
    max_hops: usize,
    decay: f64,
) -> Option<root_cause::RootCauseResult> {
    let symptom_idx = nodes.iter().position(|n| n == symptom)?;
    let idx_edges = edge_indices(nodes, edges);
    Some(root_cause::find_root_cause(
        nodes.len(),
        &idx_edges,
        scores,
        symptom_idx,
        max_hops,
        decay,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_root_cause(
    req_id: u64,
    core: &GraphCore,
    nodes: Vec<String>,
    scores: Vec<f64>,
    edges: Vec<(String, String, f64)>,
    symptom: String,
    max_hops: usize,
    decay: f64,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    let Some(out) = run_root_cause(&nodes, &scores, &edges, &symptom, max_hops, decay) else {
        return Response::err(
            req_id,
            "mining: root_cause requires `symptom` to be present in `nodes`",
        );
    };
    let written = if writeback {
        materialize_root_cause(core, &out, &nodes, &symptom)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_root_cause_claim(core, &out, &nodes, &symptom);
    }
    let candidates: Vec<serde_json::Value> = out
        .candidates
        .iter()
        .map(|c| {
            serde_json::json!({
                "node": nodes.get(c.node).cloned().unwrap_or_else(|| c.node.to_string()),
                "score": c.score,
                "hops": c.hops,
            })
        })
        .collect();
    let best = out.best().map(|c| {
        nodes
            .get(c.node)
            .cloned()
            .unwrap_or_else(|| c.node.to_string())
    });
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "symptom": symptom,
            "candidates": candidates,
            "best": best,
            "written_back": written,
        })),
    )
}

/// Materialize the TOP candidate as a typed `:RootCause` node (CONCEPT:EG-KG.mining.root-cause),
/// linked to the symptom (`ROOT_CAUSE_OF`) and the candidate itself
/// (`ROOT_CAUSE_CANDIDATE`) when resident.
fn materialize_root_cause(
    core: &GraphCore,
    out: &root_cause::RootCauseResult,
    nodes: &[String],
    symptom: &str,
) -> usize {
    let Some(best) = out.best() else {
        return 0;
    };
    let cause_id = nodes
        .get(best.node)
        .cloned()
        .unwrap_or_else(|| best.node.to_string());
    let node_id = root_cause_node_id(symptom, &cause_id);
    let props = serde_json::json!({
        "type": "RootCause",
        "symptom": symptom,
        "cause": cause_id,
        "score": best.score,
        "hops": best.hops,
    });
    let Ok(blob) = rmp_serde::to_vec_named(&props) else {
        return 0;
    };
    core.add_node(node_id.clone(), blob);
    if core.has_node(symptom) {
        let edge = serde_json::json!({ "relationship": "ROOT_CAUSE_OF" });
        if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
            let _ = core.add_edge(node_id.clone(), symptom.to_string(), eb);
        }
    }
    if core.has_node(&cause_id) {
        let edge = serde_json::json!({ "relationship": "ROOT_CAUSE_CANDIDATE" });
        if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
            let _ = core.add_edge(node_id, cause_id, eb);
        }
    }
    1
}

fn root_cause_node_id(symptom: &str, cause: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(symptom.as_bytes());
    hasher.update([0u8]);
    hasher.update(cause.as_bytes());
    format!("root_cause:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality mirrors `anomaly`'s `score / (1 + score)` confidence mapping — the
/// TOP candidate's OWN raw responsibility score, not normalized against the
/// candidate list (which would be trivially `1.0` for the top candidate).
#[cfg(feature = "epistemic")]
fn materialize_root_cause_claim(
    core: &GraphCore,
    out: &root_cause::RootCauseResult,
    nodes: &[String],
    symptom: &str,
) {
    let Some(best) = out.best() else {
        return;
    };
    let cause_id = nodes
        .get(best.node)
        .cloned()
        .unwrap_or_else(|| best.node.to_string());
    let node_id = root_cause_node_id(symptom, &cause_id);
    let confidence = (best.score / (1.0 + best.score)).clamp(0.0, 1.0);
    materialize_claim(
        core,
        &node_id,
        "root_cause",
        confidence,
        &format!("symptom:{symptom}"),
    );
}

// ─────────────────────────── Seeded risk propagation ───────────────────────────

fn run_risk_propagation(
    nodes: &[String],
    seed: &[f64],
    edges: &[(String, String, f64)],
    damping: f64,
    tolerance: f64,
    max_iterations: usize,
) -> risk_propagation::RiskScores {
    let idx_edges = edge_indices(nodes, edges);
    let config = risk_propagation::RiskConfig {
        damping: if damping > 0.0 { damping } else { 0.85 },
        tolerance: if tolerance > 0.0 { tolerance } else { 1e-7 },
        max_iterations: max_iterations.max(1),
    };
    risk_propagation::propagate(nodes.len(), &idx_edges, seed, &config)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_risk_propagation(
    req_id: u64,
    core: &GraphCore,
    nodes: Vec<String>,
    seed: Vec<f64>,
    edges: Vec<(String, String, f64)>,
    damping: f64,
    tolerance: f64,
    max_iterations: usize,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    if nodes.is_empty() {
        return Response::err(
            req_id,
            "mining: risk_propagation requires non-empty `nodes`",
        );
    }
    let out = run_risk_propagation(&nodes, &seed, &edges, damping, tolerance, max_iterations);
    let written = if writeback {
        materialize_risk_scores(core, &out, &nodes)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_risk_score_claims(core, &out, &nodes);
    }
    let rows: Vec<serde_json::Value> = nodes
        .iter()
        .zip(&out.scores)
        .map(|(id, &s)| serde_json::json!({ "node": id, "score": s }))
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "scores": rows,
            "iterations": out.iterations,
            "converged": out.converged,
            "written_back": written,
        })),
    )
}

/// Materialize every node with a NON-ZERO propagated score as a typed
/// `:RiskScore` node (CONCEPT:EG-KG.mining.risk-propagation), linked via
/// `RISK_SCORE_OF` when resident.
fn materialize_risk_scores(
    core: &GraphCore,
    out: &risk_propagation::RiskScores,
    nodes: &[String],
) -> usize {
    let mut written = 0usize;
    for (i, id) in nodes.iter().enumerate() {
        let score = out.scores.get(i).copied().unwrap_or(0.0);
        if score <= 0.0 {
            continue;
        }
        let node_id = risk_score_node_id(id);
        let props = serde_json::json!({ "type": "RiskScore", "of": id, "score": score });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        if core.has_node(id) {
            let edge = serde_json::json!({ "relationship": "RISK_SCORE_OF" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id, id.clone(), eb);
            }
        }
        written += 1;
    }
    written
}

fn risk_score_node_id(of: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"risk_score");
    hasher.update([0u8]);
    hasher.update(of.as_bytes());
    format!("risk_score:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality = the node's OWN propagated share, already `[0,1]` (mass-conserving).
#[cfg(feature = "epistemic")]
fn materialize_risk_score_claims(
    core: &GraphCore,
    out: &risk_propagation::RiskScores,
    nodes: &[String],
) {
    for (i, id) in nodes.iter().enumerate() {
        let score = out.scores.get(i).copied().unwrap_or(0.0);
        if score <= 0.0 {
            continue;
        }
        let node_id = risk_score_node_id(id);
        materialize_claim(
            core,
            &node_id,
            "risk_propagation",
            score.clamp(0.0, 1.0),
            "risk:propagated",
        );
    }
}

// ─────────────────────────── Ontology-gap detection ───────────────────────────

/// Project the resident graph's class nodes into [`ontology_gap::ClassNode`]s
/// (CONCEPT:EG-KG.mining.ontology-gap, GRAPH-NATIVE). A node is a "class" when
/// `label` names its exact type, or (when `label` is `None`) its
/// `type`/`node_type` is `Class` or `OwlClass`. `HAS_PROPERTY` edges count
/// declared properties; a `SUBCLASS_OF` edge names a declared parent (resolved
/// when the target is ALSO in this class set); `edge_count` is the class
/// node's total incident edge count anywhere in the graph.
fn build_ontology_classes(
    core: &GraphCore,
    label: &Option<String>,
) -> (Vec<ontology_gap::ClassNode>, Vec<String>) {
    let all_nodes = core.get_nodes();
    let mut ids: Vec<String> = Vec::new();
    for (node_id, blob) in &all_nodes {
        let lbl = node_type_label(blob).unwrap_or_default();
        let is_class = match label {
            Some(want) => &lbl == want,
            None => lbl == "Class" || lbl == "OwlClass",
        };
        if is_class {
            ids.push(node_id.clone());
        }
    }
    if ids.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let class_set: std::collections::HashSet<String> = ids.iter().cloned().collect();
    let mut property_count: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut subclass_of: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut edge_count: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    let all_edges = core.get_edges();
    for (src, dst, blob) in &all_edges {
        let rel = edge_relation_label(blob);
        if class_set.contains(src) {
            *edge_count.entry(src.clone()).or_insert(0) += 1;
            if rel.eq_ignore_ascii_case("has_property") || rel.eq_ignore_ascii_case("hasproperty") {
                *property_count.entry(src.clone()).or_insert(0) += 1;
            }
            if (rel.eq_ignore_ascii_case("subclass_of") || rel.eq_ignore_ascii_case("subclassof"))
                && !subclass_of.contains_key(src)
            {
                subclass_of.insert(src.clone(), dst.clone());
            }
        }
        if class_set.contains(dst) {
            *edge_count.entry(dst.clone()).or_insert(0) += 1;
        }
    }

    let classes: Vec<ontology_gap::ClassNode> = ids
        .iter()
        .map(|id| {
            let declares_parent = subclass_of.contains_key(id);
            let parent_resolves = subclass_of
                .get(id)
                .map(|p| class_set.contains(p))
                .unwrap_or(false);
            ontology_gap::ClassNode {
                property_count: property_count.get(id).copied().unwrap_or(0),
                declares_parent,
                parent_resolves,
                edge_count: edge_count.get(id).copied().unwrap_or(0),
            }
        })
        .collect();
    (classes, ids)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_ontology_gap(
    req_id: u64,
    core: &GraphCore,
    label: Option<String>,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    let (classes, class_ids) = build_ontology_classes(core, &label);
    let gaps = ontology_gap::find_gaps(&classes);
    let written = if writeback {
        materialize_ontology_gaps(core, &gaps, &class_ids)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_ontology_gap_claims(core, &gaps, &class_ids, &label);
    }
    let rows: Vec<serde_json::Value> = gaps
        .iter()
        .map(|g| {
            serde_json::json!({
                "class": class_ids.get(g.class_index).cloned().unwrap_or_default(),
                "kind": g.kind.name(),
                "severity": g.kind.severity(),
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "gaps": rows,
            "n_classes": classes.len(),
            "n_gaps": gaps.len(),
            "written_back": written,
        })),
    )
}

/// Materialize each gap as a typed `:OntologyGap` node (CONCEPT:EG-KG.mining.ontology-gap),
/// linked to its class via a `GAP_OF` edge.
fn materialize_ontology_gaps(
    core: &GraphCore,
    gaps: &[ontology_gap::OntologyGap],
    class_ids: &[String],
) -> usize {
    let mut written = 0usize;
    for g in gaps {
        let Some(class_id) = class_ids.get(g.class_index) else {
            continue;
        };
        let node_id = ontology_gap_node_id(class_id, g.kind.name());
        let props = serde_json::json!({
            "type": "OntologyGap",
            "class": class_id,
            "kind": g.kind.name(),
            "severity": g.kind.severity(),
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        if core.has_node(class_id) {
            let edge = serde_json::json!({ "relationship": "GAP_OF" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id, class_id.clone(), eb);
            }
        }
        written += 1;
    }
    written
}

fn ontology_gap_node_id(class_id: &str, kind: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(class_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(kind.as_bytes());
    format!("ontology_gap:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality = the gap kind's fixed documented severity (see `ontology_gap` module docs).
#[cfg(feature = "epistemic")]
fn materialize_ontology_gap_claims(
    core: &GraphCore,
    gaps: &[ontology_gap::OntologyGap],
    class_ids: &[String],
    label: &Option<String>,
) {
    let provenance = match label {
        Some(l) => format!("ontology:{l}"),
        None => "ontology:*".to_string(),
    };
    for g in gaps {
        let Some(class_id) = class_ids.get(g.class_index) else {
            continue;
        };
        let node_id = ontology_gap_node_id(class_id, g.kind.name());
        materialize_claim(
            core,
            &node_id,
            "ontology_gap",
            g.kind.severity(),
            &provenance,
        );
    }
}

// ─────────────────────────── Retrieval quality ───────────────────────────

fn to_retrieval_trace(spec: &RetrievalTraceSpec) -> retrieval_quality::RetrievalTrace {
    retrieval_quality::RetrievalTrace {
        retrieved: spec.retrieved.clone(),
        relevant: spec.relevant.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_retrieval_quality(
    req_id: u64,
    core: &GraphCore,
    traces: Vec<RetrievalTraceSpec>,
    k: usize,
    query_id: String,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    if traces.is_empty() {
        return Response::err(
            req_id,
            "mining: retrieval_quality requires non-empty `traces`",
        );
    }
    let specs: Vec<retrieval_quality::RetrievalTrace> =
        traces.iter().map(to_retrieval_trace).collect();
    let report = retrieval_quality::evaluate(&specs, k);
    let written = if writeback {
        materialize_retrieval_quality(core, &report, &query_id)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_retrieval_quality_claim(core, &report, &query_id);
    }
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "precision_at_k": report.precision_at_k,
            "recall_at_k": report.recall_at_k,
            "mrr": report.mrr,
            "f1": report.f1,
            "n_queries": report.n_queries,
            "k": report.k,
            "written_back": written,
        })),
    )
}

/// Materialize the aggregate report as a typed `:RetrievalQuality` node
/// (CONCEPT:EG-KG.mining.retrieval-quality), linked to a resident node named
/// `query_id` via `RETRIEVAL_QUALITY_OF` when one exists.
fn materialize_retrieval_quality(
    core: &GraphCore,
    report: &retrieval_quality::RetrievalQuality,
    query_id: &str,
) -> usize {
    let node_id = retrieval_quality_node_id(query_id, report);
    let props = serde_json::json!({
        "type": "RetrievalQuality",
        "precision_at_k": report.precision_at_k,
        "recall_at_k": report.recall_at_k,
        "mrr": report.mrr,
        "f1": report.f1,
        "n_queries": report.n_queries,
        "k": report.k,
        "query_id": query_id,
    });
    let Ok(blob) = rmp_serde::to_vec_named(&props) else {
        return 0;
    };
    core.add_node(node_id.clone(), blob);
    if !query_id.is_empty() && core.has_node(query_id) {
        let edge = serde_json::json!({ "relationship": "RETRIEVAL_QUALITY_OF" });
        if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
            let _ = core.add_edge(node_id, query_id.to_string(), eb);
        }
    }
    1
}

fn retrieval_quality_node_id(
    query_id: &str,
    report: &retrieval_quality::RetrievalQuality,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(query_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(report.n_queries.to_le_bytes());
    hasher.update(report.k.to_le_bytes());
    hasher.update(report.precision_at_k.to_le_bytes());
    hasher.update(report.recall_at_k.to_le_bytes());
    format!(
        "retrieval_quality:{}",
        hex::encode(&hasher.finalize()[..12])
    )
}

/// Quality = the report's own F1 (harmonic mean of precision@k/recall@k), already `[0,1]`.
#[cfg(feature = "epistemic")]
fn materialize_retrieval_quality_claim(
    core: &GraphCore,
    report: &retrieval_quality::RetrievalQuality,
    query_id: &str,
) {
    let node_id = retrieval_quality_node_id(query_id, report);
    let provenance = if query_id.is_empty() {
        "retrieval:traces".to_string()
    } else {
        format!("retrieval:{query_id}")
    };
    materialize_claim(
        core,
        &node_id,
        "retrieval_quality",
        report.f1.clamp(0.0, 1.0),
        &provenance,
    );
}

// ─────────────────────────── Community detection (wraps existing GDS) ───────────────────────────

/// Project the resident graph (optionally restricted to one `label`) into a
/// dense `AdjacencyGraph<usize>` for [`community::detect`], mirroring
/// `build_host_graph`'s node/edge projection but over dense usize indices
/// (what `eg_compute::graph_algos` operates on) instead of [`HostGraph`].
fn build_id_graph(
    core: &GraphCore,
    label: &Option<String>,
) -> (AdjacencyGraph<usize>, Vec<String>) {
    let all_nodes = core.get_nodes();
    let mut ids: Vec<String> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (node_id, blob) in &all_nodes {
        let node_label = node_type_label(blob).unwrap_or_else(|| "_".to_string());
        if let Some(want) = label {
            if &node_label != want {
                continue;
            }
        }
        index.insert(node_id.clone(), ids.len());
        ids.push(node_id.clone());
    }
    let all_edges = core.get_edges();
    let mut adjacency: Vec<(usize, Vec<(usize, f64)>)> =
        (0..ids.len()).map(|i| (i, Vec::new())).collect();
    for (src, dst, _blob) in &all_edges {
        let (Some(&si), Some(&di)) = (index.get(src), index.get(dst)) else {
            continue;
        };
        adjacency[si].1.push((di, 1.0));
    }
    (AdjacencyGraph::from_adjacency(adjacency), ids)
}

fn to_community_algo(a: CommunityAlgorithm) -> community::Algorithm {
    match a {
        CommunityAlgorithm::Louvain => community::Algorithm::Louvain,
        CommunityAlgorithm::LabelPropagation => community::Algorithm::LabelPropagation,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_community(
    req_id: u64,
    core: &GraphCore,
    label: Option<String>,
    algorithm: CommunityAlgorithm,
    resolution: f64,
    max_iterations: usize,
    seed: u64,
    weighted: bool,
    writeback: bool,
    #[cfg(feature = "epistemic")] as_claim: bool,
) -> Response {
    let (graph, ids) = build_id_graph(core, &label);
    let algo = to_community_algo(algorithm);
    let out = community::detect(&graph, algo, resolution, max_iterations, seed, weighted);
    let written = if writeback {
        materialize_communities(core, &out, &ids)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback && as_claim {
        materialize_community_claims(core, &out, &ids, community_provenance(&label));
    }
    let communities: Vec<serde_json::Value> = out
        .communities
        .iter()
        .map(|c| {
            serde_json::json!({
                "members": c.members.iter().map(|&i| ids.get(i).cloned().unwrap_or_else(|| i.to_string())).collect::<Vec<_>>(),
                "density": c.density,
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "communities": communities,
            "modularity": out.modularity,
            "n_nodes": graph.node_count(),
            "written_back": written,
        })),
    )
}

/// Materialize each MULTI-MEMBER community as a typed `:Community` node
/// (CONCEPT:EG-KG.mining.community-writeback) — a singleton community carries
/// no relational signal worth writing back — linked to its members via
/// `COMMUNITY_MEMBER` edges when resident.
fn materialize_communities(
    core: &GraphCore,
    out: &community::CommunityResult,
    ids: &[String],
) -> usize {
    let mut written = 0usize;
    for c in &out.communities {
        let member_ids: Vec<String> = c
            .members
            .iter()
            .map(|&i| ids.get(i).cloned().unwrap_or_else(|| i.to_string()))
            .collect();
        if member_ids.len() < 2 {
            continue;
        }
        let node_id = community_node_id(&member_ids);
        let props = serde_json::json!({
            "type": "Community",
            "members": member_ids,
            "density": c.density,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for m in &member_ids {
            if core.has_node(m) {
                let edge = serde_json::json!({ "relationship": "COMMUNITY_MEMBER" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), m.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

fn community_node_id(member_ids: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted = member_ids.to_vec();
    sorted.sort();
    let mut hasher = Sha256::new();
    hasher.update(sorted.join("\u{1}").as_bytes());
    format!("community:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality = the community's own internal-edge density, already `[0,1]`.
#[cfg(feature = "epistemic")]
fn materialize_community_claims(
    core: &GraphCore,
    out: &community::CommunityResult,
    ids: &[String],
    provenance: String,
) {
    for c in &out.communities {
        let member_ids: Vec<String> = c
            .members
            .iter()
            .map(|&i| ids.get(i).cloned().unwrap_or_else(|| i.to_string()))
            .collect();
        if member_ids.len() < 2 {
            continue;
        }
        let node_id = community_node_id(&member_ids);
        materialize_claim(
            core,
            &node_id,
            "community",
            c.density.clamp(0.0, 1.0),
            &provenance,
        );
    }
}

#[cfg(feature = "epistemic")]
fn community_provenance(label: &Option<String>) -> String {
    match label {
        Some(l) => format!("graph:{l}"),
        None => "graph:*".to_string(),
    }
}

// ─────────────────────────── Row builders (shared) ───────────────────────────

/// Resolve the cluster feature rows: explicit `features` win; then a fused
/// upstream `plan` (CONCEPT:EG-KG.mining.fused-plan-source); then the `source`
/// node-label embedding scan (the cross-modal hook). Returns the rows AND a
/// parallel `ids` vec (node ids for the embedding/plan path, empty for explicit).
/// `Err` only ever originates from the plan leg (CONCEPT:EG-KG.mining.tsdb-typed-absent).
fn build_vectors(
    core: &Arc<GraphCore>,
    features: &[Vec<f64>],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Result<(Vec<Vec<f64>>, Vec<String>), String> {
    if !features.is_empty() {
        return Ok((features.to_vec(), Vec::new()));
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows(
            core,
            p,
            #[cfg(feature = "tsdb")]
            tsdb,
        );
    }
    match source {
        Some(spec) => Ok(gather_embeddings(core, spec)),
        None => Ok((Vec::new(), Vec::new())),
    }
}

/// Resolve the anomaly rows: explicit `features` win, then a 1-D `values` series
/// (each scalar → a one-element row — the tsdb RCA path), then a fused upstream
/// `plan`, then node embeddings. `Err` only ever originates from the plan leg
/// (CONCEPT:EG-KG.mining.tsdb-typed-absent).
fn build_anomaly_rows(
    core: &Arc<GraphCore>,
    features: &[Vec<f64>],
    values: &[f64],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Result<(Vec<Vec<f64>>, Vec<String>), String> {
    if !features.is_empty() {
        return Ok((features.to_vec(), Vec::new()));
    }
    if !values.is_empty() {
        return Ok((values.iter().map(|&v| vec![v]).collect(), Vec::new()));
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows(
            core,
            p,
            #[cfg(feature = "tsdb")]
            tsdb,
        );
    }
    match source {
        Some(spec) => Ok(gather_embeddings(core, spec)),
        None => Ok((Vec::new(), Vec::new())),
    }
}

/// WAL-replay counterpart of [`build_vectors`] (CONCEPT:EG-KG.mining.frequent-itemset-mining, L34). `replay`
/// (crash recovery) runs off a bare `&GraphCore` — no live `Arc` in hand, unlike the served
/// `Mine*` handlers `try_handle` dispatches with one — so it cannot construct a
/// `ServedTextIndex` for a plan-sourced `RankText`/`FuseRrf` leg; that pushdown is a served-
/// path optimization (L34 mirrors EG-P1-4's `run_unified` served call sites), not a crash-
/// recovery requirement, and re-deriving `wal.rs`'s `apply`/`replay` call chain to carry an
/// `Arc<GraphCore>` instead would be an unrelated, much larger refactor. This keeps the
/// pre-L34 snapshot-derived text-index behavior for replay's plan leg (a documented, narrow
/// scope cut); its explicit-features and embedding-label-scan legs are identical either way.
fn build_vectors_replay(
    core: &GraphCore,
    features: &[Vec<f64>],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
) -> (Vec<Vec<f64>>, Vec<String>) {
    if !features.is_empty() {
        return (features.to_vec(), Vec::new());
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows_snapshot(core, p);
    }
    match source {
        Some(spec) => gather_embeddings(core, spec),
        None => (Vec::new(), Vec::new()),
    }
}

/// WAL-replay counterpart of [`build_anomaly_rows`] — see [`build_vectors_replay`]'s docs
/// for why replay keeps the snapshot-derived (non-`Arc`) plan leg.
fn build_anomaly_rows_replay(
    core: &GraphCore,
    features: &[Vec<f64>],
    values: &[f64],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
) -> (Vec<Vec<f64>>, Vec<String>) {
    if !features.is_empty() {
        return (features.to_vec(), Vec::new());
    }
    if !values.is_empty() {
        return (values.iter().map(|&v| vec![v]).collect(), Vec::new());
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows_snapshot(core, p);
    }
    match source {
        Some(spec) => gather_embeddings(core, spec),
        None => (Vec::new(), Vec::new()),
    }
}

/// Gather the stored embedding of every node carrying `spec.node_label` (skipping
/// nodes without one). Compute-near-data: the vectors are read straight off the
/// resident semantic store.
fn gather_embeddings(core: &GraphCore, spec: &VectorSource) -> (Vec<Vec<f64>>, Vec<String>) {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let store = core.semantic_store.read();
    let mut rows: Vec<Vec<f64>> = Vec::with_capacity(owners.len());
    let mut ids: Vec<String> = Vec::with_capacity(owners.len());
    for (node_id, _blob) in owners {
        if let Some(vec) = store.get_embedding(&node_id) {
            rows.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(node_id);
        }
    }
    (rows, ids)
}

/// Bundles what a plan-sourced mining leg (CONCEPT:EG-KG.mining.fused-plan-source) needs to
/// resolve and bind the server's live tsdb store for an `Op::TsScan` leg, mirroring
/// `query::run_unified`'s own `TsdbLegBind` — the difference is WHERE the scope gets
/// resolved: a served `UnifiedQuery` resolves it once per request at the top of its own
/// handler, while mining resolves it once per plan inside [`gather_plan_rows`], since
/// FIVE different `Mine*` handlers share that one call site (resolving there, rather
/// than duplicating the "does this plan need tsdb" check in every handler, keeps a
/// single source of truth). `read_authority`/`tsdb_store` are `None` when no live server
/// wiring is available (the `#[cfg(test)]` `dispatch_for_test` harness); a `TsScan`-bearing
/// plan is then a typed error rather than the old silent-empty degrade
/// (CONCEPT:EG-KG.mining.tsdb-typed-absent) — a plan that never touches tsdb is unaffected.
#[cfg(all(feature = "query", feature = "tsdb"))]
#[derive(Clone, Copy)]
pub(crate) struct MiningTsdbBind<'a> {
    pub graph_name: &'a str,
    pub read_authority: Option<&'a crate::server::access::GraphReadAuthority>,
    pub tsdb_store: Option<&'a Arc<eg_tsdb::store::SeriesStore>>,
}

/// Run an upstream cross-modal RETRIEVAL `plan` (`Op::Scan|Filter|Traverse|Rank|…`)
/// over a fresh graph+semantic snapshot and resolve each resulting row's id to its
/// stored embedding — the SAME lookup [`gather_embeddings`] uses for a bare
/// `VectorSource` label scan, generalized to an ARBITRARY upstream plan
/// (CONCEPT:EG-KG.mining.fused-plan-source). This is the fused `retrieve → mine →
/// writeback` mechanism: the retrieval legs (vector rank / graph traverse / SQL
/// filter / OWL reason / …) run FIRST, compute-near-data, over the SAME snapshot
/// the mining op then reads embeddings from — ONE round-trip, no client
/// marshalling between "retrieve the candidate set" and "mine it". A plan
/// execution error degrades to an empty row set (never panics/propagates) —
/// consistent with every other mining source's "no match ⇒ empty" contract —
/// EXCEPT for a plan-sourced `Op::TsScan` leg (CONCEPT:EG-KG.mining.tsdb-typed-absent):
/// that ONE case now returns a typed `Err` when the server genuinely has no live
/// tsdb store (or no verified carrier authority) bound, rather than silently
/// degrading to empty — a `TsScan`-bearing mining plan is otherwise
/// indistinguishable from "your query legitimately matched nothing".
#[cfg(feature = "query")]
fn gather_plan_rows(
    core: &Arc<GraphCore>,
    plan: &crate::wire::Plan,
    #[cfg(feature = "tsdb")] tsdb: MiningTsdbBind<'_>,
) -> Result<(Vec<Vec<f64>>, Vec<String>), String> {
    let snap = core.analysis_snapshot();
    // CONCEPT:EG-KG.query.served-vector-index-binding / served-text-index-binding — push the
    // vector leg into the LIVE persistent `SemanticStore` via a guard, reused for the embedding
    // lookups below too, instead of a `.clone()` that (on the default HNSW backend) would have
    // forced a full rebuild on its first search. L34: `gather_plan_rows` now takes an
    // `Arc<GraphCore>` (mirroring EG-P1-4's served `run_unified` call sites), so a `RankText`/
    // `FuseRrf` leg in a mining-sourced plan ALSO pushes down into the graph's MAINTAINED
    // persistent `GraphTextIndex` via `ServedTextIndex`, instead of falling back to a
    // snapshot-derived index rebuilt from `snap` on every mining request.
    #[cfg(feature = "text")]
    let served_text = crate::server::secondary_indexes::ServedTextIndex::new(core.clone());
    // L37: an `Arc<GraphCore>` in hand ⇒ a `SpatialScan` leg in a mining-sourced plan ALSO
    // pushes down into the graph's MAINTAINED persistent spatial index, same as the text leg.
    #[cfg(feature = "geo")]
    let served_spatial = crate::server::secondary_indexes::ServedSpatialIndex::new(core.clone());
    // CONCEPT:EG-KG.mining.tsdb-typed-absent — resolve the SAME verified tenant/namespace scope
    // the served `UnifiedQuery` path resolves (`query::served_tsdb_scope`, single source of
    // truth), THEN require the live store to actually be bound before falling through to the
    // old silent-empty degrade. `None` (the plan has no `TsScan` leg) is unaffected.
    #[cfg(feature = "tsdb")]
    let tsdb_scope = crate::server::handlers::query::served_tsdb_scope(
        plan,
        tsdb.graph_name,
        tsdb.read_authority,
    )?;
    #[cfg(feature = "tsdb")]
    if tsdb_scope.is_some() && tsdb.tsdb_store.is_none() {
        return Err(
            "graph_mine: plan requires Op::TsScan but this server has no time-series store \
             configured"
                .to_string(),
        );
    }
    let store = core.semantic_store.read();
    let rows = match crate::server::handlers::query::run_unified(
        plan.clone(),
        &snap,
        &store,
        crate::server::handlers::query::ServedIndexes {
            #[cfg(feature = "text")]
            text: Some(&served_text),
            #[cfg(feature = "geo")]
            spatial: Some(&served_spatial),
            #[cfg(not(any(feature = "text", feature = "geo")))]
            _marker: std::marker::PhantomData,
        },
        #[cfg(feature = "tsdb")]
        match &tsdb_scope {
            Some((tenant, graph)) => crate::server::handlers::query::TsdbLegBind {
                tsdb: tsdb.tsdb_store.map(|store| store.as_ref()),
                tsdb_tenant: Some(tenant.as_str()),
                tsdb_graph: Some(graph.as_str()),
                staged_series: None,
            },
            None => crate::server::handlers::query::TsdbLegBind {
                tsdb: None,
                tsdb_tenant: None,
                tsdb_graph: None,
                staged_series: None,
            },
        },
    ) {
        Ok(rows) => rows,
        Err(_) => return Ok((Vec::new(), Vec::new())),
    };
    let mut feats: Vec<Vec<f64>> = Vec::with_capacity(rows.len());
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    for (row_id, score) in rows {
        if let Some(vec) = store.get_embedding(&row_id) {
            feats.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(row_id);
            continue;
        }
        // CONCEPT:EG-KG.mining.tsdb-typed-absent — a time-series SOURCE row (an `Op::TsScan`
        // point): the id IS the point timestamp — not a graph node, so it never resolves to
        // an embedding — and `score` IS the value. Mirrors EXACTLY how `window_aggregate`
        // (CONCEPT:EG-KG.compute.tsscan-series-window-60s, `crates/eg-plan/src/exec.rs`)
        // already disambiguates a TsScan-produced row from a graph-node row. Without this
        // fallback a TsScan-sourced mining plan would still yield zero feature rows even
        // after binding the live tsdb store above: the row would round-trip through
        // `run_unified` correctly, then be silently dropped HERE by the embedding lookup —
        // the exact same "a TsScan row's id is not a node, so it's dropped" failure mode
        // `window_aggregate`'s own fix note describes. A row id that merely LOOKS numeric
        // but isn't a real timestamp is indistinguishable from a genuine one at this layer,
        // exactly as `window_aggregate` accepts the same ambiguity.
        if let (Ok(_ts), Some(value)) = (row_id.parse::<i64>(), score) {
            feats.push(vec![value as f64]);
            ids.push(row_id);
        }
    }
    Ok((feats, ids))
}

/// WAL-replay counterpart of [`gather_plan_rows`] — the pre-L34 behavior, kept for
/// [`build_vectors_replay`]/[`build_anomaly_rows_replay`] (see their docs): no `Arc` in
/// hand, so no `ServedTextIndex` — a `RankText`/`FuseRrf` leg in a replay-sourced plan falls
/// back to the snapshot-derived text index built fresh from `snap`, exactly as every
/// mining plan leg behaved before the served-path pushdown.
#[cfg(feature = "query")]
fn gather_plan_rows_snapshot(
    core: &GraphCore,
    plan: &crate::wire::Plan,
) -> (Vec<Vec<f64>>, Vec<String>) {
    let snap = core.analysis_snapshot();
    let store = core.semantic_store.read();
    let rows = match crate::server::handlers::query::run_unified(
        plan.clone(),
        &snap,
        &store,
        crate::server::handlers::query::ServedIndexes::default(),
        #[cfg(feature = "tsdb")]
        crate::server::handlers::query::TsdbLegBind {
            tsdb: None,
            tsdb_tenant: None,
            tsdb_graph: None,
            staged_series: None,
        },
    ) {
        Ok(rows) => rows,
        Err(_) => return (Vec::new(), Vec::new()),
    };
    let mut feats: Vec<Vec<f64>> = Vec::with_capacity(rows.len());
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    for (node_id, _score) in rows {
        if let Some(vec) = store.get_embedding(&node_id) {
            feats.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(node_id);
        }
    }
    (feats, ids)
}

/// Reject a ragged feature matrix (rows of differing width) with a clean error
/// rather than letting a distance computation panic; an empty matrix is allowed
/// (⇒ an empty result).
fn validate_matrix(rows: &[Vec<f64>]) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }
    let width = rows[0].len();
    if width == 0 {
        return Err("mining: feature rows must be non-empty".into());
    }
    if rows.iter().any(|r| r.len() != width) {
        return Err("mining: all feature rows must have the same dimensionality".into());
    }
    Ok(())
}

// ─────────────────── Epistemic claim write-back (E6, feature `epistemic`) ───────────────────
//
// CONCEPT:EG-KG.epistemic.epistemic-substrate — turn a mined finding into a
// first-class epistemic object. When a `Mine*` request sets `as_claim=true` (which
// requires `writeback`, since the mined `:AssociationRule`/`:Cluster`/… node is the
// claim's evidence anchor), each finding ADDITIONALLY materializes:
//
//   * one `:Claim` node — `confidence` seeded from that family's quality score,
//     normalized to `[0,1]`, and `validation_state = "unvalidated"`;
//   * one `:Evidence` node — capturing the request's OWN `source`/`plan` provenance
//     (no new provenance plumbing; reuse what the request already carries);
//   * two `SUPPORTS` edges (`mined_node -> claim`, `evidence -> claim`) written with
//     the canonical `relationship` property that `eg_epistemic::classify_relationship`
//     + `BeliefGraph::from_graph_view` read — so the belief layer propagates confidence
//     over the finding. (The structural mining edges use the `relation` key instead, so
//     they stay epistemically neutral and never pollute belief.)
//   * one `:Activity` node (CONCEPT:EG-P3-1, the universal writeback-lineage tuple) +
//     one `claim --GENERATED_BY--> activity` edge — the generating transformation: the
//     mining family/algorithm, this crate's own build version, a runtime/feature-set
//     env fingerprint, and the graph's OCC version at commit time. `GENERATED_BY`, like
//     `relation`, is NOT one of `classify_relationship`'s whitelisted values, so it is
//     automatically epistemically neutral (ignored by `BeliefGraph`) — never pollutes
//     belief propagation. `eg-plan`'s `KnowledgeSet::from_rowset` resolves it into
//     `KnowledgeRow::transformation_ids` (see that crate's `knowledge.rs` docs).
//
// Ids are deterministic: the claim id folds in the mined node id (re-mining the same
// finding re-points at the SAME claim — idempotent WAL replay); the evidence id ALSO
// folds in the provenance, so a DISTINCT provenance corroborates the SAME claim with an
// ADDITIONAL supporter (the E1↔E6 corroboration path — two mining runs raise the belief
// above one); the activity id folds in `(family, provenance)` too, so repeated runs over
// the SAME provenance converge on the SAME Activity (idempotent, mirrors the evidence id).
// With `as_claim` unset the whole path is skipped, so behavior is byte-identical to the
// pre-E6 write-back.

/// `validation_state` metadata seeded on every fresh `:Claim`/`:Evidence` (E6). The
/// claim is asserted from a mined finding, not yet validated by a downstream check.
#[cfg(feature = "epistemic")]
const CLAIM_VALIDATION_STATE: &str = "unvalidated";

/// This crate's own build version — the `algo_code_version` leg of the universal
/// writeback-lineage tuple (CONCEPT:EG-P3-1), mirroring `eg_jobs::AlgoVersion::code_version`'s
/// own doc ("the engine build that ran it, e.g. `CARGO_PKG_VERSION`").
#[cfg(feature = "epistemic")]
const ALGO_CODE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The runtime/feature-set fingerprint the algorithm ran under (CONCEPT:EG-P3-1) — the
/// `algo_env_version` leg, mirroring `eg_jobs::AlgoVersion::env_version`'s own doc. Kept
/// simple/deterministic-per-build: target OS + architecture.
#[cfg(feature = "epistemic")]
fn algo_env_version() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

/// Materialize the epistemic quartet (`:Claim` + `:Evidence` + `:Activity` + their
/// `SUPPORTS`/`GENERATED_BY` edges) for ONE mined finding whose typed node
/// (`mined_node_id`) was just written back. Beyond the claim/evidence pair (E6), this now
/// ALSO stamps the universal writeback-lineage tuple (CONCEPT:EG-P3-1): an input-snapshot
/// handle (the core's OCC `version()` at commit time), algo family/code/env version,
/// a `calibration` slot (honestly `null` — no calibration signal is computed by the
/// generic mining path today; a future family-specific caller can populate it),
/// and `invalidation_deps` — the ids whose change/removal invalidates this claim (the
/// mined finding + its evidence node; this is also exactly the `SUPPORTS` topology
/// `eg_epistemic::propagate_confidence` already walks, so a change to either
/// automatically reflows through the claim's belief — this property just makes that
/// dependency set an explicit, directly-queryable list).
#[cfg(feature = "epistemic")]
fn materialize_claim(
    core: &GraphCore,
    mined_node_id: &str,
    family: &str,
    confidence: f64,
    provenance: &str,
) {
    let confidence = confidence.clamp(0.0, 1.0);
    let claim_id = claim_node_id(family, mined_node_id);
    let evidence_id = evidence_node_id(family, mined_node_id, provenance);
    let activity_id = activity_node_id(family, provenance);
    let claim_props = serde_json::json!({
        "type": "Claim",
        "family": family,
        "about": mined_node_id,
        "confidence": confidence,
        "validation_state": CLAIM_VALIDATION_STATE,
        // CONCEPT:EG-P3-1 — universal writeback-lineage tuple.
        "input_snapshot_version": core.version(),
        "algo_family": family,
        "algo_provenance": provenance,
        "algo_code_version": ALGO_CODE_VERSION,
        "algo_env_version": algo_env_version(),
        "calibration": serde_json::Value::Null,
        "invalidation_deps": [mined_node_id, evidence_id.as_str()],
    });
    if let Ok(blob) = rmp_serde::to_vec_named(&claim_props) {
        core.add_node(claim_id.clone(), blob);
    }
    // The mined finding itself is evidence FOR the claim.
    supports_edge(core, mined_node_id, &claim_id);
    // A provenance-anchored Evidence node (distinct provenance ⇒ corroboration).
    let ev_props = serde_json::json!({
        "type": "Evidence",
        "family": family,
        "about": mined_node_id,
        "provenance": provenance,
        "confidence": confidence,
        "validation_state": CLAIM_VALIDATION_STATE,
    });
    if let Ok(blob) = rmp_serde::to_vec_named(&ev_props) {
        core.add_node(evidence_id.clone(), blob);
    }
    supports_edge(core, &evidence_id, &claim_id);

    // CONCEPT:EG-P3-1 — the generating Activity (the mining run itself). One per
    // `(family, provenance)` (idempotent, mirrors `evidence_node_id`'s dedup-by-
    // provenance): re-running the same family+provenance converges on the SAME
    // Activity rather than accumulating a fresh one per call.
    let activity_props = serde_json::json!({
        "type": "Activity",
        "family": family,
        "provenance": provenance,
        "algo_code_version": ALGO_CODE_VERSION,
        "algo_env_version": algo_env_version(),
        "input_snapshot_version": core.version(),
    });
    if let Ok(blob) = rmp_serde::to_vec_named(&activity_props) {
        core.add_node(activity_id.clone(), blob);
    }
    generated_by_edge(core, &claim_id, &activity_id);
}

/// Write one epistemic `source --SUPPORTS--> target` edge using the canonical `relationship`
/// property key `eg_epistemic` reads (NOT the `relation` key the structural mining edges
/// use). Both endpoints are freshly resident, so `add_edge` always binds.
#[cfg(feature = "epistemic")]
fn supports_edge(core: &GraphCore, source: &str, target: &str) {
    let edge = serde_json::json!({ "relationship": "SUPPORTS" });
    if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
        let _ = core.add_edge(source.to_string(), target.to_string(), eb);
    }
}

/// Write one `claim --GENERATED_BY--> activity` edge (CONCEPT:EG-P3-1) — deliberately
/// NOT one of `classify_relationship`'s whitelisted values, so `BeliefGraph` ignores it
/// (epistemically neutral, exactly like the mining handlers' `relation`-keyed structural
/// edges). `eg-plan`'s `KnowledgeSet::from_rowset` resolves it into
/// `KnowledgeRow::transformation_ids`.
#[cfg(feature = "epistemic")]
fn generated_by_edge(core: &GraphCore, source: &str, target: &str) {
    let edge = serde_json::json!({ "relationship": "GENERATED_BY" });
    if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
        let _ = core.add_edge(source.to_string(), target.to_string(), eb);
    }
}

/// Deterministic `:Claim` node id — folds in `family` + the mined node id, so re-mining
/// the same finding re-points at the same claim (idempotent replay + corroboration).
#[cfg(feature = "epistemic")]
fn claim_node_id(family: &str, mined_node_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"claim");
    h.update([0u8]);
    h.update(family.as_bytes());
    h.update([0u8]);
    h.update(mined_node_id.as_bytes());
    format!("claim:{}", hex::encode(&h.finalize()[..12]))
}

/// Deterministic `:Evidence` node id — ALSO folds in `provenance`, so two runs over
/// DIFFERENT provenance produce distinct evidence nodes that both support the same claim.
#[cfg(feature = "epistemic")]
fn evidence_node_id(family: &str, mined_node_id: &str, provenance: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"evidence");
    h.update([0u8]);
    h.update(family.as_bytes());
    h.update([0u8]);
    h.update(mined_node_id.as_bytes());
    h.update([0u8]);
    h.update(provenance.as_bytes());
    format!("evidence:{}", hex::encode(&h.finalize()[..12]))
}

/// Deterministic `:Activity` node id (CONCEPT:EG-P3-1) — folds in `(family, provenance)`,
/// so repeated runs over the SAME provenance converge on the SAME generating Activity
/// (idempotent, mirrors `evidence_node_id`'s dedup-by-provenance).
#[cfg(feature = "epistemic")]
fn activity_node_id(family: &str, provenance: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"activity");
    h.update([0u8]);
    h.update(family.as_bytes());
    h.update([0u8]);
    h.update(provenance.as_bytes());
    format!("activity:{}", hex::encode(&h.finalize()[..12]))
}

// ── Per-family claim passes (mirror each `materialize_*` node/id + quality score) ──

/// Association rules → claims. Quality = `support × confidence` (both already `[0,1]`,
/// jointly monotonic). Provenance = the transaction `source` label (or `explicit`).
#[cfg(feature = "epistemic")]
fn materialize_rule_claims(
    core: &GraphCore,
    rules: &[LabeledRule],
    source: &Option<TransactionSource>,
) {
    let provenance = assoc_provenance(source);
    for r in rules {
        let node_id = rule_node_id(&r.antecedent, &r.consequent);
        let confidence = (r.support * r.confidence).clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "association", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
fn assoc_provenance(source: &Option<TransactionSource>) -> String {
    match source {
        Some(s) => format!("txn:{}/{}", s.node_label, s.direction),
        None => "txn:explicit".to_string(),
    }
}

/// Clusters → claims (skipping the DBSCAN noise bucket, mirroring `materialize_clusters`).
/// Quality = `1/(1 + compactness score)` — a tighter cluster (lower mean member→centroid
/// distance) yields a higher-confidence claim.
#[cfg(feature = "epistemic")]
fn materialize_cluster_claims(
    core: &GraphCore,
    out: &cluster::Clustering,
    ids: &[String],
    algorithm: ClusterAlgorithm,
    provenance: String,
) {
    let algo = cluster_algo_name(algorithm);
    for c in &out.clusters {
        if c.cluster_id < 0 {
            continue; // never claim the DBSCAN noise bucket
        }
        let member_ids: Vec<String> = c
            .members
            .iter()
            .map(|&i| match ids.get(i) {
                Some(id) => id.clone(),
                None => i.to_string(),
            })
            .collect();
        let node_id = cluster_node_id(algo, &member_ids);
        let confidence = 1.0 / (1.0 + c.score.max(0.0));
        materialize_claim(core, &node_id, "cluster", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
fn cluster_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

/// Flagged anomalies → claims (only the flagged rows, mirroring `materialize_anomalies`).
/// Quality = `score / (1 + score)` — a higher anomaly score yields a higher-confidence
/// "this row is anomalous" claim.
#[cfg(feature = "epistemic")]
fn materialize_anomaly_claims(
    core: &GraphCore,
    out: &anomaly::Anomalies,
    ids: &[String],
    algorithm: AnomalyAlgorithm,
    provenance: String,
) {
    let algo = anomaly_algo_name(algorithm);
    for i in 0..out.scores.len() {
        if !out.is_anomaly[i] {
            continue;
        }
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = anomaly_node_id(algo, &src);
        let s = out.scores[i].max(0.0);
        let confidence = s / (1.0 + s);
        materialize_claim(core, &node_id, "anomaly", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
fn anomaly_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

/// Sequential patterns → claims. Quality = the pattern's fractional `support`.
#[cfg(feature = "epistemic")]
fn materialize_sequence_claims(core: &GraphCore, patterns: &[LabeledPattern], provenance: String) {
    for p in patterns {
        let node_id = pattern_node_id(&p.items);
        let confidence = p.support.clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "sequence", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
fn sequence_provenance(source: &Option<SequenceSource>) -> String {
    match source {
        Some(s) => format!("seq:{}/{}", s.node_label, s.direction),
        None => "seq:explicit".to_string(),
    }
}

/// The single forecast → one claim. Quality = the forecast's `confidence` band level.
#[cfg(feature = "epistemic")]
fn materialize_forecast_claim(
    core: &GraphCore,
    series_id: &str,
    values: &[f64],
    algo: &str,
    confidence: f64,
) {
    let node_id = forecast_node_id(algo, series_id, values);
    let provenance = if series_id.is_empty() {
        "series:values".to_string()
    } else {
        format!("series:{series_id}")
    };
    materialize_claim(
        core,
        &node_id,
        "forecast",
        confidence.clamp(0.0, 1.0),
        &provenance,
    );
}

/// Frequent subgraph patterns → claims. Quality = the pattern's fractional `support`.
#[cfg(feature = "epistemic")]
fn materialize_subgraph_claims(
    core: &GraphCore,
    results: &[subgraph::FrequentSubgraph],
    provenance: String,
) {
    for r in results {
        let node_id = subgraph_node_id(&r.pattern);
        let confidence = r.support.clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "subgraph", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
fn subgraph_provenance(label: &Option<String>) -> String {
    match label {
        Some(l) => format!("graph:{l}"),
        None => "graph:*".to_string(),
    }
}

// ── D3 — the 3 remaining mining families (E6 left these ungated) ──
//
// MineClassifyPredict → claims (principled: the prediction's OWN max class
// probability, already [0,1] by construction — a probability-simplex row).
// MineReduce → claims, `svd` ONLY (principled: the retained explained-variance
// ratio; `lda`/`umap`/`tsne` have no such score — documented no-op, NOT fabricated).
// MineText → claims, `lda`/`nmf` ONLY (principled: per-topic mean doc-membership
// strength among its dominantly-assigned documents — both engines' `doc_topics`
// are already [0,1] distributions summing to 1, see `eg_compute::mining::text`
// module docs; `tfidf` has no topics — documented no-op, mirroring `writeback`).

/// Each prediction → a claim. Quality = the row's OWN max class probability
/// (`out.proba[i]`'s argmax) — already `[0,1]`, no normalization needed.
#[cfg(feature = "epistemic")]
fn materialize_classification_claims(
    core: &GraphCore,
    out: &classify::Classification,
    ids: &[String],
    provenance: String,
) {
    for i in 0..out.labels.len() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = classification_node_id(&src);
        let confidence = out.proba[i]
            .iter()
            .cloned()
            .fold(0.0_f64, f64::max)
            .clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "classification", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
fn classify_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

/// Reduced rows → claims, `svd` ONLY (mirroring `writeback`'s per-algorithm gate —
/// this is a DOCUMENTED SKIP for `lda`/`umap`/`tsne`, not a fabricated score: LDA's
/// discriminant eigenvalues aren't returned by `reduce::reduce`; UMAP/t-SNE are
/// approximate neighborhood LAYOUTS with no reconstruction-error analogue). Quality =
/// the retained EXPLAINED-VARIANCE RATIO `Σ singular_values² / Σ ‖row‖²` — the SAME
/// Frobenius-energy ratio `eg_compute::mining::reduce`'s own
/// `truncated_svd_reconstructs_low_rank` test validates against, so it is principled
/// and requires no extra normalization beyond the final `[0,1]` clamp (guards the
/// pathological case where retained energy rounds slightly above total due to
/// floating-point error). The ratio is a property of the WHOLE projection, not any
/// one row, so every materialized `:Embedding2D` row node gets a claim sharing this
/// ONE reduction-level score (mirrors the "one score per materialized artifact"
/// shape every other mining family's claim pass uses).
#[cfg(feature = "epistemic")]
fn materialize_reduce_claims(
    core: &GraphCore,
    rows: &[Vec<f64>],
    out: &reduce::Reduction,
    ids: &[String],
    algorithm: ReduceAlgorithm,
    source: &Option<VectorSource>,
) {
    if !matches!(algorithm, ReduceAlgorithm::Svd) || out.singular_values.is_empty() {
        return; // no principled [0,1] score for lda/umap/tsne — documented skip
    }
    let total_energy: f64 = rows.iter().flatten().map(|&v| v * v).sum();
    if total_energy <= 0.0 {
        return; // degenerate (all-zero) input — no meaningful ratio to claim
    }
    let retained: f64 = out.singular_values.iter().map(|&s| s * s).sum();
    let confidence = (retained / total_energy).clamp(0.0, 1.0);
    let provenance = reduce_provenance(source);
    for i in 0..out.coords.len() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = embedding2d_node_id(&src);
        materialize_claim(core, &node_id, "reduce", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
fn reduce_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

/// Each topic → a claim, `lda`/`nmf` ONLY (mirroring `writeback`'s tfidf no-op — a
/// bag-of-weights table has no topics to claim about). Quality = the topic's mean
/// doc-membership strength among the documents DOMINANTLY assigned to it
/// (`mean(doc_topics[d][t])` over docs `d` whose argmax topic is `t`) — a topic-
/// coherence proxy: both LDA's Dirichlet posterior and NMF's row-normalized `W` are
/// already `[0,1]` distributions summing to 1 across topics (see
/// `eg_compute::mining::text` module docs), so this is principled and needs no extra
/// normalization. A topic nobody is dominantly assigned to (can happen for `nmf`,
/// whose factors are not a hard partition) has no coherence signal — skipped rather
/// than fabricated.
#[cfg(feature = "epistemic")]
fn materialize_topic_claims(core: &GraphCore, out: &text::LabeledTextResult, algo: &str) {
    let dominant: Vec<usize> = out
        .doc_topics
        .iter()
        .map(|dist| {
            dist.iter()
                .enumerate()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(t, _)| t)
                .unwrap_or(0)
        })
        .collect();

    for (t, terms) in out.topics.iter().enumerate() {
        let term_labels: Vec<&str> = terms.iter().map(|(term, _)| term.as_str()).collect();
        let mut sum = 0.0_f64;
        let mut n = 0usize;
        for (i, dist) in out.doc_topics.iter().enumerate() {
            if dominant.get(i) == Some(&t) {
                if let Some(&w) = dist.get(t) {
                    sum += w;
                    n += 1;
                }
            }
        }
        if n == 0 {
            continue; // no document dominantly assigned to this topic — no signal
        }
        let node_id = topic_node_id(algo, &term_labels);
        let confidence = (sum / n as f64).clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "topic", confidence, &format!("text:{algo}"));
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
                .add_embedding(id.to_string(), e.to_vec());
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
                .add_embedding(id.to_string(), e.to_vec());
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
                .add_embedding(id.to_string(), e.to_vec());
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
                .add_embedding(id.to_string(), e.to_vec());
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
                .add_embedding(id.to_string(), e.to_vec());
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
                    .add_embedding(id.to_string(), e.to_vec());
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
                    .add_embedding(id.to_string(), e.to_vec());
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
                    .add_embedding(id.to_string(), e.to_vec());
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
                    .add_embedding(id.to_string(), e.to_vec());
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
        #[cfg(feature = "epistemic")]
        {
            assert_no_claims(&core);
            let c1 = Arc::new(GraphCore::new());
            dispatch_for_test(102, Arc::clone(&c1), mk(true)).expect("handled");
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
        assert_eq!(v["written_back"], 1);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("RetrievalQuality", 0).len(), 1);
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
