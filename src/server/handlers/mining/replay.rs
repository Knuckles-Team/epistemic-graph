use super::*;
use super::{
    association::*, classic::*, entity::*, input::*, insight::*, process::*, vector::*,
    writeback::*,
};
use eg_compute::mining::{
    anomaly, association, causal_impact,
    classify::{self, FittedClassifier},
    cluster, community, entity_resolution, forecast, ontology_gap, process_mining, reduce,
    retrieval_quality, sequence, subgraph, text,
};

/// Re-run mining write-back operations during WAL replay. Deterministic explicit
/// inputs are reused, while graph-derived inputs are projected from current state.
#[allow(dead_code)]
pub(crate) fn replay(core: &GraphCore, method: &Method) {
    // Keep the replay entry point as a small family router. Each family helper
    // recognizes its own writeback request and returns true even for a
    // validation no-op, so one request cannot be replayed twice.
    let _ = replay_transaction_family(core, method)
        || replay_vector_family(core, method)
        || replay_series_family(core, method)
        || replay_document_family(core, method)
        || replay_graph_family(core, method);
}

/// Route transaction-shaped replay requests to their family-specific execution
/// helpers. A true result means the request matched, even when validation
/// produced an intentional no-op.
pub(super) fn replay_transaction_family(core: &GraphCore, method: &Method) -> bool {
    replay_associate(core, method) || replay_sequence(core, method)
}

/// Route vector/model replay requests to their family-specific execution helpers.
pub(super) fn replay_vector_family(core: &GraphCore, method: &Method) -> bool {
    replay_cluster(core, method)
        || replay_anomaly(core, method)
        || replay_classify_or_reduce(core, method)
        || replay_entity_resolve(core, method)
}

/// Build replay rows once, skipping the operation when the source is empty.
pub(super) fn replay_vectors<F>(
    core: &GraphCore,
    features: &[Vec<f64>],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
    apply: F,
) where
    F: FnOnce(&[Vec<f64>], &[String]),
{
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
    apply(&rows, &ids);
}

/// Route scalar-series replay requests to their family-specific execution helpers.
pub(super) fn replay_series_family(core: &GraphCore, method: &Method) -> bool {
    replay_forecast(core, method) || replay_causal_impact(core, method)
}

/// Route document/topology replay requests to their family-specific execution helpers.
pub(super) fn replay_document_family(core: &GraphCore, method: &Method) -> bool {
    replay_text(core, method) || replay_subgraph(core, method)
}

/// Route graph-insight replay requests to their family-specific execution helpers.
pub(super) fn replay_graph_family(core: &GraphCore, method: &Method) -> bool {
    replay_process(core, method)
        || replay_root_cause(core, method)
        || replay_risk_propagation(core, method)
        || replay_ontology_gap(core, method)
        || replay_retrieval_quality(core, method)
        || replay_community(core, method)
}

/// Recognize, validate, and execute an association-rule writeback replay.
pub(super) fn replay_associate(core: &GraphCore, method: &Method) -> bool {
    let Method::MineAssociate {
        transactions,
        source,
        min_support,
        min_confidence,
        algorithm,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    let txns = build_transactions(core, transactions, source);
    let rules =
        association::mine_labeled(&txns, *min_support, *min_confidence, to_algo(*algorithm));
    materialize_rules(core, &rules);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_rule_claims(core, &rules, source);
    }
    true
}

/// Recognize, validate, and execute a clustering writeback replay.
pub(super) fn replay_cluster(core: &GraphCore, method: &Method) -> bool {
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
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    let (rows, ids) = build_vectors_replay(
        core,
        features,
        source,
        #[cfg(feature = "query")]
        plan,
    );
    if rows.is_empty() {
        return true;
    }
    let algo = cluster_algo(*algorithm, *eps, *min_pts, *k, *linkage, *max_iter, *seed);
    let out = cluster::cluster(&rows, algo);
    materialize_clusters(core, &out, &ids, *algorithm);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_cluster_claims(core, &out, &ids, *algorithm, cluster_provenance(source));
    }
    true
}

/// Recognize, validate, and execute an anomaly-detection writeback replay.
pub(super) fn replay_anomaly(core: &GraphCore, method: &Method) -> bool {
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
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    let (rows, ids) = build_anomaly_rows_replay(
        core,
        features,
        values,
        source,
        #[cfg(feature = "query")]
        plan,
    );
    if rows.is_empty() {
        return true;
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
        materialize_anomaly_claims(core, &out, &ids, *algorithm, anomaly_provenance(source));
    }
    true
}

/// Recognize and execute classifier-prediction or dimensional-reduction replay.
pub(super) fn replay_classify_or_reduce(core: &GraphCore, method: &Method) -> bool {
    match method {
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
            replay_classify_predict_rows(
                core,
                model,
                x,
                source,
                #[cfg(feature = "query")]
                plan,
                #[cfg(feature = "epistemic")]
                as_claim,
            );
            true
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
            replay_reduce_rows(
                core,
                ReduceReplay {
                    x,
                    source,
                    #[cfg(feature = "query")]
                    plan,
                    labels,
                    algorithm: *algorithm,
                    n_components: *n_components,
                    n_neighbors: *n_neighbors,
                    min_dist: *min_dist,
                    perplexity: *perplexity,
                    epochs: *epochs,
                    lr: *lr,
                    seed: *seed,
                    #[cfg(feature = "epistemic")]
                    as_claim,
                },
            );
            true
        }
        _ => false,
    }
}

/// Execute a classifier-prediction writeback replay after request matching.
pub(super) fn replay_classify_predict_rows(
    core: &GraphCore,
    model: &FittedClassifier,
    x: &[Vec<f64>],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
    #[cfg(feature = "epistemic")] as_claim: &bool,
) {
    replay_vectors(
        core,
        x,
        source,
        #[cfg(feature = "query")]
        plan,
        |rows, ids| {
            let out = classify::predict(model, rows);
            materialize_classifications(core, &out, ids);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_classification_claims(core, &out, ids, classify_provenance(source));
            }
        },
    );
}

/// Execute a dimensional-reduction writeback replay after request matching.
pub(super) struct ReduceReplay<'a> {
    x: &'a [Vec<f64>],
    source: &'a Option<VectorSource>,
    #[cfg(feature = "query")]
    plan: &'a Option<crate::wire::Plan>,
    labels: &'a [i64],
    algorithm: ReduceAlgorithm,
    n_components: usize,
    n_neighbors: usize,
    min_dist: f64,
    perplexity: f64,
    epochs: usize,
    lr: f64,
    seed: u64,
    #[cfg(feature = "epistemic")]
    as_claim: &'a bool,
}

pub(super) fn replay_reduce_rows(core: &GraphCore, request: ReduceReplay<'_>) {
    let ReduceReplay {
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
        #[cfg(feature = "epistemic")]
        as_claim,
    } = request;
    replay_vectors(
        core,
        x,
        source,
        #[cfg(feature = "query")]
        plan,
        |rows, ids| {
            let algo = reduce_algo(
                algorithm,
                n_neighbors,
                min_dist,
                perplexity,
                epochs,
                lr,
                seed,
            );
            let lbls = (!labels.is_empty()).then_some(labels);
            let out = reduce::reduce(rows, lbls, algo, n_components);
            materialize_embeddings(core, &out, ids);
            #[cfg(feature = "epistemic")]
            if *as_claim {
                materialize_reduce_claims(core, rows, &out, ids, algorithm, source);
            }
        },
    );
}

/// Recognize, validate, and execute a sequential-pattern writeback replay.
pub(super) fn replay_sequence(core: &GraphCore, method: &Method) -> bool {
    let Method::MineSequence {
        sequences,
        source,
        min_support,
        algorithm,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    let seqs = build_sequences(core, sequences, source);
    let patterns = sequence::mine_labeled(&seqs, *min_support, to_seq_algo(*algorithm));
    materialize_patterns(core, &patterns);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_sequence_claims(core, &patterns, sequence_provenance(source));
    }
    true
}

/// Recognize, validate, and execute a forecast writeback replay.
pub(super) fn replay_forecast(core: &GraphCore, method: &Method) -> bool {
    let Method::MineForecast {
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
    } = method
    else {
        return false;
    };
    if values.is_empty() {
        return true;
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
    true
}

/// Recognize, validate, and execute a topic-mining writeback replay.
pub(super) fn replay_text(core: &GraphCore, method: &Method) -> bool {
    let Method::MineText {
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
    } = method
    else {
        return false;
    };
    if matches!(algorithm, TextAlgorithm::Tfidf) {
        return true; // tfidf has no topics to write back
    }
    let (tokenized, ids) = build_text_docs(core, docs, source);
    if tokenized.is_empty() {
        return true;
    }
    let algo = to_text_algo(*algorithm, *k, *alpha, *beta, *iterations, *seed);
    let out = text::mine_labeled(&tokenized, algo, *top_n);
    materialize_topics(core, &out, &ids, text_algo_name(*algorithm));
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_topic_claims(core, &out, text_algo_name(*algorithm));
    }
    true
}

/// Recognize, validate, and execute a frequent-subgraph writeback replay.
pub(super) fn replay_subgraph(core: &GraphCore, method: &Method) -> bool {
    let Method::MineSubgraph {
        label,
        min_support,
        max_edges,
        algorithm,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    if matches!(algorithm, SubgraphAlgorithm::Motif) {
        return true; // motif has no patterns to write back
    }
    let (host, ids) = build_host_graph(core, label);
    if host.node_count() == 0 {
        return true;
    }
    let results = subgraph::mine_gspan(&host, *min_support, *max_edges);
    materialize_subgraphs(core, &results, &ids);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_subgraph_claims(core, &results, subgraph_provenance(label));
    }
    true
}

/// Recognize, validate, and execute an entity-resolution writeback replay.
pub(super) fn replay_entity_resolve(core: &GraphCore, method: &Method) -> bool {
    let Method::MineEntityResolve {
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
    } = method
    else {
        return false;
    };
    if !records.is_empty() {
        let keys = resolve_block_keys(block_keys, records.len());
        let matches = entity_resolution::link_records(records, &keys, *threshold);
        let resolved_ids = resolve_ids(ids, records.len());
        materialize_entity_matches(core, &matches, &resolved_ids, "jaccard");
        #[cfg(feature = "epistemic")]
        if *as_claim {
            materialize_entity_match_claims(core, &matches, &resolved_ids, "records:explicit");
        }
        return true;
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
        return true;
    }
    let matches = entity_resolution::resolve_entities(&rows, *bucket_precision, *threshold);
    materialize_entity_matches(core, &matches, &resolved_ids, "cosine");
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_entity_match_claims(core, &matches, &resolved_ids, &entity_provenance(source));
    }
    true
}

/// Recognize, validate, and execute a causal-impact writeback replay.
pub(super) fn replay_causal_impact(core: &GraphCore, method: &Method) -> bool {
    let Method::MineCausalImpact {
        series,
        control,
        intervention_index,
        series_id,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    if series.is_empty() {
        return true;
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
    true
}

/// Recognize, validate, and execute a process-model writeback replay.
pub(super) fn replay_process(core: &GraphCore, method: &Method) -> bool {
    let Method::MineProcess {
        traces,
        process_id,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    if traces.is_empty() {
        return true;
    }
    let (labels, model) = process_mining::alpha_lite_labeled(traces);
    materialize_process_model(core, &model, &labels, process_id);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_process_model_claim(core, &model, &labels, process_id);
    }
    true
}

/// Recognize, validate, and execute a root-cause writeback replay.
pub(super) fn replay_root_cause(core: &GraphCore, method: &Method) -> bool {
    let Method::MineRootCause {
        nodes,
        scores,
        edges,
        symptom,
        max_hops,
        decay,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    let Some(out) = run_root_cause(nodes, scores, edges, symptom, *max_hops, *decay) else {
        return true;
    };
    materialize_root_cause(core, &out, nodes, symptom);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_root_cause_claim(core, &out, nodes, symptom);
    }
    true
}

/// Recognize, validate, and execute a risk-propagation writeback replay.
pub(super) fn replay_risk_propagation(core: &GraphCore, method: &Method) -> bool {
    let Method::MineRiskPropagation {
        nodes,
        seed,
        edges,
        damping,
        tolerance,
        max_iterations,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    if nodes.is_empty() {
        return true;
    }
    let out = run_risk_propagation(nodes, seed, edges, *damping, *tolerance, *max_iterations);
    materialize_risk_scores(core, &out, nodes);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_risk_score_claims(core, &out, nodes);
    }
    true
}

/// Recognize, validate, and execute an ontology-gap writeback replay.
pub(super) fn replay_ontology_gap(core: &GraphCore, method: &Method) -> bool {
    let Method::MineOntologyGap {
        label,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    let (classes, class_ids) = build_ontology_classes(core, label);
    if classes.is_empty() {
        return true;
    }
    let gaps = ontology_gap::find_gaps(&classes);
    materialize_ontology_gaps(core, &gaps, &class_ids);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_ontology_gap_claims(core, &gaps, &class_ids, label);
    }
    true
}

/// Recognize, validate, and execute a retrieval-quality writeback replay.
pub(super) fn replay_retrieval_quality(core: &GraphCore, method: &Method) -> bool {
    let Method::MineRetrievalQuality {
        traces,
        k,
        query_id,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    if traces.is_empty() {
        return true;
    }
    let specs: Vec<retrieval_quality::RetrievalTrace> =
        traces.iter().map(to_retrieval_trace).collect();
    let report = retrieval_quality::evaluate(&specs, *k);
    materialize_retrieval_quality(core, &report, query_id);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_retrieval_quality_claim(core, &report, query_id);
    }
    true
}

/// Recognize, validate, and execute a community-detection writeback replay.
pub(super) fn replay_community(core: &GraphCore, method: &Method) -> bool {
    let Method::MineCommunity {
        label,
        algorithm,
        resolution,
        max_iterations,
        seed,
        weighted,
        writeback: true,
        #[cfg(feature = "epistemic")]
        as_claim,
    } = method
    else {
        return false;
    };
    let (graph, ids) = build_id_graph(core, label);
    if graph.node_count() == 0 {
        return true;
    }
    let algo = to_community_algo(*algorithm);
    let out = community::detect(&graph, algo, *resolution, *max_iterations, *seed, *weighted);
    materialize_communities(core, &out, &ids);
    #[cfg(feature = "epistemic")]
    if *as_claim {
        materialize_community_claims(core, &out, &ids, community_provenance(label));
    }
    true
}
